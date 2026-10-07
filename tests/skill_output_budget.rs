//! Public Runtime -> genuine unregistered Reader overlay -> compressor -> next request.
use async_trait::async_trait;
use bamboo_agent_core::{
    tools::{
        FunctionSchema, Tool, ToolCall, ToolClass, ToolCtx, ToolError, ToolExecutor,
        ToolMutability, ToolOutcome, ToolResult, ToolSchema,
    },
    AgentEvent, FunctionCall, Message, Role, Session, Storage,
};
use bamboo_engine::{AgentRuntimeBuilder, ExecuteRequestBuilder, SessionCache, SessionRepository};
use bamboo_llm::{
    provider::{LLMProvider, LLMStream},
    Config, LLMChunk,
};
use bamboo_server_tools::{
    OverlayToolExecutor, SkillCatalogCaller, SkillCatalogCallerResolver, SkillCatalogInvocation,
    SkillsListTool, SkillsReadTool,
};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use futures::stream;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, Notify, RwLock};
use tokio_util::sync::CancellationToken;

struct Caller {
    current: RwLock<SkillCatalogCaller>,
    repo: SessionRepository,
    compose: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl SkillCatalogCallerResolver for Caller {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let mut caller = self.current.read().await.clone();
        if self.compose.load(std::sync::atomic::Ordering::SeqCst) {
            let session = self
                .repo
                .try_load(&caller.session_id)
                .await
                .map_err(|e| ToolError::Execution(e.to_string()))?
                .ok_or_else(|| ToolError::Execution("current host session missing".into()))?;
            // Only this test-owned adapter uses a persisted, explicit override.
            // It makes no claim about the serde-skipped model-resolved default.
            caller.response_bytes = bamboo_server_tools::skill_response_byte_budget(
                caller.response_bytes,
                session
                    .token_budget
                    .as_ref()
                    .map(|b| b.max_tool_output_tokens),
            )?;
        }
        Ok(caller)
    }
}

struct ObservedReader {
    reader: SkillsReadTool,
    started: Arc<Notify>,
    pages: Arc<Mutex<Vec<(String, String)>>>,
    errors: Arc<Mutex<Vec<(String, String)>>>,
}
#[async_trait]
impl Tool for ObservedReader {
    fn name(&self) -> &str {
        self.reader.name()
    }
    fn description(&self) -> &str {
        self.reader.description()
    }
    fn parameters_schema(&self) -> Value {
        self.reader.parameters_schema()
    }
    fn classify(&self, args: &Value) -> ToolClass {
        self.reader.classify(args)
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.started.notify_one();
        let id = ctx.tool_call_id.clone();
        let outcome = match self.reader.invoke(args, ctx).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.errors
                    .lock()
                    .unwrap()
                    .push((id.to_string(), error.to_string()));
                return Err(error); // Actual Reader error, unchanged through Runtime's Err path.
            }
        };
        let ToolOutcome::Completed(ref result) = outcome else {
            panic!("Reader must complete")
        };
        assert!(result.success);
        let page: Value =
            serde_json::from_str(&result.result).expect("Reader's original complete JSON");
        assert!(page["contents"].is_string());
        self.pages
            .lock()
            .unwrap()
            .push((id.to_string(), result.result.clone()));
        Ok(outcome)
    }
}

struct Neighbor {
    started: Arc<Notify>,
}
#[async_trait]
impl ToolExecutor for Neighbor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        assert_eq!(call.function.name, "neighbor");
        // The model issued neighbor first. Completion requires the real Reader
        // to start concurrently; sequential dispatch would time out.
        self.started.notified().await;
        Ok(ToolResult::text(true, "neighbor unchanged"))
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".into(),
            function: FunctionSchema {
                name: "neighbor".into(),
                description: "Independent read-only neighbor".into(),
                parameters: json!({"type":"object","properties":{}}),
            },
        }]
    }
    fn tool_mutability(&self, _: &str) -> ToolMutability {
        ToolMutability::ReadOnly
    }
}

struct RecordingProvider {
    args: Value,
    eof: bool,
    parallel: bool,
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
}
#[async_trait]
impl LLMProvider for RecordingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(messages.to_vec());
        let cursor = messages
            .iter()
            .rev()
            .find(|m| {
                m.role == Role::Tool
                    && m.tool_call_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("actual-reader-call"))
            })
            .and_then(|m| serde_json::from_str::<Value>(&m.content).ok())
            .and_then(|page| page["next_cursor"].as_str().map(str::to_string));
        let chunks = if index == 0 || (self.eof && cursor.is_some()) {
            assert!(index < 50, "bounded fixture must advance to EOF");
            let mut args = self.args.clone();
            if let Some(cursor) = cursor {
                args["cursor"] = json!(cursor);
            }
            let mut calls = vec![ToolCall {
                id: if index == 0 {
                    "actual-reader-call".into()
                } else {
                    format!("actual-reader-call-{index}")
                },
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "skills_read".into(),
                    arguments: args.to_string(),
                },
            }];
            if self.parallel && index == 0 {
                calls.insert(
                    0,
                    ToolCall {
                        id: "neighbor-call".into(),
                        tool_type: "function".into(),
                        function: FunctionCall {
                            name: "neighbor".into(),
                            arguments: "{}".into(),
                        },
                    },
                );
            }
            vec![Ok(LLMChunk::ToolCalls(calls)), Ok(LLMChunk::Done)]
        } else {
            vec![Ok(LLMChunk::Token("done".into())), Ok(LLMChunk::Done)]
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

struct Observation {
    requests: Vec<Vec<Message>>,
    pages: Vec<(String, String)>,
    errors: Vec<(String, String)>,
    persisted: Session,
    raw: String,
}
async fn public_reader_round(
    response_bytes: usize,
    tokens: Option<u32>,
    compose: bool,
    eof: bool,
    parallel: bool,
) -> Observation {
    let dir = tempfile::tempdir().unwrap();
    let skills = dir.path().join("skills");
    let bundle = skills.join("output-fixture");
    std::fs::create_dir_all(bundle.join("agents")).unwrap();
    std::fs::write(
        bundle.join("SKILL.md"),
        "---\nname: output-fixture\ndescription: Reader budget fixture\n---\nbody",
    )
    .unwrap();
    std::fs::write(
        bundle.join("agents").join("bamboo.yaml"),
        "invocation_policy:\n  explicit: true\n  automatic: false\n",
    )
    .unwrap();
    std::fs::create_dir_all(bundle.join("references")).unwrap();
    let raw = "界🦀\r\n\0\"\\ <|endoftext|> <|endofprompt|>\t".repeat(if eof { 20 } else { 200 });
    std::fs::write(bundle.join("references").join("raw.txt"), &raw).unwrap();
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: skills,
        ..Default::default()
    }));
    manager.initialize().await.unwrap();
    let storage = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
            .await
            .unwrap(),
    );
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = SessionRepository::new(
        SessionCache::default(),
        storage.clone(),
        persistence.clone(),
    );
    let mut session = Session::new("output-session", "test-model");
    session
        .messages
        .push(Message::user("use the exact current output-fixture input"));
    session.token_budget = tokens.map(|tokens| bamboo_domain::TokenBudget {
        max_tool_output_tokens: tokens,
        ..Default::default()
    });
    repo.save(&mut session).await.unwrap();
    let persisted = repo.load(&session.id).await.unwrap();
    assert_eq!(
        persisted
            .token_budget
            .as_ref()
            .map(|b| b.max_tool_output_tokens),
        tokens
    );
    let caller = Arc::new(Caller {
        current: RwLock::new(SkillCatalogCaller {
            caller_id: "known-test-host".into(),
            session_id: session.id.clone(),
            input_id: session.messages[0].id.clone(),
            ceiling: Some(BTreeSet::from(["output-fixture".into()])),
            invocation: Some(SkillCatalogInvocation {
                input_id: session.messages[0].id.clone(),
                skills: BTreeSet::from(["output-fixture".into()]),
            }),
            mode: None,
            context_window: Some(128_000),
            metadata_tokens: None,
            response_bytes,
        }),
        repo: repo.clone(),
        compose: std::sync::atomic::AtomicBool::new(false),
    });
    let config = Arc::new(RwLock::new(Config::default()));
    let catalog = SkillsListTool::new(
        manager.clone(),
        config.clone(),
        repo.clone(),
        caller.clone(),
    );
    let mut ctx = ToolCtx::none("locate-only");
    ctx.session_id = Some(session.id.clone().into());
    let listed = catalog
        .invoke(json!({}), ctx)
        .await
        .unwrap()
        .into_tool_result();
    let list: Value = serde_json::from_str(&listed.result).unwrap();
    let package = list["skills"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["name"] == "output-fixture")
        .unwrap()["package"]
        .clone();
    caller
        .compose
        .store(compose, std::sync::atomic::Ordering::SeqCst);
    let started = Arc::new(Notify::new());
    let pages = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let overlay = Arc::new(OverlayToolExecutor::new(
        Arc::new(Neighbor {
            started: started.clone(),
        }),
        Arc::new(ObservedReader {
            reader: SkillsReadTool::new(catalog),
            started,
            pages: pages.clone(),
            errors: errors.clone(),
        }),
    ));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(RecordingProvider {
        args: json!({"package":package,"resource":"references/raw.txt"}),
        eof,
        parallel,
        requests: requests.clone(),
    });
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            dir.path().join("metrics.db"),
        )),
        7,
    );
    let runtime = AgentRuntimeBuilder::new()
        .storage(storage.clone())
        .persistence(persistence)
        .attachment_reader(storage.clone())
        .skill_manager(manager)
        .metrics_collector(metrics)
        .config(config)
        .provider(provider)
        .default_tools(overlay)
        .build()
        .unwrap();
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(128);
    let events = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = event_rx.recv().await {
            events.push(e);
        }
        events
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        runtime.execute(
            &mut session,
            ExecuteRequestBuilder::new("", event_tx, CancellationToken::new())
                .model("test-model")
                .selected_skill_ids(vec![])
                .build(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    events.await.unwrap();
    let requests = requests.lock().unwrap().clone();
    let pages = pages.lock().unwrap().clone();
    let errors = errors.lock().unwrap().clone();
    Observation {
        requests,
        pages,
        errors,
        persisted: storage.load_session(&session.id).await.unwrap().unwrap(),
        raw,
    }
}

fn exact_reply<'a>(messages: &'a [Message], id: &str) -> &'a Message {
    let replies = messages
        .iter()
        .filter(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(id))
        .collect::<Vec<_>>();
    assert_eq!(
        replies.len(),
        1,
        "each original call must have exactly one reply"
    );
    replies[0]
}

#[tokio::test]
async fn skill_output_reader_page_survives_public_runtime_hard_cap() {
    let observed = public_reader_round(8_000, Some(1_024), true, false, false).await;
    assert_eq!(observed.requests.len(), 2);
    assert_eq!(observed.pages.len(), 1);
    assert!(observed.errors.is_empty());
    let (id, raw) = &observed.pages[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(true));
    assert_eq!(
        reply.content, *raw,
        "complete Reader success must survive actual generic compressor"
    );
    assert!(raw.len() <= 1_024);
    serde_json::from_str::<Value>(raw).unwrap();
    assert_eq!(exact_reply(&observed.persisted.messages, id).content, *raw);
}

#[tokio::test]
async fn skill_output_uncomposed_reader_still_uses_generic_same_name_hard_cap() {
    let observed = public_reader_round(8_000, Some(1_024), false, false, false).await;
    assert_eq!(observed.pages.len(), 1);
    let (id, raw) = &observed.pages[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(true));
    assert_ne!(
        reply.content, *raw,
        "a tool name is not a compression bypass"
    );
    assert!(reply.content.contains("tool output truncated"));
}

#[tokio::test]
async fn skill_output_tiny_envelope_is_genuine_reader_error_and_plain_failed_same_id() {
    let observed = public_reader_round(8_000, Some(256), true, false, true).await;
    assert!(
        observed.pages.is_empty(),
        "never manufacture partial success JSON"
    );
    assert_eq!(observed.errors.len(), 1);
    let (id, error) = &observed.errors[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(false));
    assert!(
        reply.content.contains(error),
        "Runtime must project the actual Reader ToolError"
    );
    assert!(serde_json::from_str::<Value>(&reply.content).is_err());
    assert_eq!(
        exact_reply(&observed.persisted.messages, id).content,
        reply.content
    );
    assert_eq!(
        exact_reply(&observed.requests[1], "neighbor-call").content,
        "neighbor unchanged"
    );
    assert_provider_pages(&observed.requests[1], reply, None);
}

#[tokio::test]
async fn skill_output_public_runtime_parallel_pages_reconstruct_true_utf8_eof() {
    let observed = public_reader_round(8_000, Some(1_024), true, true, true).await;
    assert!(observed.errors.is_empty());
    assert!(observed.pages.len() > 1);
    assert_eq!(observed.requests.len(), observed.pages.len() + 1);
    let final_request = observed.requests.last().unwrap();
    let mut reconstructed = String::new();
    let mut cursors = BTreeSet::new();
    for (index, (id, raw)) in observed.pages.iter().enumerate() {
        let reply = exact_reply(final_request, id);
        assert_eq!(reply.tool_success, Some(true));
        assert_eq!(reply.content, *raw);
        assert_eq!(exact_reply(&observed.persisted.messages, id).content, *raw);
        let page: Value = serde_json::from_str(raw).unwrap();
        let content = page["contents"].as_str().unwrap();
        assert!(!content.is_empty(), "nonempty fixture must make progress");
        reconstructed.push_str(content);
        if index + 1 == observed.pages.len() {
            assert!(page["next_cursor"].is_null());
        } else {
            assert!(cursors.insert(page["next_cursor"].as_str().unwrap().to_string()));
        }
        assert_provider_pages(final_request, reply, Some(1_024));
    }
    assert_eq!(reconstructed, observed.raw);
    println!("actual Runtime EOF: {} Reader pages, {} exact raw UTF-8 bytes, original parallel IDs closed", observed.pages.len(), reconstructed.len());
    let batch = final_request
        .iter()
        .find(|m| m.tool_calls.as_ref().is_some_and(|calls| calls.len() == 2))
        .unwrap();
    let batch_index = final_request.iter().position(|m| m.id == batch.id).unwrap();
    let pair = &final_request[batch_index + 1..batch_index + 3];
    assert_eq!(
        pair.iter()
            .filter_map(|m| m.tool_call_id.as_deref())
            .collect::<Vec<_>>(),
        ["neighbor-call", "actual-reader-call"]
    );
    assert_eq!(pair[0].content, "neighbor unchanged");
}

#[tokio::test]
async fn skill_output_unknown_persisted_cap_is_real_failed_reader_call() {
    let observed = public_reader_round(8_000, None, true, false, false).await;
    assert!(observed.persisted.token_budget.is_none());
    assert!(observed.pages.is_empty());
    assert_eq!(observed.errors.len(), 1);
    let (id, error) = &observed.errors[0];
    assert!(error.contains("known current tool-output cap"));
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(false));
    assert!(reply.content.contains(error));
    assert_provider_pages(&observed.requests[1], reply, None);
}

#[tokio::test]
async fn skill_output_known_no_hard_cap_still_preserves_finite_reader_envelope() {
    let observed = public_reader_round(8_000, Some(0), true, false, false).await;
    assert!(observed.errors.is_empty());
    assert_eq!(observed.pages.len(), 1);
    let (id, raw) = &observed.pages[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(true));
    assert_eq!(reply.content, *raw);
    assert_provider_pages(&observed.requests[1], reply, Some(8_000));
}

// These are the actual public outbound converters, applied to the real next-request
// history. Check only the page-bearing item/block, never charge a merged neighbor.
fn assert_provider_pages(messages: &[Message], reply: &Message, bound: Option<usize>) {
    use bamboo_llm::providers::{
        anthropic::build_anthropic_request_with_cache,
        common::{
            openai_compat::messages_to_openai_compat_json,
            openai_responses::{build_responses_body, messages_to_responses_input_json},
        },
    };
    use bamboo_llm::{
        cache::{CacheTtl, PromptCachePlan},
        protocol::{gemini::GeminiRequest, ToProvider},
    };
    let id = reply.tool_call_id.as_deref().unwrap();
    let check = |block: &Value| {
        if let Some(limit) = bound {
            assert!(
                serde_json::to_vec(block).unwrap().len() <= limit,
                "whole page-bearing block: {block}"
            );
        }
    };
    let chat = messages_to_openai_compat_json(messages);
    let block = chat.iter().find(|v| v["tool_call_id"] == id).unwrap();
    assert_eq!(block["content"], reply.content);
    check(block);
    let responses = messages_to_responses_input_json(messages);
    let block = responses
        .iter()
        .find(|v| v["type"] == "function_call_output" && v["call_id"] == id)
        .unwrap();
    assert_eq!(block["output"], reply.content);
    check(block);
    for ttl in [CacheTtl::Default, CacheTtl::Extended] {
        let cache = PromptCachePlan {
            breakpoint_message_ids: vec![reply.id.clone()],
            ttl,
            ..Default::default()
        };
        let responses =
            build_responses_body("gpt-6", messages, &[], None, None, None, None, Some(&cache));
        let block = responses["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["type"] == "function_call_output" && v["call_id"] == id)
            .unwrap();
        assert_eq!(block["output"][0]["text"], reply.content);
        assert_eq!(
            block["output"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        check(block);
        let anthropic = build_anthropic_request_with_cache(
            messages,
            &[],
            "test-model",
            128,
            false,
            None,
            None,
            Some(&cache),
        );
        let blocks = anthropic["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|m| m["content"].as_array().unwrap());
        let block = blocks
            .filter(|v| v["type"] == "tool_result")
            .find(|v| v["tool_use_id"] == id)
            .unwrap();
        assert_eq!(block["content"], reply.content);
        assert_eq!(block["cache_control"]["type"], "ephemeral");
        if ttl == CacheTtl::Extended {
            assert_eq!(block["cache_control"]["ttl"], "1h");
        } else {
            assert!(block["cache_control"].get("ttl").is_none());
        }
        check(block);
    }
    let gemini: GeminiRequest = messages.to_vec().to_provider().unwrap();
    let gemini = serde_json::to_value(gemini).unwrap();
    let block = gemini["contents"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| m["parts"].as_array().unwrap())
        .find(|p| p["functionResponse"]["name"] == id)
        .unwrap();
    let normalized = serde_json::from_str::<Value>(&reply.content)
        .unwrap_or_else(|_| json!({"result":reply.content}));
    assert_eq!(block["functionResponse"]["response"], normalized);
    check(block);
}
