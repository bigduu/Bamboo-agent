//! Public Runtime -> genuine unregistered Reader overlay -> compressor -> next request.
use async_trait::async_trait;
use bamboo_agent_core::{
    tools::{
        observed_tool_output_cap, scope_tool_output_cap, FunctionSchema, Tool, ToolCall, ToolClass,
        ToolCtx, ToolError, ToolExecutionContext, ToolExecutor, ToolMutability, ToolOutcome,
        ToolResult, ToolSchema,
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

// Separate from the persisted-override adapter above: this consumer can only
// compose the actual dispatch observation. Disk/model defaults never fill it.
struct ScopedCaller {
    caller: Arc<Caller>,
    caps: Arc<Mutex<Vec<(String, Option<u32>)>>>,
}
#[async_trait]
impl SkillCatalogCallerResolver for ScopedCaller {
    async fn resolve(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let mut caller = self.caller.current.read().await.clone();
        if self
            .caller
            .compose
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let cap = observed_tool_output_cap(ctx);
            if cap.is_some() {
                assert_eq!(ctx.session_id(), Some(caller.session_id.as_str()));
            }
            self.caps
                .lock()
                .unwrap()
                .push((ctx.tool_call_id.to_string(), cap));
            caller.response_bytes =
                bamboo_server_tools::skill_response_byte_budget(caller.response_bytes, cap)?;
        }
        Ok(caller)
    }
}

struct ObservedReader {
    reader: Arc<SkillsReadTool>,
    replacement: Mutex<Option<Arc<SkillsReadTool>>>,
    shadow: bool,
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
        let reader = self
            .replacement
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| self.reader.clone());
        let result = if self.shadow {
            // Intentional same-name custom implementation, with no Reader/resolver call.
            Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                json!({"contents":"界🦀\n\"\\".repeat(2_000)}).to_string(),
            )))
        } else {
            reader.invoke(args, ctx).await
        };
        let outcome = match result {
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
    args: Mutex<Value>,
    generation: std::sync::atomic::AtomicUsize,
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
            let mut args = self.args.lock().unwrap().clone();
            let generation = self.generation.load(std::sync::atomic::Ordering::SeqCst);
            if let Some(cursor) = cursor {
                args["cursor"] = json!(cursor);
            }
            let mut calls = vec![ToolCall {
                id: if generation > 0 {
                    format!("actual-reader-call-r{generation}-{index}")
                } else if index == 0 {
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
    caps: Vec<(String, Option<u32>)>,
    actual_cap: Option<u32>,
    resolved_cap: Option<u32>,
    previous_cap: Option<u32>,
}
async fn public_reader_round(
    response_bytes: usize,
    tokens: Option<u32>,
    compose: bool,
    eof: bool,
    parallel: bool,
) -> Observation {
    reader_round(response_bytes, tokens, compose, eof, parallel, false).await
}
async fn reader_round(
    response_bytes: usize,
    tokens: Option<u32>,
    compose: bool,
    eof: bool,
    parallel: bool,
    scoped: bool,
) -> Observation {
    reader_round_with(response_bytes, tokens, compose, eof, parallel, scoped, None).await
}
async fn reader_round_with(
    response_bytes: usize,
    tokens: Option<u32>,
    compose: bool,
    eof: bool,
    parallel: bool,
    scoped: bool,
    repeat: Option<(&str, bool)>,
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
    let caps = Arc::new(Mutex::new(Vec::new()));
    let resolver: Arc<dyn SkillCatalogCallerResolver> = if scoped {
        Arc::new(ScopedCaller {
            caller: caller.clone(),
            caps: caps.clone(),
        })
    } else {
        caller.clone()
    };
    let catalog = SkillsListTool::new(
        manager.clone(),
        config.clone(),
        repo.clone(),
        resolver.clone(),
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
    let reader = Arc::new(ObservedReader {
        reader: Arc::new(SkillsReadTool::new(catalog)),
        replacement: Mutex::new(None),
        shadow: repeat == Some(("shadow", false)),
        started: started.clone(),
        pages: pages.clone(),
        errors: errors.clone(),
    });
    let overlay = Arc::new(OverlayToolExecutor::new(
        Arc::new(Neighbor { started }),
        reader.clone(),
    ));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(RecordingProvider {
        args: Mutex::new(json!({"package":package,"resource":"references/raw.txt"})),
        generation: std::sync::atomic::AtomicUsize::new(0),
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
        .skill_manager(manager.clone())
        .metrics_collector(metrics)
        .config(config.clone())
        .provider(provider.clone())
        .default_tools(overlay)
        .build()
        .unwrap();
    run_reader_runtime(&runtime, &mut session, dir.path()).await;
    let mut previous_cap = None;
    if let Some((change, cold)) = repeat.filter(|(change, _)| *change != "shadow") {
        assert!(errors.lock().unwrap().is_empty());
        let (id, raw) = pages.lock().unwrap()[0].clone();
        let page: Value = serde_json::from_str(&raw).unwrap();
        let cursor = page["next_cursor"]
            .as_str()
            .expect("first call must leave a continuation");
        previous_cap = session
            .effective_token_budget()
            .map(|b| b.max_tool_output_tokens);
        {
            let recorded = requests.lock().unwrap();
            let reply = exact_reply(recorded.last().unwrap(), &id);
            assert_eq!(reply.content, raw);
            assert_provider_pages(recorded.last().unwrap(), reply, Some(1_024));
        }
        match change {
            "cap" => {
                session
                    .token_budget
                    .as_mut()
                    .unwrap()
                    .max_tool_output_tokens = 2_048
            }
            "clear" => {
                session.token_budget = None;
                session.resolved_token_budget = None;
            }
            "ceiling" => caller.current.write().await.ceiling = Some(BTreeSet::new()),
            "disabled" => config
                .write()
                .await
                .skills
                .disabled
                .push("output-fixture".into()),
            "input" => {
                let next = Message::user("new current input without a Skill selection");
                caller.current.write().await.input_id = next.id.clone();
                caller.current.write().await.invocation = None;
                session.messages.push(next);
            }
            "manual" => caller.current.write().await.invocation = None,
            "session" => {
                session.root_orchestration_only = true;
                session.root_tool_authority_revision += 1;
            }
            "raw" => std::fs::write(bundle.join("references/raw.txt"), "foreign bytes").unwrap(),
            "policy" => {
                std::fs::write(
                    bundle.join("agents/bamboo.yaml"),
                    "invocation_policy:\n  explicit: false\n  automatic: false\n",
                )
                .unwrap();
                manager.store().reload().await.unwrap();
            }
            _ => panic!("unknown repeat {change}"),
        }
        repo.save(&mut session).await.unwrap();
        if cold {
            *reader.replacement.lock().unwrap() =
                Some(Arc::new(SkillsReadTool::new(SkillsListTool::new(
                    manager.clone(),
                    config.clone(),
                    repo.clone(),
                    resolver.clone(),
                ))));
        }
        if !cold && change != "clear" {
            provider.args.lock().unwrap()["cursor"] = json!(cursor);
        }
        provider
            .generation
            .store(1, std::sync::atomic::Ordering::SeqCst);
        requests.lock().unwrap().clear();
        pages.lock().unwrap().clear();
        errors.lock().unwrap().clear();
        caps.lock().unwrap().clear();
        run_reader_runtime(&runtime, &mut session, dir.path()).await;
    }
    let requests = requests.lock().unwrap().clone();
    let pages = pages.lock().unwrap().clone();
    let errors = errors.lock().unwrap().clone();
    let caps = caps.lock().unwrap().clone();
    Observation {
        requests,
        pages,
        errors,
        persisted: storage.load_session(&session.id).await.unwrap().unwrap(),
        raw,
        caps,
        actual_cap: session
            .effective_token_budget()
            .map(|b| b.max_tool_output_tokens),
        previous_cap,
        resolved_cap: session
            .resolved_token_budget
            .as_ref()
            .map(|(_, b)| b.max_tool_output_tokens),
    }
}

async fn run_reader_runtime(
    runtime: &bamboo_engine::AgentRuntime,
    session: &mut Session,
    data_dir: &std::path::Path,
) {
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
            session,
            ExecuteRequestBuilder::new("", event_tx, CancellationToken::new())
                .app_data_dir(data_dir.to_path_buf())
                .model("test-model")
                .selected_skill_ids(vec![])
                .build(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    events.await.unwrap();
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
    for scoped in [false, true] {
        let observed = reader_round(8_000, Some(256), true, false, true, scoped).await;
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

#[tokio::test]
async fn skill_output_scoped_original_uncomposed_page_requires_same_dispatch_budget() {
    let observed = reader_round(8_000, Some(1_024), true, false, false, true).await;
    assert_eq!(observed.pages.len(), 1);
    let (id, raw) = &observed.pages[0];
    assert_eq!(
        exact_reply(&observed.requests[1], id).content,
        *raw,
        "original actual Reader JSON is truncated without same-dispatch composition"
    );
}

#[tokio::test]
async fn skill_output_scoped_producer_supplies_actual_runtime_reader_cap() {
    let observed = reader_round(8_000, Some(1_024), true, false, false, true).await;
    assert!(
        observed.errors.is_empty(),
        "scoped producer missing: {:?}",
        observed.errors
    );
    assert_eq!(observed.pages.len(), 1);
    assert_eq!(observed.actual_cap, Some(1_024));
    assert!(!observed.caps.is_empty());
    for (id, cap) in &observed.caps {
        assert_eq!(id, "actual-reader-call");
        assert_eq!(*cap, observed.actual_cap);
    }
    let (id, raw) = &observed.pages[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.content, *raw);
    assert_eq!(reply.tool_success, Some(true));
    assert_provider_pages(&observed.requests[1], reply, Some(1_024));
}

fn assert_scoped_success(observed: &Observation) {
    assert!(observed.errors.is_empty(), "{:?}", observed.errors);
    assert!(!observed.pages.is_empty());
    assert!(!observed.caps.is_empty());
    for (id, cap) in &observed.caps {
        assert!(observed.pages.iter().any(|(call, _)| call == id));
        assert_eq!(*cap, observed.actual_cap);
    }
    let limit = observed
        .actual_cap
        .filter(|cap| *cap != 0)
        .unwrap_or(8_000)
        .min(8_000);
    for (id, raw) in &observed.pages {
        let next = observed
            .requests
            .iter()
            .skip(1)
            .find(|messages| {
                messages
                    .iter()
                    .any(|m| m.tool_call_id.as_deref() == Some(id))
            })
            .unwrap();
        let reply = exact_reply(next, id);
        assert_eq!(reply.tool_success, Some(true));
        assert_eq!(reply.content, *raw);
        assert_eq!(exact_reply(&observed.persisted.messages, id).content, *raw);
        assert_provider_pages(next, reply, Some(limit as usize));
    }
}

#[tokio::test]
async fn skill_output_scoped_fresh_resolved_default_override_and_known_zero() {
    for tokens in [None, Some(1_024), Some(0)] {
        let observed = reader_round(8_000, tokens, true, false, false, true).await;
        assert_scoped_success(&observed);
        assert!(observed.persisted.resolved_token_budget.is_none());
        assert!(!serde_json::to_value(&observed.persisted)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("resolved_token_budget"));
        match tokens {
            None => {
                assert!(observed.persisted.token_budget.is_none());
                assert!(observed.resolved_cap.is_some());
                assert_eq!(observed.actual_cap, observed.resolved_cap);
            }
            Some(cap) => assert_eq!(observed.actual_cap, Some(cap)),
        }
    }
}

#[tokio::test]
async fn skill_output_scoped_runtime_cleared_defaults_resolve_again() {
    let observed = reader_round_with(
        8_000,
        Some(1_024),
        true,
        false,
        false,
        true,
        Some(("clear", false)),
    )
    .await;
    assert_eq!(observed.previous_cap, Some(1_024));
    assert!(observed.persisted.token_budget.is_none());
    assert!(observed.persisted.resolved_token_budget.is_none());
    assert!(observed.resolved_cap.is_some());
    assert_eq!(observed.actual_cap, observed.resolved_cap);
    assert_scoped_success(&observed);
}

#[tokio::test]
async fn skill_output_scoped_continuation_rejects_changed_actual_cap() {
    let observed = reader_round_with(
        8_000,
        Some(1_024),
        true,
        false,
        false,
        true,
        Some(("cap", false)),
    )
    .await;
    assert_eq!(observed.previous_cap, Some(1_024));
    assert_eq!(observed.actual_cap, Some(2_048));
    assert!(observed.caps.iter().all(|(_, cap)| *cap == Some(2_048)));
    assert!(observed.pages.is_empty());
    assert_eq!(observed.errors.len(), 1);
    assert!(observed.errors[0].1.contains("changed"));
    let reply = exact_reply(&observed.requests[1], &observed.errors[0].0);
    assert_eq!(reply.tool_success, Some(false));
    assert!(reply.content.contains(&observed.errors[0].1));
    assert_provider_pages(&observed.requests[1], reply, None);
}

#[tokio::test]
async fn skill_output_scoped_runtime_warm_and_cold_revocation_remains_mandatory() {
    for change in [
        "ceiling", "disabled", "input", "manual", "session", "raw", "policy",
    ] {
        for cold in [false, true] {
            // An authorized fresh cold capture can read changed auxiliary bytes;
            // only the historical warm continuation must reject that replacement.
            if cold && change == "raw" {
                continue;
            }
            let observed = reader_round_with(
                8_000,
                Some(1_024),
                true,
                false,
                false,
                true,
                Some((change, cold)),
            )
            .await;
            assert!(observed.pages.is_empty(), "{change}, cold={cold}");
            if change == "session" {
                // Root tightening is enforced even earlier than Reader freshness:
                // no executor dispatch or cap observation may occur at all.
                assert!(observed.errors.is_empty());
                assert!(observed.caps.is_empty());
                let reply = exact_reply(&observed.requests[1], "actual-reader-call-r1-0");
                assert_eq!(reply.tool_success, Some(false));
                assert_provider_pages(&observed.requests[1], reply, None);
                continue;
            }
            assert_eq!(observed.errors.len(), 1, "{change}, cold={cold}");
            assert!(!observed.caps.is_empty());
            assert!(observed
                .caps
                .iter()
                .all(|(_, cap)| *cap == observed.actual_cap));
            let reply = exact_reply(&observed.requests[1], &observed.errors[0].0);
            assert_eq!(reply.tool_success, Some(false));
            assert!(reply.content.contains(&observed.errors[0].1));
            assert_provider_pages(&observed.requests[1], reply, None);
        }
    }
}

#[tokio::test]
async fn skill_output_scoped_parallel_pages_reconstruct_utf8_through_true_eof() {
    let observed = reader_round(8_000, Some(1_024), true, true, true, true).await;
    assert_scoped_success(&observed);
    assert!(observed.pages.len() > 1);
    let mut raw = String::new();
    let mut cursors = BTreeSet::new();
    for (index, (_, text)) in observed.pages.iter().enumerate() {
        let page: Value = serde_json::from_str(text).unwrap();
        let content = page["contents"].as_str().unwrap();
        assert!(!content.is_empty());
        raw.push_str(content);
        if index + 1 == observed.pages.len() {
            assert!(page["next_cursor"].is_null());
        } else {
            assert!(cursors.insert(page["next_cursor"].as_str().unwrap().to_string()));
        }
    }
    assert_eq!(raw, observed.raw);
    assert_eq!(
        exact_reply(observed.requests.last().unwrap(), "neighbor-call").content,
        "neighbor unchanged"
    );
}

#[tokio::test]
async fn skill_output_scoped_same_name_shadow_keeps_generic_truncation() {
    let observed = reader_round_with(
        8_000,
        Some(1_024),
        true,
        false,
        false,
        true,
        Some(("shadow", false)),
    )
    .await;
    assert!(
        observed.caps.is_empty(),
        "shadow must not call the real Reader resolver"
    );
    assert_eq!(observed.pages.len(), 1);
    let (id, raw) = &observed.pages[0];
    let reply = exact_reply(&observed.requests[1], id);
    assert_eq!(reply.tool_success, Some(true));
    assert_ne!(reply.content, *raw);
    assert!(reply.content.contains("tool output truncated"));
    assert_provider_pages(&observed.requests[1], reply, None);
}

struct ScopeProbe(&'static str);
#[async_trait]
impl Tool for ScopeProbe {
    fn name(&self) -> &str {
        self.0
    }
    fn description(&self) -> &str {
        "inline observation probe"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn classify(&self, _: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        tokio::task::yield_now().await;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            json!({
                "cap": observed_tool_output_cap(&ctx), "call": ctx.tool_call_id.as_ref(),
                "session":ctx.session_id(), "args":args
            })
            .to_string(),
        )))
    }
}
struct OpaqueExecutor(Arc<dyn ToolExecutor>);
#[async_trait]
impl ToolExecutor for OpaqueExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.execute(call).await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.0.list_tools()
    }
}
struct ContextForwarder(Arc<dyn ToolExecutor>);
#[async_trait]
impl ToolExecutor for ContextForwarder {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.execute(call).await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.0.execute_with_context(call, ctx).await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.0.list_tools()
    }
}

#[tokio::test]
async fn skill_output_scope_crosses_real_inline_wrappers_but_not_default_context_loss() {
    let registry = bamboo_tools::tools::ToolRegistry::new();
    registry.register(ScopeProbe("builtin_probe")).unwrap();
    let builtin: Arc<dyn ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(registry));
    let secondary_registry = bamboo_tools::tools::ToolRegistry::new();
    secondary_registry
        .register(ScopeProbe("secondary_probe"))
        .unwrap();
    let secondary = Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(
        secondary_registry,
    ));
    let composite = Arc::new(bamboo_mcp::CompositeToolExecutor::new(builtin, secondary));
    let overlay: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(
        composite,
        Arc::new(ScopeProbe("overlay_probe")),
    ));
    let forwarded = ContextForwarder(overlay.clone());
    let opaque = OpaqueExecutor(overlay.clone());
    for name in ["builtin_probe", "secondary_probe", "overlay_probe"] {
        let call = ToolCall {
            id: "inline-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: json!({"exact":"界"}).to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some("inline-session");
        scope_tool_output_cap("inline-session", &call.id, Some(37), async {
            let result = forwarded
                .execute_with_context_outcome(&call, ctx)
                .await
                .unwrap()
                .into_tool_result();
            let value: Value = serde_json::from_str(&result.result).unwrap();
            assert_eq!(value["cap"], 37);
            assert_eq!(value["call"], call.id);
            assert_eq!(value["session"], "inline-session");
            assert_eq!(value["args"], json!({"exact":"界"}));
            for result in [
                opaque
                    .execute_with_context_outcome(&call, ctx)
                    .await
                    .unwrap(),
                overlay
                    .execute_with_context_outcome(&call, ToolExecutionContext::none(&call.id))
                    .await
                    .unwrap(),
            ] {
                let value: Value = serde_json::from_str(&result.into_tool_result().result).unwrap();
                assert!(value["cap"].is_null());
                assert!(value["session"].is_null());
                assert_eq!(value["call"], call.id);
            }
            let mut wrong = ctx;
            wrong.tool_call_id = "different-call";
            let result = forwarded
                .execute_with_context_outcome(&call, wrong)
                .await
                .unwrap()
                .into_tool_result();
            let value: Value = serde_json::from_str(&result.result).unwrap();
            assert!(value["cap"].is_null());
        })
        .await;
        assert_eq!(observed_tool_output_cap(&ctx.to_tool_ctx()), None);
    }
}

async fn remote_cap_rpc(body: actix_web::web::Json<Value>) -> actix_web::HttpResponse {
    use actix_web::HttpResponse;
    if body.get("id").is_none() {
        return HttpResponse::Accepted().finish();
    }
    if body["method"] == "server/discover" {
        return HttpResponse::Ok().json(json!({"jsonrpc":"2.0","id":body["id"],
            "error":{"code":-32601,"message":"legacy remote fixture"}}));
    }
    let result = match body["method"].as_str().unwrap() {
        "initialize" => json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},
            "serverInfo":{"name":"remote-cap-fixture","version":"1"}}),
        "tools/list" => json!({"tools":[{"name":"probe","description":"remote probe",
            "inputSchema":{"type":"object"}}]}),
        "tools/call" => {
            assert_eq!(body["params"]["name"], "probe");
            assert_eq!(body["params"]["arguments"], json!({"value":"unchanged"}));
            assert!(body["params"].get("cap").is_none());
            assert!(body["params"].get("session_id").is_none());
            let mut ctx = ToolCtx::none("remote-call");
            ctx.session_id = Some("remote-session".into());
            json!({"content":[{"type":"text", "text":json!({
                "cap":observed_tool_output_cap(&ctx)}).to_string()}],"isError":false})
        }
        _ => json!({}),
    };
    HttpResponse::Ok().json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
}

#[actix_web::test]
async fn skill_output_scope_does_not_cross_actual_mcp_http_dispatch() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = actix_web::HttpServer::new(|| {
        actix_web::App::new().route("/mcp", actix_web::web::post().to(remote_cap_rpc))
    })
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let stop = server.handle();
    let server = tokio::spawn(server);
    let manager = Arc::new(bamboo_mcp::McpServerManager::new());
    manager.start_server(serde_json::from_value(json!({"id":"remote", "name":null,
        "transport":{"type":"streamable_http","url":format!("http://{address}/mcp"),"connect_timeout_ms":3_000},
        "request_timeout_ms":3_000,"reconnect":{"enabled":false,"initial_backoff_ms":1,"max_backoff_ms":1,"max_attempts":1}
    })).unwrap()).await.unwrap();
    let executor = bamboo_mcp::McpToolExecutor::from_manager(manager.clone());
    let name = executor.list_tools()[0].function.name.clone();
    let call = ToolCall {
        id: "remote-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name,
            arguments: json!({"value":"unchanged"}).to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some("remote-session");
    for cap in [None, Some(0), Some(37)] {
        let result = scope_tool_output_cap("remote-session", &call.id, cap, async {
            assert_eq!(observed_tool_output_cap(&ctx.to_tool_ctx()), cap);
            executor.execute_with_context_outcome(&call, ctx).await
        })
        .await
        .unwrap()
        .into_tool_result();
        assert!(result.success);
        assert_eq!(
            serde_json::from_str::<Value>(&result.result).unwrap(),
            json!({"cap":null})
        );
    }
    manager.shutdown_all().await;
    stop.stop(true).await;
    server.await.unwrap().unwrap();
}
