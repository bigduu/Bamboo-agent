//! Actual Native HTTP routes -> reserved Engine -> Reader -> provider/task tests.
use super::*;
use actix_web::{http::StatusCode, test, App};
use bamboo_agent_core::tools::{FunctionCall, ToolSchema};
use bamboo_agent_core::Role;
use bamboo_domain::TokenBudget;
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_llm::{ProviderModelRouter, ProviderRegistry};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Default)]
struct Trace {
    requests: Vec<Vec<Message>>,
    schemas: Vec<Vec<String>>,
    packages: BTreeMap<String, String>,
    contents: BTreeMap<(String, String), String>,
    eof: Vec<(String, String)>,
    pending: Option<(String, String)>,
    index: usize,
    reference: bool,
    task_started: bool,
    task_completed: bool,
    failures: Vec<String>,
    main_pages: usize,
}
static NEXT_PROVIDER_CALL: AtomicU64 = AtomicU64::new(0);

struct SkillsProvider {
    wanted: Vec<String>,
    task: PathBuf,
    read_references: bool,
    cap: u32,
    trace: Mutex<Trace>,
}
impl SkillsProvider {
    fn call(trace: &mut Trace, name: &str, args: Value) -> LLMChunk {
        let id = format!(
            "native-http-call-{}",
            NEXT_PROVIDER_CALL.fetch_add(1, Ordering::Relaxed)
        );
        trace.pending = Some((id.clone(), name.to_owned()));
        LLMChunk::ToolCalls(vec![ToolCall {
            id,
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.to_owned(),
                arguments: args.to_string(),
            },
        }])
    }
    fn next(&self, messages: &[Message], tools: &[ToolSchema]) -> Vec<LLMChunk> {
        let mut trace = self.trace.lock().unwrap();
        trace.requests.push(messages.to_vec());
        trace.schemas.push(
            tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
        );
        assert!(tools.iter().any(|tool| tool.function.name == "skills_read"));
        assert!(!tools.iter().any(|tool| matches!(
            tool.function.name.as_str(),
            "load_skill" | "read_skill_resource"
        )));
        if let Some((call_id, name)) = trace.pending.take() {
            let message = messages
                .iter()
                .rev()
                .find(|message| message.tool_call_id.as_deref() == Some(call_id.as_str()))
                .unwrap();
            if message.tool_success != Some(true) {
                trace.failures.push(message.content.clone());
                return vec![
                    LLMChunk::Token("Skill unavailable; bounded fallback".into()),
                    LLMChunk::Done,
                ];
            }
            if name == "Read" {
                assert!(message.content.contains("NORMAL_TASK_ACTION"));
                trace.task_completed = true;
                return vec![
                    LLMChunk::Token("Normal task completed after complete Skill EOF".into()),
                    LLMChunk::Done,
                ];
            }
            let page: Value = serde_json::from_str(&message.content)
                .expect("generic compressor must preserve complete page JSON");
            let chat = bamboo_llm::providers::common::openai_compat::messages_to_openai_compat_json(
                messages,
            );
            let actual = chat
                .iter()
                .find(|block| block["tool_call_id"] == call_id)
                .unwrap();
            assert_eq!(actual["content"], message.content);
            let ceiling = if self.cap == 0 {
                RESPONSE_BYTES
            } else {
                self.cap as usize
            };
            assert!(
                serde_json::to_vec(actual).unwrap().len() <= ceiling,
                "actual provider page envelope exceeded current dispatch cap"
            );
            if name == "skills_list" {
                for skill in page["skills"].as_array().unwrap() {
                    if self
                        .wanted
                        .iter()
                        .any(|wanted| skill["name"].as_str() == Some(wanted.as_str()))
                    {
                        trace.packages.insert(
                            skill["name"].as_str().unwrap().to_owned(),
                            skill["package"].as_str().unwrap().to_owned(),
                        );
                    }
                }
                if trace.packages.len() != self.wanted.len() {
                    if let Some(cursor) = page["next_cursor"].as_str() {
                        return vec![
                            Self::call(
                                &mut trace,
                                "skills_list",
                                json!({"cursor":cursor,"limit":20}),
                            ),
                            LLMChunk::Done,
                        ];
                    }
                    trace
                        .failures
                        .push("wanted Skill absent from current catalog".into());
                    return vec![
                        LLMChunk::Token("No matching Skill; ordinary fallback".into()),
                        LLMChunk::Done,
                    ];
                }
            } else {
                let wanted = self.wanted[trace.index].clone();
                let resource = if trace.reference {
                    "references/proof.txt"
                } else {
                    "SKILL.md"
                };
                trace
                    .contents
                    .entry((wanted.clone(), resource.to_owned()))
                    .or_default()
                    .push_str(page["contents"].as_str().unwrap());
                if !trace.reference {
                    trace.main_pages += 1;
                }
                if let Some(cursor) = page["next_cursor"].as_str() {
                    let package = trace.packages[&wanted].clone();
                    return vec![
                        Self::call(
                            &mut trace,
                            "skills_read",
                            json!({"package":package,"resource":resource,"cursor":cursor}),
                        ),
                        LLMChunk::Done,
                    ];
                }
                trace.eof.push((wanted, resource.to_owned()));
                if !trace.reference && self.read_references {
                    trace.reference = true;
                } else {
                    trace.reference = false;
                    trace.index += 1;
                }
            }
        } else {
            return vec![
                LLMChunk::Token("Reading the selected Skills.".into()),
                Self::call(&mut trace, "skills_list", json!({"limit":20})),
                LLMChunk::Done,
            ];
        }
        if trace.index < self.wanted.len() {
            let package = trace.packages[&self.wanted[trace.index]].clone();
            let resource = if trace.reference {
                "references/proof.txt"
            } else {
                "SKILL.md"
            };
            return vec![
                Self::call(
                    &mut trace,
                    "skills_read",
                    json!({"package":package,"resource":resource}),
                ),
                LLMChunk::Done,
            ];
        }
        assert_eq!(
            trace.eof.len(),
            self.wanted.len() * if self.read_references { 2 } else { 1 }
        );
        trace.task_started = true;
        vec![
            Self::call(&mut trace, "Read", json!({"file_path":self.task})),
            LLMChunk::Done,
        ]
    }
}
#[async_trait]
impl LLMProvider for SkillsProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        Ok(Box::pin(futures::stream::iter(
            self.next(messages, tools).into_iter().map(Ok),
        )))
    }
}

fn write_skill(home: &Path, id: &str, automatic: bool) -> String {
    let root = home.join("skills").join(id);
    std::fs::create_dir_all(root.join("references")).unwrap();
    std::fs::create_dir_all(root.join("agents")).unwrap();
    let body = format!("---\nname: {id}\ndescription: Inspect the proof task carefully and then perform the normal action.\n---\nRead references/proof.txt when proof is requested.\n{}\nMAIN_EOF_{id}\n", "quoted \"界\" slash \\ content\n".repeat(450));
    std::fs::write(root.join("SKILL.md"), &body).unwrap();
    std::fs::write(
        root.join("references/proof.txt"),
        format!("REFERENCE_EOF_{id}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("agents/bamboo.yaml"),
        format!("invocation_policy:\n  explicit: true\n  automatic: {automatic}\n"),
    )
    .unwrap();
    body
}

async fn state(home: &Path, provider: Arc<dyn LLMProvider>) -> web::Data<AppState> {
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.into()));
    config.provider = "openai".into();
    config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
        model: Some("test-model".into()),
        ..Default::default()
    });
    let mut state = AppState::new_with_provider(home.into(), config, provider.clone())
        .await
        .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        std::collections::HashMap::from([("openai".into(), provider)]),
        "openai".into(),
    ));
    state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
    web::Data::new(state)
}
async fn http(state: &web::Data<AppState>, path: &str, body: Value) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    let req = test::TestRequest::post()
        .uri(path)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .set_json(body)
        .to_request();
    let response = test::call_service(&app, req).await;
    let status = response.status();
    (status, test::read_body_json(response).await)
}
async fn seed(state: &web::Data<AppState>, id: &str, cap: u32) {
    let mut session = Session::new(id, "test-model");
    session.title_generated = true;
    session.token_budget = Some(TokenBudget {
        max_tool_output_tokens: cap,
        ..Default::default()
    });
    state.storage.save_session(&session).await.unwrap();
}
async fn selection(state: &web::Data<AppState>, name: &str) -> bamboo_skills::WorkflowSelection {
    let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
    let entry = catalog
        .entries
        .iter()
        .find(|e| e.id == name && e.winner)
        .unwrap();
    bamboo_skills::WorkflowSelection {
        id: entry.id.clone(),
        source: entry.source,
        revision: entry.revision,
        args: json!({}),
    }
}
async fn done(state: &web::Data<AppState>, id: &str) -> AgentStatus {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(status) = state
                .agent_runners
                .read()
                .await
                .get(id)
                .map(|r| r.status.clone())
            {
                if matches!(
                    status,
                    AgentStatus::Completed | AgentStatus::Error(_) | AgentStatus::Cancelled
                ) {
                    return status;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real HTTP execution must terminate")
}
fn provider(home: &Path, cap: u32, references: bool) -> Arc<SkillsProvider> {
    let task = home.join("task.txt");
    std::fs::write(&task, "NORMAL_TASK_ACTION").unwrap();
    Arc::new(SkillsProvider {
        wanted: vec!["native-proof".into()],
        task,
        read_references: references,
        cap,
        trace: Mutex::default(),
    })
}

#[actix_web::test]
async fn native_http_typed_main_reads_complete_pages_reference_then_actual_task() {
    let home = tempfile::tempdir().unwrap();
    let expected = write_skill(home.path(), "native-proof", false);
    let provider = provider(home.path(), 4096, true);
    let state = state(home.path(), provider.clone()).await;
    let id = "native-typed-pages";
    seed(&state, id, 4096).await;
    let selection = selection(&state, "native-proof").await;
    let image = "data:image/png;base64,aGVsbG8=";
    let (status, receipt) = http(
        &state,
        "/api/v1/chat",
        json!({
            "session_id":id,"message":"Perform the proof task", "workspace_path":home.path(),
            "workflow_selection":selection,"images":[{"base64":image},{"base64":image}]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    let input_id = receipt["message_id"].as_str().unwrap();
    let before = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(!before.messages.iter().any(|m| m.id == input_id));
    assert!(state
        .session_inbox
        .inspect(id)
        .await
        .unwrap()
        .activation_pending());
    let (status, response) =
        http(&state, &format!("/api/v1/sessions/{id}/execute"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let stored = state.storage.load_session(id).await.unwrap().unwrap();
    let user = stored.messages.iter().find(|m| m.id == input_id).unwrap();
    assert_eq!(
        stored.messages.iter().filter(|m| m.id == input_id).count(),
        1
    );
    assert!(user.content.contains("Explicit Skill") && user.content.contains("native-proof"));
    let trace = provider.trace.lock().unwrap();
    assert!(trace.failures.is_empty(), "{:?}", trace.failures);
    assert!(trace.main_pages > 2 && trace.task_completed);
    assert_eq!(
        trace.contents[&("native-proof".into(), "SKILL.md".into())],
        expected
    );
    assert_eq!(
        trace.contents[&("native-proof".into(), "references/proof.txt".into())],
        "REFERENCE_EOF_native-proof\n"
    );
    let actual_user = trace.requests[0].iter().find(|m| m.id == input_id).unwrap();
    assert_eq!(
        serde_json::to_value(actual_user).unwrap(),
        serde_json::to_value(user).unwrap()
    );
    assert_eq!(
        actual_user
            .content_parts
            .as_ref()
            .unwrap()
            .iter()
            .filter(|p| matches!(p, bamboo_domain::MessagePart::ImageUrl { .. }))
            .count(),
        2
    );
    assert!(trace.requests[0]
        .iter()
        .any(|m| m.content.contains("Using Skills") && m.content.contains("native-proof")));
    drop(trace);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
    assert!(state
        .session_inbox
        .was_admitted(
            id,
            &bamboo_domain::SessionMessageId::parse(input_id).unwrap()
        )
        .await
        .unwrap());
}

#[actix_web::test]
async fn native_http_implicit_main_retains_current_user_across_nonew_pages() {
    let home = tempfile::tempdir().unwrap();
    let expected = write_skill(home.path(), "native-proof", true);
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let id = "native-implicit-pages";
    seed(&state, id, 4096).await;
    let (status, receipt) = http(&state, "/api/v1/chat", json!({
        "session_id":id,"message":"Inspect proof and perform its normal action", "workspace_path":home.path()
    })).await;
    assert_eq!(status, StatusCode::CREATED, "{receipt}");
    let (status, body) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let trace = provider.trace.lock().unwrap();
    assert!(trace.failures.is_empty(), "{:?}", trace.failures);
    assert!(trace.requests.len() > 4 && trace.main_pages > 2 && trace.task_completed);
    assert_eq!(
        trace.contents[&("native-proof".into(), "SKILL.md".into())],
        expected
    );
    let user = trace.requests[0]
        .iter()
        .find(|m| m.id == receipt["message_id"].as_str().unwrap())
        .unwrap();
    assert_eq!(user.role, Role::User);
    assert_eq!(user.content, "Inspect proof and perform its normal action");
}
