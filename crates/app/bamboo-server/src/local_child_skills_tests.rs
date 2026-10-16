use super::*;
use crate::skill_runtime::tests::{provider, write_skill, SkillsProvider};
use crate::{AgentStatus, AppState};
use bamboo_agent_core::tools::{FunctionCall, ToolSchema};
use bamboo_agent_core::{Message, Role};
use bamboo_domain::{SessionActivationPolicy, SessionMessageEnvelope};
use bamboo_engine::execution::SpawnJob;
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use serde_json::json;
use std::{collections::HashMap, path::Path, sync::Mutex, time::Duration};

static BOUND: std::sync::LazyLock<Mutex<HashMap<String, Arc<ChildExecutor>>>> =
    std::sync::LazyLock::new(Mutex::default);
pub(super) fn observe_bound(tools: &Arc<ChildExecutor>) {
    BOUND
        .lock()
        .unwrap()
        .insert(tools.run.authority.session_id().into(), tools.clone());
}
fn bound(id: &str) -> Arc<ChildExecutor> {
    BOUND.lock().unwrap()[id].clone()
}
fn release(id: &str) {
    BOUND.lock().unwrap().remove(id);
}

async fn state(home: &Path, model: Arc<dyn LLMProvider>) -> actix_web::web::Data<AppState> {
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.into()));
    config.provider = "openai".into();
    let state = AppState::new_with_provider(home.into(), config, model)
        .await
        .unwrap();
    actix_web::web::Data::new(state)
}
async fn seed(
    state: &AppState,
    id: &str,
    ceiling: Option<&str>,
    disabled: Option<&str>,
) -> Session {
    let mut parent = Session::new(format!("{id}-parent"), "test-model");
    parent.title_generated = true;
    parent.add_message(Message::system("Wait for the Child."));
    state.storage.save_session(&parent).await.unwrap();
    let mut child = Session::new_child_of(id, &parent, "test-model", "local reader proof");
    child.title_generated = true;
    child.set_workspace_path_meta(state.app_data_dir.to_string_lossy().into_owned());
    child.set_last_run_status("queued");
    child.add_message(Message::system("Perform the current task."));
    child.add_message(Message::user("OLD_ASSIGNMENT_MUST_NOT_AUTHORIZE_SKILLS"));
    child.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        "[\"stale\"]".into(),
    );
    child.metadata.insert(
        bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_SUMMARY_METADATA_KEY.into(),
        "STALE_SKILL_BODY_NEVER_INJECT".into(),
    );
    if let Some(ceiling) = ceiling {
        child
            .metadata
            .insert("selected_skill_ids".into(), ceiling.into());
    }
    if let Some(disabled) = disabled {
        child
            .metadata
            .insert("disabled_tools".into(), disabled.into());
    }
    // The actual captured local Agent is the supplied fixture provider. A
    // provider reference would route elsewhere; forbid one before launch.
    assert!(
        bamboo_engine::session_app::provider_model::session_effective_model_ref(&child).is_none()
    );
    state.storage.save_session(&child).await.unwrap();
    child
}
async fn admit(state: &AppState, child: &Session, explicit: bool) -> String {
    let mut envelope =
        SessionMessageEnvelope::user_input(&child.id, "Perform the normal proof action");
    if explicit {
        let source = state.skill_manager.store().skill_catalog_snapshot().await;
        let skill = source
            .entries
            .iter()
            .find(|e| e.id == "native-proof" && e.winner)
            .unwrap();
        let request = bamboo_domain::SessionSkillRequest {
            selections: vec![bamboo_domain::SessionSkillSelection {
                id: skill.id.clone(),
                source: "user".into(),
                revision: skill.revision,
                args: json!({}),
            }],
            mode: None,
        };
        let bamboo_domain::SessionMessageBody::Content(content) = &mut envelope.body else {
            unreachable!()
        };
        content.skill_request = Some(request);
    }
    let id = envelope.id.as_str().to_owned();
    state
        .session_messenger
        .admit_with_activation_intent(envelope, SessionActivationPolicy::RespectSpecificWait, None)
        .await
        .unwrap();
    let before = state
        .storage
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!before.messages.iter().any(|m| m.id == id));
    id
}
async fn launch(state: &AppState, child: &Session) {
    state
        .spawn_scheduler
        .enqueue(SpawnJob {
            parent_session_id: child.parent_session_id.clone().unwrap(),
            child_session_id: child.id.clone(),
            model: child.model.clone(),
            disabled_tools: None,
        })
        .await
        .unwrap();
}
async fn done(state: &AppState, id: &str) -> AgentStatus {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let status = state
                .agent_runners
                .read()
                .await
                .get(id)
                .map(|r| r.status.clone());
            if let Some(
                status @ (AgentStatus::Completed | AgentStatus::Error(_) | AgentStatus::Cancelled),
            ) = status
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real same-process Child must terminate")
}
fn call(name: &str) -> ToolCall {
    ToolCall {
        id: "retained-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: "{}".into(),
        },
    }
}

#[actix_web::test]
async fn ordinary_local_child_actual_user_reads_eof_reference_then_allowed_task() {
    let home = tempfile::tempdir().unwrap();
    let expected = write_skill(home.path(), "native-proof", true);
    let model = provider(home.path(), 4096, true);
    let state = state(home.path(), model.clone()).await;
    let child = seed(&state, "child-positive-reader", None, None).await;
    let input = admit(&state, &child, false).await;
    launch(&state, &child).await;
    let terminal = done(&state, &child.id).await;
    assert!(matches!(terminal, AgentStatus::Completed), "{terminal:?}");
    let tools = bound(&child.id);
    assert!(!tools.run.live.load(Ordering::Acquire));
    {
        let trace = model.trace.lock().unwrap();
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
        assert!(trace
            .requests
            .iter()
            .flatten()
            .all(|m| !m.content.contains("STALE_SKILL_BODY_NEVER_INJECT")
                && !m.content.contains("Required skill activation")));
    }
    let stored = state
        .storage
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .messages
            .iter()
            .filter(|m| m.id == input && m.role == Role::User)
            .count(),
        1
    );
    assert!(stored
        .messages
        .iter()
        .any(|m| m.role == Role::Assistant && m.content.contains("Normal task completed")));
    let read = call("skills_list");
    let mut ctx = ToolExecutionContext::none(&read.id);
    ctx.session_id = Some(&child.id);
    assert!(tools
        .execute_with_context_outcome(&read, ctx)
        .await
        .is_err());
    state.shutdown().await;
    release(&child.id);
}

#[actix_web::test]
async fn ordinary_local_child_no_current_user_and_empty_ceiling_have_no_source() {
    for (case, current, ceiling) in [("history", false, None), ("empty", true, Some("[]"))] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "native-proof", true);
        let model = provider(home.path(), 4096, false);
        let state = state(home.path(), model.clone()).await;
        let child = seed(&state, &format!("child-reader-{case}"), ceiling, None).await;
        if current {
            admit(&state, &child, false).await;
        }
        launch(&state, &child).await;
        let terminal = done(&state, &child.id).await;
        let trace = model.trace.lock().unwrap();
        assert!(
            trace.main_pages == 0 && trace.eof.is_empty(),
            "{case}: {terminal:?}"
        );
        assert!(
            matches!(terminal, AgentStatus::Completed),
            "{case}: {terminal:?}"
        );
        if case == "history" {
            assert!(trace
                .requests
                .iter()
                .flatten()
                .all(|m| !m.content.contains("native-proof")));
        }
        drop(trace);
        state.shutdown().await;
        release(&child.id);
    }
}

#[actix_web::test]
async fn ordinary_local_child_unprepared_explicit_input_cannot_authorize_reader() {
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let model = provider(home.path(), 4096, false);
    let state = state(home.path(), model.clone()).await;
    let child = seed(&state, "child-unprepared-explicit", None, None).await;
    admit(&state, &child, true).await;
    launch(&state, &child).await;
    let terminal = done(&state, &child.id).await;
    assert!(
        matches!(terminal, AgentStatus::Error(ref e) if e.contains("not prepared")),
        "{terminal:?}"
    );
    assert!(model.trace.lock().unwrap().requests.is_empty());
    state.shutdown().await;
    release(&child.id);
}

struct HeldProvider {
    inner: Arc<SkillsProvider>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    held: AtomicBool,
}
impl HeldProvider {
    fn new(inner: Arc<SkillsProvider>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            held: AtomicBool::new(false),
        })
    }
}
#[async_trait]
impl LLMProvider for HeldProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        limit: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        if self.inner.has_warm_page(messages) && !self.held.swap(true, Ordering::AcqRel) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.chat_stream(messages, tools, limit, model).await
    }
}

#[actix_web::test]
async fn ordinary_local_child_warm_reader_rechecks_source_disabled_policy_and_cancellation() {
    for change in ["source", "disabled", "policy", "cancel"] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "native-proof", true);
        let model = HeldProvider::new(provider(home.path(), 4096, false));
        let state = state(home.path(), model.clone()).await;
        let child = seed(&state, &format!("child-warm-{change}"), None, None).await;
        admit(&state, &child, false).await;
        launch(&state, &child).await;
        tokio::time::timeout(Duration::from_secs(30), model.entered.notified())
            .await
            .unwrap();
        let retained = bound(&child.id);
        let wrong = call("skills_list");
        let mut ctx = ToolExecutionContext::none(&wrong.id);
        ctx.session_id = Some("other-child");
        assert!(retained
            .execute_with_context_outcome(&wrong, ctx)
            .await
            .is_err());
        let correct = call("skills_list");
        let mut ctx = ToolExecutionContext::none(&correct.id);
        ctx.session_id = Some(&child.id);
        // A live retained executor outside its real Tool/Q scope is not a caller.
        assert!(retained
            .execute_with_context_outcome(&correct, ctx)
            .await
            .is_err());
        let nested = call("SubAgent");
        let mut ctx = ToolExecutionContext::none(&nested.id);
        ctx.session_id = Some(&child.id);
        assert!(retained
            .check_permissions_for_resolved(&nested, "SubAgent", &json!({"action":"list"}), &ctx)
            .await
            .is_ok());
        for name in ["load_skill", "read_skill_resource"] {
            let legacy = call(name);
            let mut ctx = ToolExecutionContext::none(&legacy.id);
            ctx.session_id = Some(&child.id);
            assert!(retained.exact_tool_owner(name).is_none());
            assert!(retained
                .check_permissions_for_exact(&legacy, name, &ctx)
                .await
                .is_err());
            assert!(retained
                .check_permissions_for_resolved(&legacy, name, &json!({}), &ctx)
                .await
                .is_err());
        }
        match change {
            "source" => {
                std::fs::write(
                    home.path().join("skills/native-proof/SKILL.md"),
                    "---\nname: native-proof\ndescription: changed\n---\nCHANGED_SOURCE\n",
                )
                .unwrap();
            }
            "disabled" => {
                state.config.write().await.skills.disabled = vec!["native-proof".into()];
            }
            "policy" => {
                let policy = state.permission_checker.permission_config().unwrap();
                policy.set_enabled(!policy.is_enabled());
            }
            "cancel" => {
                state
                    .agent_runners
                    .read()
                    .await
                    .get(&child.id)
                    .unwrap()
                    .cancel_token
                    .cancel();
            }
            _ => unreachable!(),
        }
        model.release.notify_one();
        let terminal = done(&state, &child.id).await;
        assert!(matches!(
            terminal,
            AgentStatus::Completed | AgentStatus::Error(_) | AgentStatus::Cancelled
        ));
        let trace = model.inner.trace.lock().unwrap();
        assert!(
            trace.eof.is_empty() && !trace.task_started && !trace.task_completed,
            "{change}: {terminal:?}"
        );
        assert!(trace.main_pages <= 1);
        if change != "cancel" {
            assert_eq!(trace.main_pages, 1);
        }
        drop(trace);
        let stored = state
            .storage
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap();
        let warm_pages = stored
            .messages
            .iter()
            .filter(|m| {
                m.role == Role::Tool
                    && m.tool_success == Some(true)
                    && serde_json::from_str::<serde_json::Value>(&m.content)
                        .is_ok_and(|p| p["contents"].is_string() && p["next_cursor"].is_string())
            })
            .count();
        assert_eq!(
            warm_pages, 1,
            "one real page is durable before the held provider; cancel may drop its parsing future"
        );
        assert!(retained.run.check_owner().await.is_err());
        let read = call("skills_read");
        let mut ctx = ToolExecutionContext::none(&read.id);
        ctx.session_id = Some(&child.id);
        assert!(retained
            .execute_with_context_outcome(&read, ctx)
            .await
            .is_err());
        state.shutdown().await;
        release(&child.id);
    }
}

#[derive(Default)]
struct DisabledReaderProvider {
    calls: std::sync::atomic::AtomicUsize,
    denied: AtomicBool,
}
#[async_trait]
impl LLMProvider for DisabledReaderProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert!(!tools.iter().any(|s| matches!(
            s.function.name.as_str(),
            "skills_read" | "load_skill" | "read_skill_resource"
        )));
        let chunks = if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            vec![
                bamboo_llm::LLMChunk::ToolCalls(vec![ToolCall {
                    id: "disabled-reader-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "skills_read".into(),
                        arguments: json!({"package":"unavailable"}).to_string(),
                    },
                }]),
                bamboo_llm::LLMChunk::Done,
            ]
        } else {
            assert!(messages.iter().any(|m| m.tool_call_id.as_deref()
                == Some("disabled-reader-call")
                && m.tool_success == Some(false)));
            self.denied.store(true, Ordering::Release);
            vec![
                bamboo_llm::LLMChunk::Token("Disabled Reader denied; task answered".into()),
                bamboo_llm::LLMChunk::Done,
            ]
        };
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}

#[actix_web::test]
async fn ordinary_local_child_actual_disabled_reader_schema_and_dispatch_agree() {
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let model = Arc::new(DisabledReaderProvider::default());
    let state = state(home.path(), model.clone()).await;
    let child = seed(
        &state,
        "child-disabled-reader",
        None,
        Some("[\"skills_read\"]"),
    )
    .await;
    admit(&state, &child, false).await;
    launch(&state, &child).await;
    let terminal = done(&state, &child.id).await;
    assert!(matches!(terminal, AgentStatus::Completed), "{terminal:?}");
    assert!(model.denied.load(Ordering::Acquire));
    let stored = state
        .storage
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored
        .messages
        .iter()
        .filter(|m| m.role == Role::Tool)
        .all(|m| !m.content.contains("MAIN_EOF_native-proof")));
    state.shutdown().await;
    release(&child.id);
}
