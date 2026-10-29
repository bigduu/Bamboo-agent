//! Real V2, immutable publication, Maildir and native Host activation. The
//! ordinary Work wait below is deliberately not a stand-in for Planner #1059.
#![cfg(any(target_os = "linux", target_os = "macos"))]
use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{
    Message, ObserverStageOutcome, ObserverViewChange, ProviderModelRef, Session, SessionInboxPort,
    SessionPermissionMode,
};
use bamboo_engine::session_app::child_session::{
    create_child_action, ChildSessionPort, CreateChildInput,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex as StdMutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

const PROVIDER: &str = "subscription-fixture";
const READER: &str = "subscription-reader";
const PARENT: &str = "subscription-parent";
const WORK: &str = "subscription-work";
const SOURCE: &str = "subscription-real-source";
const OBSERVER: &str = "subscription-observer";
const ROOT_A: &str = "subscription-source-a";
const ROOT_B: &str = "subscription-source-b";
const PRIVATE: &str = "SOURCE_PRIVATE_TRANSCRIPT";
const WAIT: Duration = Duration::from_secs(40);

#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    source_calls: AtomicUsize,
    source_inputs: StdMutex<Vec<Vec<Message>>>,
    parent_calls: AtomicUsize,
    parent_inputs: StdMutex<Vec<Vec<Message>>>,
    parent_request_diagnostics: StdMutex<Vec<Value>>,
    diagnostic_home: StdMutex<Option<std::path::PathBuf>>,
    diagnostic_phase: StdMutex<&'static str>,
    observed: StdMutex<Vec<ObserverViewChange>>,
    observed_status: StdMutex<Vec<(ObserverViewChange, String)>>,
    hold_reader: AtomicBool,
    reader_entered: Notify,
    reader_release: Notify,
    work_entered: Notify,
    work_release: Notify,
}

// Read only this test's bounded physical transport. These are diagnostics,
// never admission authority; atomic writes may advance between observations.
fn provider_boundary_snapshot(home: &std::path::Path) -> Value {
    fn read(path: &std::path::Path) -> Option<Value> {
        let file = std::fs::File::open(path).ok()?;
        let mut reader = std::io::Read::take(file, 128 * 1024);
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
    fn collect(
        value: &Value,
        reports: &mut BTreeSet<String>,
        cursors: &mut BTreeSet<u64>,
        runs: &mut BTreeSet<String>,
    ) {
        match value {
            Value::Object(object) => {
                if object
                    .get("content")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.contains("OBSERVATION "))
                {
                    if let Some(id) = object.get("id").and_then(Value::as_str) {
                        reports.insert(id.into());
                    }
                }
                if let Some(cursor) = object.get("last_admitted_sequence").and_then(Value::as_u64) {
                    cursors.insert(cursor);
                }
                if let Some(run) = object.get("run_id").and_then(Value::as_str) {
                    if !run.is_empty() {
                        runs.insert(run.into());
                    }
                }
                for value in object.values() {
                    collect(value, reports, cursors, runs);
                }
            }
            Value::Array(values) => {
                for value in values {
                    collect(value, reports, cursors, runs);
                }
            }
            _ => {}
        }
    }
    let root = home
        .join("sessions")
        .join(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID);
    let mut persisted = Vec::new();
    for name in ["session.json", "runtime.json"] {
        let mut reports = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut runs = BTreeSet::new();
        if let Some(value) = read(&root.join(name)) {
            collect(&value, &mut reports, &mut cursors, &mut runs);
        }
        persisted.push(json!({"file":name,"report_ids":reports,"cursors":cursors,"run_ids":runs.into_iter().take(4).collect::<Vec<_>>()}));
    }
    let mut transport = Vec::new();
    for queue in ["new", "cur"] {
        if let Ok(entries) = std::fs::read_dir(root.join("inbox").join(queue)) {
            for entry in entries.flatten().take(8) {
                if let Some(wrapper) = read(&entry.path()) {
                    let envelope = wrapper
                        .get("body")
                        .filter(|value| value.get("id").is_some())
                        .unwrap_or(&wrapper);
                    transport.push(json!({"queue":queue,"id":envelope.get("id"),"kind":envelope.get("kind"),
                        "correlation_id":envelope.get("correlation_id"),"generation":wrapper.get("generation"),
                        "report1":envelope.to_string().contains("OBSERVATION 1"),"report2":envelope.to_string().contains("OBSERVATION 2")}));
                }
            }
        }
    }
    json!({"persisted":persisted,"transport":transport})
}

#[async_trait]
impl LLMProvider for Probe {
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::provider::LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        if model == PARENT {
            let home = self.diagnostic_home.lock().unwrap().clone();
            let boundary = home.as_deref().map(provider_boundary_snapshot);
            let phase = *self.diagnostic_phase.lock().unwrap();
            let bounded: Vec<_> = messages.iter().enumerate()
                .filter(|(index, _)| *index < 2 || *index + 4 >= messages.len())
                .map(|(_, message)| json!({
                    "role":format!("{:?}",message.role),"characters":message.content.chars().count(),
                    "report1":message.content.contains("OBSERVATION 1"),
                    "report2":message.content.contains("OBSERVATION 2"),
                    "prefix":message.content.chars().take(240).collect::<String>(),
                    "parts":message.content_parts.as_ref().map(Vec::len),
                    "runtime_kind":message.metadata.as_ref().and_then(|value|value.get("runtime_kind")),
                })).collect();
            let mut diagnostics = self.parent_request_diagnostics.lock().unwrap();
            if diagnostics.len() < 16 {
                diagnostics.push(
                    json!({"purpose":options.and_then(|value|value.request_purpose.as_deref()),
                    "session_id":options.and_then(|value|value.session_id.as_deref()),
                    "tools":tools.len(),"messages":messages.len(),"bounded":bounded,
                    "phase":phase,"observed_at":Utc::now(),"boundary":boundary}),
                );
            }
        }
        self.chat_stream(messages, tools, max_output_tokens, model)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        if model != SOURCE {
            assert!(messages
                .iter()
                .all(|message| !message.content.contains(PRIVATE)));
        }
        let chunk = match model {
            READER => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.hold_reader.swap(false, Ordering::SeqCst) {
                    self.reader_entered.notify_one();
                    self.reader_release.notified().await;
                }
                assert_eq!(
                    tools
                        .iter()
                        .map(|tool| tool.function.name.as_str())
                        .collect::<BTreeSet<_>>(),
                    BTreeSet::from(["session_history", "session_history_current"])
                );
                let change = messages
                    .iter()
                    .rev()
                    .find_map(|message| {
                        let (_, suffix) = message.content.split_once("immutable selector: ")?;
                        serde_json::Deserializer::from_str(suffix)
                            .into_iter::<ObserverViewChange>()
                            .next()?
                            .ok()
                    })
                    .expect("actual runtime notice must reach the native provider");
                let id = format!("read-{}", change.event_id.as_str());
                if let Some(result) = messages
                    .iter()
                    .find(|message| message.tool_call_id.as_deref() == Some(id.as_str()))
                {
                    assert_ne!(result.tool_success, Some(false), "{}", result.content);
                    let result: Value = serde_json::from_str(&result.content).unwrap();
                    let content = result["content"].as_str().unwrap().to_owned();
                    assert!(content.contains(&format!("- Root: {}", change.target_session_id)));
                    self.observed.lock().unwrap().push(change.clone());
                    self.observed_status
                        .lock()
                        .unwrap()
                        .push((change.clone(), content));
                    LLMChunk::Token(format!(
                        "OBSERVATION {} is a report, not a control command.",
                        change.sequence
                    ))
                } else {
                    LLMChunk::ToolCalls(vec![ToolCall{id,tool_type:"function".into(),function:FunctionCall{
                        name:"session_history".into(),arguments:json!({"action":"read_context","session_id":change.target_session_id,"revision":change.view_revision,"file":"status","offset":0,"limit":40}).to_string()}}])
                }
            }
            WORK => {
                self.work_entered.notify_one();
                self.work_release.notified().await;
                LLMChunk::Token("Independent original Work completed.".into())
            }
            SOURCE => {
                self.source_inputs.lock().unwrap().push(messages.to_vec());
                self.source_calls.fetch_add(1, Ordering::SeqCst);
                LLMChunk::Token("Synthetic original Root finished.".into())
            }
            PARENT => {
                self.parent_calls.fetch_add(1, Ordering::SeqCst);
                self.parent_inputs.lock().unwrap().push(messages.to_vec());
                LLMChunk::Token("Parent processed its current canonical input.".into())
            }
            _ => panic!("source Roots must not invoke a provider: {model}"),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(7),
                output_tokens: Some(3),
                total_tokens: Some(10),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn host(home: &std::path::Path, probe: Arc<Probe>) -> AppState {
    *probe.diagnostic_home.lock().unwrap() = Some(home.into());
    let mut config = Config::from_data_dir(Some(home.into()));
    config.provider = PROVIDER.into();
    config.subagents_mut().max_concurrent = Some(2);
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    // The genuine Host and native consumer, with only the background wake tick
    // disabled so each failure/restart boundary can be driven deterministically.
    let state =
        AppState::new_with_provider_for_legacy_ingress_test(home.into(), config, probe.clone())
            .await
            .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), probe as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    state
}

struct Fixture {
    home: tempfile::TempDir,
    state: AppState,
    probe: Arc<Probe>,
    reference: SupervisorReference,
}
impl Fixture {
    async fn new() -> Self {
        Self::new_with_native_source(false).await
    }
    async fn new_with_native_source(native_source: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let state = host(home.path(), probe.clone()).await;
        let service = SupervisorSessionService::new(state.session_store.clone());
        let reference =
            SupervisorReference::from(&service.get_or_create_default(PARENT).await.unwrap());
        let mut parent = load(&state, &reference.session_id).await;
        let workspace = home.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        parent.model_ref = Some(ProviderModelRef::new(PROVIDER, PARENT));
        parent.set_provider_name(PROVIDER);
        parent.workspace = Some(workspace.to_string_lossy().into());
        parent.set_workspace_path_meta(workspace.to_string_lossy());
        // A fresh canonical Supervisor has no runtime sidecar until its first
        // run. Initialize the fixture's requested runtime policy just as the
        // actual Observer fixture does, before admitting any execution.
        parent
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .set_permission_mode(SessionPermissionMode::Bypass);
        state.storage.save_session(&parent).await.unwrap();
        let mut projects = BTreeSet::new();
        for id in [ROOT_A, ROOT_B] {
            let project = state.project_store.create(id, None).unwrap();
            projects.insert(project.id.clone());
            let mut target = Session::new(id, "never-source-model");
            target.set_project_id_meta(project.id.to_string());
            if native_source && id == ROOT_A {
                let workspace = home.path().join("source-workspace");
                std::fs::create_dir(&workspace).unwrap();
                target.model = SOURCE.into();
                target.model_ref = Some(ProviderModelRef::new(PROVIDER, SOURCE));
                target.set_provider_name(PROVIDER);
                target.workspace = Some(workspace.to_string_lossy().into());
                target.set_workspace_path_meta(workspace.to_string_lossy());
            }
            target.add_message(Message::user(PRIVATE));
            state.storage.save_session(&target).await.unwrap();
        }
        if !native_source {
            // Preserve the older compatibility fixtures. New native Root
            // outcomes use the current-user domain without Project grants.
            service
                .configure_project_scope(&reference, 0, projects)
                .await
                .unwrap();
            service.attach(&reference, 1, ROOT_A).await.unwrap();
            service.attach(&reference, 2, ROOT_B).await.unwrap();
        } else {
            // Synthetic per-tool permission for this isolated fixture. This is
            // not a Project grant and does not alter the target's permissions.
            use bamboo_tools::permission::{
                PermissionMatcher, PermissionMatcherKind, PermissionType,
            };
            state
                .permission_checker
                .permission_config()
                .unwrap()
                .grant_typed_scoped_session_permission(
                    &reference.session_id,
                    PermissionType::ExecuteCommand,
                    PermissionMatcher {
                        id: "outcome-1053-followup".into(),
                        kind: PermissionMatcherKind::ExactResource,
                        value: format!("session_control followup {ROOT_A}"),
                    },
                )
                .unwrap();
        }
        Self {
            home,
            state,
            probe,
            reference,
        }
    }
    async fn child(&self, id: &str, read_only: bool, model: &str) -> Session {
        let parent = load(&self.state, &self.reference.session_id).await;
        let creator = self.state.supervisor_observer_creator();
        let (workspace, workspace_source) = creator
            .resolve_child_workspace(&parent, None)
            .await
            .unwrap();
        create_child_action(
            &creator,
            CreateChildInput {
                parent_session: parent.clone(),
                child_id: id.into(),
                title: "Subscription reader".into(),
                responsibility: "Report bounded observations".into(),
                assignment_prompt:
                    "Read only fixed views from the supplied observer notices and report.".into(),
                subagent_type: "tracker".into(),
                workspace,
                workspace_source,
                model_override: None,
                model_ref_override: Some(ProviderModelRef::new(PROVIDER, model)),
                runtime_metadata: HashMap::new(),
                read_only,
                auto_run: false,
                reasoning_effort: None,
                lifecycle: None,
                resident_name: None,
                resident_context: None,
                disabled_tools: None,
                context_fork: None,
            },
        )
        .await
        .unwrap();
        self.state
            .storage
            .load_child_wait_session(&parent, id, true)
            .await
            .unwrap()
            .unwrap()
    }
    async fn revision(&self) -> u64 {
        self.state
            .storage
            .inspect_supervisor_scope(&self.reference)
            .await
            .unwrap()
            .state_revision
    }
    async fn register(&self, filter: BTreeSet<ObserverChangeKind>) -> ObserverSubscription {
        self.register_targets(filter, BTreeSet::from([ROOT_A.into(), ROOT_B.into()]))
            .await
    }
    async fn register_targets(
        &self,
        filter: BTreeSet<ObserverChangeKind>,
        targets: BTreeSet<String>,
    ) -> ObserverSubscription {
        self.child(OBSERVER, true, READER).await;
        let receipt = self
            .state
            .session_store
            .register_supervisor_observer(
                &self.reference,
                self.revision().await,
                Uuid::new_v4(),
                OBSERVER,
                targets,
            )
            .await
            .unwrap();
        self.state
            .session_store
            .subscribe_supervisor_observer(
                &self.reference,
                receipt.state_revision,
                OBSERVER,
                filter,
                1000,
            )
            .await
            .unwrap()
    }
    async fn subscription(&self) -> ObserverSubscription {
        load(&self.state, &self.reference.session_id)
            .await
            .supervisor_management
            .unwrap()
            .subscriptions[OBSERVER]
            .clone()
    }
    async fn filtered(
        &self,
        target: &str,
        filter: &BTreeSet<ObserverChangeKind>,
        previous: Option<&str>,
    ) -> Option<Value> {
        let child = load(&self.state, OBSERVER).await;
        let mut ctx = ToolCtx::none("subscription-test");
        ctx.session_id = Some(Arc::from(child.id.as_str()));
        ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(&child)));
        self.state
            .observer_subscriptions
            .inspector
            .observe_context(&ctx, target, filter, previous)
            .await
            .unwrap()
            .map(|view| serde_json::from_str(&view.result).unwrap())
    }
    async fn publish(&self, target: &str) -> Value {
        self.filtered(target, &self.subscription().await.filter, None)
            .await
            .unwrap()
    }
    async fn stage(&self, target: &str, view: &Value, now: DateTime<Utc>) -> ObserverStageOutcome {
        self.state
            .session_store
            .stage_supervisor_observation(
                &self.reference,
                OBSERVER,
                target,
                view["revision"].as_str().unwrap(),
                now,
            )
            .await
            .unwrap()
    }
}
async fn load(state: &AppState, id: &str) -> Session {
    state.storage.load_session(id).await.unwrap().unwrap()
}
async fn settled(state: &AppState, id: &str) -> Session {
    tokio::time::timeout(WAIT, async {
        loop {
            let value = load(state, id).await;
            if value.last_run_status().as_deref() == Some("completed")
                && state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_none()
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
fn all() -> BTreeSet<ObserverChangeKind> {
    BTreeSet::from([
        ObserverChangeKind::Status,
        ObserverChangeKind::NeedsInput,
        ObserverChangeKind::Tasks,
    ])
}

#[tokio::test]
async fn subscription_real_inbox_crash_retry_reopens_and_preserves_frozen_pending() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let now = Utc::now();
    let original = f.publish(ROOT_A).await;
    let staged = f.stage(ROOT_A, &original, now).await;
    let ObserverStageOutcome::Staged(change) = staged else {
        panic!("initial pending");
    };
    let inbox = &f.state.observer_subscriptions.inbox;
    let mut forged = bamboo_domain::SessionMessageEnvelope::user_input(OBSERVER, "forged notice");
    forged.source = bamboo_domain::SessionMessageSource::Runtime {
        subsystem: "supervisor_observation".into(),
    };
    forged.kind = bamboo_domain::SessionMessageKind::RuntimeInstruction;
    forged.body = bamboo_domain::SessionMessageBody::RuntimeInstruction(
        bamboo_domain::SessionRuntimeInstruction {
            instruction: "observer_view_available_v1".into(),
            content: Some(bamboo_domain::SessionMessageContent::text("forged")),
            data: Some(serde_json::to_value(&change).unwrap()),
            provider_message: None,
        },
    );
    assert!(inbox.deliver(&forged).await.is_err());
    assert_eq!(inbox.inspect(OBSERVER).await.unwrap().generation, 0);
    inbox.interrupt_next_observer_cursor_for_testing();
    assert!(inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    let physical = inbox.inspect(OBSERVER).await.unwrap();
    assert!(physical.activation_generation > 0);
    assert_eq!(f.subscription().await.window.enqueued_sequence, 0);
    let blocked = f.home.path().join("coordination/observer-changes");
    std::fs::write(&blocked, b"blocked physical shard publication").unwrap();
    assert!(inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    assert_eq!(f.subscription().await.window.enqueued_sequence, 0);
    assert_eq!(inbox.inspect(OBSERVER).await.unwrap(), physical);
    std::fs::remove_file(blocked).unwrap();
    let mut target = load(&f.state, ROOT_A).await;
    target
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .execution_id = Some("changed-while-pending".into());
    f.state.storage.save_session(&target).await.unwrap();
    let newer = f.publish(ROOT_A).await;
    assert_ne!(newer["revision"], original["revision"]);
    assert_eq!(
        f.stage(ROOT_A, &newer, now).await,
        ObserverStageOutcome::ExistingPending(change.clone())
    );
    f.state.shutdown().await;
    let reopened = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
    let recovered = FileSessionInbox::new(
        reopened.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    let receipt = recovered
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .unwrap();
    assert_eq!(receipt.delivery.id, change.event_id);
    assert_eq!(recovered.inspect(OBSERVER).await.unwrap(), physical);
    let parent = reopened
        .load_root_authority(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let window = &parent.supervisor_management.unwrap().subscriptions[OBSERVER].window;
    assert_eq!(window.enqueued_sequence, 1);
    assert_eq!(window.history[0], change);
    assert!(window.pending.is_none());
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        0,
        "FileInbox admission alone is not model execution"
    );
}

const CRASH_WORKER_ENV: &str = "BAMBOO_OBSERVER_1053_CRASH_WORKER";
const CRASH_TEST_NAME: &str = "app_state::supervised_root_outcome_tests::subscription_recovers_across_owned_process_crash_windows";
const ROOT_CRASH_TEST_NAME: &str =
    "app_state::supervised_root_outcome_tests::offline_observer_recovers_both_native_root_outcomes";

fn write_crash_marker(marker: &std::path::Path, value: &Value) {
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(temporary, marker).unwrap();
}

async fn kill_owned_at_marker(
    command: &mut tokio::process::Command,
    marker: &std::path::Path,
    stdout: &std::path::Path,
    stderr: &std::path::Path,
    boundary: &str,
) -> Value {
    use std::os::unix::process::ExitStatusExt;

    let mut child = command.spawn().unwrap();
    let child_pid = child.id().unwrap();
    let deadline = if boundary == "after-root-final" {
        WAIT * 3
    } else {
        WAIT
    };
    let reached = tokio::time::timeout(deadline, async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Err(format!(
                    "owned crash worker exited before {boundary}: {status}"
                ));
            }
            if let Ok(bytes) = std::fs::read(marker) {
                break serde_json::from_slice::<Value>(&bytes).map_err(|error| error.to_string());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    // Reap the exact owned process even if the marker or deadline failed.
    if child.try_wait().unwrap().is_none() {
        child.start_kill().unwrap();
    }
    let status = tokio::time::timeout(WAIT, child.wait())
        .await
        .unwrap()
        .unwrap();
    let signal = reached
        .map_err(|_| format!("owned worker timed out before {boundary}"))
        .and_then(|result| result)
        .unwrap_or_else(|error| {
            panic!(
                "{error}; stdout={} stderr={}",
                std::fs::read_to_string(stdout).unwrap_or_default(),
                std::fs::read_to_string(stderr).unwrap_or_default(),
            )
        });
    assert_eq!(signal["boundary"], boundary);
    assert_eq!(signal["pid"].as_u64(), Some(u64::from(child_pid)));
    assert_eq!(
        status.signal(),
        Some(9),
        "owned worker must receive SIGKILL"
    );
    signal
}

// Only the parent fixture sets this process-local switch. The actual stage and
// Inbox transaction still go through the normal V2 authority and Maildir.
async fn observer_crash_worker(boundary: &str) {
    let home = std::path::PathBuf::from(std::env::var_os("BAMBOO_1053_HOME").unwrap());
    let reference: SupervisorReference =
        serde_json::from_str(&std::env::var("BAMBOO_1053_SUPERVISOR").unwrap()).unwrap();
    let revision = std::env::var("BAMBOO_1053_VIEW_REVISION").unwrap();
    let marker = std::path::PathBuf::from(std::env::var_os("BAMBOO_1053_MARKER").unwrap());
    let store = Arc::new(SessionStoreV2::new(home).await.unwrap());
    let now = Utc::now();
    let ObserverStageOutcome::Staged(change) = store
        .stage_supervisor_observation(&reference, OBSERVER, ROOT_A, &revision, now)
        .await
        .unwrap()
    else {
        panic!("worker must persist a new observation");
    };

    if boundary == "after-inbox" {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let mut inbox = FileSessionInbox::new(store, bamboo_domain::SessionInboxLimits::default());
        inbox.pause_observer_cursor_for_testing(reached.clone(), resume);
        let mut delivery = tokio::spawn(async move {
            inbox
                .deliver_supervisor_observation(&reference, OBSERVER, now)
                .await
        });
        tokio::select! {
            () = reached.notified() => {},
            result = &mut delivery => panic!("delivery ended before the crash boundary: {result:?}"),
        }
        // The delivery task still holds its authority guards at this point.
    } else {
        assert_eq!(boundary, "after-stage");
    }

    write_crash_marker(
        &marker,
        &json!({
            "boundary": boundary,
            "pid": std::process::id(),
            "change": change,
        }),
    );
    // The parent sends SIGKILL only to this owned fixture after the marker.
    std::future::pending::<()>().await;
}

pub(in crate::app_state) async fn subscription_recovers_across_owned_process_crash_windows() {
    if let Ok(boundary) = std::env::var(CRASH_WORKER_ENV) {
        observer_crash_worker(&boundary).await;
        return;
    }

    for boundary in ["after-stage", "after-inbox"] {
        let f = Fixture::new().await;
        f.register(all()).await;
        let view = f.publish(ROOT_A).await;
        let revision = view["revision"].as_str().unwrap().to_owned();
        let Fixture {
            home,
            state,
            probe,
            reference,
        } = f;
        state.shutdown().await;
        drop(state);

        let marker = home.path().join("observer-crash-marker.json");
        let stdout = home.path().join("observer-crash-stdout.log");
        let stderr = home.path().join("observer-crash-stderr.log");
        std::fs::create_dir(home.path().join("tmp")).unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                CRASH_TEST_NAME,
                "--nocapture",
                "--test-threads=1",
            ])
            .env_clear()
            .env(CRASH_WORKER_ENV, boundary)
            .env("BAMBOO_1053_HOME", home.path())
            .env(
                "BAMBOO_1053_SUPERVISOR",
                serde_json::to_string(&reference).unwrap(),
            )
            .env("BAMBOO_1053_VIEW_REVISION", &revision)
            .env("BAMBOO_1053_MARKER", &marker)
            .env("HOME", home.path())
            .env("TMPDIR", home.path().join("tmp"))
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("RUST_MIN_STACK", "8388608")
            .current_dir(home.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(&stdout).unwrap())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .kill_on_drop(true);
        for name in [
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
            "DYLD_FALLBACK_LIBRARY_PATH",
        ] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        let signal = kill_owned_at_marker(&mut command, &marker, &stdout, &stderr, boundary).await;
        let change: ObserverViewChange = serde_json::from_value(signal["change"].clone()).unwrap();

        tokio::time::timeout(WAIT, async {
            let reopened = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
            let inbox = FileSessionInbox::new(
                reopened.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            );
            let parent = reopened
                .load_root_authority(&reference.session_id)
                .await
                .unwrap()
                .unwrap();
            let before_window =
                &parent.supervisor_management.unwrap().subscriptions[OBSERVER].window;
            assert_eq!(before_window.enqueued_sequence, 0);
            assert_eq!(before_window.pending.as_ref(), Some(&change));
            let before = inbox.inspect(OBSERVER).await.unwrap();
            let expected_pending = usize::from(boundary == "after-inbox");
            assert_eq!(before.pending, expected_pending);
            assert_eq!(before.claimed, 0);
            assert_eq!(before.generation, expected_pending as u64);

            let receipt = inbox
                .deliver_supervisor_observation(&reference, OBSERVER, Utc::now())
                .await
                .unwrap();
            assert_eq!(receipt.delivery.id, change.event_id);
            assert_eq!(receipt.delivery.generation, 1);
            assert!(receipt.pending);
            let after = inbox.inspect(OBSERVER).await.unwrap();
            assert_eq!(after.pending, 1);
            assert_eq!(after.claimed, 0);
            assert_eq!(after.generation, 1);
            if boundary == "after-inbox" {
                assert_eq!(after, before, "retry must not enqueue a duplicate");
            }
            let parent = reopened
                .load_root_authority(&reference.session_id)
                .await
                .unwrap()
                .unwrap();
            let window = &parent.supervisor_management.unwrap().subscriptions[OBSERVER].window;
            assert_eq!(window.enqueued_sequence, 1);
            assert_eq!(window.history, vec![change]);
            assert!(window.pending.is_none());
            assert_eq!(
                probe.calls.load(Ordering::SeqCst),
                0,
                "Inbox admission is not model execution"
            );
        })
        .await
        .expect("recovery must finish within the fixture deadline");
    }
}

async fn run_native_source(
    f: &Fixture,
    text: &str,
    calls: usize,
) -> (String, String, u64, chrono::DateTime<Utc>) {
    use crate::tools::ToolSurface;
    use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolOutcome};
    use bamboo_agent_core::ToolExecutionContext;
    use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort};

    let supervisor = f
        .state
        .storage
        .load_execution_admission_snapshot(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let target = f
        .state
        .storage
        .list_supervisor_current_user_targets(&f.reference, None, None, 20)
        .await
        .unwrap()
        .targets
        .into_iter()
        .find(|target| target.session_id == ROOT_A)
        .expect("the real current-user directory must contain the original Root");
    let target_created_at = target.created_at;
    let operation = format!("outcome-1053-operation-{calls}");
    let call = ToolCall {
        id: format!("outcome-1053-tool-{calls}"),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: json!({"action":"followup", "target_session_id":ROOT_A,
                "target_ref":target, "operation_id":&operation, "message":text})
            .to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&supervisor.id);
    ctx.root_session_id = Some(&supervisor.id);
    ctx = ctx.with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
    );
    let tools = f.state.tools_for(ToolSurface::Root);
    let outcome = bamboo_agent_core::tools::with_executing_session_caller(
        Some(Arc::new(SessionCallerReference::from(&supervisor))),
        tools.execute_with_context_outcome(&call, ctx),
    )
    .await
    .unwrap();
    let ToolOutcome::Completed(result) = outcome else {
        panic!("the real Supervisor followup must return its admission receipt");
    };
    assert!(result.success, "{}", result.result);
    let receipt: Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(receipt["operation_id"], operation);
    assert_eq!(receipt["admitted"], true);
    assert!(
        matches!(
            receipt["activation"].as_str(),
            Some("activation_reserved" | "active_notified" | "activation_coalesced")
        ),
        "admission is not an active execution: {receipt}"
    );
    let input_id = receipt["receipt_id"].as_str().unwrap().to_owned();
    let generation = receipt["generation"]
        .as_u64()
        .filter(|value| *value > 0)
        .expect("the real Inbox receipt must retain its delivery generation");

    let activation = tokio::time::timeout(WAIT, async {
        loop {
            if f.probe.source_calls.load(Ordering::SeqCst) >= calls
                && f.state
                    .session_activation_router
                    .current_run_id(ROOT_A)
                    .await
                    .is_none()
            {
                let entry = f.state.session_store.inspect_actor(ROOT_A).await.unwrap();
                if let Some(activation) = entry.activation {
                    if activation.status == ActorActivationStatus::Succeeded {
                        break activation;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real synthetic-provider Root must commit and release its Actor");
    // Read the source only after this Actor has finished: a read taken before
    // the final save could still show the preceding run's completed snapshot.
    let source = load(&f.state, ROOT_A).await;
    assert_eq!(source.last_run_status().as_deref(), Some("completed"));
    assert_eq!(
        source
            .messages
            .iter()
            .filter(|message| message.id == input_id)
            .count(),
        1,
        "the admitted input must be committed once"
    );
    let input = source
        .messages
        .iter()
        .find(|message| message.id == input_id)
        .unwrap();
    let provenance = &input.metadata.as_ref().unwrap()["session_message"];
    assert_eq!(provenance["source"]["type"], "session");
    assert_eq!(provenance["source"]["session_id"], f.reference.session_id);
    assert_eq!(provenance["target_session_id"], ROOT_A);
    assert_eq!(provenance["correlation_id"], operation);
    let requests = f.probe.source_inputs.lock().unwrap();
    assert_eq!(
        requests.len(),
        calls,
        "this synthetic provider must receive exactly one call per Root activation"
    );
    let request = requests
        .get(calls - 1)
        .expect("real provider call is recorded");
    assert!(
        request
            .iter()
            .any(|message| message.id == input_id && message.content == text),
        "the matching input must reach this actual provider call"
    );
    (input_id, activation.run_id, generation, target_created_at)
}

fn completed_root_fingerprint(actual_run_id: &str) -> String {
    use sha2::{Digest, Sha256};

    // Status v1 already defines this digest shape. The identity must come
    // from the actual Actor activation, not Session metadata or a test nonce.
    let status = json!({"status":"completed", "run_id":actual_run_id});
    let digest = hex::encode(Sha256::digest(
        bamboo_domain::session::inbox::canonical_json_bytes(&status),
    ));
    bamboo_domain::observer_filtered_fingerprint(
        &BTreeSet::from([ObserverChangeKind::Status]),
        &std::collections::BTreeMap::from([(ObserverChangeKind::Status, digest)]),
    )
    .unwrap()
}

pub(in crate::app_state) async fn consecutive_native_root_outcomes_have_distinct_observations() {
    let f = Fixture::new_with_native_source(true).await;
    let filter = BTreeSet::from([ObserverChangeKind::Status]);
    f.register_targets(filter.clone(), BTreeSet::from([ROOT_A.into()]))
        .await;
    let first = run_native_source(&f, "first synthetic operation", 1).await;
    let previous = f.filtered(ROOT_A, &filter, None).await.unwrap();
    let second = run_native_source(&f, "second synthetic operation", 2).await;
    assert_ne!(
        first.0, second.0,
        "real admissions must have distinct identities"
    );
    assert_ne!(
        first.1, second.1,
        "real Actor activations must have distinct run IDs"
    );
    assert_eq!(
        previous["source_fingerprint"],
        completed_root_fingerprint(&first.1),
        "the first completed observation must bind the real first Actor"
    );
    let current = f
        .filtered(ROOT_A, &filter, previous["source_fingerprint"].as_str())
        .await
        .expect("a distinct committed Root run must not disappear just because both completed");
    assert_ne!(
        current["source_fingerprint"],
        previous["source_fingerprint"]
    );
    assert_eq!(
        current["source_fingerprint"],
        completed_root_fingerprint(&second.1),
        "the second completed observation must bind the real second Actor"
    );
    f.state.shutdown().await;
}

#[tokio::test]
async fn historical_pending_requires_supervisor_receipt_before_observer_cursor() {
    let f = Fixture::new_with_native_source(true).await;
    let filter = BTreeSet::from([ObserverChangeKind::Status]);
    f.register_targets(filter.clone(), BTreeSet::from([ROOT_A.into()]))
        .await;
    let source = run_native_source(&f, "guarded historical operation", 1).await;
    // Model the narrow Main-before-ACK crash window with this isolated test
    // receipt; no failure counter or cursor may advance while it is absent.
    let admitted = f
        .home
        .path()
        .join("sessions")
        .join(ROOT_A)
        .join("inbox")
        .join("admitted");
    let ack = std::fs::read_dir(&admitted)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|value| {
                    value.get("id").and_then(Value::as_str) == Some(source.0.as_str())
                })
        })
        .expect("the real input ACK must exist");
    let held = ack.with_extension("test-held");
    std::fs::rename(&ack, &held).unwrap();
    assert!(f.filtered(ROOT_A, &filter, None).await.is_none());
    f.state.observer_subscriptions.tick(Utc::now()).await;
    let waiting = f.subscription().await;
    assert_eq!(waiting.failures, 0);
    assert_eq!(waiting.window.root_outcome_cursors[ROOT_A].sequence, 0);
    std::fs::rename(&held, &ack).unwrap();
    let view = f.filtered(ROOT_A, &filter, None).await.unwrap();
    let position: bamboo_domain::ObserverRootOutcomeCursor =
        serde_json::from_value(view["root_outcome"].clone()).unwrap();
    let now = Utc::now();
    f.state
        .session_store
        .stage_supervisor_observation_with_root_outcome(
            &f.reference,
            OBSERVER,
            ROOT_A,
            view["revision"].as_str().unwrap(),
            Some(&position),
            now,
        )
        .await
        .unwrap();
    let before = f.state.session_inbox.inspect(OBSERVER).await.unwrap();
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    let after = f.state.session_inbox.inspect(OBSERVER).await.unwrap();
    assert_eq!(before.generation, after.generation);
    let pending = f.subscription().await.window;
    assert_eq!(pending.root_outcome_cursors[ROOT_A].sequence, 0);
    assert_eq!(
        pending.pending.unwrap().root_outcome,
        Some(position.clone())
    );

    let observer = load(&f.state, OBSERVER).await;
    let receipts = f
        .state
        .observer_subscriptions
        .inbox
        .deliver_observer_root_outcome_to_supervisor(
            &SessionCallerReference::from(&observer),
            &f.reference,
            ROOT_A,
            position.sequence,
            now,
        )
        .await
        .unwrap()
        .expect("root outcome source is ready");
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].delivery.generation > 0);
    f.state
        .observer_subscriptions
        .inbox
        .interrupt_next_observer_cursor_for_testing();
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    let interrupted = f.subscription().await.window;
    assert_eq!(interrupted.root_outcome_cursors[ROOT_A].sequence, 0);
    assert_eq!(
        interrupted.pending.unwrap().root_outcome,
        Some(position.clone())
    );
    let replay = f
        .state
        .observer_subscriptions
        .inbox
        .deliver_observer_root_outcome_to_supervisor(
            &SessionCallerReference::from(&observer),
            &f.reference,
            ROOT_A,
            position.sequence,
            now,
        )
        .await
        .unwrap()
        .expect("root outcome source remains ready");
    assert_eq!(replay[0].delivery, receipts[0].delivery);
    f.state
        .observer_subscriptions
        .inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .unwrap();
    let window = f.subscription().await.window;
    assert_eq!(window.root_outcome_cursors[ROOT_A], position);
    assert_eq!(window.enqueued_sequence, 1);
    assert_ne!(source.0, "");
    f.state.shutdown().await;
}

pub(in crate::app_state) async fn offline_observer_recovers_both_native_root_outcomes() {
    use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort};

    if std::env::var(CRASH_WORKER_ENV).as_deref() == Ok("after-root-final") {
        let f = Fixture::new_with_native_source(true).await;
        f.register_targets(
            BTreeSet::from([ObserverChangeKind::Status]),
            BTreeSet::from([ROOT_A.into()]),
        )
        .await;
        // No Observer pass occurs between these actual Root activations.
        let first = run_native_source(&f, "first offline operation", 1).await;
        let second = run_native_source(&f, "second offline operation", 2).await;
        assert_ne!(first.0, second.0);
        assert_ne!(first.1, second.1);
        let marker = std::path::PathBuf::from(std::env::var_os("BAMBOO_1053_MARKER").unwrap());
        write_crash_marker(
            &marker,
            &json!({
                "boundary":"after-root-final", "pid":std::process::id(),
                "home":f.home.path(), "reference":&f.reference,
                "first":first, "second":second,
            }),
        );
        // Leave the whole owned Host alive so its parent can kill it abruptly.
        std::future::pending::<()>().await;
        drop(f);
        return;
    }

    let outer = tempfile::tempdir().unwrap();
    let temporary = outer.path().join("tmp");
    std::fs::create_dir(&temporary).unwrap();
    let marker = outer.path().join("root-final-marker.json");
    let stdout = outer.path().join("root-final-stdout.log");
    let stderr = outer.path().join("root-final-stderr.log");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            ROOT_CRASH_TEST_NAME,
            "--nocapture",
            "--test-threads=1",
        ])
        .env_clear()
        .env(CRASH_WORKER_ENV, "after-root-final")
        .env("BAMBOO_1053_MARKER", &marker)
        .env("HOME", outer.path())
        .env("TMPDIR", &temporary)
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("RUST_MIN_STACK", "8388608")
        .current_dir(outer.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .kill_on_drop(true);
    for name in [
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    let signal =
        kill_owned_at_marker(&mut command, &marker, &stdout, &stderr, "after-root-final").await;
    let home = std::fs::canonicalize(signal["home"].as_str().unwrap()).unwrap();
    assert!(home.starts_with(std::fs::canonicalize(&temporary).unwrap()));
    let reference: SupervisorReference =
        serde_json::from_value(signal["reference"].clone()).unwrap();
    let first: (String, String, u64, chrono::DateTime<Utc>) =
        serde_json::from_value(signal["first"].clone()).unwrap();
    let second: (String, String, u64, chrono::DateTime<Utc>) =
        serde_json::from_value(signal["second"].clone()).unwrap();
    let probe = Arc::new(Probe::default());
    let state = host(&home, probe.clone()).await;
    let source = load(&state, ROOT_A).await;
    assert_eq!(first.3, second.3, "both inputs belong to one Root birth");
    assert_eq!(
        source.created_at, first.3,
        "restart must retain that Root birth"
    );
    assert_eq!(source.last_run_status().as_deref(), Some("completed"));
    for id in [&first.0, &second.0] {
        assert_eq!(
            source
                .messages
                .iter()
                .filter(|message| &message.id == id)
                .count(),
            1
        );
    }
    let activation = state
        .session_store
        .inspect_actor(ROOT_A)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_eq!(activation.status, ActorActivationStatus::Succeeded);
    assert_eq!(activation.run_id, second.1);
    // This fixture disables the background wake tick. These explicit passes
    // exercise recovery after a real Host crash, but do not prove automatic
    // startup scheduling or power-loss durability.
    let now = Utc::now();
    state.observer_subscriptions.tick(now).await;
    settled(&state, OBSERVER).await;
    let parent = load(&state, &reference.session_id).await;
    assert_eq!(
        parent.supervisor_management.unwrap().subscriptions[OBSERVER]
            .window
            .enqueued_sequence,
        1
    );
    state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(2))
        .await;
    let parent = load(&state, &reference.session_id).await;
    let window = &parent.supervisor_management.unwrap().subscriptions[OBSERVER].window;
    assert_eq!(
        window.enqueued_sequence, 2,
        "both committed Root outcomes must survive the offline interval"
    );
    assert_eq!(window.history.len(), 2);
    assert_ne!(window.history[0].event_id, window.history[1].event_id);
    assert_ne!(
        window.history[0].source_fingerprint,
        window.history[1].source_fingerprint
    );
    assert_eq!(
        window.history[0].source_fingerprint,
        completed_root_fingerprint(&first.1),
        "the older admitted outcome must be observed first"
    );
    assert_eq!(
        window.history[1].source_fingerprint,
        completed_root_fingerprint(&second.1),
        "the successor outcome must remain distinct"
    );
    let history = window.history.clone();
    tokio::time::timeout(WAIT, async {
        loop {
            let observed = probe.observed_status.lock().unwrap().len();
            if observed >= 2
                && state
                    .session_activation_router
                    .current_run_id(OBSERVER)
                    .await
                    .is_none()
                && load(&state, OBSERVER).await.last_run_status().as_deref() == Some("completed")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both distinct Observer reads must settle");
    let observed = probe.observed_status.lock().unwrap();
    assert_eq!(
        observed.len(),
        2,
        "both immutable status views must be read"
    );
    for (index, (input, run)) in [(&first.0, &first.1), (&second.0, &second.1)]
        .into_iter()
        .enumerate()
    {
        assert_eq!(observed[index].0.event_id, history[index].event_id);
        assert!(
            observed[index].1.contains(input.as_str()),
            "view must identify its admitted receipt"
        );
        assert!(
            observed[index].1.contains(run.as_str()),
            "view must identify its actual Root activation"
        );
        assert!(
            observed[index]
                .1
                .contains(&format!("outcome-1053-operation-{}", index + 1)),
            "view must preserve the Supervisor operation association"
        );
    }
    drop(observed);
    // Reading a view in the Observer Child is not evidence that the original
    // Supervisor received the outcome. This Observer-path fixture proposes a
    // structured runtime notification carrying each actual run/input; a
    // free-form synthetic model summary cannot satisfy it. A direct path that
    // does not depend on an Observer model also needs its own acceptance test.
    let expected = [
        (
            &first.0,
            &first.1,
            first.2,
            &first.3,
            "outcome-1053-operation-1",
        ),
        (
            &second.0,
            &second.1,
            second.2,
            &second.3,
            "outcome-1053-operation-2",
        ),
    ];
    let parent_outcomes = tokio::time::timeout(WAIT, async {
        loop {
            let parent = load(&state, &reference.session_id).await;
            let mut events = Vec::new();
            for message in &parent.messages {
                let Some(marker) = message
                    .metadata
                    .as_ref()
                    .and_then(|value| value.get("session_message"))
                else {
                    continue;
                };
                let Ok(envelope) =
                    serde_json::from_value::<bamboo_domain::SessionMessageEnvelope>(marker.clone())
                else {
                    continue;
                };
                if !matches!(
                    &envelope.source,
                    bamboo_domain::SessionMessageSource::Runtime { .. }
                ) || envelope.target_session_id != reference.session_id
                {
                    continue;
                }
                let mut translated = message.clone();
                let mut checkpoint_generation = None;
                let mut checkpoint_intent = None;
                let mut checkpoint_policy = None;
                if let Some(fields) = translated.metadata.as_mut().and_then(Value::as_object_mut) {
                    if let Some(checkpoint) = fields.get("_bamboo_owned_input_checkpoint") {
                        let Some(checkpoint) = checkpoint.as_object() else {
                            continue;
                        };
                        let keys = [
                            "version",
                            "envelope_id",
                            "target_session_id",
                            "session_created_at",
                            "generation",
                            "activation_intent",
                            "activation_policy",
                        ];
                        if checkpoint.len() != keys.len()
                            || keys.iter().any(|key| !checkpoint.contains_key(*key))
                            || checkpoint["version"] != 1
                            || checkpoint["envelope_id"] != envelope.id.as_str()
                            || checkpoint["target_session_id"] != reference.session_id
                            || checkpoint["session_created_at"] != json!(parent.created_at)
                            || checkpoint["generation"]
                                .as_u64()
                                .is_none_or(|value| value == 0)
                        {
                            continue;
                        }
                        let Ok(intent) = serde_json::from_value::<
                            Option<bamboo_domain::SessionInboxActivationIntent>,
                        >(checkpoint["activation_intent"].clone()) else {
                            continue;
                        };
                        let Ok(policy) = serde_json::from_value::<
                            bamboo_domain::SessionActivationPolicy,
                        >(checkpoint["activation_policy"].clone()) else {
                            continue;
                        };
                        if let Some(requested) = intent.as_ref() {
                            let Ok(requested_policy) = requested.policy() else {
                                continue;
                            };
                            if (requested.delivery().is_some()
                                && policy != bamboo_domain::SessionActivationPolicy::RespectSpecificWait)
                                || (requested_policy == bamboo_domain::SessionActivationPolicy::InterruptSpecificWait
                                    && policy != bamboo_domain::SessionActivationPolicy::InterruptSpecificWait)
                            {
                                continue;
                            }
                        }
                        checkpoint_generation = checkpoint["generation"].as_u64();
                        checkpoint_intent = Some(intent);
                        checkpoint_policy = Some(policy);
                        fields.remove("_bamboo_owned_input_checkpoint");
                    }
                }
                if !bamboo_domain::is_matching_session_message(&translated, &envelope) {
                    continue;
                }
                let receipt = state
                    .observer_subscriptions
                    .inbox
                    .admitted_receipt_for_testing(&envelope)
                    .await
                    .expect("Supervisor Inbox receipt and semantics must be readable");
                if receipt.is_some_and(|(receipt, intent, policy)| {
                    receipt.id == envelope.id
                        && receipt.generation > 0
                        && checkpoint_generation
                            .is_none_or(|generation| generation == receipt.generation)
                        && checkpoint_intent
                            .as_ref()
                            .is_none_or(|expected| expected == &intent)
                        && checkpoint_policy.is_none_or(|expected| policy == Some(expected))
                })
                {
                    events.push(envelope);
                }
            }
            let matches = |receipt: &str,
                           run: &str,
                           generation: u64,
                           birth: &chrono::DateTime<Utc>,
                           operation: &str| {
                events
                    .iter()
                    .filter(|envelope| {
                        let bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) =
                            &envelope.body
                        else {
                            return false;
                        };
                        let Some(data) = instruction.data.as_ref() else {
                            return false;
                        };
                        data["target_session_id"] == ROOT_A
                            && data["target_created_at"] == json!(birth)
                            && data["run_id"] == run
                            && data["status"] == "completed"
                            && data["inputs"].as_array().is_some_and(|inputs| {
                                inputs.iter().any(|input| {
                                    input["receipt_id"] == receipt
                                        && input["generation"] == generation
                                        && input["operation_id"] == operation
                                })
                            })
                    })
                    .count()
            };
            let counts: Vec<_> = expected
                .iter()
                .map(|(receipt, run, generation, birth, operation)| {
                    matches(receipt, run, *generation, birth, operation)
                })
                .collect();
            if counts.iter().all(|count| *count > 0) {
                break counts;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("each committed outcome must reach the canonical Supervisor Inbox");
    assert_eq!(
        parent_outcomes,
        vec![1, 1],
        "the checked transcript snapshot must not contain duplicate outcomes"
    );
    assert_eq!(
        probe.source_calls.load(Ordering::SeqCst),
        0,
        "recovery must not rerun the source Root"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn subscription_failed_publication_is_visible_bounded_and_never_enqueues() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let now = Utc::now();
    assert!(f
        .state
        .session_store
        .stage_supervisor_observation(&f.reference, OBSERVER, ROOT_A, &"e".repeat(64), now)
        .await
        .is_err());
    assert!(f.subscription().await.window.pending.is_none());
    // Replace the real publication parent with a regular file: the production
    // publisher must fail instead of persisting a fictitious selector.
    let view = f.publish(ROOT_A).await;
    let path = std::path::PathBuf::from(view["manifest_path"].as_str().unwrap());
    let directory = path.parent().unwrap().parent().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    std::fs::write(directory, b"blocked").unwrap();
    for attempt in 0..8 {
        f.state
            .observer_subscriptions
            .tick(now + chrono::Duration::minutes(attempt * 6))
            .await;
    }
    let subscription = f.subscription().await;
    assert_eq!(subscription.failures, 8);
    assert_eq!(
        subscription.last_error,
        Some(Failure::PublicationUnavailable)
    );
    assert!(!subscription.ready(now + chrono::Duration::days(1)));
    assert!(subscription.window.pending.is_none());
    let backlog = f.state.session_inbox.inspect(OBSERVER).await.unwrap();
    assert_eq!(backlog.activation_generation, 0);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
}

#[tokio::test]
async fn subscription_rotates_derived_shards_has_gap_and_revocation_is_final() {
    let f = Fixture::new().await;
    let subscription = f
        .register(BTreeSet::from([ObserverChangeKind::Status]))
        .await;
    let epoch = subscription.window.scope_epoch;
    let now = Utc::now();
    for n in 0..18 {
        let mut target = load(&f.state, ROOT_A).await;
        target
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .execution_id = Some(format!("run-{n}"));
        f.state.storage.save_session(&target).await.unwrap();
        let view = f.publish(ROOT_A).await;
        let due = now + chrono::Duration::seconds(n * 2);
        f.stage(ROOT_A, &view, due).await;
        f.state
            .observer_subscriptions
            .inbox
            .deliver_supervisor_observation(&f.reference, OBSERVER, due)
            .await
            .unwrap();
    }
    let read = f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 0)
        .await
        .unwrap();
    assert!(read.page.gap);
    assert_eq!(read.page.changes.len(), 16);
    assert_eq!(read.jsonl_shards.len(), 4);
    assert_eq!(
        read.snapshot_targets,
        BTreeSet::from([ROOT_A.into(), ROOT_B.into()])
            .into_iter()
            .collect::<Vec<String>>()
    );
    let decoded: Vec<ObserverViewChange> = read
        .jsonl_shards
        .iter()
        .flat_map(|shard| {
            shard
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
        })
        .collect();
    assert_eq!(decoded, read.page.changes);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(read.shard_directory.join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["revision"], read.shard_revision);
    assert_eq!(manifest["files"].as_array().unwrap().len(), 4);
    for (index, shard) in read.jsonl_shards.iter().enumerate() {
        assert_eq!(
            std::fs::read_to_string(read.shard_directory.join(format!("{index}.jsonl"))).unwrap(),
            *shard
        );
    }
    std::fs::write(read.shard_directory.join("0.jsonl"), b"corrupt").unwrap();
    assert!(f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 0)
        .await
        .is_err());
    std::fs::remove_dir_all(&read.shard_directory).unwrap();
    let rebuilt = f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 0)
        .await
        .unwrap();
    assert_eq!(rebuilt.shard_revision, read.shard_revision);
    // A corrupt FIFO must fail before blocking open can retain the complete
    // canonical authority indefinitely. Release a regressed blocking reader
    // before asserting, so a RED never leaves the test runtime wedged.
    for name in ["0.jsonl", "manifest.json"] {
        use std::os::unix::ffi::OsStrExt;
        let file = rebuilt.shard_directory.join(name);
        let original = std::fs::read(&file).unwrap();
        std::fs::remove_file(&file).unwrap();
        let path = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let before = f.subscription().await.window.enqueued_sequence;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            f.state.session_store.read_supervisor_observer_changes(
                &f.reference,
                OBSERVER,
                epoch,
                0,
            ),
        )
        .await;
        let writer = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_WRONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if writer >= 0 {
            unsafe { libc::close(writer) };
        }
        std::fs::remove_file(&file).unwrap();
        std::fs::write(&file, original).unwrap();
        tokio::time::timeout(WAIT, f.state.storage.inspect_supervisor_scope(&f.reference))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.subscription().await.window.enqueued_sequence, before);
        assert!(result.is_ok(), "FIFO {name} blocked canonical authority");
        assert!(
            result.unwrap().is_err(),
            "FIFO {name} must not become a valid shard"
        );
    }
    #[cfg(unix)]
    {
        let file = rebuilt.shard_directory.join("0.jsonl");
        let outside = f.home.path().join("unrelated-file");
        std::fs::write(&outside, &read.jsonl_shards[0]).unwrap();
        std::fs::remove_file(&file).unwrap();
        std::os::unix::fs::symlink(&outside, &file).unwrap();
        assert!(f
            .state
            .session_store
            .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 0)
            .await
            .is_err());
        assert_eq!(
            std::fs::read_to_string(outside).unwrap(),
            read.jsonl_shards[0]
        );
        std::fs::remove_file(&file).unwrap();
        std::fs::write(&file, &read.jsonl_shards[0]).unwrap();
    }
    assert!(f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch + 1, 0)
        .await
        .is_err());
    let current = f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 18)
        .await
        .unwrap();
    assert!(!current.page.gap);
    assert!(current.jsonl_shards.is_empty());
    let target = f.publish(ROOT_B).await;
    let due = now + chrono::Duration::hours(1);
    f.stage(ROOT_B, &target, due).await;
    let before = f.state.session_inbox.inspect(OBSERVER).await.unwrap();
    f.state
        .session_store
        .revoke_supervisor_observer(&f.reference, f.revision().await, OBSERVER)
        .await
        .unwrap();
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, due)
        .await
        .is_err());
    assert_eq!(
        f.state.session_inbox.inspect(OBSERVER).await.unwrap(),
        before
    );
    assert!(f
        .state
        .session_store
        .read_supervisor_observer_changes(&f.reference, OBSERVER, epoch, 0)
        .await
        .is_err());
    f.state.shutdown().await;
}

#[tokio::test]
async fn subscription_native_two_projects_changes_and_restart_do_not_clear_original_work_wait() {
    let f = Fixture::new().await;
    *f.probe.diagnostic_phase.lock().unwrap() = "create-existing-work";
    let work = f.child("subscription-held-work", false, WORK).await;
    let parent = load(&f.state, &f.reference.session_id).await;
    let creator = f.state.supervisor_observer_creator();
    creator
        .register_parent_wait_for_child(&parent.id, &work.id, Some("ordinary-work-wait"))
        .await
        .unwrap();
    creator.enqueue_child_run(&parent, &work).await.unwrap();
    tokio::time::timeout(WAIT, f.probe.work_entered.notified())
        .await
        .unwrap();
    let wait = load(&f.state, &parent.id)
        .await
        .agent_runtime_state
        .unwrap()
        .waiting_for_children;
    *f.probe.diagnostic_phase.lock().unwrap() = "create-observer";
    f.child(OBSERVER, true, READER).await;
    *f.probe.diagnostic_phase.lock().unwrap() = "subscribe-initial-notice";
    f.state
        .subscribe_supervisor_observer(ObserverSubscriptionStart {
            supervisor: f.reference.clone(),
            expected_state_revision: f.revision().await,
            operation_id: Uuid::new_v4(),
            child_id: OBSERVER.into(),
            targets: BTreeSet::from([ROOT_A.into(), ROOT_B.into()]),
            filter: all(),
            min_interval_ms: 1000,
        })
        .await
        .unwrap();
    settled(&f.state, OBSERVER).await;
    *f.probe.diagnostic_phase.lock().unwrap() = "second-project-notice";
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    let now = Utc::now() + chrono::Duration::seconds(2);
    f.state.observer_subscriptions.tick(now).await;
    settled(&f.state, OBSERVER).await;
    tokio::time::timeout(WAIT, async {
        while f.probe.observed.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.subscription().await.window.enqueued_sequence, 2);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        f.probe
            .observed
            .lock()
            .unwrap()
            .iter()
            .map(|change| change.target_session_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([ROOT_A, ROOT_B])
    );
    // A partial Observer report may wake the parent to reason while its
    // original Work wait stays armed. This is not completion of that Work.
    let reporting_parent = tokio::time::timeout(WAIT, async {
        loop {
            let parent = load(&f.state, &parent.id).await;
            if parent.last_run_status().as_deref() == Some("suspended")
                && f.state
                    .session_activation_router
                    .current_run_id(&parent.id)
                    .await
                    .is_none()
                && ["OBSERVATION 1", "OBSERVATION 2"].iter().all(|marker| {
                    parent
                        .messages
                        .iter()
                        .any(|message| message.content.contains(marker))
                })
            {
                break parent;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both real reports must settle without satisfying the Work wait");
    assert_eq!(
        reporting_parent
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .waiting_for_children,
        wait
    );
    for marker in ["OBSERVATION 1", "OBSERVATION 2"] {
        assert_eq!(
            reporting_parent
                .messages
                .iter()
                .filter(|message| message.content.contains(marker))
                .count(),
            1
        );
    }
    let parent_calls = f.probe.parent_calls.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&parent_calls),
        "only the two report notifications may activate the parent"
    );
    {
        let inputs = f.probe.parent_inputs.lock().unwrap();
        assert_eq!(inputs.len(), parent_calls);
        let canonical: Vec<_> = reporting_parent
            .messages
            .iter()
            .filter(|message| message.content.contains("OBSERVATION "))
            .map(|message| {
                json!({"id":message.id,"metadata":message.metadata,
                "compressed":serde_json::to_value(message).unwrap().get("compressed").cloned()})
            })
            .collect();
        assert!(inputs.iter().all(|messages| messages
            .iter()
            .any(|message| message.content.contains("OBSERVATION "))),
            "actual provider input lacks its committed reports; requests={:?}; canonical={canonical:?}",
            f.probe.parent_request_diagnostics.lock().unwrap());
        assert!(["OBSERVATION 1", "OBSERVATION 2"]
            .iter()
            .all(|marker| inputs
                .last()
                .unwrap()
                .iter()
                .any(|message| message.content.contains(marker))));
    }
    assert!(f
        .state
        .session_activation_router
        .current_run_id(&work.id)
        .await
        .is_some());
    assert!(!load(&f.state, &work.id)
        .await
        .metadata
        .contains_key("runtime.child_completion_source_v1"));
    let calls = f.probe.calls.load(Ordering::SeqCst);
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(2))
        .await;
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), calls);
    assert_eq!(f.subscription().await.window.enqueued_sequence, 2);
    let mut a = load(&f.state, ROOT_A).await;
    a.updated_at += chrono::Duration::seconds(1);
    a.agent_runtime_state
        .get_or_insert_with(Default::default)
        .round
        .total_prompt_tokens += 12;
    f.state.storage.save_session(&a).await.unwrap();
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(4))
        .await;
    assert_eq!(
        f.subscription().await.window.enqueued_sequence,
        2,
        "noise must not publish"
    );
    assert_eq!(
        load(&f.state, &parent.id)
            .await
            .agent_runtime_state
            .unwrap()
            .waiting_for_children,
        wait
    );
    assert_eq!(
        f.probe.parent_calls.load(Ordering::SeqCst),
        parent_calls,
        "unchanged/noise passes must not wake the parent"
    );
    assert!(f
        .state
        .session_activation_router
        .current_run_id(&work.id)
        .await
        .is_some());
    f.probe.work_release.notify_one();
    settled(&f.state, &work.id).await;
    f.state.shutdown().await;
    let state = host(f.home.path(), f.probe.clone()).await;
    let calls = f.probe.calls.load(Ordering::SeqCst);
    state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(6))
        .await;
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        calls,
        "reopen must not repeat unchanged notices"
    );
    let mut b = load(&state, ROOT_B).await;
    b.agent_runtime_state
        .get_or_insert_with(Default::default)
        .execution_id = Some("next-source-run".into());
    state.storage.save_session(&b).await.unwrap();
    state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(8))
        .await;
    tokio::time::timeout(WAIT, async {
        while f.probe.observed.lock().unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), calls + 2);
    state.shutdown().await;
}

#[tokio::test]
async fn subscription_owned_and_active_backpressure_coalesces_until_real_consumption() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let now = Utc::now();
    let first = f.publish(ROOT_A).await;
    f.stage(ROOT_A, &first, now).await;
    let inbox = &f.state.observer_subscriptions.inbox;
    inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .unwrap();
    let request = bamboo_domain::SessionInboxLeaseRequest {
        consumer: Default::default(),
        now,
        duration: chrono::Duration::minutes(10),
    };
    let claims = inbox
        .claim_owned(OBSERVER, 1, None, &request)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let mut source = load(&f.state, ROOT_A).await;
    source
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .execution_id = Some("coalesced-run".into());
    f.state.storage.save_session(&source).await.unwrap();
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(2))
        .await;
    assert_eq!(f.subscription().await.window.enqueued_sequence, 1);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    assert_eq!(inbox.inspect(OBSERVER).await.unwrap().claimed, 1);
    // Release the actual owned lease; the existing native consumer performs the
    // transcript checkpoint+ACK. A producer receipt never substitutes for it.
    inbox.release_owned(OBSERVER, &claims[0]).await.unwrap();
    f.probe.hold_reader.store(true, Ordering::SeqCst);
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(3))
        .await;
    tokio::time::timeout(WAIT, f.probe.reader_entered.notified())
        .await
        .unwrap();
    let sequence = f.subscription().await.window.enqueued_sequence;
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(4))
        .await;
    assert_eq!(
        f.subscription().await.window.enqueued_sequence,
        sequence,
        "active native run must coalesce sources"
    );
    f.probe.reader_release.notify_one();
    settled(&f.state, OBSERVER).await;
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::seconds(5))
        .await;
    tokio::time::timeout(WAIT, async {
        while f.probe.observed.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        f.subscription().await.window.enqueued_sequence,
        sequence + 1
    );
    f.state.shutdown().await;
}

#[tokio::test]
async fn subscription_status_filter_ignores_question_and_task_noise_but_tracks_run_identity() {
    let f = Fixture::new().await;
    f.register(BTreeSet::from([ObserverChangeKind::Status]))
        .await;
    let initial = f.publish(ROOT_A).await;
    let mut invalid = load(&f.state, &f.reference.session_id)
        .await
        .supervisor_management
        .unwrap();
    invalid
        .subscriptions
        .get_mut(OBSERVER)
        .unwrap()
        .window
        .enqueued_fingerprints
        .insert("unassigned-root".into(), "a".repeat(64));
    assert!(invalid.validate(f.reference.incarnation_id).is_err());
    let mut old_writer = load(&f.state, &f.reference.session_id)
        .await
        .supervisor_management
        .unwrap();
    old_writer.schema_version = 2;
    assert!(old_writer.validate(f.reference.incarnation_id).is_err());
    let mut source = load(&f.state, ROOT_A).await;
    source.pending_question = Some(bamboo_domain::PendingQuestion {
        tool_call_id: "question-1".into(),
        tool_name: "AskUser".into(),
        question: "private owner question".into(),
        options: Vec::new(),
        allow_custom: true,
        source: Default::default(),
    });
    source.task_list = Some(bamboo_domain::session::task::TaskList {
        session_id: source.id.clone(),
        title: "source tasks".into(),
        items: vec![bamboo_domain::session::task::TaskItem {
            id: "task-1".into(),
            description: "allowed task description".into(),
            notes: "PRIVATE-TASK-NOTES".into(),
            ..Default::default()
        }],
        created_at: source.created_at,
        updated_at: source.updated_at,
    });
    source.updated_at += chrono::Duration::seconds(1);
    source
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .round
        .total_prompt_tokens += 9;
    f.state.storage.save_session(&source).await.unwrap();
    let filter = BTreeSet::from([ObserverChangeKind::Status]);
    assert!(f
        .filtered(ROOT_A, &filter, initial["source_fingerprint"].as_str())
        .await
        .is_none());
    let needs = BTreeSet::from([ObserverChangeKind::NeedsInput]);
    let question = f.filtered(ROOT_A, &needs, None).await.unwrap();
    source.pending_question.as_mut().unwrap().tool_call_id = "question-2".into();
    f.state.storage.save_session(&source).await.unwrap();
    assert!(
        f.filtered(ROOT_A, &needs, question["source_fingerprint"].as_str())
            .await
            .is_some(),
        "question ID change while still pending must be meaningful"
    );
    let tasks = BTreeSet::from([ObserverChangeKind::Tasks]);
    let task = f.filtered(ROOT_A, &tasks, None).await.unwrap();
    source.task_list.as_mut().unwrap().items[0].notes = "NEW-PRIVATE-NOTES".into();
    f.state.storage.save_session(&source).await.unwrap();
    assert!(f
        .filtered(ROOT_A, &tasks, task["source_fingerprint"].as_str())
        .await
        .is_none());
    source.task_list.as_mut().unwrap().items[0].description =
        "changed allowed task description".into();
    f.state.storage.save_session(&source).await.unwrap();
    assert!(f
        .filtered(ROOT_A, &tasks, task["source_fingerprint"].as_str())
        .await
        .is_some());
    source.agent_runtime_state.as_mut().unwrap().execution_id = Some("new-run-same-status".into());
    f.state.storage.save_session(&source).await.unwrap();
    assert!(f
        .filtered(ROOT_A, &filter, initial["source_fingerprint"].as_str())
        .await
        .is_some());
    let disabled = ObserverSubscriptionCoordinator::new(
        f.state.session_store.clone(),
        f.state.observer_subscriptions.inbox.clone(),
        f.state.session_activation_router.clone(),
        f.state.storage.clone(),
        false,
    );
    disabled.tick(Utc::now()).await;
    assert_eq!(f.subscription().await.window.enqueued_sequence, 0);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
}

#[tokio::test]
async fn subscription_unavailable_observer_retries_are_bounded_without_borrowing_authority() {
    for scenario in ["deleted", "replaced", "missing-runtime", "missing-sidecar"] {
        let f = Fixture::new().await;
        f.register(all()).await;
        let original = load(&f.state, OBSERVER).await;
        let child_directory = f
            .home
            .path()
            .join("sessions")
            .join(&f.reference.session_id)
            .join("children")
            .join(OBSERVER);
        assert!(child_directory.join("runtime.json").is_file());
        let caller = match scenario {
            "deleted" => {
                assert!(f.state.storage.delete_session(OBSERVER).await.unwrap());
                original.clone()
            }
            "replaced" => {
                assert!(f.state.storage.delete_session(OBSERVER).await.unwrap());
                let replacement = f.child(OBSERVER, true, READER).await;
                assert_ne!(replacement.created_at, original.created_at);
                replacement
            }
            "missing-sidecar" => {
                std::fs::rename(
                    child_directory.join("runtime.json"),
                    f.home.path().join("unavailable-observer-runtime.json"),
                )
                .unwrap();
                original.clone()
            }
            _ => {
                let mut missing = original.clone();
                missing.agent_runtime_state = None;
                f.state.storage.save_session(&missing).await.unwrap();
                let missing = load(&f.state, OBSERVER).await;
                assert!(missing.agent_runtime_state.is_none());
                missing
            }
        };
        let physical_child = (
            std::fs::read(child_directory.join("session.json")).ok(),
            std::fs::read(child_directory.join("runtime.json")).ok(),
        );
        assert!(
            f.state
                .storage
                .load_supervisor_observer_context_target(
                    &bamboo_domain::SessionCallerReference::from(&caller),
                    ROOT_A,
                )
                .await
                .is_err(),
            "{scenario} must not acquire an observation grant"
        );
        let now = Utc::now();
        for attempt in 0..8 {
            f.state
                .observer_subscriptions
                .tick(now + chrono::Duration::minutes(attempt * 6))
                .await;
            assert_eq!(
                f.subscription().await.failures,
                attempt as u8 + 1,
                "{scenario}"
            );
        }
        let exhausted = f.subscription().await;
        assert_eq!(exhausted.failures, 8, "{scenario}");
        assert!(exhausted.retry_at.is_none());
        assert!(exhausted.last_error.is_some());
        assert_eq!(exhausted.window.enqueued_sequence, 0);
        assert!(exhausted.window.pending.is_none());
        assert!(!exhausted.ready(now + chrono::Duration::days(1)));
        f.state
            .observer_subscriptions
            .tick(now + chrono::Duration::days(1))
            .await;
        let after = f.subscription().await;
        assert_eq!(
            (after.failures, after.retry_at, after.last_error),
            (exhausted.failures, exhausted.retry_at, exhausted.last_error)
        );
        assert_eq!(after.window, exhausted.window);
        assert!(
            f.state
                .observer_subscriptions
                .inbox
                .deliver_supervisor_observation(
                    &f.reference,
                    OBSERVER,
                    now + chrono::Duration::days(1),
                )
                .await
                .is_err(),
            "administrative counters cannot authorize {scenario} delivery"
        );
        assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            f.state
                .session_inbox
                .inspect(&f.reference.session_id)
                .await
                .unwrap()
                .generation,
            0
        );
        assert_eq!(
            (
                std::fs::read(child_directory.join("session.json")).ok(),
                std::fs::read(child_directory.join("runtime.json")).ok(),
            ),
            physical_child,
            "{scenario} Child bytes must remain unchanged"
        );
        if scenario == "deleted" {
            assert!(!child_directory.exists());
        }
        let mut old_reference = f.reference.clone();
        old_reference.incarnation_id = Uuid::new_v4();
        let before = serde_json::to_value(load(&f.state, &f.reference.session_id).await).unwrap();
        assert!(f
            .state
            .session_store
            .record_observer_subscription_failure(
                &old_reference,
                OBSERVER,
                now,
                bamboo_domain::ObserverSubscriptionFailure::AuthorityUnavailable,
            )
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(load(&f.state, &f.reference.session_id).await).unwrap(),
            before
        );
        f.state
            .session_store
            .revoke_supervisor_observer(&f.reference, f.revision().await, OBSERVER)
            .await
            .unwrap();
        let revoked = serde_json::to_value(load(&f.state, &f.reference.session_id).await).unwrap();
        assert!(f
            .state
            .session_store
            .record_observer_subscription_failure(
                &f.reference,
                OBSERVER,
                now,
                bamboo_domain::ObserverSubscriptionFailure::AuthorityUnavailable,
            )
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(load(&f.state, &f.reference.session_id).await).unwrap(),
            revoked
        );
        f.state.shutdown().await;
        let reopened = SessionStoreV2::new(f.home.path().into()).await.unwrap();
        let parent = reopened
            .load_root_authority(&f.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let durable = &parent.supervisor_management.as_ref().unwrap().subscriptions[OBSERVER];
        assert_eq!(
            (durable.failures, durable.retry_at, durable.last_error),
            (exhausted.failures, exhausted.retry_at, exhausted.last_error)
        );
        assert_eq!(durable.window, exhausted.window);
    }
}

#[path = "observer_fault_tests.rs"]
mod fault;
