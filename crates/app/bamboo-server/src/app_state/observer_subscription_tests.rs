//! Real V2, immutable publication, Maildir and native Host activation. The
//! ordinary Work wait below is deliberately not a stand-in for Planner #1059.
#![cfg(any(target_os = "linux", target_os = "macos"))]
use super::*;
use actix_web::web;
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
use bamboo_tools::permission::PermissionDecisionKind;
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
const SUBSCRIBED_FIRST_INPUT: &str = "Produce a short first-input handoff.";
const SUBSCRIBED_PERMISSION_INPUT: &str = "Produce a handoff after the gated command.";
const SUBSCRIBED_DENY_INPUT: &str = "Produce a handoff if the gated command is declined.";
const SUBSCRIBED_REPEAT_INPUT: &str = "Produce a handoff after two individually gated commands.";
const HANDOFF_SECOND_PERMISSION_CALL: &str = "handoff-second-permission-command";
const HANDOFF_PERMISSION_CALL: &str = "handoff-permission-command";
const HANDOFF_COMMAND_OUTPUT: &str = "handoff-approval-executed";
const HANDOFF_HEAD: &str = "The independent task verified the requested source.";
const HANDOFF_EVIDENCE: &str = "src/fixture.rs:42";
const HANDOFF_RISK: &str = "The external integration was not exercised.";
const HANDOFF_TAIL: &str = "DETAILS_STAY_IN_THE_EXECUTION_SESSION";
const WAIT: Duration = Duration::from_secs(40);

#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    source_calls: AtomicUsize,
    source_inputs: StdMutex<Vec<Vec<Message>>>,
    parent_calls: AtomicUsize,
    parent_inputs: StdMutex<Vec<Vec<Message>>>,
    parent_dispatch: StdMutex<Option<Vec<ToolCall>>>,
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
    hold_sources: AtomicBool,
    source_entered: [Notify; 2],
    source_release: [Notify; 2],
    task_written: Notify,
    task_rewritten: Notify,
    finish_blocker: Notify,
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
        let subscribed_input = messages.iter().rev().find(|message| {
            message.role == bamboo_domain::Role::User
                && matches!(
                    message.content.as_str(),
                    SUBSCRIBED_FIRST_INPUT
                        | SUBSCRIBED_PERMISSION_INPUT
                        | SUBSCRIBED_DENY_INPUT
                        | SUBSCRIBED_REPEAT_INPUT
                )
        });
        if let Some(input) = subscribed_input.map(|message| message.content.as_str()) {
            assert!(
                messages.iter().any(|message| {
                    message.content.contains("Managed Session Delivery Summary")
                        && message.content.contains("<!-- bamboo-handoff:v1 -->")
                }),
                "the exact subscribed Worker Run must receive the delivery contract"
            );
            if self.source_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.source_entered[0].notify_one();
                self.source_release[0].notified().await;
            }
            if input != SUBSCRIBED_FIRST_INPUT {
                let calls = [HANDOFF_PERMISSION_CALL, HANDOFF_SECOND_PERMISSION_CALL];
                let call_count = if input == SUBSCRIBED_REPEAT_INPUT {
                    2
                } else {
                    1
                };
                for &id in &calls[..call_count] {
                    if let Some(result) = messages
                        .iter()
                        .find(|message| message.tool_call_id.as_deref() == Some(id))
                    {
                        assert_eq!(
                            result.content.contains(HANDOFF_COMMAND_OUTPUT),
                            input != SUBSCRIBED_DENY_INPUT,
                            "unexpected gated command result: {}",
                            result.content
                        );
                    } else {
                        return Ok(Box::pin(futures::stream::iter([
                            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                                id: id.into(),
                                tool_type: "function".into(),
                                function: FunctionCall {
                                    name: "exec_command".into(),
                                    arguments: json!({
                                        "cmd": format!("pwd -P && printf '%s\n' {HANDOFF_COMMAND_OUTPUT}"),
                                    }).to_string(),
                                },
                            }])),
                            Ok(LLMChunk::Done),
                        ])));
                    }
                }
            }
            let answer = format!(
                "{}\n<!-- bamboo-handoff:v1 -->\n- **Task** Verify fixture\n- **Conclusion** {HANDOFF_HEAD}\n- **Evidence** `{HANDOFF_EVIDENCE}`\n- **Risk** {HANDOFF_RISK}\n<!-- /bamboo-handoff:v1 -->\n{HANDOFF_TAIL}",
                "界".repeat(500),
            );
            return Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::Token(answer)),
                Ok(LLMChunk::Done),
            ])));
        }
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
                if messages
                    .iter()
                    .any(|message| message.content.contains("managed task blocker"))
                {
                    let task = |id: &str, tasks: Value| {
                        LLMChunk::ToolCalls(vec![ToolCall {
                            id: id.into(),
                            tool_type: "function".into(),
                            function: FunctionCall {
                                name: "Task".into(),
                                arguments: json!({"tasks":tasks}).to_string(),
                            },
                        }])
                    };
                    let blocker = json!({
                        "id":"dependency", "content":"Waiting for dependency", "status":"blocked"
                    });
                    let result = |id| {
                        messages
                            .iter()
                            .find(|message| message.tool_call_id.as_deref() == Some(id))
                    };
                    let chunk = if let Some(result) = result("update-other-task") {
                        assert_ne!(result.tool_success, Some(false), "{}", result.content);
                        self.task_rewritten.notify_one();
                        self.finish_blocker.notified().await;
                        LLMChunk::Token("Root finished after the dependency cleared.".into())
                    } else if let Some(result) = result("report-task-blocker") {
                        assert_ne!(result.tool_success, Some(false), "{}", result.content);
                        self.task_written.notify_one();
                        self.source_release[0].notified().await;
                        task(
                            "update-other-task",
                            json!([blocker, {
                                "id":"other", "content":"Checked independent input", "status":"completed"
                            }]),
                        )
                    } else {
                        self.source_entered[0].notify_one();
                        self.source_release[0].notified().await;
                        assert!(tools.iter().any(|tool| tool.function.name == "Task"));
                        task("report-task-blocker", json!([blocker]))
                    };
                    return Ok(Box::pin(futures::stream::iter([
                        Ok(chunk),
                        Ok(LLMChunk::Done),
                    ])));
                }
                if self.hold_sources.load(Ordering::SeqCst) {
                    let slot = usize::from(
                        messages
                            .iter()
                            .any(|message| message.content.contains("managed wait second")),
                    );
                    self.source_entered[slot].notify_one();
                    self.source_release[slot].notified().await;
                }
                LLMChunk::Token("Synthetic original Root finished.".into())
            }
            PARENT => {
                self.parent_calls.fetch_add(1, Ordering::SeqCst);
                self.parent_inputs.lock().unwrap().push(messages.to_vec());
                match self.parent_dispatch.lock().unwrap().take() {
                    Some(calls) => LLMChunk::ToolCalls(calls),
                    None => LLMChunk::Token("Parent processed its current canonical input.".into()),
                }
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

async fn host(home: &std::path::Path, probe: Arc<Probe>) -> web::Data<AppState> {
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
    web::Data::new(state)
}

struct Fixture {
    home: tempfile::TempDir,
    state: web::Data<AppState>,
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

async fn invoke_managed_control(
    state: &AppState,
    reference: &SupervisorReference,
    id: &str,
    args: Value,
) -> Value {
    use crate::tools::ToolSurface;
    use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolOutcome};
    use bamboo_agent_core::ToolExecutionContext;

    let supervisor = state
        .storage
        .load_execution_admission_snapshot(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let call = ToolCall {
        id: id.to_owned(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: args.to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&supervisor.id);
    ctx.root_session_id = Some(&supervisor.id);
    ctx = ctx.with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
    );
    let tools = state.tools_for(ToolSurface::Root);
    let outcome = bamboo_agent_core::tools::with_executing_session_caller(
        Some(Arc::new(SessionCallerReference::from(&supervisor))),
        tools.execute_with_context_outcome(&call, ctx),
    )
    .await
    .unwrap();
    let ToolOutcome::Completed(result) = outcome else {
        panic!("the real Supervisor control must return a synchronous observation");
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).unwrap()
}

async fn managed_target(
    state: &AppState,
    reference: &SupervisorReference,
    id: &str,
) -> bamboo_domain::SupervisorCurrentUserTarget {
    state
        .storage
        .inspect_supervisor_current_user_target(reference, id)
        .await
        .unwrap()
}

async fn admit_native_source(
    f: &Fixture,
    target: &bamboo_domain::SupervisorCurrentUserTarget,
    operation: &str,
    text: &str,
) -> Value {
    let receipt = invoke_managed_control(
        &f.state,
        &f.reference,
        operation,
        json!({"action":"followup", "target_session_id":target.session_id,
            "target_ref":target, "operation_id":operation, "message":text}),
    )
    .await;
    assert_eq!(receipt["operation_id"], operation);
    assert_eq!(receipt["admitted"], true);
    assert!(
        matches!(
            receipt["activation"].as_str(),
            Some("activation_reserved" | "active_notified" | "activation_coalesced")
        ),
        "admission is not an active execution: {receipt}"
    );
    receipt
}

pub(in crate::app_state) async fn exercise_managed_wait_outcomes() {
    use bamboo_config::{
        LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig,
        DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    };
    use bamboo_domain::{Role, SessionInboxReceipt, SessionMessageId, SupervisorCurrentUserTarget};
    use bamboo_engine::model_config_helper::GOLD_CONFIG_METADATA_KEY;
    use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
    use sha2::{Digest, Sha256};

    fn answer_after(session: &Session, receipt: &str) -> Value {
        use bamboo_domain::{MessagePhase, Role};
        let input = session
            .messages
            .iter()
            .position(|m| m.id == receipt)
            .unwrap();
        let mut answers = session.messages[input + 1..]
            .iter()
            .take_while(|message| message.role != Role::User)
            .filter(|message| message.phase == Some(MessagePhase::FinalAnswer));
        let answer = answers.next().expect("canonical reply after this input");
        assert!(answers.next().is_none());
        assert_eq!(answer.content, "Synthetic original Root finished.");
        json!({
            "message_id": answer.id,
            "content_sha256": hex::encode(Sha256::digest(answer.content.as_bytes())),
        })
    }

    let f = Fixture::new_with_native_source(true).await;
    f.state.config.write().await.lifecycle_hooks = LifecycleHooksConfig {
        enabled: true,
        stop: vec![LifecycleHookGroup {
            hooks: vec![LifecycleHookHandler::command(
                r#"printf '%s' '{"additional_context":"Retained Stop hook context."}'"#,
                DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
            )],
            ..Default::default()
        }],
        ..Default::default()
    };
    let workspace = f.home.path().join("second-source-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let mut second = load(&f.state, ROOT_B).await;
    second.model = SOURCE.into();
    second.model_ref = Some(ProviderModelRef::new(PROVIDER, SOURCE));
    second.set_provider_name(PROVIDER);
    second.workspace = Some(workspace.to_string_lossy().into());
    second.set_workspace_path_meta(workspace.to_string_lossy());
    // With an active Goal, the terminal answer is committed before Stop runs.
    // A zero continuation budget keeps this fixture on its single provider turn.
    second.metadata.insert(
        GOLD_CONFIG_METADATA_KEY.into(),
        json!({
            "enabled": true,
            "auto_continue_enabled": true,
            "goal": "Complete the managed assignment",
            "max_auto_continuations": 0,
        })
        .to_string(),
    );
    f.state.storage.save_session(&second).await.unwrap();
    f.state
        .permission_checker
        .permission_config()
        .unwrap()
        .grant_typed_scoped_session_permission(
            &f.reference.session_id,
            PermissionType::ExecuteCommand,
            PermissionMatcher {
                id: "managed-wait-second-followup".into(),
                kind: PermissionMatcherKind::ExactResource,
                value: format!("session_control followup {ROOT_B}"),
            },
        )
        .unwrap();

    let first = managed_target(&f.state, &f.reference, ROOT_A).await;
    let second = managed_target(&f.state, &f.reference, ROOT_B).await;
    f.probe.hold_sources.store(true, Ordering::SeqCst);
    let (first_input, second_input) = tokio::join!(
        admit_native_source(&f, &first, "managed-wait-first", "managed wait first"),
        admit_native_source(&f, &second, "managed-wait-second", "managed wait second"),
    );
    tokio::time::timeout(WAIT, async {
        tokio::join!(
            f.probe.source_entered[0].notified(),
            f.probe.source_entered[1].notified()
        );
    })
    .await
    .expect("both real Root activations reached the provider");

    let router = &f.state.session_activation_router;
    let first_run = router.current_run_id(ROOT_A).await.unwrap();
    let second_run = router.current_run_id(ROOT_B).await.unwrap();
    let admission_request = |target: &SupervisorCurrentUserTarget, delivery: &Value| {
        json!({
            "action":"observe_run",
            "target_session_id":target.session_id,
            "target_ref":target,
            "input_receipt":{"receipt_id":delivery["receipt_id"],"generation":delivery["generation"]},
        })
    };
    for (target, input, run) in [
        (&first, &first_input, &first_run),
        (&second, &second_input, &second_run),
    ] {
        let id = format!("managed-admission-{}", target.session_id);
        let admission = invoke_managed_control(
            &f.state,
            &f.reference,
            &id,
            admission_request(target, input),
        )
        .await;
        assert_eq!(
            admission,
            json!({
                "status":"admission_verified",
                "admission":{
                    "target_session_id":target.session_id,
                    "target_created_at":target.created_at,
                    "run_id":run,
                    "input":{
                        "receipt_id":input["receipt_id"],"generation":input["generation"],
                        "operation_id":input["operation_id"]
                    },
                }
            })
        );
    }
    let unconfirmed = json!({"status":"admission_unconfirmed"});
    let mut wrong_generation = admission_request(&first, &first_input);
    wrong_generation["input_receipt"]["generation"] =
        json!(first_input["generation"].as_u64().unwrap() + 1);
    for (operation, request) in [
        ("managed-admission-wrong-generation", wrong_generation),
        (
            "managed-admission-cross-target",
            admission_request(&second, &first_input),
        ),
    ] {
        assert_eq!(
            invoke_managed_control(&f.state, &f.reference, operation, request).await,
            unconfirmed
        );
    }
    let request = |first_cursor, second_cursor| {
        json!({
            "action":"wait_outcomes",
            "outcome_targets":[
                {"target_ref":first, "expected_run_id":first_run, "after_sequence":first_cursor},
                {"target_ref":second, "expected_run_id":second_run, "after_sequence":second_cursor},
            ],
        })
    };
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        invoke_managed_control(
            &f.state,
            &f.reference,
            "managed-wait-abandoned",
            request(0, 0)
        ),
    )
    .await
    .is_err());
    assert_eq!(
        router.current_run_id(ROOT_A).await.as_deref(),
        Some(first_run.as_str())
    );
    assert_eq!(
        router.current_run_id(ROOT_B).await.as_deref(),
        Some(second_run.as_str())
    );
    let result = {
        let waiter =
            invoke_managed_control(&f.state, &f.reference, "managed-wait-live", request(0, 0));
        tokio::pin!(waiter);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiter)
                .await
                .is_err()
        );
        f.probe.source_release[1].notify_one();
        tokio::time::timeout(WAIT, waiter)
            .await
            .expect("second outcome wakes the wait")
    };
    assert_eq!(result["status"], "recorded_outcome");
    assert_eq!(result["outcome"]["target_session_id"], ROOT_B);
    assert_eq!(result["outcome"]["run_id"], second_run);
    assert_eq!(result["outcome"]["status"]["kind"], "completed");
    assert_eq!(
        result["outcome"]["inputs"][0]["receipt_id"],
        second_input["receipt_id"]
    );
    let second_main = load(&f.state, ROOT_B).await;
    let hook_context = second_main.messages.last().unwrap();
    assert_eq!(hook_context.role, Role::System);
    assert!(hook_context.content.contains("Retained Stop hook context."));
    let second_answer = answer_after(&second_main, second_input["receipt_id"].as_str().unwrap());
    assert_eq!(result["outcome"]["final_message"], second_answer);
    assert!(!result
        .to_string()
        .contains("Synthetic original Root finished."));
    assert_eq!(
        router.current_run_id(ROOT_A).await.as_deref(),
        Some(first_run.as_str())
    );
    let second_cursor = result["outcome"]["sequence"].as_u64().unwrap();

    f.probe.hold_sources.store(false, Ordering::SeqCst);
    f.probe.source_release[0].notify_one();
    tokio::time::timeout(WAIT, async {
        while router.current_run_id(ROOT_A).await.is_some()
            || router.current_run_id(ROOT_B).await.is_some()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both native runs release ownership before reopening");
    let first_answer = answer_after(
        &load(&f.state, ROOT_A).await,
        first_input["receipt_id"].as_str().unwrap(),
    );
    assert_ne!(first_answer["message_id"], second_answer["message_id"]);
    let successor = run_native_source(&f, "unselected successor", 3).await;
    assert_ne!(successor.1, first_run);

    let Fixture {
        home,
        state,
        reference,
        ..
    } = f;
    drop(state);
    let cold = host(home.path(), Arc::new(Probe::default())).await;
    let next_delivery = json!({"receipt_id":successor.0,"generation":successor.2});
    for (operation, input, run) in [
        ("managed-admission-original", &first_input, &first_run),
        ("managed-admission-successor", &next_delivery, &successor.1),
    ] {
        let recovered = invoke_managed_control(
            &cold,
            &reference,
            operation,
            admission_request(&first, input),
        )
        .await;
        assert_eq!(recovered["status"], "admission_verified");
        assert_eq!(recovered["admission"]["run_id"], *run);
        assert_eq!(
            recovered["admission"]["input"]["receipt_id"],
            input["receipt_id"]
        );
    }
    let supervisor = load(&cold, &reference.session_id).await;
    let caller = SessionCallerReference::from(&supervisor);
    let mut stale_target = first.clone();
    stale_target.created_at -= chrono::Duration::seconds(1);
    let exact_receipt = SessionInboxReceipt {
        id: SessionMessageId::parse(first_input["receipt_id"].as_str().unwrap()).unwrap(),
        generation: first_input["generation"].as_u64().unwrap(),
    };
    let retained_inbox = FileSessionInbox::new(
        cold.session_store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    assert!(retained_inbox
        .read_managed_root_admission(&caller, &reference, &stale_target, &exact_receipt)
        .await
        .is_err());
    let recovered = invoke_managed_control(
        &cold,
        &reference,
        "managed-wait-reopen",
        request(0, second_cursor),
    )
    .await;
    assert_eq!(recovered["outcome"]["target_session_id"], ROOT_A);
    assert_eq!(recovered["outcome"]["run_id"], first_run);
    assert_eq!(recovered["outcome"]["final_message"], first_answer);
    let first_cursor = recovered["outcome"]["sequence"].as_u64().unwrap();
    let mut next_request = request(first_cursor, second_cursor);
    next_request["outcome_targets"][0]["expected_run_id"] = json!(successor.1);
    let next =
        invoke_managed_control(&cold, &reference, "managed-wait-successor", next_request).await;
    assert_eq!(next["outcome"]["run_id"], successor.1);
    let next_answer = answer_after(&load(&cold, ROOT_A).await, &successor.0);
    assert_ne!(next_answer["message_id"], first_answer["message_id"]);
    assert_eq!(next["outcome"]["final_message"], next_answer);
    let consumed = invoke_managed_control(
        &cold,
        &reference,
        "managed-wait-consumed",
        request(first_cursor, second_cursor),
    )
    .await;
    assert_eq!(consumed, json!({"status":"outcome_unknown"}));
    let mut stale_first = request(0, 0);
    stale_first["outcome_targets"][0]["target_ref"]["created_at"] = json!("2000-01-01T00:00:00Z");
    let sibling =
        invoke_managed_control(&cold, &reference, "managed-wait-stale-sibling", stale_first).await;
    assert_eq!(sibling["outcome"]["run_id"], second_run);
    assert_eq!(sibling["outcome"]["final_message"], second_answer);

    // Temporarily withhold only the genuine ACK in this isolated fixture.
    // The Main result already exists and the cold Host has no runner signal.
    let inbox = home
        .path()
        .join("sessions")
        .join(ROOT_B)
        .join("inbox")
        .join("admitted");
    let receipts: Vec<_> = std::fs::read_dir(inbox)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(receipts.len(), 1);
    let parked = home.path().join("parked-ack");
    std::fs::rename(&receipts[0], &parked).unwrap();
    assert_eq!(
        invoke_managed_control(
            &cold,
            &reference,
            "managed-admission-pending-ack",
            admission_request(&second, &second_input),
        )
        .await,
        unconfirmed
    );
    let pending = invoke_managed_control(
        &cold,
        &reference,
        "managed-wait-pending-ack",
        request(first_cursor, 0),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    std::fs::rename(&parked, &receipts[0]).unwrap();
    let restored_admission = invoke_managed_control(
        &cold,
        &reference,
        "managed-admission-restored-ack",
        admission_request(&second, &second_input),
    )
    .await;
    assert_eq!(restored_admission["status"], "admission_verified");
    assert_eq!(restored_admission["admission"]["run_id"], second_run);
    let verified = tokio::time::timeout(WAIT, pending)
        .await
        .expect("restored ACK wakes the wait");
    assert_eq!(verified["outcome"]["run_id"], second_run);
    assert_eq!(verified["outcome"]["final_message"], second_answer);
    assert_eq!(
        verified["outcome"]["inputs"][0]["receipt_id"],
        second_input["receipt_id"]
    );
}

async fn run_native_source(
    f: &Fixture,
    text: &str,
    calls: usize,
) -> (String, String, u64, chrono::DateTime<Utc>) {
    use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort};

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
    let receipt = admit_native_source(f, &target, &operation, text).await;
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

async fn drive_material_notices(f: &Fixture, expected_calls: usize) {
    let mut last_error = None;
    let finished = tokio::time::timeout(WAIT, async {
        loop {
            if let Err(error) = f.state.observer_subscriptions.material_tick().await {
                assert!(
                    error.contains("pending") || error.contains("busy"),
                    "{error}"
                );
                last_error = Some(error);
            }
            if f.probe.parent_calls.load(Ordering::SeqCst) >= expected_calls
                && f.state
                    .session_activation_router
                    .current_run_id(&f.reference.session_id)
                    .await
                    .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if finished.is_err() {
        let hints =
            ManagedRootNotificationIo::sources(f.state.observer_subscriptions.as_ref()).await;
        let cache = f.state.observer_subscriptions.material_cache.lock().await;
        let cached: Vec<_> = cache
            .iter()
            .map(|(id, cached)| {
                (
                    id,
                    &cached.hint,
                    cached.candidates.len(),
                    cached.delivered.len(),
                    cached.settled,
                )
            })
            .collect();
        panic!("checked notice expected {expected_calls} Supervisor calls, observed {}, last error {last_error:?}, hints {hints:?}, cache {cached:?}", f.probe.parent_calls.load(Ordering::SeqCst));
    }
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), expected_calls);
}

async fn settle_material_notices(f: &Fixture) {
    tokio::time::timeout(WAIT, async {
        loop {
            match f.state.observer_subscriptions.material_tick().await {
                Ok(()) => break,
                Err(error) => assert!(
                    error.contains("pending") || error.contains("busy"),
                    "{error}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("unchanged sources must finish their finite proof I/O");
}

pub(in crate::app_state) async fn exercise_subscribed_create_handoff(
    decision: Option<PermissionDecisionKind>,
    repeat: bool,
) {
    use actix_web::{http::StatusCode, test, App};
    use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};

    let f = Fixture::new_with_native_source(true).await;
    let workspace = f.home.path().join("subscribed-dispatch");
    std::fs::create_dir(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let workspace = bamboo_config::paths::path_to_display_string(&workspace);
    let project = f
        .state
        .project_store
        .create_with_project_path("Subscribed dispatch", None, &workspace, Vec::new())
        .unwrap();
    let permissions = f.state.permission_checker.permission_config().unwrap();
    permissions.set_default_session_permission_mode(SessionPermissionMode::Auto);
    if decision.is_some() {
        permissions.set_ask_rules(["exec_command".into()]);
    }
    permissions
        .grant_typed_scoped_session_permission(
            &f.reference.session_id,
            PermissionType::ExecuteCommand,
            PermissionMatcher {
                id: "subscribed-dispatch-create".into(),
                kind: PermissionMatcherKind::ExactResource,
                value: format!("session_control create {} {workspace}", project.id),
            },
        )
        .unwrap();
    let args = json!({
        "action":"create",
        "request":{
            "operation_id":uuid::Uuid::new_v4(),
            "expected_state_revision":0,
            "project_id":project.id,
            "workspace_path":workspace,
            "title":"Subscribed dispatch",
            "message":if repeat { SUBSCRIBED_REPEAT_INPUT } else { match decision {
                Some(PermissionDecisionKind::AllowOnce) => SUBSCRIBED_PERMISSION_INPUT,
                Some(PermissionDecisionKind::DenyOnce) => SUBSCRIBED_DENY_INPUT,
                None => SUBSCRIBED_FIRST_INPUT,
                _ => unreachable!("fixture uses the supported test decisions"),
            }},
            "notify_on_material_event":true,
        }
    });
    *f.probe.parent_dispatch.lock().unwrap() = Some(
        ["dispatch", "dispatch-retry"]
            .into_iter()
            .map(|id| ToolCall {
                id: id.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "session_control".into(),
                    arguments: args.to_string(),
                },
            })
            .collect(),
    );
    f.state
        .session_messenger
        .send(bamboo_domain::SessionMessageEnvelope::user_input(
            &f.reference.session_id,
            "Dispatch a subscribed Session and report its eventual result.",
        ))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, f.probe.source_entered[0].notified())
        .await
        .expect("the created Root's first input must reach its provider");
    drive_material_notices(&f, 2).await;
    let parent = load(&f.state, &f.reference.session_id).await;
    let result = |id| {
        let message = parent
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(id))
            .expect("native Supervisor creation result");
        assert_eq!(message.tool_success, Some(true), "{}", message.content);
        serde_json::from_str::<Value>(&message.content).unwrap()
    };
    let created = result("dispatch");
    let retried = result("dispatch-retry");
    for key in ["target_session_id", "receipt_id", "generation"] {
        assert_eq!(created[key], retried[key]);
    }
    assert_eq!(created["notify_on_material_event"], true);
    assert_eq!(retried["replayed"], true);
    let target = created["target_session_id"].as_str().unwrap();
    let reference = managed_target(&f.state, &f.reference, target).await;
    assert_eq!(created["current_user_target"], json!(reference));
    settle_material_notices(&f).await;
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.probe.source_calls.load(Ordering::SeqCst), 1);

    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let session_uri = format!("/api/v1/sessions/{target}");
    let worker = load(&f.state, target).await;
    assert_eq!(
        worker
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .effective_permission_mode(),
        SessionPermissionMode::Bypass
    );
    let read_cards = || async {
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/supervisor/default/approvals")
                .peer_addr("127.0.0.1:8080".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        test::read_body_json::<Value, _>(response).await
    };
    let decisions_uri = format!("{session_uri}/permission-decisions");
    let submit = |body: &Value| {
        test::TestRequest::post()
            .uri(&decisions_uri)
            .peer_addr("127.0.0.1:8080".parse().unwrap())
            .set_json(body)
            .to_request()
    };

    f.probe.source_release[0].notify_one();
    drive_material_notices(&f, 3).await;
    settle_material_notices(&f).await;
    let calls = [HANDOFF_PERMISSION_CALL, HANDOFF_SECOND_PERMISSION_CALL];
    let calls = &calls[..if repeat { 2 } else { 1 }];
    let expected_calls = if let Some(decision) = decision {
        let mut previous = None;
        for (index, &expected_call) in calls.iter().enumerate() {
            let suspended = tokio::time::timeout(WAIT, async {
                loop {
                    let current = load(&f.state, target).await;
                    if let Some(occurrence) = bamboo_domain::pending_permission_occurrence(&current)
                        .filter(|occurrence| previous.as_ref() != Some(occurrence))
                    {
                        if f.state
                            .session_activation_router
                            .current_run_id(target)
                            .await
                            .is_none()
                        {
                            break (current, occurrence);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("next typed permission occurrence must park");
            let (root, occurrence) = suspended;
            assert_eq!(occurrence.tool_call_id, expected_call);
            let facts = &root
                .runtime_metadata
                .as_ref()
                .unwrap()
                .supervised_root_outcomes
                .as_ref()
                .unwrap()
                .facts;
            let parked = facts.last().unwrap();
            assert_eq!(
                parked.result,
                Some(bamboo_domain::SupervisedRootOutcomeResult::Suspended)
            );
            assert_eq!(parked.questions.len(), 1);
            assert_eq!(parked.resumed_from, (index > 0).then_some(index as u64));
            let observed = invoke_managed_control(
                &f.state,
                &f.reference,
                "observe-approval",
                json!({"action":"observe_run", "target_session_id":target, "target_ref":reference}),
            )
            .await;
            assert_eq!(observed["status"], "awaiting_response");
            assert_eq!(observed["tool_name"], "exec_command");
            assert_eq!(observed["approval_card"], true);
            if index == 0 {
                let parent = load(&f.state, &f.reference.session_id).await;
                assert!(parent.messages.iter().any(|message| {
                    message
                        .content
                        .starts_with("# Managed Session approval required")
                        && message.content.contains(observed["link"].as_str().unwrap())
                        && bamboo_engine::session_app::execute::is_hidden_from_ui(message)
                }));
            }
            let cards = read_cards().await;
            let [card] = cards["approvals"].as_array().unwrap().as_slice() else {
                panic!("exactly one managed approval card");
            };
            assert_eq!(card["session_id"], target);
            assert_eq!(card["created_at"], json!(root.created_at));
            assert_eq!(card["title"], "Subscribed dispatch");
            assert_eq!(card["workspace_path"], workspace);
            let pending = &card["pending"];
            let request = &pending["permission_request"];
            assert_eq!(request["request_id"], occurrence.tool_call_id);
            assert_eq!(
                request["request_generation"],
                json!(occurrence.permission_generation)
            );
            assert_eq!(request["reason_code"], "configured_always_ask");
            let command = pending["tool_arguments"]["cmd"].as_str().unwrap();
            assert!(command.contains(HANDOFF_COMMAND_OUTPUT));
            assert!(request["operation_summary"]
                .as_str()
                .unwrap()
                .contains(command));
            if index == 0 {
                let independent = managed_target(&f.state, &f.reference, ROOT_A).await;
                admit_native_source(
                    &f,
                    &independent,
                    "during-approval",
                    "Check an independent task.",
                )
                .await;
                settled(&f.state, ROOT_A).await;
                assert_eq!(
                    bamboo_domain::pending_permission_occurrence(&load(&f.state, target).await)
                        .as_ref(),
                    Some(&occurrence)
                );
            }
            let submission = json!({
                "request_id": request["request_id"],
                "request_generation": request["request_generation"],
                "expected_policy_revision": request["policy_revision"],
                "decision": decision,
            });
            let older_generation = previous
                .as_ref()
                .and_then(|occurrence| occurrence.permission_generation.as_deref())
                .unwrap_or("older-generation");
            for (field, invalid) in [
                ("request_generation", json!(older_generation)),
                (
                    "expected_policy_revision",
                    json!(request["policy_revision"].as_u64().unwrap() + 1),
                ),
            ] {
                let mut stale = submission.clone();
                stale[field] = invalid;
                let response = test::call_service(&app, submit(&stale)).await;
                assert_eq!(response.status(), StatusCode::CONFLICT, "{field}");
            }
            let response = test::call_service(&app, submit(&submission)).await;
            assert_eq!(response.status(), StatusCode::OK);
            let receipt: Value = test::read_body_json(response).await;
            assert_eq!(receipt["auto_resume_status"], "started");
            assert_eq!(receipt["receipt"]["decision"]["decision"], json!(decision));
            previous = Some(occurrence);
        }
        drive_material_notices(&f, 4).await;
        settle_material_notices(&f).await;
        assert_eq!(read_cards().await["approvals"], json!([]));
        4
    } else {
        3
    };
    let worker = load(&f.state, target).await;
    if decision == Some(PermissionDecisionKind::AllowOnce) {
        for &id in calls {
            let result = worker
                .messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some(id))
                .expect("approved command result");
            let result: Value =
                serde_json::from_str(&result.content).expect("structured exec result");
            assert_eq!(result["exit_code"], 0);
            let output = result["output"].as_str().unwrap();
            assert_eq!(output.lines().next(), Some(workspace.as_str()));
        }
    }
    assert_eq!(
        worker
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .effective_permission_mode(),
        SessionPermissionMode::Bypass,
    );
    assert_eq!(
        f.probe.source_calls.load(Ordering::SeqCst),
        1 + 2 * usize::from(decision.is_some()) + usize::from(repeat),
    );
    let facts = worker
        .runtime_metadata
        .as_ref()
        .unwrap()
        .supervised_root_outcomes
        .as_ref()
        .unwrap();
    let fact = facts.facts.last().unwrap();
    if decision.is_some() {
        assert_eq!(facts.facts.len(), if repeat { 3 } else { 2 });
        for pair in facts.facts.windows(2) {
            let [suspended, resumed] = pair else {
                unreachable!()
            };
            assert_ne!(suspended.run_id, resumed.run_id);
            assert_eq!(resumed.resumed_from, Some(suspended.sequence));
            assert!(resumed.inputs.is_empty());
            assert_eq!(suspended.questions.len(), 1);
        }
    } else {
        assert_eq!(facts.facts.len(), 1);
        assert_eq!(fact.inputs.len(), 1);
    }
    let reference = fact.verified_final_message(&worker).unwrap();
    assert!(worker.messages.iter().any(|message| {
        message.id == reference.message_id && message.content.ends_with(HANDOFF_TAIL)
    }));
    let parent = load(&f.state, &f.reference.session_id).await;
    let notices: Vec<_> = parent
        .messages
        .iter()
        .filter(|message| {
            message.content.starts_with("# Managed Session handoff")
                && message.content.contains("- Recorded outcome: completed\n")
        })
        .collect();
    let [notice] = notices.as_slice() else {
        panic!("exactly one durable handoff");
    };
    assert!(notice.content.contains(HANDOFF_HEAD), "{}", notice.content);
    assert!(notice.content.contains("dispatch a bounded reproduction"));
    assert!(notice.content.contains(HANDOFF_EVIDENCE));
    assert!(notice.content.contains(HANDOFF_RISK));
    assert!(!notice.content.contains('界'));
    assert!(bamboo_engine::session_app::execute::is_hidden_from_ui(
        notice
    ));
    assert!(notice.content.contains(&reference.message_id));
    assert!(notice.content.contains(&fact.run_id));
    assert!(notice.content.len() < 1_600);
    assert!(!notice.content.contains(HANDOFF_TAIL));
    assert!(f.probe.parent_inputs.lock().unwrap()[expected_calls - 1]
        .iter()
        .any(|message| message.id == notice.id && message.content == notice.content));
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), expected_calls);
    if decision.is_some() {
        let selected = bamboo_orchestration::managed_session::notification_candidates(&worker)
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.run_id == fact.run_id)
            .unwrap();
        let mut reprojected: bamboo_domain::SessionMessageEnvelope =
            serde_json::from_value(notice.metadata.as_ref().unwrap()["session_message"].clone())
                .unwrap();
        let bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) =
            &mut reprojected.body
        else {
            panic!("resumed handoff must be a typed runtime instruction");
        };
        assert_eq!(
            instruction.data.as_ref().unwrap()["final_message"],
            json!(reference)
        );
        assert!(!instruction
            .content
            .as_ref()
            .unwrap()
            .text
            .contains(HANDOFF_HEAD));
        let presentation = instruction.provider_message.as_mut().unwrap();
        assert!(presentation.content.text.contains(HANDOFF_HEAD));
        presentation.content = bamboo_domain::SessionMessageContent::text(
            "Committed result text unavailable after a history change",
        );
        let (permanent, _, _) = f
            .state
            .observer_subscriptions
            .inbox
            .admitted_receipt_for_testing(&reprojected)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permanent.id.as_str(), notice.id);
        let replayed = f
            .state
            .observer_subscriptions
            .inbox
            .deliver_managed_root_notification(&selected, Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed.delivery, permanent);
        assert!(!replayed.pending);
        assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), expected_calls);
    }
    if decision.is_none() {
        assert_eq!(
            fact.inputs[0]
                .supervisor_origin
                .as_ref()
                .and_then(|origin| origin.handoff_format()),
            Some(bamboo_domain::ManagedRootHandoffFormat::PresentationSummary),
        );
        let selected = bamboo_orchestration::managed_session::notification_candidates(&worker)
            .unwrap()
            .pop()
            .unwrap();
        let marker = &notice.metadata.as_ref().unwrap()["session_message"];
        let mut reprojected: bamboo_domain::SessionMessageEnvelope =
            serde_json::from_value(marker.clone()).unwrap();
        let bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) =
            &mut reprojected.body
        else {
            panic!("subscribed result has a typed runtime instruction");
        };
        assert_eq!(
            instruction.data.as_ref().unwrap()["final_message"],
            json!(reference)
        );
        assert!(!instruction
            .content
            .as_ref()
            .unwrap()
            .text
            .contains(HANDOFF_HEAD));
        assert!(instruction
            .provider_message
            .take()
            .unwrap()
            .content
            .text
            .contains(HANDOFF_HEAD));
        let (receipt, _, _) = f
            .state
            .observer_subscriptions
            .inbox
            .admitted_receipt_for_testing(&reprojected)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.id.as_str(), notice.id);
        let retry = f
            .state
            .observer_subscriptions
            .inbox
            .deliver_managed_root_notification(&selected, Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.delivery.id.as_str(), notice.id);
        assert!(!retry.pending);
        assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), expected_calls);

        permissions
            .grant_typed_scoped_session_permission(
                &f.reference.session_id,
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "subscribed-dispatch-followup".into(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: format!("session_control followup {target}"),
                },
            )
            .unwrap();
        let reference = managed_target(&f.state, &f.reference, target).await;
        let followed = invoke_managed_control(
            &f.state,
            &f.reference,
            "second-subscribed-dispatch",
            json!({"action":"followup","target_session_id":target,
                "target_ref":reference, "operation_id":"second-subscribed-dispatch",
                "message":SUBSCRIBED_FIRST_INPUT,"notify_on_material_event":true}),
        )
        .await;
        assert_eq!(followed["admitted"], true);
        drive_material_notices(&f, expected_calls + 1).await;
        settle_material_notices(&f).await;
        let parent = load(&f.state, &f.reference.session_id).await;
        let delivery = parent
            .messages
            .iter()
            .rev()
            .find(|message| {
                message.content.contains("Worker summary:")
                    && message.content.contains(HANDOFF_HEAD)
            })
            .unwrap();
        let target = load(&f.state, target).await;
        let last = target
            .runtime_metadata
            .unwrap()
            .supervised_root_outcomes
            .unwrap()
            .facts
            .pop()
            .unwrap();
        assert!(delivery
            .content
            .contains(&last.final_message.unwrap().message_id));
    }
    f.state.shutdown().await;
}

#[actix_web::test]
async fn ordinary_approval_handoff_waits_for_deadline_and_keeps_forced_requests_with_owner() {
    exercise_ordinary_approval_handoff(PermissionDecisionKind::DenyOnce).await;
}

#[actix_web::test]
async fn ordinary_approval_handoff_can_allow_the_exact_workspace_edit_once() {
    exercise_ordinary_approval_handoff(PermissionDecisionKind::AllowOnce).await;
}

async fn exercise_ordinary_approval_handoff(final_decision: PermissionDecisionKind) {
    use crate::handlers::agent::respond::{capture_approval_handoff, decide_delegated_approval};
    use bamboo_orchestration::managed_session::approval_handoff::DELAY;
    use bamboo_tools::permission::{
        PermissionMode, PermissionReasonCode, PermissionRequest, PermissionType, RiskLevel,
    };

    let f = Fixture::new_with_native_source(true).await;
    let state = &f.state;
    let now = Utc::now();
    let parked_at = now - DELAY - chrono::TimeDelta::seconds(1);
    let snapshot = state.permission_section.snapshot();
    let mut candidate = snapshot.data.as_ref().clone();
    candidate.supervisor_approval_handoff_since = Some(parked_at);
    state
        .permission_section
        .commit(snapshot.revision, candidate)
        .unwrap();
    let snapshot = state.permission_section.snapshot();
    let config = state.permission_checker.permission_config().unwrap();
    config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());

    let mut target = load(state, ROOT_A).await;
    let workspace = state
        .app_data_dir
        .join("workspaces")
        .join("approval-fixture");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let workspace_name = bamboo_config::paths::path_to_display_string(&workspace);
    target.workspace = Some(workspace_name.clone());
    target.set_workspace_path_meta(workspace_name.clone());
    config.set_session_workspace(ROOT_A, Some(workspace_name));
    target
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .set_permission_mode(SessionPermissionMode::Default);
    target.set_last_run_status("suspended");
    let file = workspace.join("handoff-test.txt");
    let file = bamboo_config::paths::path_to_display_string(&file);
    let workspace = bamboo_config::paths::path_to_display_string(&workspace);
    let call = "write-with-handoff";
    target
        .metadata
        .insert("session.origin.v1".into(), "supervisor_created".into());
    let tool_call = ToolCall {
        id: call.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Write".into(),
            arguments: json!({"file_path":file,"content":"test\n"}).to_string(),
        },
    };
    target.add_message(Message::assistant("", Some(vec![tool_call.clone()])));
    let request = PermissionRequest {
        request_id: call.into(),
        request_generation: "write-handoff-generation".into(),
        session_id: target.id.clone(),
        workspace_path: Some(workspace.clone()),
        tool_name: "Write".into(),
        permission_type: PermissionType::WriteFile,
        resource: file.clone(),
        operation_summary: "Write one file in the workspace".into(),
        risk_level: RiskLevel::Medium,
        reason_code: PermissionReasonCode::RiskThreshold,
        effective_mode: PermissionMode::Default,
        bypass_requested: false,
        auto_approve_requested: false,
        policy_revision: snapshot.revision,
        matched_rule: None,
        allowed_decisions: PermissionRequest::forced_decisions(),
        suggested_matchers: vec![],
    };
    let mut parked = Message::tool_result(
        call,
        json!({
            "status":"awaiting_permission_approval",
            "permission_type":"write_file",
            "resource": file,
            "permission_request":request,
        })
        .to_string(),
    );
    use bamboo_agent_core::tools::{PendingToolOperation, PENDING_TOOL_OPERATION_METADATA_KEY};
    let operation = PendingToolOperation::capture_with_contract(
        SessionCallerReference::from(&target),
        &parked.id,
        &tool_call,
        "Write",
        &request.request_generation,
        None,
        true,
    );
    parked.metadata = Some(json!({
        "permission_request": request,
        (PENDING_TOOL_OPERATION_METADATA_KEY): operation.metadata_value(),
    }));
    parked.created_at = parked_at;
    target.add_message(parked);
    target.set_pending_question_with_source(
        call.into(),
        "Write".into(),
        "Write the test file?".into(),
        vec!["Approve".into(), "Deny".into()],
        false,
        bamboo_domain::PendingQuestionSource::PauseTool,
    );
    state.save_and_cache_session(&mut target).await;
    let target = load(state, ROOT_A).await;
    let (handoff, current_request) = capture_approval_handoff(state, &f.reference, &target)
        .expect("a pending, exact native workspace edit is eligible");
    assert_eq!(handoff.due_at, parked_at + DELAY);
    let outside = f.home.path().canonicalize().unwrap().join("outside.txt");
    let outside = bamboo_config::paths::path_to_display_string(&outside);
    let mut misleading = current_request.clone();
    misleading.resource = format!(" {outside}");
    #[cfg(unix)]
    std::fs::create_dir_all(
        std::path::Path::new(&workspace)
            .join(&misleading.resource)
            .parent()
            .unwrap(),
    )
    .unwrap();
    assert!(
        bamboo_orchestration::managed_session::approval_handoff::ApprovalHandoff::capture(
            &f.reference,
            &target,
            &misleading,
            &json!({"file_path":misleading.resource}),
            &config,
            &state.app_data_dir,
            parked_at,
        )
        .is_none(),
        "the path approved by policy must be the path executed by the native tool"
    );
    let mut reparked = target.clone();
    let tool_result = reparked.messages.last_mut().unwrap();
    tool_result.metadata.as_mut().unwrap()[bamboo_domain::PERMISSION_REPARKED_AT_KEY] = json!(now);
    let (fresh, request) = capture_approval_handoff(state, &f.reference, &reparked).unwrap();
    assert_eq!(fresh.due_at, now + DELAY);
    assert!(fresh.notice(&f.reference, &request, now).is_none());
    let mut control = target.clone();
    let git_dir = std::path::Path::new(&workspace).join(".git");
    std::fs::create_dir(&git_dir).unwrap();
    let control_workspace = bamboo_config::paths::path_to_display_string(&git_dir);
    control.set_workspace_path_meta(control_workspace.clone());
    let mut control_request = current_request.clone();
    control_request.workspace_path = Some(control_workspace);
    control_request.resource =
        bamboo_config::paths::path_to_display_string(&git_dir.join("config"));
    assert!(
        bamboo_orchestration::managed_session::approval_handoff::ApprovalHandoff::capture(
            &f.reference,
            &control,
            &control_request,
            &json!({"file_path":control_request.resource}),
            &config,
            &state.app_data_dir,
            parked_at,
        )
        .is_none()
    );
    let caller = SessionCallerReference::from(&load(state, &f.reference.session_id).await);
    let decision = |kind| {
        decide_delegated_approval(
            state.clone(),
            &f.state.observer_subscriptions.inbox,
            &caller,
            ROOT_A.into(),
            &handoff.reference,
            kind,
        )
    };
    assert!(decision(PermissionDecisionKind::DenyOnce).await.is_err());

    let mut cursor = None;
    crate::app_state::approval_handoff::sweep(
        state,
        &mut cursor,
        handoff.due_at - chrono::TimeDelta::seconds(1),
    )
    .await
    .unwrap();
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
    config.set_ask_rules(["session_control".into()]);
    assert!(capture_approval_handoff(state, &f.reference, &target).is_none());
    crate::app_state::approval_handoff::sweep(state, &mut cursor, now)
        .await
        .unwrap();
    settle_material_notices(&f).await;
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
    config.set_ask_rules(Vec::new());
    let notice = handoff.notice(&f.reference, &current_request, now).unwrap();
    state
        .session_inbox
        .deliver_runtime_for_target(&notice, caller.created_at)
        .await
        .unwrap();
    assert!(decision(PermissionDecisionKind::AllowOnce).await.is_err());
    crate::app_state::approval_handoff::sweep(state, &mut cursor, now)
        .await
        .unwrap();
    drive_material_notices(&f, 1).await;
    settle_material_notices(&f).await;
    // The stable Inbox proof is reused when the sweep restarts.
    crate::app_state::approval_handoff::sweep(state, &mut None, now)
        .await
        .unwrap();
    settle_material_notices(&f).await;
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    {
        let inputs = f.probe.parent_inputs.lock().unwrap();
        let notice = inputs[0]
            .iter()
            .find(|message| message.content.contains(&handoff.reference))
            .expect("the original Supervisor received the timed review");
        assert!(notice.content.contains(&file));
        assert!(bamboo_orchestration::managed_session::is_internal_handoff(
            notice
        ));
        assert!(bamboo_engine::session_app::execute::is_hidden_from_ui(
            notice
        ));
    }

    // Revalidation happens at the decision, even after a due notice exists.
    config.set_ask_rules(["Write".into()]);
    assert!(decision(PermissionDecisionKind::AllowOnce).await.is_err());
    config.set_ask_rules([format!(
        r#"session_control(*"approval_reference":"{}"*)"#,
        handoff.reference,
    )]);
    assert!(decision(PermissionDecisionKind::AllowOnce).await.is_err());
    config.set_ask_rules(Vec::new());
    assert!(decision(PermissionDecisionKind::AllowGlobal).await.is_err());

    state
        .approval_runtime
        .set(Arc::downgrade(&state.clone().into_inner()))
        .expect("bind the native decision port");
    let result = invoke_managed_control(
        state,
        &f.reference,
        "timed-decision",
        json!({"action":"decide_approval", "target_session_id":ROOT_A,
            "approval_reference":handoff.reference, "decision":final_decision}),
    )
    .await;
    assert_eq!(
        result["receipt"]["decision"]["decision"],
        json!(final_decision)
    );
    assert!(load(state, ROOT_A).await.pending_question.is_none());
    assert!(decision(PermissionDecisionKind::AllowOnce).await.is_err());
    if final_decision == PermissionDecisionKind::AllowOnce {
        assert_eq!(result["auto_resume_status"], "started", "{result}");
        let completed = settled(state, ROOT_A).await;
        let tool_result = completed
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(call))
            .unwrap();
        assert_eq!(
            tool_result.tool_success,
            Some(true),
            "{}",
            tool_result.content
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "test\n");
    } else {
        assert!(!std::path::Path::new(&file).exists());
    }
    state.shutdown().await;
}

pub(in crate::app_state) async fn exercise_material_root_notifications() {
    use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};

    let f = Fixture::new_with_native_source(true).await;
    let mut unobserved = Session::new("unobserved-large-root", "synthetic-model");
    unobserved.add_message(Message::user("x".repeat(4 * 1024 * 1024)));
    f.state.storage.save_session(&unobserved).await.unwrap();
    assert!(
        !ManagedRootNotificationIo::sources(f.state.observer_subscriptions.as_ref())
            .await
            .contains(&unobserved.id)
    );
    let workspace = f.home.path().join("notifying-second-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let mut second_session = load(&f.state, ROOT_B).await;
    second_session.model = SOURCE.into();
    second_session.model_ref = Some(ProviderModelRef::new(PROVIDER, SOURCE));
    second_session.set_provider_name(PROVIDER);
    second_session.workspace = Some(workspace.to_string_lossy().into());
    second_session.set_workspace_path_meta(workspace.to_string_lossy());
    f.state.storage.save_session(&second_session).await.unwrap();
    f.state
        .permission_checker
        .permission_config()
        .unwrap()
        .grant_typed_scoped_session_permission(
            &f.reference.session_id,
            PermissionType::ExecuteCommand,
            PermissionMatcher {
                id: "notifying-second-followup".into(),
                kind: PermissionMatcherKind::ExactResource,
                value: format!("session_control followup {ROOT_B}"),
            },
        )
        .unwrap();
    let first = managed_target(&f.state, &f.reference, ROOT_A).await;
    let second = managed_target(&f.state, &f.reference, ROOT_B).await;
    *f.probe.parent_dispatch.lock().unwrap() = Some(
        [
            (&first, "notifying-first", "managed task blocker"),
            (&second, "notifying-second", "managed wait second"),
        ]
        .into_iter()
        .map(|(target, operation, message)| ToolCall {
            id: operation.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_control".into(),
                arguments: json!({
                    "action":"followup", "target_session_id":target.session_id,
                    "target_ref":target, "operation_id":operation, "message":message,
                    "notify_on_material_event":true,
                })
                .to_string(),
            },
        })
        .collect(),
    );
    f.probe.hold_sources.store(true, Ordering::SeqCst);
    f.state
        .session_messenger
        .send(bamboo_domain::SessionMessageEnvelope::user_input(
            &f.reference.session_id,
            "Coordinate the two recorded tasks and wait for their notices.",
        ))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        tokio::join!(
            f.probe.source_entered[0].notified(),
            f.probe.source_entered[1].notified(),
        );
    })
    .await
    .expect("the Supervisor's real tool calls must start both Roots");
    drive_material_notices(&f, 2).await;
    settle_material_notices(&f).await;
    let unchanged_reads = f
        .state
        .observer_subscriptions
        .material_main_reads
        .load(Ordering::SeqCst);
    let first_run = f
        .state
        .session_activation_router
        .current_run_id(ROOT_A)
        .await
        .unwrap();
    let second_run = f
        .state
        .session_activation_router
        .current_run_id(ROOT_B)
        .await
        .unwrap();

    let original_hint = f
        .state
        .session_store
        .get_index_entry(ROOT_A)
        .await
        .unwrap()
        .material_notification_hint;
    assert!(original_hint.is_some());
    f.state
        .session_store
        .clear_material_notification_hint(ROOT_A, first.created_at, "a-different-request")
        .await
        .unwrap();
    assert_eq!(
        f.state
            .session_store
            .get_index_entry(ROOT_A)
            .await
            .unwrap()
            .material_notification_hint,
        original_hint
    );

    for _ in 0..130 {
        f.state
            .observer_subscriptions
            .material_tick()
            .await
            .unwrap();
    }
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        f.state
            .observer_subscriptions
            .material_main_reads
            .load(Ordering::SeqCst),
        unchanged_reads,
        "unchanged running records must keep Main transcripts unread"
    );
    assert_eq!(
        f.state
            .session_activation_router
            .current_run_id(ROOT_A)
            .await,
        Some(first_run.clone())
    );
    assert_eq!(
        f.state
            .session_activation_router
            .current_run_id(ROOT_B)
            .await,
        Some(second_run.clone())
    );

    let parent = load(&f.state, &f.reference.session_id).await;
    let caller = SessionCallerReference::from(&parent);
    assert!(
        f.state
            .session_messenger
            .supervisor_current_user_followup(
                &caller,
                &f.reference,
                &first,
                "notifying-first",
                "managed task blocker",
            )
            .await
            .is_err(),
        "an exact retry cannot remove its admitted notification opt-in"
    );
    f.state
        .session_messenger
        .supervisor_followup_with_material_notifications(
            &caller,
            &f.reference,
            &first,
            "notifying-first",
            "managed task blocker",
            true,
        )
        .await
        .expect("the unchanged opt-in must replay its original receipt");
    assert_eq!(f.probe.source_calls.load(Ordering::SeqCst), 2);

    f.probe.source_release[0].notify_one();
    tokio::time::timeout(WAIT, f.probe.task_written.notified())
        .await
        .unwrap();
    drive_material_notices(&f, 3).await;
    let requests = f.probe.parent_inputs.lock().unwrap();
    let blocker_notice = requests
        .last()
        .unwrap()
        .iter()
        .find(|message| message.content.contains("recorded a Task blocker"))
        .unwrap();
    assert!(blocker_notice.content.contains(&first_run));
    assert!(blocker_notice.content.contains("dependency"));
    drop(requests);
    assert_eq!(
        f.state
            .session_activation_router
            .current_run_id(ROOT_B)
            .await,
        Some(second_run.clone())
    );

    f.probe.source_release[0].notify_one();
    tokio::time::timeout(WAIT, f.probe.task_rewritten.notified())
        .await
        .unwrap();
    settle_material_notices(&f).await;
    let reads = f
        .state
        .observer_subscriptions
        .material_main_reads
        .load(Ordering::SeqCst);
    let proof_io = Arc::clone(&f.state.observer_subscriptions.material_proof_io)
        .acquire_owned()
        .await
        .unwrap();
    for _ in 0..12 {
        f.state
            .observer_subscriptions
            .material_tick()
            .await
            .unwrap();
    }
    drop(proof_io);
    assert_eq!(
        f.state
            .observer_subscriptions
            .material_main_reads
            .load(Ordering::SeqCst),
        reads,
        "handled material evidence must keep full-history proof I/O idle"
    );
    assert_eq!(
        f.probe.parent_calls.load(Ordering::SeqCst),
        3,
        "an unrelated Task update must reuse the already delivered blocker receipt"
    );

    let root = load(&f.state, ROOT_A).await;
    let candidate = bamboo_orchestration::managed_session::notification_candidates(&root)
        .unwrap()
        .pop()
        .unwrap();
    let mut stale = candidate.clone();
    stale.target.created_at += chrono::Duration::seconds(1);
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_managed_root_notification(&stale, Utc::now())
        .await
        .is_err());
    let mut stale = candidate;
    stale.caller.created_at += chrono::Duration::seconds(1);
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_managed_root_notification(&stale, Utc::now())
        .await
        .is_err());

    // Evict the in-memory delivery acknowledgement while a real Root writer
    // owns A. B must still report its prior run during a successor execution.
    let (write_reached, write_release) = f
        .state
        .session_store
        .pause_full_save_before_filesystem_commit_for_test(ROOT_A);
    let store = Arc::clone(&f.state.session_store);
    let save = tokio::spawn(async move { store.save_session(&root).await });
    tokio::time::timeout(WAIT, write_reached.wait())
        .await
        .unwrap();
    f.state
        .observer_subscriptions
        .material_cache
        .lock()
        .await
        .get_mut(ROOT_A)
        .unwrap()
        .delivered
        .clear();
    f.probe.source_release[1].notify_one();
    tokio::time::timeout(WAIT, async {
        while f
            .state
            .session_activation_router
            .current_run_id(ROOT_B)
            .await
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    f.state
        .session_messenger
        .send(bamboo_domain::SessionMessageEnvelope::user_input(
            ROOT_B,
            "managed wait second successor",
        ))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, f.probe.source_entered[1].notified())
        .await
        .unwrap();
    let successor = f
        .state
        .session_activation_router
        .current_run_id(ROOT_B)
        .await
        .unwrap();
    assert_ne!(successor, second_run);
    drive_material_notices(&f, 4).await;
    assert_eq!(
        f.state
            .session_activation_router
            .current_run_id(ROOT_B)
            .await,
        Some(successor)
    );
    let last = f.probe.parent_inputs.lock().unwrap();
    assert!(last
        .last()
        .unwrap()
        .iter()
        .any(|message| message.content.contains(&second_run)));
    drop(last);
    write_release.wait().await;
    save.await.unwrap().unwrap();
    f.probe.finish_blocker.notify_one();
    drive_material_notices(&f, 5).await;
    f.probe.source_release[1].notify_one();
    tokio::time::timeout(WAIT, async {
        while f
            .state
            .session_activation_router
            .current_run_id(ROOT_A)
            .await
            .is_some()
            || f.state
                .session_activation_router
                .current_run_id(ROOT_B)
                .await
                .is_some()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let Fixture {
        home,
        state,
        probe,
        reference,
    } = f;
    drop(state);
    let state = host(home.path(), probe.clone()).await;
    let restarted = Fixture {
        home,
        state,
        probe,
        reference,
    };
    settle_material_notices(&restarted).await;
    for _ in 0..12 {
        restarted
            .state
            .observer_subscriptions
            .material_tick()
            .await
            .unwrap();
    }
    assert!(
        ManagedRootNotificationIo::sources(restarted.state.observer_subscriptions.as_ref())
            .await
            .is_empty()
    );
    assert_eq!(restarted.probe.parent_calls.load(Ordering::SeqCst), 5);
    assert!(restarted
        .state
        .session_activation_router
        .current_run_id(&restarted.reference.session_id)
        .await
        .is_none());
}

pub(in crate::app_state) async fn exercise_material_notification_cache_fairness() {
    let f = Fixture::new_with_native_source(true).await;
    let caller = SessionCallerReference::from(&load(&f.state, &f.reference.session_id).await);
    f.probe.hold_sources.store(true, Ordering::SeqCst);
    let mut ids = Vec::new();
    for index in 0..17 {
        let id = format!("material-cache-source-{index}");
        let workspace = f.home.path().join(&id);
        std::fs::create_dir(&workspace).unwrap();
        let mut session = Session::new(&id, SOURCE);
        session.model_ref = Some(ProviderModelRef::new(PROVIDER, SOURCE));
        session.set_provider_name(PROVIDER);
        session.workspace = Some(workspace.to_string_lossy().into());
        session.set_workspace_path_meta(workspace.to_string_lossy());
        f.state.storage.save_session(&session).await.unwrap();
        let target = managed_target(&f.state, &f.reference, &id).await;
        let message = if index < 9 {
            "managed wait second cache source"
        } else {
            "unchanged cache source"
        };
        f.state
            .session_messenger
            .supervisor_followup_with_material_notifications(
                &caller,
                &f.reference,
                &target,
                &format!("material-cache-followup-{index}"),
                message,
                true,
            )
            .await
            .unwrap();
        ids.push(id);
    }
    tokio::time::timeout(WAIT, async {
        while f.probe.source_calls.load(Ordering::SeqCst) != 17 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut runs = Vec::new();
    for id in &ids[..9] {
        runs.push(
            f.state
                .session_activation_router
                .current_run_id(id)
                .await
                .unwrap(),
        );
    }
    f.probe.source_release[1].notify_waiters();
    tokio::time::timeout(WAIT, async {
        loop {
            let mut active = false;
            for id in &ids[..9] {
                active |= f
                    .state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_some();
            }
            if !active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Keep eight unchanged active sources ahead of nine older handoffs.
    for id in &ids[9..] {
        let mut session = load(&f.state, id).await;
        session.updated_at = Utc::now();
        f.state.storage.save_session(&session).await.unwrap();
    }
    let sources = ManagedRootNotificationIo::sources(f.state.observer_subscriptions.as_ref()).await;
    assert_eq!(sources.len(), 17);
    assert!(sources[..8].iter().all(|id| ids[9..].contains(id)));
    tokio::time::timeout(WAIT * 3, async {
        loop {
            if let Err(error) = f.state.observer_subscriptions.material_tick().await {
                assert!(
                    error.contains("pending") || error.contains("busy"),
                    "{error}"
                );
            }
            let observed = {
                let requests = f.probe.parent_inputs.lock().unwrap();
                runs.iter().all(|run| {
                    requests
                        .iter()
                        .flatten()
                        .any(|message| message.content.contains(run))
                })
            };
            if observed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all nine older handoffs must reach the real native Supervisor path");
    assert!(
        f.state
            .observer_subscriptions
            .material_cache
            .lock()
            .await
            .len()
            <= 16
    );
    f.probe.source_release[0].notify_waiters();
    tokio::time::timeout(WAIT, async {
        for id in &ids[9..] {
            while f
                .state
                .session_activation_router
                .current_run_id(id)
                .await
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
    .unwrap();
}

pub(in crate::app_state) async fn exercise_managed_wait_blocker() {
    use bamboo_domain::{ManagedRootEvent, ManagedRootEventRead, SessionCallerReference};

    let f = Fixture::new_with_native_source(true).await;
    let first = managed_target(&f.state, &f.reference, ROOT_A).await;
    let second = managed_target(&f.state, &f.reference, ROOT_B).await;
    admit_native_source(&f, &first, "managed-task-blocker", "managed task blocker").await;
    tokio::time::timeout(WAIT, f.probe.source_entered[0].notified())
        .await
        .expect("Root provider reached the first Task round");
    let router = &f.state.session_activation_router;
    let run = router.current_run_id(ROOT_A).await.unwrap();
    let request = |cursor| {
        json!({
            "action":"wait_outcomes",
            "outcome_targets":[
                {"target_ref":first,"expected_run_id":run,"after_sequence":0,"after_blocker_cursor":cursor},
                {"target_ref":second,"expected_run_id":"never-started","after_sequence":0,"after_blocker_cursor":0},
            ],
        })
    };

    let result = {
        let waiter = invoke_managed_control(&f.state, &f.reference, "await-blocker", request(0));
        tokio::pin!(waiter);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiter)
                .await
                .is_err(),
            "an active Root without a recorded blocker must keep waiting"
        );
        f.probe.source_release[0].notify_one();
        tokio::time::timeout(WAIT, waiter)
            .await
            .expect("Task write must wake the Supervisor")
    };
    assert_eq!(result["status"], "recorded_blocker");
    assert_eq!(result["blocker"]["target_session_id"], ROOT_A);
    assert_eq!(result["blocker"]["run_id"], run);
    assert_eq!(result["blocker"]["task_id"], "dependency");
    let version = result["blocker"]["task_version"].as_u64().unwrap();
    let cursor = result["blocker"]["cursor"].as_u64().unwrap();
    assert!(cursor > 0);
    assert_eq!(cursor, version);
    tokio::time::timeout(WAIT, f.probe.task_written.notified())
        .await
        .unwrap();
    assert_eq!(
        router.current_run_id(ROOT_A).await.as_deref(),
        Some(run.as_str())
    );

    f.probe.source_release[0].notify_one();
    tokio::time::timeout(WAIT, f.probe.task_rewritten.notified())
        .await
        .expect("same Root committed an unrelated Task update");

    // A fresh store reads the exact evidence without an in-memory runner
    // signal. Cursors and the run selector must independently exclude replay.
    let reopened = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
    let inbox = FileSessionInbox::new(reopened, bamboo_domain::SessionInboxLimits::default());
    let supervisor = load(&f.state, &f.reference.session_id).await;
    let caller = SessionCallerReference::from(&supervisor);
    let read = |run_id, cursor| {
        inbox.read_managed_root_event(&caller, &f.reference, &first, run_id, 0, cursor)
    };
    let ManagedRootEventRead::Next(ManagedRootEvent::Blocker(blocker)) =
        read(run.as_str(), Some(0)).await.unwrap()
    else {
        panic!("fresh storage must still observe the Task blocker");
    };
    assert!(blocker.task_version > version);
    assert_eq!(blocker.cursor, cursor);
    assert_eq!(
        read(run.as_str(), Some(cursor)).await.unwrap(),
        ManagedRootEventRead::CaughtUp
    );
    assert_eq!(
        read("different-run", Some(0)).await.unwrap(),
        ManagedRootEventRead::CaughtUp
    );
    assert_eq!(
        read(run.as_str(), None).await.unwrap(),
        ManagedRootEventRead::CaughtUp
    );

    // The generic Task transaction can replace the same version. An old
    // proof must not authenticate the replacement, even for the same task ID.
    let original = f
        .state
        .storage
        .load_runtime_control_plane(ROOT_A)
        .await
        .unwrap()
        .unwrap();
    let mut replaced = original.clone();
    replaced.task_list.as_mut().unwrap().items[0].description = "external replacement".into();
    assert!(f
        .state
        .storage
        .save_task_control_plane_if_matches(&original, &replaced)
        .await
        .unwrap());
    assert_eq!(
        read(run.as_str(), Some(0)).await.unwrap(),
        ManagedRootEventRead::CaughtUp
    );
    assert!(f
        .state
        .storage
        .save_task_control_plane_if_matches(&replaced, &original)
        .await
        .unwrap());

    assert_eq!(
        read(run.as_str(), Some(0)).await.unwrap(),
        ManagedRootEventRead::CaughtUp,
        "a later generic restore cannot resurrect the original Root attribution"
    );

    let outcome = {
        let waiter =
            invoke_managed_control(&f.state, &f.reference, "await-handoff", request(cursor));
        tokio::pin!(waiter);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiter)
                .await
                .is_err()
        );
        f.probe.finish_blocker.notify_one();
        tokio::time::timeout(WAIT, waiter)
            .await
            .expect("blocker cursor cannot consume outcome")
    };
    assert_eq!(outcome["status"], "recorded_outcome");
    assert_eq!(outcome["outcome"]["run_id"], run);
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
    tokio::time::timeout(WAIT, async {
        loop {
            let visible = {
                let requests = probe.parent_inputs.lock().unwrap();
                expected.iter().all(|(receipt, run, _, _, operation)| {
                    requests.iter().flatten().any(|message| {
                        message.content.contains(&format!("- Root: {ROOT_A}"))
                            && message.content.contains(run.as_str())
                            && message.content.contains(receipt.as_str())
                            && message.content.contains(*operation)
                            && message
                                .content
                                .contains("- Last persisted status: completed")
                    })
                })
            };
            if visible {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both Root outcome identities must reach the Supervisor provider input");
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
