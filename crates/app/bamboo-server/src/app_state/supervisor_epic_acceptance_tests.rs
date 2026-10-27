//! Prepared #1048 composition. This file is deliberately not wired into mod.rs
//! until the separately owned Planner and Observer dependencies are accepted.
//! No runtime PASS is claimed by its presence. Store, Inbox, tools and native
//! execution below are production implementations; only model replies are held.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use super::AppState;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{
    Message, ObserverChangeKind, ObserverViewChange, PendingQuestion, PendingQuestionSource,
    ProviderModelRef, Session, SessionActivationPolicy, SessionInboxPort, SessionMessageEnvelope,
    SessionPermissionMode, Storage, SupervisorReference,
};
use bamboo_engine::session_app::child_session::{
    create_child_action, ChildSessionPort, CreateChildInput,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
use uuid::Uuid;

const PROVIDER: &str = "epic-chain-provider";
const PARENT: &str = "epic-supervisor";
const TRACKER: &str = "epic-tracker";
const PLANNER: &str = "epic-planner";
const WORKER: &str = "epic-original-worker";
const OBSERVER: &str = "epic-fixed-tracker";
const ROOT_A: &str = "epic-existing-a";
const ROOT_B: &str = "epic-existing-b";
const USER: &str = "EPIC_USER: plan, then execute exactly one authorized followup on Root B";
const COLD: &str = "EPIC_COLD: reread the same plan and retry the identical followup";
const FOLLOWUP: &str =
    "EPIC_AUTHORIZED_FOLLOWUP: apply the selected bounded plan to this original Root";
const OPERATION: &str = "8ce86015-a819-4e67-b688-ddd30d6ba240";
const BLOCKER: &str = "EPIC_BLOCKER: Root A awaits its existing owner decision";
const DURING_PLAN: &str = "EPIC_SECOND_PROJECT_OBSERVED_DURING_PLAN";
const DONE: &str = "EPIC_CHAIN_FINISHED";
const COLD_DONE: &str = "EPIC_COLD_FINISHED";
const APPENDIX: &str = "EXCLUDED_EPIC_APPENDIX";
const PRIVATE_A: &str = "ORIGINAL_ROOT_A_PRIVATE_HISTORY";
const PRIVATE_B: &str = "ORIGINAL_ROOT_B_PRIVATE_HISTORY";
const WAIT: Duration = Duration::from_secs(90);

fn call(name: &str, id: &str, arguments: Value) -> LLMChunk {
    LLMChunk::ToolCalls(vec![ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    }])
}
fn tool_json(messages: &[Message], id: &str) -> Option<Value> {
    let message = messages
        .iter()
        .rev()
        .find(|m| m.tool_call_id.as_deref() == Some(id))?;
    assert_ne!(
        message.tool_success,
        Some(false),
        "{id}: {}",
        message.content
    );
    let mut result: Value = serde_json::from_str(&message.content).unwrap();
    if let Some(inner) = result.get("result").and_then(Value::as_str) {
        result = serde_json::from_str(inner).unwrap();
    }
    Some(result)
}
fn artifact(messages: &[Message]) -> Option<Value> {
    messages.iter().rev().find_map(|message| {
        let (_, tail) = message.content.split_once("Child typed result:\n")?;
        let result: Value = serde_json::from_str(tail.lines().next()?).ok()?;
        (result["view"] == "planner_artifact").then_some(result)
    })
}

struct Probe {
    workspace: PathBuf,
    requests: AtomicUsize,
    worker_calls: AtomicUsize,
    planner_calls: AtomicUsize,
    tracker_calls: AtomicUsize,
    plan_started: AtomicBool,
    tracker_entered: Notify,
    tracker_release: Notify,
    planner_entered: Notify,
    planner_release: Notify,
    observed: Mutex<BTreeSet<String>>,
}
impl Probe {
    fn new(workspace: PathBuf) -> Self {
        Self {
            workspace,
            requests: AtomicUsize::new(0),
            worker_calls: AtomicUsize::new(0),
            planner_calls: AtomicUsize::new(0),
            tracker_calls: AtomicUsize::new(0),
            plan_started: AtomicBool::new(false),
            tracker_entered: Notify::new(),
            tracker_release: Notify::new(),
            planner_entered: Notify::new(),
            planner_release: Notify::new(),
            observed: Mutex::new(BTreeSet::new()),
        }
    }
    fn parent(&self, messages: &[Message]) -> LLMChunk {
        assert!(!messages.iter().any(|m| m.content.contains(APPENDIX)));
        assert!(!messages
            .iter()
            .any(|m| m.content.contains(PRIVATE_A) || m.content.contains(PRIVATE_B)));
        let cold = messages.iter().any(|m| m.content == COLD);
        if let Some(reference) = artifact(messages) {
            assert_eq!(reference["available"], true);
            assert_eq!(reference["execution_approved"], false);
            let prefix = if cold { "epic-cold" } else { "epic-plan" };
            let index_id = format!("{prefix}-index");
            let section_id = format!("{prefix}-section");
            let followup_id = format!("{prefix}-followup");
            let read = reference["read"].clone();
            let Some(index) = tool_json(messages, &index_id) else {
                return call("Plan", &index_id, read);
            };
            let Some(section) = tool_json(messages, &section_id) else {
                let section = index["sections"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|section| section["heading"] == "Chosen action")
                    .unwrap()["id"]
                    .clone();
                let mut read = read;
                read["section_id"] = section;
                read["max_bytes"] = json!(512);
                return call("Plan", &section_id, read);
            };
            assert!(section["text"]
                .as_str()
                .unwrap()
                .contains("EPIC_SELECTED_ACTION"));
            if tool_json(messages, &followup_id).is_none() {
                return call(
                    "session_control",
                    &followup_id,
                    json!({"action":"followup",
                    "target_session_id":ROOT_B,"operation_id":OPERATION,"message":FOLLOWUP}),
                );
            }
            return LLMChunk::Token(if cold { COLD_DONE } else { DONE }.into());
        }
        if messages.iter().any(|m| m.content.contains(BLOCKER)) {
            if !self.plan_started.swap(true, Ordering::SeqCst) {
                let user = messages
                    .iter()
                    .find(|m| m.content == USER)
                    .expect("canonical accepted user input");
                return call(
                    "Plan",
                    "epic-create-plan",
                    json!({
                    "task":"Read source.txt and plan the explicitly authorized bounded Root B followup. Do not implement.",
                    "context_packet":{"version":1,"objective":"Return a bounded plan for the original Root B",
                        "constraints":["Preserve both original Roots and Root A pending question"],
                        "acceptance":["Return a versioned plan with a selected action section"],
                        "non_goals":["No implementation or approval by the planner"],
                        "necessary_user_instructions":[],"recorded_decisions":[BLOCKER],
                        "source_user_message_ids":[user.id],"background_message_ids":[]}}),
                );
            }
            return LLMChunk::Token(DURING_PLAN.into());
        }
        LLMChunk::Token("EPIC_IDLE_PARENT_FINISHED_WITH_ACTIVE_TRACKER".into())
    }
    async fn tracker(&self, messages: &[Message], tools: &[ToolSchema]) -> LLMChunk {
        if self.tracker_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.tracker_entered.notify_one();
            self.tracker_release.notified().await;
        }
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["session_history", "session_history_current"])
        );
        assert!(!messages
            .iter()
            .any(|m| m.content.contains(PRIVATE_A) || m.content.contains(PRIVATE_B)));
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
            .expect("actual canonical subscription notice");
        let attack_id = format!("attack-{}", change.event_id.as_str());
        let read_id = format!("status-{}", change.event_id.as_str());
        if !messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some(attack_id.as_str()))
        {
            return call(
                "session_control",
                &attack_id,
                json!({"action":"followup",
                "target_session_id":ROOT_B,"operation_id":"6c934c1d-38cd-48db-b35e-fb5a6fed8071","message":"DENIED_TRACKER_ACTION"}),
            );
        }
        let denied = messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some(attack_id.as_str()))
            .unwrap();
        assert_eq!(denied.tool_success, Some(false), "{}", denied.content);
        if let Some(view) = tool_json(messages, &read_id) {
            assert!(view["content"]
                .as_str()
                .unwrap()
                .contains(&format!("- Root: {}", change.target_session_id)));
            self.observed
                .lock()
                .unwrap()
                .insert(change.target_session_id.clone());
            let summary = if change.target_session_id == ROOT_A {
                assert!(view["content"]
                    .as_str()
                    .unwrap()
                    .contains("- Has pending question: true"));
                BLOCKER
            } else {
                "EPIC_ROOT_B_PERSISTED_STATUS_OBSERVED"
            };
            return LLMChunk::Token(format!(
                "{summary}; bounded observation {} of {}",
                change.sequence, change.target_session_id
            ));
        }
        call(
            "session_history",
            &read_id,
            json!({"action":"read_context","session_id":change.target_session_id,
            "revision":change.view_revision,"file":"status","offset":0,"limit":40}),
        )
    }
    async fn planner(&self, messages: &[Message]) -> LLMChunk {
        let request = self.planner_calls.fetch_add(1, Ordering::SeqCst);
        assert!(request < 3, "one bounded planner only");
        if request == 0 {
            self.planner_entered.notify_one();
            self.planner_release.notified().await;
            return call(
                "Read",
                "epic-planner-read",
                json!({"file_path":self.workspace.join("source.txt")}),
            );
        }
        assert!(messages
            .iter()
            .any(|m| m.content.contains("EPIC_SOURCE_EVIDENCE")));
        if request == 1 {
            return call(
                "Write",
                "epic-planner-denied-write",
                json!({"file_path":self.workspace.join("must-not-exist.txt"),"content":"must be denied"}),
            );
        }
        let denial = messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("epic-planner-denied-write"))
            .unwrap();
        assert_eq!(denial.tool_success, Some(false), "{}", denial.content);
        assert!(!self.workspace.join("must-not-exist.txt").exists());
        LLMChunk::Token(json!({"version":1,"outcome":"completed","summary":"A bounded Root B action is proposed",
            "reported_evidence":[{"description":"Read source.txt in this isolated workspace","reference":"source.txt","sha256":null}],
            "reported_verification":[{"check":"implementation","reported_status":"not_run","details":"planning only"}],
            "proposals":[],"blockers":[],"open_decisions":[],"plan_markdown":format!(
                "# Root B plan\n## Chosen action\nEPIC_SELECTED_ACTION: perform the authorized bounded followup.\n## Appendix\n{}",
                format!("{APPENDIX} ").repeat(1000))}).to_string())
    }
}
#[async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert!(
            self.requests.fetch_add(1, Ordering::SeqCst) < 64,
            "bounded full-chain model request count"
        );
        let chunk = match model {
            PARENT => self.parent(messages),
            TRACKER => self.tracker(messages, tools).await,
            PLANNER => self.planner(messages).await,
            WORKER => {
                assert_eq!(
                    self.worker_calls.fetch_add(1, Ordering::SeqCst),
                    0,
                    "followup never re-executes"
                );
                assert!(messages.iter().any(|m| m.content.contains(PRIVATE_B)));
                assert!(messages.iter().any(|m| m.content.contains(FOLLOWUP)));
                assert!(!messages.iter().any(|m| m.content.contains(PRIVATE_A)));
                LLMChunk::Token("EPIC_ORIGINAL_WORKER_FINISHED".into())
            }
            other => panic!("unrequested model route: {other}"),
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

async fn host(home: &Path, probe: Arc<Probe>) -> AppState {
    let mut config = Config::from_data_dir(Some(home.into()));
    config.provider = PROVIDER.into();
    config.features.provider_model_ref = true;
    config.defaults = Some(
        serde_json::from_value(json!({"chat":{"provider":PROVIDER,"model":PARENT},
        "subagent_models":{"planner":{"provider":PROVIDER,"model":PLANNER}}}))
        .unwrap(),
    );
    config.subagents_mut().max_concurrent = Some(2);
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
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
async fn load(state: &AppState, id: &str) -> Session {
    state.storage.load_session(id).await.unwrap().unwrap()
}
async fn user(state: &AppState, id: &str, content: &str) {
    let receipt = state
        .session_messenger
        .admit_with_activation_intent(
            SessionMessageEnvelope::user_input(id, content),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    state
        .session_messenger
        .activate_prepared(&receipt)
        .await
        .unwrap();
}
async fn wait(state: &AppState, id: &str, predicate: impl Fn(&Session) -> bool) -> Session {
    match tokio::time::timeout(WAIT, async {
        loop {
            let session = load(state, id).await;
            if predicate(&session) {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    {
        Ok(session) => session,
        Err(_) => {
            let session = load(state, id).await;
            let inbox = state.session_inbox.inspect(id).await.unwrap();
            panic!(
                "epic chain timeout id={id} status={:?} error={:?} wait={:?} inbox={inbox:?}",
                session.last_run_status(),
                session.last_run_error(),
                session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|runtime| runtime.waiting_for_children.as_ref())
            );
        }
    }
}
async fn quiet(state: &AppState, id: &str) -> Session {
    let result = tokio::time::timeout(WAIT, async {
        loop {
            let session = load(state, id).await;
            let inbox = state.session_inbox.inspect(id).await.unwrap();
            if session.last_run_status().as_deref() == Some("completed")
                && state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_none()
                && inbox.pending == 0
                && inbox.claimed == 0
            {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(session) => session,
        Err(_) => panic!(
            "epic quiescence timeout id={id}; status={:?}; inbox={:?}; owner={:?}",
            load(state, id).await.last_run_status(),
            state.session_inbox.inspect(id).await.unwrap(),
            state.session_activation_router.current_run_id(id).await
        ),
    }
}
fn marker(session: &Session, text: &str) -> bool {
    session
        .messages
        .iter()
        .any(|message| message.content == text)
}
fn subscription(parent: &Session) -> bamboo_domain::ObserverSubscription {
    parent.supervisor_management.as_ref().unwrap().subscriptions[OBSERVER].clone()
}
fn context_identity(session: &Session) -> Value {
    json!({"id":session.id,"birth":session.created_at,"parent":session.parent_session_id,
        "parent_birth":session.parent_created_at,"authority":session.authority_identity,
        "project":session.project_id_meta(),"workspace":session.workspace,"model_ref":session.model_ref,
        "provider":session.provider_name(),"permissions":session.agent_runtime_state.as_ref().map(|s|s.effective_permission_mode())})
}

#[tokio::test]
async fn two_project_tracker_planner_followup_and_restart_preserve_original_authority() {
    let home = tempfile::tempdir().unwrap();
    let workspace = home.path().join("supervisor-workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("source.txt"), "EPIC_SOURCE_EVIDENCE\n").unwrap();
    let probe = Arc::new(Probe::new(workspace.clone()));
    let state = host(home.path(), probe.clone()).await;
    let service = SupervisorSessionService::new(state.session_store.clone());
    let receipt = service.get_or_create_default(PARENT).await.unwrap();
    let reference = SupervisorReference::from(&receipt);
    let parent_id = &reference.session_id;
    let mut parent = load(&state, parent_id).await;
    parent.model_ref = Some(ProviderModelRef::new(PROVIDER, PARENT));
    parent.set_provider_name(PROVIDER);
    parent.workspace = Some(workspace.to_string_lossy().into());
    parent.set_workspace_path_meta(workspace.to_string_lossy());
    parent
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .set_permission_mode(SessionPermissionMode::Bypass);
    state.storage.save_session(&parent).await.unwrap();
    let mut projects = BTreeSet::new();
    for (id, private) in [(ROOT_A, PRIVATE_A), (ROOT_B, PRIVATE_B)] {
        let project = state.project_store.create(id, None).unwrap();
        projects.insert(project.id.clone());
        let directory = home.path().join(id);
        std::fs::create_dir(&directory).unwrap();
        let mut root = Session::new(id, WORKER);
        root.set_project_id_meta(project.id.to_string());
        root.model_ref = Some(ProviderModelRef::new(PROVIDER, WORKER));
        root.set_provider_name(PROVIDER);
        root.workspace = Some(directory.to_string_lossy().into());
        root.set_workspace_path_meta(directory.to_string_lossy());
        root.agent_runtime_state
            .get_or_insert_with(Default::default)
            .set_permission_mode(SessionPermissionMode::Bypass);
        root.add_message(Message::user(private));
        if id == ROOT_A {
            root.pending_question = Some(PendingQuestion {
                tool_call_id: "existing-owner-question".into(),
                tool_name: "Pause".into(),
                question: "Which bounded Root A approach is authorized?".into(),
                options: vec!["A".into(), "B".into()],
                allow_custom: true,
                source: PendingQuestionSource::PauseTool,
            });
        }
        state.storage.save_session(&root).await.unwrap();
    }
    service
        .configure_project_scope(&reference, 0, projects)
        .await
        .unwrap();
    service.attach(&reference, 1, ROOT_A).await.unwrap();
    service.attach(&reference, 2, ROOT_B).await.unwrap();
    let original_a = load(&state, ROOT_A).await;
    let original_b = load(&state, ROOT_B).await;
    let parent = load(&state, parent_id).await;
    let parent_identity = context_identity(&parent);
    let creator = state.supervisor_observer_creator();
    let (child_workspace, workspace_source) = creator
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    create_child_action(&creator, CreateChildInput { parent_session:parent, child_id:OBSERVER.into(),
        title:"Finite Epic tracker".into(), responsibility:"Observe only assigned bounded state".into(),
        assignment_prompt:"Read the supplied immutable views; report blockers without controlling either Root.".into(),
        subagent_type:"tracker".into(), workspace:child_workspace, workspace_source,
        model_override:None, model_ref_override:Some(ProviderModelRef::new(PROVIDER, TRACKER)),
        runtime_metadata:HashMap::new(), read_only:true, auto_run:false, reasoning_effort:None,
        lifecycle:None, resident_name:None, resident_context:None, disabled_tools:None, context_fork:None }).await.unwrap();
    let revision = state
        .storage
        .inspect_supervisor_scope(&reference)
        .await
        .unwrap()
        .state_revision;
    state
        .subscribe_supervisor_observer(super::observer_subscriptions::ObserverSubscriptionStart {
            supervisor: reference.clone(),
            expected_state_revision: revision,
            operation_id: Uuid::new_v4(),
            child_id: OBSERVER.into(),
            targets: BTreeSet::from([ROOT_A.into(), ROOT_B.into()]),
            filter: BTreeSet::from([ObserverChangeKind::Status, ObserverChangeKind::NeedsInput]),
            min_interval_ms: 1000,
        })
        .await
        .unwrap();
    tokio::time::timeout(WAIT, probe.tracker_entered.notified())
        .await
        .unwrap();
    user(&state, parent_id, USER).await;
    let idle = wait(&state, parent_id, |s| {
        s.last_run_status().as_deref() == Some("completed")
            && marker(s, "EPIC_IDLE_PARENT_FINISHED_WITH_ACTIVE_TRACKER")
    })
    .await;
    assert!(idle
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 0);
    probe.tracker_release.notify_one();
    tokio::time::timeout(WAIT, probe.planner_entered.notified())
        .await
        .unwrap();
    let planning = wait(&state, parent_id, |s| {
        s.agent_runtime_state
            .as_ref()
            .is_some_and(|r| r.waiting_for_children.is_some())
    })
    .await;
    let planner_wait = planning
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .clone();
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 0);
    quiet(&state, OBSERVER).await;
    state
        .observer_subscriptions
        .tick(chrono::Utc::now() + chrono::Duration::seconds(2))
        .await;
    let during = wait(&state, parent_id, |s| marker(s, DURING_PLAN)).await;
    assert_eq!(
        during
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .waiting_for_children,
        planner_wait
    );
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *probe.observed.lock().unwrap(),
        BTreeSet::from([ROOT_A.into(), ROOT_B.into()])
    );
    assert_eq!(
        serde_json::to_value(load(&state, ROOT_A).await).unwrap(),
        serde_json::to_value(&original_a).unwrap()
    );
    assert_eq!(
        serde_json::to_value(load(&state, ROOT_B).await).unwrap(),
        serde_json::to_value(&original_b).unwrap()
    );
    probe.planner_release.notify_one();
    let finished = wait(&state, parent_id, |s| {
        s.last_run_status().as_deref() == Some("completed") && marker(s, DONE)
    })
    .await;
    let worker = wait(&state, ROOT_B, |s| {
        s.last_run_status().as_deref() == Some("completed")
            && marker(s, "EPIC_ORIGINAL_WORKER_FINISHED")
    })
    .await;
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 3);
    assert_eq!(context_identity(&worker), context_identity(&original_b));
    assert!(!workspace.join("must-not-exist.txt").exists());
    assert!(finished
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    let selector =
        artifact(&finished.messages).expect("canonical artifact reference")["read"].clone();
    let planner_id = selector["child_session_id"].as_str().unwrap();
    let actual_planner = load(&state, planner_id).await;
    assert!(
        actual_planner
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .read_only
    );
    assert_eq!(
        actual_planner.parent_session_id.as_deref(),
        Some(parent_id.as_str())
    );
    let children: BTreeSet<_> = state
        .storage
        .list_child_run_statuses(parent_id)
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        children,
        BTreeSet::from([OBSERVER.to_string(), planner_id.to_string()])
    );
    assert_eq!(context_identity(&finished), parent_identity);
    assert!(finished
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .plan_mode
        .is_none());
    let incarnation = finished.supervisor_incarnation_id();
    // Reconcile the genuine worker run change before freezing the cursor. A
    // post-followup observation is meaningful; it is not a duplicate old event.
    for n in 1..=3 {
        state
            .observer_subscriptions
            .tick(chrono::Utc::now() + chrono::Duration::seconds(n * 3))
            .await;
        quiet(&state, OBSERVER).await;
        quiet(&state, parent_id).await;
    }
    let before_restart = load(&state, parent_id).await;
    let checkpoint = subscription(&before_restart);
    let calls = probe.requests.load(Ordering::SeqCst);
    state.shutdown().await;
    drop(state);
    let reopened = host(home.path(), probe.clone()).await;
    crate::app_state::wake_reconciler::reconcile_startup(
        reopened.session_store.clone(),
        reopened.session_activation_router.clone(),
        reopened.mcp_proxy_shutdown.clone(),
    )
    .await;
    reopened
        .observer_subscriptions
        .tick(chrono::Utc::now() + chrono::Duration::minutes(1))
        .await;
    let restored = load(&reopened, parent_id).await;
    assert_eq!(restored.supervisor_incarnation_id(), incarnation);
    assert_eq!(subscription(&restored), checkpoint);
    assert_eq!(
        serde_json::to_value(&restored.supervisor_management).unwrap(),
        serde_json::to_value(&before_restart.supervisor_management).unwrap()
    );
    assert_eq!(artifact(&restored.messages).unwrap()["read"], selector);
    assert_eq!(
        probe.requests.load(Ordering::SeqCst),
        calls,
        "startup must not replay acknowledged work"
    );
    user(&reopened, parent_id, COLD).await;
    let cold = wait(&reopened, parent_id, |s| {
        s.last_run_status().as_deref() == Some("completed") && marker(s, COLD_DONE)
    })
    .await;
    assert_eq!(artifact(&cold.messages).unwrap()["read"], selector);
    assert_eq!(context_identity(&cold), parent_identity);
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        cold.messages.iter().filter(|m| m.content == USER).count(),
        1
    );
    assert_eq!(
        cold.messages.iter().filter(|m| m.content == COLD).count(),
        1
    );
    assert_eq!(
        context_identity(&load(&reopened, ROOT_B).await),
        context_identity(&original_b)
    );
    assert_eq!(
        serde_json::to_value(load(&reopened, ROOT_A).await).unwrap(),
        serde_json::to_value(original_a).unwrap()
    );
    assert!(cold
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    reopened.shutdown().await;
}
