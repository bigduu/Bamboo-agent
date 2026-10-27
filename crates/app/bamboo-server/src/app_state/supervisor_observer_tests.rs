//! Real Creator -> native provider -> committed Child source -> FileInbox.
//! A held ordinary work Child exercises the unrelated wait. It is not a
//! manufactured Planner or evidence for #1059's unfinished artifact adapter.

use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, Tool, ToolCall, ToolCtx, ToolSchema};
use bamboo_domain::{
    ActorAncestorObservation, ActorDirectoryPort, HostScopeSessionObservation, HostSessionRight,
    HostSessionScopeBinding, HostSessionTargetScope, Message, ProviderModelRef, Session,
    SessionCallerReference, SessionMessageEnvelope, SessionPermissionMode,
    HOST_SESSION_SCOPE_VERSION,
};
use bamboo_engine::execution::{ChildCompletion, ChildCompletionHandler, ChildCompletionSource};
use bamboo_engine::session_app::child_session::{create_child_action, CreateChildInput};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_server_tools::SessionInspectorTool;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

const PROVIDER: &str = "observer-fixture";
const PARENT_MODEL: &str = "observer-parent";
const OBSERVER_MODEL: &str = "observer-reader";
const FAILURE_MODEL: &str = "observer-failed-source";
const FAILURE: &str = "CONTROLLED_OBSERVER_FAILURE";
const WORK_MODEL: &str = "observer-held-work";
const ROOT_A: &str = "observer-source-a";
const ROOT_B: &str = "observer-source-b";
const OBSERVER: &str = "registered-observer";
const WORK: &str = "held-ordinary-work";
const REPORT: &str = "TRACKER_BLOCKER: root A needs owner input; root B is ready.";
const USER: &str = "USER_INPUT_REMAINS_CANONICAL";
const PRIVATE: &str = "PRIVATE_ROOT_TRANSCRIPT_MUST_NOT_BE_EXPORTED";
const WAIT: Duration = Duration::from_secs(40);

fn call(name: &str, id: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    }
}

struct Probe {
    turns: AtomicUsize,
    parent: Mutex<Vec<Vec<Message>>>,
    entered: Notify,
    release: Notify,
    work_entered: Notify,
    work_release: Notify,
    attack: Option<ToolCall>,
}

impl Probe {
    fn new(attack: Option<ToolCall>) -> Self {
        Self {
            turns: AtomicUsize::new(0),
            parent: Mutex::new(Vec::new()),
            entered: Notify::new(),
            release: Notify::new(),
            work_entered: Notify::new(),
            work_release: Notify::new(),
            attack,
        }
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
        assert!(!messages
            .iter()
            .any(|message| message.content.contains(PRIVATE)));
        let chunk = match model {
            PARENT_MODEL => {
                self.parent.lock().unwrap().push(messages.to_vec());
                LLMChunk::Token("Supervisor processed its canonical input.".into())
            }
            WORK_MODEL => {
                self.work_entered.notify_one();
                self.work_release.notified().await;
                LLMChunk::Token("Original work completed after its own release.".into())
            }
            FAILURE_MODEL => {
                self.turns.fetch_add(1, Ordering::SeqCst);
                return Err(LLMError::Auth(FAILURE.into()));
            }
            OBSERVER_MODEL => {
                let names = tools
                    .iter()
                    .map(|tool| tool.function.name.as_str())
                    .collect::<BTreeSet<_>>();
                assert_eq!(
                    names,
                    BTreeSet::from(["session_history", "session_history_current"])
                );
                let turn = self.turns.fetch_add(1, Ordering::SeqCst);
                if turn == 0 {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                if let Some(attack) = &self.attack {
                    if turn == 0 {
                        LLMChunk::ToolCalls(vec![attack.clone()])
                    } else {
                        let result = messages
                            .iter()
                            .find(|message| {
                                message.tool_call_id.as_deref() == Some(attack.id.as_str())
                            })
                            .expect("runtime must retain the denied tool result");
                        assert_eq!(result.tool_success, Some(false), "{}", result.content);
                        LLMChunk::Token("The restricted action was denied.".into())
                    }
                } else {
                    let revision = |id: &str| -> Value {
                        let output = messages
                            .iter()
                            .find(|message| message.tool_call_id.as_deref() == Some(id))
                            .expect("actual scoped export result");
                        assert_ne!(output.tool_success, Some(false), "{}", output.content);
                        serde_json::from_str::<Value>(&output.content).unwrap()["revision"].clone()
                    };
                    match turn {
                        0 => LLMChunk::ToolCalls(vec![call(
                            "session_history",
                            "view-a",
                            json!({"action":"export_context","session_id":ROOT_A}),
                        )]),
                        1 => LLMChunk::ToolCalls(vec![call(
                            "session_history",
                            "read-a",
                            json!({"action":"read_context","session_id":ROOT_A,"revision":revision("view-a"),
                                "file":"status","offset":11,"limit":1}),
                        )]),
                        2 => {
                            let output = messages
                                .iter()
                                .find(|m| m.tool_call_id.as_deref() == Some("read-a"))
                                .unwrap();
                            assert_ne!(output.tool_success, Some(false), "{}", output.content);
                            assert!(output.content.contains("ROOT-A-BLOCKED"));
                            LLMChunk::ToolCalls(vec![call(
                                "session_history",
                                "view-b",
                                json!({"action":"export_context","session_id":ROOT_B}),
                            )])
                        }
                        3 => LLMChunk::ToolCalls(vec![call(
                            "session_history",
                            "read-b",
                            json!({"action":"read_context","session_id":ROOT_B,"revision":revision("view-b"),
                                "file":"status","offset":11,"limit":1}),
                        )]),
                        4 => {
                            let output = messages
                                .iter()
                                .find(|m| m.tool_call_id.as_deref() == Some("read-b"))
                                .unwrap();
                            assert_ne!(output.tool_success, Some(false), "{}", output.content);
                            assert!(output.content.contains("ROOT-B-READY"));
                            LLMChunk::Token(REPORT.into())
                        }
                        _ => panic!("a short Observer must stop after its bounded report"),
                    }
                }
            }
            _ => panic!("unexpected model route: {model}"),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(10),
                output_tokens: Some(5),
                total_tokens: Some(15),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn state(home: &std::path::Path, probe: Arc<Probe>) -> AppState {
    let mut config = Config::from_data_dir(Some(home.into()));
    config.provider = PROVIDER.into();
    config.subagents_mut().max_concurrent = Some(2);
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = AppState::new_with_provider(home.into(), config, probe.clone())
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
    targets: Vec<Session>,
}

impl Fixture {
    async fn new(attack: Option<ToolCall>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::new(attack));
        let state = state(home.path(), probe.clone()).await;
        let service = SupervisorSessionService::new(state.session_store.clone());
        let reference =
            SupervisorReference::from(&service.get_or_create_default(PARENT_MODEL).await.unwrap());
        let workspace = home.path().join("supervisor-workspace");
        std::fs::create_dir(&workspace).unwrap();
        let mut parent = state
            .storage
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        parent.model_ref = Some(ProviderModelRef::new(PROVIDER, PARENT_MODEL));
        parent.set_provider_name(PROVIDER);
        parent.set_workspace_path_meta(workspace.to_string_lossy());
        parent.workspace = Some(workspace.to_string_lossy().into());
        parent
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .set_permission_mode(SessionPermissionMode::Bypass);
        state.storage.save_session(&parent).await.unwrap();
        let mut projects = BTreeSet::new();
        let mut targets = Vec::new();
        for (id, title) in [(ROOT_A, "ROOT-A-BLOCKED"), (ROOT_B, "ROOT-B-READY")] {
            let project = state.project_store.create(title, None).unwrap();
            projects.insert(project.id.clone());
            let mut target = Session::new(id, "unrun-source-model");
            target.title = title.into();
            target.set_project_id_meta(project.id.to_string());
            target.add_message(Message::user(PRIVATE));
            state.storage.save_session(&target).await.unwrap();
            targets.push(target);
        }
        service
            .configure_project_scope(&reference, 0, projects)
            .await
            .unwrap();
        service.attach(&reference, 1, ROOT_A).await.unwrap();
        service.attach(&reference, 2, ROOT_B).await.unwrap();
        Self {
            home,
            state,
            probe,
            reference,
            targets,
        }
    }

    async fn child(&self, id: &str, read_only: bool, model: &str) -> Session {
        let parent = self
            .state
            .storage
            .load_session(&self.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let creator = self.state.supervisor_observer_creator();
        let (workspace, source) = creator
            .resolve_child_workspace(&parent, None)
            .await
            .unwrap();
        create_child_action(&creator, CreateChildInput {
            parent_session: parent.clone(), child_id: id.into(), title: "Bounded observation".into(),
            responsibility: "Report only scoped observations".into(),
            assignment_prompt: "Read the two fixed Root status views and report their blocker; do not act on it.".into(),
            subagent_type: "tracker".into(), workspace, workspace_source: source,
            model_override: None, model_ref_override: Some(ProviderModelRef::new(PROVIDER, model)),
            runtime_metadata: HashMap::new(), read_only, auto_run: false,
            reasoning_effort: None, lifecycle: None, resident_name: None, resident_context: None,
            disabled_tools: None, context_fork: None,
        }).await.unwrap();
        self.state
            .storage
            .load_child_wait_session(&parent, id, true)
            .await
            .unwrap()
            .unwrap()
    }

    fn targets(&self) -> BTreeSet<String> {
        self.targets
            .iter()
            .map(|target| target.id.clone())
            .collect()
    }

    async fn revision(&self) -> u64 {
        self.state
            .storage
            .inspect_supervisor_scope(&self.reference)
            .await
            .unwrap()
            .state_revision
    }
}

async fn saved(state: &AppState, id: &str) -> Session {
    state.storage.load_session(id).await.unwrap().unwrap()
}

async fn wait_saved(state: &AppState, id: &str, predicate: impl Fn(&Session) -> bool) -> Session {
    let settled = tokio::time::timeout(WAIT, async {
        loop {
            let session = saved(state, id).await;
            if predicate(&session) {
                return session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match settled {
        Ok(session) => session,
        Err(_) => {
            let current = saved(state, id).await;
            let child = state.storage.load_session(OBSERVER).await.unwrap();
            let child_diagnostic = child.as_ref().map(|child| {
                let final_message = child
                    .messages
                    .iter()
                    .rev()
                    .find(|message| matches!(message.role, bamboo_agent_core::Role::Assistant))
                    .map(|message| {
                        (
                            format!("{:?}", message.phase),
                            message.content.chars().take(256).collect::<String>(),
                        )
                    });
                (
                    child.last_run_status(),
                    child.last_run_error(),
                    child.metadata.get("runtime.completion_reason"),
                    source(child),
                    final_message,
                    child.agent_runtime_state.as_ref().map(|runtime| {
                        (
                            runtime.round.total_tool_calls,
                            runtime.round.total_prompt_tokens,
                        )
                    }),
                )
            });
            panic!("canonical state did not settle for {id}; status={:?}; error={:?}; wait={:?}; user_seen={}; report_seen={}; child={child_diagnostic:?}",
                current.last_run_status(), current.last_run_error(),
                current.agent_runtime_state.as_ref().and_then(|runtime| runtime.waiting_for_children.as_ref()),
                current.messages.iter().any(|message| message.content.contains(USER)),
                current.messages.iter().any(|message| message.content.contains(REPORT)));
        }
    }
}

fn source(session: &Session) -> Option<ChildCompletionSource> {
    serde_json::from_str(session.metadata.get("runtime.child_completion_source_v1")?).ok()
}

fn complete(session: &Session) -> bool {
    session.last_run_status().as_deref() == Some("completed") && source(session).is_some()
}

async fn native_case(with_work_wait: bool) {
    let f = Fixture::new(None).await;
    let parent_id = &f.reference.session_id;
    if !with_work_wait {
        let mut parent = saved(&f.state, parent_id).await;
        parent
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .set_permission_mode(SessionPermissionMode::Auto);
        f.state.storage.save_session(&parent).await.unwrap();
    }
    let wait = if with_work_wait {
        let work = f.child(WORK, false, WORK_MODEL).await;
        let parent = saved(&f.state, parent_id).await;
        let creator = f.state.supervisor_observer_creator();
        creator
            .register_parent_wait_for_child(parent_id, WORK, Some("real-work-wait"))
            .await
            .unwrap();
        creator.enqueue_child_run(&parent, &work).await.unwrap();
        tokio::time::timeout(WAIT, f.probe.work_entered.notified())
            .await
            .unwrap();
        saved(&f.state, parent_id)
            .await
            .agent_runtime_state
            .unwrap()
            .waiting_for_children
    } else {
        None
    };
    f.child(OBSERVER, true, OBSERVER_MODEL).await;
    let operation = Uuid::new_v4();
    f.state
        .start_supervisor_observer(
            &f.reference,
            f.revision().await,
            operation,
            OBSERVER,
            f.targets(),
        )
        .await
        .unwrap();
    tokio::time::timeout(WAIT, f.probe.entered.notified())
        .await
        .unwrap();
    // Match normal chat admission: a staged coordinator message alone does
    // not authorize a user input or wake an idle parent. Respect the existing
    // Work wait while retaining the input for its eventual safe consumption.
    let user_admission = f
        .state
        .session_messenger
        .admit_with_activation_intent(
            SessionMessageEnvelope::user_input(parent_id, USER),
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    f.state
        .session_messenger
        .activate_prepared(&user_admission)
        .await
        .unwrap();
    if !with_work_wait {
        let parent = wait_saved(&f.state, parent_id, |s| {
            s.last_run_status().as_deref() == Some("completed")
        })
        .await;
        assert!(
            parent
                .agent_runtime_state
                .unwrap()
                .waiting_for_children
                .is_none(),
            "the real active Observer must not stall the parent"
        );
        assert_eq!(f.probe.turns.load(Ordering::SeqCst), 1);
    }
    f.probe.release.notify_one();
    let observer = wait_saved(&f.state, OBSERVER, complete).await;
    let original_source = source(&observer).unwrap();
    assert_eq!(f.probe.turns.load(Ordering::SeqCst), 5,
        "the completed Observer must execute its two exports, two reads and final report; source={original_source:?}");
    let sealed_report = observer
        .messages
        .iter()
        .find(|message| Some(&message.id) == original_source.result_message_id.as_ref())
        .expect("the current terminal source must seal the actual Observer report");
    assert!(
        sealed_report.content.contains(REPORT),
        "the sealed Observer result must retain its report: {}",
        sealed_report.content
    );
    let parent = wait_saved(&f.state, parent_id, |parent| {
        parent
            .messages
            .iter()
            .any(|message| message.content.contains(REPORT))
            && parent.last_run_status().as_deref()
                == Some(if with_work_wait {
                    "suspended"
                } else {
                    "completed"
                })
    })
    .await;
    assert_eq!(
        parent
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .waiting_for_children,
        wait
    );
    assert_eq!(
        f.probe.turns.load(Ordering::SeqCst),
        5,
        "ordinary sequential two-Root reads must fit the Observer budget"
    );
    assert!(observer.authority_identity.is_ordinary());
    assert!(observer.agent_runtime_state.as_ref().unwrap().read_only);
    assert_eq!(
        observer
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .total_prompt_tokens,
        50
    );
    assert_eq!(
        observer
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .total_completion_tokens,
        25
    );
    assert_eq!(
        observer
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .total_tool_calls,
        4
    );
    assert!(!parent.agent_runtime_state.as_ref().unwrap().read_only);
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .plan_mode
        .is_none());
    assert_eq!(observer.parent_created_at, Some(parent.created_at));
    assert_eq!(original_source.child_created_at, observer.created_at);
    if with_work_wait {
        assert!(f
            .state
            .session_activation_router
            .current_run_id(WORK)
            .await
            .is_some());
        f.probe.work_release.notify_one();
        wait_saved(&f.state, WORK, complete).await;
        wait_saved(&f.state, parent_id, |parent| {
            parent.last_run_status().as_deref() == Some("completed")
                && parent
                    .messages
                    .iter()
                    .any(|message| message.content.contains(USER))
        })
        .await;
    }
    let retry = f
        .state
        .start_supervisor_observer(&f.reference, 0, operation, OBSERVER, f.targets())
        .await
        .unwrap();
    assert!(!retry.changed);
    assert_eq!(f.probe.turns.load(Ordering::SeqCst), 5);
    for target in &f.targets {
        assert_eq!(
            serde_json::to_value(saved(&f.state, &target.id).await).unwrap(),
            serde_json::to_value(target).unwrap(),
            "read-only views must preserve the original Root"
        );
    }
    let parent = saved(&f.state, parent_id).await;
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(USER))
            .count(),
        1
    );
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(REPORT))
            .count(),
        1
    );
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert_eq!(
        f.state
            .session_inbox
            .inspect(parent_id)
            .await
            .unwrap()
            .pending,
        0
    );
    let before = f.probe.parent.lock().unwrap().len();
    let Fixture {
        home,
        state: old,
        probe,
        reference,
        ..
    } = f;
    old.shutdown().await;
    drop(old);
    let reopened = state(home.path(), probe.clone()).await;
    reopened.wait_for_boot_reconcile_services().await;
    let generation = reopened
        .session_inbox
        .inspect(&reference.session_id)
        .await
        .unwrap()
        .generation;
    reopened
        .child_completion_coordinator
        .on_child_completed(ChildCompletion {
            parent_session_id: reference.session_id.clone(),
            child_session_id: OBSERVER.into(),
            status: original_source.status.clone(),
            error: None,
            completed_at: chrono::Utc::now(),
            source: Some(original_source),
        })
        .await;
    assert_eq!(
        reopened
            .session_inbox
            .inspect(&reference.session_id)
            .await
            .unwrap()
            .generation,
        generation
    );
    assert_eq!(probe.parent.lock().unwrap().len(), before);
    let parent = saved(&reopened, &reference.session_id).await;
    assert!(reopened
        .storage
        .is_supervisor_observer_child(&parent, OBSERVER)
        .await
        .unwrap());
    assert_eq!(probe.turns.load(Ordering::SeqCst), 5);
    reopened.shutdown().await;
}

#[tokio::test]
async fn native_observer_reads_two_projects_and_never_holds_idle_parent() {
    native_case(false).await;
}

#[tokio::test]
async fn native_observer_result_preserves_live_work_wait_and_user_input() {
    native_case(true).await;
}

#[tokio::test]
async fn failed_observer_reports_without_wait_and_restart_never_replays_it() {
    let f = Fixture::new(None).await;
    f.child(OBSERVER, true, FAILURE_MODEL).await;
    let operation = Uuid::new_v4();
    f.state
        .start_supervisor_observer(
            &f.reference,
            f.revision().await,
            operation,
            OBSERVER,
            f.targets(),
        )
        .await
        .unwrap();
    let child = wait_saved(&f.state, OBSERVER, |child| {
        child.last_run_status().as_deref() == Some("error") && source(child).is_some()
    })
    .await;
    let failed_source = source(&child).unwrap();
    assert_eq!(failed_source.status, "error");
    let failed_error = child
        .last_run_error()
        .expect("canonical failed source error");
    assert!(failed_error.contains(FAILURE));
    let parent = wait_saved(&f.state, &f.reference.session_id, |parent| {
        parent.last_run_status().as_deref() == Some("completed")
            && parent
                .messages
                .iter()
                .any(|message| message.content.contains(FAILURE))
    })
    .await;
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(FAILURE))
            .count(),
        1
    );
    assert_eq!(f.probe.turns.load(Ordering::SeqCst), 1);
    let retry = f
        .state
        .start_supervisor_observer(&f.reference, 0, operation, OBSERVER, f.targets())
        .await
        .unwrap();
    assert!(!retry.changed);
    assert_eq!(f.probe.turns.load(Ordering::SeqCst), 1);
    let Fixture {
        home,
        state: old,
        probe,
        reference,
        ..
    } = f;
    old.shutdown().await;
    drop(old);
    let reopened = state(home.path(), probe.clone()).await;
    reopened.wait_for_boot_reconcile_services().await;
    let generation = reopened
        .session_inbox
        .inspect(&reference.session_id)
        .await
        .unwrap()
        .generation;
    reopened
        .child_completion_coordinator
        .on_child_completed(ChildCompletion {
            parent_session_id: reference.session_id.clone(),
            child_session_id: OBSERVER.into(),
            status: failed_source.status.clone(),
            error: Some(failed_error),
            completed_at: chrono::Utc::now(),
            source: Some(failed_source),
        })
        .await;
    assert_eq!(
        reopened
            .session_inbox
            .inspect(&reference.session_id)
            .await
            .unwrap()
            .generation,
        generation
    );
    assert_eq!(
        saved(&reopened, &reference.session_id)
            .await
            .messages
            .iter()
            .filter(|message| message.content.contains(FAILURE))
            .count(),
        1
    );
    assert_eq!(probe.turns.load(Ordering::SeqCst), 1);
    reopened.shutdown().await;
}

#[tokio::test]
async fn active_observer_cancel_drains_its_owner_without_cancelling_work() {
    let f = Fixture::new(None).await;
    let parent = saved(&f.state, &f.reference.session_id).await;
    let work = f.child(WORK, false, WORK_MODEL).await;
    let creator = f.state.supervisor_observer_creator();
    creator
        .register_parent_wait_for_child(&parent.id, WORK, Some("owned-work-wait"))
        .await
        .unwrap();
    creator.enqueue_child_run(&parent, &work).await.unwrap();
    tokio::time::timeout(WAIT, f.probe.work_entered.notified())
        .await
        .unwrap();
    let wait = saved(&f.state, &parent.id)
        .await
        .agent_runtime_state
        .unwrap()
        .waiting_for_children;
    f.child(OBSERVER, true, OBSERVER_MODEL).await;
    f.state
        .start_supervisor_observer(
            &f.reference,
            f.revision().await,
            Uuid::new_v4(),
            OBSERVER,
            f.targets(),
        )
        .await
        .unwrap();
    tokio::time::timeout(WAIT, f.probe.entered.notified())
        .await
        .unwrap();
    creator.cancel_child_run_and_wait(OBSERVER).await.unwrap();
    let observer = wait_saved(&f.state, OBSERVER, |child| {
        child.last_run_status().as_deref() == Some("cancelled") && source(child).is_some()
    })
    .await;
    assert_eq!(
        observer
            .metadata
            .get("runtime.local_task.state")
            .map(String::as_str),
        Some("stopped")
    );
    assert_eq!(source(&observer).unwrap().status, "cancelled");
    assert_eq!(
        saved(&f.state, &parent.id)
            .await
            .agent_runtime_state
            .unwrap()
            .waiting_for_children,
        wait
    );
    assert!(f
        .state
        .session_activation_router
        .current_run_id(WORK)
        .await
        .is_some());
    assert_eq!(f.probe.turns.load(Ordering::SeqCst), 1);
    f.probe.work_release.notify_one();
    wait_saved(&f.state, WORK, complete).await;
    wait_saved(&f.state, &parent.id, |s| {
        s.last_run_status().as_deref() == Some("completed")
    })
    .await;
    f.state.shutdown().await;
}

fn reader_ctx(child: &Session) -> ToolCtx {
    let mut ctx = ToolCtx::none("observer-view");
    ctx.session_id = Some(Arc::from(child.id.as_str()));
    ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(child)));
    ctx
}

#[tokio::test]
async fn observer_grants_are_explicit_fresh_and_durably_revoked() {
    let f = Fixture::new(None).await;
    let writable = f.child("cosmetic-tracker", false, OBSERVER_MODEL).await;
    let parent = saved(&f.state, &f.reference.session_id).await;
    assert!(!f
        .state
        .storage
        .is_supervisor_observer_child(&parent, &writable.id)
        .await
        .unwrap());
    assert!(
        tokio::time::timeout(
            WAIT,
            f.state
                .storage
                .is_supervisor_observer_child(&parent, &parent.id)
        )
        .await
        .unwrap()
        .is_err(),
        "self IDs must fail before acquiring the same maintenance lock twice"
    );
    assert!(f
        .state
        .session_store
        .register_supervisor_observer(
            &f.reference,
            f.revision().await,
            Uuid::new_v4(),
            &writable.id,
            f.targets()
        )
        .await
        .is_err());
    let child = f.child(OBSERVER, true, OBSERVER_MODEL).await;
    let operation = Uuid::new_v4();
    let receipt = f
        .state
        .session_store
        .register_supervisor_observer(
            &f.reference,
            f.revision().await,
            operation,
            OBSERVER,
            f.targets(),
        )
        .await
        .unwrap();
    let creator = f.state.supervisor_observer_creator();
    assert!(creator
        .register_parent_wait_for_child(&f.reference.session_id, OBSERVER, None)
        .await
        .is_err());
    let tool =
        SessionInspectorTool::host_routed(f.state.session_store.clone(), f.state.storage.clone());
    let args = json!({"action":"export_context","session_id":ROOT_A});
    assert!(tool.invoke(args.clone(), reader_ctx(&child)).await.is_ok());
    let parent = saved(&f.state, &f.reference.session_id).await;
    f.state
        .session_store
        .install_host_session_scope(
            0,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: Uuid::new_v4(),
                revision: 1,
                caller: HostScopeSessionObservation::capture(
                    &child,
                    vec![ActorAncestorObservation {
                        actor_id: parent.id.clone(),
                        session_created_at: parent.created_at,
                        metadata_version: parent.metadata_version,
                    }],
                )
                .unwrap(),
                targets: f
                    .targets
                    .iter()
                    .map(|target| {
                        (
                            target.id.clone(),
                            HostSessionTargetScope {
                                observation: HostScopeSessionObservation::capture(target, vec![])
                                    .unwrap(),
                                rights: [HostSessionRight::ListMetadata].into(),
                            },
                        )
                    })
                    .collect(),
                creates: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(
        tool.invoke(args.clone(), reader_ctx(&child)).await.is_err(),
        "installed narrower Host scope must not fall back to the Observer registration"
    );
    f.state
        .session_store
        .revoke_host_session_scope(OBSERVER, 1)
        .await
        .unwrap();
    assert!(tool.invoke(args.clone(), reader_ctx(&child)).await.is_err());
    f.state
        .session_store
        .revoke_supervisor_observer(&f.reference, receipt.state_revision, OBSERVER)
        .await
        .unwrap();
    assert!(f
        .state
        .storage
        .is_supervisor_observer_child(&parent, OBSERVER)
        .await
        .unwrap());
    assert!(f
        .state
        .storage
        .supervisor_observer_activation(&child)
        .await
        .is_err());
    let Fixture {
        home,
        state: old,
        probe,
        reference,
        ..
    } = f;
    old.shutdown().await;
    drop(old);
    let reopened = state(home.path(), probe.clone()).await;
    let parent = saved(&reopened, &reference.session_id).await;
    assert!(reopened
        .storage
        .is_supervisor_observer_child(&parent, OBSERVER)
        .await
        .unwrap());
    let tool =
        SessionInspectorTool::host_routed(reopened.session_store.clone(), reopened.storage.clone());
    assert!(tool.invoke(args, reader_ctx(&child)).await.is_err());
    assert!(reopened
        .storage
        .supervisor_observer_activation(&child)
        .await
        .is_err());
    assert_eq!(probe.turns.load(Ordering::SeqCst), 0);
    reopened.shutdown().await;
}

#[tokio::test]
async fn native_observer_cannot_read_files_mutate_send_or_delegate() {
    for name in [
        "Read",
        "Write",
        "exec_command",
        "session_message",
        "SubAgent",
    ] {
        let f = Fixture::new(None).await;
        let path = f.home.path().join("private-outside-view.txt");
        std::fs::write(&path, PRIVATE).unwrap();
        let arguments = match name {
            "Read" => json!({"file_path":path}),
            "Write" => json!({"file_path":path,"content":"MUTATED"}),
            "exec_command" => json!({"cmd":format!("printf MUTATED > '{}'", path.display())}),
            "session_message" => json!({"action":"send","target_session_id":ROOT_A,
                "operation_id":"ungranted-send","message":"MUTATED"}),
            _ => json!({"action":"create","title":"ungranted spawn","task":"MUTATED"}),
        };
        let attack = call(name, "denied-observer-action", arguments);
        let Fixture {
            home,
            state: old,
            reference: _,
            targets: _,
            probe: _,
        } = f;
        old.shutdown().await;
        drop(old);
        let probe = Arc::new(Probe::new(Some(attack)));
        let live = state(home.path(), probe.clone()).await;
        let reference = SupervisorReference::from(
            &SupervisorSessionService::new(live.session_store.clone())
                .get_or_create_default(PARENT_MODEL)
                .await
                .unwrap(),
        );
        let f = Fixture {
            home,
            state: live,
            probe,
            reference,
            targets: vec![],
        };
        f.child(OBSERVER, true, OBSERVER_MODEL).await;
        let targets = BTreeSet::from([ROOT_A.into(), ROOT_B.into()]);
        f.state
            .start_supervisor_observer(
                &f.reference,
                f.revision().await,
                Uuid::new_v4(),
                OBSERVER,
                targets,
            )
            .await
            .unwrap();
        tokio::time::timeout(WAIT, f.probe.entered.notified())
            .await
            .unwrap();
        f.probe.release.notify_one();
        wait_saved(&f.state, OBSERVER, complete).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), PRIVATE);
        assert_eq!(
            f.state
                .session_inbox
                .inspect(ROOT_A)
                .await
                .unwrap()
                .generation,
            0
        );
        assert_eq!(
            f.state
                .session_store
                .inspect_actor(OBSERVER)
                .await
                .unwrap()
                .actor
                .current_attempt,
            1
        );
        assert_eq!(f.probe.turns.load(Ordering::SeqCst), 2);
        f.state.shutdown().await;
    }
}
