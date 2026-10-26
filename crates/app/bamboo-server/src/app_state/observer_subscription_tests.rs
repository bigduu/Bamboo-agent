//! Real V2, immutable publication, Maildir and native Host activation. The
//! ordinary Work wait below is deliberately not a stand-in for Planner #1059.
use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{
    Message, ObserverStageOutcome, ObserverViewChange, PermissionMode, ProviderModelRef, Session,
    SessionInboxPort,
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
const OBSERVER: &str = "subscription-observer";
const ROOT_A: &str = "subscription-source-a";
const ROOT_B: &str = "subscription-source-b";
const PRIVATE: &str = "SOURCE_PRIVATE_TRANSCRIPT";
const WAIT: Duration = Duration::from_secs(40);

#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    parent_calls: AtomicUsize,
    observed: StdMutex<Vec<ObserverViewChange>>,
    hold_reader: AtomicBool,
    reader_entered: Notify,
    reader_release: Notify,
    work_entered: Notify,
    work_release: Notify,
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
        assert!(messages
            .iter()
            .all(|message| !message.content.contains(PRIVATE)));
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
                    assert!(result["content"]
                        .as_str()
                        .unwrap()
                        .contains(&format!("- Root: {}", change.target_session_id)));
                    self.observed.lock().unwrap().push(change.clone());
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
            PARENT => {
                self.parent_calls.fetch_add(1, Ordering::SeqCst);
                LLMChunk::Token("Parent completed its own Work.".into())
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
        parent
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .set_permission_mode(PermissionMode::BypassPermissions);
        state.storage.save_session(&parent).await.unwrap();
        let mut projects = BTreeSet::new();
        for id in [ROOT_A, ROOT_B] {
            let project = state.project_store.create(id, None).unwrap();
            projects.insert(project.id.clone());
            let mut target = Session::new(id, "never-source-model");
            target.set_project_id_meta(project.id.to_string());
            target.add_message(Message::user(PRIVATE));
            state.storage.save_session(&target).await.unwrap();
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
        self.child(OBSERVER, true, READER).await;
        let receipt = self
            .state
            .session_store
            .register_supervisor_observer(
                &self.reference,
                self.revision().await,
                Uuid::new_v4(),
                OBSERVER,
                BTreeSet::from([ROOT_A.into(), ROOT_B.into()]),
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
    async fn publish(&self, target: &str) -> Value {
        let child = load(&self.state, OBSERVER).await;
        let mut ctx = ToolCtx::none("subscription-test");
        ctx.session_id = Some(Arc::from(child.id.as_str()));
        ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(&child)));
        let view = self
            .state
            .observer_subscriptions
            .inspector
            .observe_context(&ctx, target, &self.subscription().await.filter, None)
            .await
            .unwrap()
            .unwrap();
        serde_json::from_str(&view.result).unwrap()
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
    assert_eq!(inbox.backlog(OBSERVER).await.unwrap().generation, 0);
    inbox.interrupt_next_observer_cursor_for_testing();
    assert!(inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    let physical = inbox.backlog(OBSERVER).await.unwrap();
    assert!(physical.activation_generation > 0);
    assert_eq!(f.subscription().await.window.enqueued_sequence, 0);
    let blocked = f.home.path().join("coordination/observer-changes");
    std::fs::write(&blocked, b"blocked physical shard publication").unwrap();
    assert!(inbox
        .deliver_supervisor_observation(&f.reference, OBSERVER, now)
        .await
        .is_err());
    assert_eq!(f.subscription().await.window.enqueued_sequence, 0);
    assert_eq!(inbox.backlog(OBSERVER).await.unwrap(), physical);
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
    assert_eq!(recovered.backlog(OBSERVER).await.unwrap(), physical);
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
    let backlog = f.state.session_inbox.backlog(OBSERVER).await.unwrap();
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
    let before = f.state.session_inbox.backlog(OBSERVER).await.unwrap();
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
        f.state.session_inbox.backlog(OBSERVER).await.unwrap(),
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
    f.child(OBSERVER, true, READER).await;
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
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
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
    assert_eq!(inbox.backlog(OBSERVER).await.unwrap().claimed, 1);
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
    let mut source = load(&f.state, ROOT_A).await;
    source.pending_question = Some(bamboo_domain::PendingQuestion {
        tool_call_id: "question-1".into(),
        tool_name: "AskUser".into(),
        question: "private owner question".into(),
        options: Vec::new(),
        allow_custom: true,
        source: Default::default(),
    });
    source.updated_at += chrono::Duration::seconds(1);
    source
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .round
        .total_prompt_tokens += 9;
    f.state.storage.save_session(&source).await.unwrap();
    let child = load(&f.state, OBSERVER).await;
    let mut ctx = ToolCtx::none("filter-test");
    ctx.session_id = Some(Arc::from(child.id.as_str()));
    ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(&child)));
    let filter = BTreeSet::from([ObserverChangeKind::Status]);
    assert!(f
        .state
        .observer_subscriptions
        .inspector
        .observe_context(
            &ctx,
            ROOT_A,
            &filter,
            initial["source_fingerprint"].as_str()
        )
        .await
        .unwrap()
        .is_none());
    source.agent_runtime_state.as_mut().unwrap().execution_id = Some("new-run-same-status".into());
    f.state.storage.save_session(&source).await.unwrap();
    assert!(f
        .state
        .observer_subscriptions
        .inspector
        .observe_context(
            &ctx,
            ROOT_A,
            &filter,
            initial["source_fingerprint"].as_str()
        )
        .await
        .unwrap()
        .is_some());
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
}
