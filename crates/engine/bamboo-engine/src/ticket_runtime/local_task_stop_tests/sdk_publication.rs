//! The first Submission is produced only by the production SDK coordinator's
//! automatic owned-task-stop -> durable-delivery path. The recovery helper is
//! exercised only after that published result has been independently verified.
use super::*;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::ActorLogicalState;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast;

#[tokio::test]
async fn production_sdk_ticket_automatically_publishes_once_and_replays_after_restart() {
    let mut f = Fixture::new().await;
    f.child.add_message(Message::system("Return the result."));
    f.child.add_message(Message::user("Complete this Ticket."));
    f.storage.save_session(&f.child).await.unwrap();
    assert!(f.service.published().unwrap().1.submissions.is_empty());
    let provider = Arc::new(OneReply(AtomicUsize::new(0)));
    let tools: Arc<dyn ToolExecutor> = Arc::new(NoTools);
    let config = Config::default();
    let agent = Arc::new(
        Agent::builder()
            .storage(f.storage.clone())
            .persistence(f.runtime.persistence.clone())
            .session_inbox(f.runtime.inbox.clone())
            .activation_router(f.runtime.router.clone())
            .attachment_reader(f.storage.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    f.dir.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(RwLock::new(config.clone())))
            .provider(provider.clone())
            .default_tools(tools.clone())
            .build()
            .unwrap(),
    );
    let runner = Arc::new(LocalTaskChildRunner::new(
        &config,
        Some(Arc::new(NoToolCeiling)),
        None,
    ));
    runner.set_actor_directory_store(Some(f.storage.clone()));
    runner.set_session_inbox_runtime(Some(f.runtime.clone()));
    runner.set_ticket_service(Some(f.service.clone()));
    let sessions_cache = Arc::default();
    runner.set_local_task_runtime(Some(LocalTaskRuntime {
        agent: agent.clone(),
        tools: tools.clone(),
        provider_router: None,
        app_data_dir: Some(f.dir.path().join("host")),
        sessions_cache: Arc::clone(&sessions_cache),
    }));
    // A common bus preserves the actual order of the Child's durable history
    // barrier and the parent's terminal notification.
    let (events, mut receive) = broadcast::channel(256);
    let ctx = crate::execution::SpawnContext {
        agent,
        tools,
        sessions_cache,
        agent_runners: Arc::new(RwLock::new(HashMap::new())),
        session_event_senders: Arc::new(RwLock::new(HashMap::from([
            (f.supervisor.id.clone(), events.clone()),
            (f.child.id.clone(), events),
        ]))),
        external_child_runner: runner.clone(),
        provider_router: None,
        app_data_dir: Some(f.dir.path().join("host")),
        completion_handler: None,
        child_run_launch_hook: None,
        account_feed_inbox: None,
    };
    crate::sdk::spawn::run_child_spawn(
        ctx.clone(),
        SpawnJob {
            parent_session_id: f.supervisor.id.clone(),
            child_session_id: f.child.id.clone(),
            model: "fixture-model".into(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut agent_complete = false;
        let mut history_committed = false;
        loop {
            match receive.recv().await.unwrap() {
                AgentEvent::Complete { .. } => agent_complete = true,
                AgentEvent::SessionHistoryCommitted { session_id } => {
                    assert_eq!(session_id, f.child.id);
                    assert!(
                        agent_complete,
                        "actual Agent completion precedes the history barrier"
                    );
                    let canonical = f.storage.load_session(&f.child.id).await.unwrap().unwrap();
                    assert!(
                        crate::execution::ChildCompletionSource::from_committed_session(&canonical)
                            .is_some()
                    );
                    // No direct result checkpoint has run in this test. SDK
                    // commit_durable_child_delivery must already have published.
                    let published = f.service.published().unwrap().1;
                    assert_eq!(published.submissions.len(), 1);
                    assert_eq!(
                        published.tickets[&f.spec.work_id].state,
                        WorkState::Submitted
                    );
                    history_committed = true;
                }
                AgentEvent::SubAgentCompleted {
                    child_session_id,
                    status,
                    error,
                    ..
                } => {
                    assert_eq!(child_session_id, f.child.id);
                    assert_eq!(status, "completed", "{error:?}");
                    assert!(
                        history_committed,
                        "Ticket publication/history precede parent completion"
                    );
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the SDK must automatically publish the Ticket result");
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    let canonical = f.storage.load_session(&f.child.id).await.unwrap().unwrap();
    assert_eq!(canonical.last_run_status().as_deref(), Some("completed"));
    assert_eq!(canonical.messages.last().unwrap().content, OUTPUT);
    let source =
        crate::execution::ChildCompletionSource::from_committed_session(&canonical).unwrap();
    assert_eq!(
        source.result(&canonical, &f.supervisor).as_deref(),
        Some(OUTPUT)
    );
    let raw: serde_json::Value =
        serde_json::from_str(&canonical.metadata[TICKET_OWNED_STOP_KEY]).unwrap();
    let instance = uuid::Uuid::parse_str(raw["task_instance"].as_str().unwrap()).unwrap();
    assert!(!instance.is_nil());
    assert!(raw.get("pid").is_none());
    assert_eq!(raw["child_birth"], serde_json::json!(f.child.created_at));
    assert_eq!(raw["completed"], true);
    assert_eq!(raw["receipt"]["run_id"], source.activation_run_id);
    assert_eq!(
        canonical.metadata["runtime.local_task.instance"],
        instance.to_string()
    );
    assert_eq!(canonical.metadata["runtime.local_task.state"], "stopped");
    let snapshot = f.service.published().unwrap().1;
    assert_eq!(snapshot.submissions.len(), 1);
    let submission = snapshot.submissions.values().next().unwrap();
    let submission_id = submission.id.clone();
    let work_id = submission.work_id.clone();
    assert_eq!(submission.assignment_id, f.spec.assignment_id);
    assert_eq!(submission.runtime.run_id, source.activation_run_id);
    assert_eq!(submission.generation, f.spec.generation);
    assert!(!submission.stale);
    assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
    assert_eq!(
        snapshot.tickets[&f.spec.work_id].state,
        WorkState::Submitted
    );
    assert!(snapshot.tickets[&f.spec.work_id]
        .accepted_submission
        .is_none());
    let user = Authority::from_verified_host(
        f.spec.binding.clone(),
        Principal::User {
            user_id: "fixture-user".into(),
        },
    );
    assert_eq!(
        f.service
            .read_artifact(&user, &submission.artifacts[0], 65536)
            .unwrap(),
        OUTPUT.as_bytes()
    );
    let actor = f.storage.inspect_actor(&f.child.id).await.unwrap();
    assert_eq!(
        actor.activation.unwrap().status,
        ActorActivationStatus::Succeeded
    );
    assert_eq!(actor.actor.state, ActorLogicalState::Cold);
    assert!(f.runtime.router.current_run_id(&f.child.id).await.is_none());
    assert!(!ctx
        .agent_runners
        .read()
        .await
        .get(&f.child.id)
        .is_some_and(|entry| matches!(entry.status, crate::execution::AgentStatus::Running)));
    runner.shutdown().await.unwrap();
    drop(ctx);
    drop(runner);
    let binding = snapshot.binding.clone();
    let Fixture {
        dir,
        service,
        storage,
        runtime,
        ..
    } = f;
    drop(runtime);
    drop(storage);
    drop(service);
    let reopened_store = SessionStoreV2::new(dir.path().join("host")).await.unwrap();
    let restarted = TicketService::open(dir.path().join("tickets"), binding).unwrap();
    let replay_child = reopened_store
        .load_session(&canonical.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay_child.metadata[TICKET_OWNED_STOP_KEY],
        canonical.metadata[TICKET_OWNED_STOP_KEY]
    );
    let before_replay = restarted.published().unwrap();
    assert_eq!(
        before_replay.1.submissions.len(),
        1,
        "result existed before recovery helper"
    );
    assert!(before_replay.1.submissions.contains_key(&submission_id));
    let before_bytes = canonical_bytes(&before_replay).unwrap();
    // Recovery-only replay: initial publication above came from SDK assembly.
    // This does not claim to restart the complete application/SDK runtime.
    for _ in 0..2 {
        let replay = checkpoint_ticket_result(
            &restarted,
            &reopened_store,
            &replay_child,
            &source.activation_run_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(replay.ids["submission"], submission_id);
        assert_eq!(
            canonical_bytes(&restarted.published().unwrap()).unwrap(),
            before_bytes
        );
    }
    let replayed = restarted.published().unwrap().1;
    assert_eq!(replayed.submissions.len(), 1);
    assert_eq!(replayed.tickets[&work_id].state, WorkState::Submitted);
}
