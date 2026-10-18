//! Real queue backpressure must not preserve an obsolete Host resource check.
//! Capacity is held by actual Tokio permits, without synthetic jobs or runners.
use super::*;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::AdmissionCommit;
use bamboo_engine::events::journal;
use bamboo_orchestration::ready_recovery::{
    ReadyRetryKey, ReadyRetryPhase, ReadyRetryRequest, ReadyRetryStore,
};
use bamboo_server::app_state::ticket_application::FileReadyRetryStore;

const QUEUE_CAPACITY: usize = 128;

#[derive(Clone, Copy, Debug)]
enum ChangedResource {
    ArchivedProject,
    UnboundSelectedWorkspace,
    DifferentGitBranch,
    DifferentGitHead,
}

impl ChangedResource {
    fn change(self, fixture: &Fixture) {
        match self {
            Self::ArchivedProject => {
                let manifest = fixture.state.project_store.get(&fixture.project).unwrap();
                fixture
                    .state
                    .project_store
                    .archive(&fixture.project, manifest.revision)
                    .unwrap();
            }
            Self::UnboundSelectedWorkspace => {
                let manifest = fixture.state.project_store.get(&fixture.project).unwrap();
                fixture
                    .state
                    .project_store
                    .unbind_workspace(
                        &fixture.project,
                        manifest.revision,
                        &fixture.works[0].workspace.repo,
                    )
                    .unwrap();
            }
            Self::DifferentGitHead => {
                let workspace = &fixture.works[0].workspace;
                git(
                    Path::new(&workspace.worktree),
                    &[
                        "-c",
                        "user.name=Ready Work Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "commit",
                        "--allow-empty",
                        "-m",
                        "changed HEAD while waiting for queue capacity",
                    ],
                );
                // Keep the adopted branch so this independently exercises HEAD.
                assert_eq!(
                    git(
                        Path::new(&workspace.worktree),
                        &["symbolic-ref", "--short", "HEAD"],
                    ),
                    workspace.branch
                );
                assert_ne!(
                    git(Path::new(&workspace.worktree), &["rev-parse", "HEAD"]),
                    workspace.base_commit
                );
            }
            Self::DifferentGitBranch => {
                let workspace = &fixture.works[0].workspace;
                git(
                    Path::new(&workspace.worktree),
                    &["switch", "-c", "changed-while-waiting-for-queue"],
                );
                // Keep HEAD fixed so this exercises the adopted branch check.
                assert_eq!(
                    git(Path::new(&workspace.worktree), &["rev-parse", "HEAD"]),
                    workspace.base_commit
                );
                assert_ne!(
                    git(
                        Path::new(&workspace.worktree),
                        &["symbolic-ref", "--short", "HEAD"],
                    ),
                    workspace.branch
                );
            }
        }
    }
}

async fn wait_prepared_child(fixture: &Fixture) -> (Snapshot, Session) {
    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = fixture.snapshot();
            if snapshot.assignments.len() == 1 {
                let assignment = snapshot.assignments.values().next().unwrap();
                let child_id = ticket_runtime::ticket_child_id(&assignment.dispatch_key);
                if let Some(child) = fixture.state.storage.load_session(&child_id).await.unwrap() {
                    let generation = child.child_launch_generation();
                    if generation > 0
                        && child.recoverable_child_launch_generation() == Some(generation)
                    {
                        assert_eq!(assignment.state, AssignmentState::DispatchPending);
                        assert!(assignment.runtime.is_none());
                        assert_eq!(
                            ticket_runtime::read_dispatch(&child)
                                .unwrap()
                                .unwrap()
                                .max_rounds,
                            Some(MAX_ROUNDS)
                        );
                        return (snapshot, child);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the original Start and exact prepared Child precede queue admission")
}

async fn shutdown_with_controlled_replies(fixture: &mut Fixture) -> bool {
    // A RED run may have entered the local provider. Release only those gates
    // during cleanup, then assert the recorded call count after shutdown.
    let shutdown = fixture.state.shutdown();
    tokio::pin!(shutdown);
    tokio::time::timeout(WAIT, async {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                Some(request) = fixture.requests.recv() => {
                    let _ = request.reply.send(Reply::Complete);
                },
            }
        }
    })
    .await
    .is_ok()
}

async fn changed_resource_during_queue_wait(change: ChangedResource) {
    let mut fixture = Fixture::new(1).await;
    let supervisor = fixture.snapshot().binding.supervisor_session_id;
    let mut events = fixture
        .state
        .get_session_event_sender(&supervisor)
        .await
        .subscribe();
    let scheduler = fixture.state.spawn_scheduler.clone();
    let mut held = Vec::with_capacity(QUEUE_CAPACITY);
    for _ in 0..QUEUE_CAPACITY {
        match tokio::time::timeout(WAIT, scheduler.reserve_queue_capacity(None))
            .await
            .expect("the idle real queue has its configured capacity")
            .expect("live native SpawnScheduler")
        {
            AdmissionCommit::Committed(permit) => held.push(permit),
            _ => panic!("an ungated reservation must hold real queue capacity"),
        }
    }
    let mut probe = Box::pin(scheduler.reserve_queue_capacity(None));
    assert!(
        futures::poll!(probe.as_mut()).is_pending(),
        "the actual queue, rather than the native runner concurrency limit, is full"
    );
    // Do not let this diagnostic waiter steal the one slot released below.
    drop(probe);

    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    let (before, prepared) = wait_prepared_child(&fixture).await;
    let key = ReadyRetryKey {
        scope_id: before.binding.scope_id.clone(),
        work_id: fixture.works[0].id.clone(),
    };
    let store = FileReadyRetryStore::new(fixture._root.path());
    let retry_before = store.load(&key).await.unwrap().unwrap();
    assert_eq!(retry_before.record.attempts, 1);
    assert_eq!(retry_before.record.phase, ReadyRetryPhase::Reserved);
    assert!(matches!(
        &retry_before.record.request,
        Some(ReadyRetryRequest::Start { operation_id, request_hash })
            if before.receipts.get(operation_id)
                .is_some_and(|receipt| receipt.request_hash == *request_hash)
    ));
    fixture.assert_quiet(0).await;

    // The installed worker owns the original pass. This next pass is a serial
    // completion barrier; its Pending state is not used as queue-capacity proof.
    let tickets = fixture.state.tickets.clone();
    let mut barrier = Box::pin(tickets.reconcile_ready_work());
    assert!(futures::poll!(barrier.as_mut()).is_pending());
    change.change(&fixture);
    drop(held.pop().unwrap());
    let reconciled = tokio::time::timeout(WAIT, barrier).await;
    let revoked =
        tokio::time::timeout(WAIT, fixture.state.tickets.revoke_ready_work_policy()).await;
    drop(held);
    let stopped = shutdown_with_controlled_replies(&mut fixture).await;

    // Collect evidence before reporting a failed expectation, including RED's
    // real local-provider calls. No panic leaves a deliberately gated runner.
    let after = fixture.snapshot();
    let retry_after = store.load(&key).await.unwrap().unwrap();
    let child = fixture
        .state
        .storage
        .load_session(&prepared.id)
        .await
        .unwrap()
        .unwrap();
    let calls = fixture.provider.calls.load(Ordering::SeqCst);
    let mut starts = 0;
    loop {
        match events.try_recv() {
            Ok(AgentEvent::SubAgentStarted {
                child_session_id, ..
            }) if child_session_id == prepared.id => starts += 1,
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(error) => panic!("lost queue-publication evidence: {error}"),
        }
    }
    let durable_starts = journal::read_since(fixture.state.account_sink.events_dir(), 0)
        .unwrap()
        .into_iter()
        .filter(|change| {
            matches!(&change.event, AgentEvent::SubAgentStarted { child_session_id, .. }
                if child_session_id == &prepared.id)
        })
        .count();

    assert!(revoked.is_ok(), "revoke did not finish: {change:?}");
    assert!(
        stopped,
        "controlled native cleanup did not finish: {change:?}"
    );
    let result = reconciled
        .expect("freeing queue capacity must complete the original and barrier passes")
        .expect("resource rejection must stay an observable bounded reconcile result");
    assert!(!matches!(result, ReadyReconcile::Dispatched { .. }));
    assert_eq!(
        starts, 0,
        "obsolete Host resource check admitted a queue job: {change:?}"
    );
    assert_eq!(
        durable_starts, 0,
        "obsolete queue Start reached the journal: {change:?}"
    );
    assert_eq!(
        calls, 0,
        "changed resources reached the local provider: {change:?}"
    );
    assert_eq!(after.assignments.len(), 1);
    assert_eq!(after.intents.len(), 1);
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap(),
        "preserve the original Start receipt, Assignment, exact spec, context, epoch and generation"
    );
    assert_eq!(
        child.child_launch_generation(),
        prepared.child_launch_generation()
    );
    assert_eq!(child.last_run_status().as_deref(), Some("pending"));
    assert_eq!(
        serde_json::to_value(ticket_runtime::read_dispatch(&child).unwrap()).unwrap(),
        serde_json::to_value(ticket_runtime::read_dispatch(&prepared).unwrap()).unwrap()
    );
    assert_eq!(
        retry_after.record.identity_hash,
        retry_before.record.identity_hash
    );
    assert_eq!(
        retry_after.record.authority_epoch,
        retry_before.record.authority_epoch
    );
    assert_eq!(
        retry_after.record.created_at_ms,
        retry_before.record.created_at_ms
    );
    assert_eq!(
        retry_after.record.expires_at_ms,
        retry_before.record.expires_at_ms
    );
    assert_eq!(retry_after.record.attempts, retry_before.record.attempts);
    assert_eq!(retry_after.record.request, retry_before.record.request);
    assert!(retry_after.record.last_seen_at_ms >= retry_before.record.last_seen_at_ms);
    assert_eq!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    fixture.assert_quiet(0).await;
}

#[tokio::test]
async fn project_archived_while_real_queue_is_full_cannot_admit_prepared_child() {
    changed_resource_during_queue_wait(ChangedResource::ArchivedProject).await;
}

#[tokio::test]
async fn selected_binding_removed_while_real_queue_is_full_cannot_admit_prepared_child() {
    changed_resource_during_queue_wait(ChangedResource::UnboundSelectedWorkspace).await;
}

#[tokio::test]
async fn git_branch_changed_while_real_queue_is_full_cannot_admit_prepared_child() {
    changed_resource_during_queue_wait(ChangedResource::DifferentGitBranch).await;
}

#[tokio::test]
async fn git_head_changed_while_real_queue_is_full_cannot_admit_prepared_child() {
    changed_resource_during_queue_wait(ChangedResource::DifferentGitHead).await;
}
