//! Readback races use the original physical journal and fresh Host authority.
use super::*;
use bamboo_orchestration::patrol::{CheckpointPort, PortError, Scope};
use bamboo_server::app_state::ticket_application::PatrolCheckpointStore;
use bamboo_tickets::Error;

pub(super) async fn open(
    tickets: &TicketApplication,
    scope: &Scope,
) -> Result<PatrolCheckpointStore, Error> {
    open_with_conflict(tickets, scope, || {}).await
}

async fn open_with_conflict(
    tickets: &TicketApplication,
    scope: &Scope,
    mut on_conflict: impl FnMut(),
) -> Result<PatrolCheckpointStore, Error> {
    tokio::time::timeout(WAIT, async {
        loop {
            // Each call reacquires current authority and rechecks the canonical
            // scope/commit. A previous observation never becomes a read grant.
            match tickets.open_patrol_checkpoints(scope).await {
                Ok(journal) => return Ok(journal),
                Err(Error::RevisionConflict) => {
                    on_conflict();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .expect("fresh journal readback within the original finite wait")
}

#[tokio::test]
async fn revoked_ready_ack_does_not_release_a_detached_blocking_journal_writer() {
    let mut fixture = Fixture::new(1).await;
    let work_id = fixture.works[0].id.clone();
    let binding = bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(&fixture.snapshot().binding).unwrap(),
    );
    let journal_root = fixture
        ._root
        .path()
        .canonicalize()
        .unwrap()
        .join("orchestration")
        .join("patrol")
        .join(binding);
    let writer = PatrolCheckpointStore::open(&journal_root).unwrap();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (exited, mut exited_rx) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            // Dropping the sender during a panic also releases this real job.
            let _ = release_rx.blocking_recv();
            drop(writer);
            let _ = exited.send(());
        })
        .await
        .unwrap();
    });
    tokio::time::timeout(WAIT, entered_rx)
        .await
        .unwrap()
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());

    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    fixture.state.tickets.wake_ready_work();
    let running = fixture.next_request().await;
    let dispatched = fixture.snapshot();
    let assignment = dispatched.assignments.values().next().unwrap();
    let assignment_id = assignment.id.clone();
    let runtime = assignment.runtime.clone().unwrap();
    let tickets = fixture.state.tickets.clone();
    let scope = tickets
        .patrol_scope(fixture.project.as_str(), &work_id, &assignment_id)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, tickets.revoke_ready_work_policy())
        .await
        .unwrap();
    assert!(matches!(
        tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    assert!(matches!(
        exited_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let service = tickets.service().unwrap();
    let (commit, before) = service.published().unwrap();
    // This direct physical refusal and unchanged canonical commit distinguish
    // namespace-lock contention from the adapter's earlier commit-check race.
    assert_eq!(
        PatrolCheckpointStore::open(&journal_root).err(),
        Some(PortError::Conflict)
    );
    assert!(matches!(
        tickets.open_patrol_checkpoints(&scope).await,
        Err(Error::RevisionConflict)
    ));
    assert_eq!(service.published().unwrap().0, commit);
    eprintln!("2490 physical namespace contention after revoke ACK; unchanged commit={commit}; scope={scope:?}");

    let mut journal = {
        let (conflicted, conflicted_rx) = oneshot::channel();
        let mut conflicted = Some(conflicted);
        let readback = open_with_conflict(&tickets, &scope, || {
            if let Some(sender) = conflicted.take() {
                let _ = sender.send(());
            }
        });
        tokio::pin!(readback);
        tokio::select! {
            result = &mut readback => panic!(
                "readback completed while the blocking writer still owns its lease: {:?}",
                result.err()
            ),
            signal = tokio::time::timeout(WAIT, conflicted_rx) => {
                signal.unwrap().unwrap();
            }
        }
        // A malformed/stale selector must fail before physical I/O, rather
        // than being retried as the contention just observed above.
        let mut stale = scope.clone();
        stale.generation += 1;
        assert!(matches!(
            open(&tickets, &stale).await,
            Err(Error::ScopeDenied(_))
        ));
        assert!(matches!(
            exited_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        tokio::time::timeout(WAIT, exited_rx)
            .await
            .unwrap()
            .unwrap();
        readback.await.unwrap()
    };
    assert_eq!(journal.load(&scope).unwrap(), None);
    drop(journal);
    let (after_commit, after) = service.published().unwrap();
    assert_eq!(after_commit, commit);
    assert_eq!(
        bamboo_tickets::canonical_bytes(&after).unwrap(),
        bamboo_tickets::canonical_bytes(&before).unwrap()
    );
    fixture.assert_quiet(1).await;

    running.complete();
    fixture.wait_submitted(&work_id).await;
    let submitted = fixture.snapshot();
    assert_eq!(submitted.assignments.len(), 1);
    assert_eq!(submitted.intents.len(), 1);
    assert_eq!(submitted.submissions.len(), 1);
    assert_eq!(submitted.assignments[&assignment_id].runtime, Some(runtime));
    assert!(submitted.tickets[&work_id].accepted_submission.is_none());
    fixture.assert_quiet(1).await;
    drop(service);
    fixture.state.shutdown().await;
}
