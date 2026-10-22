//! #2062 focused writer lifecycle regressions, resumed from the preserved draft.
//! The original complete Host/Ready/Patrol fixture retains its separate source
//! and dependency requirements; these cases exercise the existing native runner.
//!
//! Reuses the real Harness, accepted Actor preparation and no-work stop helper.
//! No Agent/provider call, subprocess, synthetic stop proof, lock deletion,
//! replacement scope root or manually assigned authority epoch participates.

use super::*;
use bamboo_tickets::{canonical_bytes, Health, ScopeBinding, TicketService};
use std::{path::Path, sync::Weak};

fn bind_lifecycle_writer(harness: &Harness) -> (PathBuf, ScopeBinding, Arc<TicketService>) {
    let root = harness.home.join("ticket-writer-lifecycle");
    let binding = ScopeBinding {
        scope_id: format!("writer-lifecycle/{}", harness.parent.id),
        supervisor_session_id: harness.parent.id.clone(),
        binding_revision: 1,
    };
    let service = Arc::new(TicketService::open(&root, binding.clone()).unwrap());
    assert_eq!(service.health(), Health::Writable);
    harness.runner.set_ticket_service(Some(service.clone()));
    (root, binding, service)
}

fn assert_lifecycle_writer_still_locked(
    root: &Path,
    binding: &ScopeBinding,
    owner: &Weak<TicketService>,
    published_before: &[u8],
) {
    // Probe the actual identical physical scope before reading through Weak;
    // an accidental temporary external Arc cannot make this probe pass.
    match TicketService::open(root, binding.clone()) {
        Err(bamboo_tickets::Error::AuthorityUnavailable(reason)) => {
            assert!(reason.contains("another local writer holds the process lock"), "{reason}");
        }
        Err(error) => panic!("same-root writer failed for an unrelated reason: {error}"),
        Ok(_) => panic!("same-root writer reopened while the old owner must remain live"),
    }
    let actual = {
        let service = owner.upgrade().expect("runner retains the original writer");
        assert_eq!(service.health(), Health::Writable);
        canonical_bytes(&service.published().unwrap()).unwrap()
    };
    assert_eq!(actual.as_slice(), published_before, "failed reopen cannot change HEAD, epoch or Ticket facts");
    assert_eq!(owner.strong_count(), 1, "only the runner's cache owns this Ticket writer");
    assert!(root.join("writer.lock").is_file());
}

fn assert_lifecycle_provider_unused(control: &mut ProviderControl) {
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(control.provider.active.load(Ordering::SeqCst), 0);
}

/// Sends only the fixture's private tail gate on unwind; it never stops or
/// detaches a production task. This prevents a failed assertion leaving a
/// deliberately blocked coordinator behind in the test runtime.
struct LifecycleTailRelease(Option<oneshot::Sender<()>>);

impl LifecycleTailRelease {
    fn release(&mut self) {
        self.0.take().unwrap().send(()).expect("actual coordinator still awaits its tail");
    }
}

impl Drop for LifecycleTailRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test]
async fn idle_shutdown_releases_ticket_writer_and_closed_late_binding_cannot_resurrect_it() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let (root, binding, service) = bind_lifecycle_writer(&harness);
    let before = service.published().unwrap().1;
    let old_writer = Arc::downgrade(&service);
    assert!(lock(&harness.runner.executions).is_empty());
    assert!(lock(&harness.runner.coordinators).is_empty());

    tokio::time::timeout(WAIT, harness.runner.shutdown())
        .await.expect("idle shutdown completes").unwrap();
    assert!(harness.runner.closed.load(Ordering::Acquire));
    // A late scope publisher still holding its own legitimate handle must not
    // reattach that handle to a runtime whose shutdown already succeeded.
    harness.runner.set_ticket_service(Some(service.clone()));
    drop(service);
    // Obtain the actual physical-open result before asserting Weak, preserving
    // the genuine lock error as RED evidence if the runner retained its cache.
    let reopened = TicketService::open(&root, binding.clone()).unwrap_or_else(|error| {
        panic!("idle closed runtime retained/rebound its writer: {error}; old strong count={}", old_writer.strong_count())
    });
    assert_eq!(reopened.health(), Health::Writable);
    assert!(old_writer.upgrade().is_none());
    let after = reopened.published().unwrap().1;
    assert_eq!(after.binding, binding);
    assert_eq!(after.authority_epoch, before.authority_epoch.checked_add(1).unwrap());
    assert_eq!(canonical_bytes(&after.assignments).unwrap(), canonical_bytes(&before.assignments).unwrap());
    assert_eq!(canonical_bytes(&after.intents).unwrap(), canonical_bytes(&before.intents).unwrap());
    assert!(root.join("writer.lock").is_file());
    assert_lifecycle_provider_unused(&mut control);
}

#[tokio::test]
async fn unknown_unstarted_stop_keeps_the_unique_ticket_writer_across_shutdown_retries() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let (root, binding, service) = bind_lifecycle_writer(&harness);
    let original = canonical_bytes(&service.published().unwrap()).unwrap();
    let old_writer = Arc::downgrade(&service);
    drop(service);
    assert_eq!(old_writer.strong_count(), 1);
    let child = harness.child("writer-unknown-stop", "never enters the Agent").await;
    let run_id = "writer-accepted-unstarted-cleanup-failure";
    let registration = harness.router.register_run(&child.id, run_id).await.unwrap();
    harness.runner.prepare_child_execution(&child, run_id, CancellationToken::new()).await.unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    // Same actual resource-domain fault as the existing unknown-stop test:
    // abandon really attempts cleanup and cannot obtain a confirmed stop.
    execution.resources.record_unconfirmed("owned setup resource stop cannot be confirmed");
    assert!(harness.runner.abandon_child_execution(&child, run_id).await.is_err());
    let cold = bamboo_storage::SessionStoreV2::new(home.path().to_path_buf()).await.unwrap();
    let saved = cold.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(saved.metadata["runtime.local_task.state"], "stop_unconfirmed");
    assert_eq!(saved.metadata["runtime.local_task.run_id"], run_id);
    assert_eq!(saved.metadata["runtime.local_task.child_birth"], child.created_at.to_rfc3339());
    assert_eq!(saved.metadata["runtime.local_task.reason"], "execution_stop_unconfirmed");
    assert!(!saved.metadata.contains_key("runtime.local_task.instance"));
    assert!(!saved.metadata.contains_key("ticket.runtime.owned_stop.v1"));
    assert!(lock(&execution.stop).is_none());
    assert!(execution.task.lock().await.is_none());
    registration.quarantine().await;
    assert_eq!(harness.router.current_run_id(&child.id).await.as_deref(), Some(run_id));

    for attempt in 1..=2 {
        let error = tokio::time::timeout(WAIT, harness.runner.shutdown())
            .await.expect("unknown-stop shutdown reports instead of hanging")
            .expect_err("unknown stop must not release the writer or report successful shutdown");
        assert!(error.contains("unconfirmed"), "shutdown {attempt}: {error}");
        assert!(harness.runner.closed.load(Ordering::Acquire));
        assert_lifecycle_writer_still_locked(&root, &binding, &old_writer, &original);
        assert!(lock(&execution.stop).is_none());
        assert_eq!(harness.router.current_run_id(&child.id).await.as_deref(), Some(run_id));
        assert_lifecycle_provider_unused(&mut control);
    }
}

#[tokio::test]
async fn concurrent_and_aborted_shutdown_keep_the_actual_publication_tail_for_retry() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let (root, binding, service) = bind_lifecycle_writer(&harness);
    let before = service.published().unwrap().1;
    let original = canonical_bytes(&service.published().unwrap()).unwrap();
    let old_writer = Arc::downgrade(&service);
    drop(service);
    assert_eq!(old_writer.strong_count(), 1);
    let child = harness.child("writer-publication-tail", "unused unstarted input").await;
    let run_id = "writer-actual-publication-tail-coordinator";
    let registration = harness.router.register_run(&child.id, run_id).await.unwrap();
    harness.runner.prepare_child_execution(&child, run_id, CancellationToken::new()).await.unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    let (release_finish, finish_released) = oneshot::channel();
    let (finished, finish_observed) = oneshot::channel();
    let (release_tail, tail_released) = oneshot::channel();
    let mut tail = LifecycleTailRelease(Some(release_tail));
    let coordinator = {
        let runner = harness.runner.clone();
        let execution = execution.clone();
        let child = child.clone();
        tokio::spawn(async move {
            finish_released.await.unwrap();
            let stopped = checkpoint_unstarted_cancellation(&runner, &execution, child, run_id).await;
            runner.finish_child_execution(&stopped, run_id, true).await.unwrap();
            finished.send(()).unwrap();
            // No execution remains in the map, but this actual coordinator
            // still owns a live publication tail until the private gate opens.
            tail_released.await.unwrap();
        })
    };
    assert!(harness.runner.track_child_task(&child.id, run_id, coordinator).is_none());
    release_finish.send(()).unwrap();
    tokio::time::timeout(WAIT, finish_observed).await.unwrap().unwrap();
    assert!(lock(&execution.stop).is_some());
    assert!(execution.finalized.load(Ordering::Acquire));
    assert!(lock(&harness.runner.executions).is_empty());
    assert_eq!(
        harness.store.inspect_actor(&child.id).await.unwrap().activation.unwrap().status,
        ActorActivationStatus::Cancelled
    );
    assert_eq!(lock(&harness.runner.coordinators).len(), 1);

    let first_shutdown = {
        let runner = harness.runner.clone();
        tokio::spawn(async move { runner.shutdown().await })
    };
    tokio::time::timeout(WAIT, async {
        while !lock(&harness.runner.coordinators).is_empty() {
            assert!(!first_shutdown.is_finished(), "first shutdown must await the actual closed tail gate");
            tokio::task::yield_now().await;
        }
    }).await.expect("first shutdown takes/pops the retained coordinator before waiting");
    assert!(!first_shutdown.is_finished());
    assert_lifecycle_writer_still_locked(&root, &binding, &old_writer, &original);

    // The existing all-preparations permit is also the shutdown mutex. A
    // second caller must wait while the first still owns the actual join.
    let mut retry = Box::pin(harness.runner.shutdown());
    let parallel_poll = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(retry.as_mut(), cx))
    }).await;
    assert!(parallel_poll.is_pending(), "parallel shutdown bypassed the active drain: {parallel_poll:?}");
    assert_lifecycle_writer_still_locked(&root, &binding, &old_writer, &original);
    first_shutdown.abort();
    assert!(first_shutdown.await.expect_err("owned first shutdown is explicitly aborted").is_cancelled());

    // Poll the retry itself, so Pending proves it really entered shutdown while
    // the actual tail is closed; merely spawning an unpolled future is weaker.
    let first_poll = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(retry.as_mut(), cx))
    }).await;
    assert!(first_poll.is_pending(), "cancelled shutdown lost its unjoined coordinator: {first_poll:?}");
    assert!(lock(&harness.runner.executions).is_empty());
    assert_lifecycle_writer_still_locked(&root, &binding, &old_writer, &original);
    assert_lifecycle_provider_unused(&mut control);

    tail.release();
    tokio::time::timeout(WAIT, retry).await.expect("retry joins the released actual tail").unwrap();
    registration.finish(0).await.unwrap();
    // First probe the real writer, then check retained references: a successful
    // result requires both the joined tail and removal of the runner's cache.
    let reopened = TicketService::open(&root, binding.clone()).unwrap_or_else(|error| {
        panic!("joined shutdown retained its writer: {error}; old strong count={}", old_writer.strong_count())
    });
    assert_eq!(reopened.health(), Health::Writable);
    assert!(old_writer.upgrade().is_none());
    let after = reopened.published().unwrap().1;
    assert_eq!(after.binding, binding);
    assert_eq!(after.authority_epoch, before.authority_epoch.checked_add(1).unwrap());
    assert_eq!(canonical_bytes(&after.assignments).unwrap(), canonical_bytes(&before.assignments).unwrap());
    assert_eq!(canonical_bytes(&after.intents).unwrap(), canonical_bytes(&before.intents).unwrap());
    assert!(root.join("writer.lock").is_file());
    assert_lifecycle_provider_unused(&mut control);
}

#[tokio::test]
async fn failed_publication_join_keeps_the_writer_across_shutdown_retries() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let (root, binding, service) = bind_lifecycle_writer(&harness);
    let original = canonical_bytes(&service.published().unwrap()).unwrap();
    let old_writer = Arc::downgrade(&service);
    drop(service);
    // A real owned join fails without fabricating a Child stop or a provider
    // outcome. Once observed, that failed drain cannot become success merely
    // because its one-shot JoinHandle has already been consumed.
    lock(&harness.runner.coordinators).push(tokio::spawn(async {
        Err("controlled publication-tail failure".to_string())
    }));
    for attempt in 1..=2 {
        let error = tokio::time::timeout(WAIT, harness.runner.shutdown())
            .await.expect("failed join shutdown returns").expect_err("failed publication is retained");
        assert!(error.contains("controlled publication-tail failure"), "attempt {attempt}: {error}");
        assert_lifecycle_writer_still_locked(&root, &binding, &old_writer, &original);
    }
    assert_lifecycle_provider_unused(&mut control);
}
