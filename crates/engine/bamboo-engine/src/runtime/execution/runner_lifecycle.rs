//! Runner lifecycle helpers for background agent execution.
//!
//! Provides shared implementations for:
//! - Runner reservation (check existing → create new with cancel token)
//! - Runner finalization (map execution result to `AgentStatus`)
//! - Status mapping

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent};

use super::runner_state::{AgentRunner, AgentStatus};

/// Reservation result from [`reserve_runner_core`] / [`try_reserve_runner`].
#[derive(Debug, Clone)]
pub struct RunnerReservation {
    pub cancel_token: CancellationToken,
    pub run_id: String,
}

/// Outcome of the shared reservation core.
#[derive(Debug, Clone)]
pub enum ReserveOutcome {
    /// A fresh runner was reserved (any stale runner was replaced).
    Reserved(RunnerReservation),
    /// A `Running` runner already exists for this session; carries its `run_id`
    /// so the caller can correlate subsequent SSE/WS events.
    AlreadyRunning(String),
}

/// Shared runner-reservation core used by BOTH the server's `reserve_runner`
/// and the engine's [`try_reserve_runner`], so the idle-eviction sender
/// re-assert can never drift between the two paths again (#346).
///
/// While holding the `runners` write lock it:
/// 1. short-circuits with [`ReserveOutcome::AlreadyRunning`] if a `Running`
///    runner already exists;
/// 2. otherwise replaces any stale runner with a fresh `Running` one whose
///    `event_sender` is `event_sender`;
/// 3. **re-asserts `event_sender` into `senders`** (`entry().or_insert_with`),
///    STILL holding the `runners` write lock.
///
/// Step 3 closes the idle-sweep TOCTOU: a caller obtains the sender via
/// `get_or_create_event_sender` and then reserves, but the 60s idle sweep can
/// evict that session's sender in between (it drops a terminal runner and its
/// sender together under the same `runners` ⊃ `senders` lock order). Without the
/// re-assert, a resumed / re-executed run would publish to a channel no longer
/// registered in `senders`, and a later subscriber's `get_or_create_event_sender`
/// would mint a *different* channel and silently miss every event of that run.
/// Because both this re-assert and the sweep acquire `runners` first, they are
/// mutually exclusive, and a freshly-inserted `Running` runner is never swept —
/// so the re-asserted sender is durable for the run.
pub async fn reserve_runner_core(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
    event_sender: &broadcast::Sender<AgentEvent>,
) -> ReserveOutcome {
    let mut runners_guard = runners.write().await;
    if let Some(runner) = runners_guard.get(session_id) {
        if matches!(runner.status, AgentStatus::Running) {
            return ReserveOutcome::AlreadyRunning(runner.run_id.clone());
        }
    }

    // Acquire every fallible/cancellation point before mutating the runner
    // registry. In particular, cancellation while waiting for the sender map
    // must not leave a `Running` slot with no task behind it.
    let mut senders_guard = senders.write().await;
    if let Some(previous) = runners_guard.get(session_id) {
        previous.event_publication.retire().await;
    }
    runners_guard.remove(session_id);

    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    runner.event_sender = event_sender.clone();
    let reservation = RunnerReservation {
        cancel_token: runner.cancel_token.clone(),
        run_id: runner.run_id.clone(),
    };
    runners_guard.insert(session_id.to_string(), runner);

    // (3) Re-assert the session sender under the held `runners` write lock. Same
    // channel as `event_sender`, so a no-op in the common case; restores the map
    // entry if the idle sweep removed it. Lock order (runners ⊃ senders) matches
    // the sweep, so this cannot deadlock.
    senders_guard
        .entry(session_id.to_string())
        .or_insert_with(|| event_sender.clone());

    ReserveOutcome::Reserved(reservation)
}

/// Try to reserve a runner for the given session.
///
/// Returns `None` if a `Running` runner already exists (the caller skips
/// execution and surfaces the existing `run_id` separately). Otherwise reserves
/// a fresh runner AND re-asserts the session sender into `senders` — see
/// [`reserve_runner_core`].
///
/// The re-assert is **required, not optional**: this function is reached by the
/// resume paths (`/respond`, child-completion coordinator, gold auto-answer via
/// `resume_session_execution`), which re-execute a long-lived session under the
/// SAME id after a wait that easily exceeds the idle TTL — exactly the
/// evict-then-re-execute race #346's sweep newly opens.
pub async fn try_reserve_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
    event_sender: &broadcast::Sender<AgentEvent>,
) -> Option<RunnerReservation> {
    match reserve_runner_core(runners, senders, session_id, event_sender).await {
        ReserveOutcome::Reserved(reservation) => Some(reservation),
        ReserveOutcome::AlreadyRunning(_) => {
            tracing::debug!("[{}] Runner already running, skipping", session_id);
            None
        }
    }
}

/// Drain publication before removing the old generation from the registry.
/// If the caller is cancelled at the await, the closed old fence remains in
/// the map, so a subsequent reservation still has to drain it before Started.
pub async fn remove_runner_entry(
    runners: &mut HashMap<String, AgentRunner>,
    session_id: &str,
) -> Option<AgentRunner> {
    if let Some(runner) = runners.get(session_id) {
        runner.event_publication.retire().await;
    }
    runners.remove(session_id)
}

/// Map an execution result to `AgentStatus`.
pub fn status_from_execution_result(result: &Result<(), AgentError>) -> AgentStatus {
    match result {
        Ok(_) => AgentStatus::Completed,
        Err(error) if error.is_cancelled() => AgentStatus::Cancelled,
        Err(error) => AgentStatus::Error(error.to_string()),
    }
}

/// Update a runner's terminal status and completion timestamp.
pub async fn finalize_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    result: &Result<(), AgentError>,
) {
    let mut guard = runners.write().await;
    if let Some(runner) = guard.get_mut(session_id) {
        runner.status = status_from_execution_result(result);
        runner.completed_at = Some(Utc::now());
    }
}

/// Finalize only the exact reserved runner.
///
/// Registration collisions can expose a stale caller after another entry
/// point has become authoritative. That caller must release its own slot
/// without ever terminalizing a newer runner stored under the same session id.
pub async fn finalize_runner_exact(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    run_id: &str,
    result: &Result<(), AgentError>,
) -> bool {
    let mut guard = runners.write().await;
    let Some(runner) = guard.get_mut(session_id) else {
        return false;
    };
    if runner.run_id != run_id {
        return false;
    }
    runner.status = status_from_execution_result(result);
    runner.completed_at = Some(Utc::now());
    true
}

/// Finalize a rejected caller only when it owns a distinct runner slot.
///
/// A duplicate task can race with the live task while both observe the same
/// registry run id. Rejecting that duplicate must not terminalize the slot that
/// still belongs to the registered owner.
pub async fn finalize_rejected_runner_if_distinct(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    existing_owner_run_id: &str,
    attempted_run_id: &str,
    result: &Result<(), AgentError>,
) -> bool {
    if existing_owner_run_id == attempted_run_id {
        return false;
    }
    finalize_runner_exact(runners, session_id, attempted_run_id, result).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_removal_keeps_the_predecessor_fence_discoverable() {
        use std::sync::Barrier;
        let runner = AgentRunner::new();
        let publication = runner.event_publication.clone();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let publisher = {
            let publication = publication.clone();
            let entered = entered.clone();
            let release = release.clone();
            std::thread::spawn(move || {
                publication.publish(|| {
                    entered.wait();
                    release.wait();
                })
            })
        };
        entered.wait();
        let mut runners = HashMap::from([("child".to_string(), runner)]);
        let mut removing = Box::pin(remove_runner_entry(&mut runners, "child"));
        assert!(futures::poll!(removing.as_mut()).is_pending());
        drop(removing);
        assert!(
            runners.contains_key("child"),
            "a successor must still find the old fence after cancellation"
        );
        assert!(!publication.publish(|| panic!("retired run must reject late frames")));
        release.wait();
        publisher.join().unwrap();
        assert!(remove_runner_entry(&mut runners, "child").await.is_some());
        assert!(runners.is_empty());
    }

    fn new_runners() -> Arc<RwLock<HashMap<String, AgentRunner>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn new_senders() -> Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn new_broadcaster() -> broadcast::Sender<AgentEvent> {
        broadcast::channel(100).0
    }

    #[tokio::test]
    async fn try_reserve_runner_creates_runner_with_running_status() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let token = try_reserve_runner(&runners, &senders, "s1", &tx).await;
        assert!(token.is_some());

        let guard = runners.read().await;
        let runner = guard.get("s1").unwrap();
        assert!(matches!(runner.status, AgentStatus::Running));
    }

    #[tokio::test]
    async fn try_reserve_runner_returns_none_when_already_running() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let _ = try_reserve_runner(&runners, &senders, "s1", &tx).await;
        let second = try_reserve_runner(&runners, &senders, "s1", &tx).await;
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn try_reserve_runner_replaces_completed_runner() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let _ = try_reserve_runner(&runners, &senders, "s1", &tx).await;

        {
            let mut guard = runners.write().await;
            let runner = guard.get_mut("s1").unwrap();
            runner.status = AgentStatus::Completed;
        }

        let second = try_reserve_runner(&runners, &senders, "s1", &tx).await;
        assert!(second.is_some());
    }

    #[tokio::test]
    async fn rejected_duplicate_does_not_terminalize_shared_owner_slot() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let reservation = try_reserve_runner(&runners, &senders, "s1", &tx)
            .await
            .unwrap();
        let rejected = Err(AgentError::Cancelled);

        assert!(
            !finalize_rejected_runner_if_distinct(
                &runners,
                "s1",
                &reservation.run_id,
                &reservation.run_id,
                &rejected,
            )
            .await
        );
        assert!(matches!(
            runners.read().await.get("s1").map(|runner| &runner.status),
            Some(AgentStatus::Running)
        ));
    }

    #[tokio::test]
    async fn rejected_distinct_runner_releases_only_its_exact_slot() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let reservation = try_reserve_runner(&runners, &senders, "s1", &tx)
            .await
            .unwrap();
        let rejected = Err(AgentError::Cancelled);

        assert!(
            finalize_rejected_runner_if_distinct(
                &runners,
                "s1",
                "different-live-owner",
                &reservation.run_id,
                &rejected,
            )
            .await
        );
        assert!(matches!(
            runners.read().await.get("s1").map(|runner| &runner.status),
            Some(AgentStatus::Cancelled)
        ));
    }

    #[tokio::test]
    async fn try_reserve_runner_reasserts_evicted_sender_so_late_subscriber_receives() {
        // Regression for #346: the resume path (`resume_session_execution`)
        // obtains the session sender, then reserves — and the idle sweep can
        // evict that sender in between. `try_reserve_runner` must re-assert it so
        // the resumed run's channel stays registered and a late subscriber lands
        // on the SAME channel the run publishes to.
        use super::super::session_events::get_or_create_event_sender;
        use bamboo_agent_core::AgentEvent;

        let runners = new_runners();
        let senders = new_senders();

        // Resume ordering: obtain sender (inserts into the map) ...
        let session_tx = get_or_create_event_sender(&senders, "s1").await;
        // ... then the idle sweep evicts this session's sender before reservation.
        senders.write().await.remove("s1");
        assert!(senders.read().await.get("s1").is_none());

        // Reserve with the clone obtained before the eviction.
        let reservation = try_reserve_runner(&runners, &senders, "s1", &session_tx).await;
        assert!(reservation.is_some(), "reservation must succeed");

        // The sender MUST be re-asserted into the map. Without the fix it is
        // absent here and the run's channel is orphaned.
        assert!(
            senders.read().await.get("s1").is_some(),
            "reservation must re-assert the evicted session sender into the map"
        );

        // A late subscriber (SSE/WS handler: get_or_create then subscribe) must
        // land on the SAME channel the run publishes to.
        let subscriber_tx = get_or_create_event_sender(&senders, "s1").await;
        let mut rx = subscriber_tx.subscribe();

        // The resumed run publishes via its reserved channel (== session_tx).
        let _ = session_tx.send(AgentEvent::SessionDeleted {
            session_id: "s1".to_string(),
        });

        let received = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await;
        assert!(
            matches!(received, Ok(Ok(_))),
            "late subscriber must receive events from the resumed run; without the \
             re-assert, get_or_create mints a fresh channel and the event is lost"
        );
    }

    #[tokio::test]
    async fn cancellation_before_atomic_registry_commit_leaves_no_zombie_runner() {
        let runners = new_runners();
        let senders = new_senders();
        let tx = new_broadcaster();
        let held_senders = senders.write().await;

        let task = {
            let runners = runners.clone();
            let senders = senders.clone();
            let tx = tx.clone();
            tokio::spawn(
                async move { reserve_runner_core(&runners, &senders, "cancelled", &tx).await },
            )
        };

        // Wait until the reservation owns the runners lock and is blocked on
        // the sender lock. No registry mutation is allowed before that final
        // cancellation point completes.
        for _ in 0..100 {
            if runners.try_write().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            runners.try_write().is_err(),
            "reservation never reached the sender-lock barrier"
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(held_senders);

        assert!(
            runners.read().await.get("cancelled").is_none(),
            "a cancelled reservation must not leave a Running slot without a task"
        );
        assert!(matches!(
            reserve_runner_core(&runners, &senders, "cancelled", &tx).await,
            ReserveOutcome::Reserved(_)
        ));
    }

    #[test]
    fn status_from_execution_result_maps_correctly() {
        let ok_result: Result<(), AgentError> = Ok(());
        assert!(matches!(
            status_from_execution_result(&ok_result),
            AgentStatus::Completed
        ));

        // Cancellation is detected by matching the `AgentError::Cancelled`
        // variant, not by substring-matching the (display) message — note the
        // variant's message is "Cancelled", which would not even contain the
        // lowercase "cancelled" the old code searched for.
        let cancelled: Result<(), AgentError> = Err(AgentError::Cancelled);
        assert!(matches!(
            status_from_execution_result(&cancelled),
            AgentStatus::Cancelled
        ));

        let failed: Result<(), AgentError> = Err(AgentError::LLM("network error".to_string()));
        match status_from_execution_result(&failed) {
            AgentStatus::Error(message) => assert!(message.contains("network error")),
            other => panic!("unexpected status: {other:?}"),
        }
    }
}
