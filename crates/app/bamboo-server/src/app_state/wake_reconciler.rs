//! Startup and bounded runtime recovery for missed durable SessionInbox wakes.
//!
//! The Inbox remains the only source of eligible work. Each wake is backed by
//! a storage-locked readiness proof and the eventual claim still uses CAS.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::Duration,
};

use bamboo_domain::{SessionKind, Storage};
use bamboo_engine::{session_activation::SessionWakeReconcileResult, SessionActivationRouter};
use bamboo_storage::SessionStoreV2;
use chrono::{DateTime, Utc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

const INITIAL_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(60);
const INDEX_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const SCAN_TICK: Duration = Duration::from_secs(1);
const MAX_INSPECTIONS_PER_TICK: usize = 32;
const RESERVED_INDEX_INSPECTIONS: usize = MAX_INSPECTIONS_PER_TICK / 2;
const STARTUP_INLINE_BUDGET: usize = MAX_INSPECTIONS_PER_TICK;

/// One immutable cutoff covers the initial and later paced passes. Revisit old
/// terminal records regardless of age/index label. Live recovery uses this
/// fixed pre-startup cutoff; a trusted terminal fact needs no death inference.
/// Neither lane depends on Session version migration.
struct TerminalRecovery {
    store: Arc<SessionStoreV2>,
    cutoff: DateTime<Utc>,
    cache: Option<bamboo_engine::SessionCache>,
}

impl TerminalRecovery {
    async fn inspect(&self, target: &str) {
        if self.store.get_index_entry(target).await.is_none() {
            return;
        }
        let result = async {
            let Some(expected) = self.store.load_session(target).await? else {
                return Ok(());
            };
            let cache = self.cache.clone();
            let selected = expected.clone();
            self.store
                .reconcile_session_runtime(
                    &expected,
                    self.cutoff,
                    None,
                    Arc::new(move |saved| {
                        if let Some(cache) = &cache {
                            if saved.kind == SessionKind::Child {
                                bamboo_engine::SessionRepository::publish_reconciled_child(
                                    cache, &selected, saved,
                                );
                            } else {
                                cache.insert(
                                    saved.id.clone(),
                                    Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
                                );
                            }
                        }
                    }),
                )
                .await?;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(error) = result {
            // The bounded index sweep retries with the same cutoff. Neither a
            // failed/uncertain write nor an absent local runner publishes a
            // completion, changes Inbox eligibility or grants a Worker proof.
            tracing::warn!(session_id = %target, %error, "Session terminal reconciliation unconfirmed; retained for paced retry");
        }
    }
}

#[derive(Default)]
pub(super) struct WakeReport {
    pub attempted: usize,
    pub retrying: usize,
    pub blocked: usize,
    pub checked: usize,
    pub queued: usize,
}

enum WakeError {
    Retry(String),
    Blocked(String),
}

impl WakeError {
    fn message(&self) -> &str {
        match self {
            Self::Retry(message) | Self::Blocked(message) => message,
        }
    }
}

async fn wake_once(
    target: &str,
    activation: &SessionActivationRouter,
    now: DateTime<Utc>,
) -> Result<SessionWakeReconcileResult, WakeError> {
    activation
        .reconcile_wake(target, now)
        .await
        .map_err(|error| match error {
            bamboo_domain::SessionActivationError::Internal(_) => {
                WakeError::Retry(error.to_string())
            }
            bamboo_domain::SessionActivationError::TargetNotFound(_) => {
                WakeError::Blocked(error.to_string())
            }
        })
}

#[derive(Default)]
struct WakeSchedule {
    due: BinaryHeap<Reverse<(DateTime<Utc>, u64, String)>>,
    current: HashMap<String, (DateTime<Utc>, u64)>,
    retry_intervals: HashMap<String, Duration>,
    sequence: u64,
}

impl WakeSchedule {
    fn schedule(&mut self, target: String, due: DateTime<Utc>, now: DateTime<Utc>) {
        let due = due.max(now + chrono::Duration::seconds(1));
        self.sequence = self.sequence.wrapping_add(1);
        self.current.insert(target.clone(), (due, self.sequence));
        self.due.push(Reverse((due, self.sequence, target)));
    }

    fn schedule_retry(&mut self, target: String, now: DateTime<Utc>) {
        let interval = *self
            .retry_intervals
            .entry(target.clone())
            .and_modify(|interval| *interval = (*interval * 2).min(MAX_RETRY_INTERVAL))
            .or_insert(INITIAL_RETRY_INTERVAL);
        self.schedule(
            target,
            now + chrono::Duration::from_std(interval).unwrap(),
            now,
        );
    }

    fn take_due(&mut self, now: DateTime<Utc>) -> Option<String> {
        while let Some(Reverse((due, _, _))) = self.due.peek() {
            if *due > now {
                return None;
            }
            let Reverse((due, sequence, target)) = self.due.pop().unwrap();
            if self.current.get(&target) == Some(&(due, sequence)) {
                self.current.remove(&target);
                return Some(target);
            }
        }
        None
    }

    fn pending_after(&self, target: &str, now: DateTime<Utc>) -> bool {
        self.current.get(target).is_some_and(|(due, _)| *due > now)
    }

    fn observe(
        &mut self,
        target: String,
        outcome: Result<SessionWakeReconcileResult, WakeError>,
        now: DateTime<Utc>,
    ) {
        match outcome {
            Ok(result) => {
                self.retry_intervals.remove(&target);
                self.current.remove(&target);
                if let Some(due) = result.next_due_at {
                    self.schedule(target, due, now);
                }
            }
            Err(WakeError::Retry(error)) => {
                tracing::warn!(session_id = %target, %error,
                    "durable SessionInbox wake unconfirmed; scheduling bounded retry");
                self.schedule_retry(target, now);
            }
            Err(WakeError::Blocked(error)) => {
                self.current.remove(&target);
                self.retry_intervals.remove(&target);
                tracing::error!(session_id = %target, %error,
                    "durable SessionInbox wake is blocked by absent authority");
            }
        }
    }
}

/// One task handles exact due times and a paced index sweep. The index is
/// copied once per cycle; at most 32 Inbox inspections run per second. At
/// least 16 index slots remain available even under a continuous due backlog,
/// so a missed wake is revisited within 60 seconds plus ceil(index size / 16)
/// seconds for entries in a snapshot; entries created during a long sweep may
/// wait one extra sweep. Known due items are serviced in batches of at least
/// 16 per tick.
fn spawn_periodic(
    store: Arc<SessionStoreV2>,
    activation: Arc<SessionActivationRouter>,
    shutdown: CancellationToken,
    mut schedule: WakeSchedule,
    initial_ids: Vec<String>,
    observers: Option<Arc<super::observer_subscriptions::ObserverSubscriptionCoordinator>>,
    terminal: TerminalRecovery,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SCAN_TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut ids = initial_ids;
        let mut cursor = 0;
        let mut next_refresh = tokio::time::Instant::now() + INDEX_REFRESH_INTERVAL;
        let mut initial_sweep_pending = !ids.is_empty();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => {}
            }
            let now = Utc::now();
            if let Some(observers) = &observers {
                observers.tick(now).await;
            }
            if cursor >= ids.len() && tokio::time::Instant::now() >= next_refresh {
                ids = store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .map(|entry| entry.id)
                    .collect();
                cursor = 0;
                next_refresh = tokio::time::Instant::now() + INDEX_REFRESH_INTERVAL;
            }
            let mut budget = MAX_INSPECTIONS_PER_TICK;
            while budget > RESERVED_INDEX_INSPECTIONS {
                let Some(target) = schedule.take_due(now) else {
                    break;
                };
                budget -= 1;
                terminal.inspect(&target).await;
                let outcome = wake_once(&target, activation.as_ref(), now).await;
                schedule.observe(target, outcome, now);
            }
            let mut examined = 0;
            while budget > 0 && examined < MAX_INSPECTIONS_PER_TICK && cursor < ids.len() {
                let target = &ids[cursor];
                cursor += 1;
                examined += 1;
                if schedule.pending_after(target, now) {
                    continue;
                }
                budget -= 1;
                terminal.inspect(target).await;
                let outcome = wake_once(target, activation.as_ref(), now).await;
                schedule.observe(target.clone(), outcome, now);
            }
            if initial_sweep_pending && cursor >= ids.len() {
                initial_sweep_pending = false;
                tracing::info!(
                    queued = ids.len(),
                    "completed paced initial SessionInbox wake sweep"
                );
            }
        }
    });
}

/// Inspect at most one tick's worth of indexed Sessions before serving. The
/// report distinguishes checked from queued targets; the same paced task
/// finishes the first sweep and keeps recovering later due/missed wakes.
#[cfg(test)]
pub(super) async fn reconcile_startup(
    store: Arc<SessionStoreV2>,
    activation: Arc<SessionActivationRouter>,
    shutdown: CancellationToken,
) -> WakeReport {
    reconcile_startup_inner(store, activation, shutdown, None, None).await
}

pub(super) async fn reconcile_startup_with_observers(
    store: Arc<SessionStoreV2>,
    activation: Arc<SessionActivationRouter>,
    shutdown: CancellationToken,
    observers: Arc<super::observer_subscriptions::ObserverSubscriptionCoordinator>,
    cache: bamboo_engine::SessionCache,
) -> WakeReport {
    reconcile_startup_inner(store, activation, shutdown, Some(observers), Some(cache)).await
}

async fn reconcile_startup_inner(
    store: Arc<SessionStoreV2>,
    activation: Arc<SessionActivationRouter>,
    shutdown: CancellationToken,
    observers: Option<Arc<super::observer_subscriptions::ObserverSubscriptionCoordinator>>,
    cache: Option<bamboo_engine::SessionCache>,
) -> WakeReport {
    let terminal = TerminalRecovery {
        store: store.clone(),
        cutoff: Utc::now(),
        cache,
    };
    let mut report = WakeReport::default();
    let mut schedule = WakeSchedule::default();
    let entries = store.list_index_entries().await;
    let mut queued = Vec::new();
    for (index, entry) in entries.into_iter().enumerate() {
        if index >= STARTUP_INLINE_BUDGET {
            queued.push(entry.id);
            continue;
        }
        report.checked += 1;
        terminal.inspect(&entry.id).await;
        let now = Utc::now();
        let outcome = wake_once(&entry.id, activation.as_ref(), now).await;
        match &outcome {
            Ok(result) if result.disposition.is_some() => report.attempted += 1,
            Ok(_) => {}
            Err(error @ WakeError::Blocked(_)) => {
                report.blocked += 1;
                tracing::error!(session_id = %entry.id, error = error.message(),
                    "durable SessionInbox startup wake is blocked by absent authority");
            }
            Err(error @ WakeError::Retry(_)) => {
                report.retrying += 1;
                tracing::warn!(session_id = %entry.id, error = error.message(),
                    "durable SessionInbox startup wake unconfirmed; scheduling retry");
            }
        }
        schedule.observe(entry.id, outcome, now);
    }
    report.queued = queued.len();
    spawn_periodic(
        store, activation, shutdown, schedule, queued, observers, terminal,
    );
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{
        SessionActivationError, SessionActivationPolicy, SessionInboxConsumerId,
        SessionInboxLeaseRequest, SessionInboxLimits, SessionInboxPort, SessionMessageEnvelope,
    };
    use bamboo_engine::{
        SessionActivationLaunch, SessionActivationReserveOutcome, SessionActivationRouter,
        SessionActivationSpawner,
    };
    use bamboo_storage::FileSessionInbox;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    };

    struct TestSpawner {
        fail_once: AtomicBool,
        reservations: AtomicUsize,
        generations: Mutex<Vec<u64>>,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SessionActivationSpawner for TestSpawner {
        async fn reserve_activation(
            &self,
            _: &str,
            generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            self.reservations.fetch_add(1, Ordering::SeqCst);
            self.generations.lock().unwrap().push(generation);
            if self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(SessionActivationError::Internal(
                    "injected reservation failure".into(),
                ));
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new("recovered-run", move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    async fn seed(home: &std::path::Path) -> (Arc<SessionStoreV2>, Arc<FileSessionInbox>) {
        let store = Arc::new(SessionStoreV2::new(home.to_path_buf()).await.unwrap());
        store
            .save_session(&bamboo_domain::Session::new("wake-target", "model"))
            .await
            .unwrap();
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        inbox
            .deliver_with_activation_intent(
                &SessionMessageEnvelope::user_input("wake-target", "recover me"),
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();
        (store, inbox)
    }

    async fn router(
        inbox: Arc<FileSessionInbox>,
        fail_once: bool,
    ) -> (Arc<SessionActivationRouter>, Arc<TestSpawner>) {
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox);
        let spawner = Arc::new(TestSpawner {
            fail_once: AtomicBool::new(fail_once),
            reservations: AtomicUsize::new(0),
            generations: Mutex::new(Vec::new()),
            launches: Arc::new(AtomicUsize::new(0)),
        });
        router.set_spawner(spawner.clone()).await;
        (router, spawner)
    }

    #[tokio::test]
    async fn restart_reconciles_terminal_sessions_and_cache_without_migration_or_new_execution() {
        use bamboo_domain::{
            ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, AgentRuntimeState,
            AgentStatusState, Session,
        };
        let home = tempfile::tempdir().unwrap();
        let original = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let cache: bamboo_engine::SessionCache = Arc::default();
        let parent = Session::new("terminal-parent", "model");
        original.save_session(&parent).await.unwrap();
        let mut evidence = Vec::new();
        for (index, outcome, typed) in [
            (
                0,
                ActorActivationFinish::Succeeded,
                AgentStatusState::Completed,
            ),
            (
                1,
                ActorActivationFinish::Cancelled,
                AgentStatusState::Cancelled,
            ),
            (2, ActorActivationFinish::Failed, AgentStatusState::Failed),
        ] {
            for child in [false, true] {
                let mut session = if child {
                    Session::new_child_of(
                        format!("terminal-child-{index}"),
                        &parent,
                        "model",
                        "Child",
                    )
                } else {
                    Session::new(format!("terminal-root-{index}"), "model")
                };
                session.session_version = None;
                session.agent_runtime_state = Some(AgentRuntimeState::new(&session.id));
                session.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
                session.set_last_run_status("running");
                original.save_session(&session).await.unwrap();
                let now = Utc::now();
                let activation = original
                    .claim_activation(&ActorActivationClaim {
                        actor_id: session.id.clone(),
                        run_id: format!("run-{index}"),
                        lease_owner: format!("synthetic-owner-{index}"),
                        lease_expires_at: now + chrono::Duration::minutes(5),
                        inbox_generation: 0,
                        placement_ref: None,
                        now,
                    })
                    .await
                    .unwrap();
                original
                    .start_activation(&activation.fence(), now)
                    .await
                    .unwrap();
                original
                    .finish_activation(&activation.fence(), now, outcome)
                    .await
                    .unwrap();
                let mut zombie = original.load_session(&session.id).await.unwrap().unwrap();
                zombie.agent_runtime_state.as_mut().unwrap().status =
                    AgentStatusState::Initializing;
                zombie.metadata.insert(
                    "agent.runtime.state".into(),
                    serde_json::to_string(zombie.agent_runtime_state.as_ref().unwrap()).unwrap(),
                );
                zombie.messages.clear();
                // Seed only this TempDir with the audited legacy split. The trusted
                // Actor/Main files are the actual commits, not enumeration stubs.
                let directory = if child {
                    home.path()
                        .join("sessions")
                        .join(&parent.id)
                        .join("children")
                        .join(&session.id)
                } else {
                    home.path().join("sessions").join(&session.id)
                };
                tokio::fs::write(
                    directory.join("runtime.json"),
                    serde_json::to_vec_pretty(&zombie).unwrap(),
                )
                .await
                .unwrap();
                let main = tokio::fs::read(directory.join("session.json"))
                    .await
                    .unwrap();
                cache.insert(
                    zombie.id.clone(),
                    Arc::new(bamboo_engine::SessionSnapshot::new(zombie)),
                );
                evidence.push((session.id, typed, main, directory));
            }
        }
        let mut unknown = Session::new("unknown-live-root", "model");
        unknown.agent_runtime_state = Some(AgentRuntimeState::new(&unknown.id));
        unknown.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
        unknown.set_last_run_status("running");
        original.save_session(&unknown).await.unwrap();
        original.flush_search_index().await;
        drop(original);

        let reopened = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            reopened.clone(),
            SessionInboxLimits::default(),
        ));
        let (router, spawner) = router(inbox, false).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup_inner(
            reopened.clone(),
            router,
            shutdown.clone(),
            None,
            Some(cache.clone()),
        )
        .await;
        assert_eq!(report.checked, 8);
        assert_eq!(report.attempted, 0);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 0);
        shutdown.cancel();
        let replay = TerminalRecovery {
            store: reopened.clone(),
            cutoff: Utc::now() - chrono::Duration::hours(1),
            cache: Some(cache.clone()),
        };
        for (id, typed, main, directory) in evidence {
            let repaired = reopened.load_session(&id).await.unwrap().unwrap();
            assert_eq!(repaired.session_version, None);
            assert_eq!(repaired.agent_runtime_state.as_ref().unwrap().status, typed);
            assert_eq!(
                cache
                    .get(&id)
                    .unwrap()
                    .read()
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .status,
                typed
            );
            let runtime = tokio::fs::read(directory.join("runtime.json"))
                .await
                .unwrap();
            replay.inspect(&id).await;
            assert_eq!(
                tokio::fs::read(directory.join("runtime.json"))
                    .await
                    .unwrap(),
                runtime
            );
            assert_eq!(
                tokio::fs::read(directory.join("session.json"))
                    .await
                    .unwrap(),
                main
            );
        }
        assert_eq!(
            reopened
                .load_session(&unknown.id)
                .await
                .unwrap()
                .unwrap()
                .agent_runtime_state
                .unwrap()
                .status,
            AgentStatusState::Running
        );
        assert!(!cache.contains_key(&unknown.id));
        reopened.flush_search_index().await;
    }

    #[tokio::test]
    async fn restart_recovers_immediate_inbox_intent_with_one_cold_owner() {
        let home = tempfile::tempdir().unwrap();
        let (original_store, original_inbox) = seed(home.path()).await;
        drop(original_inbox);
        drop(original_store);
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let (router, spawner) = router(inbox.clone(), false).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup(store.clone(), router.clone(), shutdown.clone()).await;
        assert_eq!(report.attempted, 1);
        assert_eq!(report.retrying, 0);
        let repeated = reconcile_startup(store, router, shutdown.clone()).await;
        assert_eq!(repeated.attempted, 1);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn failed_startup_reservation_retries_the_same_durable_generation() {
        let home = tempfile::tempdir().unwrap();
        let (store, inbox) = seed(home.path()).await;
        let generation = inbox
            .inspect("wake-target")
            .await
            .unwrap()
            .activation_generation;
        let (router, spawner) = router(inbox.clone(), true).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup(store, router, shutdown.clone()).await;
        assert_eq!(report.attempted, 0);
        assert_eq!(report.retrying, 1);
        tokio::time::timeout(Duration::from_secs(3), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("failed startup wake must retry after reading the same Inbox");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(
            *spawner.generations.lock().unwrap(),
            vec![generation, generation]
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn due_owned_lease_recovers_after_restart_without_early_wake() {
        let home = tempfile::tempdir().unwrap();
        let (original_store, original_inbox) = seed(home.path()).await;
        let now = Utc::now();
        let claim = original_inbox
            .claim_owned(
                "wake-target",
                1,
                None,
                &SessionInboxLeaseRequest {
                    consumer: SessionInboxConsumerId::new(),
                    now,
                    duration: chrono::Duration::seconds(2),
                },
            )
            .await
            .unwrap()
            .remove(0);
        drop(original_inbox);
        drop(original_store);
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let (router, spawner) = router(inbox, false).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup(store, router, shutdown.clone()).await;
        assert_eq!(report.attempted, 0);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
        assert!(Utc::now() < claim.lease.expires_at);
        tokio::time::timeout(Duration::from_secs(5), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("due owned lease must wake from the startup deadline timer");
        assert!(Utc::now() >= claim.lease.expires_at);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn startup_wake_sweep_is_paced_and_completes_queued_targets() {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        for index in 0..=STARTUP_INLINE_BUDGET {
            let target = format!("wake-{index}");
            store
                .save_session(&bamboo_domain::Session::new(target.as_str(), "model"))
                .await
                .unwrap();
            inbox
                .deliver_with_activation_intent(
                    &SessionMessageEnvelope::user_input(target.as_str(), "recover"),
                    SessionActivationPolicy::RespectSpecificWait,
                    None,
                )
                .await
                .unwrap();
        }
        let (router, spawner) = router(inbox, false).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup(store, router, shutdown.clone()).await;
        assert_eq!(report.checked, STARTUP_INLINE_BUDGET);
        assert_eq!(report.queued, 1);
        assert_eq!(report.attempted, STARTUP_INLINE_BUDGET);
        tokio::time::timeout(Duration::from_secs(4), async {
            while spawner.launches.load(Ordering::SeqCst) <= STARTUP_INLINE_BUDGET {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("queued startup target must be reconciled by the paced task");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 33);
        shutdown.cancel();
    }
}
