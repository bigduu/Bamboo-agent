//! Server-owned, best-effort routing from framework [`ToolEventV1`] values to
//! supervised plugin services.
//!
//! The tool path is deliberately small and synchronous: it takes a
//! non-blocking snapshot read, matches subscriptions, performs one explicit
//! bounded-size serialization for per-sink admission, and enters each
//! independent sink queue with `try_send`. The downstream service-input
//! boundary serializes again off the tool path. Service discovery,
//! process-generation changes, queue workers, and shutdown joins happen
//! outside tool execution.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bamboo_plugin::manifest::{
    EventSinkCapabilityState, EventSinkInactiveReason, EventSinkManifestEntry,
    MAX_EVENT_SINKS_PER_PLUGIN, MAX_EVENT_SINK_ID_BYTES,
};
use bamboo_plugin::registry::EventSinkReconciliation;
use bamboo_plugin::PluginManifest;
use bamboo_plugin_protocol::{
    ToolEventPublishError, ToolEventPublisher, ToolEventV1, FILE_CHANGED_SUBSCRIPTION_ID_V1,
};
use parking_lot::RwLock as ParkingRwLock;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::service_manager::{
    ServiceInputHealth, ServiceInputSendError, ServiceInputSender, ServiceManager,
};

const RECONCILE_INTERVAL: Duration = Duration::from_millis(100);

/// Sanitized live state for one declared event sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEventSinkState {
    /// Provenance names this bounded id, but no trustworthy live declaration
    /// is available yet (boot still reconciling or on-disk manifest damage).
    Unavailable,
    Inactive,
    WaitingForService,
    Live,
}

/// Payload-free, bounded status exposed through the plugin API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolEventSinkStatusSnapshot {
    pub id: String,
    pub service_id: String,
    pub state: ToolEventSinkState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactive_reason: Option<EventSinkInactiveReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub queue_capacity: usize,
    pub max_event_bytes: usize,
    /// Accepted by the exact live service-generation input queue. This is
    /// best-effort admission, not a durable acknowledgement from the plugin.
    pub delivered: u64,
    pub queue_full: u64,
    pub service_down: u64,
    pub serialization: u64,
    pub oversize: u64,
}

#[derive(Default)]
struct SinkCounters {
    delivered: AtomicU64,
    queue_full: AtomicU64,
    service_down: AtomicU64,
    serialization: AtomicU64,
    oversize: AtomicU64,
}

impl SinkCounters {
    fn snapshot(&self) -> SinkCounterSnapshot {
        SinkCounterSnapshot {
            delivered: self.delivered.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
            service_down: self.service_down.load(Ordering::Relaxed),
            serialization: self.serialization.load(Ordering::Relaxed),
            oversize: self.oversize.load(Ordering::Relaxed),
        }
    }
}

struct SinkCounterSnapshot {
    delivered: u64,
    queue_full: u64,
    service_down: u64,
    serialization: u64,
    oversize: u64,
}

fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

#[derive(Clone)]
struct SubscriptionFilter {
    id: String,
    tool_names: Vec<String>,
}

impl SubscriptionFilter {
    fn matches(&self, event: &ToolEventV1) -> bool {
        if self.id != event.subscription_id.as_str() {
            return false;
        }
        // ToolEventV1 currently has one implemented event-type subscription.
        // Keep the event-type check explicit rather than treating a matching
        // subscription string as sufficient on its own.
        if self.id == FILE_CHANGED_SUBSCRIPTION_ID_V1 && !event.event_type.is_file_changed() {
            return false;
        }
        self.tool_names.is_empty()
            || self
                .tool_names
                .iter()
                .any(|name| name == &event.context.tool_name)
    }
}

struct SinkRegistration {
    plugin_id: String,
    id: String,
    service_id: String,
    capability: EventSinkCapabilityState,
    subscriptions: Vec<SubscriptionFilter>,
    queue_capacity: usize,
    max_event_bytes: usize,
    counters: Arc<SinkCounters>,
}

impl SinkRegistration {
    fn from_manifest(
        plugin_id: &str,
        manifest: &EventSinkManifestEntry,
        capability: EventSinkCapabilityState,
        counters: Arc<SinkCounters>,
    ) -> Self {
        Self {
            plugin_id: plugin_id.to_string(),
            id: manifest.id.clone(),
            service_id: manifest.service_id.clone(),
            capability,
            subscriptions: manifest
                .subscriptions
                .iter()
                .map(|subscription| SubscriptionFilter {
                    id: subscription.id.as_str().to_string(),
                    tool_names: subscription.tool_names.clone(),
                })
                .collect(),
            // PluginManifest::validate has already enforced non-zero absolute
            // bounds. Conversions are lossless on every supported target.
            queue_capacity: manifest.delivery.queue_capacity as usize,
            max_event_bytes: manifest.delivery.max_event_bytes as usize,
            counters,
        }
    }

    fn is_eligible(&self) -> bool {
        matches!(self.capability, EventSinkCapabilityState::Eligible)
    }

    fn matches(&self, event: &ToolEventV1) -> bool {
        self.subscriptions
            .iter()
            .any(|subscription| subscription.matches(event))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkInputError {
    QueueFull,
    ServiceDown,
    Serialization,
    Oversize,
}

trait SinkInput: Send + Sync {
    fn generation(&self) -> u64;
    fn try_send(&self, event: &ToolEventV1) -> Result<(), SinkInputError>;
}

impl SinkInput for ServiceInputSender {
    fn generation(&self) -> u64 {
        ServiceInputSender::generation(self)
    }

    fn try_send(&self, event: &ToolEventV1) -> Result<(), SinkInputError> {
        ServiceInputSender::try_send(self, event).map_err(|error| match error {
            ServiceInputSendError::QueueFull { .. } => SinkInputError::QueueFull,
            ServiceInputSendError::StaleGeneration { .. }
            | ServiceInputSendError::Stopped { .. }
            | ServiceInputSendError::BrokenStdin { .. } => SinkInputError::ServiceDown,
            ServiceInputSendError::Serialization => SinkInputError::Serialization,
            ServiceInputSendError::Oversize { .. } => SinkInputError::Oversize,
        })
    }
}

struct LiveSink {
    generation: u64,
    active: Arc<AtomicBool>,
    cancel: CancellationToken,
    tx: mpsc::Sender<Arc<ToolEventV1>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl LiveSink {
    fn spawn(registration: &SinkRegistration, input: Arc<dyn SinkInput>) -> Arc<Self> {
        let generation = input.generation();
        let active = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(registration.queue_capacity);
        let task = tokio::spawn(run_sink_worker(
            registration.id.clone(),
            registration.counters.clone(),
            input,
            active.clone(),
            cancel.clone(),
            rx,
        ));
        Arc::new(Self {
            generation,
            active,
            cancel,
            tx,
            task: Mutex::new(Some(task)),
        })
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    fn begin_stop(&self) {
        self.active.store(false, Ordering::SeqCst);
        self.cancel.cancel();
    }

    async fn join(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    fn try_enqueue(&self, event: Arc<ToolEventV1>, counters: &SinkCounters) -> Result<(), ()> {
        if !self.is_active() {
            increment(&counters.service_down);
            return Err(());
        }
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                increment(&counters.queue_full);
                Err(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                increment(&counters.service_down);
                Err(())
            }
        }
    }
}

async fn run_sink_worker(
    sink_id: String,
    counters: Arc<SinkCounters>,
    input: Arc<dyn SinkInput>,
    active: Arc<AtomicBool>,
    cancel: CancellationToken,
    mut rx: mpsc::Receiver<Arc<ToolEventV1>>,
) {
    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            event = rx.recv() => match event {
                Some(event) => event,
                None => break,
            }
        };
        if !active.load(Ordering::SeqCst) {
            break;
        }
        match input.try_send(event.as_ref()) {
            Ok(()) => increment(&counters.delivered),
            Err(SinkInputError::QueueFull) => increment(&counters.queue_full),
            Err(SinkInputError::Serialization) => increment(&counters.serialization),
            Err(SinkInputError::Oversize) => increment(&counters.oversize),
            Err(SinkInputError::ServiceDown) => {
                increment(&counters.service_down);
                active.store(false, Ordering::SeqCst);
                tracing::debug!(
                    sink_id,
                    generation = input.generation(),
                    "event-sink generation became unavailable"
                );
                break;
            }
        }
    }
    active.store(false, Ordering::SeqCst);
}

#[derive(Clone)]
struct PublishedSink {
    registration: Arc<SinkRegistration>,
    live: Option<Arc<LiveSink>>,
}

#[derive(Default)]
struct RouterState {
    desired: BTreeMap<String, Arc<SinkRegistration>>,
    active: HashMap<String, Arc<LiveSink>>,
}

/// AppState-owned ToolEvent publisher and plugin-sink lifecycle registry.
pub struct ToolEventRouter {
    service_manager: Arc<ServiceManager>,
    state: AsyncMutex<RouterState>,
    published: ParkingRwLock<Vec<PublishedSink>>,
    has_routeable_sinks: AtomicBool,
    monitor_enabled: bool,
    reconcile_monitor: Mutex<Option<ReconcileMonitor>>,
}

struct ReconcileMonitor {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl ToolEventRouter {
    pub fn new(service_manager: Arc<ServiceManager>) -> Arc<Self> {
        Self::new_inner(service_manager, true)
    }

    fn new_inner(service_manager: Arc<ServiceManager>, monitor_enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            service_manager,
            state: AsyncMutex::new(RouterState::default()),
            published: ParkingRwLock::new(Vec::new()),
            has_routeable_sinks: AtomicBool::new(false),
            monitor_enabled,
            reconcile_monitor: Mutex::new(None),
        })
    }

    /// Start generation polling only while at least one eligible declaration
    /// exists. A default AppState (and an AppState with inactive-only sinks)
    /// owns no router task at all.
    async fn refresh_monitor(self: &Arc<Self>) {
        if !self.monitor_enabled {
            return;
        }
        let routeable = self.has_routeable_sinks.load(Ordering::SeqCst);
        let stopped = {
            let mut monitor = self
                .reconcile_monitor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if routeable {
                if monitor.is_some() {
                    return;
                }
                let weak = Arc::downgrade(self);
                let cancel = CancellationToken::new();
                let task_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = task_cancel.cancelled() => break,
                            _ = interval.tick() => {}
                        }
                        let Some(router) = weak.upgrade() else {
                            break;
                        };
                        router.reconcile_once().await;
                    }
                });
                *monitor = Some(ReconcileMonitor { cancel, task });
                return;
            }
            monitor.take()
        };
        if let Some(stopped) = stopped {
            stopped.cancel.cancel();
            let _ = stopped.task.await;
        }
    }

    #[cfg(test)]
    fn monitor_is_running(&self) -> bool {
        self.reconcile_monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Replace all runtime declarations owned by `plugin_id` with one pure
    /// #903 reconciliation plan. Existing workers are removed from the hot
    /// snapshot and joined before any replacement generation can activate.
    pub async fn apply_plugin_plan(
        self: &Arc<Self>,
        plugin_id: &str,
        manifest: &PluginManifest,
        plan: &EventSinkReconciliation,
    ) {
        let mut state = self.state.lock().await;
        let prior_counters: HashMap<String, Arc<SinkCounters>> = state
            .desired
            .values()
            .filter(|registration| registration.plugin_id == plugin_id)
            .map(|registration| (registration.id.clone(), registration.counters.clone()))
            .collect();

        let mut stopped = Vec::new();
        let owned_ids: Vec<String> = state
            .desired
            .values()
            .filter(|registration| registration.plugin_id == plugin_id)
            .map(|registration| registration.id.clone())
            .collect();
        for id in owned_ids {
            state.desired.remove(&id);
            if let Some(live) = state.active.remove(&id) {
                live.begin_stop();
                stopped.push(live);
            }
        }
        for id in &plan.deactivate_before_services {
            let owned = state
                .desired
                .get(id)
                .is_some_and(|registration| registration.plugin_id == plugin_id);
            if owned {
                state.desired.remove(id);
                if let Some(live) = state.active.remove(id) {
                    live.begin_stop();
                    stopped.push(live);
                }
            }
        }
        self.rebuild_published(&state);
        for live in stopped {
            live.join().await;
        }

        for reconciled in &plan.sinks_after_services {
            let Some(entry) = manifest
                .provides
                .event_sinks
                .iter()
                .find(|entry| entry.id == reconciled.id)
            else {
                continue;
            };
            if state
                .desired
                .get(&entry.id)
                .is_some_and(|registration| registration.plugin_id != plugin_id)
            {
                tracing::warn!(
                    plugin_id,
                    sink_id = %entry.id,
                    "event-sink runtime ownership conflict; refusing replacement"
                );
                continue;
            }
            let counters = prior_counters
                .get(&entry.id)
                .cloned()
                .unwrap_or_else(|| Arc::new(SinkCounters::default()));
            state.desired.insert(
                entry.id.clone(),
                Arc::new(SinkRegistration::from_manifest(
                    plugin_id,
                    entry,
                    reconciled.state.clone(),
                    counters,
                )),
            );
        }
        self.rebuild_published(&state);
        drop(state);
        self.refresh_monitor().await;
        self.reconcile_once().await;
    }

    /// Remove exact provenance-owned ids. The hot snapshot is revoked first;
    /// this future returns only after every matching worker has exited.
    pub async fn unregister_sinks(self: &Arc<Self>, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        let mut stopped = Vec::new();
        for id in ids {
            state.desired.remove(id);
            if let Some(live) = state.active.remove(id) {
                live.begin_stop();
                stopped.push(live);
            }
        }
        self.rebuild_published(&state);
        for live in stopped {
            live.join().await;
        }
        drop(state);
        self.refresh_monitor().await;
    }

    /// Revoke only the prior plugin sinks whose current declaration is backed
    /// by a service that an upgrade is about to remove. Provenance records ids
    /// but not sink-to-service edges, so the live desired registration is the
    /// authority for this narrow Step-0 ordering seam.
    ///
    /// The plugin id and provenance-owned sink ids are both checked before a
    /// registration can be removed. This prevents a stale/corrupt provenance
    /// row from borrowing another plugin's route while still guaranteeing
    /// that every matching worker is joined before its service is stopped.
    pub(crate) async fn unregister_plugin_sinks_backed_by_services(
        self: &Arc<Self>,
        plugin_id: &str,
        ids: &[String],
        service_ids: &[String],
    ) {
        if ids.is_empty() || service_ids.is_empty() {
            return;
        }
        let ids: HashSet<&str> = ids.iter().map(String::as_str).collect();
        let service_ids: HashSet<&str> = service_ids.iter().map(String::as_str).collect();
        let mut state = self.state.lock().await;
        let matching_ids: Vec<String> = state
            .desired
            .values()
            .filter(|registration| {
                registration.plugin_id == plugin_id
                    && ids.contains(registration.id.as_str())
                    && service_ids.contains(registration.service_id.as_str())
            })
            .map(|registration| registration.id.clone())
            .collect();
        let mut stopped = Vec::with_capacity(matching_ids.len());
        for id in matching_ids {
            state.desired.remove(&id);
            if let Some(live) = state.active.remove(&id) {
                live.begin_stop();
                stopped.push(live);
            }
        }
        self.rebuild_published(&state);
        for live in stopped {
            live.join().await;
        }
        drop(state);
        self.refresh_monitor().await;
    }

    /// Reconcile each eligible declaration to the exact currently-ready
    /// `ServiceInputSender` generation. A scheduled/starting service, a
    /// legacy null-stdin service, and a broken/stale generation all remain
    /// visible as `waiting_for_service` and never receive a queue worker.
    pub async fn reconcile_once(&self) {
        let mut state = self.state.lock().await;
        let desired: Vec<Arc<SinkRegistration>> = state.desired.values().cloned().collect();
        let mut inputs: HashMap<String, Option<ServiceInputSender>> = HashMap::new();

        for registration in desired {
            let sender = if registration.is_eligible() {
                if let Some(cached) = inputs.get(&registration.service_id) {
                    cached.clone()
                } else {
                    let ready = self
                        .service_manager
                        .status(&registration.service_id)
                        .await
                        .and_then(|status| status.input)
                        .is_some_and(|input| input.health == ServiceInputHealth::Ready);
                    let sender = if ready {
                        self.service_manager
                            .input_sender(&registration.service_id)
                            .await
                    } else {
                        None
                    };
                    inputs.insert(registration.service_id.clone(), sender.clone());
                    sender
                }
            } else {
                None
            };

            let current_matches = match (state.active.get(&registration.id), sender.as_ref()) {
                (Some(live), Some(sender)) => {
                    live.is_active() && live.generation == sender.generation()
                }
                (None, None) => true,
                _ => false,
            };
            if current_matches {
                continue;
            }

            if let Some(live) = state.active.remove(&registration.id) {
                live.begin_stop();
                self.rebuild_published(&state);
                live.join().await;
            }
            if let Some(sender) = sender {
                let live = LiveSink::spawn(&registration, Arc::new(sender));
                state.active.insert(registration.id.clone(), live);
            }
            self.rebuild_published(&state);
        }
    }

    /// Status is capped even if an on-disk provenance row was hand-corrupted
    /// to contain more ids than manifest validation permits.
    pub async fn status_for_ids(&self, ids: &[String]) -> Vec<ToolEventSinkStatusSnapshot> {
        let state = self.state.lock().await;
        ids.iter()
            .take(MAX_EVENT_SINKS_PER_PLUGIN)
            .filter(|id| !id.trim().is_empty() && id.len() <= MAX_EVENT_SINK_ID_BYTES)
            .map(|id| {
                let Some(registration) = state.desired.get(id) else {
                    return ToolEventSinkStatusSnapshot {
                        id: id.clone(),
                        service_id: String::new(),
                        state: ToolEventSinkState::Unavailable,
                        inactive_reason: None,
                        generation: None,
                        queue_capacity: 0,
                        max_event_bytes: 0,
                        delivered: 0,
                        queue_full: 0,
                        service_down: 0,
                        serialization: 0,
                        oversize: 0,
                    };
                };
                let live = state.active.get(id).filter(|live| live.is_active());
                let (state_value, inactive_reason, generation) = match &registration.capability {
                    EventSinkCapabilityState::Inactive { detail } => {
                        (ToolEventSinkState::Inactive, Some(detail.clone()), None)
                    }
                    EventSinkCapabilityState::Eligible => match live {
                        Some(live) => (ToolEventSinkState::Live, None, Some(live.generation)),
                        None => (ToolEventSinkState::WaitingForService, None, None),
                    },
                };
                let counters = registration.counters.snapshot();
                ToolEventSinkStatusSnapshot {
                    id: registration.id.clone(),
                    service_id: registration.service_id.clone(),
                    state: state_value,
                    inactive_reason,
                    generation,
                    queue_capacity: registration.queue_capacity,
                    max_event_bytes: registration.max_event_bytes,
                    delivered: counters.delivered,
                    queue_full: counters.queue_full,
                    service_down: counters.service_down,
                    serialization: counters.serialization,
                    oversize: counters.oversize,
                }
            })
            .collect()
    }

    fn rebuild_published(&self, state: &RouterState) {
        let snapshot: Vec<PublishedSink> = state
            .desired
            .values()
            .map(|registration| PublishedSink {
                registration: registration.clone(),
                live: state.active.get(&registration.id).cloned(),
            })
            .collect();
        let has_routeable = snapshot.iter().any(|sink| sink.registration.is_eligible());
        *self.published.write() = snapshot;
        self.has_routeable_sinks
            .store(has_routeable, Ordering::SeqCst);
    }

    #[cfg(test)]
    async fn install_input_for_test(self: &Arc<Self>, id: &str, input: Arc<dyn SinkInput>) {
        let mut state = self.state.lock().await;
        let registration = state.desired.get(id).cloned().expect("test sink exists");
        if let Some(live) = state.active.remove(id) {
            live.begin_stop();
            self.rebuild_published(&state);
            live.join().await;
        }
        state
            .active
            .insert(id.to_string(), LiveSink::spawn(&registration, input));
        self.rebuild_published(&state);
    }

    #[cfg(test)]
    async fn configure_test_sink(
        self: &Arc<Self>,
        plugin_id: &str,
        entry: EventSinkManifestEntry,
        capability: EventSinkCapabilityState,
    ) {
        let mut state = self.state.lock().await;
        state.desired.insert(
            entry.id.clone(),
            Arc::new(SinkRegistration::from_manifest(
                plugin_id,
                &entry,
                capability,
                Arc::new(SinkCounters::default()),
            )),
        );
        self.rebuild_published(&state);
        drop(state);
        self.refresh_monitor().await;
    }
}

impl ToolEventPublisher for ToolEventRouter {
    fn is_enabled(&self) -> bool {
        self.has_routeable_sinks.load(Ordering::SeqCst)
    }

    fn try_publish(&self, event: ToolEventV1) -> Result<(), ToolEventPublishError> {
        event
            .validate_bounds()
            .map_err(ToolEventPublishError::InvalidEvent)?;
        let snapshot = self
            .published
            .try_read()
            .ok_or(ToolEventPublishError::Busy)?;
        let matching: Vec<&PublishedSink> = snapshot
            .iter()
            .filter(|sink| sink.registration.is_eligible() && sink.registration.matches(&event))
            .collect();
        if matching.is_empty() {
            return Ok(());
        }
        let serialized_len = match serde_json::to_vec(&event) {
            Ok(serialized) => serialized.len(),
            Err(_) => {
                for sink in matching {
                    increment(&sink.registration.counters.serialization);
                }
                return Ok(());
            }
        };
        let event = Arc::new(event);
        for sink in matching {
            if serialized_len > sink.registration.max_event_bytes {
                increment(&sink.registration.counters.oversize);
                continue;
            }
            match &sink.live {
                Some(live) => {
                    let _ = live.try_enqueue(event.clone(), &sink.registration.counters);
                }
                None => increment(&sink.registration.counters.service_down),
            }
        }
        Ok(())
    }
}

impl Drop for ToolEventRouter {
    fn drop(&mut self) {
        if let Some(monitor) = self
            .reconcile_monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            monitor.cancel.cancel();
            monitor.task.abort();
        }
        if let Ok(mut state) = self.state.try_lock() {
            for live in state.active.values() {
                live.begin_stop();
            }
            state.active.clear();
        }
    }
}

/// Keeps the existing injected-publisher test/embedder seam while making the
/// AppState router the production publisher.
pub(crate) struct CombinedToolEventPublisher {
    router: Arc<ToolEventRouter>,
    additional: Arc<dyn ToolEventPublisher>,
}

impl CombinedToolEventPublisher {
    pub(crate) fn new(
        router: Arc<ToolEventRouter>,
        additional: Arc<dyn ToolEventPublisher>,
    ) -> Self {
        Self { router, additional }
    }
}

impl ToolEventPublisher for CombinedToolEventPublisher {
    fn is_enabled(&self) -> bool {
        self.router.is_enabled() || self.additional.is_enabled()
    }

    fn try_publish(&self, event: ToolEventV1) -> Result<(), ToolEventPublishError> {
        let router_enabled = self.router.is_enabled();
        let additional_enabled = self.additional.is_enabled();
        match (router_enabled, additional_enabled) {
            (false, false) => Ok(()),
            (true, false) => self.router.try_publish(event),
            (false, true) => self.additional.try_publish(event),
            (true, true) => {
                let router_result = self.router.try_publish(event.clone());
                let additional_result = self.additional.try_publish(event);
                router_result.and(additional_result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Condvar, Mutex};

    use bamboo_plugin::manifest::{
        EventSinkDeliveryLimits, EventSinkProtocolManifest, EventSinkSubscriptionManifest,
        ObservationPermissionId,
    };
    use bamboo_plugin_protocol::{
        FileChangedV1, ToolEventContextV1, ToolEventSubscriptionId, TOOL_EVENT_PROTOCOL_NAME,
        TOOL_EVENT_V1_SCHEMA_VERSION,
    };

    use super::*;

    struct RecordingInput {
        generation: u64,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingInput {
        fn new(generation: u64) -> Self {
            Self {
                generation,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl SinkInput for RecordingInput {
        fn generation(&self) -> u64 {
            self.generation
        }

        fn try_send(&self, event: &ToolEventV1) -> Result<(), SinkInputError> {
            self.calls
                .lock()
                .unwrap()
                .push(event.context.tool_call_id.clone());
            Ok(())
        }
    }

    struct BlockingInput {
        generation: u64,
        entered: AtomicBool,
        release: (Mutex<bool>, Condvar),
        calls: AtomicUsize,
    }

    impl BlockingInput {
        fn new(generation: u64) -> Self {
            Self {
                generation,
                entered: AtomicBool::new(false),
                release: (Mutex::new(false), Condvar::new()),
                calls: AtomicUsize::new(0),
            }
        }

        fn release(&self) {
            let mut released = self.release.0.lock().unwrap();
            *released = true;
            self.release.1.notify_all();
        }
    }

    impl SinkInput for BlockingInput {
        fn generation(&self) -> u64 {
            self.generation
        }

        fn try_send(&self, _event: &ToolEventV1) -> Result<(), SinkInputError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.store(true, Ordering::SeqCst);
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
            Ok(())
        }
    }

    fn sink(
        id: &str,
        tools: &[&str],
        capacity: u32,
        max_event_bytes: u32,
    ) -> EventSinkManifestEntry {
        EventSinkManifestEntry {
            id: id.to_string(),
            service_id: format!("{id}-service"),
            protocol: EventSinkProtocolManifest {
                name: TOOL_EVENT_PROTOCOL_NAME.to_string(),
                version: TOOL_EVENT_V1_SCHEMA_VERSION,
                extensions: BTreeMap::new(),
            },
            subscriptions: vec![EventSinkSubscriptionManifest {
                id: ToolEventSubscriptionId::file_changed_v1(),
                tool_names: tools.iter().map(|tool| (*tool).to_string()).collect(),
                extensions: BTreeMap::new(),
            }],
            delivery: EventSinkDeliveryLimits {
                queue_capacity: capacity,
                max_event_bytes,
                extensions: BTreeMap::new(),
            },
            requested_permissions: vec![ObservationPermissionId::new("metadata")],
            platforms: None,
            extensions: BTreeMap::new(),
        }
    }

    fn event(tool: &str, call: &str, path_len: usize) -> ToolEventV1 {
        ToolEventV1::file_changed(
            ToolEventContextV1::bounded("session", "root", tool, call).unwrap(),
            FileChangedV1::bounded(format!("/{}", "x".repeat(path_len))).unwrap(),
        )
        .unwrap()
    }

    fn router() -> Arc<ToolEventRouter> {
        ToolEventRouter::new_inner(Arc::new(ServiceManager::new()), false)
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition reached");
    }

    #[tokio::test]
    async fn filters_by_event_subscription_and_canonical_tool_name() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("write", &["Write"], 4, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        router
            .configure_test_sink(
                "plugin",
                sink("edit", &["Edit"], 4, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        let write = Arc::new(RecordingInput::new(1));
        let edit = Arc::new(RecordingInput::new(2));
        router.install_input_for_test("write", write.clone()).await;
        router.install_input_for_test("edit", edit.clone()).await;

        router.try_publish(event("Write", "write-1", 8)).unwrap();
        router.try_publish(event("Edit", "edit-1", 8)).unwrap();
        wait_until(|| write.calls().len() == 1 && edit.calls().len() == 1).await;
        assert_eq!(write.calls(), vec!["write-1"]);
        assert_eq!(edit.calls(), vec!["edit-1"]);
    }

    #[tokio::test]
    async fn generation_monitor_is_lazy_and_stops_with_last_eligible_sink() {
        let router = ToolEventRouter::new(Arc::new(ServiceManager::new()));
        assert!(!router.monitor_is_running());
        router
            .configure_test_sink(
                "plugin",
                sink("inactive", &[], 4, 16_384),
                EventSinkCapabilityState::Inactive {
                    detail: EventSinkInactiveReason::ServiceDisabled,
                },
            )
            .await;
        assert!(!router.monitor_is_running());
        router
            .configure_test_sink(
                "plugin",
                sink("eligible", &[], 4, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        assert!(router.monitor_is_running());
        router
            .unregister_sinks(&["inactive".to_string(), "eligible".to_string()])
            .await;
        assert!(!router.monitor_is_running());
    }

    #[tokio::test]
    async fn service_filtered_unregister_checks_owner_and_preserves_unrelated_route() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("removed", &[], 4, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        router
            .configure_test_sink(
                "plugin",
                sink("retained", &[], 4, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        let removed = Arc::new(RecordingInput::new(1));
        let retained = Arc::new(RecordingInput::new(2));
        router
            .install_input_for_test("removed", removed.clone())
            .await;
        router
            .install_input_for_test("retained", retained.clone())
            .await;

        let prior_ids = vec!["removed".to_string(), "retained".to_string()];
        let dropped_services = vec!["removed-service".to_string()];
        router
            .unregister_plugin_sinks_backed_by_services(
                "other-plugin",
                &prior_ids,
                &dropped_services,
            )
            .await;
        assert_eq!(
            router.status_for_ids(&["removed".to_string()]).await[0].state,
            ToolEventSinkState::Live,
            "a mismatched plugin owner must not revoke the route"
        );

        router
            .unregister_plugin_sinks_backed_by_services("plugin", &prior_ids, &dropped_services)
            .await;
        let statuses = router.status_for_ids(&prior_ids).await;
        assert_eq!(statuses[0].state, ToolEventSinkState::Unavailable);
        assert_eq!(statuses[1].state, ToolEventSinkState::Live);

        router.try_publish(event("Write", "after-drop", 8)).unwrap();
        wait_until(|| retained.calls().len() == 1).await;
        assert!(removed.calls().is_empty());
        assert_eq!(retained.calls(), vec!["after-drop"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_slow_sink_isolated_from_tools_and_other_sink() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("slow", &[], 1, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        router
            .configure_test_sink(
                "plugin",
                sink("fast", &[], 8, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        let slow = Arc::new(BlockingInput::new(1));
        let fast = Arc::new(RecordingInput::new(2));
        router.install_input_for_test("slow", slow.clone()).await;
        router.install_input_for_test("fast", fast.clone()).await;

        router.try_publish(event("Write", "one", 8)).unwrap();
        wait_until(|| slow.entered.load(Ordering::SeqCst)).await;
        router.try_publish(event("Write", "two", 8)).unwrap();
        router.try_publish(event("Write", "three", 8)).unwrap();
        wait_until(|| fast.calls().len() == 3).await;
        let status = router.status_for_ids(&["slow".to_string()]).await;
        assert_eq!(status[0].queue_full, 1);
        assert_eq!(fast.calls(), vec!["one", "two", "three"]);
        slow.release();
    }

    #[tokio::test]
    async fn outage_and_sink_event_bound_are_counted_without_payloads() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("down", &[], 2, 256),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        router.try_publish(event("Write", "down", 8)).unwrap();
        router.try_publish(event("Write", "oversize", 300)).unwrap();

        let status = router.status_for_ids(&["down".to_string()]).await;
        assert_eq!(status[0].state, ToolEventSinkState::WaitingForService);
        assert_eq!(status[0].service_down, 1);
        assert_eq!(status[0].oversize, 1);
        let safe = serde_json::to_string(&status).unwrap();
        assert!(!safe.contains(&"x".repeat(32)));
        assert!(!safe.contains("/xxxxxxxx"));
    }

    #[tokio::test]
    async fn replacement_generation_is_ordered_and_does_not_follow_old_sender() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("restart", &[], 8, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        let first = Arc::new(RecordingInput::new(10));
        router
            .install_input_for_test("restart", first.clone())
            .await;
        router.try_publish(event("Write", "old-1", 8)).unwrap();
        router.try_publish(event("Write", "old-2", 8)).unwrap();
        wait_until(|| first.calls().len() == 2).await;

        let second = Arc::new(RecordingInput::new(11));
        router
            .install_input_for_test("restart", second.clone())
            .await;
        router.try_publish(event("Write", "new-1", 8)).unwrap();
        router.try_publish(event("Write", "new-2", 8)).unwrap();
        wait_until(|| second.calls().len() == 2).await;
        assert_eq!(first.calls(), vec!["old-1", "old-2"]);
        assert_eq!(second.calls(), vec!["new-1", "new-2"]);
        assert_eq!(
            router.status_for_ids(&["restart".to_string()]).await[0].generation,
            Some(11)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_unregister_revokes_snapshot_and_joins_worker() {
        let router = router();
        router
            .configure_test_sink(
                "plugin",
                sink("remove", &[], 64, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;
        let input = Arc::new(RecordingInput::new(1));
        router.install_input_for_test("remove", input.clone()).await;
        let publishing = {
            let router = router.clone();
            tokio::spawn(async move {
                for index in 0..500 {
                    let _ = router.try_publish(event("Write", &format!("call-{index}"), 8));
                    tokio::task::yield_now().await;
                }
            })
        };
        router.unregister_sinks(&["remove".to_string()]).await;
        publishing.await.unwrap();
        let count_after_unregister = input.calls().len();
        for index in 0..10 {
            router
                .try_publish(event("Write", &format!("after-{index}"), 8))
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert_eq!(input.calls().len(), count_after_unregister);
        let status = router.status_for_ids(&["remove".to_string()]).await;
        assert_eq!(status[0].state, ToolEventSinkState::Unavailable);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_service_manager_stop_start_rebinds_a_fresh_generation() {
        use std::path::PathBuf;

        use bamboo_domain::mcp_config::ReconnectConfig;
        use bamboo_plugin::manifest::{GracefulShutdown, HealthCheckSpec, ServiceInputProtocol};

        use crate::service_manager::ServiceRuntimeConfig;

        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("events.ndjson");
        let manager = Arc::new(ServiceManager::new());
        let router = ToolEventRouter::new(manager.clone());
        router
            .configure_test_sink(
                "plugin",
                sink("real", &[], 8, 16_384),
                EventSinkCapabilityState::Eligible,
            )
            .await;

        let config = ServiceRuntimeConfig {
            id: "real-service".to_string(),
            plugin_id: "plugin".to_string(),
            name: None,
            command: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "while IFS= read -r line; do printf '%s\\n' \"$line\" >> \"$1\"; done".to_string(),
                "bamboo-event-sink-test".to_string(),
                output.to_string_lossy().into_owned(),
            ],
            cwd: None,
            env: HashMap::new(),
            health_check: HealthCheckSpec::default(),
            restart_policy: ReconnectConfig {
                enabled: false,
                ..ReconnectConfig::default()
            },
            graceful_shutdown: GracefulShutdown::default(),
            input_protocol: ServiceInputProtocol::NdjsonV1,
            user_config_path: temp.path().join("config.json"),
        };

        manager.start_service(config.clone()).await.unwrap();
        let first_generation = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = router.status_for_ids(&["real".to_string()]).await;
                if let Some(generation) = status.first().and_then(|status| status.generation) {
                    break generation;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first service generation becomes routeable");
        router.try_publish(event("Write", "first", 8)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::fs::read_to_string(&output)
                    .await
                    .is_ok_and(|raw| raw.contains("first"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first generation receives event");

        manager.stop_service("real-service").await.unwrap();
        manager.start_service(config).await.unwrap();
        let second_generation = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = router.status_for_ids(&["real".to_string()]).await;
                if let Some(generation) = status.first().and_then(|status| status.generation) {
                    if generation > first_generation {
                        break generation;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replacement service generation becomes routeable");
        assert!(second_generation > first_generation);
        router.try_publish(event("Write", "second", 8)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::fs::read_to_string(&output)
                    .await
                    .is_ok_and(|raw| raw.contains("second"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replacement generation receives event");

        router.unregister_sinks(&["real".to_string()]).await;
        manager.stop_service("real-service").await.unwrap();
    }
}
