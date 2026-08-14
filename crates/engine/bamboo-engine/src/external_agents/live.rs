//! Live actor registry: in-band delivery to currently-running actor children.
//!
//! While `ActorChildRunner` drives a child over WebSocket, it registers a frame
//! sender here keyed by `child_session_id`. `send_message` (running, no
//! interrupt) consults this map: when the child is live, the message rides the
//! existing WS as a `ParentFrame::Message` and is admitted by the worker's
//! agent loop at its next round boundary — the same mechanism in-process
//! children use, extended across the process boundary. When the child is not
//! live, callers fall back to the durable `pending_injected_messages` queue.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use bamboo_agent_core::AgentEvent;
use bamboo_domain::poison::PoisonRecover;
use bamboo_subagent::proto::ParentFrame;
use tokio::sync::mpsc;

use super::approval_registry::{
    ApprovalDecisionCasResult, ApprovalRegistry, ApprovalState, DurableApproval,
    SharedApprovalRegistry,
};

type ScopeId = usize;
type LiveKey = (ScopeId, String, u32);
type PendingKey = (ScopeId, String, String, u32, String);

fn scope_id(registry: Option<&SharedApprovalRegistry>) -> ScopeId {
    registry.map_or(0, |registry| registry.lock().recover_poison().scope_id())
}

fn map() -> &'static Mutex<HashMap<LiveKey, mpsc::UnboundedSender<ParentFrame>>> {
    static MAP: OnceLock<Mutex<HashMap<LiveKey, mpsc::UnboundedSender<ParentFrame>>>> =
        OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Process-global registry of pending human-loop approval requests, keyed by
/// `child_id` → set of `request_id`s currently awaiting a decision. Only the
/// human-in-the-loop path (top orchestrator) registers here; trusted internal
/// paths (model-review, escalation-bridge) do NOT — so the external handler's
/// [`deliver_approval_checked`] correctly rejects any stray external POST aimed
/// at a request that isn't a genuinely-pending human-loop one.
#[derive(Clone)]
struct PendingApproval {
    parent_session_id: String,
    tool_name: String,
    permission: String,
    resource: String,
    created_at: String,
    version: u64,
    child_attempt: u32,
    event_tx: mpsc::Sender<AgentEvent>,
}

fn pending() -> &'static Mutex<HashMap<PendingKey, PendingApproval>> {
    static PENDING: OnceLock<Mutex<HashMap<PendingKey, PendingApproval>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Configure durable approval storage and fail-close records whose live
/// transport was lost across restart.
pub fn initialize_durable_approvals(
    path: std::path::PathBuf,
) -> std::io::Result<(SharedApprovalRegistry, Vec<AgentEvent>)> {
    let mut registry = ApprovalRegistry::open(path)?;
    let reconciled = registry.reconcile_restart()?;
    let events = reconciled.into_iter().map(record_event).collect();
    Ok((std::sync::Arc::new(Mutex::new(registry)), events))
}

/// Named identity, audit, and delivery inputs for one observed approval.
pub struct PendingApprovalObservation<'a> {
    pub registry: Option<&'a SharedApprovalRegistry>,
    pub parent_session_id: &'a str,
    pub child_id: &'a str,
    pub child_attempt: u32,
    pub request_id: &'a str,
    pub tool_name: &'a str,
    pub permission: &'a str,
    pub resource: &'a str,
    pub event_tx: mpsc::Sender<AgentEvent>,
}

/// Record a `(child_id, request_id)` as a pending human-loop approval. Called
/// just before surfacing `ChildApprovalRequested` so an external POST can be
/// correlated against a genuinely-pending request.
pub fn observe_pending_approval(observation: PendingApprovalObservation<'_>) -> (u64, String) {
    let PendingApprovalObservation {
        registry,
        parent_session_id,
        child_id,
        child_attempt,
        request_id,
        tool_name,
        permission,
        resource,
        event_tx,
    } = observation;
    let now = chrono::Utc::now();
    let version = now.timestamp_micros().max(0) as u64;
    let created_at = now.to_rfc3339();
    let durable_record = DurableApproval {
        parent_session_id: parent_session_id.to_string(),
        child_session_id: child_id.to_string(),
        child_attempt,
        request_id: request_id.to_string(),
        tool_name: tool_name.to_string(),
        permission: permission.to_string(),
        resource: resource.to_string(),
        created_at: created_at.clone(),
        updated_at: created_at.clone(),
        version,
        state: ApprovalState::Pending,
        approved: None,
        reason: None,
    };
    if let Some(registry) = registry {
        if let Err(error) = registry.lock().recover_poison().register(durable_record) {
            tracing::error!("failed to persist pending child approval: {error}");
            return (0, created_at);
        }
    }
    pending().lock().recover_poison().insert(
        (
            scope_id(registry),
            parent_session_id.to_string(),
            child_id.to_string(),
            child_attempt,
            request_id.to_string(),
        ),
        PendingApproval {
            parent_session_id: parent_session_id.to_string(),
            tool_name: tool_name.to_string(),
            permission: permission.to_string(),
            resource: resource.to_string(),
            created_at: created_at.clone(),
            version,
            child_attempt,
            event_tx,
        },
    );
    (version, created_at)
}

/// Backward-compatible positional wrapper for existing engine consumers.
///
/// New call sites should use [`observe_pending_approval`] so each approval
/// identity and audit field is named at the call site.
#[allow(
    clippy::too_many_arguments,
    reason = "public compatibility wrapper; the typed observation is the canonical API"
)]
pub fn register_pending_approval_observed(
    registry: Option<&SharedApprovalRegistry>,
    parent_session_id: &str,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
    tool_name: &str,
    permission: &str,
    resource: &str,
    event_tx: mpsc::Sender<AgentEvent>,
) -> (u64, String) {
    observe_pending_approval(PendingApprovalObservation {
        registry,
        parent_session_id,
        child_id,
        child_attempt,
        request_id,
        tool_name,
        permission,
        resource,
        event_tx,
    })
}

#[cfg(test)]
fn register_pending_approval(child_id: &str, request_id: &str) {
    let (event_tx, _rx) = mpsc::channel(1);
    let _ = observe_pending_approval(PendingApprovalObservation {
        registry: None,
        parent_session_id: "test-parent",
        child_id,
        child_attempt: 0,
        request_id,
        tool_name: "test-tool",
        permission: "test-permission",
        resource: "test-resource",
        event_tx,
    });
}

/// One-shot consume of a `(child_id, request_id)` pending pair: remove it and
/// return whether it WAS present. A second call for the same pair returns
/// `false`, so a request can't be answered (or replayed) twice.
pub fn take_pending_approval(child_id: &str, request_id: &str) -> bool {
    remove_unique_pending(None, child_id, request_id).is_some()
}

/// Drop all pending approvals for a child (e.g. when its live connection ends).
pub fn clear_pending_approvals_for(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    child_attempt: u32,
) {
    let records: Vec<_> = {
        let mut guard = pending().lock().recover_poison();
        let keys: Vec<_> = guard
            .keys()
            .filter(|(scope, _, child, attempt, _)| {
                *scope == scope_id(registry) && child == child_id && *attempt == child_attempt
            })
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| guard.remove(&key).map(|record| (key.4, record)))
            .collect()
    };
    for (request_id, record) in records {
        let durable = finish_durable(
            registry,
            &record.parent_session_id,
            child_id,
            record.child_attempt,
            &request_id,
            false,
            Some("child_disconnected"),
        );
        if registry.is_none() || durable.is_some() {
            emit_resolution(
                child_id,
                &request_id,
                record,
                "delivery_failed",
                Some("child_disconnected"),
                durable.as_ref(),
            );
        }
    }
}

pub fn expire_pending_approval(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    request_id: &str,
) -> bool {
    let record = remove_unique_pending(registry, child_id, request_id);
    let Some(record) = record else {
        return false;
    };
    let durable = finish_durable(
        registry,
        &record.parent_session_id,
        child_id,
        record.child_attempt,
        request_id,
        false,
        Some("approval_timeout"),
    );
    if registry.is_none() || durable.is_some() {
        emit_resolution(
            child_id,
            request_id,
            record,
            "expired",
            Some("approval_timeout"),
            durable.as_ref(),
        );
    }
    true
}

fn emit_resolution(
    child_id: &str,
    request_id: &str,
    record: PendingApproval,
    status: &str,
    reason: Option<&str>,
    durable: Option<&DurableApproval>,
) {
    let now = chrono::Utc::now();
    let event = AgentEvent::ChildApprovalChanged {
        parent_session_id: record.parent_session_id,
        child_session_id: child_id.to_string(),
        child_attempt: record.child_attempt,
        request_id: request_id.to_string(),
        version: durable.map_or_else(
            || (now.timestamp_micros().max(0) as u64).max(record.version.saturating_add(1)),
            |record| record.version,
        ),
        status: status.to_string(),
        reason: reason.map(str::to_string),
        tool_name: record.tool_name,
        permission: record.permission,
        resource: record.resource,
        created_at: record.created_at,
        resolved_at: Some(
            durable
                .map(|record| record.updated_at.clone())
                .unwrap_or_else(|| now.to_rfc3339()),
        ),
    };
    match record.event_tx.try_send(event) {
        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        Err(mpsc::error::TrySendError::Full(event)) => {
            let tx = record.event_tx;
            tokio::spawn(async move {
                let _ = tx.send(event).await;
            });
        }
    }
}

/// Validated external entry point: deliver an approval decision ONLY if the
/// `(child_id, request_id)` pair is currently pending. Consumes the pending
/// entry (one-shot) before delivering, so the same request can't be replayed,
/// and rejects (returns `false`) any `request_id` that isn't currently pending
/// — unknown, already-answered/timed-out, or a non-human-loop path
/// (model-review / escalation) that never registered. This is the entry the
/// external HTTP handler must use.
pub fn deliver_approval_checked(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    request_id: &str,
    approved: bool,
) -> bool {
    let Some((_, record)) = find_unique_pending(registry, child_id, request_id) else {
        return false;
    };
    deliver_approval_checked_cas(
        registry,
        &record.parent_session_id,
        child_id,
        record.child_attempt,
        request_id,
        record.version,
        approved,
    ) == ApprovalDeliveryResult::Delivered
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDeliveryResult {
    Delivered,
    NotFound,
    Conflict,
    DeliveryFailed,
}

/// Versioned external entry point used by typed HTTP clients. The complete
/// durable identity is checked against both live pending state and the durable
/// registry before either is mutated. Identity/version mismatches are reported
/// as conflicts and never fall back to another attempt with the same request id.
#[allow(
    clippy::too_many_arguments,
    reason = "the complete durable approval CAS identity must remain explicit"
)]
pub fn deliver_approval_checked_cas(
    registry: Option<&SharedApprovalRegistry>,
    parent_session_id: &str,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
    expected_version: u64,
    approved: bool,
) -> ApprovalDeliveryResult {
    let (key, record) = match find_pending_cas(
        registry,
        parent_session_id,
        child_id,
        child_attempt,
        request_id,
        expected_version,
    ) {
        Ok(found) => found,
        Err(result) => return result,
    };
    // Persist DecisionRecorded before touching the live transport. Duplicate or
    // concurrent decisions fail this transition and cannot deliver twice.
    if let Some(registry) = registry {
        match registry.lock().recover_poison().record_decision_cas(
            parent_session_id,
            child_id,
            child_attempt,
            request_id,
            expected_version,
            approved,
        ) {
            Ok(ApprovalDecisionCasResult::Recorded(_)) => {}
            Ok(ApprovalDecisionCasResult::Conflict) => return ApprovalDeliveryResult::Conflict,
            Ok(ApprovalDecisionCasResult::NotFound) => return ApprovalDeliveryResult::NotFound,
            Err(error) => {
                tracing::error!("failed to persist child approval decision: {error}");
                return ApprovalDeliveryResult::DeliveryFailed;
            }
        }
    }
    let Some(record) = pending().lock().recover_poison().remove(&key) else {
        let _ = finish_durable(
            registry,
            &record.parent_session_id,
            child_id,
            record.child_attempt,
            request_id,
            false,
            Some("pending_state_lost"),
        );
        return ApprovalDeliveryResult::DeliveryFailed;
    };
    let delivered = deliver_approval_scoped(
        registry,
        child_id,
        record.child_attempt,
        request_id,
        approved,
    );
    let status = if delivered {
        if approved {
            "approved"
        } else {
            "denied"
        }
    } else {
        "delivery_failed"
    };
    let durable = finish_durable(
        registry,
        &record.parent_session_id,
        child_id,
        record.child_attempt,
        request_id,
        delivered,
        (!delivered).then_some("child_not_live"),
    );
    if registry.is_none() || durable.is_some() {
        emit_resolution(
            child_id,
            request_id,
            record,
            status,
            (!delivered).then_some("child_not_live"),
            durable.as_ref(),
        );
    }
    if delivered {
        ApprovalDeliveryResult::Delivered
    } else {
        ApprovalDeliveryResult::DeliveryFailed
    }
}

fn finish_durable(
    registry: Option<&SharedApprovalRegistry>,
    parent_id: &str,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
    delivered: bool,
    reason: Option<&str>,
) -> Option<DurableApproval> {
    if let Some(registry) = registry {
        match registry.lock().recover_poison().finish(
            parent_id,
            child_id,
            child_attempt,
            request_id,
            delivered,
            reason,
        ) {
            Ok(record) => return record,
            Err(error) => {
                tracing::error!("failed to persist child approval resolution: {error}");
            }
        }
    }
    None
}

fn record_event(record: DurableApproval) -> AgentEvent {
    AgentEvent::ChildApprovalChanged {
        parent_session_id: record.parent_session_id,
        child_session_id: record.child_session_id,
        child_attempt: record.child_attempt,
        request_id: record.request_id,
        version: record.version,
        status: match record.state {
            ApprovalState::Pending => "pending",
            ApprovalState::DecisionRecorded => "decision_recorded",
            ApprovalState::Delivered if record.approved == Some(true) => "approved",
            ApprovalState::Delivered => "denied",
            ApprovalState::DeliveryFailed => "delivery_failed",
            ApprovalState::Expired => "expired",
        }
        .to_string(),
        reason: record.reason,
        tool_name: record.tool_name,
        permission: record.permission,
        resource: record.resource,
        created_at: record.created_at,
        resolved_at: Some(record.updated_at),
    }
}

/// Unregisters the child on drop, so a panicking/returning runner can't leak
/// a stale sender.
pub struct LiveActorGuard {
    scope_id: ScopeId,
    child_id: String,
    child_attempt: u32,
    approval_registry: Option<SharedApprovalRegistry>,
}

impl Drop for LiveActorGuard {
    fn drop(&mut self) {
        map().lock().recover_poison().remove(&(
            self.scope_id,
            self.child_id.clone(),
            self.child_attempt,
        ));
        // A disconnecting child can't answer any still-pending approval — drop
        // them so a late external POST finds nothing pending and is rejected.
        clear_pending_approvals_for(
            self.approval_registry.as_ref(),
            &self.child_id,
            self.child_attempt,
        );
    }
}

/// Register a live child's frame sender for the duration of its run.
pub fn register(
    child_id: &str,
    tx: mpsc::UnboundedSender<ParentFrame>,
    child_attempt: u32,
    approval_registry: Option<SharedApprovalRegistry>,
) -> LiveActorGuard {
    let scope_id = scope_id(approval_registry.as_ref());
    map()
        .lock()
        .recover_poison()
        .insert((scope_id, child_id.to_string(), child_attempt), tx);
    LiveActorGuard {
        scope_id,
        child_id: child_id.to_string(),
        child_attempt,
        approval_registry,
    }
}

fn find_unique_pending(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    request_id: &str,
) -> Option<(PendingKey, PendingApproval)> {
    let guard = pending().lock().recover_poison();
    let scope = scope_id(registry);
    let mut matches = guard
        .iter()
        .filter(|(key, _)| key.0 == scope && key.2 == child_id && key.4 == request_id);
    let (key, record) = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some((key.clone(), record.clone()))
}

fn find_pending_cas(
    registry: Option<&SharedApprovalRegistry>,
    parent_session_id: &str,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
    expected_version: u64,
) -> Result<(PendingKey, PendingApproval), ApprovalDeliveryResult> {
    let scope = scope_id(registry);
    let guard = pending().lock().recover_poison();
    let key = (
        scope,
        parent_session_id.to_string(),
        child_id.to_string(),
        child_attempt,
        request_id.to_string(),
    );
    if let Some(record) = guard.get(&key) {
        if record.parent_session_id != parent_session_id
            || record.child_attempt != child_attempt
            || record.version != expected_version
        {
            return Err(ApprovalDeliveryResult::Conflict);
        }
        return Ok((key, record.clone()));
    }
    if guard.keys().any(|candidate| {
        candidate.0 == scope && candidate.2 == child_id && candidate.4 == request_id
    }) {
        Err(ApprovalDeliveryResult::Conflict)
    } else {
        Err(ApprovalDeliveryResult::NotFound)
    }
}

fn remove_unique_pending(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    request_id: &str,
) -> Option<PendingApproval> {
    let mut guard = pending().lock().recover_poison();
    let scope = scope_id(registry);
    let mut keys = guard
        .keys()
        .filter(|(key_scope, _, child, _, request)| {
            *key_scope == scope && child == child_id && request == request_id
        })
        .cloned();
    let key = keys.next()?;
    if keys.next().is_some() {
        return None;
    }
    guard.remove(&key)
}

/// Deliver an in-band steering message to a live child. Returns `false` when
/// the child is not live (caller should use the durable queue instead).
pub fn deliver_message(child_id: &str, text: &str) -> bool {
    let guard = map().lock().recover_poison();
    let mut senders = guard
        .iter()
        .filter(|((_, child, _), _)| child == child_id)
        .map(|(_, sender)| sender);
    let Some(tx) = senders.next() else {
        return false;
    };
    if senders.next().is_some() {
        return false;
    }
    tx.send(ParentFrame::Message {
        text: text.to_string(),
    })
    .is_ok()
}

/// Deliver a host/human approval decision to a live child's pending gated-tool
/// request (Phase 2: child → parent approval delegation). Sends
/// `ParentFrame::ApprovalReply{id, approved}` over the child's live WS
/// connection; `drive()` forwards it to the worker, whose pending map resolves
/// the `host.approval_call` the child's gated tool is blocked on (approve ⇒ the
/// tool proceeds, deny ⇒ it fails closed). This is the decision-DOWN half of the
/// human-in-the-loop route: a parent-side responder (e.g. a `/respond`-style
/// handler) calls this with the `request_id` it surfaced to the human. Returns
/// `false` when the child is not live (no connection to answer on — the caller
/// should treat that as a denied/expired request).
pub fn deliver_approval(child_id: &str, request_id: &str, approved: bool) -> bool {
    deliver_approval_scoped(None, child_id, 0, request_id, approved)
}

pub fn deliver_approval_scoped(
    registry: Option<&SharedApprovalRegistry>,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
    approved: bool,
) -> bool {
    let guard = map().lock().recover_poison();
    match guard.get(&(scope_id(registry), child_id.to_string(), child_attempt)) {
        Some(tx) => tx
            .send(ParentFrame::ApprovalReply {
                id: request_id.to_string(),
                approved,
            })
            .is_ok(),
        None => false,
    }
}

/// Whether a child currently has a live actor connection.
pub fn is_live(child_id: &str) -> bool {
    map()
        .lock()
        .recover_poison()
        .keys()
        .any(|(_, child, _)| child == child_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> SharedApprovalRegistry {
        std::sync::Arc::new(Mutex::new(
            ApprovalRegistry::open(tempfile::tempdir().unwrap().keep().join("registry.json"))
                .unwrap(),
        ))
    }

    #[test]
    fn register_deliver_unregister() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let guard = register("c-live", tx, 0, None);
        assert!(is_live("c-live"));
        assert!(deliver_message("c-live", "hi"));
        match rx.try_recv() {
            Ok(ParentFrame::Message { text }) => assert_eq!(text, "hi"),
            other => panic!("expected message frame, got {other:?}"),
        }

        drop(guard);
        assert!(!is_live("c-live"));
        assert!(!deliver_message("c-live", "gone"));
    }

    #[test]
    fn deliver_fails_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel();
        let _guard = register("c-dead", tx, 0, None);
        drop(rx);
        assert!(!deliver_message("c-dead", "hi"));
    }

    #[test]
    fn deliver_approval_routes_reply_frame() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let guard = register("c-appr", tx, 0, None);
        assert!(deliver_approval("c-appr", "req-7", true));
        match rx.try_recv() {
            Ok(ParentFrame::ApprovalReply { id, approved }) => {
                assert_eq!(id, "req-7");
                assert!(approved);
            }
            other => panic!("expected approval reply, got {other:?}"),
        }
        drop(guard);
        // Not-live child ⇒ false (no connection to answer on).
        assert!(!deliver_approval("c-appr", "req-8", false));
    }

    #[test]
    fn app_scopes_and_attempt_guards_do_not_cross_talk() {
        let first_registry = registry();
        let second_registry = registry();
        let (first_tx, mut first_rx) = mpsc::unbounded_channel();
        let (retry_tx, mut retry_rx) = mpsc::unbounded_channel();
        let (second_tx, mut second_rx) = mpsc::unbounded_channel();
        let old_guard = register("shared-child", first_tx, 1, Some(first_registry.clone()));
        let retry_guard = register("shared-child", retry_tx, 2, Some(first_registry.clone()));
        let second_guard = register("shared-child", second_tx, 1, Some(second_registry.clone()));

        drop(old_guard);
        assert!(deliver_approval_scoped(
            Some(&first_registry),
            "shared-child",
            2,
            "retry-request",
            true,
        ));
        assert!(matches!(
            retry_rx.try_recv(),
            Ok(ParentFrame::ApprovalReply { id, .. }) if id == "retry-request"
        ));
        assert!(deliver_approval_scoped(
            Some(&second_registry),
            "shared-child",
            1,
            "other-app-request",
            false,
        ));
        assert!(matches!(
            second_rx.try_recv(),
            Ok(ParentFrame::ApprovalReply { id, .. }) if id == "other-app-request"
        ));
        assert!(first_rx.try_recv().is_err());
        drop(retry_guard);
        drop(second_guard);
    }

    #[test]
    fn pending_approval_is_one_shot() {
        register_pending_approval("c-pend", "req-1");
        // First take consumes it; the second finds nothing.
        assert!(take_pending_approval("c-pend", "req-1"));
        assert!(!take_pending_approval("c-pend", "req-1"));
    }

    #[test]
    fn take_of_unregistered_pair_is_false() {
        // Unknown child entirely.
        assert!(!take_pending_approval("c-unknown", "req-x"));
        // Known child, but an unregistered request_id.
        register_pending_approval("c-known", "req-real");
        assert!(!take_pending_approval("c-known", "req-bogus"));
        // The real one is still pending (a bogus take didn't disturb it).
        assert!(take_pending_approval("c-known", "req-real"));
    }

    #[test]
    fn deliver_approval_checked_only_delivers_for_registered_pair() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _guard = register("c-checked", tx, 0, None);

        // Not registered ⇒ rejected, nothing on the wire.
        assert!(!deliver_approval_checked(
            None,
            "c-checked",
            "req-stray",
            true
        ));
        assert!(rx.try_recv().is_err());

        // Registered ⇒ delivered, frame rides the wire, and consumed.
        register_pending_approval("c-checked", "req-ok");
        assert!(deliver_approval_checked(None, "c-checked", "req-ok", true));
        match rx.try_recv() {
            Ok(ParentFrame::ApprovalReply { id, approved }) => {
                assert_eq!(id, "req-ok");
                assert!(approved);
            }
            other => panic!("expected approval reply, got {other:?}"),
        }
        // One-shot: a replay is rejected (and nothing further on the wire).
        assert!(!deliver_approval_checked(None, "c-checked", "req-ok", true));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn clear_pending_approvals_for_drops_them() {
        register_pending_approval("c-clear", "req-a");
        register_pending_approval("c-clear", "req-b");
        clear_pending_approvals_for(None, "c-clear", 0);
        assert!(!take_pending_approval("c-clear", "req-a"));
        assert!(!take_pending_approval("c-clear", "req-b"));
    }

    #[tokio::test]
    async fn observed_approval_emits_exactly_one_terminal_outcome() {
        let (wire_tx, _wire_rx) = mpsc::unbounded_channel();
        let _guard = register("c-audit", wire_tx, 0, None);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        observe_pending_approval(PendingApprovalObservation {
            registry: None,
            parent_session_id: "parent-audit",
            child_id: "c-audit",
            child_attempt: 0,
            request_id: "req-audit",
            tool_name: "Bash",
            permission: "execute",
            resource: "/tmp/x",
            event_tx,
        });

        assert!(deliver_approval_checked(
            None,
            "c-audit",
            "req-audit",
            false
        ));
        assert!(!deliver_approval_checked(
            None,
            "c-audit",
            "req-audit",
            true
        ));
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "denied"
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn durable_resolution_uses_registry_version_and_attempt() {
        let registry = registry();
        let (wire_tx, _wire_rx) = mpsc::unbounded_channel();
        let _guard = register("c-versioned", wire_tx, 7, Some(registry.clone()));
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let (pending_version, _) = observe_pending_approval(PendingApprovalObservation {
            registry: Some(&registry),
            parent_session_id: "parent-versioned",
            child_id: "c-versioned",
            child_attempt: 7,
            request_id: "req-versioned",
            tool_name: "Bash",
            permission: "execute",
            resource: "/tmp/versioned",
            event_tx,
        });

        assert!(deliver_approval_checked(
            Some(&registry),
            "c-versioned",
            "req-versioned",
            true,
        ));
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged {
                child_attempt: 7,
                version,
                status,
                ..
            }) if version == pending_version + 2 && status == "approved"
        ));
    }

    #[tokio::test]
    async fn delayed_attempt_one_decision_cannot_approve_attempt_two() {
        let registry = registry();
        let child_id = "c-delayed-attempt";
        let request_id = "req-reused";
        let (attempt_one_tx, mut attempt_one_rx) = mpsc::unbounded_channel();
        let attempt_one_guard = register(child_id, attempt_one_tx, 1, Some(registry.clone()));
        let (event_tx, _event_rx) = mpsc::channel(8);
        let (attempt_one_version, _) = observe_pending_approval(PendingApprovalObservation {
            registry: Some(&registry),
            parent_session_id: "parent-delayed",
            child_id,
            child_attempt: 1,
            request_id,
            tool_name: "Bash",
            permission: "execute",
            resource: "cargo test",
            event_tx: event_tx.clone(),
        });
        clear_pending_approvals_for(Some(&registry), child_id, 1);

        let (attempt_two_tx, mut attempt_two_rx) = mpsc::unbounded_channel();
        let _attempt_two_guard = register(child_id, attempt_two_tx, 2, Some(registry.clone()));
        let (attempt_two_version, _) = observe_pending_approval(PendingApprovalObservation {
            registry: Some(&registry),
            parent_session_id: "parent-delayed",
            child_id,
            child_attempt: 2,
            request_id,
            tool_name: "Bash",
            permission: "execute",
            resource: "cargo test",
            event_tx,
        });

        assert_eq!(
            deliver_approval_checked_cas(
                Some(&registry),
                "parent-delayed",
                child_id,
                1,
                request_id,
                attempt_one_version,
                true,
            ),
            ApprovalDeliveryResult::Conflict
        );
        assert!(attempt_one_rx.try_recv().is_err());
        assert!(attempt_two_rx.try_recv().is_err());

        assert_eq!(
            deliver_approval_checked_cas(
                Some(&registry),
                "parent-delayed",
                child_id,
                2,
                request_id,
                attempt_two_version,
                true,
            ),
            ApprovalDeliveryResult::Delivered
        );
        assert!(matches!(
            attempt_two_rx.try_recv(),
            Ok(ParentFrame::ApprovalReply { id, approved }) if id == request_id && approved
        ));
        drop(attempt_one_guard);
    }

    #[tokio::test]
    async fn parent_and_version_mismatches_do_not_consume_current_approval() {
        let registry = registry();
        let child_id = "c-identity-cas";
        let request_id = "req-identity-cas";
        let (wire_tx, mut wire_rx) = mpsc::unbounded_channel();
        let _guard = register(child_id, wire_tx, 3, Some(registry.clone()));
        let (event_tx, _event_rx) = mpsc::channel(4);
        let (version, _) = observe_pending_approval(PendingApprovalObservation {
            registry: Some(&registry),
            parent_session_id: "parent-current",
            child_id,
            child_attempt: 3,
            request_id,
            tool_name: "Write",
            permission: "write",
            resource: "/tmp/current",
            event_tx,
        });

        assert_eq!(
            deliver_approval_checked_cas(
                Some(&registry),
                "parent-stale",
                child_id,
                3,
                request_id,
                version,
                true,
            ),
            ApprovalDeliveryResult::Conflict
        );
        assert_eq!(
            deliver_approval_checked_cas(
                Some(&registry),
                "parent-current",
                child_id,
                3,
                request_id,
                version.saturating_add(1),
                true,
            ),
            ApprovalDeliveryResult::Conflict
        );
        assert!(wire_rx.try_recv().is_err());

        assert_eq!(
            deliver_approval_checked_cas(
                Some(&registry),
                "parent-current",
                child_id,
                3,
                request_id,
                version,
                false,
            ),
            ApprovalDeliveryResult::Delivered
        );
        assert!(matches!(
            wire_rx.try_recv(),
            Ok(ParentFrame::ApprovalReply { id, approved }) if id == request_id && !approved
        ));
    }

    #[tokio::test]
    async fn timeout_and_disconnect_emit_terminal_outcomes() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        observe_pending_approval(PendingApprovalObservation {
            registry: None,
            parent_session_id: "parent-audit",
            child_id: "c-expire",
            child_attempt: 0,
            request_id: "req-expire",
            tool_name: "Bash",
            permission: "execute",
            resource: "/tmp/x",
            event_tx: event_tx.clone(),
        });
        assert!(expire_pending_approval(None, "c-expire", "req-expire"));
        assert!(!expire_pending_approval(None, "c-expire", "req-expire"));
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "expired"
        ));

        observe_pending_approval(PendingApprovalObservation {
            registry: None,
            parent_session_id: "parent-audit",
            child_id: "c-disconnect",
            child_attempt: 0,
            request_id: "req-disconnect",
            tool_name: "Write",
            permission: "write",
            resource: "/tmp/y",
            event_tx,
        });
        clear_pending_approvals_for(None, "c-disconnect", 0);
        assert!(matches!(
            event_rx.recv().await,
            Some(AgentEvent::ChildApprovalChanged { status, .. }) if status == "delivery_failed"
        ));
    }
}
