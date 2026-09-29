//! Reconcile fixed-deadline direct-parent forced permission requests.
//!
//! The canonical parent transcript is the only source of pending requests and
//! terminal decisions. A sweep can record a deny after the deadline, but it
//! cannot reconstruct a lost live approval scope or authorize a child tool.

use std::{sync::Arc, time::Duration};

use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ActorSession, ParentRequest, Session, SessionMessageEnvelope};
use bamboo_engine::SessionRepository;
use bamboo_storage::SessionStoreV2;
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::parent_permission_outcome::{self as outcome, State};

const MAX_DEADLINE_SLEEP: Duration = Duration::from_secs(240);
const INITIAL_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(60);
const SUBSYSTEM: &str = "direct_parent_permission_review";
const REQUEST: &str = "direct_parent_forced_permission_request_v1";

#[derive(Default)]
pub(super) struct ReconcileReport {
    pub denied: usize,
    pub errors: usize,
}

fn pending_requests(parent: &Session) -> Vec<(ParentRequest, SessionMessageEnvelope)> {
    let Some(parent_actor) = ActorSession::from_session(parent).ok() else {
        return Vec::new();
    };
    parent
        .messages
        .iter()
        .filter_map(|message| {
            let marker = message.metadata.as_ref()?.get("session_message")?;
            if marker.pointer("/source/subsystem").and_then(|v| v.as_str()) != Some(SUBSYSTEM)
                || marker.pointer("/body/instruction").and_then(|v| v.as_str()) != Some(REQUEST)
            {
                return None;
            }
            let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
            let request = ParentRequest::from_forced_permission_envelope(&envelope)?;
            if request.parent.session_id != parent.id
                || request.parent.created_at != parent.created_at
                || request.root_session_id != parent_actor.root_actor_id
                || request.project_id != parent_actor.project_id
                || outcome::state(parent, &envelope) != Ok(State::Pending)
            {
                return None;
            }
            Some((request, envelope))
        })
        .collect()
}

/// Read-only diagnostic projection for one exact, directly owned Child. The
/// canonical parent transcript is the source of truth; a live reviewer map is
/// not sufficient after a restart. No request body or operation is exposed.
pub(crate) fn pending_for_child(parent: &Session, child: &Session) -> Option<Vec<ParentRequest>> {
    let parent_actor = ActorSession::from_session(parent).ok()?;
    let child_actor = ActorSession::from_session(child).ok()?;
    if child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
        || child_actor.root_actor_id != parent_actor.root_actor_id
        || child_actor.project_id != parent_actor.project_id
        || parent_actor.spawn_depth.checked_add(1) != Some(child_actor.spawn_depth)
    {
        return None;
    }
    Some(
        pending_requests(parent)
            .into_iter()
            .filter_map(|(request, _)| {
                (request.child.session_id == child.id
                    && request.child.created_at == child.created_at
                    && request.parent.session_id == parent.id
                    && request.parent.created_at == parent.created_at
                    && request.root_session_id == child_actor.root_actor_id
                    && request.project_id == child_actor.project_id)
                    .then_some(request)
            })
            .take(9)
            .collect(),
    )
}

/// Safe to repeat after a crash or concurrent review. `expire` rechecks the
/// deadline and Pending state under the canonical parent mutation lock.
async fn scan_startup(
    store: &SessionStoreV2,
    sessions: &SessionRepository,
    schedule: Option<(&Arc<SessionStoreV2>, &CancellationToken)>,
) -> ReconcileReport {
    let mut report = ReconcileReport::default();
    for entry in store.list_index_entries().await {
        let parent = match store.load_session(&entry.id).await {
            Ok(Some(parent)) => parent,
            Ok(None) => {
                report.errors += 1;
                tracing::warn!(session_id = %entry.id,
                    "indexed parent Session is temporarily missing during forced permission reconciliation");
                if let Some((store, shutdown)) = schedule {
                    spawn_parent_load_retry(store, sessions, entry.id, shutdown.clone());
                }
                continue;
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(session_id = %entry.id, %error,
                    "could not read parent Session during forced permission reconciliation");
                if let Some((store, shutdown)) = schedule {
                    spawn_parent_load_retry(store, sessions, entry.id, shutdown.clone());
                }
                continue;
            }
        };
        reconcile_parent(&parent, sessions, schedule, &mut report).await;
    }
    report
}

async fn reconcile_parent(
    parent: &Session,
    sessions: &SessionRepository,
    schedule: Option<(&Arc<SessionStoreV2>, &CancellationToken)>,
    report: &mut ReconcileReport,
) {
    for (typed, request) in pending_requests(parent) {
        if typed.deadline > Utc::now() {
            if let Some((store, shutdown)) = schedule {
                spawn_deadline(store, sessions, request, shutdown.clone());
            }
            continue;
        }
        match outcome::expire(sessions, &request).await {
            Ok(State::Terminal(false)) => report.denied += 1,
            Ok(State::Terminal(true)) => {}
            Ok(State::Pending | State::Missing) | Err(()) => {
                report.errors += 1;
                tracing::warn!(parent_session_id = %parent.id,
                    request_id = %request.id,
                    "forced permission deadline resolution is unconfirmed");
                if let Some((store, shutdown)) = schedule {
                    spawn_deadline(store, sessions, request, shutdown.clone());
                }
            }
        }
    }
}

/// One startup census followed by narrow, exact-deadline timers. It is run
/// before SessionInbox activation recovery and never grants permission.
pub(super) async fn reconcile_startup(
    store: &Arc<SessionStoreV2>,
    sessions: &SessionRepository,
    shutdown: &CancellationToken,
) -> ReconcileReport {
    scan_startup(store, sessions, Some((store, shutdown))).await
}

#[cfg(test)]
async fn reconcile_once(store: &SessionStoreV2, sessions: &SessionRepository) -> ReconcileReport {
    scan_startup(store, sessions, None).await
}

/// Schedule one durable Pending request, using only its canonical parent
/// Session at wakeup. The reviewer calls this after Pending is committed; the
/// boot census covers a crash between that commit and this registration.
/// Sleep and retries stop when AppState shuts down. Storage errors retry with
/// capped backoff; no in-memory timer is an authority for the decision.
pub(super) fn spawn_deadline(
    store: &Arc<SessionStoreV2>,
    sessions: &SessionRepository,
    request: SessionMessageEnvelope,
    shutdown: CancellationToken,
) {
    let Some(typed) = ParentRequest::from_forced_permission_envelope(&request) else {
        return;
    };
    let store = Arc::downgrade(store);
    let persistence = Arc::downgrade(sessions.persistence());
    let cache = Arc::downgrade(sessions.cache());
    tokio::spawn(async move {
        let mut retry_interval = INITIAL_RETRY_INTERVAL;
        loop {
            let remaining = (typed.deadline - Utc::now()).to_std().unwrap_or_default();
            if !remaining.is_zero() {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(remaining.min(MAX_DEADLINE_SLEEP)) => {}
                }
                continue;
            }
            if shutdown.is_cancelled() {
                break;
            }
            let result = {
                let (Some(store), Some(persistence), Some(cache)) =
                    (store.upgrade(), persistence.upgrade(), cache.upgrade())
                else {
                    break;
                };
                let storage: Arc<dyn Storage> = store;
                let sessions = SessionRepository::new(cache, storage, persistence);
                outcome::expire(&sessions, &request).await
            };
            match result {
                Ok(State::Terminal(_)) => break,
                Ok(State::Pending) => {
                    // A backwards clock shift can make the lock's deadline
                    // check reject an apparently due request. Re-evaluate.
                    continue;
                }
                Ok(State::Missing) | Err(()) => {
                    tracing::warn!(parent_session_id = %request.target_session_id,
                        request_id = %request.id,
                        "forced permission deadline persistence unconfirmed; retrying");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(retry_interval) => {}
                    }
                    retry_interval = (retry_interval * 2).min(MAX_RETRY_INTERVAL);
                }
            }
        }
    });
}

/// A startup read failure has no known request IDs to schedule. Retry only
/// this Session, then install its exact request timers. The global index is
/// read once at boot and is never polled by this reconciler.
fn spawn_parent_load_retry(
    store: &Arc<SessionStoreV2>,
    sessions: &SessionRepository,
    parent_id: String,
    shutdown: CancellationToken,
) {
    let store = Arc::downgrade(store);
    let persistence = Arc::downgrade(sessions.persistence());
    let cache = Arc::downgrade(sessions.cache());
    tokio::spawn(async move {
        let mut retry_interval = INITIAL_RETRY_INTERVAL;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(retry_interval) => {}
            }
            let (Some(store), Some(persistence), Some(cache)) =
                (store.upgrade(), persistence.upgrade(), cache.upgrade())
            else {
                break;
            };
            match store.load_session(&parent_id).await {
                Ok(Some(parent)) => {
                    let storage: Arc<dyn Storage> = store.clone();
                    let sessions = SessionRepository::new(cache, storage, persistence);
                    let mut report = ReconcileReport::default();
                    reconcile_parent(&parent, &sessions, Some((&store, &shutdown)), &mut report)
                        .await;
                    break;
                }
                Ok(None) => {
                    tracing::warn!(%parent_id,
                        "indexed forced permission parent still missing; retrying");
                    retry_interval = (retry_interval * 2).min(MAX_RETRY_INTERVAL);
                }
                Err(error) => {
                    tracing::warn!(%parent_id, %error,
                        "forced permission parent read still failing; retrying");
                    retry_interval = (retry_interval * 2).min(MAX_RETRY_INTERVAL);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ParentResolution;
    use bamboo_engine::external_agents::actor_adapter::ChildApprovalScope;
    use bamboo_storage::LockedSessionStore;
    use bamboo_tools::permission::{
        PermissionMode, PermissionReasonCode, PermissionRequest, PermissionType, RiskLevel,
    };
    use std::sync::atomic::AtomicU64;
    use tokio_util::sync::CancellationToken;

    struct Seed {
        home: tempfile::TempDir,
        store: Arc<SessionStoreV2>,
        sessions: SessionRepository,
        request: SessionMessageEnvelope,
    }

    async fn seed_request(deadline_from_setup: chrono::Duration, running_child: bool) -> Seed {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut parent = Session::new("permission-parent", "model");
        parent.created_at = Utc::now() - chrono::Duration::minutes(8);
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("permission-child", &parent, "model", "Child");
        child.created_at = Utc::now() - chrono::Duration::minutes(7);
        if running_child {
            child.set_last_run_status("running");
            child.updated_at = Utc::now() - chrono::Duration::minutes(1);
        }
        store.save_session(&child).await.unwrap();

        let permission = PermissionRequest {
            request_id: "old-forced-call".into(),
            request_generation: PermissionRequest::fresh_generation(),
            session_id: child.id.clone(),
            workspace_path: None,
            tool_name: "Write".into(),
            permission_type: PermissionType::WriteFile,
            resource: "/tmp/permission-reconcile-fixture".into(),
            operation_summary: "fixture exact write".into(),
            risk_level: RiskLevel::Medium,
            reason_code: PermissionReasonCode::ConfiguredAlwaysAsk,
            effective_mode: PermissionMode::BypassPermissions,
            bypass_requested: true,
            auto_approve_requested: false,
            policy_revision: 0,
            matched_rule: None,
            allowed_decisions: PermissionRequest::forced_decisions(),
            suggested_matchers: vec![],
        };
        let child_actor = ActorSession::from_session(&child).unwrap();
        let parent_actor = ActorSession::from_session(&parent).unwrap();
        let (mut request, _) =
            super::super::parent_permission_request::envelope(&parent, &child_actor, &permission)
                .unwrap();
        let router = bamboo_engine::SessionActivationRouter::new();
        let _run = router.register_run(&child.id, "old-run").await.unwrap();
        let deadline = Utc::now() + deadline_from_setup;
        let scope = ChildApprovalScope::new(
            &parent.id,
            &child.id,
            (0, "old-run", 1, "old-reply", deadline),
            router,
            CancellationToken::new(),
            Arc::new(AtomicU64::new(1)),
        );
        super::super::parent_permission_outcome::bind(
            &mut request,
            &scope,
            &[child_actor, parent_actor],
        )
        .unwrap();
        let persistence = Arc::new(LockedSessionStore::new(store.clone()));
        let sessions = SessionRepository::new(Default::default(), store.clone(), persistence);
        assert_eq!(
            outcome::commit(&sessions, &request, None, true).await,
            Ok(State::Pending)
        );
        Seed {
            home,
            store,
            sessions,
            request,
        }
    }

    #[tokio::test]
    async fn pending_diagnostics_follow_exact_saved_child_birth_and_terminal() {
        let seed = seed_request(chrono::Duration::seconds(-1), false).await;
        let parent = seed
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        let child = seed
            .store
            .load_session("permission-child")
            .await
            .unwrap()
            .unwrap();
        let pending = pending_for_child(&parent, &child).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, seed.request.id);

        let mut replacement = child.clone();
        replacement.created_at += chrono::Duration::seconds(1);
        assert!(pending_for_child(&parent, &replacement).unwrap().is_empty());

        assert_eq!(reconcile_once(&seed.store, &seed.sessions).await.denied, 1);
        let resolved = seed
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert!(pending_for_child(&resolved, &child).unwrap().is_empty());
    }

    #[tokio::test]
    async fn deadline_reconcile_is_exact_and_a_late_approval_cannot_replace_deny() {
        let future = seed_request(chrono::Duration::minutes(2), false).await;
        assert_eq!(
            reconcile_once(&future.store, &future.sessions).await.denied,
            0
        );
        let parent = future
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome::state(&parent, &future.request), Ok(State::Pending));

        let expired = seed_request(-chrono::Duration::seconds(1), false).await;
        assert_eq!(
            reconcile_once(&expired.store, &expired.sessions)
                .await
                .denied,
            1
        );
        assert_eq!(
            reconcile_once(&expired.store, &expired.sessions)
                .await
                .denied,
            0
        );
        let parent = expired
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome::state(&parent, &expired.request),
            Ok(State::Terminal(false))
        );
        assert_eq!(parent.messages.len(), 2);
        assert_eq!(
            outcome::commit(&expired.sessions, &expired.request, Some(true), false).await,
            Ok(State::Terminal(false)),
            "a late approval must observe the Deny winner"
        );
        let parent = expired
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parent.messages.len(), 2);
        let marker = parent.messages[1]
            .metadata
            .as_ref()
            .unwrap()
            .get("session_message")
            .unwrap();
        let terminal: SessionMessageEnvelope = serde_json::from_value(marker.clone()).unwrap();
        let resolution =
            ParentResolution::from_forced_permission_terminal(&expired.request, &terminal).unwrap();
        assert_eq!(resolution.request_id, expired.request.id);
        assert_eq!(
            resolution.decision,
            bamboo_domain::ParentRequestOption::Deny
        );
    }

    #[tokio::test]
    async fn future_request_expires_at_its_own_deadline_without_a_global_sweep() {
        let seed = seed_request(chrono::Duration::seconds(5), false).await;
        let shutdown = CancellationToken::new();
        let report = reconcile_startup(&seed.store, &seed.sessions, &shutdown).await;
        assert_eq!(report.denied, 0);
        assert_eq!(report.errors, 0);

        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let parent = seed
                    .store
                    .load_session("permission-parent")
                    .await
                    .unwrap()
                    .unwrap();
                if outcome::state(&parent, &seed.request) == Ok(State::Terminal(false)) {
                    assert_eq!(parent.messages.len(), 2);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the one-request timer must persist Deny soon after its deadline");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn transient_store_failure_at_deadline_retries_until_deny_is_durable() {
        let seed = seed_request(chrono::Duration::seconds(5), false).await;
        let entry = seed
            .store
            .get_index_entry("permission-parent")
            .await
            .unwrap();
        let main = seed.home.path().join(entry.rel_path).join("session.json");
        let offline = main.with_extension("json.offline");
        std::fs::rename(&main, &offline).unwrap();
        let shutdown = CancellationToken::new();
        spawn_deadline(
            &seed.store,
            &seed.sessions,
            seed.request.clone(),
            shutdown.clone(),
        );
        tokio::time::sleep(Duration::from_millis(5_250)).await;
        std::fs::rename(&offline, &main).unwrap();

        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let parent = seed
                    .store
                    .load_session("permission-parent")
                    .await
                    .unwrap()
                    .unwrap();
                if outcome::state(&parent, &seed.request) == Ok(State::Terminal(false)) {
                    assert_eq!(parent.messages.len(), 2);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retry must survive temporary loss of the authoritative Session file");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn concurrent_expiry_uses_one_terminal_winner() {
        let seed = seed_request(-chrono::Duration::seconds(1), false).await;
        let (first, second) = tokio::join!(
            outcome::expire(&seed.sessions, &seed.request),
            outcome::expire(&seed.sessions, &seed.request)
        );
        assert_eq!(first, Ok(State::Terminal(false)));
        assert_eq!(second, Ok(State::Terminal(false)));
        let parent = seed
            .store
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parent.messages.len(), 2);
        assert_eq!(
            outcome::state(&parent, &seed.request),
            Ok(State::Terminal(false))
        );
    }

    #[tokio::test]
    async fn server_restart_denies_expired_request_and_orphaned_child_stops_running() {
        let seed = seed_request(-chrono::Duration::seconds(1), true).await;
        let Seed {
            home,
            store,
            sessions,
            request,
        } = seed;
        drop(sessions);
        drop(store);
        let restarted = super::super::AppState::new(home.path().to_path_buf())
            .await
            .unwrap();
        let parent = restarted
            .storage
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome::state(&parent, &request),
            Ok(State::Terminal(false))
        );
        assert_eq!(parent.messages.len(), 2);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let child = restarted
                    .storage
                    .load_session("permission-child")
                    .await
                    .unwrap()
                    .unwrap();
                if child.last_run_status().as_deref() == Some("error") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("existing child-wait boot watchdog must retire the orphaned run");
        assert_eq!(
            outcome::commit(&restarted.session_repo, &request, Some(true), false).await,
            Ok(State::Terminal(false))
        );
        let current = restarted
            .storage
            .load_session("permission-parent")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.messages.len(), 2);
    }
}
