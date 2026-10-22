//! Synthetic App orchestration only. No provider, real account or real grant.
//! Injected-port cases test orchestration. Canonical-port cases below exercise
//! the existing real FileSessionInbox over synthetic TempDir data only.

use super::*;
use bamboo_domain::{
    host_session_create_identity, ActorAncestorObservation, HostScopeSessionObservation,
    HostSessionCreateScope, HostSessionRight, HostSessionScopeBinding, HostSessionScopeToken,
    SessionCallerReference, SessionMessageId, HOST_SESSION_SCOPE_VERSION,
};
use bamboo_engine::{SessionMessengerReceipt, SessionPeerDeliveryReceipt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

#[derive(Default)]
struct FirstInputMock {
    succeed: AtomicBool,
    calls: Mutex<Vec<(SessionCallerReference, uuid::Uuid, Session, String)>>,
}

#[async_trait]
impl HostRootFirstInputPort for FirstInputMock {
    async fn deliver(
        &self,
        _token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        operation_id: uuid::Uuid,
        reserved_root: &Session,
        message: &str,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        self.calls.lock().unwrap().push((
            caller.clone(),
            operation_id,
            reserved_root.clone(),
            message.to_string(),
        ));
        if !self.succeed.load(Ordering::SeqCst) {
            return Err(SessionMessengerError::Storage(
                "synthetic first-input unavailable".into(),
            ));
        }
        Ok(SessionMessengerReceipt {
            delivery: bamboo_domain::SessionInboxReceipt {
                id: SessionMessageId::stable(
                    "synthetic_create_input",
                    &serde_json::json!({
                        "caller":caller.session_id, "operation":operation_id, "target":reserved_root.id,
                    }),
                ),
                generation: 1,
            },
            activation: SessionActivationDisposition::ActivationReserved,
        }.into())
    }
}

async fn caller(f: &Fixture, role: &str) -> (Session, Vec<ActorAncestorObservation>) {
    if role == "supervisor" {
        return (
            f.creator
                .store
                .load_session(&f.supervisor.session_id)
                .await
                .unwrap()
                .unwrap(),
            vec![],
        );
    }
    let mut parent = Session::new("host-caller", "caller-model");
    parent.set_project_id_meta(f.request.project_id.as_str());
    f.creator.store.save_session(&parent).await.unwrap();
    if role == "root" {
        return (parent, vec![]);
    }
    let mut child = Session::new_child_of("host-child", &parent, "caller-model", "short task");
    child.set_project_id_meta(f.request.project_id.as_str());
    f.creator.store.save_session(&child).await.unwrap();
    (
        child,
        vec![ActorAncestorObservation {
            actor_id: parent.id,
            session_created_at: parent.created_at,
            metadata_version: parent.metadata_version,
        }],
    )
}

fn request(f: &Fixture) -> SupervisedRootCreateRequest {
    let mut request = f.request.clone();
    request.workspace_path = bamboo_config::paths::path_to_display_string(
        &std::fs::canonicalize(&request.workspace_path).unwrap(),
    );
    request
}

async fn install(
    f: &Fixture,
    caller: &Session,
    ancestors: Vec<ActorAncestorObservation>,
    request: &SupervisedRootCreateRequest,
    revision: u64,
    created_target_rights: BTreeSet<HostSessionRight>,
) -> HostSessionScopeToken {
    f.creator
        .store
        .install_host_session_scope(
            revision - 1,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: uuid::Uuid::new_v4(),
                revision,
                caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
                targets: BTreeMap::new(),
                creates: BTreeMap::from([(
                    request.operation_id,
                    HostSessionCreateScope {
                        operation_id: request.operation_id,
                        project_id: request.project_id.clone(),
                        workspace_path: request.workspace_path.clone(),
                        created_target_rights,
                    },
                )]),
            },
        )
        .await
        .unwrap()
}

fn authority(
    caller: &SessionCallerReference,
    token: &HostSessionScopeToken,
    port: Arc<FirstInputMock>,
) -> CreationAuthority {
    CreationAuthority::Host {
        caller: caller.clone(),
        token: token.clone(),
        first_input: Some(port),
    }
}

async fn reserved_record(
    f: &Fixture,
    caller: &SessionCallerReference,
    token: &HostSessionScopeToken,
    request: &SupervisedRootCreateRequest,
) -> SessionCreateOperationRecord {
    let owner = authority(caller, token, Arc::new(FirstInputMock::default()));
    let (digest, id, fingerprint) = owner.operation_identity(request).unwrap();
    SessionCreateOperationRecord::pending(digest, fingerprint, id)
}

#[tokio::test]
async fn finite_host_path_creates_independent_roots_for_all_roles_without_management_links() {
    for role in ["root", "child", "supervisor"] {
        let f = Fixture::new().await;
        let (source, ancestors) = caller(&f, role).await;
        let original = SessionCallerReference::from(&source);
        let request = request(&f);
        let token = install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
        let mock = Arc::new(FirstInputMock::default());
        let port = f.creator.with_host_first_input(mock.clone());
        let scope = port.scope(&original).await.unwrap();
        assert!(scope.success);
        assert_eq!(value(scope)["authority"], "host_finite");
        let result = port.create(&original, request.clone()).await.unwrap();
        assert!(!result.success);
        assert_eq!(value(result)["code"], "first_input_pending");
        let (digest, id) = host_session_create_identity(&original, request.operation_id).unwrap();
        let target = f.creator.store.load_session(&id).await.unwrap().unwrap();
        assert_eq!(target.kind, bamboo_domain::SessionKind::Root);
        assert!(target.parent_session_id.is_none());
        assert_eq!(target.root_session_id, id);
        assert_eq!(
            target.project_id_meta().as_deref(),
            Some(request.project_id.as_str())
        );
        assert_eq!(
            target.workspace_path_meta().as_deref(),
            Some(request.workspace_path.as_str())
        );
        assert_eq!(
            target.authority_identity,
            bamboo_domain::SessionAuthorityIdentity::Ordinary
        );
        let record = f
            .creator
            .operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.status, StoredOperationStatus::Pending);
        assert!(record.supervised_delivery.is_none());
        assert!(token.binding().targets.is_empty());
        let supervisor = f
            .creator
            .store
            .load_session(&f.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!supervisor
            .supervisor_management
            .unwrap()
            .links
            .contains_key(&id));
        assert_eq!(
            f.activation.calls.load(Ordering::SeqCst),
            0,
            "Host must not fall back to Supervisor delivery"
        );
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, original);
        assert_eq!(calls[0].1, request.operation_id);
        assert_eq!(calls[0].2.created_at, record.created_at);
        assert_eq!(calls[0].3, request.message);
    }
}

#[tokio::test]
async fn absent_create_input_port_blocks_before_reservation_even_for_explicit_host_supervisor() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "supervisor").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    let port = f.creator.unconfigured_caller_port();
    let scope = port.scope(&original).await.unwrap();
    assert!(!scope.success);
    assert_eq!(value(scope)["code"], "host_root_first_input_unavailable");
    let result = port.create(&original, request.clone()).await.unwrap();
    assert!(!result.success);
    assert_eq!(value(result)["code"], "host_root_first_input_unavailable");
    let (digest, id) = host_session_create_identity(&original, request.operation_id).unwrap();
    assert!(f
        .creator
        .operations
        .load_for_status(&digest)
        .await
        .unwrap()
        .is_none());
    assert!(f.creator.store.load_session(&id).await.unwrap().is_none());
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn only_never_installed_supervisor_has_the_legacy_fallback() {
    let f = Fixture::new().await;
    let (source, _) = caller(&f, "supervisor").await;
    let port = f.creator.caller_port();
    let original = SessionCallerReference::from(&source);
    assert!(port.scope(&original).await.unwrap().success);
    let result = port.create(&original, f.request.clone()).await.unwrap();
    assert_eq!(accepted(result)["linked"], true);
    let (ordinary, _) = caller(&f, "root").await;
    let error = port
        .scope(&SessionCallerReference::from(&ordinary))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("session_root_scope_unavailable"));
}

#[tokio::test]
async fn default_caller_port_uses_real_create_purpose_inbox_without_a_send_grant() {
    for role in ["root", "child", "supervisor"] {
        let f = Fixture::new().await;
        let (source, ancestors) = caller(&f, role).await;
        let original = SessionCallerReference::from(&source);
        let request = request(&f);
        let token = install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
        let port = f.creator.caller_port();
        assert!(port.scope(&original).await.unwrap().success);
        let one = accepted(port.create(&original, request.clone()).await.unwrap());
        let two = accepted(port.create(&original, request.clone()).await.unwrap());
        let (_, id) = host_session_create_identity(&original, request.operation_id).unwrap();
        assert_eq!(one["target_session_id"], id);
        assert_eq!(two["target_session_id"], id);
        assert_eq!(one["receipt_id"], two["receipt_id"]);
        assert_eq!(one["linked"], Value::Null);
        let backlog = f.inbox.inspect(&id).await.unwrap();
        assert_eq!(backlog.generation, 1);
        assert_eq!(backlog.pending, 1);
        assert!(f
            .creator
            .store
            .list_host_scoped_targets(&token, &original)
            .await
            .unwrap()
            .is_empty());
        assert!(f
            .creator
            .store
            .load_host_scoped_target(&token, &original, &id)
            .await
            .is_err());
        let message = f.inbox.claim(&id, 1).await.unwrap().remove(0).envelope;
        assert_eq!(
            message.source,
            bamboo_domain::SessionMessageSource::Session {
                session_id: source.id
            }
        );
        assert_eq!(
            message.correlation_id.as_deref(),
            Some(request.operation_id.to_string().as_str())
        );
        assert_eq!(
            message.body,
            bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent::text(
                &request.message
            ))
        );
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn canonical_create_input_keeps_known_receipt_when_activation_fails_then_retries() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    let port = f.creator.caller_port();
    f.activation.fail.store(true, Ordering::SeqCst);
    let first = value(port.create(&original, request.clone()).await.unwrap());
    assert_eq!(first["admitted"], true);
    assert_eq!(first["code"], "activation_pending");
    let (_, id) = host_session_create_identity(&original, request.operation_id).unwrap();
    assert_eq!(f.inbox.inspect(&id).await.unwrap().generation, 1);
    f.activation.fail.store(false, Ordering::SeqCst);
    let retry = accepted(port.create(&original, request).await.unwrap());
    assert_eq!(retry["receipt_id"], first["receipt_id"]);
    assert_eq!(f.inbox.inspect(&id).await.unwrap().generation, 1);
}

#[tokio::test]
async fn acknowledged_first_input_replay_does_not_activate_a_fresh_router() {
    for use_host in [true, false] {
        let f = Fixture::new().await;
        let (source, ancestors) = caller(&f, if use_host { "root" } else { "supervisor" }).await;
        let original = SessionCallerReference::from(&source);
        let request = request(&f);
        if use_host {
            install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
        }
        f.activation.fail.store(true, Ordering::SeqCst);
        let first = accepted(
            f.creator
                .caller_port()
                .create(&original, request.clone())
                .await
                .unwrap(),
        );
        assert_eq!(first["code"], "activation_pending");
        let id = first["target_session_id"].as_str().unwrap();
        let claim = f.inbox.claim(id, 1).await.unwrap().remove(0);
        let mut checkpoint = f.creator.store.load_session(id).await.unwrap().unwrap();
        checkpoint.add_message(claim.envelope.to_provider_message().unwrap());
        checkpoint
            .session_inbox_admission_mut()
            .record(claim.envelope.id.clone(), claim.generation);
        f.creator.store.save_session(&checkpoint).await.unwrap();
        f.inbox.ack(id, &claim).await.unwrap();
        let fresh_activation = Arc::new(Activation::default());
        let mut creator = f.creator.clone();
        creator.messenger = Arc::new(SessionMessenger::new(
            creator.store.clone(),
            f.inbox.clone(),
            fresh_activation.clone(),
        ));
        let replay = accepted(
            creator
                .caller_port()
                .create(&original, request)
                .await
                .unwrap(),
        );
        assert_eq!(replay["receipt_id"], first["receipt_id"]);
        assert_eq!(replay["activation"], "delivery_not_pending");
        assert_eq!(replay["activation_requested"], false);
        assert_eq!(fresh_activation.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.inbox.inspect(id).await.unwrap().generation, 1);
        assert_eq!(f.inbox.inspect(id).await.unwrap().pending, 0);
    }
}

#[tokio::test]
async fn captured_legacy_scope_cannot_return_after_host_namespace_installation() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "supervisor").await;
    let legacy = CreationAuthority::Supervisor {
        reference: f.supervisor.clone(),
        original_caller: Some(SessionCallerReference::from(&source)),
    };
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    assert!(legacy.scope(&f.creator).await.is_err());
    f.creator
        .store
        .revoke_host_session_scope(&source.id, 1)
        .await
        .unwrap();
    assert!(legacy.scope(&f.creator).await.is_err());
    assert!(
        CreationAuthority::legacy_supervisor(f.supervisor.clone())
            .scope(&f.creator)
            .await
            .unwrap()
            .success
    );
}

#[tokio::test]
async fn pending_legacy_supervisor_creation_resumes_through_host_with_one_real_inbox_address() {
    let f = Fixture::new().await;
    let request = request(&f);
    f.activation.fail.store(true, Ordering::SeqCst);
    let first = accepted(
        f.creator
            .create(&f.supervisor, request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(first["code"], "activation_pending");
    let (source, ancestors) = caller(&f, "supervisor").await;
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    f.activation.fail.store(false, Ordering::SeqCst);
    let retry = accepted(
        f.creator
            .caller_port()
            .create(&SessionCallerReference::from(&source), request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(retry["target_session_id"], first["target_session_id"]);
    assert_eq!(retry["receipt_id"], first["receipt_id"]);
    assert_eq!(retry["linked"], Value::Null);
    let (_, id) =
        host_session_create_identity(&SessionCallerReference::from(&source), request.operation_id)
            .unwrap();
    assert_eq!(f.inbox.inspect(&id).await.unwrap().generation, 1);
    assert_eq!(f.inbox.inspect(&id).await.unwrap().pending, 1);
}

#[tokio::test]
async fn revoked_host_scope_never_falls_back_to_the_supervisor_scope() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "supervisor").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    f.creator
        .store
        .revoke_host_session_scope(&source.id, 1)
        .await
        .unwrap();
    let mock = Arc::new(FirstInputMock::default());
    let port = f.creator.with_host_first_input(mock.clone());
    assert!(port.scope(&original).await.is_err());
    assert!(port.create(&original, request.clone()).await.is_err());
    assert!(mock.calls.lock().unwrap().is_empty());
    let (_, id) = host_session_create_identity(&original, request.operation_id).unwrap();
    assert!(f.creator.store.load_session(&id).await.unwrap().is_none());
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exact_retry_survives_scope_replacement_with_frozen_payload_and_birth() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    let first = install(&f, &source, ancestors.clone(), &request, 1, BTreeSet::new()).await;
    let mock = Arc::new(FirstInputMock::default());
    let port = f.creator.with_host_first_input(mock.clone());
    assert!(
        !port
            .create(&original, request.clone())
            .await
            .unwrap()
            .success
    );
    let record = reserved_record(&f, &original, &first, &request).await;
    let stored = f
        .creator
        .operations
        .load_for_status(&record.key_digest)
        .await
        .unwrap()
        .unwrap();
    let second = install(&f, &source, ancestors, &request, 2, BTreeSet::new()).await;
    assert!(!first.same_installation(&second));
    let second_identity = authority(&original, &second, mock.clone())
        .operation_identity(&request)
        .unwrap();
    assert_eq!(second_identity.0, stored.key_digest);
    assert_eq!(second_identity.1, stored.session_id);
    assert_eq!(second_identity.2, stored.payload_fingerprint);
    assert!(
        !port
            .create(&original, request.clone())
            .await
            .unwrap()
            .success
    );
    let replayed = f
        .creator
        .operations
        .load_for_status(&record.key_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed, stored);
    let mut changed = request;
    changed.message.push_str(" changed instruction");
    let result = port.create(&original, changed).await.unwrap();
    assert!(!result.success);
    assert_eq!(value(result)["code"], "idempotency_conflict");
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn durable_pending_without_a_root_requires_recovery_instead_of_reexecution() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    let token = install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    let record = reserved_record(&f, &original, &token, &request).await;
    f.creator.operations.save(&record).await.unwrap();
    let mock = Arc::new(FirstInputMock::default());
    let result = f
        .creator
        .with_host_first_input(mock.clone())
        .create(&original, request)
        .await
        .unwrap();
    assert!(!result.success);
    assert_eq!(value(result)["code"], "creation_recovery_required");
    assert!(f
        .creator
        .store
        .load_session(&record.session_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        f.creator
            .operations
            .load_for_status(&record.key_digest)
            .await
            .unwrap(),
        Some(record)
    );
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn created_target_rights_require_separate_installation_and_do_not_start_creation() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(
        &f,
        &source,
        ancestors,
        &request,
        1,
        BTreeSet::from([HostSessionRight::SendPeer]),
    )
    .await;
    let mock = Arc::new(FirstInputMock::default());
    let port = f.creator.with_host_first_input(mock.clone());
    let scope = port.scope(&original).await.unwrap();
    assert!(!scope.success);
    assert_eq!(
        value(scope)["code"],
        "host_created_target_rights_unsupported"
    );
    let result = port.create(&original, request.clone()).await.unwrap();
    assert!(!result.success);
    assert_eq!(
        value(result)["code"],
        "host_created_target_rights_unsupported"
    );
    let (digest, id) = host_session_create_identity(&original, request.operation_id).unwrap();
    assert!(f
        .creator
        .operations
        .load_for_status(&digest)
        .await
        .unwrap()
        .is_none());
    assert!(f.creator.store.load_session(&id).await.unwrap().is_none());
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn typed_mock_receipt_is_retained_and_completed_replay_does_not_redeliver_or_recreate() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    let mock = Arc::new(FirstInputMock::default());
    mock.succeed.store(true, Ordering::SeqCst);
    let port = f.creator.with_host_first_input(mock.clone());
    let first = accepted(port.create(&original, request.clone()).await.unwrap());
    assert!(first["linked"].is_null());
    assert_eq!(first["management_link_changed"], false);
    assert!(first["link_revision"].is_null());
    let (digest, id) = host_session_create_identity(&original, request.operation_id).unwrap();
    let stored = f
        .creator
        .operations
        .load_for_status(&digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, StoredOperationStatus::Succeeded);
    assert!(stored.expires_at.is_none());
    assert_eq!(f.creator.operations.prune_expired().await.unwrap(), 0);
    let replayed = accepted(port.create(&original, request.clone()).await.unwrap());
    assert_eq!(replayed["receipt_id"], first["receipt_id"]);
    assert_eq!(replayed["generation"], first["generation"]);
    assert_eq!(replayed["replayed"], true);
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    f.creator.store.delete_session(&id).await.unwrap();
    let refused = port.create(&original, request).await.unwrap();
    assert!(!refused.success);
    assert!(f.creator.store.load_session(&id).await.unwrap().is_none());
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn terminal_failed_or_corrupt_operations_are_retained_without_root_sideeffects() {
    for corrupt in [false, true] {
        let f = Fixture::new().await;
        let (source, ancestors) = caller(&f, "root").await;
        let original = SessionCallerReference::from(&source);
        let request = request(&f);
        let token = install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
        let mut record = reserved_record(&f, &original, &token, &request).await;
        record.mark_failed(
            crate::app_state::session_create_operations::StoredOperationError {
                code: "synthetic_failure".into(),
                message: "known failure".into(),
                http_status: 500,
            },
        );
        record.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        f.creator.operations.save(&record).await.unwrap();
        if corrupt {
            tokio::fs::write(
                f.creator
                    .operations
                    .root_for_test()
                    .join(format!("{}.json", record.key_digest)),
                b"malformed retained receipt",
            )
            .await
            .unwrap();
        }
        let mock = Arc::new(FirstInputMock::default());
        let result = f
            .creator
            .with_host_first_input(mock.clone())
            .create(&original, request)
            .await;
        if corrupt {
            assert!(result.is_err());
        } else {
            let result = result.unwrap();
            assert!(!result.success);
            assert_eq!(value(result)["code"], "creation_failed");
            assert!(f
                .creator
                .operations
                .load_for_status(&record.key_digest)
                .await
                .unwrap()
                .unwrap()
                .expires_at
                .is_none());
        }
        assert!(f
            .creator
            .store
            .load_session(&record.session_id)
            .await
            .unwrap()
            .is_none());
        assert!(mock.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn ungranted_operation_project_workspace_and_new_revision_refuse_before_reservation() {
    let f = Fixture::new().await;
    let (source, ancestors) = caller(&f, "root").await;
    let original = SessionCallerReference::from(&source);
    let request = request(&f);
    install(&f, &source, ancestors, &request, 1, BTreeSet::new()).await;
    let mock = Arc::new(FirstInputMock::default());
    let port = f.creator.with_host_first_input(mock.clone());
    for change in 0..4 {
        let mut denied = request.clone();
        match change {
            0 => denied.operation_id = uuid::Uuid::new_v4(),
            1 => denied.project_id = "ungranted-project".parse().unwrap(),
            2 => denied.workspace_path.push_str("-ungranted"),
            _ => denied.expected_state_revision += 1,
        }
        let result = port.create(&original, denied.clone()).await.unwrap();
        assert!(!result.success);
        assert_eq!(
            value(result)["code"],
            match change {
                2 => "workspace_invalid",
                3 => "scope_revision_conflict",
                _ => "scope_denied",
            }
        );
        let (digest, id) = host_session_create_identity(&original, denied.operation_id).unwrap();
        assert!(f
            .creator
            .operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .is_none());
        assert!(f.creator.store.load_session(&id).await.unwrap().is_none());
    }
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_none_to_host_scope_retry_keeps_the_legacy_reservation_and_raw_payload() {
    let f = Fixture::new().await;
    f.activation.fail.store(true, Ordering::SeqCst);
    let (source, _) = caller(&f, "supervisor").await;
    let original = SessionCallerReference::from(&source);
    let raw_request = f.request.clone();
    let legacy = accepted(
        f.creator
            .caller_port()
            .create(&original, raw_request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(legacy["activation"], "activation_pending");
    let stored = f
        .creator
        .operations
        .load_for_status(&f.digest())
        .await
        .unwrap()
        .unwrap();
    let before_count = f.count().await;
    // The unchanged old request may contain a physical-path alias. Host grants
    // bind the resolver's canonical workspace rather than rewriting that request.
    let source = f
        .creator
        .store
        .load_session(&source.id)
        .await
        .unwrap()
        .unwrap();
    let current = SessionCallerReference::from(&source);
    let granted_request = request(&f);
    let token = install(&f, &source, vec![], &granted_request, 1, BTreeSet::new()).await;
    let mock = Arc::new(FirstInputMock::default());
    let identity = authority(&current, &token, mock.clone())
        .operation_identity(&raw_request)
        .unwrap();
    assert_eq!(identity.0, stored.key_digest);
    assert_eq!(identity.1, stored.session_id);
    assert_eq!(identity.2, stored.payload_fingerprint);
    let result = f
        .creator
        .with_host_first_input(mock.clone())
        .create(&current, raw_request.clone())
        .await
        .unwrap();
    assert!(!result.success);
    assert_eq!(value(result)["code"], "first_input_pending");
    assert_eq!(
        f.count().await,
        before_count,
        "scope replacement allocated another Root"
    );
    assert_eq!(
        f.creator
            .operations
            .load_for_status(&stored.key_digest)
            .await
            .unwrap(),
        Some(stored.clone())
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].2.id, stored.session_id);
    assert_eq!(calls[0].2.created_at, stored.created_at);
    assert_eq!(calls[0].3, raw_request.message);
}

#[tokio::test]
async fn host_replay_pins_the_legacy_terminal_receipt_without_redelivery() {
    let f = Fixture::new().await;
    let original_request = f.request.clone();
    let legacy = accepted(f.create(original_request.clone()).await);
    let mut legacy_record = f
        .creator
        .operations
        .load_for_status(&f.digest())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(legacy_record.status, StoredOperationStatus::Succeeded);
    assert!(legacy_record.expires_at.is_some());
    // A retained legacy receipt can already be past the client-visible window
    // when the first Host retry claims it. Do not prune/reinterpret its truth.
    legacy_record.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    f.creator.operations.save(&legacy_record).await.unwrap();
    let source = f
        .creator
        .store
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let current = SessionCallerReference::from(&source);
    install(&f, &source, vec![], &request(&f), 1, BTreeSet::new()).await;
    let mock = Arc::new(FirstInputMock::default());
    let replay = accepted(
        f.creator
            .with_host_first_input(mock.clone())
            .create(&current, original_request)
            .await
            .unwrap(),
    );
    assert_eq!(replay["receipt_id"], legacy["receipt_id"]);
    assert_eq!(replay["replayed"], true);
    assert!(replay["linked"].is_null());
    assert_eq!(replay["management_link_changed"], false);
    assert!(mock.calls.lock().unwrap().is_empty());
    let pinned = f
        .creator
        .operations
        .load_for_status(&f.digest())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pinned.status, legacy_record.status);
    assert_eq!(pinned.created_at, legacy_record.created_at);
    assert_eq!(
        pinned.payload_fingerprint,
        legacy_record.payload_fingerprint
    );
    assert_eq!(
        pinned.supervised_delivery,
        legacy_record.supervised_delivery
    );
    assert!(pinned.expires_at.is_none());
    assert_eq!(f.creator.operations.prune_expired().await.unwrap(), 0);
}
