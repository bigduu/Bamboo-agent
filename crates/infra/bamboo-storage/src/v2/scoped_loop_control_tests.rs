//! Concrete canonical storage/Inbox contracts. The test activation adapter is
//! deliberately explicit; real registration issuance is tested in Engine.
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, LoopControlAction, LoopControlRequest,
    RootActorRuntimeWrite, ScopedLoopControlBinding, ScopedLoopControlCheckpoint,
    ScopedLoopControlClaim, ScopedLoopControlConsumption, ScopedLoopControlInput,
    SessionActivationDisposition, SessionActivationError, SessionActivationPort,
    SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits, SessionInboxPort,
    SessionMessageEnvelope, SessionRunConflict, SessionRunDeliveryGuard, Storage,
    SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct LiveGrant {
    binding: ScopedLoopControlBinding,
    live: Arc<AtomicBool>,
    notifications: Arc<AtomicUsize>,
    activations: AtomicUsize,
}
struct GrantGuard(ScopedLoopControlBinding, Arc<AtomicUsize>, Arc<AtomicBool>);
impl SessionRunDeliveryGuard for GrantGuard {
    fn matches(&self, target: &str, birth: DateTime<Utc>, run: &str) -> bool {
        self.2.load(Ordering::SeqCst)
            && target == self.0.target_session_id
            && birth == self.0.target_created_at
            && run == self.0.owner_run_id
    }
    fn notify_committed_delivery(&self, _generation: u64) -> bool {
        if !self.2.load(Ordering::SeqCst) {
            return false;
        }
        self.1.fetch_add(1, Ordering::SeqCst);
        true
    }
}
#[async_trait::async_trait]
impl SessionActivationPort for LiveGrant {
    fn retain_supervisor_loop_blocking(
        &self,
        binding: &ScopedLoopControlBinding,
    ) -> Result<Arc<dyn SessionRunDeliveryGuard>, SessionRunConflict> {
        if !self.live.load(Ordering::SeqCst) || binding != &self.binding {
            return Err(SessionRunConflict::unsupported(&binding.owner_run_id));
        }
        Ok(Arc::new(GrantGuard(
            binding.clone(),
            self.notifications.clone(),
            self.live.clone(),
        )))
    }
    async fn request_activation(
        &self,
        _target: &str,
        _generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.activations.fetch_add(1, Ordering::SeqCst);
        Err(SessionActivationError::Internal(
            "scoped control must not activate".into(),
        ))
    }
}
struct Fixture {
    _home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    inbox: Arc<crate::FileSessionInbox>,
    binding: ScopedLoopControlBinding,
    activation: Arc<LiveGrant>,
    target: Session,
    owner: Option<RootActorRuntimeWrite>,
}
impl Fixture {
    async fn new(owned: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let reference: SupervisorReference = (&store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap())
            .into();
        let mut target = Session::new("scoped-loop-target", "controlled-model");
        target.set_project_id_meta("project-scoped");
        target.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState {
            execution_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        });
        target.add_message(Message::user("existing independent Root history"));
        store.save_session(&target).await.unwrap();
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 0,
                mutation: SupervisorManagementMutation::ConfigureProjectScope {
                    allowed_projects: BTreeSet::from(["project-scoped".parse().unwrap()]),
                },
            })
            .await
            .unwrap();
        let attached = store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 1,
                mutation: SupervisorManagementMutation::Attach {
                    target_session_id: target.id.clone(),
                },
            })
            .await
            .unwrap();
        let source = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let binding = ScopedLoopControlBinding {
            grant_id: Uuid::new_v4(),
            supervisor: reference,
            supervisor_tool_authority_revision: source.root_tool_authority_revision,
            target_session_id: target.id.clone(),
            target_created_at: target.created_at,
            target_metadata_version: target.metadata_version,
            target_project_id: "project-scoped".parse().unwrap(),
            link_revision: attached.link_revision.unwrap(),
            owner_run_id: "scoped-run".into(),
            execution_id: target
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .execution_id
                .clone()
                .unwrap(),
            allowed_actions: BTreeSet::from([
                LoopControlAction::Status,
                LoopControlAction::Start,
                LoopControlAction::Update,
                LoopControlAction::Stop,
            ]),
            max_iterations: 2,
            max_total_rounds: 8,
        };
        let owner = if owned {
            let now = Utc::now();
            let activation = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: target.id.clone(),
                    run_id: binding.owner_run_id.clone(),
                    lease_owner: "test-host".into(),
                    lease_expires_at: now + chrono::Duration::minutes(5),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                })
                .await
                .unwrap();
            store
                .start_activation(&activation.fence(), now)
                .await
                .unwrap();
            Some(RootActorRuntimeWrite {
                fence: activation.fence(),
                created_at: target.created_at,
            })
        } else {
            None
        };
        let inbox = Arc::new(crate::FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let activation = Arc::new(LiveGrant {
            binding: binding.clone(),
            live: Arc::new(AtomicBool::new(true)),
            notifications: Arc::new(AtomicUsize::new(0)),
            activations: AtomicUsize::new(0),
        });
        Self {
            _home: home,
            store,
            inbox,
            binding,
            activation,
            target,
            owner,
        }
    }
    fn envelope(&self) -> SessionMessageEnvelope {
        ScopedLoopControlInput {
            binding: self.binding.clone(),
            command: LoopControlRequest::Start {
                operation_id: Uuid::new_v4().to_string(),
                expected_generation: None,
                expected_revision: 0,
                objective: "bounded target",
                max_iterations: 2,
                max_total_rounds: 8,
            },
        }
        .envelope()
        .unwrap()
    }
    async fn deliver(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> bamboo_domain::SessionInboxSteerReceipt {
        self.inbox
            .deliver_supervisor_loop_control(&self.binding, envelope, self.activation.clone())
            .await
            .unwrap()
    }
    async fn claim(&self) -> (Arc<dyn SessionInboxPort>, ScopedLoopControlClaim) {
        if let Some(owner) = &self.owner {
            let inbox = self
                .store
                .bind_root_actor_inbox(owner, self.inbox.clone())
                .unwrap();
            let claims = inbox
                .claim_owned_scoped_loop_controls(
                    &self.target.id,
                    1,
                    Some(&self.binding.owner_run_id),
                    &SessionInboxLeaseRequest {
                        consumer: SessionInboxConsumerId::new(),
                        now: Utc::now(),
                        duration: chrono::Duration::minutes(1),
                    },
                )
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            (inbox, ScopedLoopControlClaim::Owned(claims[0].clone()))
        } else {
            let claims = self
                .inbox
                .claim_scoped_loop_controls(&self.target.id, 1, Some(&self.binding.owner_run_id))
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            (
                self.inbox.clone(),
                ScopedLoopControlClaim::Legacy(claims[0].clone()),
            )
        }
    }
    fn candidate(&self, claim: &ScopedLoopControlClaim) -> Session {
        let envelope = claim.envelope();
        let input = bamboo_domain::scoped_loop_control_input(envelope)
            .unwrap()
            .unwrap();
        let mut session = self.target.clone();
        // A real owner can checkpoint a rejected command without fabricating a
        // provider pair, successful operation receipt or completed iteration.
        let operation_id = input.operation_id().to_owned();
        let consumption = ScopedLoopControlConsumption {
            input_id: envelope.id.clone(),
            input_sha256: Sha256::digest(bamboo_domain::canonical_json_bytes(
                &envelope.idempotency_semantics(),
            ))
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
            binding: input.binding,
            operation_id,
            configured: false,
            receipt: None,
            rejection: Some("controlled contract rejection".into()),
        };
        session.metadata.insert(
            bamboo_domain::SCOPED_LOOP_CONSUMPTION_KEY.into(),
            serde_json::to_string(&vec![consumption]).unwrap(),
        );
        session
            .session_inbox_admission_mut()
            .record(envelope.id.clone(), claim.generation());
        session
    }
    async fn checkpoint(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        claim: ScopedLoopControlClaim,
        publish: bamboo_domain::RootActorRuntimePublisher,
    ) -> io::Result<()> {
        self.store
            .checkpoint_scoped_loop(ScopedLoopControlCheckpoint {
                session,
                bindings: std::slice::from_ref(&self.binding),
                activation: self.activation.clone(),
                owner: self.owner.as_ref(),
                input: Some((inbox, claim)),
                publish,
            })
            .await
    }
}

#[tokio::test]
async fn scoped_control_is_never_provider_input_or_an_ordinary_claim_or_activation() {
    let fixture = Fixture::new(false).await;
    let envelope = fixture.envelope();
    assert!(envelope.to_provider_message().is_err());
    assert!(fixture.inbox.deliver(&envelope).await.is_err());
    let receipt = fixture.deliver(&envelope).await;
    assert!(!receipt.replayed);
    assert!(receipt.active_delivery_notified);
    assert!(fixture
        .inbox
        .claim_for_turn(&fixture.target.id, 8, Some(&fixture.binding.owner_run_id))
        .await
        .unwrap()
        .is_empty());
    assert!(fixture
        .inbox
        .claim_scoped_loop_controls(&fixture.target.id, 8, Some("different-run"))
        .await
        .unwrap()
        .is_empty());
    let (_, claim) = fixture.claim().await;
    if let ScopedLoopControlClaim::Legacy(claim) = claim {
        assert!(fixture.inbox.ack(&fixture.target.id, &claim).await.is_err());
    }
    assert_eq!(fixture.activation.activations.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.activation.notifications.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scoped_checkpoint_publishes_typed_consumption_before_legacy_or_owned_ack() {
    for owned in [false, true] {
        let fixture = Fixture::new(owned).await;
        let envelope = fixture.envelope();
        fixture.deliver(&envelope).await;
        let (inbox, claim) = fixture.claim().await;
        let mut session = fixture.candidate(&claim);
        let observed = Arc::new(AtomicUsize::new(0));
        let count = observed.clone();
        let proof = envelope.clone();
        fixture
            .checkpoint(
                &mut session,
                inbox,
                claim,
                Arc::new(move |snapshot| {
                    assert!(bamboo_domain::has_scoped_loop_consumption(snapshot, &proof));
                    assert_eq!(snapshot.messages.len(), 1);
                    count.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert!(fixture
            .inbox
            .was_admitted(&fixture.target.id, &envelope.id)
            .await
            .unwrap());
        let durable = fixture
            .store
            .load_execution_admission_snapshot(&fixture.target.id)
            .await
            .unwrap()
            .unwrap();
        assert!(bamboo_domain::has_scoped_loop_consumption(
            &durable, &envelope
        ));
        assert_eq!(durable.messages[0].id, fixture.target.messages[0].id);
    }
}

#[tokio::test]
async fn scoped_owned_and_selected_user_claims_exclude_control_without_consuming_it() {
    let fixture = Fixture::new(true).await;
    let envelope = fixture.envelope();
    let delivery = fixture.deliver(&envelope).await;
    let inbox = fixture
        .store
        .bind_root_actor_inbox(fixture.owner.as_ref().unwrap(), fixture.inbox.clone())
        .unwrap();
    let request = SessionInboxLeaseRequest {
        consumer: SessionInboxConsumerId::new(),
        now: Utc::now(),
        duration: chrono::Duration::minutes(1),
    };
    assert!(inbox
        .claim_owned(
            &fixture.target.id,
            8,
            Some(&fixture.binding.owner_run_id),
            &request
        )
        .await
        .unwrap()
        .is_empty());
    assert!(inbox
        .claim_owned_receipt(
            &fixture.target.id,
            &delivery.delivery,
            Some(&fixture.binding.owner_run_id),
            &request
        )
        .await
        .unwrap()
        .is_none());
    let (_, claim) = fixture.claim().await;
    assert_eq!(claim.envelope().id, envelope.id);
}

#[tokio::test]
async fn scoped_retries_and_checkpoints_revalidate_host_grant_and_canonical_link() {
    let fixture = Fixture::new(false).await;
    let envelope = fixture.envelope();
    fixture.deliver(&envelope).await;
    assert!(fixture.deliver(&envelope).await.replayed);
    let (inbox, claim) = fixture.claim().await;
    let mut session = fixture.candidate(&claim);
    fixture.activation.live.store(false, Ordering::SeqCst);
    assert!(fixture
        .inbox
        .deliver_supervisor_loop_control(&fixture.binding, &envelope, fixture.activation.clone())
        .await
        .is_err());
    assert!(fixture
        .checkpoint(
            &mut session,
            inbox,
            claim,
            Arc::new(|_| panic!("revoked publication"))
        )
        .await
        .is_err());
    assert!(!fixture
        .inbox
        .was_admitted(&fixture.target.id, &envelope.id)
        .await
        .unwrap());
    fixture.activation.live.store(true, Ordering::SeqCst);
    fixture
        .store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: fixture.binding.supervisor.clone(),
            expected_state_revision: 2,
            mutation: SupervisorManagementMutation::Detach {
                target_session_id: fixture.target.id.clone(),
            },
        })
        .await
        .unwrap();
    assert!(fixture
        .store
        .load_scoped_loop_session(&fixture.binding, fixture.activation.clone())
        .await
        .is_err());
}

#[tokio::test]
async fn scoped_canonical_permissions_and_pipeline_identity_fail_closed() {
    for variant in 0..7 {
        let fixture = Fixture::new(false).await;
        let mut target = fixture.target.clone();
        let mut source = fixture
            .store
            .load_session(&fixture.binding.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        match variant {
            0 => {
                source
                    .metadata
                    .insert("disabled_tools".into(), "[\"session_control\"]".into());
            }
            1 => {
                source
                    .agent_runtime_state
                    .get_or_insert_with(Default::default)
                    .read_only = true;
            }
            2 => {
                source
                    .agent_runtime_state
                    .get_or_insert_with(Default::default)
                    .set_permission_mode(bamboo_domain::SessionPermissionMode::Plan);
            }
            3 => {
                target.set_root_orchestration_only(true).unwrap();
            }
            4 => {
                target.agent_runtime_state.as_mut().unwrap().execution_id =
                    Some(Uuid::new_v4().to_string());
            }
            5 => {
                target
                    .metadata
                    .insert("disabled_tools".into(), "[\"loop_control\"]".into());
            }
            _ => {
                target.agent_runtime_state.as_mut().unwrap().read_only = true;
            }
        }
        fixture.store.save_session(&source).await.unwrap();
        fixture.store.save_session(&target).await.unwrap();
        assert!(
            fixture
                .store
                .load_scoped_loop_session(&fixture.binding, fixture.activation.clone())
                .await
                .is_err(),
            "variant {variant}"
        );
        assert!(
            fixture
                .inbox
                .deliver_supervisor_loop_control(
                    &fixture.binding,
                    &fixture.envelope(),
                    fixture.activation.clone()
                )
                .await
                .is_err(),
            "variant {variant}"
        );
    }
}

#[tokio::test]
async fn scoped_main_publication_failure_never_acks_and_reloads_real_partial_facts() {
    use super::default_actor_context_tests::DefaultWriteHook;
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let fixture = Fixture::new(false).await;
        let envelope = fixture.envelope();
        fixture.deliver(&envelope).await;
        let (inbox, claim) = fixture.claim().await;
        let mut session = fixture.candidate(&claim);
        let hook = DefaultWriteHook::install(&fixture.store, "session.json", phase, true);
        hook.release();
        assert!(fixture
            .checkpoint(
                &mut session,
                inbox,
                claim,
                Arc::new(|_| panic!("failed writer publication"))
            )
            .await
            .is_err());
        let waited = hook.clone();
        tokio::task::spawn_blocking(move || waited.wait())
            .await
            .unwrap();
        assert!(!fixture
            .inbox
            .was_admitted(&fixture.target.id, &envelope.id)
            .await
            .unwrap());
        let canonical = fixture
            .store
            .load_execution_admission_snapshot(&fixture.target.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            serde_json::to_value(&canonical).unwrap()
        );
        assert_eq!(canonical.messages.len(), 1);
        if phase == DurableWritePhase::BeforeReplace {
            assert!(!canonical
                .session_inbox_admission()
                .is_some_and(|cursor| cursor.contains(&envelope.id)));
        } else {
            assert!(canonical
                .session_inbox_admission()
                .is_some_and(|cursor| cursor.contains(&envelope.id)));
        }
    }
}

#[tokio::test]
async fn scoped_uncertain_snapshot_cannot_be_saved_through_either_common_writer() {
    let fixture = Fixture::new(false).await;
    let mut poisoned = fixture.target.clone();
    poisoned.metadata.insert(
        bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY.into(),
        "true".into(),
    );
    assert!(fixture.store.save_session(&poisoned).await.is_err());
    assert!(fixture.store.save_runtime_state(&poisoned).await.is_err());
    let canonical = fixture
        .store
        .load_execution_admission_snapshot(&fixture.target.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!canonical
        .metadata
        .contains_key(bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY));
}

#[tokio::test]
async fn scoped_cancel_after_retention_but_before_replace_prevents_state_and_ack() {
    use super::default_actor_context_tests::DefaultWriteHook;
    let fixture = Arc::new(Fixture::new(false).await);
    let envelope = fixture.envelope();
    fixture.deliver(&envelope).await;
    let (inbox, claim) = fixture.claim().await;
    let candidate = fixture.candidate(&claim);
    let hook = DefaultWriteHook::install(
        &fixture.store,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    struct Release(Arc<DefaultWriteHook>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let _release = Release(hook.clone());
    let writer = fixture.clone();
    let task = tokio::spawn(async move {
        let mut candidate = candidate;
        writer
            .checkpoint(
                &mut candidate,
                inbox,
                claim,
                Arc::new(|_| panic!("cancelled publication")),
            )
            .await
    });
    let wait = hook.clone();
    tokio::task::spawn_blocking(move || wait.wait())
        .await
        .unwrap();
    fixture.activation.live.store(false, Ordering::SeqCst);
    hook.release();
    assert!(task.await.unwrap().is_err());
    assert!(!fixture
        .inbox
        .was_admitted(&fixture.target.id, &envelope.id)
        .await
        .unwrap());
    let canonical = fixture
        .store
        .load_execution_admission_snapshot(&fixture.target.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!bamboo_domain::has_scoped_loop_consumption(
        &canonical, &envelope
    ));
}

#[tokio::test]
async fn scoped_checkpoint_rejects_missing_consumption_or_cursor_without_ack() {
    for missing_cursor in [true, false] {
        let fixture = Fixture::new(false).await;
        let envelope = fixture.envelope();
        fixture.deliver(&envelope).await;
        let (inbox, claim) = fixture.claim().await;
        let mut candidate = if missing_cursor {
            fixture.target.clone()
        } else {
            fixture.candidate(&claim)
        };
        if !missing_cursor {
            candidate
                .metadata
                .remove(bamboo_domain::SCOPED_LOOP_CONSUMPTION_KEY);
        }
        assert!(fixture
            .checkpoint(
                &mut candidate,
                inbox,
                claim,
                Arc::new(|_| panic!("invalid consumption published"))
            )
            .await
            .is_err());
        assert!(!fixture
            .inbox
            .was_admitted(&fixture.target.id, &envelope.id)
            .await
            .unwrap());
        let canonical = fixture
            .store
            .load_execution_admission_snapshot(&fixture.target.id)
            .await
            .unwrap()
            .unwrap();
        assert!(!bamboo_domain::has_scoped_loop_consumption(
            &canonical, &envelope
        ));
    }
}

#[tokio::test]
async fn scoped_stop_after_delivery_retention_prevents_the_control_rename() {
    let fixture = Fixture::new(false).await;
    let fired = Arc::new(AtomicBool::new(false));
    let seen = fired.clone();
    let live = fixture.activation.live.clone();
    let inbox = fixture
        .inbox
        .with_owned_filesystem_hook_for_test(Arc::new(move |event, _| {
            if event == "expected_run_matched" {
                seen.store(true, Ordering::SeqCst);
                live.store(false, Ordering::SeqCst);
            }
            Ok(())
        }));
    let envelope = fixture.envelope();
    assert!(inbox
        .deliver_supervisor_loop_control(&fixture.binding, &envelope, fixture.activation.clone())
        .await
        .is_err());
    assert!(fired.load(Ordering::SeqCst));
    assert!(!fixture
        .inbox
        .was_admitted(&fixture.target.id, &envelope.id)
        .await
        .unwrap());
    assert!(fixture
        .inbox
        .claim_scoped_loop_controls(&fixture.target.id, 1, Some(&fixture.binding.owner_run_id))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(fixture.activation.notifications.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn scoped_terminal_remove_retry_preserves_the_canonical_consumption_and_never_reconfigures() {
    let fixture = Fixture::new(false).await;
    let envelope = fixture.envelope();
    fixture.deliver(&envelope).await;
    let (_, claim) = fixture.claim().await;
    let failed = Arc::new(AtomicBool::new(false));
    let observed = failed.clone();
    let retry_inbox: Arc<dyn SessionInboxPort> =
        Arc::new(fixture.inbox.with_owned_filesystem_hook_for_test(Arc::new(
            move |event, path| {
                if event == "remove"
                    && path
                        .parent()
                        .and_then(Path::file_name)
                        .and_then(|name| name.to_str())
                        == Some("cur")
                    && !observed.swap(true, Ordering::SeqCst)
                {
                    return Err(io::Error::other("controlled post-receipt remove failure"));
                }
                Ok(())
            },
        )));
    let mut session = fixture.candidate(&claim);
    assert!(fixture
        .checkpoint(
            &mut session,
            retry_inbox.clone(),
            claim.clone(),
            Arc::new(|_| {})
        )
        .await
        .is_err());
    assert!(failed.load(Ordering::SeqCst));
    assert!(fixture
        .inbox
        .was_admitted(&fixture.target.id, &envelope.id)
        .await
        .unwrap());
    let committed = bamboo_domain::scoped_loop_consumptions(&session).unwrap();
    fixture
        .checkpoint(&mut session, retry_inbox, claim, Arc::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(
        bamboo_domain::scoped_loop_consumptions(&session).unwrap(),
        committed
    );
    assert_eq!(session.messages.len(), 1);
    assert!(fixture
        .inbox
        .claim_scoped_loop_controls(&fixture.target.id, 1, Some(&fixture.binding.owner_run_id))
        .await
        .unwrap()
        .is_empty());
}
