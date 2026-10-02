//! Deterministic canonical-store/admission tests. These do not launch a Worker
//! or replace the separate fresh-process P5 end-to-end acceptance.
use super::*;
use bamboo_domain::{SessionInboxLimits, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::{FileSessionInbox, LockedSessionStore, SessionStoreV2};
use bamboo_tickets::{Contract, FaultPoint, TicketKind};
use std::collections::BTreeSet;

struct Fixture {
    dir: tempfile::TempDir,
    service: Arc<TicketService>,
    storage: Arc<SessionStoreV2>,
    runtime: SessionInboxRuntimeBinding,
    supervisor: Session,
    spec: DispatchSpec,
    key: String,
    child: Session,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(dir.path().join("host")).await.unwrap());
        storage
            .get_or_create_default_supervisor("fixture-model")
            .await
            .unwrap();
        let mut supervisor = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(true).unwrap();
        storage.save_session(&supervisor).await.unwrap();
        let (binding, supervisor) = verified_scope_binding(storage.as_ref(), &supervisor.id)
            .await
            .unwrap();
        let service =
            Arc::new(TicketService::open(dir.path().join("tickets"), binding.clone()).unwrap());
        let authority = Authority::from_verified_host(
            binding,
            Principal::Supervisor {
                session_id: supervisor.id.clone(),
            },
        );
        let command = service
            .prepare_command(
                &authority,
                "create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        depends_on: BTreeSet::new(),
                        contract: Contract {
                            title: "Own work".into(),
                            objective: "One result".into(),
                            constraints: vec!["Root private".into()],
                            acceptance: vec!["Exact evidence".into()],
                            user_acceptance_required: true,
                            allowed_tools: BTreeSet::from(["Task".into()]),
                        },
                    },
                    Operation::Ready {
                        work_id: "work".into(),
                    },
                    Operation::Start {
                        work_id: "work".into(),
                        temp_id: "assignment".into(),
                        workspace: None,
                    },
                ],
            )
            .unwrap();
        let receipt = service.execute(&authority, &command).unwrap();
        let snapshot = service.published().unwrap().1;
        let key = snapshot.assignments[&receipt.ids["assignment"]]
            .dispatch_key
            .clone();
        let spec = snapshot.intents[&key].immutable_spec.clone();
        let dispatch = dispatch_binding(&service, &key, &spec).unwrap();
        let mut child = Session::new_child_of(
            ticket_child_id(&key),
            &supervisor,
            "fixture-model",
            "Ticket child",
        );
        child.metadata.insert(
            TICKET_DISPATCH_KEY.into(),
            serde_json::to_string(&dispatch).unwrap(),
        );
        child.metadata.insert(
            crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
            spec.assignment_id.clone(),
        );
        child.set_last_run_status("pending");
        child.advance_child_launch_generation().unwrap();
        storage.save_session(&child).await.unwrap();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        let inbox = Arc::new(FileSessionInbox::new(
            storage.clone(),
            SessionInboxLimits::default(),
        ));
        let router = crate::SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let runtime = SessionInboxRuntimeBinding {
            router,
            inbox,
            storage: storage.clone(),
            persistence: persistence.clone(),
            parent_question_lock: Some(persistence),
        };
        Self {
            dir,
            service,
            storage,
            runtime,
            supervisor,
            spec,
            key,
            child,
        }
    }
}

#[tokio::test]
async fn canonical_activation_receipt_is_stable_queryable_and_fenced_after_restart() {
    let mut f = Fixture::new().await;
    assert!(matches!(
        query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec)
            .await
            .unwrap(),
        DispatchObservation::Pending { .. }
    ));
    let mut changed = f.spec.clone();
    changed.contract.objective = "Different".into();
    assert!(matches!(
        query_dispatch(f.storage.as_ref(), &f.service, &f.key, &changed).await,
        Err(Error::IdempotencyConflict)
    ));
    assert!(
        admit_ticket_run(&f.service, &f.runtime, &mut f.child, "forged")
            .await
            .is_err()
    );
    let registration = f
        .runtime
        .router
        .register_run(&f.child.id, "host-run")
        .await
        .unwrap();
    admit_ticket_run(&f.service, &f.runtime, &mut f.child, "host-run")
        .await
        .unwrap();
    let snapshot = f.service.published().unwrap().1;
    let receipt = snapshot.assignments[&f.spec.assignment_id]
        .runtime
        .clone()
        .unwrap();
    assert_eq!(receipt.run_id, "host-run");
    assert_eq!(receipt.session_id, ticket_child_id(&f.key));
    let seq = snapshot.seq;
    admit_ticket_run(&f.service, &f.runtime, &mut f.child, "host-run")
        .await
        .unwrap();
    assert_eq!(f.service.published().unwrap().1.seq, seq);
    assert!(
        matches!(query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec).await.unwrap(), DispatchObservation::Admitted { receipt:ref r } if r == &receipt)
    );
    let canonical = f
        .storage
        .load_runtime_control_plane(&f.child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_dispatch(&canonical).unwrap().unwrap().receipt,
        Some(receipt.clone())
    );
    drop(registration);
    let binding = f.spec.binding.clone();
    drop(f.service);
    let restarted = Arc::new(TicketService::open(f.dir.path().join("tickets"), binding).unwrap());
    assert!(
        matches!(query_dispatch(f.storage.as_ref(), &restarted, &f.key, &f.spec).await.unwrap(), DispatchObservation::OutcomeUnknown { receipt:Some(ref r), .. } if r == &receipt)
    );
    assert!(
        admit_ticket_run(&restarted, &f.runtime, &mut f.child, "new-run")
            .await
            .is_err()
    );
    assert_eq!(
        restarted.published().unwrap().1.assignments[&f.spec.assignment_id].state,
        AssignmentState::OutcomeUnknown
    );
}

#[tokio::test]
async fn prepared_receipt_survives_ticket_publication_failure_and_no_run_is_authorized() {
    let mut f = Fixture::new().await;
    let _registration = f
        .runtime
        .router
        .register_run(&f.child.id, "host-run")
        .await
        .unwrap();
    f.service.set_publication_fault(Some(Arc::new(|point| {
        if point == FaultPoint::BeforeHeadRename {
            Err(std::io::Error::other("injected disk full"))
        } else {
            Ok(())
        }
    })));
    assert!(
        admit_ticket_run(&f.service, &f.runtime, &mut f.child, "host-run")
            .await
            .is_err()
    );
    assert!(
        crate::ticket_worker_plan::TicketWorkerPlan::from_runtime_receipt(
            f.service.clone(),
            &f.spec.assignment_id
        )
        .is_err()
    );
    assert!(matches!(
        query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec)
            .await
            .unwrap(),
        DispatchObservation::OutcomeUnknown {
            receipt: Some(_),
            ..
        }
    ));
    f.service.set_publication_fault(None);
    // Same live owner can complete the pre-Run boundary after a confirmed
    // pre-HEAD failure. It must reuse the prepared Runtime receipt.
    admit_ticket_run(&f.service, &f.runtime, &mut f.child, "host-run")
        .await
        .unwrap();
    let old = f.child.clone();
    f.child.metadata.remove(TICKET_DISPATCH_KEY);
    f.child.metadata_version = old.metadata_version.saturating_sub(1);
    f.runtime
        .parent_question_lock
        .as_ref()
        .unwrap()
        .merge_save_runtime(&mut f.child)
        .await
        .unwrap();
    assert_eq!(
        read_dispatch(&f.child).unwrap(),
        read_dispatch(&old).unwrap()
    );
    let root = f
        .storage
        .load_root_authority(&f.supervisor.id)
        .await
        .unwrap()
        .unwrap();
    assert!(root.task_list.is_none());
    assert!(root.messages.is_empty());
}
