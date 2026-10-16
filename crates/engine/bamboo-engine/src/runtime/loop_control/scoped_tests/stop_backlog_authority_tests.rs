//! Real repository-issued Root Actor capabilities; no provider or forged state.
use super::*;
use bamboo_domain::{ActorActivationFinish, RootActorExecutionBinding, RuntimeSessionPersistence};

async fn native_fixture() -> (Fixture, RootActorExecutionBinding) {
    let mut h = Fixture::new(true).await;
    // Reopen the same real directory, then let the existing repository claim
    // and start its actual physical Actor lease for the registered router owner.
    let directory = Arc::new(
        SessionStoreV2::new(h._dir.path().join("sessions"))
            .await
            .unwrap(),
    );
    let repository = SessionRepository::new(
        Arc::new(Default::default()),
        h.storage.clone(),
        Arc::new(LockedSessionStore::new(h.storage.clone())),
    )
    .with_root_actor_directory(directory);
    let binding = repository
        .bind_root_actor_execution(&h.session, OWNER)
        .await
        .unwrap()
        .expect("real repository must issue the native Root binding");
    assert_eq!(binding.owner.fence.actor_id, TARGET);
    assert_eq!(binding.owner.fence.run_id, OWNER);
    assert_eq!(binding.owner.created_at, h.session.created_at);
    let writer = binding.persistence.root_actor_writer().unwrap();
    assert_eq!(writer.fence, binding.owner.fence);
    h.config.persistence = Some(binding.persistence.clone());
    (h, binding)
}

fn stop(h: &Fixture) -> LoopControlRequest {
    let state = h.state();
    LoopControlRequest::Stop {
        operation_id: uuid::Uuid::new_v4().to_string(),
        generation: state.generation,
        expected_revision: state.control.unwrap().control_revision,
    }
}

async fn close_native(h: &mut Fixture, binding: &mut RootActorExecutionBinding) {
    let admitted = h
        .durable()
        .await
        .session_inbox_admission()
        .map_or(0, |cursor| cursor.last_admitted_sequence);
    let mut registration = h.registration.take().unwrap();
    registration.begin_finalization().await;
    assert_eq!(registration.finish(admitted).await.unwrap(), None);
    // Fixture cleanup uses the actual directory capability. This low-level
    // test has not run a provider or claimed successful task completion.
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            ActorActivationFinish::Cancelled,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
    h.assert_no_activation();
}

#[tokio::test]
async fn scoped_native_root_stop_retires_tail_under_the_real_writer_and_owned_claim() {
    let (mut h, mut binding) = native_fixture().await;
    h.start_and_admit().await;
    let command = stop(&h);
    let queued_stop = h.send(&command).await.unwrap();
    let tail = h.update("Native Root must never apply this after Stop");
    let queued_tail = h.send(&tail).await.unwrap();
    let tail_id =
        bamboo_domain::SessionMessageId::parse(queued_tail["receipt_id"].as_str().unwrap())
            .unwrap();
    h.drain(1).await.unwrap();
    assert!(!h.admit().await);
    let durable = h.durable().await;
    assert_eq!(h.state().status, LoopStatus::Stopped);
    assert!(consumption(&durable, &command).configured);
    assert_eq!(scoped_loop_consumptions(&durable).unwrap().len(), 2);
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(!cursor.contains(&tail_id));
    assert_eq!(
        cursor.last_admitted_sequence,
        queued_stop["generation"].as_u64().unwrap()
    );
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    let backlog = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    let leases = h
        .inbox
        .inspect_owned_leases(TARGET, 8, Utc::now())
        .await
        .unwrap();
    assert!(
        leases.is_empty(),
        "neither Stop nor its tail stays claimable"
    );
    let activation = binding
        .directory
        .inspect_actor(TARGET)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_eq!(activation.fence(), binding.owner.fence);
    assert_eq!(
        activation.status,
        bamboo_domain::ActorActivationStatus::Running
    );
    close_native(&mut h, &mut binding).await;
}

#[tokio::test]
async fn scoped_native_root_tail_io_failure_preserves_real_stop_and_never_acks_or_admits_tail() {
    let (mut h, mut binding) = native_fixture().await;
    h.start_and_admit().await;
    let command = stop(&h);
    let queued_stop = h.send(&command).await.unwrap();
    let tail = h.update("No application when transport retirement fails");
    let queued_tail = h.send(&tail).await.unwrap();
    let stop_id =
        bamboo_domain::SessionMessageId::parse(queued_stop["receipt_id"].as_str().unwrap())
            .unwrap();
    let tail_id =
        bamboo_domain::SessionMessageId::parse(queued_tail["receipt_id"].as_str().unwrap())
            .unwrap();
    // A real filesystem error at the exact new retirement boundary. The
    // existing Inbox exists after delivery; a file cannot be its cancelled dir.
    let blocked_directory = h
        ._dir
        .path()
        .join("sessions")
        .join("sessions")
        .join(TARGET)
        .join("inbox")
        .join("cancelled");
    tokio::fs::write(&blocked_directory, b"controlled create_dir_all failure")
        .await
        .unwrap();
    let result = h.drain(1).await;
    assert!(
        result.is_err(),
        "Stop CP must not silently ACK across retirement I/O failure: {result:?}"
    );
    let durable = h.durable().await;
    assert!(
        consumption(&durable, &command).configured,
        "an error is not rollback of real Main/typed Stop facts"
    );
    assert_eq!(scoped_loop_consumptions(&durable).unwrap().len(), 2);
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(cursor.contains(&stop_id));
    assert!(!cursor.contains(&tail_id));
    assert_eq!(
        cursor.last_admitted_sequence,
        queued_stop["generation"].as_u64().unwrap()
    );
    assert!(
        !h.inbox.was_admitted(TARGET, &stop_id).await.unwrap(),
        "actual Stop ACK is after successful retirement"
    );
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    let backlog = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (1, 1));
    assert!(
        h.session
            .metadata
            .get(bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY)
            .is_none(),
        "strict canonical reload confirmed the actual facts"
    );
    assert_eq!(
        h.session.metadata.get("runtime.counted_loop"),
        durable.metadata.get("runtime.counted_loop")
    );
    tokio::fs::remove_file(blocked_directory).await.unwrap();
    close_native(&mut h, &mut binding).await;
}

mod boundary_tests;
