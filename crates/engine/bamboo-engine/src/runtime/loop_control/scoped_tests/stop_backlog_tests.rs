//! Real scoped commands exercise transport retirement, never model execution.
//! These tests intentionally use only the pre-fix public/private fixture API.

use super::*;

fn stop_current(h: &Fixture) -> LoopControlRequest {
    let state = h.state();
    LoopControlRequest::Stop {
        operation_id: uuid::Uuid::new_v4().to_string(),
        generation: state.generation,
        expected_revision: state.control.unwrap().control_revision,
    }
}

fn queued_id(receipt: &Value) -> bamboo_domain::SessionMessageId {
    bamboo_domain::SessionMessageId::parse(receipt["receipt_id"].as_str().unwrap()).unwrap()
}

async fn finish_actual_admission(h: &mut Fixture) {
    let admitted = h
        .durable()
        .await
        .session_inbox_admission()
        .map(|cursor| cursor.last_admitted_sequence)
        .unwrap_or(0);
    let mut registration = h.registration.take().expect("real original owner");
    registration.begin_finalization().await;
    assert_eq!(registration.finish(admitted).await.unwrap(), None);
    h.assert_no_activation();
}

#[tokio::test]
async fn scoped_stop_retires_unexecuted_tail_without_admitting_it_and_reclaims_capacity() {
    let mut h = Fixture::new_with_limits(
        true,
        SessionInboxLimits {
            max_backlog: 2,
            ..Default::default()
        },
    )
    .await;
    let start = h.start_and_admit().await;
    let start_proof = consumption(&h.durable().await, &start);
    let before_messages = serde_json::to_value(&h.session.messages).unwrap();
    let stop = stop_current(&h);
    let stop_queued = h.send(&stop).await.unwrap();
    let stop_generation = stop_queued["generation"].as_u64().unwrap();
    let trailing = h.update("Unexecuted tail must never become a provider goal");
    let trailing_queued = h.send(&trailing).await.unwrap();
    let tail_id = queued_id(&trailing_queued);
    let before = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!((before.pending, before.claimed), (2, 0));

    h.drain(1).await.unwrap();
    assert!(!h.admit().await);
    let durable = h.durable().await;
    let stopped = counted_loop::read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(stopped.status, LoopStatus::Stopped);
    assert_eq!(stopped.objective, "Remote A");
    assert_eq!(
        (
            stopped.iterations_started,
            stopped.iterations_completed,
            stopped.rounds_started,
        ),
        (1, 0, 1)
    );
    assert_eq!(
        serde_json::to_value(&durable.messages).unwrap(),
        before_messages
    );
    assert_eq!(consumption(&durable, &start), start_proof);
    assert!(consumption(&durable, &stop).configured);
    assert!(!scoped_loop_consumptions(&durable)
        .unwrap()
        .iter()
        .any(|record| Some(record.operation_id.as_str()) == operation_id(&trailing)));
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(!cursor.contains(&tail_id));
    assert_eq!(cursor.last_admitted_sequence, stop_generation);
    assert_eq!(cursor.last_observed_sequence, stop_generation);
    let backlog = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(
        (backlog.pending, backlog.claimed),
        (0, 0),
        "an unexecuted old-run control cannot permanently occupy Inbox capacity"
    );

    // A cancelled transport tombstone preserves its original delivery receipt,
    // without falsely claiming that the command was configured or admitting it.
    let generation = backlog.generation;
    let retry = h.send(&trailing).await.unwrap();
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["configured"], false);
    assert_eq!(retry["receipt_id"], trailing_queued["receipt_id"]);
    let retried = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(retried.generation, generation);
    assert_eq!((retried.pending, retried.claimed), (0, 0));
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    let status = h.status(operation_id(&trailing)).await.unwrap();
    assert_eq!(status["configured"], false);
    assert!(status["operation_receipt"].is_null());
    assert_eq!(status["checkpoint_observation"], "not_observed");
    assert_eq!(status["objective_applied_actual"], false);
    let mut conflicting = trailing.clone();
    let LoopControlRequest::Update { objective, .. } = &mut conflicting else {
        unreachable!();
    };
    *objective = Some("Conflicting payload under retired operation UUID".into());
    let conflict = h.send(&conflicting).await.unwrap_err();
    assert!(conflict
        .to_string()
        .contains("different delivery semantics"));
    assert_eq!(
        h.inbox.inspect(TARGET).await.unwrap().generation,
        generation
    );

    finish_actual_admission(&mut h).await;
    for text in [
        "Independent genuine input one",
        "Independent genuine input two",
    ] {
        h.inbox
            .deliver(&bamboo_domain::SessionMessageEnvelope::user_input(
                TARGET, text,
            ))
            .await
            .expect("both reclaimed backlog slots remain available to ordinary User input");
    }
    let ordinary = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!((ordinary.pending, ordinary.claimed), (2, 0));
    h.assert_no_activation();
}

#[tokio::test]
async fn scoped_stop_closes_only_new_carriers_and_preserves_live_status_and_immutable_retry() {
    let mut h = Fixture::new(true).await;
    h.start_and_admit().await;
    let stop = stop_current(&h);
    h.send(&stop).await.unwrap();
    h.drain(1).await.unwrap();
    let stop_proof = consumption(&h.durable().await, &stop);
    assert!(stop_proof.configured);
    let before = h.inbox.inspect(TARGET).await.unwrap();
    let late = h.update("New command after committed Stop must not enqueue");
    let late_result = h.send(&late).await;
    assert!(
        late_result.is_err(),
        "canonical same-execution Stop must close new transport admission: {late_result:?}"
    );
    let after = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!((after.pending, after.claimed), (0, 0));
    assert_eq!(consumption(&h.durable().await, &stop), stop_proof);
    let operation = operation_id(&stop).unwrap();
    let status = h.status(Some(operation)).await.unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(
        status["operation_receipt"],
        serde_json::to_value(&stop_proof).unwrap()
    );
    let retry = h.send(&stop).await.unwrap();
    assert_eq!(retry["configured"], true);
    assert_eq!(retry["replayed"], true);
    assert_eq!(
        h.inbox.inspect(TARGET).await.unwrap().generation,
        before.generation
    );
    assert!(!h.admit().await);
    assert_eq!(h.state().status, LoopStatus::Stopped);
    assert_eq!(
        h.status(Some(operation)).await.unwrap()["operation_receipt"],
        status["operation_receipt"]
    );
    finish_actual_admission(&mut h).await;
    assert!(h.status(Some(operation)).await.is_err());
    assert!(h.send(&stop).await.is_err());
    h.assert_no_activation();
}

#[tokio::test]
async fn scoped_stop_tail_retirement_does_not_spend_typed_consumption_capacity() {
    let mut h = Fixture::new(true).await;
    let start = h.start_and_admit().await;
    let stop = stop_current(&h);
    let stop_queued = h.send(&stop).await.unwrap();
    let stop_generation = stop_queued["generation"].as_u64().unwrap();
    let mut tails = Vec::new();
    // More tail commands than the ENTIRE successful/rejected receipt ledger;
    // a loop that just applies and records 65 rejections is not a valid fix.
    for index in 0..=bamboo_domain::MAX_SCOPED_LOOP_CONSUMPTIONS {
        let command = h.update(&format!("Never apply trailing objective {index}"));
        let receipt = h.send(&command).await.unwrap();
        tails.push((command, queued_id(&receipt)));
    }
    let queued = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(
        queued.pending,
        2 + bamboo_domain::MAX_SCOPED_LOOP_CONSUMPTIONS
    );
    h.drain(1).await.unwrap();
    assert!(!h.admit().await);
    let durable = h.durable().await;
    let records = scoped_loop_consumptions(&durable).unwrap();
    assert_eq!(records.len(), 2, "only actual Start and Stop were consumed");
    assert!(consumption(&durable, &start).configured);
    assert!(consumption(&durable, &stop).configured);
    for (command, id) in tails {
        assert!(!records
            .iter()
            .any(|record| Some(record.operation_id.as_str()) == operation_id(&command)));
        assert!(!h.inbox.was_admitted(TARGET, &id).await.unwrap());
        assert!(!durable.session_inbox_admission().unwrap().contains(&id));
    }
    let cursor = durable.session_inbox_admission().unwrap();
    assert_eq!(cursor.last_admitted_sequence, stop_generation);
    assert_eq!(cursor.last_observed_sequence, stop_generation);
    let backlog = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(
        (backlog.pending, backlog.claimed),
        (0, 0),
        "Stop must retire the whole bounded tail without a 64-record cutoff"
    );
    assert_eq!(h.state().status, LoopStatus::Stopped);
    finish_actual_admission(&mut h).await;
}
