//! Canonical Start observation must not wait for a reconcile Host observation.
use super::*;
use bamboo_orchestration::ReadyWorkHost;

pub(super) async fn wait(fixture: &Fixture) -> (String, Snapshot) {
    let observed = tokio::time::timeout(WAIT, async {
        loop {
            let published = fixture.state.tickets.service().unwrap().published().unwrap();
            if published.1.assignments.len() >= 3 {
                return published;
            }
            // Wake the already assembled Ready worker. It owns fresh selection,
            // CAS retries and the original dispatch key; this observer does not
            // call reconcile or cancel an in-flight admission.
            fixture.state.tickets.wake_ready_work();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        let (commit, snapshot) = fixture.state.tickets.service().unwrap().published().unwrap();
        panic!(
            "three canonical Starts were not observed: {error}; commit={commit}, provider_calls={}, snapshot={}",
            fixture.provider.calls.load(Ordering::SeqCst),
            serde_json::to_string(&snapshot).unwrap()
        );
    });
    let snapshot = &observed.1;
    assert_eq!(snapshot.assignments.len(), 3);
    assert_eq!(snapshot.intents.len(), 3);
    let receipt_ids = snapshot
        .receipts
        .values()
        .filter_map(|receipt| receipt.ids.get("assignment"))
        .collect::<Vec<_>>();
    assert_eq!(receipt_ids.len(), 3);
    assert_eq!(
        receipt_ids.into_iter().collect::<BTreeSet<_>>(),
        snapshot.assignments.keys().collect()
    );
    for assignment in snapshot.assignments.values() {
        let work = &snapshot.tickets[&assignment.work_id];
        assert_eq!(work.active_assignment.as_ref(), Some(&assignment.id));
        assert_eq!(work.generation, assignment.generation);
        let intent = &snapshot.intents[&assignment.dispatch_key];
        assert_eq!(intent.assignment_id, assignment.id);
        assert_eq!(intent.dispatch_key, assignment.dispatch_key);
        assert_eq!(intent.immutable_spec.work_id, assignment.work_id);
        assert_eq!(intent.immutable_spec.generation, assignment.generation);
        assert_eq!(intent.receipt, assignment.runtime);
    }
    observed
}

#[tokio::test]
async fn three_committed_starts_remain_observable_while_original_host_ensure_waits() {
    let mut fixture = Fixture::new(3).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(3))
        .await
        .unwrap();
    let first = fixture.next_request().await;
    let second = fixture.next_request().await;
    let queued_index = (0..3)
        .find(|index| !first.is_work(*index) && !second.is_work(*index))
        .unwrap();
    let queued_work = &fixture.works[queued_index].id;
    let queued_child = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = fixture.snapshot();
            if let Some(assignment) = snapshot
                .assignments
                .values()
                .find(|assignment| &assignment.work_id == queued_work)
            {
                assert_eq!(assignment.state, AssignmentState::DispatchPending);
                assert!(assignment.runtime.is_none());
                let child_id = ticket_runtime::ticket_child_id(&assignment.dispatch_key);
                if fixture
                    .state
                    .storage
                    .load_session(&child_id)
                    .await
                    .unwrap()
                    .is_some()
                {
                    break child_id;
                }
            }
            fixture.state.tickets.wake_ready_work();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the real third Start and queued Child become durable");
    let launch = fixture
        .state
        .spawn_scheduler
        .lock_child_launch(&queued_child)
        .await;
    let before = fixture
        .state
        .tickets
        .service()
        .unwrap()
        .published()
        .unwrap();
    let tickets = fixture.state.tickets.clone();
    let intent = before
        .1
        .intents
        .values()
        .find(|intent| intent.immutable_spec.work_id == *queued_work)
        .unwrap();
    let ensure = ReadyWorkHost::ensure(
        tickets.as_ref(),
        &intent.dispatch_key,
        &intent.immutable_spec,
        MAX_ROUNDS,
    );
    tokio::pin!(ensure);
    assert!(tokio::time::timeout(QUIET, &mut ensure).await.is_err());

    // Use the real Host.ensure called by Ready reconciliation for this exact
    // existing intent. Its launch fence is held, independently of retry-store
    // cooldown or Ready selection. Keep the same future alive until explicit
    // release; do not abort it or create another Start.
    for _ in 0..2 {
        let observed = wait(&fixture).await;
        assert_eq!(observed.0, before.0);
        assert_eq!(
            serde_json::to_value(&observed.1).unwrap(),
            serde_json::to_value(&before.1).unwrap()
        );
    }
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 2);
    eprintln!(
        "three-Start launch-lock control: commit={}, assignments={}, intents={}, provider_calls=2, host_ensure_pending=true, queued_child={queued_child}",
        before.0, before.1.assignments.len(), before.1.intents.len()
    );
    drop(launch);
    match tokio::time::timeout(WAIT, &mut ensure)
        .await
        .expect("the same Host observation completes after explicit launch-fence release")
    {
        Ok(_) | Err(bamboo_tickets::Error::RevisionConflict) => {}
        Err(error) => panic!("queued admission failed: {error}"),
    }
    assert_eq!(fixture.snapshot().assignments.len(), 3);
    fixture.assert_quiet(2).await;
    first.complete();
    let queued = fixture.next_request().await;
    assert!(queued.is_work(queued_index));
    queued.complete();
    second.complete();
    for work in &fixture.works {
        fixture.wait_submitted(&work.id).await;
    }
    let final_snapshot = fixture.snapshot();
    assert_eq!(final_snapshot.assignments.len(), 3);
    assert_eq!(final_snapshot.submissions.len(), 3);
    for (id, original) in &before.1.assignments {
        let final_assignment = &final_snapshot.assignments[id];
        assert_eq!(final_assignment.dispatch_key, original.dispatch_key);
        assert_eq!(final_assignment.generation, original.generation);
        if original.runtime.is_some() {
            assert_eq!(final_assignment.runtime, original.runtime);
        }
        wait_native_terminal(
            &fixture.state,
            &ticket_runtime::ticket_child_id(&original.dispatch_key),
        )
        .await;
        assert_ne!(
            final_snapshot.tickets[&original.work_id].state,
            WorkState::Accepted
        );
    }
    fixture.assert_quiet(3).await;
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
}
