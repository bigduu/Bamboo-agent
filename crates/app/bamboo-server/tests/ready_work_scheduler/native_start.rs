//! Actual native entry evidence, independent of preparation/Actor timestamps.
use super::*;
use bamboo_orchestration::patrol::{Observation, ObservationPort};

async fn sample(fixture: &Fixture, work: &str, assignment: &str) -> Observation {
    let scope = fixture
        .state
        .tickets
        .patrol_scope(fixture.project.as_str(), work, assignment)
        .await
        .unwrap();
    let mut port = fixture
        .state
        .tickets
        .patrol_observation(&scope)
        .await
        .unwrap();
    let sample = port.observation(&scope, 0).unwrap();
    assert_eq!(sample.heartbeat_at_ms, None);
    assert_eq!(sample.progress, None);
    sample
}

#[tokio::test]
async fn live_start_requires_real_task_entry_and_never_restores_from_stop_or_shutdown() {
    let mut fixture = Fixture::new(3).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(3))
        .await
        .unwrap();
    start_observation::wait(&fixture).await;
    let first = fixture.next_request().await;
    let second = fixture.next_request().await;
    let first_index = (0..3).find(|i| first.is_work(*i)).unwrap();
    let second_index = (0..3).find(|i| second.is_work(*i)).unwrap();
    assert_ne!(first_index, second_index);
    let queued_index = (0..3)
        .find(|i| *i != first_index && *i != second_index)
        .unwrap();
    let first_work = fixture.works[first_index].id.clone();
    let second_work = fixture.works[second_index].id.clone();
    let queued_work = fixture.works[queued_index].id.clone();
    let before = fixture
        .state
        .tickets
        .service()
        .unwrap()
        .published()
        .unwrap();
    let exact_assignment = |work: &str| {
        before
            .1
            .assignments
            .values()
            .find(|a| a.work_id == work)
            .unwrap()
            .id
            .as_str()
    };
    let first_assignment = exact_assignment(&first_work);
    let second_assignment = exact_assignment(&second_work);
    let queued_assignment_id = exact_assignment(&queued_work);
    let first_sample = sample(&fixture, &first_work, first_assignment).await;
    let started = first_sample
        .started_at_ms
        .expect("actual native Agent execution entered before provider admission");
    assert!(started > 0);
    assert!(sample(&fixture, &second_work, second_assignment)
        .await
        .started_at_ms
        .is_some());
    assert_eq!(
        sample(&fixture, &queued_work, queued_assignment_id)
            .await
            .started_at_ms,
        None
    );
    let queued_assignment = before
        .1
        .assignments
        .values()
        .find(|a| a.work_id == queued_work)
        .unwrap();
    let queued_child = ticket_runtime::ticket_child_id(&queued_assignment.dispatch_key);
    let (queued_run, queued_birth) = tokio::time::timeout(WAIT, async {
        loop {
            if let (Some(run), Some(child)) = (
                fixture
                    .state
                    .session_activation_router
                    .current_run_id(&queued_child)
                    .await,
                fixture
                    .state
                    .storage
                    .load_runtime_control_plane(&queued_child)
                    .await
                    .unwrap(),
            ) {
                break (run, child.created_at);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the third native logical Run is prepared behind the two real slots");
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_start(&queued_child, &queued_run, queued_birth)
        .is_none());
    assert_eq!(
        sample(&fixture, &first_work, first_assignment)
            .await
            .started_at_ms,
        Some(started)
    );
    assert_eq!(
        fixture
            .state
            .tickets
            .service()
            .unwrap()
            .published()
            .unwrap()
            .0,
        before.0
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 2);

    let assignment = before
        .1
        .assignments
        .values()
        .find(|a| a.work_id == first_work)
        .unwrap();
    let receipt = assignment.runtime.as_ref().unwrap();
    let child = fixture
        .state
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let live = fixture
        .state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .unwrap();
    assert_eq!(live.started_at_ms(), Some(started));
    assert!(!live.instance_id.is_nil());
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_start(&child.id, "wrong-run", child.created_at)
        .is_none());
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_start(
            &child.id,
            &receipt.run_id,
            child.created_at + chrono::Duration::milliseconds(1)
        )
        .is_none());
    let mut wrong = first_sample.scope.clone();
    wrong.generation += 1;
    assert!(fixture
        .state
        .tickets
        .patrol_observation(&wrong)
        .await
        .is_err());
    wrong = first_sample.scope.clone();
    wrong.project_id = "foreign-project".into();
    assert!(fixture
        .state
        .tickets
        .patrol_observation(&wrong)
        .await
        .is_err());

    first.complete();
    let queued = fixture.next_request().await;
    assert!(queued.is_work(queued_index));
    assert!(sample(&fixture, &queued_work, queued_assignment_id)
        .await
        .started_at_ms
        .is_some());
    fixture.wait_submitted(&first_work).await;
    wait_native_terminal(&fixture.state, &child.id).await;
    assert_eq!(
        sample(&fixture, &first_work, first_assignment)
            .await
            .started_at_ms,
        None
    );
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .is_none());
    queued.complete();
    second.complete();
    for work in &fixture.works {
        fixture.wait_submitted(&work.id).await;
    }
    fixture.assert_quiet(3).await;
    let finished = fixture.snapshot();
    assert_eq!(finished.assignments.len(), 3);
    assert_eq!(finished.submissions.len(), 3);
    for (id, original) in &before.1.assignments {
        assert_eq!(finished.assignments[id].dispatch_key, original.dispatch_key);
        assert_eq!(finished.assignments[id].generation, original.generation);
        assert_ne!(
            finished.tickets[&original.work_id].state,
            WorkState::Accepted
        );
    }
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .is_none());
}
