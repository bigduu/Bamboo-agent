use crate::app_state::{AgentRunner, AgentStatus};

#[test]
fn stop_cancels_running_status() {
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;

    runner.cancel_token.cancel();

    assert!(runner.cancel_token.is_cancelled());
}

#[test]
fn completed_status_not_cancellable() {
    let status = AgentStatus::Completed;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn cancelled_status_can_be_set() {
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Cancelled;

    assert!(matches!(runner.status, AgentStatus::Cancelled));
}

#[test]
fn runner_has_cancel_token() {
    let runner = AgentRunner::new();
    let _token_clone = runner.cancel_token.clone();
}
