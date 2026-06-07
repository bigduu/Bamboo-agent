//! Metrics lifecycle helpers for the agent loop runner.

mod round_metrics;
mod session_metrics;

pub(super) use round_metrics::{
    record_round_and_session_error, record_round_completed, record_round_started,
};
pub(super) use session_metrics::{
    record_session_cancelled, record_session_completed_if_resolved, record_session_started,
};

#[cfg(test)]
mod tests;
