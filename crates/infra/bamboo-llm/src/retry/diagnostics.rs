//! Content-free initial-request evidence. This bounded process-local projection
//! is also emitted through ordinary INFO logs; it never enables telemetry export.

use std::collections::VecDeque;

use super::*;

const MAX_RECENT_EVENTS: usize = 128;
const MAX_SESSION_EVENTS: usize = 12;

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequestStopReason {
    AttemptsExhausted,
    TimeBudgetExhausted,
    RetryDelayExceedsBudget,
    NonRetryableTransport,
    UnclassifiedErrorBody,
    CancelledOrDropped,
}

impl RequestStopReason {
    fn guidance(self) -> &'static str {
        match self {
            Self::AttemptsExhausted => "Automatic retry attempt limit reached",
            Self::TimeBudgetExhausted => "Automatic retry time budget exhausted",
            Self::RetryDelayExceedsBudget => {
                "Required retry delay exceeds the remaining time budget"
            }
            Self::NonRetryableTransport => "Transport failure is not automatically retryable",
            Self::UnclassifiedErrorBody => "Upstream error response could not be safely classified",
            Self::CancelledOrDropped => "Request cancelled or its caller stopped waiting",
        }
    }
}

/// Only host-generated IDs, hashed local correlation and finite classifications
/// enter this DTO. No upstream ID/header, URL, error chain or content is copied.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InitialRequestDiagnostic {
    pub(super) schema_version: u8,
    pub(super) diagnostic_id: uuid::Uuid,
    request_id: uuid::Uuid,
    error_id: Option<uuid::Uuid>,
    observed_at: chrono::DateTime<chrono::Utc>,
    pub(super) provider: &'static str,
    pub(super) session_hash: Option<String>,
    // The caller does not supply an authoritative Engine run ID to this layer.
    pub(super) run_id: Option<uuid::Uuid>,
    pub(super) attempt: u32,
    pub(super) attempt_limit: u32,
    pub(super) event: &'static str,
    pub(super) phase: &'static str,
    pub(super) failure: &'static str,
    pub(super) status: Option<u16>,
    pub(super) attempt_timeout_ms: u64,
    pub(super) total_budget_ms: u64,
    pub(super) attempt_elapsed_ms: u64,
    pub(super) total_elapsed_ms: u64,
    pub(super) remaining_budget_ms: u64,
    pub(super) retry_delay_ms: Option<u64>,
    pub(super) stop_reason: Option<RequestStopReason>,
    // A timeout before response headers does not prove the POST was unreceived.
    pub(super) upstream_outcome: &'static str,
}

#[derive(Debug, Clone)]
pub(super) struct AttemptObservation {
    pub(super) diagnostic: InitialRequestDiagnostic,
    request_started: Instant,
    attempt_started: Instant,
    deadline: Instant,
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn recent_events() -> &'static Mutex<VecDeque<InitialRequestDiagnostic>> {
    static EVENTS: OnceLock<Mutex<VecDeque<InitialRequestDiagnostic>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Recent evidence for this exact session hash only. Bounded, process-local and
/// best-effort; absence/eviction/restart is not evidence that no failure occurred.
pub fn recent_initial_request_diagnostics(session_id: &str) -> Vec<InitialRequestDiagnostic> {
    let hash = crate::providers::common::log_identity(session_id);
    recent_events()
        .lock()
        .map(|events| project_session(&events, &hash))
        .unwrap_or_default()
}

fn project_session(
    events: &VecDeque<InitialRequestDiagnostic>,
    hash: &str,
) -> Vec<InitialRequestDiagnostic> {
    events
        .iter()
        .rev()
        .filter(|event| event.session_hash.as_deref() == Some(hash))
        .take(MAX_SESSION_EVENTS)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn push_bounded(events: &mut VecDeque<InitialRequestDiagnostic>, event: InitialRequestDiagnostic) {
    while events.len() >= MAX_RECENT_EVENTS {
        events.pop_front();
    }
    events.push_back(event);
}

impl AttemptObservation {
    fn refresh(&mut self) {
        let now = Instant::now();
        if self.diagnostic.phase != "backoff" {
            self.diagnostic.attempt_elapsed_ms =
                millis(now.saturating_duration_since(self.attempt_started));
        }
        self.diagnostic.total_elapsed_ms =
            millis(now.saturating_duration_since(self.request_started));
        self.diagnostic.remaining_budget_ms = millis(self.deadline.saturating_duration_since(now));
    }

    fn terminal(&self) -> LLMError {
        let d = &self.diagnostic;
        let status = d
            .status
            .map(|s| format!("HTTP {s}"))
            .unwrap_or_else(|| "no HTTP response".into());
        let stop = d
            .stop_reason
            .expect("terminal diagnostic has a stop reason");
        LLMError::RequestFailed(format!(
            "Anthropic upstream request failed ({status}): {}. {} after {} request(s) (limit {}). Diagnostic ID: {}. Try again later or contact the upstream gateway administrator.",
            d.failure, stop.guidance(), d.attempt, d.attempt_limit, d.diagnostic_id,
        ))
    }
}

impl InitialRequestBudget {
    pub(crate) fn with_session_id(mut self, session_id: Option<&str>) -> Self {
        self.session_hash = session_id.map(crate::providers::common::log_identity);
        self
    }

    pub(super) fn observation(
        &self,
        attempt_started: Instant,
        phase: &'static str,
        failure: &'static str,
        status: Option<StatusCode>,
    ) -> AttemptObservation {
        AttemptObservation {
            diagnostic: InitialRequestDiagnostic {
                schema_version: 1,
                diagnostic_id: self.diagnostic_id,
                request_id: self.diagnostic_id,
                error_id: None,
                observed_at: chrono::Utc::now(),
                provider: "anthropic",
                session_hash: self.session_hash.clone(),
                run_id: None,
                attempt: self.attempts.load(Ordering::Relaxed),
                attempt_limit: self.max_attempts,
                event: "attempt_start",
                phase,
                failure,
                status: status.map(|s| s.as_u16()),
                attempt_timeout_ms: millis(self.attempt_timeout),
                total_budget_ms: millis(self.deadline.saturating_duration_since(self.started)),
                attempt_elapsed_ms: 0,
                total_elapsed_ms: 0,
                remaining_budget_ms: 0,
                retry_delay_ms: None,
                stop_reason: None,
                upstream_outcome: "unknown",
            },
            request_started: self.started,
            attempt_started,
            deadline: self.deadline,
        }
    }

    pub(super) fn record(&self, mut observation: AttemptObservation) {
        observation.refresh();
        observation.diagnostic.observed_at = chrono::Utc::now();
        if observation.diagnostic.stop_reason.is_some() {
            observation.diagnostic.error_id = Some(self.diagnostic_id);
        }
        let d = &observation.diagnostic;
        // JSON comes exclusively from the typed allowlist above, never from
        // ambient tracing events. INFO is retained by the default file sink.
        if let Ok(json) = serde_json::to_string(d) {
            tracing::info!(
                diagnostic_id = %d.diagnostic_id,
                request_id = %d.diagnostic_id,
                diagnostic = %json,
                "LLM initial request diagnostic"
            );
        }
        // Unscoped calls cannot be exported by a current-session tool. Keep
        // their ordinary logs without evicting scoped evidence from this buffer.
        if d.session_hash.is_some() {
            if let Ok(mut events) = recent_events().lock() {
                push_bounded(&mut events, d.clone());
            }
        }
        if let Ok(mut latest) = self.latest.lock() {
            *latest = Some(observation);
        }
    }

    pub(super) fn decide(
        &self,
        outcome: &mut RetryAttempt<Result<InitialResponse, LLMError>>,
        decision: RetryDecision,
    ) {
        let Some(mut observation) = outcome.observation.take() else {
            return;
        };
        let d = &mut observation.diagnostic;
        match decision {
            RetryDecision::Retry(delay) => {
                d.event = "retry_scheduled";
                d.retry_delay_ms = Some(millis(delay));
            }
            RetryDecision::AttemptsExhausted => {
                d.stop_reason = Some(RequestStopReason::AttemptsExhausted)
            }
            RetryDecision::BudgetExhausted => {
                d.stop_reason = Some(RequestStopReason::TimeBudgetExhausted)
            }
            RetryDecision::DelayExceedsBudget => {
                d.stop_reason = Some(RequestStopReason::RetryDelayExceedsBudget)
            }
            RetryDecision::Return => {}
        }
        if d.stop_reason.is_some() {
            d.event = "stop";
            if matches!(outcome.result, Err(LLMError::RequestFailed(_))) {
                outcome.result = Err(observation.terminal());
            }
        }
        observation.refresh();
        self.record(observation.clone());
        if matches!(decision, RetryDecision::Retry(_)) {
            observation.diagnostic.phase = "backoff";
            if let Ok(mut latest) = self.latest.lock() {
                *latest = Some(observation);
            }
        }
    }
}

pub(super) struct CancellationObservation<'a> {
    pub(super) budget: &'a InitialRequestBudget,
    pub(super) completed: bool,
}

impl Drop for CancellationObservation<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let latest = self
            .budget
            .latest
            .lock()
            .ok()
            .and_then(|value| value.clone());
        if let Some(mut observation) = latest {
            observation.diagnostic.event = "stop";
            observation.diagnostic.stop_reason = Some(RequestStopReason::CancelledOrDropped);
            self.budget.record(observation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_is_bounded_current_session_only_and_contains_no_raw_session_text() {
        let session = format!("private_session_{}", uuid::Uuid::new_v4());
        let cfg = RetryConfig::default();
        let budget = InitialRequestBudget::new(&cfg).with_session_id(Some(&session));
        let mut buffer = VecDeque::new();
        for _ in 0..(MAX_RECENT_EVENTS + 1) {
            push_bounded(
                &mut buffer,
                budget
                    .observation(Instant::now(), "send_headers", "none", None)
                    .diagnostic,
            );
        }
        let events = project_session(&buffer, &crate::providers::common::log_identity(&session));
        assert_eq!(events.len(), MAX_SESSION_EVENTS);
        assert_eq!(buffer.len(), MAX_RECENT_EVENTS);
        assert!(project_session(
            &buffer,
            &crate::providers::common::log_identity("different-session")
        )
        .is_empty());
        let json = serde_json::to_string(&events).unwrap();
        assert!(!json.contains(&session));
        assert!(json.len() < 12 * 1024);
    }
}
