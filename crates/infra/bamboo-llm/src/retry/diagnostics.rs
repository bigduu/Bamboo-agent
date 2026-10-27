//! Content-free initial-request evidence. This bounded process-local projection
//! is also emitted through ordinary INFO logs; it never enables telemetry export.

use std::collections::VecDeque;

use super::*;

const MAX_RECENT_EVENTS: usize = 128;
const MAX_SESSION_EVENTS: usize = 12;

pub use bamboo_domain::LlmRequestStopReason as RequestStopReason;

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
    pub(super) attempt_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) retry_mode: Option<&'static str>,
    pub(super) event: &'static str,
    pub(super) phase: &'static str,
    pub(super) failure: &'static str,
    pub(super) status: Option<u16>,
    pub(super) attempt_timeout_ms: u64,
    pub(super) total_budget_ms: Option<u64>,
    pub(super) attempt_elapsed_ms: u64,
    pub(super) total_elapsed_ms: u64,
    pub(super) remaining_budget_ms: Option<u64>,
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
    deadline: Option<Instant>,
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

enum RecentDiagnostic {
    InitialRequest(InitialRequestDiagnostic),
    StreamTimeout(bamboo_domain::LlmRequestDiagnostic),
}

fn recent_events() -> &'static Mutex<VecDeque<RecentDiagnostic>> {
    static EVENTS: OnceLock<Mutex<VecDeque<RecentDiagnostic>>> = OnceLock::new();
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
    events: &VecDeque<RecentDiagnostic>,
    hash: &str,
) -> Vec<InitialRequestDiagnostic> {
    events
        .iter()
        .rev()
        .filter_map(|event| match event {
            RecentDiagnostic::InitialRequest(event) => Some(event),
            RecentDiagnostic::StreamTimeout(_) => None,
        })
        .filter(|event| event.session_hash.as_deref() == Some(hash))
        .take(MAX_SESSION_EVENTS)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn push_bounded(events: &mut VecDeque<RecentDiagnostic>, event: RecentDiagnostic) {
    while events.len() >= MAX_RECENT_EVENTS {
        events.pop_front();
    }
    events.push_back(event);
}

/// Reuse the existing bounded live-event projection. The canonical Session
/// checkpoint owns persistence; this buffer only bridges the existing interval
/// between execution returning an error and terminal owner finalization.
pub fn record_stream_timeout_diagnostic(diagnostic: bamboo_domain::LlmRequestDiagnostic) {
    if let Ok(mut events) = recent_events().lock() {
        push_bounded(&mut events, RecentDiagnostic::StreamTimeout(diagnostic));
    }
}

fn public_summary(event: &InitialRequestDiagnostic) -> Option<bamboo_domain::LlmRequestDiagnostic> {
    event.stop_reason?;
    bamboo_domain::LlmRequestDiagnostic::from_json(&serde_json::to_string(event).ok()?)
}

/// Optional live-event projection. Session-scoped and matched against the exact
/// generated error, never against an arbitrary UUID in upstream error prose.
pub fn public_request_diagnostic(
    session_id: &str,
    message: &str,
) -> Option<bamboo_domain::LlmRequestDiagnostic> {
    if message.len() > 4096 {
        return None;
    }
    recent_events()
        .lock()
        .ok()?
        .iter()
        .rev()
        .filter_map(|event| match event {
            RecentDiagnostic::InitialRequest(event) => public_summary(event),
            RecentDiagnostic::StreamTimeout(diagnostic) => Some(diagnostic.clone()),
        })
        .find(|diagnostic| {
            diagnostic.matches_session(session_id) && diagnostic.matches_error_message(message)
        })
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
        self.diagnostic.remaining_budget_ms = self
            .deadline
            .map(|d| millis(d.saturating_duration_since(now)));
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
        let limit = d
            .attempt_limit
            .map(|n| format!(" (limit {n})"))
            .unwrap_or_default();
        LLMError::RequestFailed(format!(
            "Anthropic upstream request failed ({status}): {}. {} after {} request(s){limit}. Diagnostic ID: {}. Try again later or contact the upstream gateway administrator.",
            d.failure, stop.guidance(), d.attempt, d.diagnostic_id,
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
                attempt_limit: (!self.continuous).then_some(self.max_attempts),
                retry_mode: self.continuous.then_some("until_cancelled"),
                event: "attempt_start",
                phase,
                failure,
                status: status.map(|s| s.as_u16()),
                attempt_timeout_ms: millis(self.attempt_timeout),
                total_budget_ms: (!self.continuous)
                    .then(|| millis(self.deadline.saturating_duration_since(self.started))),
                attempt_elapsed_ms: 0,
                total_elapsed_ms: 0,
                remaining_budget_ms: (!self.continuous).then_some(0),
                retry_delay_ms: None,
                stop_reason: None,
                upstream_outcome: "unknown",
            },
            request_started: self.started,
            attempt_started,
            deadline: (!self.continuous).then_some(self.deadline),
        }
    }

    pub(super) fn record(&self, mut observation: AttemptObservation) {
        observation.refresh();
        observation.diagnostic.observed_at = chrono::Utc::now();
        if observation.diagnostic.stop_reason.is_some() {
            observation.diagnostic.error_id = Some(self.diagnostic_id);
        }
        let d = &observation.diagnostic;
        if let Some(summary) = public_summary(d) {
            let _ = INITIAL_HTTP_RETRY_OBSERVER.try_with(|observer| {
                if let Ok(mut terminal) = observer.terminal_diagnostic.lock() {
                    *terminal = Some(summary);
                }
            });
        }
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
                push_bounded(&mut events, RecentDiagnostic::InitialRequest(d.clone()));
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
            RetryDecision::DelayTooLarge => {
                d.stop_reason = Some(RequestStopReason::RetryDelayTooLarge)
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
                RecentDiagnostic::InitialRequest(
                    budget
                        .observation(Instant::now(), "send_headers", "none", None)
                        .diagnostic,
                ),
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

    #[test]
    fn timeout_projection_uses_the_existing_ring_and_requires_exact_error_identity() {
        let session = format!("private-timeout-session-{}", uuid::Uuid::new_v4());
        let sample = || {
            let id = uuid::Uuid::new_v4();
            bamboo_domain::LlmRequestDiagnostic::from_stream_timeout(
                bamboo_domain::StreamTimeoutDiagnostic {
                    schema_version: 1,
                    kind: "stream_timeout".into(),
                    diagnostic_id: id,
                    error_id: id,
                    observed_at: chrono::Utc::now(),
                    provider: None,
                    model: Some("model".into()),
                    session_hash: bamboo_domain::llm_diagnostic_session_hash(&session),
                    run_id: None,
                    phase: "transport_idle".into(),
                    deadline_ms: 120_000,
                    last_transport_ms_ago: 120_000,
                    last_semantic_ms_ago: Some(120_000),
                    semantic_output_started: true,
                    retry_safe: false,
                    last_http_status: None,
                    retry_delay_ms: None,
                },
            )
            .unwrap()
        };
        let old = sample();
        let current = sample();
        record_stream_timeout_diagnostic(old.clone());
        record_stream_timeout_diagnostic(current.clone());
        assert_eq!(
            public_request_diagnostic(&session, &old.error_message()),
            Some(old.clone())
        );
        assert_eq!(
            public_request_diagnostic(&session, &current.error_message()),
            Some(current.clone())
        );
        assert!(public_request_diagnostic("foreign", &current.error_message()).is_none());
        assert!(public_request_diagnostic(
            &session,
            &format!("upstream: {}", current.error_message())
        )
        .is_none());
        assert!(recent_initial_request_diagnostics(&session).is_empty());

        let mut buffer = VecDeque::new();
        for _ in 0..(MAX_RECENT_EVENTS + 1) {
            push_bounded(
                &mut buffer,
                RecentDiagnostic::StreamTimeout(current.clone()),
            );
        }
        assert_eq!(buffer.len(), MAX_RECENT_EVENTS);
        assert!(project_session(
            &buffer,
            &bamboo_domain::llm_diagnostic_session_hash(&session)
        )
        .is_empty());
    }
}
