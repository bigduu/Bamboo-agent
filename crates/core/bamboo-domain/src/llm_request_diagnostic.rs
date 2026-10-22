//! A bounded failure receipt, carried by the existing Session checkpoint.
//! These observations never authorize execution, retries or recovery.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::Session;

pub const LAST_RUN_LLM_DIAGNOSTIC_KEY: &str = "runtime.llm_request_diagnostic";
const MAX_DIAGNOSTIC_BYTES: usize = 4096;
const DIAGNOSTIC_RUN_FINGERPRINT_KEY: &str = "runtime.llm_request_diagnostic_run";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlmRequestStopReason {
    AttemptsExhausted,
    TimeBudgetExhausted,
    RetryDelayExceedsBudget,
    NonRetryableTransport,
    UnclassifiedErrorBody,
    CancelledOrDropped,
}

impl LlmRequestStopReason {
    pub fn guidance(self) -> &'static str {
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

// This private wire shape deliberately matches the provider's stop record.
// Validation applies on every deserialize, including cold canonical reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticFields {
    schema_version: u8,
    diagnostic_id: Uuid,
    request_id: Uuid,
    error_id: Option<Uuid>,
    observed_at: DateTime<Utc>,
    provider: String,
    session_hash: Option<String>,
    run_id: Option<Uuid>,
    attempt: u32,
    attempt_limit: u32,
    event: String,
    phase: String,
    failure: String,
    status: Option<u16>,
    attempt_timeout_ms: u64,
    total_budget_ms: u64,
    attempt_elapsed_ms: u64,
    total_elapsed_ms: u64,
    remaining_budget_ms: u64,
    retry_delay_ms: Option<u64>,
    stop_reason: LlmRequestStopReason,
    upstream_outcome: String,
}

/// A validated, content-free stop record. Unknown fields and free-text values
/// fail closed instead of being echoed from editable Session metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct LlmRequestDiagnostic(DiagnosticFields);

impl<'de> Deserialize<'de> for LlmRequestDiagnostic {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = DiagnosticFields::deserialize(deserializer)?;
        if !fields.valid() {
            return Err(serde::de::Error::custom("invalid LLM failure diagnostic"));
        }
        Ok(Self(fields))
    }
}

impl DiagnosticFields {
    fn valid(&self) -> bool {
        self.schema_version == 1
            && self.provider == "anthropic"
            && self.event == "stop"
            && self.request_id == self.diagnostic_id
            && self.error_id == Some(self.diagnostic_id)
            && self.session_hash.as_deref().is_some_and(|hash| {
                hash.len() == 16
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            })
            && (1..=4).contains(&self.attempt_limit)
            && self.attempt <= self.attempt_limit
            && self
                .status
                .is_none_or(|status| (100..=599).contains(&status))
            && self.attempt_timeout_ms <= 60_000
            && self.total_budget_ms <= 60_000
            && self.remaining_budget_ms <= self.total_budget_ms
            && ["send_headers", "error_body", "response_status", "backoff"]
                .contains(&self.phase.as_str())
            && ["unknown", "response_received"].contains(&self.upstream_outcome.as_str())
            && [
                "none",
                "request deadline exceeded",
                "request attempt limit reached",
                "initial response headers timed out",
                "timeout",
                "connect",
                "request",
                "body",
                "decode",
                "status",
                "transport",
                "upstream error body deadline exceeded",
                "upstream error body read failed",
                "connector database is temporarily recovering",
                "temporary upstream failure",
            ]
            .contains(&self.failure.as_str())
    }
}

/// Same domain-separated identity as provider operational logs; never store
/// caller-controlled session text in the diagnostic payload.
pub fn llm_diagnostic_session_hash(session_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-provider-operational-log-v1\0");
    digest.update(session_id.as_bytes());
    hex::encode(&digest.finalize()[..8])
}

impl LlmRequestDiagnostic {
    pub fn from_json(raw: &str) -> Option<Self> {
        (raw.len() <= MAX_DIAGNOSTIC_BYTES)
            .then(|| serde_json::from_str(raw).ok())
            .flatten()
    }

    pub fn diagnostic_id(&self) -> Uuid {
        self.0.diagnostic_id
    }

    pub fn matches_session(&self, session_id: &str) -> bool {
        self.0.session_hash.as_deref() == Some(llm_diagnostic_session_hash(session_id).as_str())
    }

    /// Exact generated error text only. An arbitrary upstream error containing
    /// a copied UUID cannot acquire a receipt through substring matching.
    pub fn matches_error_message(&self, message: &str) -> bool {
        message.strip_prefix("LLM error: ").unwrap_or(message) == self.error_message()
    }

    pub fn error_message(&self) -> String {
        let f = &self.0;
        let status = f
            .status
            .map(|s| format!("HTTP {s}"))
            .unwrap_or_else(|| "no HTTP response".into());
        format!(
            "Anthropic upstream request failed ({status}): {}. {} after {} request(s) (limit {}). Diagnostic ID: {}. Try again later or contact the upstream gateway administrator.",
            f.failure, f.stop_reason.guidance(), f.attempt, f.attempt_limit, f.diagnostic_id,
        )
    }
}

impl Session {
    fn llm_diagnostic_run_fingerprint(&self) -> Option<String> {
        self.agent_runtime_state.as_ref().map(|state| {
            let mut digest = Sha256::new();
            digest.update(b"bamboo-llm-diagnostic-run-v1\0");
            digest.update((state.run_id.len() as u64).to_be_bytes());
            digest.update(state.run_id.as_bytes());
            if let Some(execution) = &state.execution_id {
                digest.update([1]);
                digest.update((execution.len() as u64).to_be_bytes());
                digest.update(execution.as_bytes());
            } else {
                digest.update([0]);
            }
            hex::encode(&digest.finalize()[..16])
        })
    }

    pub(crate) fn clear_llm_request_diagnostic(&mut self) {
        self.metadata.remove(LAST_RUN_LLM_DIAGNOSTIC_KEY);
        self.metadata.remove(DIAGNOSTIC_RUN_FINGERPRINT_KEY);
    }

    fn stored_llm_request_diagnostic(&self) -> Option<LlmRequestDiagnostic> {
        let diagnostic =
            LlmRequestDiagnostic::from_json(self.metadata.get(LAST_RUN_LLM_DIAGNOSTIC_KEY)?)?;
        let run_id = self
            .agent_runtime_state
            .as_ref()
            .and_then(|state| Uuid::parse_str(&state.run_id).ok());
        let expected_run = self.llm_diagnostic_run_fingerprint();
        (diagnostic.matches_session(&self.id)
            && diagnostic.0.run_id == run_id
            && self.metadata.get(DIAGNOSTIC_RUN_FINGERPRINT_KEY) == expected_run.as_ref())
        .then_some(diagnostic)
    }

    /// Capture only the observer for this actual provider call; existing final
    /// Session persistence carries it without a second store or background job.
    pub fn record_llm_request_diagnostic(&mut self, mut diagnostic: LlmRequestDiagnostic) -> bool {
        if !diagnostic.matches_session(&self.id) {
            return false;
        }
        diagnostic.0.run_id = self
            .agent_runtime_state
            .as_ref()
            .and_then(|state| Uuid::parse_str(&state.run_id).ok());
        let Ok(raw) = serde_json::to_string(&diagnostic) else {
            return false;
        };
        if raw.len() > MAX_DIAGNOSTIC_BYTES {
            return false;
        }
        self.metadata
            .insert(LAST_RUN_LLM_DIAGNOSTIC_KEY.into(), raw);
        match self.llm_diagnostic_run_fingerprint() {
            Some(fingerprint) => {
                self.metadata
                    .insert(DIAGNOSTIC_RUN_FINGERPRINT_KEY.into(), fingerprint);
            }
            None => {
                self.metadata.remove(DIAGNOSTIC_RUN_FINGERPRINT_KEY);
            }
        }
        true
    }

    pub(crate) fn retain_llm_request_diagnostic_for_error(&mut self, error: &str) {
        if !self
            .stored_llm_request_diagnostic()
            .is_some_and(|d| d.matches_error_message(error))
        {
            self.clear_llm_request_diagnostic();
        }
    }

    /// Public/cold-read projection requires the same Session, run and exact
    /// canonical error. Old, foreign, malformed or overwritten evidence is absent.
    pub fn last_run_diagnostic(&self) -> Option<LlmRequestDiagnostic> {
        let error = self.last_run_error()?;
        self.stored_llm_request_diagnostic()
            .filter(|d| d.matches_error_message(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(session: &Session) -> LlmRequestDiagnostic {
        let id = Uuid::new_v4();
        LlmRequestDiagnostic::from_json(
            &serde_json::json!({
                "schema_version":1,"diagnostic_id":id,"request_id":id,"error_id":id,
                "observed_at":Utc::now(),"provider":"anthropic",
                "session_hash":llm_diagnostic_session_hash(&session.id),"run_id":null,
                "attempt":1,"attempt_limit":4,"event":"stop","phase":"send_headers",
                "failure":"initial response headers timed out","status":null,
                "attempt_timeout_ms":20000,"total_budget_ms":60000,"attempt_elapsed_ms":20000,
                "total_elapsed_ms":60000,"remaining_budget_ms":0,"retry_delay_ms":null,
                "stop_reason":"time_budget_exhausted","upstream_outcome":"unknown",
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn failure_receipt_survives_cold_session_round_trip_and_is_bound_to_error_and_session() {
        let mut session = Session::new("diagnostic-current", "model");
        let diagnostic = sample(&session);
        assert!(session.record_llm_request_diagnostic(diagnostic.clone()));
        assert!(session.last_run_diagnostic().is_none());
        session.set_last_run_error(format!("LLM error: {}", diagnostic.error_message()));
        let cold: Session =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        assert_eq!(cold.last_run_diagnostic(), Some(diagnostic.clone()));
        let mut foreign = cold.clone();
        foreign.id = "other".into();
        assert!(foreign.last_run_diagnostic().is_none());
        let mut changed = cold.clone();
        changed.set_last_run_error("another provider failure");
        assert!(changed.last_run_diagnostic().is_none());
        assert!(!changed.metadata.contains_key(LAST_RUN_LLM_DIAGNOSTIC_KEY));
        let mut cleared = cold;
        cleared.clear_last_run_error();
        assert!(!cleared.metadata.contains_key(LAST_RUN_LLM_DIAGNOSTIC_KEY));
    }

    #[test]
    fn invalid_free_text_unknown_fields_and_stale_run_fail_closed() {
        let mut session = Session::new("diagnostic-current", "model");
        let diagnostic = sample(&session);
        let value = serde_json::to_value(&diagnostic).unwrap();
        for field in [
            "provider",
            "phase",
            "failure",
            "upstream_outcome",
            "session_hash",
            "future_secret",
        ] {
            let mut invalid = value.clone();
            invalid[field] = serde_json::json!("PRIVATE_CANARY_body_key_path");
            assert!(LlmRequestDiagnostic::from_json(&invalid.to_string()).is_none());
        }
        assert!(!diagnostic.matches_error_message(&format!(
            "arbitrary upstream body: {}",
            diagnostic.error_message()
        )));
        session.record_llm_request_diagnostic(diagnostic.clone());
        session.set_last_run_error(diagnostic.error_message());
        session.agent_runtime_state =
            Some(crate::AgentRuntimeState::new(Uuid::new_v4().to_string()));
        assert!(session.last_run_diagnostic().is_none());
    }

    #[test]
    fn non_uuid_run_and_execution_replacement_cannot_reuse_the_old_receipt() {
        let mut session = Session::new("diagnostic-current", "model");
        let mut runtime = crate::AgentRuntimeState::new("private-non-uuid-run-a");
        runtime.execution_id = Some("private-execution-a".into());
        session.agent_runtime_state = Some(runtime);
        let diagnostic = sample(&session);
        session.record_llm_request_diagnostic(diagnostic.clone());
        session.set_last_run_error(diagnostic.error_message());
        let encoded = serde_json::to_string(&session).unwrap();
        let mut cold: Session = serde_json::from_str(&encoded).unwrap();
        assert!(cold.last_run_diagnostic().is_some());
        let public = serde_json::to_string(&cold.last_run_diagnostic().unwrap()).unwrap();
        assert!(!public.contains("private-non-uuid-run-a"));
        assert!(!public.contains("private-execution-a"));
        cold.agent_runtime_state.as_mut().unwrap().run_id = "private-non-uuid-run-b".into();
        assert!(cold.last_run_diagnostic().is_none());
        let mut cold: Session = serde_json::from_str(&encoded).unwrap();
        cold.agent_runtime_state.as_mut().unwrap().execution_id =
            Some("private-execution-b".into());
        assert!(cold.last_run_diagnostic().is_none());
    }
}
