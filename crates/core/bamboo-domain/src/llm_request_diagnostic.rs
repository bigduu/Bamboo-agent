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
    RetryDelayTooLarge,
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
            Self::RetryDelayTooLarge => "Required retry delay is too large to schedule safely",
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
struct InitialRequestDiagnosticFields {
    schema_version: u8,
    diagnostic_id: Uuid,
    request_id: Uuid,
    error_id: Option<Uuid>,
    observed_at: DateTime<Utc>,
    provider: String,
    session_hash: Option<String>,
    run_id: Option<Uuid>,
    attempt: u32,
    attempt_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_mode: Option<String>,
    event: String,
    phase: String,
    failure: String,
    status: Option<u16>,
    attempt_timeout_ms: u64,
    total_budget_ms: Option<u64>,
    attempt_elapsed_ms: u64,
    total_elapsed_ms: u64,
    remaining_budget_ms: Option<u64>,
    retry_delay_ms: Option<u64>,
    stop_reason: LlmRequestStopReason,
    upstream_outcome: String,
}

/// Public watchdog evidence only. The validated wrapper below is required
/// before these fields may be persisted or attached to a client error event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTimeoutDiagnostic {
    pub schema_version: u8,
    pub kind: String,
    pub diagnostic_id: Uuid,
    pub error_id: Uuid,
    pub observed_at: DateTime<Utc>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub session_hash: String,
    pub run_id: Option<Uuid>,
    pub phase: String,
    pub deadline_ms: u64,
    pub last_transport_ms_ago: u64,
    pub last_semantic_ms_ago: Option<u64>,
    pub semantic_output_started: bool,
    pub retry_safe: bool,
    pub last_http_status: Option<u16>,
    pub retry_delay_ms: Option<u64>,
}

impl StreamTimeoutDiagnostic {
    /// Identifiers are bounded labels, never URLs or credential-like values.
    /// This same check applies before formatting the timeout message and on
    /// cold metadata reads, so redaction cannot break exact error binding.
    pub fn valid_identifier(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 120
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:/@".contains(&b))
            && !value.contains("://")
            && !value.to_ascii_lowercase().starts_with("sk-")
            && !value.to_ascii_lowercase().starts_with("bearer")
    }

    fn valid(&self) -> bool {
        // JavaScript consumers must not round a correlation-relevant duration.
        const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
        let has_semantic = self.last_semantic_ms_ago.is_some();
        self.schema_version == 1
            && self.kind == "stream_timeout"
            && self.error_id == self.diagnostic_id
            && valid_session_hash(&self.session_hash)
            && self.provider.as_deref().is_none_or(Self::valid_identifier)
            && self.model.as_deref().is_none_or(Self::valid_identifier)
            && (1_000..=86_400_000).contains(&self.deadline_ms)
            && self.last_transport_ms_ago <= MAX_SAFE_INTEGER
            && self.last_semantic_ms_ago.is_none_or(|elapsed| {
                elapsed <= MAX_SAFE_INTEGER && self.last_transport_ms_ago <= elapsed
            })
            && self.semantic_output_started == has_semantic
            && (!self.retry_safe || !has_semantic)
            && match self.phase.as_str() {
                "bootstrap" | "first_semantic" => !has_semantic,
                "transport_idle" => true,
                "semantic_idle" => has_semantic,
                _ => false,
            }
            && match (self.last_http_status, self.retry_delay_ms) {
                (None, None) => true,
                (Some(status), Some(delay)) => {
                    self.phase == "bootstrap"
                        && (100..=599).contains(&status)
                        && delay <= MAX_SAFE_INTEGER
                }
                _ => false,
            }
    }

    fn error_message(&self) -> String {
        let last_semantic = self
            .last_semantic_ms_ago
            .map(|elapsed| format!("{elapsed}ms"))
            .unwrap_or_else(|| "never".into());
        let mut message = format!(
            "Stream timed out: phase={}, deadline_ms={}, provider={}, model={}, last_transport_ms_ago={}, \
             last_semantic_ms_ago={}, semantic_output_started={}, retry_safe={}",
            self.phase,
            self.deadline_ms,
            self.provider.as_deref().unwrap_or("unknown"),
            self.model.as_deref().unwrap_or("unknown"),
            self.last_transport_ms_ago,
            last_semantic,
            self.semantic_output_started,
            self.retry_safe,
        );
        if let (Some(status), Some(delay)) = (self.last_http_status, self.retry_delay_ms) {
            use std::fmt::Write;
            let _ = write!(
                message,
                ", last_http_status={status}, retry_delay_ms={delay}"
            );
        }
        format!("{message}. Diagnostic ID: {}.", self.diagnostic_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum DiagnosticFields {
    InitialRequest(InitialRequestDiagnosticFields),
    StreamTimeout(StreamTimeoutDiagnostic),
}

impl DiagnosticFields {
    fn valid(&self) -> bool {
        match self {
            Self::InitialRequest(fields) => fields.valid(),
            Self::StreamTimeout(fields) => fields.valid(),
        }
    }

    fn run_id(&self) -> Option<Uuid> {
        match self {
            Self::InitialRequest(fields) => fields.run_id,
            Self::StreamTimeout(fields) => fields.run_id,
        }
    }

    fn set_run_id(&mut self, run_id: Option<Uuid>) {
        match self {
            Self::InitialRequest(fields) => fields.run_id = run_id,
            Self::StreamTimeout(fields) => fields.run_id = run_id,
        }
    }
}

fn valid_session_hash(hash: &str) -> bool {
    hash.len() == 16
        && hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
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

impl InitialRequestDiagnosticFields {
    fn valid(&self) -> bool {
        self.schema_version == 1
            && self.provider == "anthropic"
            && self.event == "stop"
            && self.request_id == self.diagnostic_id
            && self.error_id == Some(self.diagnostic_id)
            && self.session_hash.as_deref().is_some_and(valid_session_hash)
            && match self.retry_mode.as_deref() {
                None => {
                    self.attempt_limit
                        .is_some_and(|limit| limit > 0 && self.attempt <= limit)
                        && self
                            .total_budget_ms
                            .is_some_and(|budget| budget <= 3_600_000)
                        && self
                            .remaining_budget_ms
                            .zip(self.total_budget_ms)
                            .is_some_and(|(remaining, total)| remaining <= total)
                }
                Some("until_cancelled") => {
                    self.attempt_limit.is_none()
                        && self.total_budget_ms.is_none()
                        && self.remaining_budget_ms.is_none()
                        && !matches!(
                            self.stop_reason,
                            LlmRequestStopReason::AttemptsExhausted
                                | LlmRequestStopReason::TimeBudgetExhausted
                                | LlmRequestStopReason::RetryDelayExceedsBudget
                        )
                }
                _ => false,
            }
            && self
                .status
                .is_none_or(|status| (100..=599).contains(&status))
            && self.attempt_timeout_ms <= 60_000
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
    pub fn from_stream_timeout(fields: StreamTimeoutDiagnostic) -> Option<Self> {
        fields
            .valid()
            .then_some(Self(DiagnosticFields::StreamTimeout(fields)))
    }

    pub fn from_json(raw: &str) -> Option<Self> {
        (raw.len() <= MAX_DIAGNOSTIC_BYTES)
            .then(|| serde_json::from_str(raw).ok())
            .flatten()
    }

    pub fn diagnostic_id(&self) -> Uuid {
        match &self.0 {
            DiagnosticFields::InitialRequest(fields) => fields.diagnostic_id,
            DiagnosticFields::StreamTimeout(fields) => fields.diagnostic_id,
        }
    }

    pub fn matches_session(&self, session_id: &str) -> bool {
        let hash = llm_diagnostic_session_hash(session_id);
        match &self.0 {
            DiagnosticFields::InitialRequest(fields) => {
                fields.session_hash.as_deref() == Some(hash.as_str())
            }
            DiagnosticFields::StreamTimeout(fields) => fields.session_hash == hash,
        }
    }

    /// Exact generated error text only. An arbitrary upstream error containing
    /// a copied UUID cannot acquire a receipt through substring matching.
    pub fn matches_error_message(&self, message: &str) -> bool {
        let message = match &self.0 {
            DiagnosticFields::InitialRequest(_) => {
                message.strip_prefix("LLM error: ").unwrap_or(message)
            }
            DiagnosticFields::StreamTimeout(_) => message,
        };
        message == self.error_message()
    }

    pub fn error_message(&self) -> String {
        let f = match &self.0 {
            DiagnosticFields::InitialRequest(fields) => fields,
            DiagnosticFields::StreamTimeout(fields) => return fields.error_message(),
        };
        let status = f
            .status
            .map(|s| format!("HTTP {s}"))
            .unwrap_or_else(|| "no HTTP response".into());
        let limit = f
            .attempt_limit
            .map(|n| format!(" (limit {n})"))
            .unwrap_or_default();
        format!(
            "Anthropic upstream request failed ({status}): {}. {} after {} request(s){limit}. Diagnostic ID: {}. Try again later or contact the upstream gateway administrator.",
            f.failure, f.stop_reason.guidance(), f.attempt, f.diagnostic_id,
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
            && diagnostic.0.run_id() == run_id
            && self.metadata.get(DIAGNOSTIC_RUN_FINGERPRINT_KEY) == expected_run.as_ref())
        .then_some(diagnostic)
    }

    /// Capture only the observer for this actual provider call; existing final
    /// Session persistence carries it without a second store or background job.
    pub fn record_llm_request_diagnostic(&mut self, mut diagnostic: LlmRequestDiagnostic) -> bool {
        if !diagnostic.matches_session(&self.id) {
            return false;
        }
        diagnostic.0.set_run_id(
            self.agent_runtime_state
                .as_ref()
                .and_then(|state| Uuid::parse_str(&state.run_id).ok()),
        );
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

    fn timeout_sample(session: &Session) -> LlmRequestDiagnostic {
        let id = Uuid::new_v4();
        LlmRequestDiagnostic::from_stream_timeout(StreamTimeoutDiagnostic {
            schema_version: 1,
            kind: "stream_timeout".into(),
            diagnostic_id: id,
            error_id: id,
            observed_at: Utc::now(),
            provider: Some("a2a3cebf-8abc-45cb-8407-66c90fd9a978".into()),
            model: Some("gpt-6-astra".into()),
            session_hash: llm_diagnostic_session_hash(&session.id),
            run_id: None,
            phase: "transport_idle".into(),
            deadline_ms: 120_000,
            last_transport_ms_ago: 120_002,
            last_semantic_ms_ago: Some(120_002),
            semantic_output_started: true,
            retry_safe: false,
            last_http_status: None,
            retry_delay_ms: None,
        })
        .unwrap()
    }

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
    fn configured_and_continuous_receipts_keep_distinct_validated_limits() {
        let session = Session::new("retry-diagnostic", "model");
        let mut bounded = serde_json::to_value(sample(&session)).unwrap();
        bounded["attempt_limit"] = 12.into();
        bounded["total_budget_ms"] = 300_000.into();
        assert!(LlmRequestDiagnostic::from_json(&bounded.to_string()).is_some());
        let mut continuous = bounded.clone();
        continuous["retry_mode"] = "until_cancelled".into();
        for field in ["attempt_limit", "total_budget_ms", "remaining_budget_ms"] {
            continuous[field] = serde_json::Value::Null;
        }
        continuous["stop_reason"] = "cancelled_or_dropped".into();
        let diagnostic = LlmRequestDiagnostic::from_json(&continuous.to_string()).unwrap();
        assert!(diagnostic.matches_error_message(&diagnostic.error_message()));
        for field in ["attempt_limit", "total_budget_ms", "remaining_budget_ms"] {
            let mut invalid = continuous.clone();
            invalid[field] = 1.into();
            assert!(LlmRequestDiagnostic::from_json(&invalid.to_string()).is_none());
        }
        continuous["retry_mode"] = "unrecognized".into();
        assert!(LlmRequestDiagnostic::from_json(&continuous.to_string()).is_none());
        bounded["total_budget_ms"] = 3_600_001.into();
        assert!(LlmRequestDiagnostic::from_json(&bounded.to_string()).is_none());
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

    #[test]
    fn stream_timeout_receipt_survives_cold_read_only_for_its_exact_run_and_error() {
        let mut session = Session::new("timeout-session-private", "model");
        let mut runtime = crate::AgentRuntimeState::new(Uuid::new_v4().to_string());
        runtime.execution_id = Some("private-execution".into());
        session.agent_runtime_state = Some(runtime);
        let receipt = timeout_sample(&session);
        assert!(session.record_llm_request_diagnostic(receipt.clone()));
        assert!(session.last_run_diagnostic().is_none());
        session.set_last_run_error(receipt.error_message());
        let encoded = serde_json::to_string(&session).unwrap();
        let cold: Session = serde_json::from_str(&encoded).unwrap();
        let actual = cold.last_run_diagnostic().unwrap();
        assert_eq!(actual.diagnostic_id(), receipt.diagnostic_id());
        assert_eq!(actual.error_message(), receipt.error_message());
        let public = serde_json::to_value(&actual).unwrap();
        assert_eq!(
            public["run_id"],
            session.agent_runtime_state.as_ref().unwrap().run_id
        );
        assert!(!public.to_string().contains("timeout-session-private"));
        assert!(!public.to_string().contains("private-execution"));

        let mut wrong_session = cold.clone();
        wrong_session.id = "other-session".into();
        assert!(wrong_session.last_run_diagnostic().is_none());
        let mut wrong_run = cold.clone();
        wrong_run.agent_runtime_state.as_mut().unwrap().run_id = Uuid::new_v4().to_string();
        assert!(wrong_run.last_run_diagnostic().is_none());
        let mut wrong_execution = cold.clone();
        wrong_execution
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .execution_id = Some("next-execution".into());
        assert!(wrong_execution.last_run_diagnostic().is_none());
        let mut repeated_timeout = cold;
        repeated_timeout.set_last_run_error(timeout_sample(&session).error_message());
        assert!(repeated_timeout.last_run_diagnostic().is_none());
        assert!(!repeated_timeout
            .metadata
            .contains_key(LAST_RUN_LLM_DIAGNOSTIC_KEY));
    }

    #[test]
    fn timeout_receipt_rejects_inconsistent_observations_unknown_keys_and_unsafe_labels() {
        let session = Session::new("session", "model");
        let value = serde_json::to_value(timeout_sample(&session)).unwrap();
        for (field, invalid) in [
            ("schema_version", serde_json::json!(2)),
            ("kind", serde_json::json!("another_timeout")),
            ("error_id", serde_json::json!(Uuid::new_v4())),
            ("phase", serde_json::json!("first_semantic")),
            ("deadline_ms", serde_json::json!(0)),
            ("deadline_ms", serde_json::json!(86_400_001_u64)),
            ("last_transport_ms_ago", serde_json::json!(120_003)),
            (
                "last_semantic_ms_ago",
                serde_json::json!(9_007_199_254_740_992_u64),
            ),
            ("semantic_output_started", serde_json::json!(false)),
            ("retry_safe", serde_json::json!(true)),
            ("last_http_status", serde_json::json!(429)),
            ("retry_delay_ms", serde_json::json!(5_000)),
            (
                "provider",
                serde_json::json!("https://private.invalid/token"),
            ),
            ("model", serde_json::json!("sk-private")),
            ("model", serde_json::json!("Bearer_private")),
            ("future_secret", serde_json::json!("PRIVATE_CANARY")),
        ] {
            let mut forged = value.clone();
            forged[field] = invalid;
            assert!(
                LlmRequestDiagnostic::from_json(&forged.to_string()).is_none(),
                "accepted invalid {field}"
            );
        }
        let receipt = timeout_sample(&session);
        assert!(
            !receipt.matches_error_message(&format!("upstream said {}", receipt.error_message()))
        );
        assert!(!receipt.matches_error_message(&format!("LLM error: {}", receipt.error_message())));
    }
}
