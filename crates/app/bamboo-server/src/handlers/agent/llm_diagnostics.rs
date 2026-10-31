//! Optional public error evidence. Existing message and event ordering remain
//! authoritative; a diagnostic is an observation and never a recovery capability.

use bamboo_agent_core::AgentEvent;
use bamboo_domain::LlmRequestDiagnostic;

use crate::app_state::AppState;

pub(crate) async fn persisted(state: &AppState, session_id: &str) -> Option<LlmRequestDiagnostic> {
    // Existing canonical Session storage, no second log/receipt store. Failures
    // omit optional evidence without logging raw storage errors or changing a run.
    state
        .storage
        .load_session(session_id)
        .await
        .ok()??
        .last_run_diagnostic()
}

fn diagnostic_for_event(
    event: &AgentEvent,
    session_id: &str,
    persisted: Option<&LlmRequestDiagnostic>,
) -> Option<LlmRequestDiagnostic> {
    if let AgentEvent::Error { message } = event {
        return persisted
            .filter(|d| d.matches_session(session_id) && d.matches_error_message(message))
            .cloned()
            .or_else(|| bamboo_llm::retry::public_request_diagnostic(session_id, message));
    }
    None
}

#[derive(serde::Serialize)]
struct ObservedError<'a> {
    #[serde(flatten)]
    event: &'a AgentEvent,
    diagnostic: LlmRequestDiagnostic,
}

pub(crate) fn event_value(
    event: &AgentEvent,
    session_id: &str,
    persisted: Option<&LlmRequestDiagnostic>,
) -> serde_json::Result<serde_json::Value> {
    match diagnostic_for_event(event, session_id, persisted) {
        Some(diagnostic) => serde_json::to_value(ObservedError { event, diagnostic }),
        None => serde_json::to_value(event),
    }
}

pub(crate) fn event_json(
    event: &AgentEvent,
    session_id: &str,
    persisted: Option<&LlmRequestDiagnostic>,
) -> serde_json::Result<String> {
    // Preserve the exact existing SSE serialization, including key ordering,
    // for every non-error and every legacy error lacking a matching receipt.
    match diagnostic_for_event(event, session_id, persisted) {
        Some(diagnostic) => serde_json::to_string(&ObservedError { event, diagnostic }),
        None => serde_json::to_string(event),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{llm_diagnostic_session_hash, Session};
    use serde_json::json;

    fn sample(session: &Session) -> LlmRequestDiagnostic {
        let id = uuid::Uuid::new_v4();
        LlmRequestDiagnostic::from_json(
            &json!({
                "schema_version":1,"diagnostic_id":id,"request_id":id,"error_id":id,
                "observed_at":chrono::Utc::now(),"provider":"anthropic",
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
    fn cold_error_event_uses_the_same_receipt_as_canonical_last_run() {
        let mut session = Session::new("diagnostic-current", "model");
        let diagnostic = sample(&session);
        session.record_llm_request_diagnostic(diagnostic.clone());
        session.set_last_run_error(format!("LLM error: {}", diagnostic.error_message()));
        let cold: Session =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        let canonical = cold.last_run_diagnostic().unwrap();
        let event = AgentEvent::Error {
            message: cold.last_run_error().unwrap(),
        };
        let wire = event_value(&event, &cold.id, Some(&canonical)).unwrap();
        assert_eq!(wire["type"], "error");
        assert_eq!(wire["message"], cold.last_run_error().unwrap());
        assert_eq!(wire["diagnostic"], serde_json::to_value(canonical).unwrap());
        assert_eq!(
            wire["diagnostic"]["diagnostic_id"],
            diagnostic.diagnostic_id().to_string()
        );
        assert_eq!(wire["diagnostic"]["attempt"], 1);
        assert_eq!(wire["diagnostic"]["attempt_limit"], 4);
        assert_eq!(wire["diagnostic"]["stop_reason"], "time_budget_exhausted");
    }

    #[test]
    fn legacy_foreign_and_forged_error_messages_do_not_acquire_a_diagnostic() {
        let session = Session::new("diagnostic-current", "model");
        let diagnostic = sample(&session);
        let legacy = AgentEvent::Error {
            message: "legacy provider error".into(),
        };
        assert_eq!(
            event_value(&legacy, &session.id, None).unwrap(),
            serde_json::to_value(&legacy).unwrap()
        );
        assert_eq!(
            event_json(&legacy, &session.id, None).unwrap(),
            serde_json::to_string(&legacy).unwrap()
        );
        let genuine = AgentEvent::Error {
            message: diagnostic.error_message(),
        };
        assert!(event_value(&genuine, "other-session", Some(&diagnostic))
            .unwrap()
            .get("diagnostic")
            .is_none());
        let forged = AgentEvent::Error {
            message: format!("PRIVATE_CANARY_raw_body {}", diagnostic.error_message()),
        };
        assert!(event_value(&forged, &session.id, Some(&diagnostic))
            .unwrap()
            .get("diagnostic")
            .is_none());
        let token = AgentEvent::Token {
            content: "unchanged".into(),
        };
        assert_eq!(
            event_value(&token, &session.id, Some(&diagnostic)).unwrap(),
            serde_json::to_value(&token).unwrap()
        );
        assert_eq!(
            event_json(&token, &session.id, Some(&diagnostic)).unwrap(),
            serde_json::to_string(&token).unwrap()
        );
    }

    #[test]
    fn timeout_live_sse_websocket_and_cold_error_share_one_exact_receipt() {
        use bamboo_agent_core::{AgentError, StreamTimeoutError, StreamTimeoutPhase};
        use std::time::Duration;

        let mut session = Session::new(&format!("timeout-live-{}", uuid::Uuid::new_v4()), "model");
        session.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new(
            uuid::Uuid::new_v4().to_string(),
        ));
        let timeout = StreamTimeoutError::new(
            StreamTimeoutPhase::TransportIdle,
            Duration::from_secs(120),
            Some("provider-id".into()),
            Some("gpt-6-astra".into()),
            Duration::from_millis(120_002),
            Some(Duration::from_millis(120_002)),
            true,
        );
        let diagnostic = timeout.diagnostic(&session).unwrap();
        let message = AgentError::StreamTimeout(timeout).to_string();
        let event = AgentEvent::Error {
            message: message.clone(),
        };
        // The live event is emitted before the terminal owner's final save.
        bamboo_llm::retry::record_stream_timeout_diagnostic(diagnostic.clone());
        let websocket = event_value(&event, &session.id, None).unwrap();
        let sse: serde_json::Value =
            serde_json::from_str(&event_json(&event, &session.id, None).unwrap()).unwrap();
        assert_eq!(sse, websocket);
        assert_eq!(
            websocket["diagnostic"],
            serde_json::to_value(&diagnostic).unwrap()
        );
        assert_eq!(websocket["message"], message);

        session.record_llm_request_diagnostic(diagnostic);
        session.set_last_run_error(message.clone());
        let cold: Session =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        let persisted = cold.last_run_diagnostic().unwrap();
        assert_eq!(
            event_value(&event, &cold.id, Some(&persisted)).unwrap(),
            websocket
        );
        assert!(event_value(&event, "foreign", None)
            .unwrap()
            .get("diagnostic")
            .is_none());
        let forged = AgentEvent::Error {
            message: format!("upstream said {message}"),
        };
        assert!(event_value(&forged, &session.id, Some(&persisted))
            .unwrap()
            .get("diagnostic")
            .is_none());
        let old_message = message.split(". Diagnostic ID:").next().unwrap().to_owned();
        let legacy = AgentEvent::Error {
            message: old_message,
        };
        assert!(event_value(&legacy, &session.id, Some(&persisted))
            .unwrap()
            .get("diagnostic")
            .is_none());
    }
}
