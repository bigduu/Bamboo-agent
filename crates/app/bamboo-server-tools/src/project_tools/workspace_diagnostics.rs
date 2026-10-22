//! Correlation for Workspace transactions, never an authorization receipt.
//! Paths and arbitrary quoted payloads are not emitted to the default log.

use std::error::Error;
use std::sync::Mutex;
use bamboo_agent_core::{Session, ToolCtx, ToolError, ToolOutcome};
use serde_json::{json, Value};

pub use bamboo_engine::execution::workspace_diagnostics::{
    annotated_error, error_chain, fence_fingerprint, record_current_error, record_failure, redacted,
    safe_identifier, scope_fingerprint, WorkspaceLogContext,
};

pub(super) struct WorkspaceDiagnostic {
    id: String,
    session_id: String,
    request_id: String,
    run_id: Option<String>,
    phase: Mutex<&'static str>,
}

impl WorkspaceDiagnostic {
    pub(super) fn new(context: &ToolCtx) -> Self {
        Self {
            id: format!("WSH-{}", uuid::Uuid::new_v4().simple()),
            session_id: context.session_id().map(safe_identifier).unwrap_or_else(|| "none".into()),
            request_id: safe_identifier(&context.tool_call_id),
            run_id: bamboo_agent_core::tools::context::root_actor_tool_writer()
                .flatten().map(|owner| safe_identifier(&owner.fence.run_id)),
            phase: Mutex::new("preflight"),
        }
    }

    pub(super) fn id(&self) -> &str { &self.id }

    pub(super) fn span(&self) -> tracing::Span {
        tracing::info_span!(target: "bamboo.workspace_handoff", "workspace_change",
            diagnostic_id = %self.id, session_id = %self.session_id,
            request_id = %self.request_id, run_id = ?self.run_id,
            phase = tracing::field::Empty,
            old_scope = tracing::field::Empty, new_scope = tracing::field::Empty)
    }

    pub(super) fn stage(&self, phase: &'static str) {
        if let Ok(mut current) = self.phase.lock() { *current = phase; }
        tracing::Span::current().record("phase", phase);
        tracing::debug!(target: "bamboo.workspace_handoff", diagnostic_id = %self.id,
            phase, "Workspace transition stage");
    }

    pub(super) fn scopes(&self, before: &Session, after: &Session) {
        let old_scope = scope_fingerprint(before);
        let new_scope = scope_fingerprint(after);
        tracing::Span::current().record("old_scope", old_scope.as_str());
        tracing::Span::current().record("new_scope", new_scope.as_str());
        tracing::debug!(target: "bamboo.workspace_handoff", diagnostic_id = %self.id,
            old_scope, new_scope, old_metadata_version = before.metadata_version,
            new_metadata_version = after.metadata_version, "Workspace scope candidate");
    }

    pub(super) fn failure(&self, error: &(dyn Error + 'static)) {
        let phase = self.phase.lock().map(|phase| *phase).unwrap_or("unknown");
        record_failure(WorkspaceLogContext {
            session_id: &self.session_id, run_id: self.run_id.as_deref(),
            request_id: Some(&self.request_id), diagnostic_id: Some(&self.id),
        }, phase, error);
    }

    pub(super) fn finish(&self, outcome: Result<ToolOutcome, ToolError>) -> Result<ToolOutcome, ToolError> {
        match outcome {
            Err(error) => {
                self.failure(&error);
                Err(annotated_error(error, &self.id))
            }
            Ok(ToolOutcome::Completed(mut result)) if !result.success => {
                if let Ok(Value::Object(mut value)) = serde_json::from_str::<Value>(&result.result) {
                    let code = value.get("code").and_then(Value::as_str).unwrap_or("workspace_rejected");
                    tracing::warn!(target: "bamboo.workspace_handoff", diagnostic_id = %self.id,
                        error_code = %safe_identifier(code), "Workspace change refused before completion");
                    value.insert("diagnostic_id".into(), json!(self.id));
                    result.result = Value::Object(value).to_string();
                }
                Ok(ToolOutcome::Completed(result))
            }
            Ok(outcome) => {
                tracing::info!(target: "bamboo.workspace_handoff", diagnostic_id = %self.id,
                    phase = "completed", "Workspace transaction completed");
                Ok(outcome)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_chain_keeps_cause_and_redacts_private_locations_and_credentials() {
        let error = std::io::Error::new(std::io::ErrorKind::WouldBlock,
            bamboo_domain::SessionAuthorityConflict(
                "publication rejected at '/Users/private-company/work tree': original fence expired".into()));
        let chain = error_chain(&error);
        let serialized = serde_json::to_string(&chain).unwrap();
        assert!(!serialized.contains("private-company"));
        assert!(!serialized.contains("work tree"));
        assert!(serialized.contains("publication_rejected"));
        assert!(serialized.contains("fence_detail"));
        assert!(serialized.contains("expired"));
        assert_eq!(chain[0]["io_kind"], "WouldBlock");
        assert!(chain.iter().any(|cause| cause["authority_conflict"] == true));
        assert!(!redacted("probe /Users/private-company/work tree/config.json (os error 2)")
            .contains("private-company"));
        assert!(!redacted("failed: access_token=synthetic-secret").contains("synthetic-secret"));
        assert!(!redacted("read C:\\private-company\\work tree\\state").contains("private-company"));
        let project_error = json!({
            "code": "project_path_unavailable",
            "project_path": "/private/base/\"customer-name\"/gone",
            "project_id": "private-project",
        }).to_string();
        for detail in [project_error.as_str(),
            "invalid Project identity: relative-private-name",
            "failed \\\\private-server\\private-share\\state",
            "read '/private/a'b/private-tail'", "unterminated \"private-tail",
            "failed access_token=synthetic-secret", "opaque-private-payload"] {
            let logged = redacted(detail);
            assert!(!logged.contains("customer-name"));
            assert!(!logged.contains("private"));
            assert!(!logged.contains("synthetic-secret"));
            assert!(!logged.contains("opaque-private-payload"));
            assert!(logged.contains("<detail:"));
        }
    }

    #[test]
    fn correlation_is_opaque_stable_and_does_not_reclassify_errors() {
        let mut session = Session::new("diagnostic-test", "test");
        session.set_workspace_path_meta("/private/synthetic workspace");
        let id = scope_fingerprint(&session);
        assert_eq!(id, scope_fingerprint(&session));
        assert!(!id.contains("synthetic"));
        let mut other = session.clone();
        other.set_workspace_path_meta("/private/other workspace");
        assert_ne!(id, scope_fingerprint(&other));
        assert!(safe_identifier("s/run token").starts_with("opaque-"));
        assert!(matches!(annotated_error(ToolError::InvalidArguments("bad input".into()), "WSH-test"),
            ToolError::InvalidArguments(message) if message.contains("[WSH-test]")));
    }
}
