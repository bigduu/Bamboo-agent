//! Privacy-preserving execution/Workspace correlation; never authority.

use bamboo_agent_core::{Session, ToolError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::sync::OnceLock;

/// The salt is process-local and never logged. These identifiers are for log
/// correlation only; they are neither persistent scope pins nor capabilities.
fn opaque(value: &str) -> String {
    static SALT: OnceLock<uuid::Uuid> = OnceLock::new();
    let salt = SALT.get_or_init(uuid::Uuid::new_v4);
    let mut digest = Sha256::new();
    digest.update(salt.as_bytes());
    digest.update(value.as_bytes());
    hex::encode(digest.finalize())[..24].to_owned()
}

pub fn safe_identifier(value: &str) -> String {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_:".contains(&byte))
    {
        value.to_owned()
    } else {
        format!("opaque-{}", opaque(value))
    }
}

pub fn scope_fingerprint(session: &Session) -> String {
    let source = session
        .metadata
        .get(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY);
    // Only these three scope fields enter the digest; never the transcript or
    // the rest of Session metadata, even as a diagnostic serialization.
    let identity = json!([
        session.project_id_meta(),
        session.workspace_path_meta(),
        source
    ]);
    format!("scope-{}", opaque(&identity.to_string()))
}

pub fn fence_fingerprint(fence: &bamboo_domain::ActorActivationFence) -> String {
    // Owner and activation fields remain opaque. Epoch/attempt may be logged
    // separately as numeric observations, not as a write capability.
    format!("fence-{}", opaque(&format!("{fence:?}")))
}

/// Only fixed diagnostic labels enter default logs. Arbitrary Display text can
/// contain a relative path, an escaped JSON value, or a credential without any
/// recognizable delimiter, so a best-effort path scanner cannot make it safe.
/// The opaque fingerprint correlates the original detail without disclosing it.
pub fn redacted(value: &str) -> String {
    format!(
        "{} <detail:{}>",
        reason_tags(value).join(","),
        opaque(value)
    )
}

/// Display-derived hints are deliberately not an authority classification.
/// Typed io_kind/os_error/authority_conflict remain separate fields below.
fn reason_tags(value: &str) -> Vec<&'static str> {
    let lower: String = value
        .chars()
        .take(8192)
        .flat_map(char::to_lowercase)
        .collect();
    let patterns = [
        ("publication", "publication_rejected"),
        ("fence", "fence_detail"),
        ("expired", "expired"),
        ("stale", "stale"),
        ("scope", "scope_detail"),
        ("permission", "permission_detail"),
        ("checkpoint", "checkpoint_detail"),
        ("workspace", "workspace_detail"),
        ("project", "project_detail"),
        ("version", "version_detail"),
        ("not found", "not_found"),
        ("timed out", "timeout"),
        ("cancel", "cancelled"),
    ];
    let mut tags: Vec<_> = patterns
        .into_iter()
        .filter_map(|(needle, tag)| lower.contains(needle).then_some(tag))
        .collect();
    if tags.is_empty() {
        tags.push("unclassified_error");
    }
    tags
}

pub fn error_chain(error: &(dyn Error + 'static)) -> Vec<Value> {
    let mut chain = Vec::new();
    let mut current = Some(error);
    for _ in 0..8 {
        let Some(cause) = current else {
            break;
        };
        let io = cause.downcast_ref::<std::io::Error>();
        chain.push(json!({
            "message": redacted(&cause.to_string()),
            "display_hints": reason_tags(&cause.to_string()),
            "fingerprint": opaque(&cause.to_string()),
            "io_kind": io.map(|error| format!("{:?}", error.kind())),
            "os_error": io.and_then(std::io::Error::raw_os_error),
            "authority_conflict": cause.is::<bamboo_domain::SessionAuthorityConflict>(),
        }));
        current = io
            .and_then(std::io::Error::get_ref)
            .map(|inner| inner as &(dyn Error + 'static))
            .or_else(|| cause.source());
    }
    chain
}

/// Used inside an inherited Workspace span before an existing API flattens a
/// typed filesystem/Project error. It does not create another operation ID.
pub fn record_current_error(phase: &'static str, error: &(dyn Error + 'static)) {
    tracing::warn!(target: "bamboo.workspace_handoff", phase,
        error_chain = %serde_json::Value::Array(error_chain(error)),
        "Workspace operation failed before error conversion");
}

pub struct WorkspaceLogContext<'a> {
    pub session_id: &'a str,
    pub run_id: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub diagnostic_id: Option<&'a str>,
}

/// Preserve the source chain before callers flatten an I/O error into ToolError.
/// A returned number can be shown to the user and searched in server logs.
pub fn record_failure(
    context: WorkspaceLogContext<'_>,
    phase: &'static str,
    error: &(dyn Error + 'static),
) -> String {
    let id = context
        .diagnostic_id
        .map(safe_identifier)
        .unwrap_or_else(|| format!("WSH-{}", uuid::Uuid::new_v4().simple()));
    tracing::warn!(target: "bamboo.workspace_handoff",
        diagnostic_id = %id,
        session_id = %safe_identifier(context.session_id),
        run_id = ?context.run_id.map(safe_identifier),
        request_id = ?context.request_id.map(safe_identifier),
        phase,
        error_chain = %serde_json::Value::Array(error_chain(error)),
        "Execution scope operation rejected");
    id
}

pub fn annotated_error(error: ToolError, diagnostic_id: &str) -> ToolError {
    let annotate = |message: String| {
        if message.contains(diagnostic_id) {
            message
        } else {
            format!("{message} [{diagnostic_id}]")
        }
    };
    match error {
        ToolError::Execution(message) => ToolError::Execution(annotate(message)),
        ToolError::InvalidArguments(message) => ToolError::InvalidArguments(annotate(message)),
        ToolError::NotFound(message) => ToolError::NotFound(annotate(message)),
    }
}
