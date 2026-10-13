//! Bounded fields for MCP operational logs. Runtime payloads stay unchanged.

use crate::error::McpError;
use sha2::{Digest, Sha256};

/// Hash the entire label, including tool/call names and arbitrary suffixes.
/// Domain separation and length framing follow the existing alias codec;
/// these diagnostic IDs carry no registration or execution authority.
pub(crate) fn diagnostic_id(kind: &'static str, values: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"bamboo-mcp-operational-log-v1\0");
    for value in std::iter::once(kind).chain(values.iter().copied()) {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

/// Classify the existing typed error without formatting its remote payload.
pub(crate) fn error_kind(error: &McpError) -> &'static str {
    match error {
        McpError::Transport(_) => "transport",
        McpError::HttpStatus { .. } => "http_status",
        McpError::HttpProtocol { .. } => "http_protocol",
        McpError::Protocol(_) => "protocol",
        McpError::RemoteProtocol { .. } => "remote_protocol",
        McpError::Connection(_) => "connection",
        McpError::Timeout(_) => "timeout",
        McpError::ToolExecution(_) => "tool_execution",
        McpError::ServerNotFound(_) => "server_not_found",
        McpError::ToolNotFound(_) => "tool_not_found",
        McpError::Serialization(_) => "serialization",
        McpError::InvalidConfig(_) => "invalid_config",
        McpError::ToolRegistration(_) => "tool_registration",
        McpError::Disconnected => "disconnected",
        McpError::AlreadyRunning(_) => "already_running",
        McpError::NotRunning(_) => "not_running",
        McpError::StalePublication { .. } => "stale_publication",
        McpError::ForeignRuntimeAuthority => "foreign_runtime_authority",
        McpError::PublicationIdentityExhausted => "publication_identity_exhausted",
        McpError::RuntimeIdentityExhausted => "runtime_identity_exhausted",
    }
}

/// Measure the existing text field without serializing attached remote data.
pub(crate) fn error_text_len(error: &McpError) -> usize {
    match error {
        McpError::Transport(text)
        | McpError::Protocol(text)
        | McpError::Connection(text)
        | McpError::Timeout(text)
        | McpError::ToolExecution(text)
        | McpError::ServerNotFound(text)
        | McpError::ToolNotFound(text)
        | McpError::Serialization(text)
        | McpError::InvalidConfig(text)
        | McpError::AlreadyRunning(text)
        | McpError::NotRunning(text) => text.len(),
        McpError::HttpStatus { body, .. } => body.len(),
        McpError::HttpProtocol { message, .. } | McpError::RemoteProtocol { message, .. } => {
            message.len()
        }
        _ => 0,
    }
}
