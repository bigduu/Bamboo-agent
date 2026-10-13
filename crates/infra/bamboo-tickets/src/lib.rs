//! Trusted-host application service. No model, network or Worker runs under its lock.
//! This store owns contracts, not Session transcripts or Jiandu memory.
mod file_reconciliation;
mod migration;
mod model;
mod query;
mod resolution;
mod service;
pub mod store;
mod workspace_files;

pub use file_reconciliation::*;
pub use migration::*;
pub use model::*;
pub use query::*;
pub use resolution::*;
pub use service::{validate_native_tool_ceiling, Authority, Principal, TicketService};
pub use store::{FaultPoint, Health, PublicationFault};
pub use workspace_files::{FileOperation, FileReply, FILE_BYTES_LIMIT};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scope_denied: {0}")]
    ScopeDenied(String),
    #[error("revision_conflict")]
    RevisionConflict,
    #[error("idempotency_conflict")]
    IdempotencyConflict,
    #[error("invalid_transition: {0}")]
    InvalidTransition(String),
    #[error("dependency_cycle")]
    DependencyCycle,
    #[error("resource_blocked: {0}")]
    ResourceBlocked(String),
    #[error("authority_unavailable: {0}")]
    AuthorityUnavailable(String),
    #[error("resync_required")]
    ResyncRequired,
    #[error("context_budget_exceeded")]
    ContextBudgetExceeded,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::ScopeDenied(_) => 403,
            Self::RevisionConflict | Self::IdempotencyConflict => 409,
            Self::InvalidTransition(_) | Self::DependencyCycle | Self::ContextBudgetExceeded => 422,
            Self::ResourceBlocked(_) => 423,
            Self::ResyncRequired => 410,
            _ => 503,
        }
    }
}

/// Recursively sorted JSON, including nested client-provided values.
pub fn canonical_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    fn sort(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> =
                    map.into_iter().map(|(k, v)| (k, sort(v))).collect();
                serde_json::to_value(sorted).expect("JSON values serialize")
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(sort).collect())
            }
            other => other,
        }
    }
    Ok(serde_json::to_vec(&sort(serde_json::to_value(value)?))?)
}

pub fn content_hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}
