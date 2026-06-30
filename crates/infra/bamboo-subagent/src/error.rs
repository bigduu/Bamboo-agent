//! Error model + atomic write helper shared by `store` and `mailbox`.

use std::path::{Path, PathBuf};

/// Errors from the persistent store / mailbox layer.
///
/// Invariant: authoritative data lives in `session.json` files; index and mailbox files are
/// caches/queues — a corrupt one is recoverable (rebuild / quarantine), never fatal.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("decode {path}: {source}")]
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("corrupt index {path}, rebuild required")]
    CorruptIndex { path: PathBuf },
    #[error("not found: {0}")]
    NotFound(String),
    /// A `ProvisionSpec` violated a cross-field invariant before being shipped
    /// to a worker (e.g. both `mcp` and `mcp_proxy` set).
    #[error("invalid provision spec: {0}")]
    Invalid(String),
    /// A network/transport failure from the HTTP `Discovery` backend
    /// ([`crate::registry_fabric::RegistryFabric`]). The message is scrubbed of
    /// any credential — never format a bearer token into this.
    #[error("registry network error: {0}")]
    Network(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

impl StoreError {
    pub(crate) fn io(path: impl AsRef<Path>, source: std::io::Error) -> Self {
        StoreError::Io {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
    pub(crate) fn decode(path: impl AsRef<Path>, source: serde_json::Error) -> Self {
        StoreError::Decode {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
}

/// Write `bytes` to `path` atomically: a temp file in the same directory + `rename`.
///
/// Readers never observe a half-written file. Parent directories are created as needed.
/// The temp name is hidden (`.`-prefixed) and unique so concurrent writers and directory
/// scanners (e.g. mailbox `drain`) skip it.
pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let dir = path
        .parent()
        .ok_or_else(|| StoreError::NotFound(format!("no parent dir for {}", path.display())))?;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| StoreError::io(dir, e))?;

    let stem = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let tmp = dir.join(format!(".{stem}.tmp.{}", uuid::Uuid::new_v4()));

    {
        let mut f = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.write_all(bytes)
            .await
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.sync_all().await.map_err(|e| StoreError::io(&tmp, e))?;
    }

    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| StoreError::io(path, e))?;
    Ok(())
}
