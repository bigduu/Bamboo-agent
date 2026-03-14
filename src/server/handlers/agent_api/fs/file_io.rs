use std::path::Path;

use crate::server::error::AppError;

/// Reads a UTF-8 text file with consistent error wrapping.
pub(in crate::server::handlers::agent_api) fn read_text_file(
    path: &Path,
    label: &str,
) -> Result<String, AppError> {
    std::fs::read_to_string(path).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to read {}: {}", label, error))
    })
}

/// Writes bytes to a file with consistent error wrapping.
pub(in crate::server::handlers::agent_api) fn write_file(
    path: &Path,
    bytes: impl AsRef<[u8]>,
    label: &str,
) -> Result<(), AppError> {
    std::fs::write(path, bytes).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to write {}: {}", label, error))
    })
}
