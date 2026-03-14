use std::path::PathBuf;

use crate::server::error::AppError;

/// Gets the Claude configuration directory (~/.claude)
///
/// Creates the directory if it doesn't exist.
pub(in crate::server::handlers::agent_api) fn get_claude_dir() -> Result<PathBuf, AppError> {
    let dir = dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Could not find home directory")))?
        .join(".claude");

    if !dir.exists() {
        std::fs::create_dir_all(&dir).map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "Could not create ~/.claude directory: {}",
                error
            ))
        })?;
    }

    dir.canonicalize().map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "Could not canonicalize ~/.claude directory: {}",
            error
        ))
    })
}

/// Gets a path under the user home Claude directory (~/.claude/{file_name}).
pub(in crate::server::handlers::agent_api) fn claude_home_file(
    file_name: &str,
) -> Result<PathBuf, AppError> {
    Ok(dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Home directory not found")))?
        .join(".claude")
        .join(file_name))
}
