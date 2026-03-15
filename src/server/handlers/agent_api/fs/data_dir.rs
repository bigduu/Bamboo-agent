use std::path::PathBuf;

use crate::server::error::AppError;

/// Gets the Bamboo data directory (for agent API project/session storage).
///
/// Creates the directory if it doesn't exist.
pub(in crate::server::handlers::agent_api) fn get_data_dir() -> Result<PathBuf, AppError> {
    let dir = crate::core::paths::bamboo_dir();

    if !dir.exists() {
        std::fs::create_dir_all(&dir).map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "Could not create bamboo data directory: {}",
                error
            ))
        })?;
    }

    dir.canonicalize().map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "Could not canonicalize bamboo data directory: {}",
            error
        ))
    })
}

/// Gets a path under the Bamboo data directory ({bamboo_dir}/{file_name}).
pub(in crate::server::handlers::agent_api) fn data_home_file(
    file_name: &str,
) -> Result<PathBuf, AppError> {
    Ok(crate::core::paths::bamboo_dir().join(file_name))
}
