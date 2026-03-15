use std::path::PathBuf;

use crate::server::error::AppError;

pub(super) fn validate_project_path(project_path: &str) -> Result<PathBuf, AppError> {
    let path = PathBuf::from(project_path.trim());
    if !path.is_dir() {
        return Err(AppError::BadRequest(format!(
            "project_path is not a directory: {}",
            path.display()
        )));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::validate_project_path;

    #[test]
    fn validate_project_path_accepts_existing_directory() {
        let dir = tempdir().expect("temp dir");
        let path = validate_project_path(dir.path().to_str().expect("utf8 path"))
            .expect("existing directory should pass");
        assert!(path.is_dir());
    }

    #[test]
    fn validate_project_path_rejects_missing_directory() {
        let result = validate_project_path("/path/that/does/not/exist");
        assert!(result.is_err());
    }
}
