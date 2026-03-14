use std::path::{Path, PathBuf};

pub(super) fn path_is_within_workspace(target_path: &str, workspace_path: &str) -> bool {
    let target_path = Path::new(target_path);
    let workspace_path = Path::new(workspace_path);

    if !target_path.is_absolute() || !workspace_path.is_absolute() {
        return false;
    }

    let normalized_target = normalize_path_for_comparison(target_path);
    let normalized_workspace = normalize_path_for_comparison(workspace_path);

    normalized_target == normalized_workspace
        || normalized_target.starts_with(&normalized_workspace)
}

fn normalize_path_for_comparison(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }

    if let Some(parent) = path.parent() {
        if let Ok(canonical_parent) = std::fs::canonicalize(parent) {
            if let Some(file_name) = path.file_name() {
                return canonical_parent.join(file_name);
            }
            return canonical_parent;
        }
    }

    path.to_path_buf()
}
