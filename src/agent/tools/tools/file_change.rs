use crate::agent::core::tools::ToolError;
use chrono::Utc;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

const CONTEXT_LINES: usize = 3;
const MAX_DIFF_LINES: usize = 400;
const MAX_DIFF_CHARS: usize = 64_000;

pub async fn read_existing_bytes(path: &Path) -> Result<Option<Vec<u8>>, ToolError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ToolError::Execution(format!(
            "Failed to read file before checkpoint: {error}"
        ))),
    }
}

pub fn ensure_no_symlink_components(path: &Path) -> Result<(), ToolError> {
    let mut current = PathBuf::new();
    let mut normal_component_depth = 0usize;

    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, std::path::Component::Normal(_)) {
            normal_component_depth += 1;
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    // macOS commonly exposes canonical paths through top-level aliases
                    // such as `/tmp` -> `/private/tmp` and `/var` -> `/private/var`.
                    // Allow only that first segment alias; reject deeper symlink hops.
                    if normal_component_depth == 1 {
                        continue;
                    }
                    return Err(ToolError::Execution(format!(
                        "Refusing to write through symlinked path component: {}",
                        crate::core::paths::path_to_display_string(&current)
                    )));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(ToolError::Execution(format!(
                    "Failed to inspect path component {}: {}",
                    crate::core::paths::path_to_display_string(&current),
                    error
                )));
            }
        }
    }

    Ok(())
}

pub async fn atomic_write_text(path: &Path, content: &str) -> Result<(), ToolError> {
    if !path.is_absolute() {
        return Err(ToolError::InvalidArguments(
            "file path must be absolute".to_string(),
        ));
    }

    ensure_no_symlink_components(path)?;

    let parent = path.parent().ok_or_else(|| {
        ToolError::Execution(format!(
            "Target path has no parent directory: {}",
            crate::core::paths::path_to_display_string(path)
        ))
    })?;

    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        ToolError::Execution(format!("Failed to create parent directory: {error}"))
    })?;

    ensure_no_symlink_components(path)?;

    let tmp_path = parent.join(format!(
        ".{}.tmp-{}",
        sanitize_file_name(path),
        uuid::Uuid::new_v4()
    ));
    let content_bytes = content.as_bytes();

    let write_result = async {
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("Failed to create temporary file: {error}"))
            })?;

        tmp_file
            .write_all(content_bytes)
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to write temporary file: {error}")))?;
        tmp_file
            .flush()
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to flush temporary file: {error}")))?;
        tmp_file
            .sync_all()
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to sync temporary file: {error}")))?;

        if let Ok(metadata) = tokio::fs::symlink_metadata(path).await {
            if metadata.file_type().is_symlink() {
                return Err(ToolError::Execution(format!(
                    "Refusing to overwrite symlink target: {}",
                    crate::core::paths::path_to_display_string(path)
                )));
            }
        }

        match tokio::fs::rename(&tmp_path, path).await {
            Ok(()) => Ok(()),
            #[cfg(windows)]
            Err(error) if std::fs::metadata(path).is_ok() => {
                tokio::fs::remove_file(path).await.map_err(|remove_error| {
                    ToolError::Execution(format!(
                        "Failed to replace target file (rename failed with {error}; remove failed with {remove_error})"
                    ))
                })?;
                tokio::fs::rename(&tmp_path, path).await.map_err(|rename_error| {
                    ToolError::Execution(format!(
                        "Failed to replace target file after remove: {rename_error}"
                    ))
                })
            }
            Err(error) => Err(ToolError::Execution(format!(
                "Failed to atomically replace target file: {error}"
            ))),
        }
    }
    .await;

    if write_result.is_err() {
        let _ = tokio::fs::remove_file(&tmp_path).await;
    }

    write_result
}

pub async fn create_checkpoint(
    path: &Path,
    previous_bytes: Option<&[u8]>,
) -> Result<Value, ToolError> {
    let Some(bytes) = previous_bytes else {
        return Ok(json!({
            "created": false,
            "reason": "file_did_not_exist"
        }));
    };

    let now = Utc::now();
    let date_dir = now.format("%Y%m%d").to_string();
    let timestamp = now.format("%Y%m%dT%H%M%S%.3fZ").to_string();
    let checkpoint_id = uuid::Uuid::new_v4().to_string();
    let file_name = sanitize_file_name(path);
    let checkpoint_root = crate::core::paths::bamboo_dir()
        .join("checkpoints")
        .join(date_dir);

    tokio::fs::create_dir_all(&checkpoint_root)
        .await
        .map_err(|error| {
            ToolError::Execution(format!("Failed to create checkpoint directory: {error}"))
        })?;

    let checkpoint_path = checkpoint_root.join(format!(
        "{timestamp}-{checkpoint_id}-{file_name}.checkpoint"
    ));
    tokio::fs::write(&checkpoint_path, bytes)
        .await
        .map_err(|error| {
            ToolError::Execution(format!("Failed to write checkpoint file: {error}"))
        })?;

    Ok(json!({
        "created": true,
        "id": checkpoint_id,
        "path": crate::core::paths::path_to_display_string(&checkpoint_path),
        "size_bytes": bytes.len()
    }))
}

pub fn bytes_to_lossy_text(previous_bytes: Option<&[u8]>) -> String {
    previous_bytes
        .map(|bytes| String::from_utf8_lossy(bytes).to_string())
        .unwrap_or_default()
}

pub fn workspace_for_file(file_path: &Path) -> String {
    let workspace = file_path.parent().unwrap_or(file_path);
    crate::core::paths::path_to_display_string(workspace)
}

pub fn build_file_change_payload(
    operation: &str,
    file_path: &Path,
    message: String,
    checkpoint: Value,
    previous_text: &str,
    updated_text: &str,
) -> String {
    let file_path_display = crate::core::paths::path_to_display_string(file_path);
    json!({
        "operation": operation,
        "message": message,
        "file_path": file_path_display,
        "workspace": workspace_for_file(file_path),
        "checkpoint": checkpoint,
        "diff": build_unified_diff(file_path, previous_text, updated_text)
    })
    .to_string()
}

fn build_unified_diff(file_path: &Path, previous_text: &str, updated_text: &str) -> Value {
    let old_lines = lines(previous_text);
    let new_lines = lines(updated_text);
    let changed = previous_text != updated_text;

    let file_label = crate::core::paths::path_to_display_string(file_path);
    let mut diff_lines = vec![format!("--- a/{file_label}"), format!("+++ b/{file_label}")];

    let mut added_lines = 0usize;
    let mut removed_lines = 0usize;

    if changed {
        let mut prefix = 0usize;
        let shared_len = old_lines.len().min(new_lines.len());
        while prefix < shared_len && old_lines[prefix] == new_lines[prefix] {
            prefix += 1;
        }

        let mut old_suffix = old_lines.len();
        let mut new_suffix = new_lines.len();
        while old_suffix > prefix
            && new_suffix > prefix
            && old_lines[old_suffix - 1] == new_lines[new_suffix - 1]
        {
            old_suffix -= 1;
            new_suffix -= 1;
        }

        removed_lines = old_suffix.saturating_sub(prefix);
        added_lines = new_suffix.saturating_sub(prefix);

        let context_before = prefix.min(CONTEXT_LINES);
        let shared_trailing = (old_lines.len().saturating_sub(old_suffix))
            .min(new_lines.len().saturating_sub(new_suffix));
        let context_after = shared_trailing.min(CONTEXT_LINES);

        let old_start = prefix.saturating_sub(context_before);
        let new_start = prefix.saturating_sub(context_before);
        let old_count = context_before + removed_lines + context_after;
        let new_count = context_before + added_lines + context_after;

        diff_lines.push(format!(
            "@@ -{},{} +{},{} @@",
            old_start + 1,
            old_count,
            new_start + 1,
            new_count
        ));

        for line in &old_lines[old_start..prefix] {
            diff_lines.push(format!(" {line}"));
        }
        for line in &old_lines[prefix..old_suffix] {
            diff_lines.push(format!("-{line}"));
        }
        for line in &new_lines[prefix..new_suffix] {
            diff_lines.push(format!("+{line}"));
        }
        for line in &old_lines[old_suffix..old_suffix + context_after] {
            diff_lines.push(format!(" {line}"));
        }
        if previous_text.ends_with('\n') != updated_text.ends_with('\n') {
            if previous_text.ends_with('\n') {
                diff_lines.push("-[old had trailing newline]".to_string());
                diff_lines.push("+[new missing trailing newline]".to_string());
            } else {
                diff_lines.push("-[old missing trailing newline]".to_string());
                diff_lines.push("+[new has trailing newline]".to_string());
            }
        }
    } else {
        diff_lines.push("@@ -1,0 +1,0 @@".to_string());
    }

    let mut truncated = false;
    if diff_lines.len() > MAX_DIFF_LINES {
        truncated = true;
        let omitted = diff_lines.len() - MAX_DIFF_LINES;
        diff_lines.truncate(MAX_DIFF_LINES);
        diff_lines.push(format!("... diff truncated ({omitted} more lines)"));
    }

    let mut unified = diff_lines.join("\n");
    if unified.len() > MAX_DIFF_CHARS {
        truncated = true;
        let mut end = MAX_DIFF_CHARS;
        while end > 0 && !unified.is_char_boundary(end) {
            end -= 1;
        }
        unified.truncate(end);
        unified.push_str("\n... diff truncated (content too long)");
    }

    json!({
        "format": "unified",
        "unified": unified,
        "old_line_count": old_lines.len(),
        "new_line_count": new_lines.len(),
        "added_lines": added_lines,
        "removed_lines": removed_lines,
        "old_trailing_newline": previous_text.ends_with('\n'),
        "new_trailing_newline": updated_text.ends_with('\n'),
        "truncated": truncated
    })
}

fn lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    text.lines().collect()
}

fn sanitize_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_reports_trailing_newline_changes() {
        let diff = build_unified_diff(Path::new("/tmp/demo.txt"), "hello\n", "hello");
        assert_eq!(diff["old_trailing_newline"], true);
        assert_eq!(diff["new_trailing_newline"], false);
        let unified = diff["unified"].as_str().unwrap_or_default();
        assert!(unified.contains("trailing newline"));
    }
}
