use crate::agent::core::tools::ToolError;
use chrono::Utc;
use serde_json::{json, Value};
use std::path::Path;

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
