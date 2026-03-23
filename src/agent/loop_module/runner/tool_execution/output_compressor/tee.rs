//! Tee system: save full output to disk when compression occurs.
//!
//! When a tool result is compressed, the original full output is saved to
//! `~/.bamboo/tee/<session_id>/<timestamp>_<command_slug>.log` so the LLM
//! can later `Read` the file if it needs the uncompressed details.

use std::path::PathBuf;

use crate::core::paths::bamboo_dir;

/// Maximum length of the command slug in the filename.
const MAX_SLUG_LEN: usize = 60;

/// Minimum savings (bytes) required to bother tee-saving.
/// If compression only saved a tiny amount, skip the disk write.
const MIN_SAVINGS_BYTES: usize = 500;

/// Save the full output to disk and return a note string to append to the
/// compressed result.  Returns `None` if the savings are too small to justify
/// a tee file.
pub(crate) async fn tee_save_if_needed(
    session_id: &str,
    args_json: &str,
    full_output: &str,
    compressed_output: &str,
) -> Option<String> {
    let savings = full_output
        .len()
        .saturating_sub(compressed_output.len());
    if savings < MIN_SAVINGS_BYTES {
        return None;
    }

    match tee_save(session_id, args_json, full_output).await {
        Ok(path) => {
            let display_path = crate::core::paths::path_to_display_string(&path);
            Some(format!(
                "[full output saved: {} ({} bytes)]",
                display_path,
                full_output.len()
            ))
        }
        Err(e) => {
            tracing::warn!("Failed to tee-save output: {}", e);
            None
        }
    }
}

/// Write the full output to disk and return the path.
async fn tee_save(
    session_id: &str,
    args_json: &str,
    full_output: &str,
) -> Result<PathBuf, std::io::Error> {
    let tee_dir = bamboo_dir().join("tee").join(session_id);
    tokio::fs::create_dir_all(&tee_dir).await?;

    let command = extract_command_for_slug(args_json);
    let slug = sanitize_filename(&command);
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");

    let filename = format!("{}_{}.log", timestamp, slug);
    let path = tee_dir.join(&filename);

    tokio::fs::write(&path, full_output).await?;

    tracing::debug!("Tee-saved {} bytes to {:?}", full_output.len(), path);
    Ok(path)
}

/// Extract the `command` field from the Bash JSON args for use in the filename.
fn extract_command_for_slug(args_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(args_json)
        .ok()
        .and_then(|v| v.get("command").and_then(|c| c.as_str()).map(String::from))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Sanitize a string into a safe filename slug.
fn sanitize_filename(input: &str) -> String {
    let slug: String = input
        .chars()
        .take(MAX_SLUG_LEN)
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Trim trailing underscores
    slug.trim_end_matches('_').to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_basic() {
        assert_eq!(sanitize_filename("cargo test"), "cargo_test");
    }

    #[test]
    fn sanitize_special_chars() {
        // `-` is in the allowed set, `&&` and spaces all become `_`
        assert_eq!(
            sanitize_filename("cd /foo/bar && cargo test --lib"),
            "cd__foo_bar____cargo_test_--lib"
        );
    }

    #[test]
    fn sanitize_truncates() {
        let long = "a".repeat(200);
        let result = sanitize_filename(&long);
        assert!(result.len() <= MAX_SLUG_LEN);
    }

    #[test]
    fn extract_command_valid_json() {
        let args = r#"{"command": "cargo test --workspace"}"#;
        assert_eq!(extract_command_for_slug(args), "cargo test --workspace");
    }

    #[test]
    fn extract_command_invalid_json() {
        assert_eq!(extract_command_for_slug("not json"), "unknown");
    }

    #[test]
    fn extract_command_missing_field() {
        let args = r#"{"other": "value"}"#;
        assert_eq!(extract_command_for_slug(args), "unknown");
    }

    #[tokio::test]
    async fn tee_save_if_needed_small_savings() {
        // Savings below threshold → should return None
        let full = "short output";
        let compressed = "short out";
        let result =
            tee_save_if_needed("test-session", r#"{"command":"echo"}"#, full, compressed).await;
        assert!(result.is_none());
    }
}
