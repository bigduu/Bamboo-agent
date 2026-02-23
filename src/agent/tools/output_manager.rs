//! Tool output management for preventing large tool results from consuming the token budget.
//!
//! When tool results are too large, they are capped and stored as artifacts,
//! with a reference returned to the agent so it can retrieve the full content
//! when needed.

use std::io;
use std::path::PathBuf;

use agent_core::budget::counter::TokenCounter;
use agent_core::budget::HeuristicTokenCounter;

/// Reference to a stored artifact (full tool output stored externally).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactRef {
    /// Unique identifier for the artifact
    pub id: String,
    /// Original tool call ID
    pub tool_call_id: String,
    /// Path to the stored artifact file
    pub path: PathBuf,
    /// Token count of the full content
    pub full_token_count: u32,
}

/// Manager for capping and storing large tool outputs.
#[derive(Debug)]
pub struct ToolOutputManager {
    /// Directory to store artifacts
    artifacts_dir: PathBuf,
    /// Maximum inline tokens for tool results
    max_inline_tokens: u32,
    /// Token counter
    counter: HeuristicTokenCounter,
}

impl ToolOutputManager {
    /// Create a new tool output manager.
    pub fn new(artifacts_dir: impl Into<PathBuf>, max_inline_tokens: u32) -> Self {
        Self {
            artifacts_dir: artifacts_dir.into(),
            max_inline_tokens,
            counter: HeuristicTokenCounter::default(),
        }
    }

    /// Create with default settings.
    ///
    /// Uses `~/.bamboo/artifacts` as the storage directory and 1000 tokens as the limit.
    pub fn with_defaults() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let artifacts_dir = home.join(".bamboo").join("artifacts");
        Self::new(artifacts_dir, 1000)
    }

    /// Cap a tool result if it exceeds the token limit.
    ///
    /// Returns a tuple of (capped_content, optional_artifact_ref).
    /// If the result fits within the budget, returns (result, None).
    /// If the result is too large, returns (truncated_result, Some(artifact_ref)).
    pub async fn cap_tool_result(
        &self,
        tool_call_id: &str,
        result: String,
    ) -> io::Result<(String, Option<ArtifactRef>)> {
        let token_count = self.counter.count_text(&result);

        // If within budget, return as-is
        if token_count <= self.max_inline_tokens {
            return Ok((result, None));
        }

        // Store full result as artifact
        let artifact = self.store_artifact(tool_call_id, &result, token_count).await?;

        // Result is too large - truncate and add a short notice referencing the artifact id.
        // The notice itself costs tokens, so we reserve budget for it up-front.
        let notice = self.build_truncation_notice(token_count, &artifact.id);
        let notice_token_count = self.counter.count_text(&notice);
        let mut content_budget = self.max_inline_tokens.saturating_sub(notice_token_count);

        let mut truncated = if content_budget == 0 {
            String::new()
        } else {
            self.truncate_to_token_limit(&result, content_budget)
        };
        let mut capped = format!("{truncated}{notice}");

        // Due to rounding in the heuristic counter, ensure the final string fits the budget.
        while content_budget > 0 && self.counter.count_text(&capped) > self.max_inline_tokens {
            content_budget = content_budget.saturating_sub(1);
            truncated = if content_budget == 0 {
                String::new()
            } else {
                self.truncate_to_token_limit(&result, content_budget)
            };
            capped = format!("{truncated}{notice}");
        }

        Ok((capped, Some(artifact)))
    }

    /// Builds the truncation notice appended to capped tool output.
    ///
    /// Keep this intentionally short because it competes with the inline token budget.
    fn build_truncation_notice(&self, full_token_count: u32, artifact_id: &str) -> String {
        // NOTE: We only include the artifact id here (not the full path) to keep token usage low
        // and avoid leaking local filesystem paths into the model context. A future
        // `retrieve_artifact` tool can use this id to fetch the full content.
        let candidates = [
            format!(
                "\n\n[Output truncated. Full result ({full_token_count} tokens) stored as artifact id '{artifact_id}'.]"
            ),
            format!("\n\n[Output truncated. Artifact id '{artifact_id}'.]"),
            format!("\n\n[Truncated. Artifact '{artifact_id}'.]"),
        ];

        for candidate in candidates.iter() {
            if self.counter.count_text(candidate) <= self.max_inline_tokens {
                return candidate.clone();
            }
        }

        // Extreme edge case: even the shortest notice doesn't fit. Return a truncated notice so
        // `cap_tool_result` can still satisfy the inline token constraint.
        self.truncate_to_token_limit(&candidates[2], self.max_inline_tokens)
    }

    /// Truncate text to fit within a token budget.
    fn truncate_to_token_limit(&self, text: &str, max_tokens: u32) -> String {
        // Rough estimate: each token is about 4 characters
        // Use a conservative estimate to ensure we stay under the limit
        let max_chars = (max_tokens as f64 * 3.5) as usize;

        if text.len() <= max_chars {
            return text.to_string();
        }

        // Try to truncate at a natural boundary (newline or space)
        let truncate_at = text[..max_chars].rfind('\n')
            .or_else(|| text[..max_chars].rfind(' '))
            .unwrap_or(max_chars);

        format!("{}...", &text[..truncate_at])
    }

    /// Store the full result as an artifact file.
    async fn store_artifact(
        &self,
        tool_call_id: &str,
        content: &str,
        token_count: u32,
    ) -> io::Result<ArtifactRef> {
        // Ensure artifacts directory exists
        tokio::fs::create_dir_all(&self.artifacts_dir).await?;

        // Generate unique artifact ID
        let artifact_id = format!("{}_{}", tool_call_id, chrono::Utc::now().timestamp());
        let filename = format!("{}.txt", artifact_id);
        let artifact_path = self.artifacts_dir.join(&filename);

        // Write content to file
        tokio::fs::write(&artifact_path, content).await?;

        Ok(ArtifactRef {
            id: artifact_id,
            tool_call_id: tool_call_id.to_string(),
            path: artifact_path,
            full_token_count: token_count,
        })
    }

    /// Retrieve a stored artifact by ID.
    pub async fn retrieve_artifact(&self, artifact_id: &str) -> io::Result<Option<String>> {
        let filename = format!("{}.txt", artifact_id);
        let path = self.artifacts_dir.join(&filename);

        if !path.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(&path).await?;
        Ok(Some(content))
    }

    /// List all stored artifacts.
    pub async fn list_artifacts(&self) -> io::Result<Vec<ArtifactRef>> {
        let mut artifacts = Vec::new();

        if !self.artifacts_dir.exists() {
            return Ok(artifacts);
        }

        let mut entries = tokio::fs::read_dir(&self.artifacts_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "txt") {
                if let Some(stem) = path.file_stem() {
                    let id = stem.to_string_lossy().to_string();
                    let metadata = tokio::fs::metadata(&path).await?;

                    // Best-effort: recover the original tool_call_id from the id format
                    // `{tool_call_id}_{unix_timestamp}`.
                    let tool_call_id = id
                        .rsplit_once('_')
                        .and_then(|(prefix, suffix)| suffix.parse::<i64>().ok().map(|_| prefix))
                        .unwrap_or("")
                        .to_string();

                    // For typical artifact sizes, compute tokens from file contents for accuracy.
                    // For very large artifacts, avoid reading the entire file during listing and
                    // fall back to a byte-based estimate.
                    let full_token_count = if metadata.len() <= 1024 * 1024 {
                        match tokio::fs::read_to_string(&path).await {
                            Ok(content) => self.counter.count_text(&content),
                            Err(_) => 0,
                        }
                    } else {
                        // HeuristicTokenCounter defaults: chars/4 + 10% safety margin (ceil).
                        // Using bytes as a proxy for chars intentionally overestimates for
                        // non-ASCII content, which is acceptable for a conservative estimate.
                        let estimated = ((metadata.len() as f64 / 4.0) * 1.1).ceil();
                        estimated.min(u32::MAX as f64) as u32
                    };

                    artifacts.push(ArtifactRef {
                        id,
                        path,
                        tool_call_id,
                        full_token_count,
                    });
                }
            }
        }

        Ok(artifacts)
    }

    /// Delete an artifact by ID.
    pub async fn delete_artifact(&self, artifact_id: &str) -> io::Result<bool> {
        let filename = format!("{}.txt", artifact_id);
        let path = self.artifacts_dir.join(&filename);

        if path.exists() {
            tokio::fs::remove_file(&path).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::budget::HeuristicTokenCounter;
    use tempfile::tempdir;

    #[tokio::test]
    async fn cap_small_result_returns_as_is() {
        let dir = tempdir().unwrap();
        let manager = ToolOutputManager::new(dir.path(), 100);

        let result = "Small result".to_string();
        let (capped, artifact) = manager.cap_tool_result("call_1", result.clone()).await.unwrap();

        assert_eq!(capped, result);
        assert!(artifact.is_none());
    }

    #[tokio::test]
    async fn cap_large_result_stores_artifact() {
        let dir = tempdir().unwrap();
        let manager = ToolOutputManager::new(dir.path(), 100);

        // Create a large result (more than 100 tokens)
        let result = "x".repeat(1000);
        let (capped, artifact) = manager.cap_tool_result("call_1", result.clone()).await.unwrap();

        // Should be truncated
        assert!(capped.len() < result.len());
        assert!(artifact.is_some());

        let artifact = artifact.unwrap();
        assert_eq!(artifact.tool_call_id, "call_1");
        assert!(artifact.path.exists());

        // Should be able to retrieve full content
        let retrieved = manager.retrieve_artifact(&artifact.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), result);
    }

    #[tokio::test]
    async fn cap_large_result_keeps_inline_output_within_budget() {
        let dir = tempdir().unwrap();
        let manager = ToolOutputManager::new(dir.path(), 100);

        // Create a large result to ensure truncation.
        let result = "x".repeat(10_000);
        let (capped, artifact) = manager
            .cap_tool_result("call_budget", result)
            .await
            .unwrap();

        assert!(artifact.is_some());

        let counter = HeuristicTokenCounter::default();
        let capped_token_count = counter.count_text(&capped);
        assert!(
            capped_token_count <= 100,
            "inline output exceeded budget: {capped_token_count} > 100"
        );
    }

    #[tokio::test]
    async fn list_artifacts_includes_tool_call_id_and_token_count() {
        let dir = tempdir().unwrap();
        let manager = ToolOutputManager::new(dir.path(), 50);

        let result = "x".repeat(1_000);
        let (_capped, artifact) = manager
            .cap_tool_result("call_list_123", result)
            .await
            .unwrap();
        let artifact = artifact.unwrap();

        let artifacts = manager.list_artifacts().await.unwrap();
        let listed = artifacts.into_iter().find(|a| a.id == artifact.id).unwrap();

        assert_eq!(listed.tool_call_id, "call_list_123");
        assert!(listed.full_token_count > 0);
        assert!(listed.path.exists());
    }

    #[test]
    fn truncate_preserves_word_boundary() {
        let dir = tempdir().unwrap();
        let manager = ToolOutputManager::new(dir.path(), 100);

        let text = "This is a sentence with multiple words to truncate properly.";
        let truncated = manager.truncate_to_token_limit(text, 10);

        // Should end at a space or newline, not mid-word
        assert!(!truncated.ends_with("sen"));
        assert!(truncated.ends_with("..."));
    }
}
