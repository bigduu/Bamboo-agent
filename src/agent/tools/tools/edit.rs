use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::path::Path;

use super::read_tracker::ReadState;
use super::{file_change, read_tracker};

const MAX_PATCH_BYTES: usize = 256 * 1024;
const MAX_PATCH_BLOCKS: usize = 128;
const MAX_PATCH_BLOCK_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
struct EditArgs {
    file_path: String,
    #[serde(default)]
    old_string: Option<String>,
    #[serde(default)]
    new_string: Option<String>,
    #[serde(default)]
    replace_all: Option<bool>,
    #[serde(default)]
    patch: Option<String>,
    #[serde(default)]
    line_number: Option<usize>,
}

pub struct EditTool;

#[derive(Debug, Clone)]
struct ReplacementCandidate {
    start: usize,
    matched_len: usize,
    replacement: String,
    start_line: usize,
    end_line: usize,
}

impl EditTool {
    pub fn new() -> Self {
        Self
    }

    fn to_lf(value: &str) -> String {
        value.replace("\r\n", "\n")
    }

    fn to_crlf(value: &str) -> String {
        Self::to_lf(value).replace('\n', "\r\n")
    }

    fn has_meaningful_optional_text(value: Option<&str>) -> bool {
        value.is_some_and(|text| !text.is_empty())
    }

    fn line_starts(content: &str) -> Vec<usize> {
        let mut starts = vec![0usize];
        for (idx, byte) in content.bytes().enumerate() {
            if byte == b'\n' && idx + 1 < content.len() {
                starts.push(idx + 1);
            }
        }
        starts
    }

    fn line_for_offset(line_starts: &[usize], offset: usize) -> usize {
        line_starts.partition_point(|line_start| *line_start <= offset)
    }

    fn replacement_variants(
        content: &str,
        old_text: &str,
        new_text: &str,
    ) -> Vec<(String, String)> {
        let mut variants: Vec<(String, String)> = Vec::new();
        let mut seen_variants: HashSet<(String, String)> = HashSet::new();
        let mut push_variant = |search: String, replace: String| {
            if seen_variants.insert((search.clone(), replace.clone())) {
                variants.push((search, replace));
            }
        };

        push_variant(old_text.to_string(), new_text.to_string());
        push_variant(Self::to_lf(old_text), Self::to_lf(new_text));
        if content.contains("\r\n") {
            push_variant(Self::to_crlf(old_text), Self::to_crlf(new_text));
        }

        variants
    }

    fn collect_candidates(
        content: &str,
        old_text: &str,
        new_text: &str,
    ) -> Vec<ReplacementCandidate> {
        let variants = Self::replacement_variants(content, old_text, new_text);
        let line_starts = Self::line_starts(content);
        let mut out: Vec<ReplacementCandidate> = Vec::new();
        let mut seen_matches: HashSet<(usize, usize, String)> = HashSet::new();

        for (search, replacement) in variants {
            if search.is_empty() {
                continue;
            }
            for (start, _) in content.match_indices(&search) {
                let matched_len = search.len();
                let end = start + matched_len - 1;
                let candidate = ReplacementCandidate {
                    start,
                    matched_len,
                    replacement: replacement.clone(),
                    start_line: Self::line_for_offset(&line_starts, start),
                    end_line: Self::line_for_offset(&line_starts, end),
                };
                if seen_matches.insert((start, matched_len, candidate.replacement.clone())) {
                    out.push(candidate);
                }
            }
        }

        out.sort_by_key(|candidate| candidate.start);
        out
    }

    fn candidate_line_summary(candidates: &[ReplacementCandidate]) -> String {
        let mut lines = candidates
            .iter()
            .map(|candidate| candidate.start_line.to_string())
            .collect::<Vec<_>>();
        lines.sort();
        lines.dedup();
        lines.join(", ")
    }

    fn choose_candidate_with_line_hint(
        candidates: &[ReplacementCandidate],
        line_number: usize,
    ) -> Option<ReplacementCandidate> {
        let containing = candidates
            .iter()
            .filter(|candidate| {
                candidate.start_line <= line_number && line_number <= candidate.end_line
            })
            .cloned()
            .collect::<Vec<_>>();

        let pool = if containing.is_empty() {
            candidates.to_vec()
        } else {
            containing
        };

        let mut best: Option<ReplacementCandidate> = None;
        let mut best_distance = usize::MAX;
        let mut tie = false;

        for candidate in pool {
            let distance = candidate.start_line.abs_diff(line_number);
            if distance < best_distance {
                best_distance = distance;
                best = Some(candidate);
                tie = false;
            } else if distance == best_distance {
                tie = true;
            }
        }

        if tie {
            None
        } else {
            best
        }
    }

    fn apply_single_replacement(
        content: &str,
        old_string: &str,
        new_string: &str,
        replace_all: bool,
        line_number: Option<usize>,
    ) -> Result<(String, usize), ToolError> {
        if old_string == new_string {
            return Err(ToolError::InvalidArguments(
                "new_string must be different from old_string".to_string(),
            ));
        }
        if old_string.is_empty() {
            return Err(ToolError::InvalidArguments(
                "old_string must be non-empty".to_string(),
            ));
        }

        if let Some(line) = line_number {
            if line == 0 {
                return Err(ToolError::InvalidArguments(
                    "line_number must be >= 1".to_string(),
                ));
            }
            if replace_all {
                return Err(ToolError::InvalidArguments(
                    "line_number cannot be combined with replace_all=true".to_string(),
                ));
            }
        }

        let candidates = Self::collect_candidates(content, old_string, new_string);

        if candidates.is_empty() {
            return Err(ToolError::Execution(
                "old_string not found in target file".to_string(),
            ));
        }

        if !replace_all && candidates.len() != 1 && line_number.is_none() {
            return Err(ToolError::Execution(format!(
                "old_string matched {} times; provide a more specific old_string, set line_number, use patch mode, or set replace_all=true",
                candidates.len()
            )));
        }

        let updated = if replace_all {
            let variants = Self::replacement_variants(content, old_string, new_string);
            let mut replaced = None;
            let mut updated = None;
            for (search, replacement) in variants {
                let matches = content.match_indices(&search).count();
                if matches > 0 {
                    replaced = Some(matches);
                    updated = Some(content.replace(&search, &replacement));
                    break;
                }
            }
            return Ok((
                updated.unwrap_or_else(|| content.to_string()),
                replaced.unwrap_or(0),
            ));
        } else {
            let chosen = if let Some(line) = line_number {
                Self::choose_candidate_with_line_hint(&candidates, line).ok_or_else(|| {
                    ToolError::Execution(format!(
                        "old_string matched {} times and line_number={} was not unique; candidate start lines: {}. Provide a more specific old_string or patch context",
                        candidates.len(),
                        line,
                        Self::candidate_line_summary(&candidates),
                    ))
                })?
            } else {
                candidates[0].clone()
            };

            let mut next = String::with_capacity(
                content.len().saturating_sub(chosen.matched_len) + chosen.replacement.len(),
            );
            next.push_str(&content[..chosen.start]);
            next.push_str(&chosen.replacement);
            next.push_str(&content[chosen.start + chosen.matched_len..]);
            next
        };

        Ok((updated, 1))
    }

    fn parse_patch_blocks(patch: &str) -> Result<Vec<(String, String)>, ToolError> {
        const SEARCH: &str = "<<<<<<< SEARCH\n";
        const SEP: &str = "\n=======\n";
        const REPLACE: &str = "\n>>>>>>> REPLACE";

        let normalized = patch.replace("\r\n", "\n");
        if normalized.trim().is_empty() {
            return Err(ToolError::InvalidArguments(
                "patch must be non-empty".to_string(),
            ));
        }
        if normalized.len() > MAX_PATCH_BYTES {
            return Err(ToolError::InvalidArguments(format!(
                "patch exceeds max size of {} bytes",
                MAX_PATCH_BYTES
            )));
        }

        let mut cursor = 0usize;
        let mut blocks = Vec::new();
        while let Some(start_rel) = normalized[cursor..].find(SEARCH) {
            if blocks.len() >= MAX_PATCH_BLOCKS {
                return Err(ToolError::InvalidArguments(format!(
                    "patch exceeds max block count of {}",
                    MAX_PATCH_BLOCKS
                )));
            }
            let search_start = cursor + start_rel + SEARCH.len();
            let sep_rel = normalized[search_start..].find(SEP).ok_or_else(|| {
                ToolError::InvalidArguments("Malformed patch block: missing =======".to_string())
            })?;
            let sep_idx = search_start + sep_rel;
            let replace_start = sep_idx + SEP.len();
            let replace_rel = normalized[replace_start..].find(REPLACE).ok_or_else(|| {
                ToolError::InvalidArguments(
                    "Malformed patch block: missing >>>>>>> REPLACE".to_string(),
                )
            })?;
            let replace_idx = replace_start + replace_rel;

            let old_block = normalized[search_start..sep_idx].to_string();
            let new_block = normalized[replace_start..replace_idx].to_string();
            if old_block.is_empty() {
                return Err(ToolError::InvalidArguments(
                    "Patch SEARCH block must be non-empty".to_string(),
                ));
            }
            if old_block.len() > MAX_PATCH_BLOCK_BYTES || new_block.len() > MAX_PATCH_BLOCK_BYTES {
                return Err(ToolError::InvalidArguments(format!(
                    "Patch block exceeds max block size of {} bytes",
                    MAX_PATCH_BLOCK_BYTES
                )));
            }
            blocks.push((old_block, new_block));

            cursor = replace_idx + REPLACE.len();
            if normalized[cursor..].starts_with('\n') {
                cursor += 1;
            }
        }

        if blocks.is_empty() {
            return Err(ToolError::InvalidArguments(
                "patch must contain at least one SEARCH/REPLACE block".to_string(),
            ));
        }

        Ok(blocks)
    }

    fn apply_patch_mode(
        content: &str,
        patch: &str,
        line_number: Option<usize>,
    ) -> Result<(String, usize), ToolError> {
        if let Some(line) = line_number {
            if line == 0 {
                return Err(ToolError::InvalidArguments(
                    "line_number must be >= 1".to_string(),
                ));
            }
        }
        let blocks = Self::parse_patch_blocks(patch)?;
        let mut updated = content.to_string();
        let mut replacements = 0usize;

        for (idx, (old_block, new_block)) in blocks.iter().enumerate() {
            let candidates = Self::collect_candidates(&updated, old_block, new_block);

            if candidates.is_empty() {
                return Err(ToolError::Execution(format!(
                    "Patch block {} SEARCH content not found in target file",
                    idx + 1
                )));
            }

            let chosen = if candidates.len() == 1 {
                candidates[0].clone()
            } else if let Some(line) = line_number {
                Self::choose_candidate_with_line_hint(&candidates, line).ok_or_else(|| {
                    ToolError::Execution(format!(
                        "Patch block {} SEARCH content matched {} times and line_number={} was not unique; candidate start lines: {}. Add more context to make it unique",
                        idx + 1,
                        candidates.len(),
                        line,
                        Self::candidate_line_summary(&candidates),
                    ))
                })?
            } else {
                return Err(ToolError::Execution(format!(
                    "Patch block {} SEARCH content matched {} times; set line_number or add more context to make it unique",
                    idx + 1,
                    candidates.len()
                )));
            };

            let mut next = String::with_capacity(
                updated.len().saturating_sub(chosen.matched_len) + chosen.replacement.len(),
            );
            next.push_str(&updated[..chosen.start]);
            next.push_str(&chosen.replacement);
            next.push_str(&updated[chosen.start + chosen.matched_len..]);
            updated = next;
            replacements += 1;
        }

        Ok((updated, replacements))
    }
}

impl Default for EditTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "Edit"
    }

    fn description(&self) -> &str {
        "Edit existing files via exact replacements or SEARCH/REPLACE patch blocks. IMPORTANT: call Read first in this session or Edit will fail."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to modify"
                },
                "old_string": {
                    "type": "string",
                    "description": "Legacy mode only: exact text to replace. Do not send with patch mode."
                },
                "new_string": {
                    "type": "string",
                    "description": "Legacy mode only: replacement text. Do not send with patch mode."
                },
                "replace_all": {
                    "type": "boolean",
                    "default": false,
                    "description": "Legacy mode only: replace all occurrences. Do not send with patch mode."
                },
                "patch": {
                    "type": "string",
                    "description": "Patch mode: one or more blocks using <<<<<<< SEARCH / ======= / >>>>>>> REPLACE. Preferred mode. Do not combine with non-empty old_string/new_string or replace_all=true."
                },
                "line_number": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional 1-based line hint to disambiguate duplicate matches"
                }
            },
            "required": ["file_path"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Edit"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: EditArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Edit args: {}", e)))?;

        let file_path = parsed.file_path.trim();
        let path = Path::new(file_path);
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        if let Some(session_id) = ctx.session_id {
            match read_tracker::read_state(session_id, file_path).await {
                ReadState::Unread => {
                    return Err(ToolError::Execution(
                        "Edit requires reading the target file first via Read".to_string(),
                    ));
                }
                ReadState::Stale => {
                    return Err(ToolError::Execution(
                        "Target file changed after last Read; call Read again before Edit"
                            .to_string(),
                    ));
                }
                ReadState::Fresh => {}
            }
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read file: {}", e)))?;

        let patch = parsed
            .patch
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let old_string = parsed.old_string.as_deref();
        let new_string = parsed.new_string.as_deref();

        let (updated, replacements, mode_label) = if let Some(patch_text) = patch {
            if Self::has_meaningful_optional_text(old_string)
                || Self::has_meaningful_optional_text(new_string)
                || parsed.replace_all == Some(true)
            {
                return Err(ToolError::InvalidArguments(
                    "patch mode cannot be combined with old_string/new_string/replace_all"
                        .to_string(),
                ));
            }
            let (next, count) = Self::apply_patch_mode(&content, patch_text, parsed.line_number)?;
            (next, count, "patch")
        } else {
            let old = old_string.ok_or_else(|| {
                ToolError::InvalidArguments(
                    "old_string is required unless patch mode is used".to_string(),
                )
            })?;
            let new = new_string.ok_or_else(|| {
                ToolError::InvalidArguments(
                    "new_string is required unless patch mode is used".to_string(),
                )
            })?;
            let (next, count) = Self::apply_single_replacement(
                &content,
                old,
                new,
                parsed.replace_all.unwrap_or(false),
                parsed.line_number,
            )?;
            (next, count, "legacy")
        };

        let checkpoint = file_change::create_checkpoint(path, Some(content.as_bytes())).await?;

        file_change::atomic_write_text(path, &updated).await?;

        let payload = file_change::build_file_change_payload(
            "Edit",
            path,
            format!(
                "Edited file: {} (mode: {}, replacements: {})",
                file_path, mode_label, replacements
            ),
            checkpoint,
            &content,
            &updated,
        );

        Ok(ToolResult {
            success: true,
            result: payload,
            display_preference: Some("Default".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::ReadTool;
    use serde_json::json;

    #[tokio::test]
    async fn edit_requires_unique_match_without_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar"
            }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn edit_supports_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar",
                "replace_all": true
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "bar\nbar\n");
    }

    #[tokio::test]
    async fn edit_replace_all_does_not_reprocess_newly_inserted_matches() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "a\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "a",
                "new_string": "aa",
                "replace_all": true
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "aa\n");
    }

    #[tokio::test]
    async fn edit_requires_read_first_when_session_context_exists() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world\n")
            .await
            .unwrap();
        let call_id = "call_1";

        let edit_tool = EditTool::new();
        let read_tool = ReadTool::new();

        let denied = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await;
        assert!(denied.is_err());

        let _ = read_tool
            .execute_with_context(
                json!({"file_path": file.path()}),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        let allowed = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        assert!(allowed.success);
    }

    #[tokio::test]
    async fn edit_rejects_empty_old_string() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "",
                "new_string": "x",
                "replace_all": true
            }))
            .await;

        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn edit_legacy_mode_handles_crlf_when_old_string_uses_lf() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "alpha\r\nbeta\r\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "alpha\nbeta\n",
                "new_string": "gamma\ndelta\n"
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "gamma\r\ndelta\r\n");
    }

    #[tokio::test]
    async fn edit_legacy_mode_line_number_disambiguates_duplicates() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nbar\nfoo\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "baz",
                "line_number": 3
            }))
            .await
            .unwrap();
        assert!(result.success);

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "foo\nbar\nbaz\n");
    }

    #[tokio::test]
    async fn edit_legacy_mode_rejects_line_number_with_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar",
                "replace_all": true,
                "line_number": 1
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("line_number cannot be combined"))
        );
    }

    #[tokio::test]
    async fn edit_patch_mode_can_target_second_duplicate_with_context() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(
            file.path(),
            "fn a() {\n    let v = 1;\n}\n\nfn b() {\n    let v = 1;\n}\n",
        )
        .await
        .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": "<<<<<<< SEARCH\nfn b() {\n    let v = 1;\n}\n=======\nfn b() {\n    let v = 2;\n}\n>>>>>>> REPLACE"
            }))
            .await
            .unwrap();
        assert!(result.success);

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert!(updated.contains("fn a() {\n    let v = 1;\n}"));
        assert!(updated.contains("fn b() {\n    let v = 2;\n}"));
    }

    #[tokio::test]
    async fn edit_patch_mode_handles_crlf_when_patch_uses_lf() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "fn b() {\r\n    let v = 1;\r\n}\r\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": "<<<<<<< SEARCH\nfn b() {\n    let v = 1;\n}\n=======\nfn b() {\n    let v = 2;\n}\n>>>>>>> REPLACE"
            }))
            .await
            .unwrap();
        assert!(result.success);

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "fn b() {\r\n    let v = 2;\r\n}\r\n");
    }

    #[tokio::test]
    async fn edit_patch_mode_line_number_disambiguates_duplicates() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "x = 1;\nx = 1;\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "line_number": 2,
                "patch": "<<<<<<< SEARCH\nx = 1;\n=======\nx = 2;\n>>>>>>> REPLACE"
            }))
            .await
            .unwrap();
        assert!(result.success);

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "x = 1;\nx = 2;\n");
    }

    #[tokio::test]
    async fn edit_patch_mode_rejects_ambiguous_search_block() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "x = 1;\nx = 1;\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": "<<<<<<< SEARCH\nx = 1;\n=======\nx = 2;\n>>>>>>> REPLACE"
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::Execution(msg)) if msg.contains("matched 2 times"))
        );
    }

    #[tokio::test]
    async fn edit_rejects_mixed_patch_and_legacy_args() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "hello",
                "new_string": "world",
                "patch": "<<<<<<< SEARCH\nhello\n=======\nworld\n>>>>>>> REPLACE"
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("cannot be combined"))
        );
    }

    #[tokio::test]
    async fn edit_patch_mode_ignores_empty_legacy_placeholders() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "",
                "new_string": "",
                "replace_all": false,
                "patch": "<<<<<<< SEARCH\nhello\n=======\nworld\n>>>>>>> REPLACE"
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "world");
    }

    #[tokio::test]
    async fn edit_patch_rejects_oversized_patch_payload() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world").await.unwrap();
        let huge = "a".repeat(MAX_PATCH_BYTES + 1);

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": huge
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("max size"))
        );
    }

    #[tokio::test]
    async fn edit_patch_rejects_excessive_block_count() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world").await.unwrap();
        let mut patch = String::new();
        for _ in 0..=MAX_PATCH_BLOCKS {
            patch.push_str("<<<<<<< SEARCH\nx\n=======\ny\n>>>>>>> REPLACE\n");
        }

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": patch
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("max block count"))
        );
    }
}
