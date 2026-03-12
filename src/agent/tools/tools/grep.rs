use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use globset::{GlobBuilder, GlobSet};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::workspace_state;

const DEFAULT_HEAD_LIMIT: usize = 200;
const MAX_RESULT_BYTES: usize = 256 * 1024;
const MAX_MATCHES: usize = 2_000;
const MAX_SCANNED_FILES: usize = 50_000;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const SKIP_DIRS: [&str; 8] = [
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".cache",
    "coverage",
];
const SEARCH_SCOPE_TOO_BROAD_ERROR: &str =
    "Search scope too broad. Add path/glob/type or reduce pattern.";
const MULTILINE_REQUIRES_NARROWED_PATH_ERROR: &str = "Multiline grep requires narrowed path.";
const RESULT_TOO_LARGE_ERROR: &str = "Result too large; refine query and retry.";

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
enum OutputMode {
    Content,
    #[default]
    FilesWithMatches,
    Count,
}

#[derive(Debug, Deserialize)]
struct GrepArgs {
    pattern: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    glob: Option<String>,
    #[serde(default)]
    output_mode: Option<OutputMode>,
    #[serde(rename = "-B", default)]
    before: Option<usize>,
    #[serde(rename = "-A", default)]
    after: Option<usize>,
    #[serde(rename = "-C", default)]
    context: Option<usize>,
    #[serde(rename = "-n", default)]
    line_numbers: Option<bool>,
    #[serde(rename = "-i", default)]
    case_insensitive: Option<bool>,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    head_limit: Option<usize>,
    #[serde(default)]
    multiline: Option<bool>,
}

pub struct GrepTool;

impl GrepTool {
    pub fn new() -> Self {
        Self
    }

    fn extension_map() -> HashMap<&'static str, &'static [&'static str]> {
        HashMap::from([
            ("js", &["js", "mjs", "cjs"] as &[_]),
            ("ts", &["ts", "tsx"]),
            ("py", &["py"]),
            ("rust", &["rs"]),
            ("go", &["go"]),
            ("java", &["java"]),
            ("cpp", &["cc", "cpp", "cxx", "hpp", "h"]),
            ("c", &["c", "h"]),
            ("json", &["json"]),
            ("yaml", &["yaml", "yml"]),
            ("toml", &["toml"]),
            ("md", &["md", "markdown"]),
        ])
    }

    fn collect_files(base: &Path, type_filter: Option<&str>) -> Vec<PathBuf> {
        let ext_map = Self::extension_map();
        let allowed_ext = type_filter.and_then(|name| ext_map.get(name).copied());

        let mut files = Vec::new();
        for entry in WalkDir::new(base)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                !entry.file_type().is_dir() || !Self::should_skip_dir(entry.path())
            })
            .filter_map(|entry| entry.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            if files.len() >= MAX_SCANNED_FILES {
                break;
            }
            let path = entry.path();

            if let Some(extensions) = allowed_ext {
                let ext = path
                    .extension()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default();
                if !extensions.iter().any(|candidate| candidate == &ext) {
                    continue;
                }
            }

            files.push(path.to_path_buf());
        }

        files
    }

    fn should_skip_dir(path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| SKIP_DIRS.contains(&name))
            .unwrap_or(false)
    }

    fn compile_glob(glob: Option<&str>) -> Result<Option<GlobSet>, ToolError> {
        let Some(pattern) = glob else {
            return Ok(None);
        };

        let mut builder = globset::GlobSetBuilder::new();
        let glob = GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid glob pattern: {}", e)))?;
        builder.add(glob);
        builder
            .build()
            .map(Some)
            .map_err(|e| ToolError::Execution(format!("Failed to compile glob: {}", e)))
    }

    fn compile_regex(
        pattern: &str,
        case_insensitive: bool,
        multiline: bool,
    ) -> Result<Regex, ToolError> {
        let mut builder = RegexBuilder::new(pattern);
        builder.case_insensitive(case_insensitive);
        builder.dot_matches_new_line(multiline);
        builder.multi_line(multiline);
        builder
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid regex pattern: {}", e)))
    }

    fn byte_to_line(line_starts: &[usize], byte: usize) -> usize {
        match line_starts.binary_search(&byte) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        }
    }

    fn format_content_hits(
        path: &Path,
        content: &str,
        regex: &Regex,
        multiline: bool,
        before: usize,
        after: usize,
        line_numbers: bool,
    ) -> Vec<String> {
        let lines: Vec<&str> = content.lines().collect();
        if lines.is_empty() {
            return Vec::new();
        }

        let mut selected_lines = BTreeSet::new();

        if multiline {
            let mut line_starts = vec![0usize];
            for (idx, byte) in content.bytes().enumerate() {
                if byte == b'\n' {
                    line_starts.push(idx + 1);
                }
            }

            for mat in regex.find_iter(content) {
                let start_line = Self::byte_to_line(&line_starts, mat.start());
                let end_line = Self::byte_to_line(&line_starts, mat.end().saturating_sub(1));
                let range_start = start_line.saturating_sub(before);
                let range_end = (end_line + after).min(lines.len().saturating_sub(1));
                for line_idx in range_start..=range_end {
                    selected_lines.insert(line_idx);
                }
            }
        } else {
            for (idx, line) in lines.iter().enumerate() {
                if regex.is_match(line) {
                    let range_start = idx.saturating_sub(before);
                    let range_end = (idx + after).min(lines.len().saturating_sub(1));
                    for line_idx in range_start..=range_end {
                        selected_lines.insert(line_idx);
                    }
                }
            }
        }

        selected_lines
            .into_iter()
            .map(|idx| {
                let display_path = crate::core::paths::path_to_display_string(path);
                if line_numbers {
                    format!("{}:{}:{}", display_path, idx + 1, lines[idx])
                } else {
                    format!("{}:{}", display_path, lines[idx])
                }
            })
            .collect()
    }

    fn resolve_search_root(path: Option<&str>, cwd: &Path) -> PathBuf {
        match path {
            Some(path) => {
                let candidate = PathBuf::from(path);
                if candidate.is_absolute() {
                    candidate
                } else {
                    cwd.join(candidate)
                }
            }
            None => cwd.to_path_buf(),
        }
    }

    fn validate_scope(
        args: &GrepArgs,
        output_mode: OutputMode,
        multiline: bool,
        cwd: &Path,
    ) -> Result<(), ToolError> {
        if matches!(output_mode, OutputMode::Content)
            && args.path.is_none()
            && args.glob.is_none()
            && args.r#type.is_none()
        {
            return Err(ToolError::InvalidArguments(
                SEARCH_SCOPE_TOO_BROAD_ERROR.to_string(),
            ));
        }

        if multiline {
            let Some(path) = args.path.as_deref() else {
                return Err(ToolError::InvalidArguments(
                    MULTILINE_REQUIRES_NARROWED_PATH_ERROR.to_string(),
                ));
            };

            let resolved = Self::resolve_search_root(Some(path), cwd);
            if resolved.is_dir() {
                if let (Ok(resolved_canonical), Ok(cwd_canonical)) =
                    (resolved.canonicalize(), cwd.canonicalize())
                {
                    if resolved_canonical == cwd_canonical {
                        return Err(ToolError::InvalidArguments(
                            MULTILINE_REQUIRES_NARROWED_PATH_ERROR.to_string(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for GrepTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "Grep"
    }

    fn description(&self) -> &str {
        "Search file contents using ripgrep-style regex parameters"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Regex pattern" },
                "path": { "type": "string", "description": "File or directory to search" },
                "glob": { "type": "string", "description": "Glob file filter" },
                "output_mode": {
                    "type": "string",
                    "enum": ["content", "files_with_matches", "count"],
                    "description": "Output mode"
                },
                "-B": { "type": "number", "description": "Lines before match" },
                "-A": { "type": "number", "description": "Lines after match" },
                "-C": { "type": "number", "description": "Lines before and after match" },
                "-n": { "type": "boolean", "description": "Show line numbers" },
                "-i": { "type": "boolean", "description": "Case insensitive" },
                "type": { "type": "string", "description": "File type filter" },
                "head_limit": { "type": "number", "description": "Limit output entries" },
                "multiline": { "type": "boolean", "description": "Enable multiline regex" }
            },
            "required": ["pattern"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Grep"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: GrepArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Grep args: {}", e)))?;

        let cwd = workspace_state::workspace_or_process_cwd(ctx.session_id);
        let root = Self::resolve_search_root(parsed.path.as_deref(), &cwd);

        let output_mode = parsed.output_mode.unwrap_or_default();
        let context = parsed.context.unwrap_or(0);
        let before = parsed.before.unwrap_or(context);
        let after = parsed.after.unwrap_or(context);
        let line_numbers = parsed.line_numbers.unwrap_or(false);
        let case_insensitive = parsed.case_insensitive.unwrap_or(false);
        let multiline = parsed.multiline.unwrap_or(false);
        let head_limit = parsed.head_limit.unwrap_or(DEFAULT_HEAD_LIMIT);

        Self::validate_scope(&parsed, output_mode, multiline, &cwd)?;

        let regex = Self::compile_regex(&parsed.pattern, case_insensitive, multiline)?;
        let glob_filter = Self::compile_glob(parsed.glob.as_deref())?;

        let files = if root.is_file() {
            vec![root.clone()]
        } else if root.is_dir() {
            Self::collect_files(&root, parsed.r#type.as_deref())
        } else {
            return Err(ToolError::Execution(format!(
                "Path does not exist: {}",
                root.display()
            )));
        };

        let mut matched_files = Vec::new();
        let mut count_rows = Vec::new();
        let mut content_rows = Vec::new();
        let mut total_matches = 0usize;
        let mut partial = false;

        for file in files {
            if let Some(filter) = &glob_filter {
                let relative = file.strip_prefix(&root).unwrap_or(&file);
                if !filter.is_match(relative) && !filter.is_match(&file) {
                    continue;
                }
            }

            let Ok(metadata) = tokio::fs::metadata(&file).await else {
                continue;
            };
            if metadata.len() > MAX_FILE_BYTES {
                continue;
            }

            let Ok(content) = tokio::fs::read_to_string(&file).await else {
                continue;
            };

            if content.contains('\0') {
                continue;
            }

            let match_count = if multiline {
                regex.find_iter(&content).count()
            } else {
                content.lines().filter(|line| regex.is_match(line)).count()
            };

            if match_count == 0 {
                continue;
            }

            total_matches = total_matches.saturating_add(match_count);
            if total_matches > MAX_MATCHES {
                return Err(ToolError::Execution(RESULT_TOO_LARGE_ERROR.to_string()));
            }

            matched_files.push(crate::core::paths::path_to_display_string(&file));
            count_rows.push(format!(
                "{}:{}",
                crate::core::paths::path_to_display_string(&file),
                match_count
            ));

            if matches!(output_mode, OutputMode::Content) {
                content_rows.extend(Self::format_content_hits(
                    &file,
                    &content,
                    &regex,
                    multiline,
                    before,
                    after,
                    line_numbers,
                ));
                if content_rows.len() >= head_limit {
                    content_rows.truncate(head_limit);
                    partial = true;
                    break;
                }
            }

            if matches!(
                output_mode,
                OutputMode::FilesWithMatches | OutputMode::Count
            ) && matched_files.len() >= head_limit
            {
                partial = true;
                break;
            }
        }

        let mut result_lines = match output_mode {
            OutputMode::FilesWithMatches => matched_files,
            OutputMode::Count => count_rows,
            OutputMode::Content => content_rows,
        };

        if result_lines.len() > head_limit {
            result_lines.truncate(head_limit);
            partial = true;
        }
        if partial {
            result_lines
                .push("[PARTIAL] Output was truncated. Narrow path/pattern and retry.".to_string());
        }

        let result = result_lines.join("\n");
        if result.len() > MAX_RESULT_BYTES {
            return Err(ToolError::Execution(RESULT_TOO_LARGE_ERROR.to_string()));
        }

        Ok(ToolResult {
            success: true,
            result,
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn result_lines(result: &ToolResult) -> Vec<&str> {
        result
            .result
            .lines()
            .filter(|line| !line.is_empty())
            .collect()
    }

    fn non_partial_lines(result: &ToolResult) -> Vec<&str> {
        result_lines(result)
            .into_iter()
            .filter(|line| !line.starts_with("[PARTIAL]"))
            .collect()
    }

    #[tokio::test]
    async fn grep_defaults_to_files_with_matches() {
        let dir = tempfile::tempdir().unwrap();
        let file_hit = dir.path().join("match.rs");
        let file_miss = dir.path().join("miss.txt");
        tokio::fs::write(&file_hit, "let value = 1;\nneedle\n")
            .await
            .unwrap();
        tokio::fs::write(&file_miss, "nothing to see\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = tool
            .execute(json!({
                "pattern": "needle",
                "path": dir.path()
            }))
            .await
            .unwrap();

        assert!(result.success);
        let lines = result_lines(&result);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("match.rs"));
    }

    #[tokio::test]
    async fn grep_content_mode_supports_context_and_line_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("content.txt");
        tokio::fs::write(&file, "one\ntwo\nneedle\nfour\nfive\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = tool
            .execute(json!({
                "pattern": "needle",
                "path": file,
                "output_mode": "content",
                "-C": 1,
                "-n": true
            }))
            .await
            .unwrap();

        let output = result.result;
        assert!(output.contains(":2:two"));
        assert!(output.contains(":3:needle"));
        assert!(output.contains(":4:four"));
        assert!(!output.contains(":1:one"));
        assert!(!output.contains(":5:five"));
    }

    #[tokio::test]
    async fn grep_count_mode_respects_type_filter_and_head_limit() {
        let dir = tempfile::tempdir().unwrap();
        let file_rs_a = dir.path().join("a.rs");
        let file_rs_b = dir.path().join("b.rs");
        let file_txt = dir.path().join("c.txt");
        tokio::fs::write(&file_rs_a, "foo\nfoo\n").await.unwrap();
        tokio::fs::write(&file_rs_b, "foo\n").await.unwrap();
        tokio::fs::write(&file_txt, "foo\n").await.unwrap();

        let tool = GrepTool::new();
        let result = tool
            .execute(json!({
                "pattern": "foo",
                "path": dir.path(),
                "output_mode": "count",
                "type": "rust",
                "head_limit": 1
            }))
            .await
            .unwrap();

        let lines = non_partial_lines(&result);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains(".rs:"));
        assert!(!lines[0].contains("c.txt"));
        assert!(result.result.contains("[PARTIAL]"));
    }

    #[tokio::test]
    async fn grep_multiline_and_case_insensitive_work_with_glob_filter() {
        let dir = tempfile::tempdir().unwrap();
        let file_one = dir.path().join("one.rs");
        let file_two = dir.path().join("two.rs");
        tokio::fs::write(&file_one, "Hello\nWORLD\n").await.unwrap();
        tokio::fs::write(&file_two, "Hello\nplanet\n")
            .await
            .unwrap();

        let tool = GrepTool::new();
        let result = tool
            .execute(json!({
                "pattern": "hello\\s+world",
                "path": dir.path(),
                "glob": "**/one.rs",
                "-i": true,
                "multiline": true
            }))
            .await
            .unwrap();

        let output = result.result;
        assert!(output.contains("one.rs"));
        assert!(!output.contains("two.rs"));
    }

    #[tokio::test]
    async fn grep_content_mode_requires_scope_hint() {
        let tool = GrepTool::new();
        let error = tool
            .execute(json!({
                "pattern": "needle",
                "output_mode": "content"
            }))
            .await
            .expect_err("content mode without scope should fail");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error.to_string().contains(SEARCH_SCOPE_TOO_BROAD_ERROR));
    }

    #[tokio::test]
    async fn grep_multiline_requires_explicit_narrowed_path() {
        let tool = GrepTool::new();
        let error = tool
            .execute(json!({
                "pattern": "a\\s+b",
                "multiline": true
            }))
            .await
            .expect_err("multiline without path should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error
            .to_string()
            .contains(MULTILINE_REQUIRES_NARROWED_PATH_ERROR));

        let cwd = std::env::current_dir().unwrap();
        let error = tool
            .execute(json!({
                "pattern": "a\\s+b",
                "multiline": true,
                "path": cwd
            }))
            .await
            .expect_err("multiline at workspace root should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error
            .to_string()
            .contains(MULTILINE_REQUIRES_NARROWED_PATH_ERROR));
    }

    #[tokio::test]
    async fn grep_defaults_head_limit_to_200() {
        let dir = tempfile::tempdir().unwrap();
        for idx in 0..260 {
            let file = dir.path().join(format!("file-{idx}.txt"));
            tokio::fs::write(&file, "needle\n").await.unwrap();
        }

        let tool = GrepTool::new();
        let result = tool
            .execute(json!({
                "pattern": "needle",
                "path": dir.path()
            }))
            .await
            .unwrap();

        let lines = non_partial_lines(&result);
        assert_eq!(lines.len(), 200);
        assert!(result.result.contains("[PARTIAL]"));
    }

    #[tokio::test]
    async fn grep_rejects_excessive_match_volume() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("huge.txt");
        let mut content = String::new();
        for _ in 0..(MAX_MATCHES + 1) {
            content.push_str("needle\n");
        }
        tokio::fs::write(&file, content).await.unwrap();

        let tool = GrepTool::new();
        let error = tool
            .execute(json!({
                "pattern": "needle",
                "path": file
            }))
            .await
            .expect_err("should reject oversized results");

        assert!(matches!(error, ToolError::Execution(_)));
        assert!(error.to_string().contains(RESULT_TOO_LARGE_ERROR));
    }
}
