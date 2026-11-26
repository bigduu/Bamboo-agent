use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{cell::RefCell, future::Future, ops::Range, path::Path};

use super::parameter_schema;
use super::read_tracker::{self, MAX_TRACKED_FILE_SIZE};

pub const READ_RESULT_IDENTITY_KEY: &str = "bamboo.read_result.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReadSourceKind {
    File,
    Directory,
}

/// The exact source and displayed range of a successful built-in Read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadResultIdentity {
    canonical_path: String,
    source_kind: ReadSourceKind,
    source_version: [u8; 32],
    range_start: usize,
    range_end: usize,
    result_sha256: [u8; 32],
}

impl ReadResultIdentity {
    pub fn matches_full_result(&self, content: &str) -> bool {
        let digest: [u8; 32] = Sha256::digest(content.as_bytes()).into();
        self.result_sha256 == digest
    }
}

struct ReadObservation {
    call_id: String,
    captured: Option<ReadResultIdentity>,
    repeated: bool,
}

tokio::task_local! {
    static READ_OBSERVATION: RefCell<ReadObservation>;
}

/// Capture optional identity from the actual Read invocation with this call ID.
pub async fn observe_read_result<F: Future>(
    call_id: &str,
    invocation: F,
) -> (F::Output, Option<ReadResultIdentity>) {
    READ_OBSERVATION
        .scope(
            RefCell::new(ReadObservation {
                call_id: call_id.to_owned(),
                captured: None,
                repeated: false,
            }),
            async {
                let output = invocation.await;
                let identity =
                    READ_OBSERVATION.with(|observation| observation.borrow_mut().captured.take());
                (output, identity)
            },
        )
        .await
}

fn observing_read(call_id: &str) -> bool {
    READ_OBSERVATION
        .try_with(|observation| observation.borrow().call_id == call_id)
        .unwrap_or(false)
}

fn record_read_result(
    call_id: &str,
    canonical_path: String,
    source_kind: ReadSourceKind,
    source_version: [u8; 32],
    result: &RenderedRead,
) {
    let _ = READ_OBSERVATION.try_with(|observation| {
        let mut observation = observation.borrow_mut();
        if observation.call_id != call_id || observation.repeated {
            return;
        }
        if observation.captured.is_some() {
            observation.captured = None;
            observation.repeated = true;
            return;
        }
        observation.captured = Some(ReadResultIdentity {
            canonical_path,
            source_kind,
            source_version,
            range_start: result.range.start,
            range_end: result.range.end,
            result_sha256: Sha256::digest(result.text.as_bytes()).into(),
        });
    });
}

async fn canonical_read_path(path: &Path) -> Option<String> {
    tokio::fs::canonicalize(path)
        .await
        .ok()?
        .into_os_string()
        .into_string()
        .ok()
}

async fn confirm_read_path(path: &Path, initial: Option<String>) -> Option<String> {
    let initial = initial?;
    (canonical_read_path(path).await.as_ref() == Some(&initial)).then_some(initial)
}

async fn confirm_directory_path(
    path: &Path,
    initial: Option<String>,
    initial_modified: Option<std::time::SystemTime>,
) -> Option<String> {
    let initial_modified = initial_modified?;
    let canonical = confirm_read_path(path, initial).await?;
    let after = tokio::fs::metadata(path).await.ok()?;
    (after.is_dir() && after.modified().ok() == Some(initial_modified)).then_some(canonical)
}

fn file_version(stable: &read_tracker::StableFileRead) -> [u8; 32] {
    let (size, modified, content_digest) = stable.version_parts();
    let mut digest = Sha256::new();
    digest.update(size.to_le_bytes());
    digest.update(content_digest);
    if let Some(modified) = modified {
        digest.update(modified.to_le_bytes());
    }
    digest.finalize().into()
}

fn directory_version(entries: &[String]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for entry in entries {
        digest.update((entry.len() as u64).to_le_bytes());
        digest.update(entry.as_bytes());
    }
    digest.finalize().into()
}

const BLOCKED_DEVICE_PATHS: &[&str] = &[
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/full",
    "/dev/stdin",
    "/dev/tty",
    "/dev/console",
    "/dev/stdout",
    "/dev/stderr",
    "/dev/fd/0",
    "/dev/fd/1",
    "/dev/fd/2",
];

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
struct ReadArgs {
    /// The absolute path to the file or directory to read
    file_path: String,
    /// The line offset to start reading from. Omit when you want the full file or directory listing.
    #[serde(default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    offset: Option<usize>,
    /// The maximum number of lines or directory entries to read. Omit for the full result when safe.
    #[serde(default)]
    #[schemars(
        schema_with = "parameter_schema::number",
        skip_serializing_if = "Option::is_none"
    )]
    limit: Option<usize>,
}

pub struct ReadTool;

impl ReadTool {
    pub fn new() -> Self {
        Self
    }

    fn is_blocked_device_path(path: &Path) -> bool {
        let display = path.to_string_lossy();
        if BLOCKED_DEVICE_PATHS
            .iter()
            .any(|blocked| display == *blocked)
        {
            return true;
        }

        display.starts_with("/proc/")
            && (display.ends_with("/fd/0")
                || display.ends_with("/fd/1")
                || display.ends_with("/fd/2"))
    }
}

impl Default for ReadTool {
    fn default() -> Self {
        Self::new()
    }
}

fn slice_bounds(total: usize, offset: usize, limit: Option<usize>) -> (usize, usize) {
    let start = offset.min(total);
    let end = limit
        .map(|value| start.saturating_add(value).min(total))
        .unwrap_or(total);
    (start, end)
}

fn continuation_hint(
    noun: &str,
    start: usize,
    end: usize,
    total: usize,
    limit: Option<usize>,
) -> Option<String> {
    if end >= total {
        return None;
    }

    let shown = end.saturating_sub(start);
    let limit_fragment = match limit {
        Some(value) => format!(", limit={value}"),
        None => String::new(),
    };

    if shown == 0 {
        return Some(format!(
            "[TRUNCATED] No {noun} returned. Continue with offset={end}{limit_fragment}"
        ));
    }

    Some(format!(
        "[TRUNCATED] Showing {noun} {first}-{end} of {total}. Continue with offset={end}{limit_fragment}",
        first = start + 1
    ))
}

struct RenderedRead {
    text: String,
    range: Range<usize>,
}

fn render_read_entries(
    entries: &[impl AsRef<str>],
    offset: usize,
    limit: Option<usize>,
    noun: &str,
) -> RenderedRead {
    let (start, end) = slice_bounds(entries.len(), offset, limit);
    let mut text = entries[start..end]
        .iter()
        .enumerate()
        .map(|(idx, entry)| format!("{:>6}\t{}", start + idx + 1, entry.as_ref()))
        .collect::<Vec<_>>()
        .join("\n");

    if let Some(hint) = continuation_hint(noun, start, end, entries.len(), limit) {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&hint);
    }

    RenderedRead {
        text,
        range: start..end,
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "Read a local file or directory with line-numbered output (supports offset/limit). Use this before Edit/Write on existing files. Safe for text files and directories; binary files are omitted and blocking device paths are rejected."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    fn parameters_schema(&self) -> serde_json::Value {
        parameter_schema::for_arguments::<ReadArgs>()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: ReadArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Read args: {}", e)))?;

        let path = Path::new(parsed.file_path.trim());
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }
        if Self::is_blocked_device_path(path) {
            return Err(ToolError::InvalidArguments(format!(
                "Refusing to read blocking or unbounded device path: {}",
                path.display()
            )));
        }

        let initial_path = if observing_read(&ctx.tool_call_id) {
            canonical_read_path(path).await
        } else {
            None
        };

        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read path: {}", e)))?;

        if metadata.is_dir() {
            let mut dir = tokio::fs::read_dir(path)
                .await
                .map_err(|e| ToolError::Execution(format!("Failed to read directory: {}", e)))?;
            let mut entries = Vec::new();
            let mut lossless_names = initial_path.is_some();
            while let Some(entry) = dir
                .next_entry()
                .await
                .map_err(|e| ToolError::Execution(format!("Failed to iterate directory: {}", e)))?
            {
                let file_name = entry.file_name();
                if lossless_names && file_name.to_str().is_none() {
                    lossless_names = false;
                }
                let mut name = file_name.to_string_lossy().into_owned();
                if entry
                    .file_type()
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to inspect entry: {}", e)))?
                    .is_dir()
                {
                    name.push('/');
                }
                entries.push(name);
            }
            entries.sort();

            let rendered = render_read_entries(
                &entries,
                parsed.offset.unwrap_or(0),
                parsed.limit,
                "entries",
            );
            if let Some(canonical_path) = confirm_directory_path(
                path,
                initial_path.filter(|_| lossless_names),
                metadata.modified().ok(),
            )
            .await
            {
                record_read_result(
                    &ctx.tool_call_id,
                    canonical_path,
                    ReadSourceKind::Directory,
                    directory_version(&entries),
                    &rendered,
                );
            }
            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: rendered.text,
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }

        if metadata.len() > MAX_TRACKED_FILE_SIZE {
            return Err(ToolError::Execution(format!(
                "File is {} bytes, which exceeds the maximum readable size of {} bytes ({} MB). \
                 Use Grep to search within this file instead.",
                metadata.len(),
                MAX_TRACKED_FILE_SIZE,
                MAX_TRACKED_FILE_SIZE / 1024 / 1024
            )));
        }

        let stable = read_tracker::stable_read(parsed.file_path.trim())
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read stable file: {}", e)))?;

        if stable.bytes().len() as u64 > MAX_TRACKED_FILE_SIZE {
            return Err(ToolError::Execution(format!(
                "File is {} bytes, which exceeds the maximum readable size of {} bytes ({} MB). \
                 Use Grep to search within this file instead.",
                stable.bytes().len(),
                MAX_TRACKED_FILE_SIZE,
                MAX_TRACKED_FILE_SIZE / 1024 / 1024
            )));
        }

        if let Some(session_id) = ctx.session_id() {
            read_tracker::mark_stable_read(session_id, parsed.file_path.trim(), &stable).await;
        }

        let bytes = stable.bytes();

        if bytes.contains(&0) {
            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "[Binary file omitted]".to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }

        let content = String::from_utf8_lossy(bytes);
        let lines: Vec<_> = content.lines().collect();
        let rendered =
            render_read_entries(&lines, parsed.offset.unwrap_or(0), parsed.limit, "lines");
        if let Some(canonical_path) = confirm_read_path(path, initial_path).await {
            record_read_result(
                &ctx.tool_call_id,
                canonical_path,
                ReadSourceKind::File,
                file_version(&stable),
                &rendered,
            );
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: rendered.text,
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::WriteTool;
    use serde_json::json;

    #[test]
    fn read_args_preserve_optional_defaults_and_unknown_fields() {
        for args in [
            json!({"file_path": "/tmp/read.txt"}),
            json!({
                "file_path": "/tmp/read.txt",
                "offset": null,
                "limit": null,
                "unknown": true
            }),
        ] {
            let parsed: ReadArgs = serde_json::from_value(args).unwrap();
            assert_eq!(parsed.file_path, "/tmp/read.txt");
            assert_eq!(parsed.offset, None);
            assert_eq!(parsed.limit, None);
        }
    }

    #[test]
    fn read_args_preserve_invalid_argument_rejection() {
        for field in ["offset", "limit"] {
            for invalid in [json!(-1), json!(1.5), json!("1"), json!(true)] {
                let mut args = json!({"file_path": "/tmp/read.txt"});
                args[field] = invalid;
                assert!(serde_json::from_value::<ReadArgs>(args).is_err());
            }
        }
        for args in [json!({}), json!({"file_path": null})] {
            assert!(serde_json::from_value::<ReadArgs>(args).is_err());
        }
    }

    #[tokio::test]
    async fn binary_read_still_marks_file_as_read_for_session_write_gate() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), vec![0_u8, 1, 2, 3])
            .await
            .unwrap();
        let file_path = file.path().to_string_lossy().to_string();
        let make_ctx = || ToolCtx {
            executing_caller: None,
            executing_supervisor: None,
            session_id: Some(std::sync::Arc::from("session_binary_read")),
            tool_call_id: std::sync::Arc::from("call_1"),
            event_tx: None,
            available_tool_schemas: std::sync::Arc::from(Vec::new()),
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            async_completion_sink: None,
            bash_completion_sink: None,
        };

        let read_tool = ReadTool::new();
        let (read_out, identity) = observe_read_result(
            "call_1",
            read_tool.invoke(json!({ "file_path": file_path }), make_ctx()),
        )
        .await;
        assert!(identity.is_none());
        let ToolOutcome::Completed(read_result) = read_out.unwrap() else {
            panic!("expected Completed")
        };
        assert!(read_result.success);
        assert!(read_result.result.contains("Binary file omitted"));

        let write_tool = WriteTool::new();
        let write_out = write_tool
            .invoke(
                json!({
                    "file_path": file.path(),
                    "content": "now text"
                }),
                make_ctx(),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(write_result) = write_out else {
            panic!("expected Completed")
        };
        assert!(write_result.success);
    }

    async fn observed_read(
        call_id: &str,
        ctx_call_id: &str,
        args: serde_json::Value,
    ) -> (String, Option<ReadResultIdentity>) {
        let tool = ReadTool::new();
        let (out, identity) =
            observe_read_result(call_id, tool.invoke(args, ToolCtx::none(ctx_call_id))).await;
        let ToolOutcome::Completed(result) = out.unwrap() else {
            panic!("expected Completed")
        };
        assert!(result.success);
        (result.result, identity)
    }

    #[tokio::test]
    async fn read_observation_uses_stable_file_version_and_effective_range() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        tokio::fs::write(path, "line 1\nline 2\nold\n")
            .await
            .unwrap();

        let (full_text, full) = observed_read("full", "full", json!({"file_path": path})).await;
        let full = full.unwrap();
        let (clamped_text, clamped) = observed_read(
            "clamped",
            "clamped",
            json!({"file_path": path, "offset": 0, "limit": 1000}),
        )
        .await;
        assert_eq!(full_text, "     1\tline 1\n     2\tline 2\n     3\told");
        assert_eq!(clamped_text, full_text);
        assert_eq!(clamped.as_ref(), Some(&full));
        assert_eq!(full.source_kind, ReadSourceKind::File);
        assert_eq!((full.range_start, full.range_end), (0, 3));
        assert_eq!(
            full.canonical_path,
            std::fs::canonicalize(path).unwrap().to_str().unwrap()
        );
        let wire: ReadResultIdentity =
            serde_json::from_value(serde_json::to_value(&full).unwrap()).unwrap();
        assert_eq!(wire, full);
        assert!(wire.matches_full_result(&full_text));
        assert!(!wire.matches_full_result("     1\tline 1"));

        let args = json!({"file_path": path, "limit": 1});
        let (before_text, before) = observed_read("before", "before", args.clone()).await;
        let before = before.unwrap();
        assert_eq!((before.range_start, before.range_end), (0, 1));
        assert_eq!(before.source_version, full.source_version);
        assert_ne!(before, full);
        tokio::fs::write(path, "line 1\nline 2\nnew\n")
            .await
            .unwrap();
        let (after_text, after) = observed_read("after", "after", args.clone()).await;
        let after = after.unwrap();
        assert_eq!(after_text, before_text);
        assert_eq!(after.result_sha256, before.result_sha256);
        assert_ne!(after.source_version, before.source_version);
        assert_ne!(after, before);

        let (unmatched_text, unmatched) = observed_read("observer", "other-call", args).await;
        assert_eq!(unmatched_text, before_text);
        assert!(unmatched.is_none());
    }

    #[tokio::test]
    async fn read_observation_uses_sorted_directory_snapshot_and_effective_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        tokio::fs::create_dir(path.join("z-dir")).await.unwrap();
        tokio::fs::write(path.join("a.txt"), "a").await.unwrap();
        let args = json!({"file_path": path, "offset": 0, "limit": 1});
        let (before_text, before) = observed_read("before", "before", args.clone()).await;
        let before = before.unwrap();
        let (repeat_text, repeat) = observed_read("repeat", "repeat", args.clone()).await;
        assert_eq!(repeat.as_ref(), Some(&before));
        assert_eq!(repeat_text, before_text);
        assert!(before_text.starts_with("     1\ta.txt\n"));
        assert_eq!(before.source_kind, ReadSourceKind::Directory);
        assert_eq!((before.range_start, before.range_end), (0, 1));

        tokio::fs::rename(path.join("z-dir"), path.join("y-dir"))
            .await
            .unwrap();
        let (after_text, after) = observed_read("after", "after", args).await;
        let after = after.unwrap();
        assert_eq!(after_text, before_text);
        assert_eq!(after.result_sha256, before.result_sha256);
        assert_ne!(after.source_version, before.source_version);
        assert_ne!(after, before);
    }

    #[tokio::test]
    async fn read_directory_supports_offset_limit_and_marks_subdirs() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("b-dir"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("a.txt"), "a")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("c.txt"), "c")
            .await
            .unwrap();

        let tool = ReadTool::new();
        let out = tool
            .invoke(
                json!({
                    "file_path": dir.path(),
                    "offset": 1,
                    "limit": 1
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        assert!(result.result.contains("b-dir/"));
        assert!(result.result.contains("TRUNCATED"));
    }

    #[tokio::test]
    async fn read_file_adds_continuation_hint_when_truncated() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "l1\nl2\nl3\n").await.unwrap();

        let tool = ReadTool::new();
        let out = tool
            .invoke(
                json!({
                    "file_path": file.path(),
                    "offset": 0,
                    "limit": 1
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        assert!(result.result.contains("l1"));
        assert!(result.result.contains("Continue with offset=1"));
    }

    #[tokio::test]
    async fn read_rejects_blocking_device_paths() {
        let tool = ReadTool::new();
        let result = tool
            .invoke(
                json!({
                    "file_path": "/dev/stdin"
                }),
                ToolCtx::none("t"),
            )
            .await;

        let error = result.expect_err("device path should be rejected");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        assert!(error
            .to_string()
            .contains("Refusing to read blocking or unbounded device path"));
    }
}
