use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::read_tracker::{BaselineAdvance, ReadState};
use super::{content_diagnostics, file_change, read_tracker};

#[derive(Debug, Deserialize)]
struct WriteArgs {
    file_path: String,
    content: String,
}

pub struct WriteTool;

impl WriteTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WriteTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "Write"
    }

    fn description(&self) -> &str {
        "Write a local file (create or replace full content). IMPORTANT: for existing files, call Read first in this session or Write will fail."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to write"
                },
                "content": {
                    "type": "string",
                    "description": "The content to write to the file"
                }
            },
            "required": ["file_path", "content"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: WriteArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Write args: {}", e)))?;

        let file_path = parsed.file_path.trim();
        let path = Path::new(file_path);

        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        let session_id = ctx.session_id().map(str::to_owned);
        let target_existed = tokio::fs::try_exists(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to inspect target file: {}", e)))?;
        let validated_read = if target_existed {
            if let Some(session_id) = session_id.as_deref() {
                match read_tracker::read_if_fresh(session_id, file_path).await {
                    Ok(validated) => Some(validated),
                    Err(ReadState::Unread) => {
                        return Err(ToolError::Execution(
                            "Write requires reading the target file first via Read".to_string(),
                        ));
                    }
                    Err(ReadState::Stale) => {
                        return Err(ToolError::Execution(
                            "Target file changed after last Read; call Read again before Write"
                                .to_string(),
                        ));
                    }
                    Err(ReadState::Fresh) => {
                        unreachable!("Fresh is returned as a validated read")
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        let new_file_slot =
            if let (Some(session_id), false) = (session_id.as_deref(), target_existed) {
                Some(read_tracker::capture_write_slot(session_id, file_path).await)
            } else {
                None
            };

        let previous_bytes = if let Some(validated) = validated_read.as_ref() {
            Some(validated.bytes().to_vec())
        } else if target_existed {
            file_change::read_existing_bytes(path).await?
        } else {
            None
        };
        let checkpoint = file_change::create_checkpoint(path, previous_bytes.as_deref()).await?;
        let next_content = parsed.content;

        let write_expectation = if let Some(validated) = validated_read.as_ref() {
            file_change::AtomicWriteExpectation::Exact(validated.bytes())
        } else if session_id.is_some() && !target_existed {
            file_change::AtomicWriteExpectation::Missing
        } else {
            file_change::AtomicWriteExpectation::Unchecked
        };
        file_change::atomic_write_text_with_expectation(path, &next_content, write_expectation)
            .await?;

        let mutation_slot = validated_read
            .as_ref()
            .map(|validated| validated.slot())
            .or(new_file_slot.as_ref());
        if let Some(slot) = mutation_slot {
            if read_tracker::advance_after_verified_write(file_path, slot, next_content.as_bytes())
                .await
                == BaselineAdvance::Conflict
            {
                return Err(ToolError::Execution(
                    "Write committed, but the target changed before it could be verified; call Read again"
                        .to_string(),
                ));
            }
        }

        let previous_text = file_change::bytes_to_lossy_text(previous_bytes.as_deref());
        let mut payload = file_change::build_file_change_payload_value(
            "Write",
            path,
            format!("Wrote file: {}", file_path),
            checkpoint,
            &previous_text,
            &next_content,
        );
        content_diagnostics::attach_file_diagnostics(&mut payload, path, &next_content);

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("Default".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ReadTool;
    use serde_json::json;

    fn ctx(session_id: &str) -> ToolCtx {
        ToolCtx {
            session_id: Some(std::sync::Arc::from(session_id)),
            tool_call_id: std::sync::Arc::from("call_1"),
            event_tx: None,
            available_tool_schemas: std::sync::Arc::from(Vec::new()),
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            async_completion_sink: None,
            bash_completion_sink: None,
        }
    }

    #[tokio::test]
    async fn write_requires_fresh_read_for_existing_files() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let write_tool = WriteTool::new();
        let read_tool = ReadTool::new();

        let denied = write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "v2"}),
                ctx("session_a"),
            )
            .await;
        assert!(matches!(denied, Err(ToolError::Execution(_))));

        let _ = read_tool
            .invoke(json!({"file_path": file.path()}), ctx("session_a"))
            .await
            .unwrap();

        tokio::fs::write(file.path(), "external change")
            .await
            .unwrap();

        let stale = write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "v3"}),
                ctx("session_a"),
            )
            .await;
        assert!(matches!(stale, Err(ToolError::Execution(msg)) if msg.contains("changed")));

        let _ = read_tool
            .invoke(json!({"file_path": file.path()}), ctx("session_a"))
            .await
            .unwrap();
        let out = write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "final"}),
                ctx("session_a"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(ok) = out else {
            panic!("expected Completed")
        };
        assert!(ok.success);
    }

    #[tokio::test]
    async fn read_write_write_succeeds_without_an_external_change() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let session = format!("write-twice-{}", uuid::Uuid::new_v4());
        let read_tool = ReadTool::new();
        let write_tool = WriteTool::new();

        read_tool
            .invoke(json!({"file_path": file.path()}), ctx(&session))
            .await
            .unwrap();
        write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "v2"}),
                ctx(&session),
            )
            .await
            .unwrap();
        write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "v3"}),
                ctx(&session),
            )
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(file.path()).await.unwrap(), "v3");
    }

    #[tokio::test]
    async fn write_rejects_external_change_after_a_successful_write() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "aa").await.unwrap();
        let session = format!("write-external-{}", uuid::Uuid::new_v4());
        let read_tool = ReadTool::new();
        let write_tool = WriteTool::new();

        read_tool
            .invoke(json!({"file_path": file.path()}), ctx(&session))
            .await
            .unwrap();
        write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "bb"}),
                ctx(&session),
            )
            .await
            .unwrap();

        tokio::fs::write(file.path(), "cc").await.unwrap();
        let stale = write_tool
            .invoke(
                json!({"file_path": file.path(), "content": "dd"}),
                ctx(&session),
            )
            .await;

        assert!(matches!(stale, Err(ToolError::Execution(message)) if message.contains("changed")));
        assert_eq!(tokio::fs::read_to_string(file.path()).await.unwrap(), "cc");
    }

    #[tokio::test]
    async fn stale_write_failure_does_not_advance_the_baseline() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let session = format!("write-stale-failure-{}", uuid::Uuid::new_v4());
        let read_tool = ReadTool::new();
        let write_tool = WriteTool::new();

        read_tool
            .invoke(json!({"file_path": file.path()}), ctx(&session))
            .await
            .unwrap();
        tokio::fs::write(file.path(), "external").await.unwrap();

        for intended in ["first-attempt", "second-attempt"] {
            let stale = write_tool
                .invoke(
                    json!({"file_path": file.path(), "content": intended}),
                    ctx(&session),
                )
                .await;
            assert!(
                matches!(stale, Err(ToolError::Execution(message)) if message.contains("changed"))
            );
        }
        assert_eq!(
            tokio::fs::read_to_string(file.path()).await.unwrap(),
            "external"
        );
    }

    #[tokio::test]
    async fn consecutive_writes_to_a_new_file_use_the_verified_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.txt");
        let session = format!("write-new-twice-{}", uuid::Uuid::new_v4());
        let write_tool = WriteTool::new();

        write_tool
            .invoke(
                json!({"file_path": path, "content": "first"}),
                ctx(&session),
            )
            .await
            .unwrap();
        write_tool
            .invoke(
                json!({"file_path": path, "content": "second"}),
                ctx(&session),
            )
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "second");
    }

    #[tokio::test]
    async fn concurrent_read_of_new_file_is_idempotent_for_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new-concurrent.txt");
        let path_str = path.to_string_lossy().into_owned();
        let session = format!("write-new-concurrent-{}", uuid::Uuid::new_v4());
        let (advance_reached, resume_advance) =
            read_tracker::pause_next_advance_for_test(&session, &path_str).await;

        let writer_path = path.clone();
        let writer_session = session.clone();
        let writer = tokio::spawn(async move {
            WriteTool::new()
                .invoke(
                    json!({"file_path": writer_path, "content": "first"}),
                    ctx(&writer_session),
                )
                .await
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            advance_reached.notified(),
        )
        .await
        .expect("Write did not reach post-write baseline advancement");
        ReadTool::new()
            .invoke(json!({"file_path": path}), ctx(&session))
            .await
            .unwrap();
        resume_advance.notify_one();

        let first = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("Write did not resume")
            .unwrap()
            .unwrap();
        assert!(matches!(first, ToolOutcome::Completed(result) if result.success));

        WriteTool::new()
            .invoke(
                json!({"file_path": path, "content": "second"}),
                ctx(&session),
            )
            .await
            .unwrap();
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "second");
    }

    #[tokio::test]
    async fn committed_postverify_conflict_is_clear_and_leaves_baseline_stale() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "before").await.unwrap();
        let path = file.path().to_path_buf();
        let path_str = path.to_string_lossy().into_owned();
        let session = format!("write-postverify-conflict-{}", uuid::Uuid::new_v4());

        ReadTool::new()
            .invoke(json!({"file_path": path}), ctx(&session))
            .await
            .unwrap();
        let (advance_reached, resume_advance) =
            read_tracker::pause_next_advance_for_test(&session, &path_str).await;

        let writer_path = path.clone();
        let writer_session = session.clone();
        let writer = tokio::spawn(async move {
            WriteTool::new()
                .invoke(
                    json!({"file_path": writer_path, "content": "intended"}),
                    ctx(&writer_session),
                )
                .await
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            advance_reached.notified(),
        )
        .await
        .expect("Write did not reach post-write baseline advancement");
        tokio::fs::write(&path, "other").await.unwrap();
        ReadTool::new()
            .invoke(json!({"file_path": path}), ctx(&session))
            .await
            .unwrap();
        tokio::fs::write(&path, "intended").await.unwrap();
        resume_advance.notify_one();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("Write did not resume")
            .unwrap();
        assert!(
            matches!(result, Err(ToolError::Execution(message)) if message.contains("Write committed"))
        );
        assert_eq!(
            read_tracker::read_state(&session, &path_str).await,
            ReadState::Stale
        );
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "intended");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_rejects_symlinked_path_components() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        let link = dir.path().join("link");
        tokio::fs::create_dir_all(&real).await.unwrap();
        symlink(&real, &link).unwrap();

        let write_tool = WriteTool::new();
        let result = write_tool
            .invoke(
                json!({
                    "file_path": link.join("test.txt"),
                    "content": "hello"
                }),
                ToolCtx::none("t"),
            )
            .await;
        assert!(matches!(result, Err(ToolError::Execution(msg)) if msg.contains("symlinked")));
    }

    #[tokio::test]
    async fn write_includes_json_diagnostics_for_invalid_content() {
        let file = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
        let write_tool = WriteTool::new();

        let out = write_tool
            .invoke(
                json!({
                    "file_path": file.path(),
                    "content": "{"
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["diagnostics"]["format"], "json");
        assert_eq!(payload["diagnostics"]["valid"], false);
    }
}
