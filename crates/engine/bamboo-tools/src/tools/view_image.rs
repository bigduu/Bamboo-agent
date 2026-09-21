use async_trait::async_trait;
use bamboo_agent_core::{
    Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolResultImage,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::read_tracker::{self, MAX_TRACKED_FILE_SIZE};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewImageArgs {
    path: String,
}

/// Read a local raster image and return it through Bamboo's multimodal tool-result channel.
pub struct ViewImageTool;

impl ViewImageTool {
    pub fn new() -> Self {
        Self
    }

    fn supported_mime(bytes: &[u8]) -> Option<&'static str> {
        if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
            Some("image/png")
        } else if bytes.starts_with(b"\xff\xd8\xff") {
            Some("image/jpeg")
        } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
            Some("image/gif")
        } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
            Some("image/webp")
        } else {
            None
        }
    }
}

impl Default for ViewImageTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ViewImageTool {
    fn name(&self) -> &str {
        "ViewImage"
    }

    fn description(&self) -> &str {
        "View a local PNG, JPEG, GIF, or WebP image. Returns the image as base64 multimodal content; when hooks.image_fallback is enabled in vision mode, Bamboo uses the resolved vision model to replace it with a detailed description before the next model turn."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to a local PNG, JPEG, GIF, or WebP image"
                }
            },
            "required": ["path"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: ViewImageArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid ViewImage args: {error}"))
        })?;
        let raw_path = parsed.path.trim();
        if raw_path.is_empty() {
            return Err(ToolError::InvalidArguments(
                "path must be a non-empty absolute path".to_string(),
            ));
        }

        let path = Path::new(raw_path);
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "path must be an absolute path".to_string(),
            ));
        }

        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|error| ToolError::Execution(format!("Failed to inspect image: {error}")))?;
        if !metadata.is_file() {
            return Err(ToolError::InvalidArguments(format!(
                "Image path must point to a regular file: {}",
                path.display()
            )));
        }
        if metadata.len() > MAX_TRACKED_FILE_SIZE {
            return Err(ToolError::Execution(format!(
                "Image is {} bytes, which exceeds the maximum ViewImage size of {} bytes ({} MB)",
                metadata.len(),
                MAX_TRACKED_FILE_SIZE,
                MAX_TRACKED_FILE_SIZE / 1024 / 1024
            )));
        }

        let stable = read_tracker::stable_read(raw_path).await.map_err(|error| {
            ToolError::Execution(format!("Failed to read stable image: {error}"))
        })?;
        let bytes = stable.bytes();
        let mime_type = Self::supported_mime(bytes).ok_or_else(|| {
            ToolError::InvalidArguments(
                "Unsupported image format; expected PNG, JPEG, GIF, or WebP content".to_string(),
            )
        })?;

        if let Some(session_id) = ctx.session_id() {
            read_tracker::mark_stable_read(session_id, raw_path, &stable).await;
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "path": raw_path,
                "mime_type": mime_type,
                "size_bytes": bytes.len(),
                "delivery": "base64_image"
            })
            .to_string(),
            display_preference: None,
            images: vec![ToolResultImage {
                mime_type: mime_type.to_string(),
                data: STANDARD.encode(bytes),
            }],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn view_image_returns_detected_base64_image() {
        let file = tempfile::Builder::new().suffix(".bin").tempfile().unwrap();
        let bytes = b"\x89PNG\r\n\x1a\nview-image-test";
        tokio::fs::write(file.path(), bytes).await.unwrap();

        let output = ViewImageTool::new()
            .invoke(
                json!({"path": file.path().to_string_lossy()}),
                ToolCtx::none("view-image"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = output else {
            panic!("expected completed ViewImage result");
        };

        assert!(result.success);
        assert_eq!(result.images.len(), 1);
        assert_eq!(result.images[0].mime_type, "image/png");
        assert_eq!(result.images[0].data, STANDARD.encode(bytes));
        assert!(!result.result.contains(&result.images[0].data));
    }

    #[tokio::test]
    async fn view_image_rejects_relative_and_non_image_paths() {
        let relative = ViewImageTool::new()
            .invoke(json!({"path": "image.png"}), ToolCtx::none("relative"))
            .await;
        assert!(matches!(relative, Err(ToolError::InvalidArguments(_))));

        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), b"plain text").await.unwrap();
        let non_image = ViewImageTool::new()
            .invoke(
                json!({"path": file.path().to_string_lossy()}),
                ToolCtx::none("non-image"),
            )
            .await;
        assert!(matches!(non_image, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn view_image_rejects_oversized_files_before_reading() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(MAX_TRACKED_FILE_SIZE + 1).unwrap();

        let result = ViewImageTool::new()
            .invoke(
                json!({"path": file.path().to_string_lossy()}),
                ToolCtx::none("oversized"),
            )
            .await;
        assert!(matches!(result, Err(ToolError::Execution(_))));
    }
}
