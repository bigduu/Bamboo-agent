use async_trait::async_trait;
use bamboo_agent_core::{
    Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolResultImage,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Deserialize;
use serde_json::json;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::parameter_schema;
use super::read_tracker::{self, MAX_TRACKED_FILE_SIZE};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ViewImageArgs {
    /// Absolute local image path or exact image reference supplied by this conversation
    path: String,
}

/// Host-owned reader for references from the executing Session's original image parts.
/// It is scoped to dispatch, never deserialized from model arguments or persisted.
pub type ViewImageReferenceReader =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<u8>, ToolError>> + Send + Sync>;

pub const MAX_VIEW_IMAGE_BYTES: usize = MAX_TRACKED_FILE_SIZE as usize;

struct ImageReferenceScope {
    session_id: String,
    reader: ViewImageReferenceReader,
}

tokio::task_local! {
    static IMAGE_REFERENCES: ImageReferenceScope;
}

/// Keep reference access inside the same inline tool dispatch and cancellation scope.
pub async fn with_image_reference_reader<F: Future>(
    session_id: &str,
    reader: ViewImageReferenceReader,
    dispatch: F,
) -> F::Output {
    IMAGE_REFERENCES
        .scope(
            ImageReferenceScope {
                session_id: session_id.to_string(),
                reader,
            },
            dispatch,
        )
        .await
}

fn is_image_reference(path: &str) -> bool {
    path.starts_with("bamboo-attachment://") || path.starts_with("bamboo-image://")
}

async fn read_image_reference(ctx: &ToolCtx, reference: &str) -> Result<Vec<u8>, ToolError> {
    let reader = IMAGE_REFERENCES
        .try_with(|scope| {
            (ctx.session_id() == Some(scope.session_id.as_str())).then(|| scope.reader.clone())
        })
        .ok()
        .flatten()
        .ok_or_else(|| {
            ToolError::Execution(
                "Image reference is unavailable outside its executing conversation. Use the exact reference from the current conversation or an authorized local image path.".into(),
            )
        })?;
    reader(reference.to_string()).await
}

/// Read a local raster image or a host-bound conversation image reference.
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
        "View a local PNG, JPEG, GIF, or WebP image, or the exact image reference supplied in this conversation. Vision-capable main models receive image content directly. For a main without Vision, an explicitly configured Vision model returns a labeled, untrusted description. Uploaded images remain preserved; only this requested tool call sends them to the configured Vision model. Missing or unsupported Vision configuration returns actionable feedback. Never claim to have inspected an image before receiving a successful result."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    fn parameters_schema(&self) -> serde_json::Value {
        parameter_schema::for_arguments::<ViewImageArgs>()
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

        let (reference_bytes, stable) = if is_image_reference(raw_path) {
            // Never pass an unavailable/forged reference to the ordinary file branch.
            (Some(read_image_reference(&ctx, raw_path).await?), None)
        } else {
            let path = Path::new(raw_path);
            if !path.is_absolute() {
                return Err(ToolError::InvalidArguments(
                    "path must be an absolute local path or a current conversation image reference"
                        .to_string(),
                ));
            }

            let metadata = tokio::fs::metadata(path).await.map_err(|error| {
                ToolError::Execution(format!("Failed to inspect image: {error}"))
            })?;
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
            (None, Some(stable))
        };
        let bytes = match (&reference_bytes, &stable) {
            (Some(bytes), _) => bytes.as_slice(),
            (_, Some(stable)) => stable.bytes(),
            _ => unreachable!("image source selected above"),
        };
        if bytes.len() > MAX_VIEW_IMAGE_BYTES {
            return Err(ToolError::Execution(format!(
                "Image exceeds the maximum ViewImage size of {MAX_VIEW_IMAGE_BYTES} bytes"
            )));
        }
        let mime_type = Self::supported_mime(bytes).ok_or_else(|| {
            ToolError::InvalidArguments(
                "Unsupported image format; expected PNG, JPEG, GIF, or WebP content".to_string(),
            )
        })?;

        if let (Some(session_id), Some(stable)) = (ctx.session_id(), stable.as_ref()) {
            read_tracker::mark_stable_read(session_id, raw_path, stable).await;
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
