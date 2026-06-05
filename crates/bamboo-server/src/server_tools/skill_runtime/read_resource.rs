use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use bamboo_engine::access_control;
use bamboo_engine::resource_helpers::{
    display_relative_path, normalize_relative_resource_path, page_text_lines, truncate_text,
};
use bamboo_engine::runtime_metadata::LAST_RESOURCE_READ_SUMMARY_METADATA_KEY;
use bamboo_engine::SkillManager;
use bamboo_infrastructure::Config;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_infrastructure::LockedSessionStore;

use super::{skill_access_error_to_tool_error, SkillToolAccess, MAX_RESOURCE_CONTENT_CHARS};

#[derive(Debug, Deserialize)]
struct ReadSkillResourceArgs {
    skill_id: String,
    resource_path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

pub struct ReadSkillResourceTool {
    access: SkillToolAccess,
}

impl ReadSkillResourceTool {
    pub fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: bamboo_engine::SessionCache,
        storage: Arc<dyn Storage>,
        persistence: Arc<LockedSessionStore>,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(skill_manager, config, sessions, storage, persistence),
        }
    }
}

#[async_trait]
impl Tool for ReadSkillResourceTool {
    fn name(&self) -> &str {
        "read_skill_resource"
    }

    fn description(&self) -> &str {
        "Read a resource file under a skill directory by relative resource_path."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "skill_id": {
                    "type": "string",
                    "description": "Skill ID that owns the resource."
                },
                "resource_path": {
                    "type": "string",
                    "description": "Relative path inside the skill folder (for example: references/policies.md)."
                },
                "offset": {
                    "type": "number",
                    "description": "Optional 0-based line offset for paged text reads."
                },
                "limit": {
                    "type": "number",
                    "description": "Optional line limit for paged text reads."
                }
            },
            "required": ["skill_id", "resource_path"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ReadSkillResourceArgs = serde_json::from_value(args).map_err(|err| {
            ToolError::InvalidArguments(format!("Invalid read_skill_resource args: {err}"))
        })?;
        let skill_id = parsed.skill_id.trim();
        if skill_id.is_empty() {
            return Err(ToolError::InvalidArguments(
                "skill_id must be a non-empty string".to_string(),
            ));
        }

        access_control::ensure_skill_allowed(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
        access_control::ensure_skill_loaded(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
        let skill_mode = access_control::selected_skill_mode(&self.access, ctx.session_id).await;

        let resource_path = normalize_relative_resource_path(&parsed.resource_path)
            .map_err(ToolError::InvalidArguments)?;
        if resource_path == Path::new("SKILL.md") {
            return Err(ToolError::InvalidArguments(
                "Use load_skill for SKILL.md instructions; read_skill_resource is for auxiliary files"
                    .to_string(),
            ));
        }

        let skill_root = self
            .access
            .skill_root(skill_id, skill_mode.as_deref())
            .await?;
        let canonical_root = tokio::fs::canonicalize(&skill_root).await.map_err(|_| {
            ToolError::Execution(format!(
                "Skill directory not found for '{skill_id}'. Load the skill list first."
            ))
        })?;
        let canonical_resource = tokio::fs::canonicalize(skill_root.join(&resource_path))
            .await
            .map_err(|_| {
                ToolError::Execution(format!(
                    "Skill resource not found: {}/{}",
                    skill_id,
                    display_relative_path(&resource_path)
                ))
            })?;

        if !canonical_resource.starts_with(&canonical_root) {
            return Err(ToolError::InvalidArguments(
                "resource_path must stay inside the skill directory".to_string(),
            ));
        }

        let metadata = tokio::fs::metadata(&canonical_resource)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to stat resource: {err}")))?;
        if !metadata.is_file() {
            return Err(ToolError::InvalidArguments(format!(
                "resource_path must reference a file: {}",
                display_relative_path(&resource_path)
            )));
        }

        let bytes = tokio::fs::read(&canonical_resource)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to read skill resource: {err}")))?;
        let size_bytes = bytes.len();

        let result = match String::from_utf8(bytes) {
            Ok(text) => {
                let offset = parsed.offset.unwrap_or(0);
                let (paged, start, end, total_lines) = page_text_lines(&text, offset, parsed.limit);
                let (excerpt, truncated) = truncate_text(&paged, MAX_RESOURCE_CONTENT_CHARS);
                let has_more = end < total_lines;
                let summary = json!({
                    "skill_id": skill_id,
                    "resource_path": display_relative_path(&resource_path),
                    "offset": start,
                    "limit": parsed.limit,
                    "returned_lines": end.saturating_sub(start),
                    "total_lines": total_lines,
                    "has_more": has_more,
                    "truncated": truncated,
                    "binary": false
                });
                if let Some(session_id) = ctx.session_id {
                    if let Some(mut session) =
                        self.access.session_for_context(Some(session_id)).await
                    {
                        session.metadata.insert(
                            LAST_RESOURCE_READ_SUMMARY_METADATA_KEY.to_string(),
                            summary.to_string(),
                        );
                        let _ = self
                            .access
                            .persistence
                            .merge_save_runtime(&mut session)
                            .await;
                        self.access.sessions.insert(
                            session_id.to_string(),
                            std::sync::Arc::new(parking_lot::RwLock::new(session)),
                        );
                    }
                }
                json!({
                    "skill_id": skill_id,
                    "resource_path": display_relative_path(&resource_path),
                    "size_bytes": size_bytes,
                    "offset": start,
                    "limit": parsed.limit,
                    "returned_lines": end.saturating_sub(start),
                    "total_lines": total_lines,
                    "has_more": has_more,
                    "next_offset": if has_more { Some(end) } else { None::<usize> },
                    "truncated": truncated,
                    "content": excerpt
                })
            }
            Err(_) => json!({
                "skill_id": skill_id,
                "resource_path": display_relative_path(&resource_path),
                "size_bytes": size_bytes,
                "binary": true,
                "message": "Resource is not UTF-8 text. Use file tools when binary handling is required."
            }),
        };

        Ok(ToolResult {
            success: true,
            result: result.to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}
