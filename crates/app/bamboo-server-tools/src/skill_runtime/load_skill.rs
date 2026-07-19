use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use bamboo_llm::Config;
use bamboo_skills::access_control;
use bamboo_skills::SkillManager;

use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};

use super::{
    skill_access_error_to_tool_error, validate_runtime_activation,
    validate_runtime_activation_descriptor, SkillToolAccess,
};

#[derive(Debug, Deserialize)]
struct LoadSkillArgs {
    skill_id: String,
}

pub struct LoadSkillTool {
    access: SkillToolAccess,
}

impl LoadSkillTool {
    pub fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        session_repo: bamboo_engine::SessionRepository,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(skill_manager, config, session_repo),
        }
    }
}

#[async_trait]
impl Tool for LoadSkillTool {
    fn name(&self) -> &str {
        "load_skill"
    }

    fn description(&self) -> &str {
        "Load a skill's detailed SKILL.md instructions by skill_id."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "skill_id": {
                    "type": "string",
                    "description": "Skill ID from the advertised skill list (for example: skill-creator)."
                }
            },
            "required": ["skill_id"]
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: LoadSkillArgs = serde_json::from_value(args).map_err(|err| {
            ToolError::InvalidArguments(format!("Invalid load_skill args: {err}"))
        })?;
        let skill_id = parsed.skill_id.trim();
        if skill_id.is_empty() {
            return Err(ToolError::InvalidArguments(
                "skill_id must be a non-empty string".to_string(),
            ));
        }

        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("load_skill requires a session_id in tool context".to_string())
        })?;
        let store = self.access.skill_store(ctx.session_id()).await?;
        if !validate_runtime_activation(&self.access, store.as_ref(), session_id, skill_id).await? {
            access_control::ensure_skill_allowed(&self.access, skill_id, ctx.session_id())
                .await
                .map_err(skill_access_error_to_tool_error)?;
            let skill_mode =
                access_control::selected_skill_mode(&self.access, ctx.session_id()).await;
            let selected_ids =
                access_control::selected_skill_allowlist(&self.access, ctx.session_id())
                    .await
                    .ok_or_else(|| {
                        ToolError::Execution(
                            "load_skill cannot pin a request with no published skill selection"
                                .to_string(),
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();
            let session = self
                .access
                .session_for_context(Some(session_id))
                .await
                .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
            let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
            self.access
                .skill_manager
                .pin_current_activation_for_workspace(
                    session_id,
                    workspace.as_deref(),
                    &selected_ids,
                    skill_mode.as_deref(),
                )
                .await
                .map_err(|err| {
                    ToolError::Execution(format!(
                        "Failed to pin workflow activation for '{skill_id}': {err}"
                    ))
                })?;
        }
        let (skill, skill_root, revision, resources, payload_descriptor) = store
            .get_pinned_skill_with_root_and_descriptor(session_id, skill_id)
            .await
            .map_err(|err| {
                ToolError::Execution(format!("Failed to load skill '{skill_id}': {err}"))
            })?;
        validate_runtime_activation_descriptor(
            &self.access,
            &payload_descriptor,
            session_id,
            skill_id,
        )
        .await?;
        let canonical_skill_root = tokio::fs::canonicalize(&skill_root)
            .await
            .unwrap_or(skill_root);
        access_control::mark_skill_loaded(&self.access, skill_id, ctx.session_id())
            .await
            .map_err(skill_access_error_to_tool_error)?;

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "skill_id": skill.id,
                "revision": revision,
                "name": skill.name,
                "description": skill.description,
                "license": skill.license,
                "compatibility": skill.compatibility,
                "allowed_tools": skill.tool_refs,
                "instructions": skill.prompt,
                "skill_base_dir": bamboo_config::paths::path_to_display_string(&canonical_skill_root),
                "resource_files": resources
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}
