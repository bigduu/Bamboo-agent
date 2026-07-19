use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor,
};
use bamboo_agent_core::Session;
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
};

#[derive(Debug, Clone, Default)]
pub(super) struct SkillContextLoadResult {
    pub(super) context: String,
    pub(super) selected_skill_ids: Vec<String>,
    pub(super) selection_source: Option<String>,
    pub(super) selected_skill_mode: Option<String>,
    pub(super) request_hint_present: bool,
}

pub(super) async fn load_skill_context(
    config: &AgentLoopConfig,
    session: &Session,
    session_id: &str,
    request_hint: &str,
) -> SkillContextLoadResult {
    if let Some(skill_manager) = config.skill_manager.as_ref() {
        let selected_skills: Vec<bamboo_skills::SkillDefinition> =
            if let Some(workspace) = session.workspace_path_meta() {
                match skill_manager
                    .resolve_skills_for_request_in_workspace_with_mode(
                        std::path::Path::new(&workspace),
                        &config.disabled_skill_ids,
                        config.selected_skill_ids.as_deref(),
                        config.selected_skill_mode.as_deref(),
                        Some(request_hint),
                    )
                    .await
                {
                    Ok(skills) => skills,
                    Err(error) => {
                        tracing::warn!(
                            "[{}] Failed to resolve session workspace skills: {}",
                            session_id,
                            error
                        );
                        Vec::new()
                    }
                }
            } else {
                skill_manager
                    .resolve_skills_for_request_with_mode(
                        &config.disabled_skill_ids,
                        config.selected_skill_ids.as_deref(),
                        config.selected_skill_mode.as_deref(),
                        Some(request_hint),
                    )
                    .await
            };
        let selected_ids = selected_skills
            .iter()
            .map(|skill| skill.id.clone())
            .collect::<Vec<_>>();
        tracing::info!(
            "[{}] Skill selection trace: source={}, selected_count={}, selected_ids={:?}, skill_mode={}, request_hint_present={}",
            session_id,
            if config.selected_skill_ids.is_some() { "explicit" } else { "auto" },
            selected_ids.len(),
            selected_ids,
            config.selected_skill_mode.as_deref().unwrap_or("default"),
            !request_hint.trim().is_empty(),
        );

        let selection_source = if config.selected_skill_ids.is_some() {
            Some("explicit".to_string())
        } else {
            Some("auto".to_string())
        };

        let context = bamboo_skills::context::build_skill_context(&selected_skills);
        if !context.is_empty() {
            tracing::info!(
                "[{}] Skill context loaded, length: {} chars",
                session_id,
                context.len()
            );
            tracing::debug!("[{}] Skill context content:\n{}", session_id, context);
        } else {
            tracing::info!("[{}] No skill context loaded (empty)", session_id);
        }
        SkillContextLoadResult {
            context,
            selected_skill_ids: selected_ids,
            selection_source,
            selected_skill_mode: config.selected_skill_mode.clone(),
            request_hint_present: !request_hint.trim().is_empty(),
        }
    } else {
        tracing::info!("[{}] No skill manager configured", session_id);
        SkillContextLoadResult::default()
    }
}

/// A new explicit activation supersedes any workflow loaded on an earlier run.
/// Clear the old activation before publishing the current selection so tool
/// authorization cannot briefly observe a stale loaded workflow.
pub(super) fn reset_explicit_activation_state(
    session: &mut Session,
    selection: &SkillContextLoadResult,
) {
    if selection.selection_source.as_deref() == Some("explicit")
        && selection.selected_skill_ids.len() == 1
    {
        session.metadata.remove(LOADED_SKILL_IDS_METADATA_KEY);
        session.metadata.remove(LAST_LOADED_SKILL_ID_METADATA_KEY);
        session
            .metadata
            .remove(LAST_LOADED_SKILL_SUMMARY_METADATA_KEY);
    }
}

/// Deterministically activate a single explicitly selected workflow before the
/// first model round. Automatic selection remains metadata-only because it can
/// advertise several candidates and the model still has to choose one.
pub(super) async fn activate_explicit_skill(
    tools: &dyn ToolExecutor,
    session: &mut Session,
    session_id: &str,
    selection: &SkillContextLoadResult,
) -> Option<String> {
    if selection.selection_source.as_deref() != Some("explicit")
        || selection.selected_skill_ids.len() != 1
    {
        return None;
    }

    let skill_id = selection.selected_skill_ids.first()?.as_str();
    let available_tool_schemas = tools.list_tools();
    if !available_tool_schemas
        .iter()
        .any(|schema| schema.function.name == "load_skill")
    {
        tracing::warn!(
            "[{}] Explicit skill '{}' could not be preloaded because load_skill is unavailable",
            session_id,
            skill_id
        );
        return None;
    }

    let call_id = format!("runtime-explicit-skill-{session_id}");
    let arguments = serde_json::json!({ "skill_id": skill_id });
    let call = ToolCall {
        id: call_id.clone(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "load_skill".to_string(),
            arguments: arguments.to_string(),
        },
    };
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
    let context = ToolExecutionContext::for_dispatch(
        session_id,
        &call_id,
        &event_tx,
        &available_tool_schemas,
        ToolExecutionSessionFlags::from_session(session),
        false,
        None,
        Some(&arguments),
    );

    let result = match tools.execute_with_context(&call, context).await {
        Ok(result) if result.success => result,
        Ok(result) => {
            tracing::warn!(
                "[{}] Explicit skill '{}' preload returned an unsuccessful result: {}",
                session_id,
                skill_id,
                result.result
            );
            return None;
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Explicit skill '{}' preload failed: {}",
                session_id,
                skill_id,
                error
            );
            return None;
        }
    };

    session.metadata.insert(
        LOADED_SKILL_IDS_METADATA_KEY.to_string(),
        serde_json::json!([skill_id]).to_string(),
    );
    session.metadata.insert(
        LAST_LOADED_SKILL_ID_METADATA_KEY.to_string(),
        skill_id.to_string(),
    );
    session.metadata.insert(
        LAST_LOADED_SKILL_SUMMARY_METADATA_KEY.to_string(),
        serde_json::json!({
            "skill_id": skill_id,
            "loaded_ids": [skill_id],
            "selected_skill_mode": selection.selected_skill_mode,
            "loaded_count": 1
        })
        .to_string(),
    );

    Some(format!(
        "\n\n## Explicit Workflow Activated\n\
The user explicitly selected the `{skill_id}` workflow. Bamboo loaded its detailed instructions before model execution. Follow the payload below as the active workflow; do not call `load_skill` again for this activation.\n\n{payload}\n",
        payload = result.result
    ))
}
