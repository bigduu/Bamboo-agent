use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor,
};
use bamboo_agent_core::Session;
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Default)]
pub(super) struct SkillContextLoadResult {
    pub(super) context: String,
    pub(super) selected_skill_ids: Vec<String>,
    pub(super) selection_source: Option<String>,
    pub(super) selected_skill_mode: Option<String>,
    pub(super) request_hint_present: bool,
    pub(super) catalog_revision: Option<u64>,
    pub(super) skill_revisions: BTreeMap<String, u64>,
}

pub(super) async fn load_skill_context(
    config: &AgentLoopConfig,
    session: &Session,
    session_id: &str,
    request_hint: &str,
    must_resume_pinned_activation: bool,
) -> Result<SkillContextLoadResult, String> {
    if let Some(skill_manager) = config.skill_manager.as_ref() {
        let retained_selection_source = session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY)
            .filter(|source| matches!(source.as_str(), "explicit" | "auto"))
            .cloned();
        let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
        // Completed activations are released by finalization. Therefore an existing
        // pin identifies a suspended/in-flight continuation even though startup has
        // already replaced the session's prior status with `Initializing`.
        let mut retained_activation = skill_manager
            .pinned_activation_for_workspace(session_id, workspace.as_deref())
            .await
            .map_err(|error| format!("Failed to inspect retained workflow activation: {error}"))?;
        let persisted_selection_requires_snapshot = session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY)
            .and_then(|raw| serde_json::from_str::<BTreeMap<String, u64>>(raw).ok())
            .is_some_and(|revisions| !revisions.is_empty());
        if must_resume_pinned_activation
            && persisted_selection_requires_snapshot
            && retained_activation.is_none()
        {
            return Err(
                "Suspended workflow snapshot is unavailable after restart; retry as a new activation"
                    .to_string(),
            );
        }
        if let Some(retained) = retained_activation.as_ref() {
            let requested_mode = config
                .selected_skill_mode
                .as_deref()
                .map(str::trim)
                .filter(|mode| !mode.is_empty())
                .map(str::to_ascii_lowercase);
            let mode_matches = requested_mode.as_ref().is_none_or(|requested| {
                retained.descriptor.selected_skill_mode.as_ref() == Some(requested)
            });
            let selection_matches = if let Some(requested_ids) = config.selected_skill_ids.as_ref()
            {
                let requested = requested_ids
                    .iter()
                    .map(|id| id.trim())
                    .filter(|id| !id.is_empty())
                    .collect::<BTreeSet<_>>();
                let pinned = retained
                    .descriptor
                    .skill_revisions
                    .keys()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                requested == pinned
            } else {
                true
            };
            if !mode_matches || !selection_matches {
                if let Err(error) = skill_manager
                    .release_activation_for_workspace(session_id, workspace.as_deref())
                    .await
                {
                    tracing::warn!(
                        "[{}] Failed to supersede retained workflow activation: {}",
                        session_id,
                        error
                    );
                }
                retained_activation = None;
            }
        }
        let continues_retained_activation = retained_activation.is_some();
        let activation = if let Some(activation) = retained_activation {
            Some(activation)
        } else if let Some(workspace_path) = workspace.as_deref() {
            match skill_manager
                .resolve_and_pin_activation_in_workspace_with_mode(
                    workspace_path,
                    session_id,
                    &config.disabled_skill_ids,
                    config.selected_skill_ids.as_deref(),
                    config.selected_skill_mode.as_deref(),
                    Some(request_hint),
                )
                .await
            {
                Ok(activation) => Some(activation),
                Err(error) => {
                    if let Err(release_error) = skill_manager
                        .release_activation_for_workspace(session_id, Some(workspace_path))
                        .await
                    {
                        tracing::warn!(
                            "[{}] Failed to clear stale workflow activation: {}",
                            session_id,
                            release_error
                        );
                    }
                    return Err(format!(
                        "Failed to pin immutable workflow activation for this run: {error}. Retry as a new activation after releasing capacity or reducing workflow resources"
                    ));
                }
            }
        } else {
            match skill_manager
                .resolve_and_pin_activation_for_request_with_mode(
                    session_id,
                    &config.disabled_skill_ids,
                    config.selected_skill_ids.as_deref(),
                    config.selected_skill_mode.as_deref(),
                    Some(request_hint),
                )
                .await
            {
                Ok(activation) => Some(activation),
                Err(error) => {
                    if let Err(release_error) = skill_manager
                        .release_activation_for_workspace(session_id, None)
                        .await
                    {
                        tracing::warn!(
                            "[{}] Failed to clear stale workflow activation: {}",
                            session_id,
                            release_error
                        );
                    }
                    return Err(format!(
                        "Failed to pin immutable workflow activation for this run: {error}. Retry as a new activation after releasing capacity or reducing workflow resources"
                    ));
                }
            }
        };
        if activation.is_none() && workspace.is_none() && !continues_retained_activation {
            if let Err(error) = skill_manager
                .release_activation_for_workspace(session_id, None)
                .await
            {
                tracing::warn!(
                    "[{}] Failed to clear stale workflow activation: {}",
                    session_id,
                    error
                );
            }
        }
        let selected_skills = activation
            .as_ref()
            .map(|activation| activation.skills.clone())
            .unwrap_or_default();
        let selected_ids = selected_skills
            .iter()
            .map(|skill| skill.id.clone())
            .collect::<Vec<_>>();
        let configured_selection_source = if config.selected_skill_ids.is_some() {
            "explicit".to_string()
        } else {
            "auto".to_string()
        };
        let selection_source = Some(if continues_retained_activation {
            retained_selection_source.unwrap_or(configured_selection_source)
        } else {
            configured_selection_source
        });
        let selected_skill_mode = activation
            .as_ref()
            .and_then(|activation| activation.descriptor.selected_skill_mode.clone());
        tracing::info!(
            "[{}] Skill selection trace: source={}, selected_count={}, selected_ids={:?}, skill_mode={}, request_hint_present={}",
            session_id,
            selection_source.as_deref().unwrap_or("none"),
            selected_ids.len(),
            selected_ids,
            selected_skill_mode.as_deref().unwrap_or("default"),
            !request_hint.trim().is_empty(),
        );

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
        Ok(SkillContextLoadResult {
            context,
            selected_skill_ids: selected_ids,
            selection_source,
            selected_skill_mode,
            request_hint_present: !request_hint.trim().is_empty(),
            catalog_revision: activation
                .as_ref()
                .map(|activation| activation.descriptor.catalog_revision),
            skill_revisions: activation
                .as_ref()
                .map(|activation| activation.descriptor.skill_revisions.clone())
                .unwrap_or_default(),
        })
    } else {
        tracing::info!("[{}] No skill manager configured", session_id);
        Ok(SkillContextLoadResult::default())
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
) -> Result<Option<String>, String> {
    if selection.selection_source.as_deref() != Some("explicit")
        || selection.selected_skill_ids.len() != 1
    {
        return Ok(None);
    }

    let skill_id = selection.selected_skill_ids[0].as_str();
    let available_tool_schemas = tools.list_tools();
    if !available_tool_schemas
        .iter()
        .any(|schema| schema.function.name == "load_skill")
    {
        return Err(format!(
            "Explicit workflow '{skill_id}' cannot start because load_skill is unavailable"
        ));
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
            return Err(format!(
                "Explicit workflow '{skill_id}' preload was unsuccessful: {}",
                result.result
            ));
        }
        Err(error) => {
            return Err(format!(
                "Explicit workflow '{skill_id}' preload failed: {error}"
            ));
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

    Ok(Some(format!(
        "\n\n## Explicit Workflow Activated\n\
The user explicitly selected the `{skill_id}` workflow. Bamboo loaded its detailed instructions before model execution. Follow the payload below as the active workflow; do not call `load_skill` again for this activation.\n\n{payload}\n",
        payload = result.result
    )))
}
