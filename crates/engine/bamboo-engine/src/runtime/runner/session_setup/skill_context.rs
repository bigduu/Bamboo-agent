use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor,
};
use bamboo_agent_core::Session;
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
};
use sha2::{Digest, Sha256};
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
    pub(super) catalog_entries: Vec<bamboo_skills::WorkflowCatalogEntry>,
    pub(super) catalog_diagnostic: Option<bamboo_skills::WorkflowCatalogDiagnostic>,
    pub(super) durable_snapshot: Option<bamboo_skills::SkillActivationSnapshot>,
    pub(super) activation_diagnostic: Option<bamboo_skills::WorkflowActivationDiagnostic>,
    pub(super) restored_active_context: bool,
}

fn degraded_activation_result(
    code: bamboo_skills::WorkflowActivationErrorCode,
    message: impl Into<String>,
) -> SkillContextLoadResult {
    let message = message.into();
    SkillContextLoadResult {
        context: format!(
            "\n\n## Workflow Activation Degraded\nBamboo could not restore or activate the selected workflow: {message}\nContinue the main session without workflow instructions; do not guess or load a newer revision.\n"
        ),
        activation_diagnostic: Some(bamboo_skills::WorkflowActivationDiagnostic {
            code,
            message,
            recoverable: true,
        }),
        ..Default::default()
    }
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
        let persisted_selection_requires_snapshot = retained_selection_source.as_deref()
            == Some("explicit")
            && session
                .metadata
                .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY)
                .and_then(|raw| serde_json::from_str::<BTreeMap<String, u64>>(raw).ok())
                .is_some_and(|revisions| !revisions.is_empty());
        let has_active_workflow = session
            .metadata
            .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
            .is_some_and(|active| active.status == bamboo_skills::WorkflowActivationStatus::Active);
        if retained_activation.is_none()
            && (has_active_workflow
                || (must_resume_pinned_activation && persisted_selection_requires_snapshot))
        {
            let restored = async {
                let snapshot = if has_active_workflow {
                    let durable = session
                        .metadata
                        .get(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
                        .ok_or("durable workflow snapshot metadata is missing")
                        .and_then(|raw| {
                            serde_json::from_str::<bamboo_skills::DurableWorkflowActivation>(raw)
                                .map_err(|_| "durable workflow snapshot metadata is invalid")
                        })?;
                    if durable.active.status != bamboo_skills::WorkflowActivationStatus::Active {
                        return Err("durable workflow snapshot is not active");
                    }
                    let entry = durable
                        .snapshot
                        .skills
                        .get(&durable.active.id)
                        .ok_or("durable workflow snapshot root is missing")?;
                    if durable.snapshot.skills.len() != 1
                        || entry.revision != durable.active.revision
                        || entry.catalog_entry.source != durable.active.source
                        || entry.catalog_entry.kind != durable.active.kind
                    {
                        return Err(
                            "durable workflow snapshot identity does not match active metadata",
                        );
                    }
                    durable.snapshot
                } else {
                    session
                        .metadata
                        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
                        .ok_or("in-flight workflow candidate snapshot is missing")
                        .and_then(|raw| {
                            serde_json::from_str::<bamboo_skills::SkillActivationSnapshot>(raw)
                                .map_err(|_| "in-flight workflow candidate snapshot is invalid")
                        })?
                };
                let store = skill_manager
                    .store_for_workspace(workspace.as_deref())
                    .await
                    .map_err(|_| "durable workflow workspace is unavailable")?;
                store
                    .restore_activation_snapshot(session_id, snapshot)
                    .await
                    .map_err(|_| "durable workflow snapshot failed validation")?;
                Ok::<(), &str>(())
            }
            .await;
            if let Err(error) = restored {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::SnapshotUnavailable,
                    error,
                ));
            }
            retained_activation = skill_manager
                .pinned_activation_for_workspace(session_id, workspace.as_deref())
                .await
                .map_err(|error| {
                    format!("Failed to inspect restored workflow activation: {error}")
                })?;
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
        let max_context_tokens = config
            .token_budget
            .as_ref()
            .map(|budget| budget.max_context_tokens as usize)
            .unwrap_or(bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS);
        let activation = if let Some(activation) = retained_activation {
            Some(activation)
        } else if let Some(workspace_path) = workspace.as_deref() {
            match skill_manager
                .resolve_and_pin_activation_in_workspace_with_mode_and_budget(
                    workspace_path,
                    session_id,
                    &config.disabled_skill_ids,
                    config.selected_skill_ids.as_deref(),
                    config.selected_skill_mode.as_deref(),
                    Some(request_hint),
                    max_context_tokens,
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
                .resolve_and_pin_activation_for_request_with_mode_and_budget(
                    session_id,
                    &config.disabled_skill_ids,
                    config.selected_skill_ids.as_deref(),
                    config.selected_skill_mode.as_deref(),
                    Some(request_hint),
                    max_context_tokens,
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
        let catalog_entries = activation
            .as_ref()
            .map(|activation| activation.catalog_entries.clone())
            .unwrap_or_default();
        let catalog_diagnostic = activation
            .as_ref()
            .map(|activation| activation.catalog_diagnostic.clone());
        if let Some(selection) = session
            .metadata
            .get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<bamboo_skills::WorkflowSelection>(raw).ok())
        {
            let Some(entry) = catalog_entries
                .iter()
                .find(|entry| entry.id == selection.id)
            else {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::RevisionMissing,
                    "selected workflow revision is unavailable and no matching LKG snapshot exists",
                ));
            };
            if entry.revision != selection.revision {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::RevisionMismatch,
                    "selected workflow revision does not match the pinned catalog revision",
                ));
            }
            if entry.source != selection.source {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::SourceMismatch,
                    "selected workflow source does not match the pinned catalog source",
                ));
            }
            if entry.kind != bamboo_skills::WorkflowKind::Instruction {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::InvalidSelection,
                    "orchestration workflows must be started through workflow_run/API, not instruction activation",
                ));
            }
            if entry.invocation_policy["explicit"].as_bool() != Some(true) {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::ManualOnly,
                    "workflow does not allow explicit instruction activation",
                ));
            }
            if let Err(error) =
                bamboo_domain::validate_schema(&entry.argument_schema, &selection.args)
            {
                return Ok(degraded_activation_result(
                    bamboo_skills::WorkflowActivationErrorCode::InvalidSelection,
                    format!("workflow arguments do not match the pinned schema: {error}"),
                ));
            }
        }
        if config.selected_skill_ids.is_some()
            && catalog_entries.len() == 1
            && catalog_entries[0].kind == bamboo_skills::WorkflowKind::Orchestration
        {
            return Ok(degraded_activation_result(
                bamboo_skills::WorkflowActivationErrorCode::InvalidSelection,
                "orchestration workflows must be started through workflow_run/API, not load_skill",
            ));
        }
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

        let durable_active = session
            .metadata
            .get(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
            .and_then(|raw| {
                serde_json::from_str::<bamboo_skills::DurableWorkflowActivation>(raw).ok()
            })
            .filter(|durable| {
                durable.active.status == bamboo_skills::WorkflowActivationStatus::Active
                    && selected_ids.len() == 1
                    && selected_ids[0] == durable.active.id
                    && catalog_entries.iter().any(|entry| {
                        entry.id == durable.active.id
                            && entry.revision == durable.active.revision
                            && entry.source == durable.active.source
                            && entry.kind == durable.active.kind
                    })
            });
        let (context, restored_active_context) = if durable_active.is_some() {
            (String::new(), true)
        } else {
            (
                catalog_diagnostic
                    .as_ref()
                    .map(|diagnostic| {
                        bamboo_skills::context::build_workflow_catalog_context(
                            &selected_skills,
                            &catalog_entries,
                            diagnostic,
                        )
                    })
                    .unwrap_or_default(),
                false,
            )
        };
        // Automatic selection only advertises metadata. Keep its immutable candidate
        // pin in process for load_skill, but do not serialize every candidate's
        // resource tree into the session. The tool exports and narrows the pin to the
        // one workflow the model actually activates. Explicit preload needs the
        // candidate snapshot before invoking load_skill, while an already-active LKG
        // remains durable across restarts.
        let durable_snapshot = if activation.is_some()
            && (selection_source.as_deref() == Some("explicit") || durable_active.is_some())
        {
            let store = skill_manager
                .store_for_workspace(workspace.as_deref())
                .await
                .map_err(|error| format!("Failed to resolve workflow snapshot store: {error}"))?;
            store.export_activation_snapshot(session_id).await
        } else {
            None
        };
        if durable_snapshot
            .as_ref()
            .and_then(|snapshot| serde_json::to_vec(snapshot).ok())
            .is_some_and(|bytes| bytes.len() > 512 * 1024)
        {
            return Ok(degraded_activation_result(
                bamboo_skills::WorkflowActivationErrorCode::SnapshotTooLarge,
                "selected workflow snapshot exceeds the durable session limit",
            ));
        }
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
            catalog_entries,
            catalog_diagnostic,
            durable_snapshot,
            activation_diagnostic: None,
            restored_active_context,
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
    event_tx: &tokio::sync::mpsc::Sender<bamboo_agent_core::AgentEvent>,
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
    let context = ToolExecutionContext::for_dispatch(
        session_id,
        &call_id,
        event_tx,
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

    let payload_value = serde_json::from_str::<serde_json::Value>(&result.result)
        .map_err(|_| format!("Explicit workflow '{skill_id}' returned an invalid payload"))?;
    if let Some(dynamic) = payload_value.get("dynamic_context") {
        session.metadata.insert(
            bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY.to_string(),
            dynamic.to_string(),
        );
    }
    if payload_value["activation_status"].as_str() == Some("degraded") {
        session.metadata.insert(
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
            payload_value["dynamic_context"].to_string(),
        );
        session
            .metadata
            .remove(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY);
        session
            .metadata
            .remove(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY);
        return Ok(Some(String::new()));
    }
    let canonical_context = serde_json::json!({
        "id": skill_id,
        "revision": payload_value.get("revision"),
        "selection": session.metadata.get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY),
        "instructions": payload_value.get("instructions"),
        "dynamic_context": payload_value.get("dynamic_context"),
    });
    let fingerprint = hex::encode(Sha256::digest(canonical_context.to_string().as_bytes()));
    bamboo_skills::record_loaded_workflow_activation(&mut session.metadata, skill_id, fingerprint)
        .map_err(|diagnostic| diagnostic.message)?;
    // The tool already published WorkflowActivated through this run's real
    // event channel. Acknowledge the mirrored local pending marker before the
    // setup save, and let the per-round dedicated WorkflowRuntime ContextBlock
    // carry the exact durable instructions without duplicating system text.
    session
        .metadata
        .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);

    Ok(Some(String::new()))
}
