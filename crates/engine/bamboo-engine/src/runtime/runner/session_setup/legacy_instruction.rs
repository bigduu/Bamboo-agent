//! Stateless adapter for the existing model-issued Instruction activation path.
//! This module adds no caller authority, lifecycle state or live Codex integration.

use super::skill_context::{self, SkillContextLoadResult};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::logging::DebugLogger;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    AgentError, ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType,
    Session,
};
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY, SKILL_RUNTIME_ACTIVATION_ERROR_KEY,
    SKILL_RUNTIME_ACTIVATION_GENERATION_KEY, SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
    SKILL_RUNTIME_SELECTED_CATALOG_KEY, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY, SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
    SKILL_RUNTIME_SELECTION_COUNT_KEY, SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    SKILL_RUNTIME_SELECTION_TRACE_KEY,
};

pub(crate) async fn prepare_context(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    session_id: &str,
    debug_logger: &DebugLogger,
    must_resume_pinned_activation: bool,
) -> super::super::Result<String> {
    let skill_result = match skill_context::load_skill_context(
        config,
        session,
        session_id,
        initial_message,
        must_resume_pinned_activation,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            session.metadata.insert(
                SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
                error.clone(),
            );
            if let Some(persistence) = config.persistence.as_ref() {
                if let Err(save_error) = persistence.save_runtime_session(session).await {
                    tracing::warn!(
                        "[{}] Failed to persist workflow activation setup error: {}",
                        session_id,
                        save_error
                    );
                }
            }
            return Err(AgentError::Tool(format!(
                "Workflow activation failed before model execution: {error}"
            )));
        }
    };
    session.metadata.remove(SKILL_RUNTIME_ACTIVATION_ERROR_KEY);
    let explicit_activation_loaded = skill_result.restored_active_context
        || skill_context::selection_matches_loaded_activation(session, &skill_result);
    let skill_context = if explicit_activation_loaded {
        format!(
            "\n\n## Explicit Workflow Already Activated\n\
The `{skill_id}` workflow was loaded successfully earlier in this session. Continue following the existing `load_skill` result and its workflow instructions. Do not call `load_skill` again solely because execution resumed.\n",
            skill_id = skill_result.selected_skill_ids[0],
        )
    } else if skill_result.selection_source.as_deref() == Some("explicit")
        && skill_result.selected_skill_ids.len() == 1
    {
        format!(
            "\n\n## Required Explicit Workflow Activation\n\
The user explicitly selected `{skill_id}`. Your first response step MUST be exactly one `load_skill` call for `{skill_id}`. Do not emit commentary, an answer, or any other tool call before it completes. If the tool reports `activation_status: degraded`, do not retry it; continue without workflow instructions. Otherwise, follow the loaded workflow instructions.\n{context}",
            skill_id = skill_result.selected_skill_ids[0],
            context = skill_result.context.as_str(),
        )
    } else {
        skill_result.context.clone()
    };

    if let Some(diagnostic) = skill_result.activation_diagnostic.as_ref() {
        session.metadata.insert(
            SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
            serde_json::to_string(diagnostic).unwrap_or_else(|_| diagnostic.message.clone()),
        );
        session
            .metadata
            .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
        if let Some(persistence) = config.persistence.as_ref() {
            persistence
                .save_runtime_session(session)
                .await
                .map_err(|error| {
                    AgentError::Tool(format!(
                        "Workflow degraded state could not be persisted: {error}"
                    ))
                })?;
        }
    }

    if let Some(source) = skill_result.selection_source.as_deref() {
        debug_logger.log_event(
            session_id,
            "skill_selection_runtime_state",
            serde_json::json!({
                "source": source,
                "selected_skill_ids": skill_result.selected_skill_ids,
                "selected_skill_mode": skill_result.selected_skill_mode,
                "request_hint_present": skill_result.request_hint_present
            }),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
            source.to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(),
            serde_json::to_string(&skill_result.catalog_entries)
                .unwrap_or_else(|_| "[]".to_string()),
        );
        session.metadata.insert(
            bamboo_skills::WORKFLOW_CATALOG_DIAGNOSTIC_METADATA_KEY.to_string(),
            serde_json::to_string(&skill_result.catalog_diagnostic)
                .unwrap_or_else(|_| "null".to_string()),
        );
        if let Some(snapshot) = skill_result.durable_snapshot.as_ref() {
            session.metadata.insert(
                SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
                serde_json::to_string(snapshot).map_err(|_| {
                    AgentError::Tool(
                        "Workflow activation snapshot could not be serialized".to_string(),
                    )
                })?,
            );
        } else {
            // A metadata-only automatic catalog must not inherit an older
            // candidate resource snapshot from a prior selection.
            session.metadata.remove(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY);
        }
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_COUNT_KEY.to_string(),
            skill_result.selected_skill_ids.len().to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            serde_json::to_string(&skill_result.selected_skill_ids).unwrap_or("[]".to_string()),
        );
        if let Some(revision) = skill_result.catalog_revision {
            session.metadata.insert(
                SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
                revision.to_string(),
            );
        } else {
            session
                .metadata
                .remove(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY);
        }
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
            serde_json::to_string(&skill_result.skill_revisions).unwrap_or("{}".to_string()),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_TRACE_KEY.to_string(),
            serde_json::json!({
                "source": source,
                "selected_skill_ids": skill_result.selected_skill_ids,
                "selected_skill_mode": skill_result.selected_skill_mode,
                "request_hint_present": skill_result.request_hint_present
            })
            .to_string(),
        );
        if let Some(mode) = skill_result.selected_skill_mode.as_ref() {
            session.metadata.insert(
                SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY.to_string(),
                mode.clone(),
            );
        } else {
            session
                .metadata
                .remove(SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY);
        }

        if !skill_result.restored_active_context {
            skill_context::reset_activation_state_for_new_selection(session, &skill_result);
        }

        // Runtime tools authorize skill loads through the shared session repository.
        // Publish this run's resolved IDs before the first model/tool call so they
        // never observe a missing or previous-run allowlist from the cache.
        if let Some(persistence) = config.persistence.as_ref() {
            if let Err(error) = persistence.save_runtime_session(session).await {
                if let Some(skill_manager) = config.skill_manager.as_ref() {
                    let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
                    let _ = skill_manager
                        .release_activation_for_workspace(session_id, workspace.as_deref())
                        .await;
                }
                return Err(AgentError::Tool(format!(
                    "Workflow activation metadata could not be published before tool/model execution: {error}"
                )));
            }
        }
    }

    Ok(skill_context)
}

pub(super) fn selection_matches_loaded_activation(
    session: &Session,
    selection: &SkillContextLoadResult,
) -> bool {
    if selection.selection_source.as_deref() != Some("explicit")
        || selection.selected_skill_ids.len() != 1
    {
        return false;
    }
    let skill_id = selection.selected_skill_ids[0].as_str();
    let loaded_matches = session
        .metadata
        .get(LOADED_SKILL_IDS_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .is_some_and(|loaded| loaded == selection.selected_skill_ids);
    let active_matches = session
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
        .is_some_and(|active| {
            active.id == skill_id
                && active.status == bamboo_skills::WorkflowActivationStatus::Active
        });
    loaded_matches
        && active_matches
        && session
            .metadata
            .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
}

/// Clear a prior activation only when a newly resolved selection supersedes it.
/// The new candidate pin is kept so the model-issued `load_skill` call can load
/// the exact catalog revision selected during this setup pass.
pub(super) fn reset_activation_state_for_new_selection(
    session: &mut Session,
    selection: &SkillContextLoadResult,
) {
    if selection.selection_source.is_none()
        || selection_matches_loaded_activation(session, selection)
    {
        return;
    }
    for key in [
        LOADED_SKILL_IDS_METADATA_KEY,
        LAST_LOADED_SKILL_ID_METADATA_KEY,
        LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
        bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY,
        bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
        bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
        bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
        bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY,
    ] {
        session.metadata.remove(key);
    }
}

pub(crate) fn explicit_activation_pending(session: &Session) -> bool {
    let selected_skill_ids = session
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    if session
        .metadata
        .get(SKILL_RUNTIME_SELECTION_SOURCE_KEY)
        .is_none_or(|source| source != "explicit")
        || selected_skill_ids.len() != 1
        || session
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
    {
        return false;
    }
    let selection = SkillContextLoadResult {
        selected_skill_ids,
        selection_source: Some("explicit".to_string()),
        ..Default::default()
    };
    !selection_matches_loaded_activation(session, &selection)
}

/// Rebuild the exact active instruction workflow from its durable LKG snapshot.
/// This is a dedicated host context block, never a synthetic user message in
/// session history and never a catalog/live-filesystem re-resolution.
pub(crate) fn active_context_block(session: &Session) -> Option<ContextBlock> {
    let durable = session
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::DurableWorkflowActivation>(raw).ok())
        .filter(|durable| {
            durable.active.status == bamboo_skills::WorkflowActivationStatus::Active
        })?;
    let entry = durable.snapshot.skills.get(&durable.active.id)?;
    if durable.snapshot.skills.len() != 1
        || entry.revision != durable.active.revision
        || entry.catalog_entry.source != durable.active.source
        || entry.catalog_entry.kind != bamboo_skills::WorkflowKind::Instruction
    {
        return None;
    }
    let dynamic = durable
        .active
        .dynamic_context
        .iter()
        .map(|block| {
            serde_json::json!({
                "provider_id": block.provider_id,
                "provenance": block.provenance,
                "status": block.status,
                "content": block.content,
                "diagnostic": block.diagnostic,
            })
        })
        .collect::<Vec<_>>();
    Some(
        ContextBlock::new(
            ContextBlockType::WorkflowRuntime,
            ContextBlockPriority::Critical,
            ContextBlockStability::SessionStable,
            format!("Active Workflow: {}@{}", durable.active.id, durable.active.revision),
            format!(
                "workflow_id: {}\nsource: {:?}\nrevision: {}\nargs: {}\ncontext_fingerprint: {}\n\n### Instructions\n{}\n\n### Dynamic Context\n{}",
                durable.active.id,
                durable.active.source,
                durable.active.revision,
                durable.active.args,
                durable
                    .active
                    .context_fingerprint
                    .as_deref()
                    .unwrap_or("unavailable"),
                entry.definition.prompt,
                serde_json::to_string(&dynamic).unwrap_or_else(|_| "[]".to_string()),
            ),
        )
        .with_metadata(Some(serde_json::json!({
            "workflow_id": durable.active.id,
            "source": durable.active.source,
            "revision": durable.active.revision,
            "context_fingerprint": durable.active.context_fingerprint,
        }))),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExplicitActivationAttempt {
    call_id: String,
    skill_id: String,
}

pub(crate) fn validate_first_step(
    session: &Session,
    tool_calls: &[bamboo_agent_core::tools::ToolCall],
) -> Result<Option<ExplicitActivationAttempt>, AgentError> {
    if !skill_context::explicit_activation_pending(session) {
        return Ok(None);
    }

    let selected_skill_id = session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .and_then(|ids| ids.into_iter().next())
        .ok_or_else(|| {
            AgentError::Tool(format!(
                "[{}] explicit workflow activation is missing its selected skill",
                session.id
            ))
        })?;
    let valid_call = tool_calls.len() == 1
        && bamboo_tools::normalize_tool_ref(&tool_calls[0].function.name)
            .is_some_and(|name| name == "load_skill");
    if !valid_call {
        return Err(AgentError::Tool(format!(
            "[{}] explicit workflow activation was not completed: the first model step must be exactly one load_skill call",
            session.id
        )));
    }
    let called_skill_id =
        serde_json::from_str::<serde_json::Value>(&tool_calls[0].function.arguments)
            .ok()
            .and_then(|arguments| {
                arguments
                    .get("skill_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .map(str::to_string)
            });
    if called_skill_id.as_deref() != Some(selected_skill_id.as_str()) {
        return Err(AgentError::Tool(format!(
            "[{}] explicit workflow activation must load selected skill '{}'",
            session.id, selected_skill_id
        )));
    }

    Ok(Some(ExplicitActivationAttempt {
        call_id: tool_calls[0].id.clone(),
        skill_id: selected_skill_id,
    }))
}

pub(crate) fn apply_successful_attempt(
    session: &mut Session,
    attempt: &ExplicitActivationAttempt,
) -> Result<(), AgentError> {
    let tool_succeeded = session.messages.iter().rev().any(|message| {
        message.tool_call_id.as_deref() == Some(attempt.call_id.as_str())
            && message.tool_success == Some(true)
    });
    // The #579 success path refreshes the complete workflow activation namespace
    // from SessionRepository into this runner-owned Session before returning.
    // Require both the successful tool result and that durable active snapshot;
    // a provider/degraded/save failure must never unlock the answer round.
    if !tool_succeeded || skill_context::explicit_activation_pending(session) {
        return Err(AgentError::Tool(format!(
            "[{}] explicit workflow '{}' failed to activate; refusing to continue to a user-facing answer",
            session.id, attempt.skill_id
        )));
    }
    Ok(())
}

#[cfg(test)]
impl ExplicitActivationAttempt {
    pub(crate) fn call_id(&self) -> &str {
        &self.call_id
    }
    pub(crate) fn skill_id(&self) -> &str {
        &self.skill_id
    }
}

pub(crate) fn retain_terminal_activation_tools(
    session: &Session,
    tool_schemas: &mut Vec<ToolSchema>,
) {
    // Once a single explicitly selected workflow reaches a terminal activation
    // result, stop advertising load_skill so the model-issued attempt occurs
    // exactly once. A typed degraded result is terminal too: the main session
    // continues without workflow instructions instead of retrying forever.
    // Automatic catalogs keep the tool available until the model chooses a
    // candidate.
    let loaded_skill_ids = session
        .metadata
        .get(LOADED_SKILL_IDS_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    let selected_skill_ids = session
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    let explicit_selection = session
        .metadata
        .get(SKILL_RUNTIME_SELECTION_SOURCE_KEY)
        .is_some_and(|source| source == "explicit");
    let explicit_activation_is_current = explicit_selection
        && !loaded_skill_ids.is_empty()
        && loaded_skill_ids == selected_skill_ids;
    let explicit_activation_degraded = explicit_selection
        && session
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY);
    if explicit_activation_is_current || explicit_activation_degraded {
        tool_schemas.retain(|schema| schema.function.name != "load_skill");
    }
}

const WORKFLOW_TOOL_METADATA_KEYS: &[&str] = &[
    bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY,
    bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
    bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
    bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
    bamboo_skills::runtime_metadata::LOADED_SKILL_IDS_METADATA_KEY,
    bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_ID_METADATA_KEY,
    bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
];

pub(crate) async fn refresh_load_side_effects(
    session: &mut Session,
    config: &AgentLoopConfig,
    session_id: &str,
    tool_name: &str,
    success: bool,
) {
    // Keep the original raw-name branch distinct from normalized first-call validation.
    if !success || tool_name != "load_skill" {
        return;
    }
    let Some(persistence) = config.persistence.as_ref() else {
        return;
    };
    match persistence.load_runtime_session(session_id).await {
        Ok(Some(latest)) => {
            for key in WORKFLOW_TOOL_METADATA_KEYS {
                if let Some(value) = latest.metadata.get(*key) {
                    session
                        .metadata
                        .insert((*key).to_string(), value.clone());
                } else {
                    session.metadata.remove(*key);
                }
            }
        }
        Ok(None) => tracing::warn!(
            "[{}] load_skill completed but no repository session was available for activation refresh",
            session_id
        ),
        Err(error) => tracing::warn!(
            "[{}] load_skill activation refresh failed: {}",
            session_id,
            error
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Repository {
        latest: Option<Session>,
        error: bool,
        loads: AtomicUsize,
        saves: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for Repository {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            self.saves.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            assert_eq!(session_id, "refresh-parity");
            self.loads.fetch_add(1, Ordering::SeqCst);
            if self.error {
                Err(std::io::Error::other("repository unavailable"))
            } else {
                Ok(self.latest.clone())
            }
        }
    }

    #[tokio::test]
    async fn legacy_runner_refresh_preserves_raw_name_and_exact_namespace_without_writing() {
        // The expected namespace is an independent literal fixture, not the adapter's constant.
        let keys = [
            "workflow.active.v1",
            "workflow.active.snapshot.v1",
            "workflow.activation_event.v1",
            "workflow.dynamic_context.last.v1",
            "workflow.context_cache.v1",
            "skill_runtime_activation_error",
            "skill_runtime_pinned_snapshot_v1",
            "skill_runtime_loaded_skill_ids",
            "skill_runtime_last_loaded_skill_id",
            "skill_runtime_last_load_summary",
        ];
        let mut current = Session::new("refresh-parity", "model");
        let mut latest = current.clone();
        for (i, key) in keys.iter().enumerate() {
            current.metadata.insert((*key).into(), format!("old-{i}"));
            if i % 2 == 0 {
                latest.metadata.insert((*key).into(), format!("new-{i}"));
            }
        }
        for key in [
            "skill_runtime_selected_skill_ids",
            "workflow.run_ids.v1",
            "unrelated",
        ] {
            current.metadata.insert(key.into(), "runner-owned".into());
            latest
                .metadata
                .insert(key.into(), "repository-owned".into());
        }
        let before = current.metadata.clone();
        let repository = Arc::new(Repository {
            latest: Some(latest),
            error: false,
            loads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        });
        let config = AgentLoopConfig {
            persistence: Some(repository.clone()),
            ..Default::default()
        };
        for name in ["default::load_skill", "LOAD_SKILL", "workflow_run", "Read"] {
            refresh_load_side_effects(&mut current, &config, "refresh-parity", name, true).await;
            assert_eq!(current.metadata, before);
        }
        refresh_load_side_effects(&mut current, &config, "refresh-parity", "load_skill", false)
            .await;
        assert_eq!(repository.loads.load(Ordering::SeqCst), 0);
        refresh_load_side_effects(&mut current, &config, "refresh-parity", "load_skill", true)
            .await;
        let mut expected = before;
        for (i, key) in keys.iter().enumerate() {
            if i % 2 == 0 {
                expected.insert((*key).into(), format!("new-{i}"));
            } else {
                expected.remove(*key);
            }
        }
        assert_eq!(current.metadata, expected);
        assert_eq!(repository.loads.load(Ordering::SeqCst), 1);
        assert_eq!(repository.saves.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn legacy_runner_refresh_preserves_state_for_missing_or_errored_repository() {
        for error in [false, true] {
            let repository = Arc::new(Repository {
                latest: None,
                error,
                loads: AtomicUsize::new(0),
                saves: AtomicUsize::new(0),
            });
            let config = AgentLoopConfig {
                persistence: Some(repository.clone()),
                ..Default::default()
            };
            let mut session = Session::new("refresh-parity", "model");
            session
                .metadata
                .insert("workflow.active.v1".into(), "existing".into());
            let before = serde_json::to_value(&session).unwrap();
            refresh_load_side_effects(&mut session, &config, "refresh-parity", "load_skill", true)
                .await;
            assert_eq!(serde_json::to_value(&session).unwrap(), before);
            assert_eq!(repository.loads.load(Ordering::SeqCst), 1);
            assert_eq!(repository.saves.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn legacy_runner_gate_preserves_normalized_name_and_original_receipt_role_semantics() {
        let mut session = Session::new("legacy-gate-parity", "model");
        session
            .metadata
            .insert(SKILL_RUNTIME_SELECTION_SOURCE_KEY.into(), "explicit".into());
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
            r#"["review"]"#.into(),
        );
        let call = bamboo_agent_core::tools::ToolCall {
            id: "model-issued-load".into(),
            tool_type: "function".into(),
            function: bamboo_agent_core::tools::FunctionCall {
                name: "default::LOAD_SKILL".into(),
                arguments: r#"{"skill_id":" review "}"#.into(),
            },
        };
        let attempt = validate_first_step(&session, &[call]).unwrap().unwrap();
        // Preserve the original any-role matching receipt test; this extraction must not strengthen it.
        let mut receipt = bamboo_agent_core::Message::user("legacy matching receipt");
        receipt.tool_call_id = Some("model-issued-load".into());
        receipt.tool_success = Some(true);
        session.add_message(receipt);
        session
            .metadata
            .insert(LOADED_SKILL_IDS_METADATA_KEY.into(), r#"["review"]"#.into());
        session.metadata.insert(
            bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY.into(),
            serde_json::json!({
                "id":"review", "source":"builtin", "revision":1, "kind":"instruction", "args":{},
                "invoked_by":"user", "activated_at":"2026-07-21T00:00:00Z", "status":"active"
            })
            .to_string(),
        );
        session.metadata.insert(
            bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.into(),
            "{}".into(),
        );
        apply_successful_attempt(&mut session, &attempt).unwrap();
        assert!(
            active_context_block(&session).is_none(),
            "loaded matching does not validate snapshot contents"
        );
        assert!(!explicit_activation_pending(&session));
    }
}
