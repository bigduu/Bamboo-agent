//! Helper for creating sessions from schedule run jobs.

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::{Message, Session, SessionPermissionMode};
use bamboo_engine::session_app::execution_prep::{
    prepare_session_for_execution, publish_resolved_workspace_for_execution,
    ResolvedExecutionWorkspace,
};

use super::manager::ScheduleRunJob;

/// Apply the per-session permission posture required by an unattended root.
///
/// This must happen in the factory, before the manager records permission
/// audit metadata, so every schedule entrypoint starts from the same explicit
/// Auto request without widening the process-global permission mode.
fn apply_unattended_permission_posture(session: &mut Session) {
    let runtime = session.agent_runtime_state.get_or_insert_default();
    runtime.set_permission_mode(SessionPermissionMode::Auto);
    runtime.no_human_approver = true;
}

/// Create and configure a new session for a scheduled run.
///
/// Sets up the unattended permission posture, metadata, workspace, system
/// prompt, and optional initial user message.
pub fn create_schedule_session(
    job: &ScheduleRunJob,
    model: &str,
    system_prompt: &str,
    base_system_prompt: &str,
    workspace: Option<ResolvedExecutionWorkspace<'_>>,
    reasoning_effort: Option<ReasoningEffort>,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Session {
    let session_id = uuid::Uuid::new_v4().to_string();
    let title = format!(
        "{} ({})",
        job.schedule_name,
        chrono::Utc::now().to_rfc3339()
    );

    let mut session = Session::new(session_id.clone(), model.to_string());
    apply_unattended_permission_posture(&mut session);
    session.metadata.insert(
        bamboo_engine::session_app::chat::SESSION_START_SOURCE_METADATA_KEY.to_string(),
        "startup".to_string(),
    );
    session.title = title;
    session.title_generated = true;
    session.metadata.insert(
        "created_by_schedule_id".to_string(),
        job.schedule_id.clone(),
    );
    session
        .metadata
        .insert("schedule_run_id".to_string(), job.run_id.clone());
    session.metadata.insert(
        "base_system_prompt".to_string(),
        base_system_prompt.to_string(),
    );
    if let Some(project_id) = job.run_config.project_id.as_ref() {
        session.set_project_id_meta(project_id.to_string());
    }
    if let Some(workspace) = workspace {
        publish_resolved_workspace_for_execution(
            &mut session,
            workspace,
            workspace_resolver,
            "schedule",
        );
    }
    if let Some(effort) = reasoning_effort {
        session.set_reasoning_effort_meta(effort.as_str());
    }
    prepare_session_for_execution(&mut session, Some(system_prompt), None);

    if let Some(task) = job
        .run_config
        .task_message
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        session.add_message(Message::user(task.to_string()));
    }

    session
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolExecutionSessionFlags;
    use bamboo_agent_core::workspace_state::{WorkspaceResolver, WorkspaceRootConfig};
    use bamboo_domain::{PermissionAuditSnapshot, PermissionMode, ScheduleRunConfig};
    use bamboo_engine::project_context::{WorkspaceBindingStatus, WorkspaceSource};
    use bamboo_tools::permission::{
        EffectivePermissionPolicy, PermissionConfig, PermissionDecisionKind,
        PermissionDecisionSource, PermissionEvaluation, PermissionOutcome, PermissionReasonCode,
        PermissionRequest, PermissionType, RiskLevel,
    };

    fn test_job() -> ScheduleRunJob {
        ScheduleRunJob {
            run_id: "run-scheduled-auto".to_string(),
            schedule_id: "schedule-scheduled-auto".to_string(),
            schedule_name: "scheduled auto".to_string(),
            run_config: ScheduleRunConfig::default(),
            scheduled_for: chrono::Utc::now(),
            claimed_at: chrono::Utc::now(),
            was_catch_up: false,
        }
    }

    fn permission_evaluation(session: &Session, config: &PermissionConfig) -> PermissionEvaluation {
        let flags =
            ToolExecutionSessionFlags::from_session_and_configured_mode(session, config.mode());
        PermissionEvaluation {
            request_id: format!("request-{}", session.id),
            session_id: session.id.clone(),
            workspace_path: None,
            tool_name: "Bash".to_string(),
            tool_args: serde_json::json!({"command": "eval 'printf gated'"}),
            permission_type: PermissionType::ExecuteCommand,
            resource: "eval 'printf gated'".to_string(),
            operation_summary: "execute a forced-ask high-risk command".to_string(),
            risk_level: RiskLevel::High,
            bypass_requested: flags.bypass_permissions,
            auto_approve_requested: flags.auto_approve_permissions,
            platform_hard_deny: None,
            consume_once: true,
            supported_decisions: PermissionDecisionKind::all_supported(),
        }
    }

    #[test]
    fn scheduled_factory_sets_typed_auto_mirror_and_no_human_without_widening_global_mode() {
        let config = PermissionConfig::new();
        let interactive_before = Session::new("interactive-before", "model");
        let resolver = WorkspaceResolver::from_process_globals();

        let mut scheduled = create_schedule_session(
            &test_job(),
            "model",
            "system",
            "base",
            None,
            None,
            &resolver,
        );
        crate::permission_audit::record_bamboo_runtime_permission_metadata(&mut scheduled, &config)
            .unwrap();

        let runtime = scheduled
            .agent_runtime_state
            .as_ref()
            .expect("scheduled root runtime posture");
        assert_eq!(runtime.permission_mode, SessionPermissionMode::Auto);
        assert!(runtime.bypass_permissions, "legacy compatibility mirror");
        assert!(runtime.no_human_approver);
        let audit = PermissionAuditSnapshot::from_metadata(&scheduled.metadata).unwrap();
        assert_eq!(audit.resolution.requested, SessionPermissionMode::Auto);
        assert_eq!(audit.resolution.effective, PermissionMode::Auto);

        assert_eq!(config.mode(), PermissionMode::Default);
        for interactive in [
            interactive_before,
            Session::new("interactive-concurrent", "model"),
        ] {
            assert!(interactive.agent_runtime_state.is_none());
            assert_eq!(
                ToolExecutionSessionFlags::from_session_and_configured_mode(
                    &interactive,
                    config.mode(),
                ),
                ToolExecutionSessionFlags::default()
            );
        }
    }

    #[test]
    fn scheduled_auto_allows_forced_ask_high_risk_while_interactive_still_asks() {
        let config = PermissionConfig::new();
        let resolver = WorkspaceResolver::from_process_globals();
        let scheduled = create_schedule_session(
            &test_job(),
            "model",
            "system",
            "base",
            None,
            None,
            &resolver,
        );
        let interactive = Session::new("interactive-gated", "model");

        assert!(matches!(
            config.evaluate(permission_evaluation(&scheduled, &config)),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::Auto,
                effective_policy: EffectivePermissionPolicy {
                    mode: PermissionMode::Auto,
                    auto_approve_requested: true,
                    ..
                }
            }
        ));
        assert!(matches!(
            config.evaluate(permission_evaluation(&interactive, &config)),
            PermissionOutcome::Ask(PermissionRequest {
                reason_code: PermissionReasonCode::HardDangerous,
                effective_mode: PermissionMode::Default,
                auto_approve_requested: false,
                ..
            })
        ));
    }

    #[test]
    fn schedule_publication_uses_the_validating_instance_workspace_root() {
        let instance_root = tempfile::tempdir().expect("instance workspace root");
        let relocated = instance_root.path().join("scheduled-workspace");
        let resolver = WorkspaceResolver::new(|| None, {
            let root = instance_root.path().to_path_buf();
            move || WorkspaceRootConfig {
                root: root.clone(),
                confine: true,
            }
        });
        let mut job = ScheduleRunJob {
            run_id: "run-instance-root".to_string(),
            schedule_id: "schedule-instance-root".to_string(),
            schedule_name: "instance root".to_string(),
            ..test_job()
        };
        job.run_config.workspace_path = Some(relocated.to_string_lossy().into_owned());

        let session = create_schedule_session(
            &job,
            "model",
            "system",
            "base",
            Some(ResolvedExecutionWorkspace {
                path: &relocated,
                source: WorkspaceSource::Explicit,
                binding_status: WorkspaceBindingStatus::Unregistered,
            }),
            None,
            &resolver,
        );

        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(relocated.to_string_lossy().as_ref())
        );
        assert!(
            relocated.is_dir(),
            "the AppState resolver must materialize its own validated target"
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(WorkspaceSource::Explicit.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some(WorkspaceBindingStatus::Unregistered.as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(relocated.clone())
        );
        let system = session
            .messages
            .iter()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
            .expect("clean Schedule System");
        assert_eq!(system.content, "system");
        assert!(!system
            .content
            .contains(relocated.to_string_lossy().as_ref()));
        assert!(!system.content.contains("BAMBOO_WORKSPACE_CONTEXT"));
        let snapshot = session
            .prompt_snapshot
            .as_ref()
            .expect("first prompt snapshot");
        assert_eq!(snapshot.effective_system_prompt, "system");
        let workspace_context = snapshot
            .workspace_context
            .as_deref()
            .expect("typed Workspace context in first snapshot");
        assert!(workspace_context.contains(relocated.to_string_lossy().as_ref()));
        assert!(workspace_context.contains("Workspace source: explicit"));
        assert!(workspace_context.contains("Binding status: unregistered"));
    }
}
