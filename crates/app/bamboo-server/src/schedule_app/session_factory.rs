//! Helper for creating sessions from schedule run jobs.

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::{Message, Session};
use bamboo_engine::runner;

use super::manager::ScheduleRunJob;

/// Create and configure a new session for a scheduled run.
///
/// Sets up session metadata, workspace, system prompt, and optional initial user message.
pub fn create_schedule_session(
    job: &ScheduleRunJob,
    model: &str,
    system_prompt: &str,
    base_system_prompt: &str,
    workspace_path: Option<&str>,
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
    session.metadata.insert(
        bamboo_engine::session_app::chat::SESSION_START_SOURCE_METADATA_KEY.to_string(),
        "startup".to_string(),
    );
    session.title = title;
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
    if let Some(path) = workspace_path {
        let final_workspace = workspace_resolver.publish_resolved_workspace(
            &session_id,
            std::path::PathBuf::from(path),
            "schedule",
        );
        let final_workspace = bamboo_config::paths::path_to_display_string(&final_workspace);
        session.set_workspace_path_meta(final_workspace);
        if job.run_config.project_id.is_some() {
            let source = if job
                .run_config
                .workspace_path
                .as_deref()
                .is_some_and(|workspace| !workspace.trim().is_empty())
            {
                bamboo_engine::project_context::WorkspaceSource::Explicit
            } else {
                bamboo_engine::project_context::WorkspaceSource::ProjectDefault
            };
            session.metadata.insert(
                bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                source.as_str().to_string(),
            );
        }
    }
    if let Some(effort) = reasoning_effort {
        session.set_reasoning_effort_meta(effort.as_str());
    }
    session.add_message(Message::system(system_prompt.to_string()));
    runner::refresh_prompt_snapshot(&mut session);

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
    use bamboo_agent_core::workspace_state::{WorkspaceResolver, WorkspaceRootConfig};
    use bamboo_domain::ScheduleRunConfig;

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
        let job = ScheduleRunJob {
            run_id: "run-instance-root".to_string(),
            schedule_id: "schedule-instance-root".to_string(),
            schedule_name: "instance root".to_string(),
            run_config: ScheduleRunConfig::default(),
            scheduled_for: chrono::Utc::now(),
            claimed_at: chrono::Utc::now(),
            was_catch_up: false,
        };

        let session = create_schedule_session(
            &job,
            "model",
            "system",
            "base",
            Some(relocated.to_string_lossy().as_ref()),
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
    }
}
