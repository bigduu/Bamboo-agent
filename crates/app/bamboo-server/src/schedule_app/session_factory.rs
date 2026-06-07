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
) -> Session {
    let session_id = uuid::Uuid::new_v4().to_string();
    let title = format!(
        "{} ({})",
        job.schedule_name,
        chrono::Utc::now().to_rfc3339()
    );

    let mut session = Session::new(session_id.clone(), model.to_string());
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
    if let Some(path) = workspace_path {
        session.set_workspace_path_meta(path);
        bamboo_tools::tools::workspace_state::ensure_session_workspace(
            &session_id,
            Some(std::path::PathBuf::from(path)),
        );
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
