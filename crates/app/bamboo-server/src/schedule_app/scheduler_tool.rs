use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use super::{ScheduleManager, ScheduleRunConfig, ScheduleRunJob, ScheduleStore, ScheduleTrigger};
use crate::handlers::agent::schedules::ScheduleView;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::{Session, SessionKind};
use bamboo_engine::model_config_helper::get_schedule_model_from_config;
use bamboo_llm::Config;
use bamboo_storage::SessionStoreV2;
use tokio::sync::RwLock;

/// One tool for schedule CRUD + actions.
///
/// We intentionally keep schedule operations as a server-only tool that calls internal
/// Rust methods (NOT http_request) to avoid SSRF issues and to keep latency low.
pub struct ScheduleTasksTool {
    schedule_store: Arc<ScheduleStore>,
    schedule_manager: Arc<ScheduleManager>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<Config>>,
    project_store: Arc<bamboo_projects::ProjectStore>,
    workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
}

impl ScheduleTasksTool {
    pub fn new(
        schedule_store: Arc<ScheduleStore>,
        schedule_manager: Arc<ScheduleManager>,
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
        config: Arc<RwLock<Config>>,
        project_store: Arc<bamboo_projects::ProjectStore>,
        workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    ) -> Self {
        Self {
            schedule_store,
            schedule_manager,
            session_store,
            storage,
            config,
            project_store,
            workspace_resolver,
        }
    }

    async fn load_caller_session(&self, session_id: &str) -> Result<Session, ToolError> {
        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => Ok(session),
            Ok(None) => Err(ToolError::Execution(format!(
                "session not found: {session_id}"
            ))),
            Err(e) => Err(ToolError::Execution(format!(
                "failed to load session {session_id}: {e}"
            ))),
        }
    }

    async fn validate_auto_execute_run_config(
        &self,
        run_config: &ScheduleRunConfig,
    ) -> Result<ScheduleRunConfig, ToolError> {
        let mut normalized = run_config.clone();
        if let Some(project_id) = run_config.project_id.as_ref() {
            match self.project_store.get(project_id) {
                Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
                Ok(_) => {
                    return Err(ToolError::InvalidArguments(format!(
                        "run_config.project_id references archived Project {project_id}"
                    )));
                }
                Err(error) => {
                    return Err(ToolError::InvalidArguments(format!(
                        "run_config.project_id is unavailable: {error}"
                    )));
                }
            }
        }
        let validated_workspace =
            crate::project_context::validate_workspace_assignment_with_resolver(
                &self.project_store,
                run_config.project_id.as_ref(),
                run_config.workspace_path.as_deref(),
                &self.workspace_resolver,
            )
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        // Validate the Project default now, but preserve omission in the
        // durable schedule. A missing explicit workspace means "resolve the
        // current Project path when this job fires", not "pin today's path
        // as an explicit override".
        normalized.workspace_path = run_config
            .workspace_path
            .as_deref()
            .map(str::trim)
            .filter(|workspace| !workspace.is_empty())
            .and(
                validated_workspace
                    .as_deref()
                    .map(bamboo_config::paths::path_to_display_string),
            );
        if !run_config.auto_execute {
            return Ok(normalized);
        }

        let has_task = run_config
            .task_message
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .is_some();
        if !has_task {
            return Err(ToolError::InvalidArguments(
                "run_config.task_message is required when auto_execute is true".to_string(),
            ));
        }

        let has_explicit_model = run_config
            .model
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .is_some();
        if has_explicit_model {
            return Ok(normalized);
        }

        let snapshot = self.config.read().await.clone();
        let provider = snapshot.provider.clone();
        get_schedule_model_from_config(&snapshot).map_err(|error| {
            ToolError::InvalidArguments(format!(
                "run_config.model not provided and no fast/default model configured for provider {}: {}",
                provider, error
            ))
        })?;

        Ok(normalized)
    }

    fn caller_project_id(caller: &Session) -> Result<Option<bamboo_domain::ProjectId>, ToolError> {
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            caller,
        ) {
            bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                Ok(Some(project_id))
            }
            bamboo_engine::project_context::SessionProjectIdentity::Unassigned => Ok(None),
            bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                Err(ToolError::InvalidArguments(format!(
                    "invalid_caller_project_identity: session carries invalid Project identity '{raw}': {message}"
                )))
            }
        }
    }

    fn apply_caller_project_identity(
        caller_project_id: Option<&bamboo_domain::ProjectId>,
        mut run_config: ScheduleRunConfig,
    ) -> Result<ScheduleRunConfig, ToolError> {
        match (caller_project_id, run_config.project_id.as_ref()) {
            (Some(caller_project_id), Some(requested)) if requested != caller_project_id => {
                return Err(ToolError::InvalidArguments(format!(
                    "project_scope_conflict: run_config.project_id cannot override caller Project '{caller_project_id}'"
                )));
            }
            (None, Some(requested)) => {
                return Err(ToolError::InvalidArguments(format!(
                    "project_scope_conflict: an Unassigned session cannot create or manage a schedule in Project '{requested}'"
                )));
            }
            _ => {}
        }
        run_config.project_id = caller_project_id.cloned();
        Ok(run_config)
    }

    fn schedule_in_caller_scope(
        caller_project_id: Option<&bamboo_domain::ProjectId>,
        schedule: &super::ScheduleEntry,
    ) -> bool {
        schedule.run_config.project_id.as_ref() == caller_project_id
    }

    fn require_schedule_in_caller_scope(
        caller_project_id: Option<&bamboo_domain::ProjectId>,
        schedule: &super::ScheduleEntry,
    ) -> Result<(), ToolError> {
        if Self::schedule_in_caller_scope(caller_project_id, schedule) {
            Ok(())
        } else {
            // Do not disclose the existence or identity of another Project's
            // schedule through this session-scoped tool.
            Err(ToolError::Execution(format!(
                "Schedule not found: {}",
                schedule.id
            )))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ScheduleTasksArgs {
    List {},
    Create {
        name: String,
        trigger: ScheduleTrigger,
        #[serde(default)]
        timezone: Option<String>,
        #[serde(default)]
        start_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        end_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        misfire_policy: Option<super::MisFirePolicy>,
        #[serde(default)]
        overlap_policy: Option<super::OverlapPolicy>,
        #[serde(default)]
        enabled: Option<bool>,
        #[serde(default)]
        run_config: Option<ScheduleRunConfig>,
    },
    Patch {
        schedule_id: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        enabled: Option<bool>,
        #[serde(default)]
        trigger: Option<ScheduleTrigger>,
        #[serde(default)]
        timezone: Option<String>,
        #[serde(default)]
        start_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        end_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        misfire_policy: Option<super::MisFirePolicy>,
        #[serde(default)]
        overlap_policy: Option<super::OverlapPolicy>,
        #[serde(default)]
        run_config: Option<ScheduleRunConfig>,
    },
    Delete {
        schedule_id: String,
    },
    RunNow {
        schedule_id: String,
    },
    ListSessions {
        schedule_id: String,
    },
}

#[async_trait]
impl Tool for ScheduleTasksTool {
    fn name(&self) -> &str {
        "scheduler"
    }

    fn description(&self) -> &str {
        "Manage Bamboo scheduled automation jobs (list/create/patch/delete/run_now/list_sessions). Server-only tool that calls the internal scheduler directly instead of HTTP. Child sessions cannot use this."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        // Keep schema permissive; the Rust parser enforces the action-specific requirements.
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "create", "patch", "delete", "run_now", "list_sessions"],
                    "description": "Which schedule operation to perform."
                },
                "schedule_id": { "type": "string", "description": "Schedule id for patch/delete/run_now/list_sessions." },
                "name": { "type": "string", "description": "Schedule name (create/patch)." },
                "enabled": { "type": "boolean", "description": "Enable/disable schedule (create/patch)." },
                "trigger": {
                    "type": "object",
                    "description": "Canonical schedule trigger definition for create/patch. Required for create. Tagged by 'type': interval {every_seconds}, once {at: RFC3339 UTC instant, fires exactly one time}, daily {hour,minute[,second]}, weekly {weekdays,hour,minute[,second]}, monthly {days,hour,minute[,second]}, cron {expr}."
                },
                "timezone": { "type": "string", "description": "Optional IANA timezone for calendar-based triggers." },
                "start_at": { "type": "string", "description": "Optional RFC3339 inclusive schedule window start." },
                "end_at": { "type": "string", "description": "Optional RFC3339 exclusive schedule window end." },
                "misfire_policy": { "type": "object", "description": "Optional misfire handling policy." },
                "overlap_policy": { "type": "string", "enum": ["allow", "skip", "queue_one"], "description": "Optional overlap policy." },
                "run_config": {
                    "type": "object",
                    "description": "Schedule run configuration (create/patch).",
                    "properties": {
                        "system_prompt": { "type": "string" },
                        "task_message": { "type": "string" },
                        "model": { "type": "string" },
                        "project_id": {
                            "type": "string",
                            "description": "Stable Project id for sessions created by this schedule. It must exactly match the caller session's Project; Unassigned callers cannot set one."
                        },
                        "reasoning_effort": {
                            "type": "string",
                            "enum": ["low", "medium", "high", "xhigh", "max"]
                        },
                        "workspace_path": { "type": "string" },
                        "enhance_prompt": { "type": "string" },
                        "auto_execute": { "type": "boolean" }
                    }
                }
            },
            "required": ["action"]
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let caller_session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("scheduler requires a session_id in tool context".to_string())
        })?;

        let caller = self.load_caller_session(caller_session_id).await?;
        if caller.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "scheduler is not allowed inside child sessions".to_string(),
            ));
        }

        let parsed: ScheduleTasksArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid scheduler args: {e}")))?;
        let caller_project_id = Self::caller_project_id(&caller)?;

        match parsed {
            ScheduleTasksArgs::List {} => {
                let items = self
                    .schedule_store
                    .list_schedules()
                    .await
                    .into_iter()
                    .filter(|schedule| {
                        Self::schedule_in_caller_scope(caller_project_id.as_ref(), schedule)
                    })
                    .map(ScheduleView::from)
                    .collect::<Vec<_>>();
                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({ "schedules": items }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
            ScheduleTasksArgs::Create {
                name,
                trigger,
                timezone,
                start_at,
                end_at,
                misfire_policy,
                overlap_policy,
                enabled,
                run_config,
            } => {
                let name = name.trim().to_string();
                if name.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "name must be a non-empty string".to_string(),
                    ));
                }
                if matches!(
                    trigger,
                    ScheduleTrigger::Interval {
                        every_seconds: 0,
                        ..
                    }
                ) {
                    return Err(ToolError::InvalidArguments(
                        "trigger.every_seconds must be > 0".to_string(),
                    ));
                }
                let run_config = Self::apply_caller_project_identity(
                    caller_project_id.as_ref(),
                    run_config.unwrap_or_default(),
                )?;
                let run_config = self.validate_auto_execute_run_config(&run_config).await?;

                let created = self
                    .schedule_store
                    .create_schedule_with_definition(
                        name,
                        enabled.unwrap_or(false),
                        run_config,
                        super::store::ScheduleDefinitionChanges {
                            trigger: Some(trigger),
                            timezone,
                            start_at,
                            end_at,
                            misfire_policy,
                            overlap_policy,
                        },
                    )
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to create schedule: {e}")))?;
                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({
                        "schedule": ScheduleView::from(created),
                        "note": "If run_config.auto_execute is false, scheduled runs will only create sessions (they will not run the agent loop)."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
            ScheduleTasksArgs::Patch {
                schedule_id,
                name,
                enabled,
                trigger,
                timezone,
                start_at,
                end_at,
                misfire_policy,
                overlap_policy,
                run_config,
            } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                if matches!(
                    trigger,
                    Some(ScheduleTrigger::Interval {
                        every_seconds: 0,
                        ..
                    })
                ) {
                    return Err(ToolError::InvalidArguments(
                        "trigger.every_seconds must be > 0".to_string(),
                    ));
                }

                let run_config = match run_config {
                    Some(cfg) => {
                        let cfg =
                            Self::apply_caller_project_identity(caller_project_id.as_ref(), cfg)?;
                        Some(self.validate_auto_execute_run_config(&cfg).await?)
                    }
                    None => None,
                };
                let existing = self
                    .schedule_store
                    .get_schedule(schedule_id.trim())
                    .await
                    .ok_or_else(|| {
                        ToolError::Execution(format!("Schedule not found: {}", schedule_id.trim()))
                    })?;
                Self::require_schedule_in_caller_scope(caller_project_id.as_ref(), &existing)?;

                let updated = self
                    .schedule_store
                    .patch_schedule_with_definition_in_project(
                        schedule_id.trim(),
                        name.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
                        enabled,
                        run_config,
                        super::store::ScheduleDefinitionChanges {
                            trigger,
                            timezone,
                            start_at,
                            end_at,
                            misfire_policy,
                            overlap_policy,
                        },
                        caller_project_id.as_ref(),
                    )
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to patch schedule: {e}")))?;

                let Some(schedule) = updated else {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {}",
                        schedule_id.trim()
                    )));
                };

                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({ "schedule": ScheduleView::from(schedule) }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
            ScheduleTasksArgs::Delete { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let schedule_id = schedule_id.trim();
                let schedule = self
                    .schedule_store
                    .get_schedule(schedule_id)
                    .await
                    .ok_or_else(|| {
                        ToolError::Execution(format!("Schedule not found: {schedule_id}"))
                    })?;
                Self::require_schedule_in_caller_scope(caller_project_id.as_ref(), &schedule)?;
                let deleted = self
                    .schedule_store
                    .delete_schedule_in_project(schedule_id, caller_project_id.as_ref())
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to delete schedule: {e}")))?;
                if !deleted {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {}",
                        schedule_id
                    )));
                }
                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({ "success": true, "schedule_id": schedule_id }).to_string(),
                    display_preference: Some("Default".to_string()),
                    images: Vec::new(),
                }))
            }
            ScheduleTasksArgs::RunNow { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let schedule_id = schedule_id.trim();
                let Some(schedule) = self.schedule_store.get_schedule(schedule_id).await else {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {schedule_id}"
                    )));
                };
                Self::require_schedule_in_caller_scope(caller_project_id.as_ref(), &schedule)?;
                self.validate_auto_execute_run_config(&schedule.run_config)
                    .await?;
                let Some(claimed) = self
                    .schedule_store
                    .create_run_now_if_config(schedule_id, &schedule.run_config)
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to create run job: {e}")))?
                else {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {schedule_id}"
                    )));
                };

                let enqueued_at = claimed.claimed_at;
                self.schedule_manager
                    .enqueue_run_now(ScheduleRunJob {
                        run_id: claimed.run_id.clone(),
                        schedule_id: claimed.schedule_id.clone(),
                        schedule_name: claimed.schedule_name.clone(),
                        run_config: claimed.run_config.clone(),
                        scheduled_for: claimed.scheduled_for,
                        claimed_at: claimed.claimed_at,
                        was_catch_up: claimed.was_catch_up,
                    })
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to enqueue run: {e}")))?;

                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({
                        "success": true,
                        "schedule_id": claimed.schedule_id,
                        "run_id": claimed.run_id,
                        "enqueued_at": enqueued_at
                    })
                    .to_string(),
                    display_preference: Some("Default".to_string()),
                    images: Vec::new(),
                }))
            }
            ScheduleTasksArgs::ListSessions { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let schedule_id = schedule_id.trim().to_string();
                let schedule = self
                    .schedule_store
                    .get_schedule(&schedule_id)
                    .await
                    .ok_or_else(|| {
                        ToolError::Execution(format!("Schedule not found: {schedule_id}"))
                    })?;
                Self::require_schedule_in_caller_scope(caller_project_id.as_ref(), &schedule)?;
                let sessions = self
                    .session_store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .filter(|entry| {
                        entry.created_by_schedule_id.as_deref() == Some(schedule_id.as_str())
                            && entry.project_id.as_deref()
                                == caller_project_id
                                    .as_ref()
                                    .map(|project_id| project_id.as_str())
                    })
                    .map(|e| crate::handlers::agent::sessions::SessionSummary::from_entry(e, false))
                    .collect::<Vec<_>>();

                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({ "schedule_id": schedule_id, "sessions": sessions }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{
        FunctionCall, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags,
    };
    use bamboo_domain::{PermissionAuditSnapshot, PermissionMode, SessionPermissionMode};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use futures::stream;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};

    struct ScriptedScheduleProvider {
        responses: Mutex<VecDeque<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
        calls: AtomicUsize,
        first_call_started: Arc<Notify>,
        release_first_call: Arc<Notify>,
    }

    #[async_trait]
    impl LLMProvider for ScriptedScheduleProvider {
        async fn chat_stream(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_call_started.notify_one();
                self.release_first_call.notified().await;
            }
            let chunks =
                self.responses.lock().await.pop_front().ok_or_else(|| {
                    LLMError::Api("scripted schedule provider exhausted".to_string())
                })?;
            Ok(Box::pin(stream::iter(chunks)))
        }
    }

    fn context(session_id: &str) -> ToolCtx {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id: "scheduler-test",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    }

    #[tokio::test]
    async fn auto_execute_schedule_runs_forced_ask_tool_without_approval_while_interactive_asks() {
        // The workspace suite runs many process- and server-backed tests in
        // parallel. Give the background scheduler enough time to be selected
        // even when those tests temporarily saturate the runtime.
        const BACKGROUND_SCHEDULE_TIMEOUT: Duration = Duration::from_secs(30);
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let workspace = dir.path().join("scheduled-project");
        std::fs::create_dir_all(&workspace).unwrap();
        let scheduled_output = workspace.join("scheduled-auto.txt");
        let interactive_output = workspace.join("interactive-must-not-run.txt");
        let scheduled_command = format!("printf scheduled-auto > {}", scheduled_output.display());
        let scheduled_call = ToolCall {
            id: "scheduled-forced-ask".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Bash".to_string(),
                arguments: serde_json::json!({"command": scheduled_command}).to_string(),
            },
        };
        let first_call_started = Arc::new(Notify::new());
        let release_first_call = Arc::new(Notify::new());
        let provider = Arc::new(ScriptedScheduleProvider {
            responses: Mutex::new(VecDeque::from([
                vec![
                    Ok(LLMChunk::ToolCalls(vec![scheduled_call])),
                    Ok(LLMChunk::Done),
                ],
                vec![
                    Ok(LLMChunk::Token("scheduled complete".to_string())),
                    Ok(LLMChunk::Done),
                ],
            ])),
            calls: AtomicUsize::new(0),
            first_call_started: first_call_started.clone(),
            release_first_call: release_first_call.clone(),
        });
        let state = crate::AppState::new_with_provider(
            dir.path().to_path_buf(),
            Config::default(),
            provider,
        )
        .await
        .expect("AppState with scripted provider");
        let permission_config = state
            .permission_checker
            .permission_config()
            .expect("config-backed permission checker");
        permission_config.set_ask_rules(["Bash(printf *)".to_string()]);
        assert_eq!(permission_config.mode(), PermissionMode::Default);

        let project = state
            .project_store
            .create_with_project_path(
                "Scheduled Auto",
                None,
                workspace.to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let mut caller = Session::new("scheduled-auto-caller", "model");
        caller.kind = SessionKind::Root;
        caller.set_project_id_meta(project.id.to_string());
        state.storage.save_session(&caller).await.unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );
        tool.invoke(
            json!({
                "action": "create",
                "name": "forced ask scheduled auto",
                "trigger": {"type": "interval", "every_seconds": 3600},
                "enabled": false,
                "run_config": {
                    "auto_execute": true,
                    "model": "test-model",
                    "task_message": "run the scripted forced-ask command"
                }
            }),
            context(&caller.id),
        )
        .await
        .expect("create auto schedule");
        let schedule = state.schedule_store.list_schedules().await.remove(0);
        let mut account_feed = state.account_sink.subscribe();
        tool.invoke(
            json!({"action": "run_now", "schedule_id": schedule.id}),
            context(&caller.id),
        )
        .await
        .expect("enqueue scheduled auto run");
        tokio::time::timeout(BACKGROUND_SCHEDULE_TIMEOUT, first_call_started.notified())
            .await
            .expect("scheduled agent reaches scripted provider");

        // The interactive request runs while the scheduled loop is active and
        // uses the exact same process-global Default permission config.
        let interactive = Session::new("concurrent-interactive", "model");
        permission_config.register_session_workspace(
            interactive.id.clone(),
            workspace.to_string_lossy().to_string(),
        );
        let interactive_command = format!("printf interactive > {}", interactive_output.display());
        let interactive_call = ToolCall {
            id: "interactive-forced-ask".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Bash".to_string(),
                arguments: serde_json::json!({"command": interactive_command}).to_string(),
            },
        };
        let (interactive_event_tx, mut interactive_event_rx) = tokio::sync::mpsc::channel(8);
        let interactive_result = state
            .tools_for(crate::tools::ToolSurface::Root)
            .execute_with_context(
                &interactive_call,
                ToolExecutionContext {
                    session_id: Some(&interactive.id),
                    tool_call_id: &interactive_call.id,
                    event_tx: Some(&interactive_event_tx),
                    available_tool_schemas: None,
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    bash_completion_sink: None,
                    pre_parsed_args: None,
                },
            )
            .await
            .expect("interactive forced ask returns an approval pause");
        assert_eq!(
            interactive_result.display_preference.as_deref(),
            Some("request_permissions")
        );
        let interactive_event =
            tokio::time::timeout(Duration::from_secs(2), interactive_event_rx.recv())
                .await
                .expect("interactive approval event arrives before timeout");
        assert!(matches!(
            interactive_event,
            Some(bamboo_agent_core::AgentEvent::ToolApprovalRequested { .. })
        ));
        assert!(!interactive_output.exists());
        assert_eq!(permission_config.mode(), PermissionMode::Default);

        release_first_call.notify_one();
        let terminal = tokio::time::timeout(BACKGROUND_SCHEDULE_TIMEOUT, async {
            loop {
                if let Some(record) = state
                    .schedule_store
                    .list_run_records_for_schedule(&schedule.id)
                    .await
                    .into_iter()
                    .find(|record| record.status.is_terminal())
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduled auto run reaches terminal state");
        assert_eq!(terminal.status, bamboo_domain::ScheduleRunStatus::Success);
        assert_eq!(
            std::fs::read_to_string(&scheduled_output).unwrap(),
            "scheduled-auto"
        );
        let scheduled_session_id = terminal.session_id.expect("scheduled session id");

        let mut scheduled_approval_seen = false;
        tokio::time::timeout(BACKGROUND_SCHEDULE_TIMEOUT, async {
            loop {
                let change = account_feed.recv().await.expect("account event");
                if change.session_id.as_deref() != Some(scheduled_session_id.as_str()) {
                    continue;
                }
                if matches!(
                    &change.event,
                    bamboo_agent_core::AgentEvent::ToolApprovalRequested { .. }
                ) {
                    scheduled_approval_seen = true;
                }
                if matches!(
                    &change.event,
                    bamboo_agent_core::AgentEvent::Complete { .. }
                        | bamboo_agent_core::AgentEvent::Cancelled { .. }
                        | bamboo_agent_core::AgentEvent::Error { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .expect("scheduled terminal event reaches account feed");
        assert!(
            !scheduled_approval_seen,
            "scheduled Auto must never emit a human approval request"
        );
        let saved = state
            .storage
            .load_session(&scheduled_session_id)
            .await
            .unwrap()
            .expect("saved scheduled session");
        assert!(saved.pending_question.is_none());
        let runtime = saved.agent_runtime_state.as_ref().unwrap();
        assert_eq!(runtime.permission_mode, SessionPermissionMode::Auto);
        assert!(runtime.bypass_permissions);
        assert!(runtime.no_human_approver);
    }

    #[tokio::test]
    async fn run_now_tool_rejects_unavailable_project_before_run_creation() {
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let state = crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("AppState");
        let mut caller = Session::new("scheduler-caller", "model");
        caller.kind = SessionKind::Root;
        state.storage.save_session(&caller).await.unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );

        let project = state.project_store.create("Scheduled", None).unwrap();
        let archived = state
            .schedule_store
            .create_schedule(
                "archived".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 3600,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig {
                    project_id: Some(project.id.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        state
            .project_store
            .archive(&project.id, project.revision)
            .unwrap();
        let missing = state
            .schedule_store
            .create_schedule(
                "missing".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 3600,
                    anchor_at: None,
                },
                true,
                ScheduleRunConfig {
                    project_id: Some("project-missing".parse().unwrap()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        for schedule in [archived, missing] {
            caller.set_project_id_meta(
                schedule
                    .run_config
                    .project_id
                    .as_ref()
                    .expect("test schedule Project")
                    .to_string(),
            );
            state.storage.save_session(&caller).await.unwrap();
            let error = tool
                .invoke(
                    json!({"action":"run_now", "schedule_id":schedule.id.clone()}),
                    context(&caller.id),
                )
                .await
                .expect_err("unavailable Project must reject run_now");
            assert!(matches!(error, ToolError::InvalidArguments(_)));
            assert!(state
                .schedule_store
                .list_run_records_for_schedule(&schedule.id)
                .await
                .is_empty());
        }
    }

    #[tokio::test]
    async fn create_inherits_caller_project_and_fired_session_keeps_membership() {
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let state = crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("AppState");
        let project_path = tempfile::tempdir().expect("Project path");
        let project = state
            .project_store
            .create_with_project_path(
                "Scheduled",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let mut caller = Session::new("project-scheduler-caller", "model");
        caller.kind = SessionKind::Root;
        caller.set_project_id_meta(project.id.to_string());
        state.storage.save_session(&caller).await.unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );
        assert_eq!(
            tool.parameters_schema()["properties"]["run_config"]["properties"]["project_id"]
                ["type"],
            "string"
        );

        tool.invoke(
            json!({
                "action": "create",
                "name": "inherit caller Project",
                "trigger": {"type": "interval", "every_seconds": 3600},
                "enabled": false,
                "run_config": {"auto_execute": false, "model": "test-model"}
            }),
            context(&caller.id),
        )
        .await
        .expect("create schedule");
        let schedules = state.schedule_store.list_schedules().await;
        assert_eq!(schedules.len(), 1);
        let schedule = &schedules[0];
        assert_eq!(schedule.run_config.project_id.as_ref(), Some(&project.id));
        assert!(
            schedule.run_config.workspace_path.is_none(),
            "Project fallback must stay symbolic in durable schedule config"
        );
        let moved_project_path = tempfile::tempdir().expect("Moved Project path");
        let updated_project = state
            .project_store
            .update_with_project_path(
                &project.id,
                project.revision,
                moved_project_path.path().to_string_lossy().as_ref(),
                |_| Ok(()),
            )
            .expect("move Project path before schedule fires");

        tool.invoke(
            json!({"action": "run_now", "schedule_id": schedule.id}),
            context(&caller.id),
        )
        .await
        .expect("run schedule");

        let fired = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(entry) = state
                    .session_store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|entry| {
                        entry.created_by_schedule_id.as_deref() == Some(schedule.id.as_str())
                    })
                {
                    break entry;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("scheduled session should be persisted");
        assert_eq!(fired.project_id.as_deref(), Some(project.id.as_str()));
        assert_eq!(
            fired.workspace_path.as_deref(),
            updated_project.project_path.as_deref()
        );
        let fired_session = state
            .storage
            .load_session(&fired.id)
            .await
            .expect("load fired session")
            .expect("fired session");
        let runtime = fired_session
            .agent_runtime_state
            .as_ref()
            .expect("scheduled session permission posture");
        assert_eq!(runtime.permission_mode, SessionPermissionMode::Auto);
        assert!(
            runtime.bypass_permissions,
            "legacy Auto compatibility mirror"
        );
        assert!(runtime.no_human_approver);
        let initial_audit =
            PermissionAuditSnapshot::from_metadata(&fired_session.metadata).unwrap();
        assert_eq!(
            initial_audit.resolution.requested,
            SessionPermissionMode::Auto
        );
        assert_eq!(initial_audit.resolution.effective, PermissionMode::Auto);

        let permission_config = state
            .permission_checker
            .permission_config()
            .expect("config-backed permission checker");
        assert_eq!(permission_config.mode(), PermissionMode::Default);
        let interactive = Session::new("concurrent-interactive", "model");
        assert_eq!(
            ToolExecutionSessionFlags::from_session_and_configured_mode(
                &interactive,
                permission_config.mode(),
            ),
            ToolExecutionSessionFlags::default()
        );

        let mut first_runtime_carry_forward = fired_session.clone();
        first_runtime_carry_forward.set_last_run_status("carried-forward");
        state
            .persistence
            .merge_save_runtime(&mut first_runtime_carry_forward)
            .await
            .expect("ordinary runtime carry-forward");
        let carried = state
            .storage
            .load_session(&fired.id)
            .await
            .expect("load carried scheduled session")
            .expect("carried scheduled session");
        let carried_runtime = carried.agent_runtime_state.as_ref().unwrap();
        assert_eq!(carried_runtime.permission_mode, SessionPermissionMode::Auto);
        assert!(carried_runtime.bypass_permissions);
        assert!(carried_runtime.no_human_approver);
        assert_eq!(
            PermissionAuditSnapshot::from_metadata(&carried.metadata).unwrap(),
            initial_audit
        );
        assert_eq!(
            bamboo_engine::project_context::ProjectContextResolver::project_id_from_session(
                &fired_session
            )
            .as_ref(),
            Some(&project.id)
        );
        assert_eq!(
            fired_session.workspace_path_meta().as_deref(),
            updated_project.project_path.as_deref()
        );
        assert_eq!(
            fired_session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str())
        );
    }

    #[tokio::test]
    async fn create_rejects_project_override_and_invalid_caller_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let state = crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("AppState");
        let caller_project = state.project_store.create("Caller", None).unwrap();
        let other_project = state.project_store.create("Other", None).unwrap();
        let mut caller = Session::new("project-scheduler-caller", "model");
        caller.set_project_id_meta(caller_project.id.to_string());
        state.storage.save_session(&caller).await.unwrap();
        let mut invalid = Session::new("invalid-scheduler-caller", "model");
        invalid.set_project_id_meta("../invalid".to_string());
        state.storage.save_session(&invalid).await.unwrap();
        let unassigned = Session::new("unassigned-scheduler-caller", "model");
        state.storage.save_session(&unassigned).await.unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );

        let base_args = |project_id: Option<&bamboo_domain::ProjectId>| {
            json!({
                "action": "create",
                "name": "must not persist",
                "trigger": {"type": "interval", "every_seconds": 3600},
                "run_config": {
                    "auto_execute": false,
                    "project_id": project_id.map(ToString::to_string)
                }
            })
        };
        let error = tool
            .invoke(base_args(Some(&other_project.id)), context(&caller.id))
            .await
            .expect_err("assigned caller cannot cross Project");
        assert!(
            matches!(error, ToolError::InvalidArguments(ref message) if message.contains("project_scope_conflict"))
        );
        let error = tool
            .invoke(base_args(Some(&other_project.id)), context(&unassigned.id))
            .await
            .expect_err("Unassigned caller cannot enter a Project through a schedule");
        assert!(
            matches!(error, ToolError::InvalidArguments(ref message) if message.contains("project_scope_conflict"))
        );
        let error = tool
            .invoke(base_args(None), context(&invalid.id))
            .await
            .expect_err("malformed caller Project must fail closed");
        assert!(
            matches!(error, ToolError::InvalidArguments(ref message) if message.contains("invalid_caller_project_identity"))
        );
        assert!(state.schedule_store.list_schedules().await.is_empty());
    }

    #[tokio::test]
    async fn same_scope_patch_without_project_id_preserves_membership() {
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let state = crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("AppState");
        let project_path = tempfile::tempdir().expect("Project path");
        let project = state
            .project_store
            .create_with_project_path(
                "Scheduled",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let mut caller = Session::new("assigned-scheduler-caller", "model");
        caller.set_project_id_meta(project.id.to_string());
        state.storage.save_session(&caller).await.unwrap();
        let schedule = state
            .schedule_store
            .create_schedule(
                "preserve membership".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 3600,
                    anchor_at: None,
                },
                false,
                ScheduleRunConfig {
                    project_id: Some(project.id.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );

        tool.invoke(
            json!({
                "action": "patch",
                "schedule_id": schedule.id,
                "run_config": {
                    "task_message": "updated",
                    "auto_execute": false
                }
            }),
            context(&caller.id),
        )
        .await
        .expect("patch schedule");
        let updated = state
            .schedule_store
            .get_schedule(&schedule.id)
            .await
            .expect("updated schedule");
        assert_eq!(updated.run_config.project_id.as_ref(), Some(&project.id));
        assert_eq!(updated.run_config.task_message.as_deref(), Some("updated"));
    }

    #[tokio::test]
    async fn foreign_project_schedules_are_hidden_and_rejected_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(dir.path().to_path_buf());
        let state = crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("AppState");
        let caller_project = state.project_store.create("Caller", None).unwrap();
        let foreign_project = state.project_store.create("Foreign", None).unwrap();
        let mut caller = Session::new("schedule-scope-caller", "model");
        caller.set_project_id_meta(caller_project.id.to_string());
        state.storage.save_session(&caller).await.unwrap();
        let foreign = state
            .schedule_store
            .create_schedule(
                "foreign schedule".to_string(),
                ScheduleTrigger::Interval {
                    every_seconds: 3600,
                    anchor_at: None,
                },
                false,
                ScheduleRunConfig {
                    project_id: Some(foreign_project.id.clone()),
                    model: Some("test-model".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let tool = ScheduleTasksTool::new(
            state.schedule_store.clone(),
            state.schedule_manager.clone(),
            state.session_store.clone(),
            state.storage.clone(),
            state.config.clone(),
            state.project_store.clone(),
            state.workspace_resolver.clone(),
        );

        let listed = tool
            .invoke(json!({"action": "list"}), context(&caller.id))
            .await
            .expect("list caller schedules");
        let ToolOutcome::Completed(listed) = listed else {
            panic!("list should complete");
        };
        let listed: serde_json::Value =
            serde_json::from_str(&listed.result).expect("list result JSON");
        assert_eq!(listed["schedules"], serde_json::json!([]));

        for args in [
            json!({
                "action": "patch",
                "schedule_id": foreign.id.clone(),
                "name": "must not persist"
            }),
            json!({"action": "run_now", "schedule_id": foreign.id.clone()}),
            json!({"action": "delete", "schedule_id": foreign.id.clone()}),
            json!({"action": "list_sessions", "schedule_id": foreign.id.clone()}),
        ] {
            let error = tool
                .invoke(args, context(&caller.id))
                .await
                .expect_err("foreign schedule must be inaccessible");
            assert!(
                matches!(error, ToolError::Execution(ref message) if message.contains("Schedule not found"))
            );
        }

        let unchanged = state
            .schedule_store
            .get_schedule(&foreign.id)
            .await
            .expect("foreign schedule must remain");
        assert_eq!(unchanged.name, "foreign schedule");
        assert_eq!(
            unchanged.run_config.project_id.as_ref(),
            Some(&foreign_project.id)
        );
        assert!(state
            .schedule_store
            .list_run_records_for_schedule(&foreign.id)
            .await
            .is_empty());
    }
}
