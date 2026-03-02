use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::agent::core::storage::{SessionStoreV2, Storage};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{Session, SessionKind};
use crate::server::schedules::store::ScheduleRunConfig;
use crate::server::schedules::{ScheduleManager, ScheduleRunJob, ScheduleStore};

/// One tool for schedule CRUD + actions.
///
/// We intentionally keep schedule operations as a server-only tool that calls internal
/// Rust methods (NOT http_request) to avoid SSRF issues and to keep latency low.
pub struct ScheduleTasksTool {
    schedule_store: Arc<ScheduleStore>,
    schedule_manager: Arc<ScheduleManager>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
}

impl ScheduleTasksTool {
    pub fn new(
        schedule_store: Arc<ScheduleStore>,
        schedule_manager: Arc<ScheduleManager>,
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            schedule_store,
            schedule_manager,
            session_store,
            storage,
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
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ScheduleTasksArgs {
    List {},
    Create {
        name: String,
        interval_seconds: u64,
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
        interval_seconds: Option<u64>,
        #[serde(default)]
        run_config: Option<ScheduleRunConfig>,
    },
    Delete { schedule_id: String },
    RunNow { schedule_id: String },
    ListSessions { schedule_id: String },
}

#[async_trait]
impl Tool for ScheduleTasksTool {
    fn name(&self) -> &str {
        "schedule_tasks"
    }

    fn description(&self) -> &str {
        "Manage scheduler tasks (list/create/patch/delete/run_now/list_sessions). Server-only tool that calls internal schedule store (not HTTP). Child sessions cannot use this."
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
                "interval_seconds": { "type": "number", "description": "Interval in seconds (create/patch)." },
                "run_config": {
                    "type": "object",
                    "description": "Schedule run configuration (create/patch).",
                    "properties": {
                        "system_prompt": { "type": "string" },
                        "task_message": { "type": "string" },
                        "model": { "type": "string" },
                        "workspace_path": { "type": "string" },
                        "enhance_prompt": { "type": "string" },
                        "auto_execute": { "type": "boolean" }
                    }
                }
            },
            "required": ["action"]
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
        let caller_session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("schedule_tasks requires a session_id in tool context".to_string())
        })?;

        let caller = self.load_caller_session(caller_session_id).await?;
        if caller.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "schedule_tasks is not allowed inside child sessions".to_string(),
            ));
        }

        let parsed: ScheduleTasksArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid schedule_tasks args: {e}"))
        })?;

        match parsed {
            ScheduleTasksArgs::List {} => {
                let items = self.schedule_store.list_schedules().await;
                Ok(ToolResult {
                    success: true,
                    result: json!({ "schedules": items }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
            ScheduleTasksArgs::Create {
                name,
                interval_seconds,
                enabled,
                run_config,
            } => {
                let name = name.trim().to_string();
                if name.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "name must be a non-empty string".to_string(),
                    ));
                }
                if interval_seconds == 0 {
                    return Err(ToolError::InvalidArguments(
                        "interval_seconds must be > 0".to_string(),
                    ));
                }
                let run_config = run_config.unwrap_or_default();
                if run_config.auto_execute {
                    let has_task = run_config
                        .task_message
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .is_some();
                    if !has_task {
                        return Err(ToolError::InvalidArguments(
                            "run_config.task_message is required when auto_execute is true"
                                .to_string(),
                        ));
                    }
                }

                let created = self
                    .schedule_store
                    .create_schedule(
                        name,
                        interval_seconds,
                        enabled.unwrap_or(false),
                        run_config,
                    )
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to create schedule: {e}")))?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "schedule": created,
                        "note": "If run_config.auto_execute is false, scheduled runs will only create sessions (they will not run the agent loop)."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
            ScheduleTasksArgs::Patch {
                schedule_id,
                name,
                enabled,
                interval_seconds,
                run_config,
            } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                if matches!(interval_seconds, Some(0)) {
                    return Err(ToolError::InvalidArguments(
                        "interval_seconds must be > 0".to_string(),
                    ));
                }

                if let Some(cfg) = run_config.as_ref() {
                    if cfg.auto_execute {
                        let has_task = cfg
                            .task_message
                            .as_deref()
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .is_some();
                        if !has_task {
                            return Err(ToolError::InvalidArguments(
                                "run_config.task_message is required when auto_execute is true"
                                    .to_string(),
                            ));
                        }
                    }
                }

                let updated = self
                    .schedule_store
                    .patch_schedule(
                        schedule_id.trim(),
                        name.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
                        enabled,
                        interval_seconds,
                        run_config,
                    )
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to patch schedule: {e}")))?;

                let Some(schedule) = updated else {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {}",
                        schedule_id.trim()
                    )));
                };

                Ok(ToolResult {
                    success: true,
                    result: json!({ "schedule": schedule }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
            ScheduleTasksArgs::Delete { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let deleted = self
                    .schedule_store
                    .delete_schedule(schedule_id.trim())
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to delete schedule: {e}")))?;
                if !deleted {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {}",
                        schedule_id.trim()
                    )));
                }
                Ok(ToolResult {
                    success: true,
                    result: json!({ "success": true, "schedule_id": schedule_id.trim() })
                        .to_string(),
                    display_preference: Some("Default".to_string()),
                })
            }
            ScheduleTasksArgs::RunNow { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let Some(claimed) = self
                    .schedule_store
                    .create_run_now(schedule_id.trim())
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to create run job: {e}")))? else {
                        return Err(ToolError::Execution(format!(
                            "Schedule not found: {}",
                            schedule_id.trim()
                        )));
                    };

                let now = Utc::now();
                self.schedule_manager
                    .enqueue_run_now(ScheduleRunJob {
                        schedule_id: claimed.schedule_id.clone(),
                        schedule_name: claimed.schedule_name.clone(),
                        run_config: claimed.run_config.clone(),
                        claimed_at: now,
                    })
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to enqueue run: {e}")))?;

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "success": true,
                        "schedule_id": claimed.schedule_id,
                        "enqueued_at": now
                    })
                    .to_string(),
                    display_preference: Some("Default".to_string()),
                })
            }
            ScheduleTasksArgs::ListSessions { schedule_id } => {
                if schedule_id.trim().is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "schedule_id must be a non-empty string".to_string(),
                    ));
                }
                let schedule_id = schedule_id.trim().to_string();
                let sessions = self
                    .session_store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .filter(|e| e.created_by_schedule_id.as_deref() == Some(schedule_id.as_str()))
                    .map(|e| {
                        crate::server::handlers::agent::sessions::SessionSummary::from_entry(
                            e,
                            false,
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(ToolResult {
                    success: true,
                    result: json!({ "schedule_id": schedule_id, "sessions": sessions }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
        }
    }
}
