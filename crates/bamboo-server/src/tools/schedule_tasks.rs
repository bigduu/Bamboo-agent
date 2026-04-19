use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::handlers::agent::schedules::ScheduleView;
use crate::schedules::{
    ScheduleManager, ScheduleRunConfig, ScheduleRunJob, ScheduleStore, ScheduleTrigger,
};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_agent_core::{Session, SessionKind};
use bamboo_infrastructure::SessionStoreV2;

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
        trigger: ScheduleTrigger,
        #[serde(default)]
        timezone: Option<String>,
        #[serde(default)]
        start_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        end_at: Option<chrono::DateTime<chrono::Utc>>,
        #[serde(default)]
        misfire_policy: Option<crate::schedules::MisFirePolicy>,
        #[serde(default)]
        overlap_policy: Option<crate::schedules::OverlapPolicy>,
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
        misfire_policy: Option<crate::schedules::MisFirePolicy>,
        #[serde(default)]
        overlap_policy: Option<crate::schedules::OverlapPolicy>,
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
                    "description": "Canonical schedule trigger definition for create/patch. Required for create."
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

        match parsed {
            ScheduleTasksArgs::List {} => {
                let items = self
                    .schedule_store
                    .list_schedules()
                    .await
                    .into_iter()
                    .map(ScheduleView::from)
                    .collect::<Vec<_>>();
                Ok(ToolResult {
                    success: true,
                    result: json!({ "schedules": items }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
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
                    .create_schedule_with_definition(
                        name,
                        enabled.unwrap_or(false),
                        run_config,
                        crate::schedules::store::ScheduleDefinitionChanges {
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
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "schedule": ScheduleView::from(created),
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
                    .patch_schedule_with_definition(
                        schedule_id.trim(),
                        name.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
                        enabled,
                        run_config,
                        crate::schedules::store::ScheduleDefinitionChanges {
                            trigger,
                            timezone,
                            start_at,
                            end_at,
                            misfire_policy,
                            overlap_policy,
                        },
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
                    result: json!({ "schedule": ScheduleView::from(schedule) }).to_string(),
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
                    .map_err(|e| ToolError::Execution(format!("Failed to create run job: {e}")))?
                else {
                    return Err(ToolError::Execution(format!(
                        "Schedule not found: {}",
                        schedule_id.trim()
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

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "success": true,
                        "schedule_id": claimed.schedule_id,
                        "run_id": claimed.run_id,
                        "enqueued_at": enqueued_at
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
                    .map(|e| crate::handlers::agent::sessions::SessionSummary::from_entry(e, false))
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
