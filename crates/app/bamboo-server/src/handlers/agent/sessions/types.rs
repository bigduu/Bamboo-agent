use serde::{Deserialize, Serialize};

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::ProviderModelRef;
use bamboo_engine::config::GoldConfig;
use bamboo_storage::SessionIndexEntry;

use bamboo_engine::model_config_helper::parse_session_gold_config;

#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: String,
    pub kind: bamboo_agent_core::SessionKind,
    pub title: String,
    pub title_version: u64,
    pub pinned: bool,
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    pub spawn_depth: u32,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_schedule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_run_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_activity_at: chrono::DateTime<chrono::Utc>,
    pub message_count: usize,
    pub has_attachments: bool,
    pub is_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<bamboo_agent_core::TokenBudgetUsage>,
    /// SubAgent profile id mirrored from `session.metadata["subagent_type"]`.
    /// Allows the frontend to render role badges on child-session lists
    /// without loading every session.json. Always `None` for root sessions
    /// and for legacy children created before this field was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    /// Whether the session currently has a pending question awaiting user response.
    /// Sourced from `SessionIndexEntry.has_pending_question` for list endpoints
    /// and from `session.has_pending_question()` for detail endpoints.
    #[serde(default)]
    pub has_pending_question: bool,
    /// Active plan mode runtime state mirrored into the lightweight session summary.
    /// Lets the frontend render plan-mode UI without loading full session history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_mode: Option<bamboo_domain::PlanModeState>,
    /// Number of child sessions currently running under this session.
    /// Computed dynamically at query time by scanning running sessions.
    #[serde(default)]
    pub running_child_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gold_config: Option<GoldConfig>,
}

impl SessionSummary {
    pub(crate) fn from_entry(entry: SessionIndexEntry, is_running: bool) -> Self {
        Self {
            id: entry.id,
            kind: entry.kind,
            title: entry.title,
            title_version: entry.title_version,
            pinned: entry.pinned,
            parent_session_id: entry.parent_session_id,
            root_session_id: entry.root_session_id,
            spawn_depth: entry.spawn_depth,
            model: entry.model,
            model_ref: entry.model_ref,
            provider: None,
            reasoning_effort: entry.reasoning_effort,
            created_by_schedule_id: entry.created_by_schedule_id,
            schedule_run_id: entry.schedule_run_id,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            last_activity_at: entry.last_activity_at,
            message_count: entry.message_count,
            has_attachments: entry.has_attachments,
            is_running,
            last_run_status: entry.last_run_status,
            last_run_error: entry.last_run_error,
            token_usage: entry.token_usage,
            subagent_type: entry.subagent_type,
            has_pending_question: entry.has_pending_question,
            plan_mode: entry.plan_mode,
            running_child_count: 0,
            gold_config: parse_session_gold_config(entry.gold_config_json.as_deref()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
}

/// Snapshot of an actively running session for frontend boot/reconnect replay.
#[derive(Debug, Serialize)]
pub struct RunningSessionEntry {
    pub session_id: String,
    pub run_id: String,
    pub started_at: String,
    pub round_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool_phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<String>,
    /// Recent critical events replayed for late subscribers.
    pub last_critical_events: Vec<bamboo_agent_core::AgentEvent>,
    /// IDs of child sessions currently running under this session.
    pub running_child_session_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RunningSessionsResponse {
    pub sessions: Vec<RunningSessionEntry>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default)]
    pub gold_config: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct CreateSessionResponse {
    pub session: SessionSummary,
}

#[derive(Debug, Serialize)]
pub struct GetSessionResponse {
    pub session: SessionSummary,
}

#[derive(Debug, Serialize)]
pub struct SessionSystemPromptResponse {
    pub session_id: String,
    pub base_system_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enhancement_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_guide_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dream_notebook: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_memory_note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_memory_index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevant_durable_memories: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_dream_fallback: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_memory_observability: Option<bamboo_agent_core::PromptMemoryObservability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "task_list", alias = "todo_list")]
    pub task_list: Option<String>,
    pub effective_system_prompt: String,
}

#[derive(Debug, Deserialize)]
pub struct PatchSessionRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub pinned: Option<bool>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default)]
    pub clear_reasoning_effort: Option<bool>,
    #[serde(default)]
    pub gold_config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ActivateDiscoverableToolsRequest {
    #[serde(default)]
    pub tools: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DiscoverableToolsResponse {
    pub session_id: String,
    pub tools: Vec<serde_json::Value>,
    pub activated: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CleanupRequest {
    pub mode: String,
    #[serde(default)]
    pub keep_pinned: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_create_session_request_minimal() {
        let json = r#"{}"#;
        let req: CreateSessionRequest = serde_json::from_str(json).unwrap();

        assert!(req.title.is_none());
        assert!(req.system_prompt.is_none());
        assert!(req.model.is_none());
        assert!(req.reasoning_effort.is_none());
    }

    #[test]
    fn test_create_session_request_full() {
        let json = r#"{"title":"Test Session","system_prompt":"You are helpful","model":"gpt-4","reasoning_effort":"high"}"#;
        let req: CreateSessionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.title, Some("Test Session".to_string()));
        assert_eq!(req.system_prompt, Some("You are helpful".to_string()));
        assert_eq!(req.model, Some("gpt-4".to_string()));
        assert_eq!(req.reasoning_effort, Some(ReasoningEffort::High));
    }

    #[test]
    fn test_create_session_request_debug() {
        let req = CreateSessionRequest {
            title: Some("Test".to_string()),
            system_prompt: None,
            model: None,
            provider: None,
            model_ref: None,
            reasoning_effort: None,
            gold_config: None,
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CreateSessionRequest"));
    }

    #[test]
    fn test_patch_session_request_partial() {
        let json = r#"{"title":"New Title"}"#;
        let req: PatchSessionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.title, Some("New Title".to_string()));
        assert!(req.pinned.is_none());
        assert!(req.model.is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.clear_reasoning_effort.is_none());
    }

    #[test]
    fn test_patch_session_request_both() {
        let json =
            r#"{"title":"New Title","pinned":true,"model":"gpt-5","reasoning_effort":"medium"}"#;
        let req: PatchSessionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.title, Some("New Title".to_string()));
        assert_eq!(req.pinned, Some(true));
        assert_eq!(req.model, Some("gpt-5".to_string()));
        assert_eq!(req.reasoning_effort, Some(ReasoningEffort::Medium));
    }

    #[test]
    fn test_patch_session_request_empty() {
        let json = r#"{}"#;
        let req: PatchSessionRequest = serde_json::from_str(json).unwrap();

        assert!(req.title.is_none());
        assert!(req.pinned.is_none());
        assert!(req.model.is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.clear_reasoning_effort.is_none());
    }

    #[test]
    fn test_cleanup_request_minimal() {
        let json = r#"{"mode":"all"}"#;
        let req: CleanupRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.mode, "all");
        assert!(!req.keep_pinned);
    }

    #[test]
    fn test_cleanup_request_with_keep_pinned() {
        let json = r#"{"mode":"old","keep_pinned":true}"#;
        let req: CleanupRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.mode, "old");
        assert!(req.keep_pinned);
    }

    #[test]
    fn test_list_sessions_response_serialization() {
        let response = ListSessionsResponse { sessions: vec![] };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"sessions\":[]"));
    }

    #[test]
    fn test_create_session_response_serialization() {
        let summary = SessionSummary {
            id: "test-id".to_string(),
            kind: bamboo_agent_core::SessionKind::Root,
            title: "Test".to_string(),
            title_version: 0,
            pinned: false,
            parent_session_id: None,
            root_session_id: "root-id".to_string(),
            spawn_depth: 0,
            model: "gpt-4".to_string(),
            model_ref: None,
            provider: None,
            reasoning_effort: Some(ReasoningEffort::Medium),
            created_by_schedule_id: None,
            schedule_run_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_activity_at: Utc::now(),
            message_count: 0,
            has_attachments: false,
            is_running: false,
            last_run_status: None,
            last_run_error: None,
            token_usage: None,
            subagent_type: None,
            has_pending_question: false,
            plan_mode: None,
            running_child_count: 0,
            gold_config: None,
        };

        let response = CreateSessionResponse { session: summary };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session\""));
        assert!(json.contains("\"test-id\""));
    }

    #[test]
    fn test_get_session_response_serialization() {
        let summary = SessionSummary {
            id: "session-123".to_string(),
            kind: bamboo_agent_core::SessionKind::Child,
            title: "My Session".to_string(),
            title_version: 0,
            pinned: true,
            parent_session_id: Some("parent-id".to_string()),
            root_session_id: "root-id".to_string(),
            spawn_depth: 1,
            model: "gpt-4.1".to_string(),
            model_ref: None,
            provider: None,
            reasoning_effort: None,
            created_by_schedule_id: None,
            schedule_run_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_activity_at: Utc::now(),
            message_count: 5,
            has_attachments: true,
            is_running: false,
            last_run_status: Some("success".to_string()),
            last_run_error: None,
            token_usage: None,
            subagent_type: None,
            has_pending_question: false,
            plan_mode: None,
            running_child_count: 0,
            gold_config: None,
        };

        let response = GetSessionResponse { session: summary };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session-123\""));
        assert!(json.contains("\"My Session\""));
        assert!(json.contains("\"pinned\":true"));
    }

    #[test]
    fn test_session_system_prompt_response_minimal() {
        let response = SessionSystemPromptResponse {
            session_id: "session-id".to_string(),
            base_system_prompt: "You are helpful".to_string(),
            enhancement_prompt: None,
            workspace_context: None,
            instruction_context: None,
            env_context: None,
            skill_context: None,
            tool_guide_context: None,
            dream_notebook: None,
            session_memory_note: None,
            project_memory_index: None,
            relevant_durable_memories: None,
            project_dream: None,
            global_dream_fallback: None,
            prompt_memory_observability: None,
            external_memory: None,
            task_list: None,
            effective_system_prompt: "You are helpful".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session_id\""));
        assert!(json.contains("\"You are helpful\""));
        assert!(!json.contains("\"enhancement_prompt\""));
    }

    #[test]
    fn test_session_system_prompt_response_full() {
        let response = SessionSystemPromptResponse {
            session_id: "session-id".to_string(),
            base_system_prompt: "Base".to_string(),
            enhancement_prompt: Some("Enhancement".to_string()),
            workspace_context: Some("Workspace".to_string()),
            instruction_context: Some("Instruction".to_string()),
            env_context: Some("Env".to_string()),
            skill_context: Some("Skill".to_string()),
            tool_guide_context: Some("Tool guide".to_string()),
            dream_notebook: Some("Dream".to_string()),
            session_memory_note: Some("Session note".to_string()),
            project_memory_index: Some("Project index".to_string()),
            relevant_durable_memories: Some("Relevant memories".to_string()),
            project_dream: Some("Project dream".to_string()),
            global_dream_fallback: Some("Global fallback".to_string()),
            prompt_memory_observability: Some(bamboo_agent_core::PromptMemoryObservability {
                project_prompt_injection_enabled: true,
                relevant_recall_enabled: true,
                relevant_recall_rerank_enabled: false,
                project_first_dream_enabled: true,
                latest_user_query_present: true,
                resolved_project_key: Some("project-key".to_string()),
                session_notes_status: "loaded".to_string(),
                project_memory_index_status: "loaded".to_string(),
                relevant_memory_status: "lexical".to_string(),
                project_dream_status: "loaded".to_string(),
                global_dream_fallback_status: "skipped_project_memory_or_dream_present".to_string(),
                dream_source: "project".to_string(),
                session_topic_count: 1,
                truncated_session_topic_count: 0,
                relevant_memory_count: 2,
                session_note_section_chars: 12,
                project_memory_index_section_chars: 34,
                relevant_memory_section_chars: 56,
                project_dream_section_chars: 78,
                global_dream_fallback_section_chars: 0,
                context_pressure_warning_chars: 0,
                external_memory_section_chars: 180,
            }),
            external_memory: Some("Memory".to_string()),
            task_list: Some("Task".to_string()),
            effective_system_prompt: "Full prompt".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"enhancement_prompt\""));
        assert!(json.contains("\"workspace_context\""));
        assert!(json.contains("\"skill_context\""));
        assert!(json.contains("\"project_memory_index\""));
        assert!(json.contains("\"relevant_durable_memories\""));
        assert!(json.contains("\"project_dream\""));
        assert!(json.contains("\"global_dream_fallback\""));
        assert!(json.contains("\"prompt_memory_observability\""));
    }

    #[test]
    fn test_session_summary_debug() {
        let summary = SessionSummary {
            id: "test".to_string(),
            kind: bamboo_agent_core::SessionKind::Root,
            title: "Test".to_string(),
            title_version: 0,
            pinned: false,
            parent_session_id: None,
            root_session_id: "root".to_string(),
            spawn_depth: 0,
            model: "gpt-4o".to_string(),
            model_ref: None,
            provider: None,
            reasoning_effort: Some(ReasoningEffort::Low),
            created_by_schedule_id: None,
            schedule_run_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_activity_at: Utc::now(),
            message_count: 0,
            has_attachments: false,
            is_running: false,
            last_run_status: None,
            last_run_error: None,
            token_usage: None,
            subagent_type: None,
            has_pending_question: false,
            plan_mode: None,
            running_child_count: 0,
            gold_config: None,
        };

        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("SessionSummary"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_cleanup_request_debug() {
        let req = CleanupRequest {
            mode: "all".to_string(),
            keep_pinned: true,
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CleanupRequest"));
    }
}
