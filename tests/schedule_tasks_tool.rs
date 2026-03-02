//! Integration tests for the server-only `schedule_tasks` tool.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};

use bamboo_agent::agent::core::storage::{SessionStoreV2, Storage};
use bamboo_agent::agent::core::tools::{Tool, ToolExecutionContext, ToolExecutor, ToolResult};
use bamboo_agent::agent::core::{AgentEvent, Message, Session};
use bamboo_agent::agent::llm::provider::{LLMProvider, LLMStream};
use bamboo_agent::agent::llm::provider::{Result as LLMResult};
use bamboo_agent::agent::metrics::collector::MetricsCollector;
use bamboo_agent::agent::metrics::storage::SqliteMetricsStorage;
use bamboo_agent::agent::skill::SkillManager;
use bamboo_agent::server::app_state::AgentRunner;
use bamboo_agent::server::schedules::manager::ScheduleContext;
use bamboo_agent::server::schedules::{ScheduleManager, ScheduleRunConfig, ScheduleStore};
use bamboo_agent::server::tools::ScheduleTasksTool;

mod common;

#[derive(Clone)]
struct DummyProvider;

#[async_trait]
impl LLMProvider for DummyProvider {
    async fn chat_stream(
        &self,
        _messages: &[bamboo_agent::agent::core::Message],
        _tools: &[bamboo_agent::agent::core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> LLMResult<LLMStream> {
        // Never used in these tests (we set auto_execute=false).
        Ok(Box::pin(stream::empty()))
    }
}

struct NoopTools;

#[async_trait]
impl ToolExecutor for NoopTools {
    async fn execute(
        &self,
        _call: &bamboo_agent::agent::core::tools::ToolCall,
    ) -> std::result::Result<ToolResult, bamboo_agent::agent::core::tools::ToolError> {
        Err(bamboo_agent::agent::core::tools::ToolError::NotFound(
            "noop".to_string(),
        ))
    }

    fn list_tools(&self) -> Vec<bamboo_agent::agent::core::tools::ToolSchema> {
        vec![]
    }
}

fn ctx_for_session<'a>(session_id: &'a str) -> ToolExecutionContext<'a> {
    ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool_call",
        event_tx: None,
    }
}

#[tokio::test]
async fn schedule_tasks_requires_session_id() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    let metrics = MetricsCollector::spawn(metrics_storage, 1);

    let manager = Arc::new(ScheduleManager::new(ScheduleContext {
        schedule_store: schedule_store.clone(),
        session_store: store.clone(),
        storage: store.clone(),
        provider: Arc::new(DummyProvider),
        tools: Arc::new(NoopTools),
        skill_manager: Arc::new(SkillManager::new()),
        metrics_collector: metrics,
        sessions_cache: Arc::new(RwLock::new(HashMap::new())),
        agent_runners: Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new())),
        session_event_senders: Arc::new(RwLock::new(
            HashMap::<String, broadcast::Sender<AgentEvent>>::new(),
        )),
        config: Arc::new(RwLock::new(bamboo_agent::core::Config::default())),
    }));

    let tool = ScheduleTasksTool::new(
        schedule_store,
        manager,
        store.clone(),
        store,
    );

    let err = tool
        .execute_with_context(
            serde_json::json!({ "action": "list" }),
            ToolExecutionContext::none("tool_call"),
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("requires a session_id"), "got: {msg}");
}

#[tokio::test]
async fn schedule_tasks_crud_and_list_sessions() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    let metrics = MetricsCollector::spawn(metrics_storage, 1);

    let manager = Arc::new(ScheduleManager::new(ScheduleContext {
        schedule_store: schedule_store.clone(),
        session_store: store.clone(),
        storage: store.clone(),
        provider: Arc::new(DummyProvider),
        tools: Arc::new(NoopTools),
        skill_manager: Arc::new(SkillManager::new()),
        metrics_collector: metrics,
        sessions_cache: Arc::new(RwLock::new(HashMap::new())),
        agent_runners: Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new())),
        session_event_senders: Arc::new(RwLock::new(
            HashMap::<String, broadcast::Sender<AgentEvent>>::new(),
        )),
        config: Arc::new(RwLock::new(bamboo_agent::core::Config::default())),
    }));

    let tool = ScheduleTasksTool::new(
        schedule_store.clone(),
        manager,
        store.clone(),
        store.clone(),
    );

    // Create a caller root session (tool checks SessionKind::Root).
    let mut caller = Session::new("root-session", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    // Create schedule.
    let created = tool
        .execute_with_context(
            serde_json::json!({
                "action": "create",
                "name": "My Schedule",
                "interval_seconds": 60,
                "enabled": false,
                "run_config": { "auto_execute": false }
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    let created_v: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let schedule_id = created_v["schedule"]["id"].as_str().unwrap().to_string();
    assert!(!schedule_id.is_empty());

    // List schedules should include it.
    let listed = tool
        .execute_with_context(
            serde_json::json!({ "action": "list" }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    let listed_v: serde_json::Value = serde_json::from_str(&listed.result).unwrap();
    assert!(
        listed_v["schedules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["id"].as_str() == Some(schedule_id.as_str())),
        "schedule missing from list: {}",
        listed.result
    );

    // Patch schedule.
    let patched = tool
        .execute_with_context(
            serde_json::json!({
                "action": "patch",
                "schedule_id": schedule_id,
                "enabled": true,
                "interval_seconds": 120
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    let patched_v: serde_json::Value = serde_json::from_str(&patched.result).unwrap();
    assert_eq!(patched_v["schedule"]["enabled"].as_bool(), Some(true));
    assert_eq!(patched_v["schedule"]["interval_seconds"].as_u64(), Some(120));

    // Create a session that looks like it was created by schedule.
    let mut scheduled = Session::new("scheduled-session", "test-model");
    scheduled
        .metadata
        .insert("created_by_schedule_id".to_string(), patched_v["schedule"]["id"].as_str().unwrap().to_string());
    scheduled.add_message(Message::system("x".to_string()));
    store.save_session(&scheduled).await.unwrap();

    let sessions = tool
        .execute_with_context(
            serde_json::json!({
                "action": "list_sessions",
                "schedule_id": patched_v["schedule"]["id"]
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    let sessions_v: serde_json::Value = serde_json::from_str(&sessions.result).unwrap();
    assert!(
        sessions_v["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["id"].as_str() == Some("scheduled-session")),
        "scheduled session missing: {}",
        sessions.result
    );

    // Delete schedule.
    let deleted = tool
        .execute_with_context(
            serde_json::json!({ "action": "delete", "schedule_id": patched_v["schedule"]["id"] }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(&deleted.result)
        .unwrap()["success"]
        .as_bool()
        .unwrap());
}

#[tokio::test]
async fn schedule_run_skips_when_no_model_available() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    let metrics = MetricsCollector::spawn(metrics_storage, 1);

    // Default config has no model for most providers (openai/anthropic/gemini), so scheduled runs
    // without an explicit model should be skipped (no session created).
    let manager = Arc::new(ScheduleManager::new(ScheduleContext {
        schedule_store: schedule_store.clone(),
        session_store: store.clone(),
        storage: store.clone(),
        provider: Arc::new(DummyProvider),
        tools: Arc::new(NoopTools),
        skill_manager: Arc::new(SkillManager::new()),
        metrics_collector: metrics,
        sessions_cache: Arc::new(RwLock::new(HashMap::new())),
        agent_runners: Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new())),
        session_event_senders: Arc::new(RwLock::new(
            HashMap::<String, broadcast::Sender<AgentEvent>>::new(),
        )),
        config: Arc::new(RwLock::new(bamboo_agent::core::Config::default())),
    }));

    manager
        .enqueue_run_now(bamboo_agent::server::schedules::ScheduleRunJob {
            schedule_id: "s1".to_string(),
            schedule_name: "No Model".to_string(),
            run_config: ScheduleRunConfig {
                auto_execute: false,
                ..Default::default()
            },
            claimed_at: Utc::now(),
        })
        .await
        .unwrap();

    // Best-effort wait for the background worker to process the job.
    for _ in 0..20 {
        let entries = store.list_index_entries().await;
        if entries.is_empty() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }

    let entries = store.list_index_entries().await;
    assert!(entries.is_empty(), "expected no sessions, got: {entries:?}");
}

#[tokio::test]
async fn schedule_run_uses_config_get_model_fallback() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    let metrics = MetricsCollector::spawn(metrics_storage, 1);

    // Copilot has a built-in get_model() fallback ("gpt-4o") even when not configured.
    let mut cfg = bamboo_agent::core::Config::default();
    cfg.provider = "copilot".to_string();
    let expected_model = cfg.get_model().expect("copilot should always have a model fallback");

    let manager = Arc::new(ScheduleManager::new(ScheduleContext {
        schedule_store: schedule_store.clone(),
        session_store: store.clone(),
        storage: store.clone(),
        provider: Arc::new(DummyProvider),
        tools: Arc::new(NoopTools),
        skill_manager: Arc::new(SkillManager::new()),
        metrics_collector: metrics,
        sessions_cache: Arc::new(RwLock::new(HashMap::new())),
        agent_runners: Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new())),
        session_event_senders: Arc::new(RwLock::new(
            HashMap::<String, broadcast::Sender<AgentEvent>>::new(),
        )),
        config: Arc::new(RwLock::new(cfg)),
    }));

    manager
        .enqueue_run_now(bamboo_agent::server::schedules::ScheduleRunJob {
            schedule_id: "s2".to_string(),
            schedule_name: "Config Model Fallback".to_string(),
            run_config: ScheduleRunConfig {
                auto_execute: false,
                ..Default::default()
            },
            claimed_at: Utc::now(),
        })
        .await
        .unwrap();

    let mut created_id: Option<String> = None;
    for _ in 0..40 {
        let entries = store.list_index_entries().await;
        if let Some(first) = entries.first() {
            created_id = Some(first.id.clone());
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    let session_id = created_id.expect("expected a scheduled session to be created");
    let session = store
        .load_session(&session_id)
        .await
        .unwrap()
        .expect("session exists");
    assert_eq!(session.model, expected_model);
}
