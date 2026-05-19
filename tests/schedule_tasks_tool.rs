//! Integration tests for the server-only `scheduler` tool (legacy alias: `schedule_tasks`).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};

use async_trait::async_trait;
use futures::stream;
use tokio::sync::{broadcast, Notify, RwLock};
use tokio::time::{sleep, Duration};

use bamboo_agent::agent::{Agent, AgentBuilder, AgentEvent, Message, Session};
use bamboo_agent::server::app_state::AgentRunner;
use bamboo_agent::server::schedule_app::{ResolvedRunConfig, ScheduleContext};
use bamboo_agent::server::schedules::{
    ScheduleManager, ScheduleRunConfig, ScheduleRunJob, ScheduleStore,
};
use bamboo_agent::server::tools::ScheduleTasksTool;
use bamboo_agent::Config;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    Tool, ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::SessionKind;
use bamboo_infrastructure::provider::Result as LLMResult;
use bamboo_infrastructure::provider::{LLMProvider, LLMStream};
use bamboo_infrastructure::LLMChunk;
use bamboo_infrastructure::SessionStoreV2;

mod common;

#[derive(Clone)]
struct DummyProvider;

#[async_trait]
impl LLMProvider for DummyProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> LLMResult<LLMStream> {
        Ok(Box::pin(stream::empty()))
    }
}

#[derive(Clone)]
struct BlockingProvider {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl LLMProvider for BlockingProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> LLMResult<LLMStream> {
        let started = self.started.clone();
        let release = self.release.clone();
        Ok(Box::pin(async_stream::stream! {
            started.notify_one();
            release.notified().await;
            yield Ok(LLMChunk::Token("ok".to_string()));
            yield Ok(LLMChunk::Done);
        }))
    }
}

struct NoopTools;

#[async_trait]
impl ToolExecutor for NoopTools {
    async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        Err(ToolError::NotFound("noop".to_string()))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![]
    }
}

fn ctx_for_session<'a>(session_id: &'a str) -> ToolExecutionContext<'a> {
    ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool_call",
        event_tx: None,
        available_tool_schemas: None,
    }
}

fn clean_config(data_dir: &std::path::Path) -> Config {
    Config::from_data_dir(Some(data_dir.to_path_buf()))
}

fn new_schedule_tasks_tool(
    schedule_store: Arc<ScheduleStore>,
    manager: Arc<ScheduleManager>,
    store: Arc<SessionStoreV2>,
    config: Config,
) -> ScheduleTasksTool {
    ScheduleTasksTool::new(
        schedule_store,
        manager,
        store.clone(),
        store,
        Arc::new(RwLock::new(config)),
    )
}

/// Build an `Agent` and a `ScheduleManager` for tests using the new `ScheduleContext` API.
fn build_manager(
    dir: &std::path::Path,
    schedule_store: Arc<ScheduleStore>,
    store: Arc<SessionStoreV2>,
    provider: Arc<dyn LLMProvider>,
    config: Config,
) -> (Arc<Agent>, Arc<ScheduleManager>) {
    use bamboo_engine::metrics::collector::MetricsCollector;
    use bamboo_engine::metrics::storage::SqliteMetricsStorage;
    use bamboo_engine::SkillManager;

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(dir.join("metrics.db")));
    let metrics = MetricsCollector::spawn(metrics_storage, 1);

    let persistence = Arc::new(bamboo_infrastructure::LockedSessionStore::new(
        store.clone(),
    ));

    let agent = AgentBuilder::new()
        .storage(store.clone())
        .attachment_reader(store.clone())
        .persistence(persistence)
        .skill_manager(Arc::new(SkillManager::new()))
        .metrics_collector(metrics)
        .config(Arc::new(RwLock::new(config.clone())))
        .provider(provider)
        .default_tools(Arc::new(NoopTools))
        .build()
        .expect("build agent");

    let agent = Arc::new(agent);
    let resolve_run_config = Arc::new(move |_job: &ScheduleRunJob| {
        let model =
            bamboo_agent::server::model_config_helper::get_schedule_model_from_config(&config)
                .ok()
                .map(|m| m.trim().to_string())
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "test-model".to_string());
        ResolvedRunConfig {
            model,
            provider_name: None,
            provider_type: None,
            fast_model: None,
            fast_model_provider: None,
            background_model: None,
            background_model_provider: None,
            summarization_model: None,
            summarization_model_provider: None,
            reasoning_effort: None,
            system_prompt: String::new(),
            base_system_prompt: String::new(),
            workspace_path: None,
        }
    });

    let ctx = ScheduleContext {
        schedule_store,
        agent: agent.clone(),
        persistence: Arc::new(bamboo_infrastructure::LockedSessionStore::new(
            store.clone(),
        )),
        tools: Arc::new(NoopTools),
        sessions_cache: Arc::new(RwLock::new(HashMap::new())),
        agent_runners: Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new())),
        session_event_senders: Arc::new(RwLock::new(HashMap::<
            String,
            broadcast::Sender<AgentEvent>,
        >::new())),
        app_data_dir: None,
        trigger_engine: bamboo_agent::server::schedules::default_trigger_engine(),
        resolve_run_config,
    };

    let manager = Arc::new(ScheduleManager::new(ctx));
    (agent, manager)
}

#[tokio::test]
async fn schedule_tasks_requires_session_id() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let tool = new_schedule_tasks_tool(schedule_store, manager, store, clean_config(dir.path()));

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
async fn schedule_tasks_rejects_child_sessions() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let tool = new_schedule_tasks_tool(
        schedule_store,
        manager,
        store.clone(),
        clean_config(dir.path()),
    );

    let mut child = Session::new("child-session", "test-model");
    child.kind = SessionKind::Child;
    child.parent_session_id = Some("root-session".to_string());
    child.root_session_id = "root-session".to_string();
    child.add_message(Message::user("hi".to_string()));
    store.save_session(&child).await.unwrap();

    let err = tool
        .execute_with_context(
            serde_json::json!({ "action": "list" }),
            ctx_for_session("child-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("not allowed inside child sessions"));
}

#[tokio::test]
async fn schedule_tasks_rejects_invalid_create_arguments() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let tool = new_schedule_tasks_tool(
        schedule_store,
        manager,
        store.clone(),
        clean_config(dir.path()),
    );

    let mut caller = Session::new("root-session", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "create",
                "name": "   ",
                "trigger": {"type": "interval", "every_seconds": 60}
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("name must be a non-empty string"));

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "create",
                "name": "bad",
                "trigger": {"type": "interval", "every_seconds": 0}
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("trigger.every_seconds must be > 0"));

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "create",
                "name": "bad-auto",
                "trigger": {"type": "interval", "every_seconds": 60},
                "run_config": {"auto_execute": true}
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("run_config.task_message is required when auto_execute is true"));
}

#[tokio::test]
async fn schedule_tasks_rejects_invalid_patch_arguments() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let tool = new_schedule_tasks_tool(
        schedule_store.clone(),
        manager,
        store.clone(),
        clean_config(dir.path()),
    );

    let mut caller = Session::new("root-session", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "patch",
                "schedule_id": "",
                "enabled": true
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("schedule_id must be a non-empty string"));

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "patch",
                "schedule_id": "missing",
                "trigger": {"type": "interval", "every_seconds": 0}
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("trigger.every_seconds must be > 0"));

    let err = tool
        .execute_with_context(
            serde_json::json!({
                "action": "patch",
                "schedule_id": "missing",
                "run_config": {"auto_execute": true}
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("run_config.task_message is required when auto_execute is true"));
}

#[tokio::test]
async fn schedule_tasks_crud_and_list_sessions() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let tool = new_schedule_tasks_tool(
        schedule_store.clone(),
        manager,
        store.clone(),
        clean_config(dir.path()),
    );

    // Create a caller root session (tool checks SessionKind::Root).
    let mut caller = Session::new("root-session", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    // Create schedule.
    let now = Utc::now();
    let start_at = (now - ChronoDuration::days(1)).to_rfc3339();
    let end_at = (now + ChronoDuration::days(30)).to_rfc3339();
    let created = tool
        .execute_with_context(
            serde_json::json!({
                "action": "create",
                "name": "My Schedule",
                "trigger": {"type": "daily", "hour": 9, "minute": 30},
                "timezone": "Asia/Shanghai",
                "start_at": start_at,
                "end_at": end_at,
                "misfire_policy": { "type": "catch_up_window", "max_catch_up_runs": 2, "max_lateness_seconds": 1800 },
                "overlap_policy": "skip",
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
    assert!(
        created_v["schedule"].get("state").is_some(),
        "schedule view should expose state"
    );
    assert_eq!(
        created_v["schedule"]["timezone"].as_str(),
        Some("Asia/Shanghai")
    );
    assert_eq!(
        created_v["schedule"]["overlap_policy"].as_str(),
        Some("skip")
    );
    assert_eq!(
        created_v["schedule"]["misfire_policy"]["type"].as_str(),
        Some("catch_up_window")
    );
    assert_eq!(
        created_v["schedule"]["trigger"]["type"].as_str(),
        Some("daily")
    );

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
        listed_v["schedules"].as_array().unwrap().iter().any(|s| {
            s["id"].as_str() == Some(schedule_id.as_str()) && s.get("state").is_some()
        }),
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
                "trigger": {"type": "interval", "every_seconds": 120},
                "timezone": "UTC",
                "misfire_policy": { "type": "skip" },
                "overlap_policy": "queue_one"
            }),
            ctx_for_session("root-session"),
        )
        .await
        .unwrap();
    let patched_v: serde_json::Value = serde_json::from_str(&patched.result).unwrap();
    assert_eq!(patched_v["schedule"]["enabled"].as_bool(), Some(true));
    assert!(
        patched_v["schedule"].get("state").is_some(),
        "patched schedule should expose state"
    );
    assert_eq!(patched_v["schedule"]["timezone"].as_str(), Some("UTC"));
    assert_eq!(
        patched_v["schedule"]["misfire_policy"]["type"].as_str(),
        Some("skip")
    );
    assert_eq!(
        patched_v["schedule"]["overlap_policy"].as_str(),
        Some("queue_one")
    );
    assert!(matches!(
        patched_v["schedule"]["trigger"]["type"].as_str(),
        Some("interval")
    ));
    assert_eq!(
        patched_v["schedule"]["trigger"]["every_seconds"].as_u64(),
        Some(120)
    );

    // Create a session that looks like it was created by schedule.
    let mut scheduled = Session::new("scheduled-session", "test-model");
    scheduled.metadata.insert(
        "created_by_schedule_id".to_string(),
        patched_v["schedule"]["id"].as_str().unwrap().to_string(),
    );
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
    assert!(
        serde_json::from_str::<serde_json::Value>(&deleted.result).unwrap()["success"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn schedule_run_non_auto_execute_completes_with_success_accounting() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let created = schedule_store
        .create_schedule(
            "Non Auto Execute".to_string(),
            bamboo_agent::server::schedules::ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig {
                auto_execute: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        clean_config(dir.path()),
    );

    let claimed = schedule_store
        .create_run_now(&created.id)
        .await
        .unwrap()
        .expect("run job should be created");
    manager
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
        .unwrap();

    for _ in 0..20 {
        let schedule = schedule_store.get_schedule(&created.id).await.unwrap();
        if schedule.state.last_success_at.is_some() {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    let entries = store.list_index_entries().await;
    assert_eq!(entries.len(), 1, "expected one session, got: {entries:?}");
    assert_eq!(
        entries[0].schedule_run_id.as_deref(),
        Some(claimed.run_id.as_str())
    );

    let updated = schedule_store.get_schedule(&created.id).await.unwrap();
    assert_eq!(updated.state.running_run_count, 0);
    assert!(updated.state.last_started_at.is_some());
    assert!(updated.state.last_finished_at.is_some());
    assert!(updated.state.last_success_at.is_some());
    assert_eq!(updated.state.total_run_count, 1);
    assert_eq!(updated.state.total_success_count, 1);
    assert_eq!(updated.state.total_failure_count, 0);
}

#[tokio::test]
async fn schedule_run_uses_config_get_fast_then_default_fallback() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let created = schedule_store
        .create_schedule(
            "Config Model Fallback".to_string(),
            bamboo_agent::server::schedules::ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig {
                auto_execute: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Copilot has no dedicated fast model configured here, so schedules should
    // fall back from fast to the default chat model.
    let mut cfg = clean_config(dir.path());
    cfg.provider = "copilot".to_string();
    let expected_model = cfg
        .get_fast_model()
        .expect("copilot should have a schedule model fallback");

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        Arc::new(DummyProvider),
        cfg,
    );

    let claimed = schedule_store
        .create_run_now(&created.id)
        .await
        .unwrap()
        .expect("run job should be created");
    manager
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
        .unwrap();

    let mut created_id: Option<String> = None;
    for _ in 0..40 {
        let entries = store.list_index_entries().await;
        if let Some(first) = entries.first() {
            created_id = Some(first.id.clone());
        }
        let schedule = schedule_store.get_schedule(&created.id).await.unwrap();
        if created_id.is_some() && schedule.state.last_success_at.is_some() {
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
    assert_eq!(
        session.metadata.get("schedule_run_id").map(String::as_str),
        Some(claimed.run_id.as_str())
    );
    let run_record = schedule_store
        .get_run_record(&claimed.run_id)
        .await
        .expect("run record should exist");
    assert_eq!(run_record.session_id.as_deref(), Some(session_id.as_str()));

    let updated = schedule_store.get_schedule(&created.id).await.unwrap();
    assert_eq!(updated.state.running_run_count, 0);
    assert!(updated.state.last_started_at.is_some());
    assert!(updated.state.last_finished_at.is_some());
    assert!(updated.state.last_success_at.is_some());
    assert_eq!(updated.state.total_run_count, 1);
    assert_eq!(updated.state.total_success_count, 1);
    assert_eq!(updated.state.total_failure_count, 0);
}

#[tokio::test]
async fn schedule_auto_execute_keeps_running_until_background_completion() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());
    let schedule_store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());

    let created = schedule_store
        .create_schedule(
            "Auto Execute".to_string(),
            bamboo_agent::server::schedules::ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig {
                auto_execute: true,
                task_message: Some("Say hi".to_string()),
                model: Some("test-model".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = Arc::new(BlockingProvider {
        started: started.clone(),
        release: release.clone(),
    });

    let (_agent, manager) = build_manager(
        dir.path(),
        schedule_store.clone(),
        store.clone(),
        provider,
        clean_config(dir.path()),
    );

    let claimed = schedule_store
        .create_run_now(&created.id)
        .await
        .unwrap()
        .expect("run job should be created");
    manager
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
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("background run should start");

    let in_progress = schedule_store.get_schedule(&created.id).await.unwrap();
    assert_eq!(in_progress.state.running_run_count, 1);
    assert!(in_progress.state.last_started_at.is_some());
    assert!(in_progress.state.last_finished_at.is_none());
    let running_record = schedule_store
        .get_run_record(&claimed.run_id)
        .await
        .expect("run record should exist while running");
    assert_eq!(
        running_record.status,
        bamboo_agent::server::schedules::ScheduleRunStatus::Running
    );
    assert!(running_record.started_at.is_some());
    assert!(running_record.session_id.is_some());

    release.notify_one();

    for _ in 0..40 {
        let schedule = schedule_store.get_schedule(&created.id).await.unwrap();
        if schedule.state.last_success_at.is_some() && schedule.state.running_run_count == 0 {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    let updated = schedule_store.get_schedule(&created.id).await.unwrap();
    assert_eq!(updated.state.running_run_count, 0);
    assert!(updated.state.last_finished_at.is_some());
    assert!(updated.state.last_success_at.is_some());
    assert_eq!(updated.state.total_run_count, 1);
    assert_eq!(updated.state.total_success_count, 1);
    let completed_record = schedule_store
        .get_run_record(&claimed.run_id)
        .await
        .expect("run record should still exist after completion");
    assert_eq!(
        completed_record.status,
        bamboo_agent::server::schedules::ScheduleRunStatus::Success
    );
    assert!(completed_record.completed_at.is_some());
    assert!(completed_record.execution_duration_ms.is_some());
}
