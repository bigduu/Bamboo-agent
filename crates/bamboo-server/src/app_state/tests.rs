use super::{AppState, DEFAULT_BASE_PROMPT};
use crate::tools::ToolSurface;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolError};
use bamboo_agent_core::Session;
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_engine::{MetricsStorage, SessionStatus, SqliteMetricsStorage};
use bamboo_infrastructure::SessionStoreV2;
use bamboo_tools::permission::config::{PermissionConfig, PermissionRule, PermissionType};
use bamboo_tools::permission::storage::PermissionStorage;
use serde_json::json;

fn make_tool_call(name: &str, args: serde_json::Value) -> ToolCall {
    ToolCall {
        id: format!("call_{name}"),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: args.to_string(),
        },
    }
}

#[test]
fn default_base_prompt_does_not_unconditionally_require_conclusion_with_options() {
    let normalized = DEFAULT_BASE_PROMPT.to_ascii_lowercase();
    assert!(!normalized.contains("before ending a task, always call conclusion_with_options"));
    assert!(!normalized.contains("do not ask final confirmation in plain assistant text"));
}
#[test]
fn default_base_prompt_prefers_using_injected_context_before_reasking() {
    assert!(DEFAULT_BASE_PROMPT.contains("treat it as available working context"));
    assert!(DEFAULT_BASE_PROMPT.contains("Prefer a minimal verifiable attempt first"));
    assert!(DEFAULT_BASE_PROMPT
        .contains("only ask follow-up questions for information that is still genuinely missing"));
}

#[tokio::test]
async fn test_app_state_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    // Verify basic fields
    assert!(state.sessions.read().await.is_empty());
}

#[tokio::test]
async fn app_state_creation_reconciles_stale_running_metrics_from_durable_sessions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let session_store = SessionStoreV2::new(data_dir.clone())
        .await
        .expect("session store should initialize");
    let mut session = Session::new("startup-stale-session", "test-model");
    let mut runtime_state = AgentRuntimeState::new("run-startup-stale");
    runtime_state.status = AgentStatusState::Suspended;
    session.agent_runtime_state = Some(runtime_state);
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "waiting_for_children".to_string(),
    );
    session_store
        .save_session(&session)
        .await
        .expect("session should save");

    let metrics_storage = SqliteMetricsStorage::new(data_dir.join("metrics.db"));
    metrics_storage
        .init()
        .await
        .expect("metrics storage should initialize");
    metrics_storage
        .upsert_session_start("startup-stale-session", "test-model", session.created_at)
        .await
        .expect("session start metrics should save");

    let state = AppState::new(data_dir)
        .await
        .expect("app state should initialize");

    let sessions = state
        .metrics_service
        .sessions(Default::default())
        .await
        .expect("sessions query should succeed");
    let session_metrics = sessions
        .iter()
        .find(|entry| entry.session_id == "startup-stale-session")
        .expect("startup-stale-session metrics should exist");
    assert_eq!(session_metrics.status, SessionStatus::AwaitingResponse);
    assert!(session_metrics.completed_at.is_some());

    let summary = state
        .metrics_service
        .summary(None, None)
        .await
        .expect("summary query should succeed");
    assert_eq!(summary.active_sessions, 0);
    assert_eq!(summary.awaiting_response_sessions, 1);
    assert_eq!(summary.completed_sessions, 0);
}

#[tokio::test]
async fn root_tools_include_server_overlays_and_session_note() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let names: std::collections::HashSet<String> = state
        .get_all_tool_schemas()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();

    assert!(names.contains("Task"));
    assert!(names.contains("SubAgent"));
    assert!(names.contains("scheduler"));
    assert!(names.contains("session_history"));
    assert!(names.contains("memory"));
    assert!(names.contains("load_skill"));
    assert!(names.contains("read_skill_resource"));
    assert!(names.contains("session_note"));
}

#[tokio::test]
async fn default_first_round_tool_surface_is_smaller_than_full_root_tool_catalog() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    let full = state.get_all_tool_schemas();
    let visible: Vec<_> = full
        .iter()
        .filter(|schema| bamboo_tools::exposure::is_core_tool(&schema.function.name))
        .collect();
    let visible_names: std::collections::HashSet<&str> = visible
        .iter()
        .map(|schema| schema.function.name.as_str())
        .collect();
    eprintln!(
        "tool_surface_metrics: full={}, visible={}, hidden={}",
        full.len(),
        visible.len(),
        full.len().saturating_sub(visible.len())
    );

    assert!(
        visible.len() < full.len(),
        "expected reduced first-round surface: visible={}, full={}",
        visible.len(),
        full.len()
    );
    assert!(!visible_names.contains("scheduler"));
    assert!(!visible_names.contains("sub_session_manager"));
    assert!(!visible_names.contains("session_history"));
}

#[tokio::test]
async fn child_tools_exclude_scheduler_and_session_history() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let names: std::collections::HashSet<String> = state
        .tools_for(ToolSurface::Child)
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();

    assert!(!names.contains("scheduler"));
    assert!(!names.contains("sub_session_manager"));
    assert!(!names.contains("session_history"));
    assert!(names.contains("memory"));
    assert!(names.contains("load_skill"));
    assert!(names.contains("read_skill_resource"));
    assert!(names.contains("session_note"));
}

#[tokio::test]
async fn overlay_tools_require_session_context() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    let schedule_result = state
        .tools_for(ToolSurface::Root)
        .execute(&make_tool_call("scheduler", json!({ "action": "list" })))
        .await;
    assert!(matches!(
        schedule_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));

    let inspector_result = state
        .tools_for(ToolSurface::Root)
        .execute(&make_tool_call(
            "session_history",
            json!({ "action": "list" }),
        ))
        .await;
    assert!(matches!(
        inspector_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));

    let memory_result = state
        .tools_for(ToolSurface::Root)
        .execute(&make_tool_call(
            "memory",
            json!({ "action": "inspect", "scope": "global" }),
        ))
        .await;
    assert!(matches!(
        memory_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));

    let sub_agent_result = state
        .tools_for(ToolSurface::Root)
        .execute(&make_tool_call("SubAgent", json!({ "action": "list" })))
        .await;
    assert!(matches!(
        sub_agent_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));
}

#[tokio::test]
async fn memory_tool_merge_action_updates_existing_project_memory() {
    let temp_dir = tempfile::tempdir().unwrap();
    bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    bamboo_tools::tools::workspace_state::ensure_session_workspace(
        "session-merge",
        Some(temp_dir.path().to_path_buf()),
    );

    let write_target = make_tool_call(
        "memory",
        json!({
            "action": "write",
            "scope": "project",
            "type": "project",
            "title": "Release freeze begins next week",
            "content": "Merge freeze begins on Tuesday.",
            "tags": ["release"]
        }),
    );
    let target_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &write_target,
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-merge"),
                tool_call_id: "tool-call-write-target",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write target should succeed");
    let target_json: serde_json::Value = serde_json::from_str(&target_result.result).unwrap();
    let target_id = target_json["memory"]["id"].as_str().unwrap().to_string();

    let write_source = make_tool_call(
        "memory",
        json!({
            "action": "write",
            "scope": "project",
            "type": "project",
            "title": "Mobile release note",
            "content": "Stakeholders confirmed freeze applies to mobile release cut.",
            "tags": ["mobile"]
        }),
    );
    let source_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &write_source,
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-merge"),
                tool_call_id: "tool-call-write-source",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write source should succeed");
    let source_json: serde_json::Value = serde_json::from_str(&source_result.result).unwrap();
    let source_id = source_json["memory"]["id"].as_str().unwrap().to_string();

    let merge_call = make_tool_call(
        "memory",
        json!({
            "action": "merge",
            "id": target_id,
            "content": "Additional confirmation from a later session.",
            "tags": ["confirmed"],
            "source_memory_ids": [source_id]
        }),
    );
    let merge_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &merge_call,
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-merge"),
                tool_call_id: "tool-call-merge",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("merge should succeed");
    let merge_json: serde_json::Value = serde_json::from_str(&merge_result.result).unwrap();
    assert_eq!(merge_json["action"], "merge");
    assert_eq!(merge_json["data"]["appended"], true);
    assert_eq!(
        merge_json["data"]["superseded_ids"][0],
        source_json["memory"]["id"]
    );
}

#[tokio::test]
async fn memory_tool_write_merges_heuristically_similar_memory_when_enabled() {
    let temp_dir = tempfile::tempdir().unwrap();
    bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    bamboo_tools::tools::workspace_state::ensure_session_workspace(
        "session-heuristic-merge",
        Some(temp_dir.path().to_path_buf()),
    );

    let original = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "project",
                    "title": "Release freeze begins next week",
                    "content": "Merge freeze begins on Tuesday for mobile release cut.",
                    "tags": ["release", "freeze"],
                    "options": { "allow_merge_if_similar": false }
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-heuristic-merge"),
                tool_call_id: "tool-call-write-heuristic-original",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("original write should succeed");
    let original_json: serde_json::Value = serde_json::from_str(&original.result).unwrap();
    let original_id = original_json["memory"]["id"].as_str().unwrap().to_string();

    let merged = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "project",
                    "title": "Mobile release freeze starts Tuesday",
                    "content": "Stakeholders confirmed the mobile release freeze starts Tuesday.",
                    "tags": ["mobile", "release"],
                    "options": { "allow_merge_if_similar": true }
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-heuristic-merge"),
                tool_call_id: "tool-call-write-heuristic-merge",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("heuristic merge write should succeed");
    let merged_json: serde_json::Value = serde_json::from_str(&merged.result).unwrap();
    let merged_id = merged_json["memory"]["id"].as_str().unwrap().to_string();
    assert_eq!(merged_id, original_id);

    let inspect = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "inspect",
                    "scope": "project"
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-heuristic-merge"),
                tool_call_id: "tool-call-inspect-heuristic-merge",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("inspect should succeed");
    let inspect_json: serde_json::Value = serde_json::from_str(&inspect.result).unwrap();
    assert_eq!(inspect_json["data"]["total_memories"], 1);
}

#[tokio::test]
async fn memory_tool_merge_mode_contradict_marks_memory_contradicted() {
    let temp_dir = tempfile::tempdir().unwrap();
    bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    bamboo_tools::tools::workspace_state::ensure_session_workspace(
        "session-contradict",
        Some(temp_dir.path().to_path_buf()),
    );

    let target = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "project",
                    "title": "Release freeze begins next week",
                    "content": "Freeze begins on Tuesday."
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-contradict"),
                tool_call_id: "tool-call-write-contradict-target",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write target should succeed");
    let target_json: serde_json::Value = serde_json::from_str(&target.result).unwrap();
    let target_id = target_json["memory"]["id"].as_str().unwrap().to_string();

    let source = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "project",
                    "title": "Updated release note",
                    "content": "Freeze is postponed."
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-contradict"),
                tool_call_id: "tool-call-write-contradict-source",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write source should succeed");
    let source_json: serde_json::Value = serde_json::from_str(&source.result).unwrap();
    let source_id = source_json["memory"]["id"].as_str().unwrap().to_string();

    let contradict_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "merge",
                    "mode": "contradict",
                    "id": target_id,
                    "content": "newer info conflicts",
                    "reason": "newer release update conflicts",
                    "source_memory_ids": [source_id]
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-contradict"),
                tool_call_id: "tool-call-contradict",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("contradict should succeed");
    let contradict_json: serde_json::Value =
        serde_json::from_str(&contradict_result.result).unwrap();
    assert_eq!(contradict_json["action"], "merge");
    assert_eq!(contradict_json["mode"], "contradict");
    assert_eq!(contradict_json["data"]["changed"], true);
    assert_eq!(
        contradict_json["data"]["contradicted_ids"][0],
        source_json["memory"]["id"]
    );
}

#[tokio::test]
async fn memory_tool_batch_purge_archives_filtered_items() {
    let temp_dir = tempfile::tempdir().unwrap();
    bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    bamboo_tools::tools::workspace_state::ensure_session_workspace(
        "session-batch-purge",
        Some(temp_dir.path().to_path_buf()),
    );

    let stale_write = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "reference",
                    "title": "Old dashboard link",
                    "content": "Legacy dashboard."
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-batch-purge"),
                tool_call_id: "tool-call-write-stale",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write stale memory should succeed");
    let stale_json: serde_json::Value = serde_json::from_str(&stale_write.result).unwrap();
    let stale_id = stale_json["memory"]["id"].as_str().unwrap().to_string();

    state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "purge",
                    "id": stale_id,
                    "mode": "stale",
                    "reason": "mark stale first"
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-batch-purge"),
                tool_call_id: "tool-call-mark-stale",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("mark stale should succeed");

    state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "reference",
                    "title": "Current dashboard link",
                    "content": "Current dashboard."
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-batch-purge"),
                tool_call_id: "tool-call-write-active",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write active memory should succeed");

    let batch_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "purge",
                    "scope": "project",
                    "mode": "archived",
                    "reason": "archive stale references",
                    "filters": {
                        "type": ["reference"],
                        "status": ["stale"]
                    }
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-batch-purge"),
                tool_call_id: "tool-call-batch-purge",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("batch purge should succeed");
    let batch_json: serde_json::Value = serde_json::from_str(&batch_result.result).unwrap();
    assert_eq!(batch_json["action"], "purge");
    assert_eq!(batch_json["data"]["matched_count"], 1);
}

#[tokio::test]
async fn memory_tool_inspect_and_rebuild_expose_observability_fields() {
    let temp_dir = tempfile::tempdir().unwrap();
    bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    bamboo_tools::tools::workspace_state::ensure_session_workspace(
        "session-inspect",
        Some(temp_dir.path().to_path_buf()),
    );

    state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "write",
                    "scope": "project",
                    "type": "reference",
                    "title": "Old dashboard link",
                    "content": "Legacy dashboard."
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-inspect"),
                tool_call_id: "tool-call-write-inspect",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("write memory should succeed");

    let inspect_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "inspect",
                    "scope": "project"
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-inspect"),
                tool_call_id: "tool-call-inspect",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("inspect should succeed");
    let inspect_json: serde_json::Value = serde_json::from_str(&inspect_result.result).unwrap();
    assert_eq!(inspect_json["action"], "inspect");
    assert!(inspect_json["data"]["index_files"].is_array());
    assert!(inspect_json["data"]["state_files"].is_array());
    assert!(inspect_json["data"]["stale_candidate_count"].is_number());
    assert!(inspect_json["data"]["last_reindex_at"].is_string());
    assert!(inspect_json["data"]["last_dream_at"].is_string());

    let rebuild_result = state
        .tools_for(ToolSurface::Root)
        .execute_with_context(
            &make_tool_call(
                "memory",
                json!({
                    "action": "rebuild",
                    "scope": "project"
                }),
            ),
            bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some("session-inspect"),
                tool_call_id: "tool-call-rebuild",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("rebuild should succeed");
    let rebuild_json: serde_json::Value = serde_json::from_str(&rebuild_result.result).unwrap();
    assert_eq!(rebuild_json["action"], "rebuild");
    assert!(rebuild_json["data"]["index_files"].is_array());
    assert!(rebuild_json["data"]["state_files"].is_array());
    assert!(rebuild_json["data"]["stale_candidate_count"].is_number());
    assert!(rebuild_json["data"]["last_reindex_at"].is_string());
    assert!(rebuild_json["data"]["last_dream_at"].is_string());
}

#[tokio::test]
async fn app_state_uses_persisted_permission_config_in_data_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = PermissionStorage::new(temp_dir.path());
    let config = PermissionConfig::new();
    config.set_enabled(true);
    config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*", false));
    storage.save(&config).await.unwrap();

    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let target = temp_dir.path().join("blocked.txt");
    let call = make_tool_call(
        "Write",
        json!({
            "file_path": target,
            "content": "blocked"
        }),
    );

    let result = state.tools_for(ToolSurface::Root).execute(&call).await;
    assert!(matches!(result, Err(ToolError::Execution(_))));
    assert!(!target.exists());
}
