use async_trait::async_trait;

use super::prompt_envelope::StablePromptFrame;
use super::prompt_setup::build_stable_prompt_frame_with_sections;
use super::tool_schemas::{
    resolve_available_tool_schemas_for_session, resolve_classified_tool_catalog_for_session,
};
use bamboo_agent_core::agent::types::{TaskItem, TaskItemStatus, TaskList};
use bamboo_agent_core::tools::{
    FunctionSchema, ToolCall, ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::{Message, Session};
use bamboo_domain::{
    CapabilityLoadingClass, CapabilityLoadingMode, EffectiveCallableSet, RuntimeSessionPersistence,
};
use bamboo_skills::runtime_metadata::{
    SKILL_RUNTIME_ACTIVATION_GENERATION_KEY, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use chrono::Utc;
use std::sync::{Arc, Mutex};

const COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY: &str =
    "copilot_conclusion_with_options_enhancement_enabled";
const ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT: &str =
    "If you are wrapping up a task turn, asking the user to choose next steps, or handing off execution, you must call this tool instead of ending with plain assistant text.";

struct StaticToolExecutor {
    schemas: Vec<ToolSchema>,
}

#[derive(Default)]
struct RecordingToolExecutor {
    calls: Mutex<Vec<ToolCall>>,
    schemas: Vec<ToolSchema>,
}

#[derive(Default)]
struct RecordingPersistence {
    sessions: Mutex<Vec<Session>>,
}

#[async_trait]
impl RuntimeSessionPersistence for RecordingPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.sessions
            .lock()
            .expect("recording lock")
            .push(session.clone());
        Ok(())
    }
}

#[async_trait]
impl ToolExecutor for StaticToolExecutor {
    async fn execute(
        &self,
        _call: &ToolCall,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        Ok(ToolResult {
            success: true,
            result: "ok".to_string(),
            display_preference: None,
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }
}

#[async_trait]
impl ToolExecutor for RecordingToolExecutor {
    async fn execute(
        &self,
        call: &ToolCall,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        self.calls
            .lock()
            .expect("recording tool lock")
            .push(call.clone());
        Ok(ToolResult {
            success: true,
            result: serde_json::json!({
                "skill_id": "review",
                "instructions": "REPORT_ONLY_ACTIONABLE_FINDINGS"
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        })
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        let result = self.execute(call).await?;
        ctx.emit(bamboo_agent_core::AgentEvent::WorkflowActivated {
            event_id: "test-explicit-review-activation".to_string(),
            session_id: "explicit-review".to_string(),
            workflow_id: "review".to_string(),
            revision: 1,
            invoked_by: "user".to_string(),
        })
        .await;
        Ok(result)
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }
}

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: name.to_string(),
            description: format!("{name} tool"),
            parameters: serde_json::json!({ "type": "object", "properties": {} }),
        },
    }
}

fn mark_test_activation_current(session: &mut Session, skill_id: &str) {
    session.metadata.insert(
        bamboo_skills::runtime_metadata::LOADED_SKILL_IDS_METADATA_KEY.to_string(),
        serde_json::json!([skill_id]).to_string(),
    );
    session.metadata.insert(
        bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY.to_string(),
        serde_json::json!({
            "id": skill_id,
            "source": "builtin",
            "revision": 1,
            "kind": "instruction",
            "args": {},
            "invoked_by": "user",
            "activated_at": "2026-07-21T00:00:00Z",
            "status": "active"
        })
        .to_string(),
    );
    session.metadata.insert(
        bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.to_string(),
        "{}".to_string(),
    );
}

fn record_test_activation_from_pinned_snapshot(session: &mut Session, skill_id: &str) {
    session.metadata.insert(
        bamboo_skills::runtime_metadata::LOADED_SKILL_IDS_METADATA_KEY.to_string(),
        serde_json::json!([skill_id]).to_string(),
    );
    bamboo_skills::record_loaded_workflow_activation(
        &mut session.metadata,
        skill_id,
        "test-context-fingerprint".to_string(),
    )
    .expect("record pinned test activation");
    session
        .metadata
        .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
}

#[tokio::test]
async fn pending_workflow_deactivation_is_published_and_acked_exactly_once() {
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let mut session = Session::new("deactivation-event", "model");
    session.metadata.insert(
        bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY.to_string(),
        serde_json::json!({
            "type": "workflow.deactivated",
            "workflow_id": "review",
            "revision": 7,
        })
        .to_string(),
    );
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
    super::publish_pending_workflow_lifecycle_event(&mut session, &config, &event_tx)
        .await
        .expect("publish pending deactivation");
    super::publish_pending_workflow_lifecycle_event(&mut session, &config, &event_tx)
        .await
        .expect("acked event is a no-op");
    assert!(matches!(
        event_rx.try_recv(),
        Ok(bamboo_agent_core::AgentEvent::WorkflowDeactivated {
            ref session_id,
            ref workflow_id,
            revision: 7,
            ..
        }) if session_id == "deactivation-event" && workflow_id == "review"
    ));
    assert!(event_rx.try_recv().is_err(), "deactivation must emit once");
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY));
    assert!(persistence
        .sessions
        .lock()
        .expect("recording lock")
        .last()
        .is_some_and(|saved| !saved
            .metadata
            .contains_key(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY)));
}

#[tokio::test]
async fn lifecycle_replay_after_ack_save_failure_has_one_observable_identity() {
    struct FailingPersistence;

    #[async_trait]
    impl RuntimeSessionPersistence for FailingPersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            Err(std::io::Error::other("simulated acknowledgement crash"))
        }
    }

    let pending = serde_json::json!({
        "type": "workflow.activated",
        "workflow_id": "review",
        "revision": 7,
        "invoked_by": "model",
        "activated_at": "2026-07-20T00:00:00Z",
    })
    .to_string();
    let mut durable_before_crash = Session::new("activation-replay", "model");
    durable_before_crash.metadata.insert(
        bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY.to_string(),
        pending,
    );

    let mut first_attempt = durable_before_crash.clone();
    let first_config = crate::runtime::config::AgentLoopConfig {
        persistence: Some(Arc::new(FailingPersistence)),
        ..Default::default()
    };
    let (first_tx, mut first_rx) = tokio::sync::mpsc::channel(2);
    super::publish_pending_workflow_lifecycle_event(&mut first_attempt, &first_config, &first_tx)
        .await
        .expect_err("send succeeds but acknowledgement save fails");
    let first = first_rx.recv().await.expect("first delivery");

    // A process restart reloads the durable pre-ack outbox and replays it.
    let persistence = Arc::new(RecordingPersistence::default());
    let restart_config = crate::runtime::config::AgentLoopConfig {
        persistence: Some(persistence),
        ..Default::default()
    };
    let (restart_tx, mut restart_rx) = tokio::sync::mpsc::channel(2);
    super::publish_pending_workflow_lifecycle_event(
        &mut durable_before_crash,
        &restart_config,
        &restart_tx,
    )
    .await
    .expect("restart replay and ack");
    let replay = restart_rx.recv().await.expect("replayed delivery");
    let (first_id, replay_id) = match (&first, &replay) {
        (
            bamboo_agent_core::AgentEvent::WorkflowActivated {
                event_id: first_id, ..
            },
            bamboo_agent_core::AgentEvent::WorkflowActivated {
                event_id: replay_id,
                ..
            },
        ) => (first_id, replay_id),
        other => panic!("unexpected lifecycle events: {other:?}"),
    };
    assert_eq!(first_id, replay_id, "replay identity must be stable");

    let mut consumer = crate::runtime::execution::AgentRunner::new();
    consumer.push_critical_event(first);
    consumer.push_critical_event(replay);
    assert_eq!(
        consumer.last_critical_events.len(),
        1,
        "idempotent consumer exposes one state transition"
    );
}

#[tokio::test]
async fn session_setup_publishes_current_skill_allowlist_before_tool_execution() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: Vec::new(),
    };
    let mut session = Session::new("selection-publish", "model");
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );

    super::prepare_session_for_loop(
        &mut session,
        "Review the current changes",
        &config,
        &tools,
        None,
        "selection-publish",
        &crate::runtime::runner::logging::DebugLogger::new(false),
        false,
        &event_tx,
    )
    .await
    .expect("session setup");

    let current_ids = session
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .unwrap_or_else(|| {
            panic!(
                "current runtime selection metadata; activation_error={:?}",
                session
                    .metadata
                    .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
            )
        });
    assert!(current_ids.contains("review"));
    assert!(!current_ids.contains("plan"));
    let candidate_snapshot = manager
        .store()
        .export_activation_snapshot("selection-publish")
        .await
        .expect("immutable automatic candidate pin");
    assert_eq!(
        candidate_snapshot.skills.len(),
        1,
        "automatic activation must pin only the unique matched Skill"
    );
    assert!(candidate_snapshot.skills.contains_key("review"));
    assert!(
        !session
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY),
        "automatic catalog publication must remain metadata-only"
    );
    let published_catalog = session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY)
        .expect("metadata-only catalog publication");
    assert!(published_catalog.contains("review"));
    assert!(!published_catalog.contains("pinned instructions"));
    assert!(
        !session
            .metadata
            .contains_key("skill_runtime_loaded_skill_ids"),
        "a new automatic selection must not inherit the previous run's activation"
    );
    let saved = persistence.sessions.lock().expect("recording lock");
    let published = saved.last().expect("selection published");
    let ids = published
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .expect("runtime selection metadata");
    assert!(ids.contains("review"));
    assert!(!ids.contains("plan"));
}

#[tokio::test]
async fn pre_execute_explicit_snapshot_restores_after_restart_without_suspended_runtime() {
    let directory = tempfile::tempdir().expect("tempdir");
    let config = SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    };
    let first = Arc::new(SkillManager::with_config(config.clone()));
    first.initialize().await.expect("initialize first manager");
    let review = first
        .store()
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| entry.id == "review" && entry.winner)
        .expect("review entry");
    let selected_ids = [review.id.clone()];
    let activation = first
        .resolve_and_pin_activation_for_request_with_mode_and_budget(
            "pre-execute-restart",
            &std::collections::BTreeSet::new(),
            Some(&selected_ids),
            None,
            None,
            bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        )
        .await
        .expect("pin first process candidate");
    let snapshot = first
        .store()
        .export_activation_snapshot("pre-execute-restart")
        .await
        .expect("export candidate");
    let selection = bamboo_skills::WorkflowSelection {
        id: review.id,
        source: review.source,
        revision: review.revision,
        args: serde_json::json!({}),
    };
    let mut session = Session::new("pre-execute-restart", "model");
    session.metadata.insert(
        bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY.to_string(),
        serde_json::to_string(&selection).expect("selection JSON"),
    );
    bamboo_skills::persist_explicit_workflow_candidate(
        &mut session.metadata,
        &selection,
        &activation,
        &snapshot,
    )
    .expect("persist candidate");
    first
        .release_activation_for_workspace("pre-execute-restart", None)
        .await
        .expect("simulate process exit");
    drop(first);

    let restarted = Arc::new(SkillManager::with_config(config));
    restarted
        .initialize()
        .await
        .expect("initialize restarted manager");
    let loop_config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(restarted.clone()),
        selected_skill_ids: Some(selected_ids.to_vec()),
        ..Default::default()
    };
    let loaded = super::skill_context::load_skill_context(
        &loop_config,
        &session,
        "pre-execute-restart",
        "Review this change",
        false,
    )
    .await
    .expect("restore chat-boundary candidate without suspended runtime");
    assert_eq!(loaded.selected_skill_ids, vec!["review"]);
    assert_eq!(loaded.skill_revisions["review"], selection.revision);
    assert!(
        restarted
            .store()
            .activation_was_restored("pre-execute-restart")
            .await,
        "restart must use durable pinned bytes, not silently re-resolve live catalog"
    );
}

#[tokio::test]
async fn session_setup_requires_model_issued_load_for_one_explicit_skill() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        selected_skill_ids: Some(vec!["review".to_string()]),
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let tools = RecordingToolExecutor {
        calls: Mutex::new(Vec::new()),
        schemas: vec![schema("load_skill")],
    };
    let mut session = Session::new("explicit-review", "model");
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);

    super::prepare_session_for_loop(
        &mut session,
        "Review this change",
        &config,
        &tools,
        None,
        "explicit-review",
        &crate::runtime::runner::logging::DebugLogger::new(false),
        false,
        &event_tx,
    )
    .await
    .expect("session setup");

    let calls = tools.calls.lock().expect("recording tool lock");
    assert!(
        calls.is_empty(),
        "the runtime must not impersonate the model"
    );
    assert!(session.messages.iter().all(|message| {
        !message.content.contains("## Explicit Workflow Activated")
            && !message.content.contains("REPORT_ONLY_ACTIONABLE_FINDINGS")
    }));
    assert!(
        super::prompt_envelope::build_active_workflow_context_block(&session).is_none(),
        "the workflow is not active until the model-issued load_skill succeeds"
    );
    assert!(
        event_rx.try_recv().is_err(),
        "selection alone must not emit WorkflowActivated"
    );
    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));
    let skill_context = session
        .metadata
        .get("skill.context")
        .expect("required activation context");
    assert!(skill_context.contains("## Required Explicit Workflow Activation"));
    assert!(skill_context.contains("first response step MUST be exactly one `load_skill` call"));
    assert!(skill_context.contains("review"));
    assert!(session.messages.iter().all(|message| {
        !message
            .content
            .contains("## Required Explicit Workflow Activation")
    }));

    let saved = persistence.sessions.lock().expect("recording lock");
    assert!(saved.iter().all(|saved_session| !saved_session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids")));
}

#[tokio::test]
async fn permission_style_second_prepare_preserves_unchanged_explicit_activation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        selected_skill_ids: Some(vec!["review".to_string()]),
        persistence: Some(persistence),
        ..Default::default()
    };
    let tools = RecordingToolExecutor {
        calls: Mutex::new(Vec::new()),
        schemas: vec![schema("load_skill")],
    };
    let mut session = Session::new("explicit-review-resume", "model");
    let logger = crate::runtime::runner::logging::DebugLogger::new(false);
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);

    super::prepare_session_for_loop(
        &mut session,
        "Review this change",
        &config,
        &tools,
        None,
        "explicit-review-resume",
        &logger,
        false,
        &event_tx,
    )
    .await
    .expect("initial explicit setup");
    assert!(super::skill_context::explicit_activation_pending(&session));
    record_test_activation_from_pinned_snapshot(&mut session, "review");
    assert!(!super::skill_context::explicit_activation_pending(&session));

    // Mirrors re-entry after a permission/tool suspension: same session, same
    // explicit workflow, and a second prepare before the model continues.
    super::prepare_session_for_loop(
        &mut session,
        "Review this change",
        &config,
        &tools,
        None,
        "explicit-review-resume",
        &logger,
        true,
        &event_tx,
    )
    .await
    .expect("resume explicit setup");

    assert_eq!(
        session
            .metadata
            .get("skill_runtime_loaded_skill_ids")
            .map(String::as_str),
        Some("[\"review\"]")
    );
    assert!(!super::skill_context::explicit_activation_pending(&session));
    let skill_context = session
        .metadata
        .get("skill.context")
        .expect("restored activation context");
    assert!(!skill_context.contains("## Required Explicit Workflow Activation"));
    assert!(skill_context.contains("## Explicit Workflow Already Activated"));
    assert!(
        skill_context.contains("Do not call `load_skill` again solely because execution resumed")
    );
    assert!(session.messages.iter().all(|message| {
        !message
            .content
            .contains("## Explicit Workflow Already Activated")
    }));
    assert!(tools.calls.lock().expect("recording tool lock").is_empty());
}

#[test]
fn activation_reset_preserves_same_explicit_and_clears_superseded_selection() {
    let explicit_review = super::skill_context::SkillContextLoadResult {
        selected_skill_ids: vec!["review".to_string()],
        selection_source: Some("explicit".to_string()),
        ..Default::default()
    };
    let mut session = Session::new("activation-reset", "model");
    mark_test_activation_current(&mut session, "review");

    super::skill_context::reset_activation_state_for_new_selection(&mut session, &explicit_review);
    assert_eq!(
        session
            .metadata
            .get("skill_runtime_loaded_skill_ids")
            .map(String::as_str),
        Some("[\"review\"]")
    );

    let explicit_plan = super::skill_context::SkillContextLoadResult {
        selected_skill_ids: vec!["plan".to_string()],
        selection_source: Some("explicit".to_string()),
        ..Default::default()
    };
    super::skill_context::reset_activation_state_for_new_selection(&mut session, &explicit_plan);
    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));

    mark_test_activation_current(&mut session, "review");
    let automatic_review = super::skill_context::SkillContextLoadResult {
        selected_skill_ids: vec!["review".to_string()],
        selection_source: Some("auto".to_string()),
        ..Default::default()
    };
    super::skill_context::reset_activation_state_for_new_selection(&mut session, &automatic_review);
    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));
}

#[tokio::test]
async fn pin_failure_clears_stale_runtime_selection_and_revision_metadata() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let review = vec!["review".to_string()];
    for index in 0..256 {
        manager
            .store()
            .pin_current_activation(&format!("capacity-{index}"), &review, None)
            .await
            .expect("fill active snapshot capacity");
    }
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        selected_skill_ids: Some(review),
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: Vec::new(),
    };
    let mut session = Session::new("over-capacity", "model");
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["plan"]"#.to_string(),
    );
    session.metadata.insert(
        SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        "999".to_string(),
    );
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        r#"{"plan":999}"#.to_string(),
    );

    super::prepare_session_for_loop(
        &mut session,
        "review",
        &config,
        &tools,
        None,
        "over-capacity",
        &crate::runtime::runner::logging::DebugLogger::new(false),
        false,
        &event_tx,
    )
    .await
    .expect_err("capacity failure must reject the run before model execution");

    assert!(session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
        .is_some_and(|message| message.contains("capacity")));
    let saved = persistence.sessions.lock().expect("recording lock");
    assert!(saved.iter().any(|published| {
        published
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
            .is_some_and(|message| message.contains("capacity"))
    }));
}

#[test]
fn resolve_available_tool_schemas_uses_executor_when_registry_empty() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("z_tool"), schema("a_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_dedupes_and_merges_additional_entries() {
    let config = crate::runtime::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool"), schema("a_tool")],
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "b_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_excludes_disabled_tools() {
    let config = crate::runtime::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool")],
        disabled_tools: ["a_tool".to_string(), "b_tool".to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_hides_load_skill_after_activation() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("load_skill"), schema("read_skill_resource")],
    };
    let mut session = Session::new("session-loaded-skill", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "explicit".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names = resolved
        .iter()
        .map(|schema| schema.function.name.as_str())
        .collect::<Vec<_>>();

    assert!(!names.contains(&"load_skill"));
    assert!(names.contains(&"read_skill_resource"));
}

#[test]
fn resolve_available_tool_schemas_keeps_load_skill_for_new_automatic_selection() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("load_skill"), schema("read_skill_resource")],
    };
    let mut session = Session::new("session-auto-after-loaded-skill", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"debug\",\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "auto".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names = resolved
        .iter()
        .map(|schema| schema.function.name.as_str())
        .collect::<Vec<_>>();

    assert!(names.contains(&"load_skill"));
    assert!(names.contains(&"read_skill_resource"));
}

#[test]
fn explicit_activation_state_becomes_current_only_after_successful_model_load() {
    let mut session = Session::new("explicit-activation-state", "model");
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "explicit".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    assert!(super::skill_context::explicit_activation_pending(&session));

    mark_test_activation_current(&mut session, "review");

    assert!(!super::skill_context::explicit_activation_pending(&session));
    assert_eq!(
        session
            .metadata
            .get("skill_runtime_loaded_skill_ids")
            .map(String::as_str),
        Some("[\"review\"]")
    );
}

#[test]
fn resolve_available_tool_schemas_excludes_canonicalized_disabled_tool_aliases() {
    let config = crate::runtime::config::AgentLoopConfig {
        disabled_tools: [
            "apply_patch".to_string(),
            "FileExists".to_string(),
            "sub_session_manager".to_string(),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![
            schema("Edit"),
            schema("GetFileInfo"),
            schema("SubAgent"),
            schema("Write"),
        ],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Write"]);
}

#[test]
fn legacy_projection_keeps_deferred_tools_with_short_guide_descriptions() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Read", "Sleep", "scheduler"]);

    // Inactive discoverable tools get shortened descriptions
    let sleep = resolved
        .iter()
        .find(|s| s.function.name == "Sleep")
        .unwrap();
    assert!(sleep.function.description.contains("Discoverable"));
    let scheduler = resolved
        .iter()
        .find(|s| s.function.name == "scheduler")
        .unwrap();
    assert!(scheduler.function.description.contains("Discoverable"));
}

#[test]
fn classified_catalog_drives_legacy_projection_without_hiding_deferred_tools() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: [
            "Bash",
            "Read",
            "Grep",
            "Edit",
            "Write",
            "Glob",
            "GetFileInfo",
            "load_skill",
            "workflow_run",
            "custom_tool",
            "mcp__alpha__inspect",
            "mcp__beta__inspect",
            "Workspace",
            "conclusion_with_options",
            "request_permissions",
        ]
        .into_iter()
        .map(schema)
        .collect(),
    };
    let session = Session::new("classified-catalog", "model");

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let classes = catalog
        .iter()
        .map(|entry| (entry.execution_name(), entry.loading_class()))
        .collect::<std::collections::BTreeMap<_, _>>();

    for name in ["Bash", "Read", "Grep", "Edit", "Write"] {
        assert_eq!(classes[name], CapabilityLoadingClass::Core, "{name}");
    }
    for name in [
        "Glob",
        "GetFileInfo",
        "load_skill",
        "workflow_run",
        "custom_tool",
        "mcp__alpha__inspect",
        "mcp__beta__inspect",
    ] {
        assert_eq!(classes[name], CapabilityLoadingClass::Deferred, "{name}");
    }
    for name in [
        "Workspace",
        "conclusion_with_options",
        "request_permissions",
    ] {
        assert_eq!(classes[name], CapabilityLoadingClass::HostOnly, "{name}");
    }

    let model_names = resolve_available_tool_schemas_for_session(&config, &tools, &session)
        .into_iter()
        .map(|schema| schema.function.name)
        .collect::<std::collections::BTreeSet<_>>();
    for name in [
        "Glob",
        "GetFileInfo",
        "load_skill",
        "workflow_run",
        "custom_tool",
        "mcp__alpha__inspect",
        "mcp__beta__inspect",
    ] {
        assert!(model_names.contains(name), "legacy projection lost {name}");
    }
    for name in [
        "Workspace",
        "conclusion_with_options",
        "request_permissions",
    ] {
        assert!(!model_names.contains(name), "HostOnly leaked: {name}");
    }

    let discovery_names =
        crate::capability_discovery::project_classified_tool_capability_metadata(&catalog)
            .into_iter()
            .map(|entry| entry.canonical_name)
            .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        discovery_names, model_names,
        "legacy provider projection and discovery must consume one classified catalog"
    );
}

#[test]
fn progressive_effective_set_intersects_final_session_eligible_catalog() {
    let config = crate::runtime::config::AgentLoopConfig {
        disabled_tools: std::collections::BTreeSet::from(["Glob".to_string()]),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: ["Read", "Glob", "custom_tool", "Workspace"]
            .into_iter()
            .map(schema)
            .collect(),
    };
    let session = Session::new("progressive-session-eligibility", "model");

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let effective = EffectiveCallableSet::from_catalog(
        &catalog,
        CapabilityLoadingMode::Progressive,
        ["Glob", "custom_tool", "Workspace", "missing_tool"],
    );

    assert_eq!(
        effective.execution_names().collect::<Vec<_>>(),
        vec!["Read", "custom_tool"]
    );
    assert!(!effective.contains_execution_name("Glob"));
    assert!(!effective.contains_execution_name("Workspace"));
    assert!(!effective.contains_execution_name("missing_tool"));
}

#[test]
fn ordinary_discover_named_function_is_deferred_and_disableable() {
    let config = crate::runtime::config::AgentLoopConfig {
        disabled_tools: ["discover".to_string()].into_iter().collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("discover")],
    };
    let session = Session::new("custom-discover", "model");

    assert!(resolve_classified_tool_catalog_for_session(&config, &tools, &session).is_empty());
    assert!(resolve_available_tool_schemas_for_session(&config, &tools, &session).is_empty());
}

#[test]
fn exact_custom_alias_registrations_remain_deferred_and_keep_execution_names() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![
            schema("Edit"),
            schema("apply_patch"),
            schema("read_file"),
            schema("execute_command"),
            schema("bash"),
            schema("a::custom_tool"),
            schema("custom_tool"),
        ],
    };
    let session = Session::new("reserved-alias-collision", "model");

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let entries = catalog
        .iter()
        .map(|entry| {
            (
                entry.execution_name().to_string(),
                entry.schema().function.name.clone(),
                entry.loading_class(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        entries,
        vec![
            (
                "Edit".to_string(),
                "Edit".to_string(),
                CapabilityLoadingClass::Core
            ),
            (
                "a::custom_tool".to_string(),
                "a::custom_tool".to_string(),
                CapabilityLoadingClass::Deferred
            ),
            (
                "apply_patch".to_string(),
                "apply_patch".to_string(),
                CapabilityLoadingClass::Deferred
            ),
            (
                "bash".to_string(),
                "bash".to_string(),
                CapabilityLoadingClass::Deferred
            ),
            (
                "custom_tool".to_string(),
                "custom_tool".to_string(),
                CapabilityLoadingClass::Deferred
            ),
            (
                "execute_command".to_string(),
                "execute_command".to_string(),
                CapabilityLoadingClass::Deferred
            ),
            (
                "read_file".to_string(),
                "read_file".to_string(),
                CapabilityLoadingClass::Deferred
            ),
        ]
    );

    let model_names = resolve_available_tool_schemas_for_session(&config, &tools, &session)
        .into_iter()
        .map(|schema| schema.function.name)
        .collect::<Vec<_>>();
    assert_eq!(
        model_names,
        vec![
            "Edit",
            "a::custom_tool",
            "apply_patch",
            "bash",
            "custom_tool",
            "execute_command",
            "read_file"
        ]
    );
    assert_eq!(
        catalog
            .iter()
            .find(|entry| entry.execution_name() == "custom_tool")
            .expect("exact custom execution entry")
            .schema()
            .function
            .description,
        "custom_tool tool"
    );
}

#[test]
fn session_disabled_filter_resolves_exact_shadow_before_alias_fallback() {
    let config = crate::runtime::config::AgentLoopConfig {
        disabled_tools: std::collections::BTreeSet::from(["default::applyPatch".to_string()]),
        ..Default::default()
    };
    let session = Session::new("disabled-exact-shadow", "model");

    let shadowed = StaticToolExecutor {
        schemas: vec![schema("Edit"), schema("apply_patch")],
    };
    let shadowed_names = resolve_classified_tool_catalog_for_session(&config, &shadowed, &session)
        .into_iter()
        .map(|entry| entry.execution_name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(shadowed_names, vec!["Edit"]);

    let unshadowed = StaticToolExecutor {
        schemas: vec![schema("Edit")],
    };
    assert!(
        resolve_classified_tool_catalog_for_session(&config, &unshadowed, &session).is_empty(),
        "without an exact custom shadow, applyPatch must fall back to Edit"
    );
}

#[test]
fn session_eligibility_is_shared_by_model_and_discovery_projections() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![
            schema("update_goal"),
            schema("load_skill"),
            schema("default::update_goal"),
            schema("default::load_skill"),
            schema("Glob"),
            schema("Workspace"),
        ],
    };
    let mut session = Session::new("shared-session-eligibility", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "explicit".to_string(),
    );

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let catalog_names = catalog
        .iter()
        .map(|entry| entry.execution_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        catalog_names,
        std::collections::BTreeSet::from([
            "Glob",
            "Workspace",
            "default::load_skill",
            "default::update_goal",
        ])
    );

    let model_names = resolve_available_tool_schemas_for_session(&config, &tools, &session)
        .into_iter()
        .map(|schema| schema.function.name)
        .collect::<std::collections::BTreeSet<_>>();
    let discovery_names =
        crate::capability_discovery::project_classified_tool_capability_metadata(&catalog)
            .into_iter()
            .map(|entry| entry.canonical_name)
            .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        model_names,
        std::collections::BTreeSet::from([
            "Glob".to_string(),
            "default::load_skill".to_string(),
            "default::update_goal".to_string(),
        ])
    );
    assert_eq!(discovery_names, model_names);
}

#[test]
fn resolve_available_tool_schemas_includes_activated_discoverable_tools() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let mut session = Session::new("session-1", "model");
    bamboo_tools::exposure::activate_discoverable_tools(&mut session, ["Sleep", "scheduler"]);

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Read", "Sleep", "scheduler"]);

    // Activated discoverable tools keep full descriptions
    let sleep = resolved
        .iter()
        .find(|s| s.function.name == "Sleep")
        .unwrap();
    assert!(!sleep.function.description.contains("Discoverable"));
}

#[test]
fn resolve_available_tool_schemas_does_not_mutate_session_metadata() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Write"), schema("session_history")],
    };
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::system("sys"));
    session
        .metadata
        .insert("existing".to_string(), "value".to_string());

    let resolved =
        super::tool_schemas::resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    // All tools are available; inactive discoverable ones get shortened descriptions
    assert_eq!(names, vec!["Write", "session_history"]);
    let session_history = resolved
        .iter()
        .find(|s| s.function.name == "session_history")
        .unwrap();
    assert!(session_history
        .function
        .description
        .contains("Discoverable"));
    assert_eq!(
        session.metadata.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(session.metadata.len(), 1);
}

#[test]
fn model_catalog_excludes_conclusion_with_options_when_enhancement_flag_is_disabled() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("conclusion_with_options")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    assert!(resolved
        .iter()
        .all(|schema| schema.function.name != "conclusion_with_options"));

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let host_entry = catalog
        .iter()
        .find(|entry| entry.execution_name() == "conclusion_with_options")
        .expect("host catalog keeps compatibility entry");
    assert_eq!(
        host_entry.schema().function.description,
        "conclusion_with_options tool"
    );
    assert_eq!(host_entry.loading_class(), CapabilityLoadingClass::HostOnly);
    assert!(!host_entry
        .schema()
        .function
        .description
        .contains(ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT));
}

#[test]
fn model_catalog_excludes_conclusion_with_options_when_enhancement_flag_is_enabled() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("conclusion_with_options")],
    };
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY.to_string(),
        "true".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    assert!(resolved
        .iter()
        .all(|schema| schema.function.name != "conclusion_with_options"));

    let catalog = resolve_classified_tool_catalog_for_session(&config, &tools, &session);
    let host_entry = catalog
        .iter()
        .find(|entry| entry.execution_name() == "conclusion_with_options")
        .expect("host catalog keeps compatibility entry");
    assert_eq!(host_entry.loading_class(), CapabilityLoadingClass::HostOnly);
    assert!(host_entry
        .schema()
        .function
        .description
        .contains(ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT));
    assert!(host_entry
        .schema()
        .function
        .description
        .contains("conclusion"));
    assert!(host_entry.schema().function.description.contains("OK"));
}

#[test]
fn apply_system_prompt_contexts_persists_shared_prompt_snapshot() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let config = bamboo_llm::Config::default();
    config.publish_env_vars();

    let loop_config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some("Base prompt".to_string()),
        ..Default::default()
    };
    let mut session = Session::new("snapshot-session", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.metadata.insert(
        "workspace_path".to_string(),
        "/tmp/snapshot-workspace".to_string(),
    );
    session.add_message(Message::system("Base prompt"));

    let _report = super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &loop_config,
        "## Skill System\nSkill details",
        "## Tool Usage Guidelines\nTool details",
    );

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(snapshot.base_system_prompt, "Base prompt");
    assert!(snapshot
        .skill_context
        .as_deref()
        .unwrap_or_default()
        .contains("Skill details"));
    assert!(snapshot
        .tool_guide_context
        .as_deref()
        .unwrap_or_default()
        .contains("Tool details"));
    assert!(snapshot.effective_system_prompt.contains("Base prompt"));
    assert!(snapshot.prompt_memory_observability.is_none());
}

#[test]
fn legacy_workspace_prompt_migration_recovers_metadata_and_strips_derived_sections_once() {
    let legacy_project = format!(
        "{}\nProject ID: legacy-project\nProject path: /legacy/workspace\nProject home: /private/project-home\n{}",
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::PROJECT_CONTEXT_END_MARKER,
    );
    let legacy_workspace =
        crate::runtime::context::build_workspace_prompt_context("/legacy/workspace")
            .expect("legacy workspace block");
    let legacy_instruction = format!(
        "{}\nlegacy policy\n{}",
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_END_MARKER,
    );
    let legacy_env = format!(
        "{}\nlegacy env /private/env-path\n{}",
        crate::runtime::context::ENV_CONTEXT_START_MARKER,
        crate::runtime::context::ENV_CONTEXT_END_MARKER,
    );
    let legacy_skill = "<!-- BAMBOO_SKILL_CONTEXT_START -->\nlegacy skill /private/skill-path\n<!-- BAMBOO_SKILL_CONTEXT_END -->";
    let legacy_tool_guide = "<!-- BAMBOO_TOOL_GUIDE_START -->\nlegacy guide /private/tool-path\n<!-- BAMBOO_TOOL_GUIDE_END -->";
    let mut session = Session::new("legacy-workspace-migration", "model");
    session.add_message(Message::system(format!(
        "Base prompt\n\n{legacy_project}\n\n{legacy_workspace}\n\n{legacy_instruction}\n\n{legacy_env}\n\n{legacy_skill}\n\n{legacy_tool_guide}"
    )));

    assert!(super::prompt_setup::migrate_legacy_workspace_prompt(
        &mut session
    ));
    assert_eq!(
        session.workspace_path_meta().as_deref(),
        Some("/legacy/workspace")
    );
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].content, "Base prompt");
    assert!(!session.messages[0]
        .content
        .contains(crate::runtime::context::PROJECT_CONTEXT_START_MARKER));
    for marker in [
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
        crate::runtime::context::ENV_CONTEXT_START_MARKER,
        "<!-- BAMBOO_SKILL_CONTEXT_START -->",
        "<!-- BAMBOO_TOOL_GUIDE_START -->",
    ] {
        assert!(!session.messages[0].content.contains(marker));
    }
    assert!(!session.messages[0]
        .content
        .contains(crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER));
    assert!(!session.messages[0].content.contains("/legacy/workspace"));
    assert!(!session.messages[0]
        .content
        .contains("/private/project-home"));
    assert!(!session.messages[0].content.contains("/private/env-path"));
    assert!(!session.messages[0].content.contains("/private/skill-path"));
    assert!(!session.messages[0].content.contains("/private/tool-path"));
    assert!(!super::prompt_setup::migrate_legacy_workspace_prompt(
        &mut session
    ));
}

#[test]
fn legacy_workspace_prompt_migration_accepts_complete_unwrapped_generated_block() {
    let guidance = crate::runtime::context::workspace_prompt_guidance();
    let mut session = Session::new("legacy-unwrapped-workspace-migration", "model");
    session.add_message(Message::system(format!(
        "Base policy\n\nWorkspace path: /legacy/unwrapped\n{guidance}\n\nKeep this policy"
    )));

    assert!(super::prompt_setup::migrate_legacy_workspace_prompt(
        &mut session
    ));
    assert_eq!(
        session.workspace_path_meta().as_deref(),
        Some("/legacy/unwrapped")
    );
    assert_eq!(
        session
            .metadata
            .get(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str),
        Some("session")
    );
    assert_eq!(
        session.messages[0].content,
        "Base policy\n\nKeep this policy"
    );
}

#[test]
fn legacy_workspace_prompt_migration_ignores_ordinary_workspace_path_text() {
    let original = "Base policy\nWorkspace path: /private/attacker-selected\nKeep this policy";
    let mut session = Session::new("ordinary-workspace-path-text", "model");
    session.add_message(Message::system(original));

    assert!(!super::prompt_setup::migrate_legacy_workspace_prompt(
        &mut session
    ));
    assert!(session.workspace_path_meta().is_none());
    assert!(!session
        .metadata
        .contains_key(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY));
    assert!(!session
        .metadata
        .contains_key(crate::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY));
    assert_eq!(session.messages[0].content, original);
}

#[test]
fn legacy_workspace_prompt_migration_preserves_authoritative_metadata() {
    let stale = crate::runtime::context::build_workspace_prompt_context("/stale/workspace")
        .expect("stale workspace block");
    let mut session = Session::new("legacy-workspace-authority", "model");
    session.set_workspace_path_meta("/authoritative/workspace");
    session.add_message(Message::system(format!("Base prompt\n\n{stale}")));

    assert!(super::prompt_setup::migrate_legacy_workspace_prompt(
        &mut session
    ));
    assert_eq!(
        session.workspace_path_meta().as_deref(),
        Some("/authoritative/workspace")
    );
    assert_eq!(session.messages[0].content, "Base prompt");
}

#[test]
fn normalize_base_prompt_strips_repeated_instruction_and_environment_sections() {
    let instruction = |path: &str| {
        format!(
            "{}\n## AGENTS.md\nSource: {path}\nlegacy policy\n{}",
            crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
            crate::runtime::context::instruction::INSTRUCTION_CONTEXT_END_MARKER,
        )
    };
    let prompt = format!(
        "Base prompt\n\n{}\n\n{}\n\n{}\nenv one\n{}\n\n{}\nenv two\n{}",
        instruction("/private/workspace-one/AGENTS.md"),
        instruction("/private/workspace-two/AGENTS.md"),
        crate::runtime::context::ENV_CONTEXT_START_MARKER,
        crate::runtime::context::ENV_CONTEXT_END_MARKER,
        crate::runtime::context::ENV_CONTEXT_START_MARKER,
        crate::runtime::context::ENV_CONTEXT_END_MARKER,
    );

    let normalized = super::prompt_setup::normalize_base_prompt(&prompt);

    assert_eq!(normalized, "Base prompt");
    assert!(!normalized.contains("/private/workspace-one"));
    assert!(!normalized.contains("/private/workspace-two"));
    assert!(!normalized.contains("env one"));
    assert!(!normalized.contains("env two"));
    assert!(!normalized
        .contains(crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER));
    assert!(!normalized.contains(crate::runtime::context::ENV_CONTEXT_START_MARKER));
}

#[test]
fn refresh_prompt_snapshot_from_session_preserves_multi_topic_memory_split_fields() {
    let mut session = Session::new("snapshot-memory-topics", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Cross-session Dream Notebook (read-only)\n````md\nDream note content\n````\n\n### Session Memory Topic: `backend-api`\n````md\n/users and /orders finalized\n````\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream note content")
    );
    let merged = snapshot
        .session_memory_note
        .as_deref()
        .expect("session memory note should be merged from topic blocks");
    assert!(merged.contains("### Session Memory Topic: `backend-api`"));
    assert!(merged.contains("/users and /orders finalized"));
    assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
    assert!(merged.contains("CTA wording approved"));
}

#[test]
fn refresh_prompt_snapshot_from_session_supports_global_dream_fallback_heading() {
    let mut session = Session::new("snapshot-memory-fallback-dream", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.session_memory_note.as_deref(),
        Some("Session note content")
    );
}

#[test]
fn refresh_prompt_snapshot_from_session_extracts_fine_grained_external_memory_fields() {
    let mut session = Session::new("snapshot-memory-fine-grained", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Relevant Durable Memories\nTurn-specific historical memories shortlisted for the latest user request.\n- [active][project] Release rule\n  Summary: Use the release checklist.\n\n### Project Durable Memory Index\n````md\n# Bamboo Memory Index\n- memory entry\n````\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert!(snapshot
        .relevant_durable_memories
        .as_deref()
        .is_some_and(|value| value.contains("Release rule")));
    assert_eq!(
        snapshot.project_memory_index.as_deref(),
        Some("# Bamboo Memory Index\n- memory entry")
    );
    assert_eq!(
        snapshot.global_dream_fallback.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.session_memory_note.as_deref(),
        Some("Session note content")
    );
}

#[test]
fn refresh_prompt_snapshot_from_session_restores_prompt_memory_observability_from_metadata() {
    let mut session = Session::new("snapshot-memory-observability", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.metadata.insert(
        "runtime_prompt_memory_observability".to_string(),
        serde_json::to_string(&bamboo_agent_core::PromptMemoryObservability {
            project_prompt_injection_enabled: true,
            relevant_recall_enabled: false,
            relevant_recall_rerank_enabled: false,
            project_first_dream_enabled: false,
            latest_user_query_present: true,
            resolved_project_key: Some("project-key".to_string()),
            session_notes_status: "loaded".to_string(),
            project_memory_index_status: "loaded".to_string(),
            relevant_memory_status: "disabled".to_string(),
            project_dream_status: "disabled".to_string(),
            global_dream_fallback_status: "forced_loaded".to_string(),
            dream_source: "global_fallback".to_string(),
            session_topic_count: 1,
            truncated_session_topic_count: 0,
            relevant_memory_count: 0,
            session_note_section_chars: 10,
            project_memory_index_section_chars: 20,
            relevant_memory_section_chars: 0,
            project_dream_section_chars: 0,
            global_dream_fallback_section_chars: 40,
            context_pressure_warning_chars: 0,
            external_memory_section_chars: 120,
        })
        .expect("observability should serialize"),
    );
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    let observability = snapshot
        .prompt_memory_observability
        .expect("observability should be restored");
    assert!(!observability.relevant_recall_enabled);
    assert_eq!(observability.global_dream_fallback_status, "forced_loaded");
    assert_eq!(observability.dream_source, "global_fallback");
}

#[test]
fn refresh_prompt_snapshot_from_session_ignores_topic_truncation_note_outside_code_block() {
    let mut session = Session::new("snapshot-memory-topic-note", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Session Memory Topic: `backend-api`\n````md\n/users and /orders finalized\n````\n_(showing 12 of 120 chars — use action=read topic=backend-api to see full content)_\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    let merged = snapshot
        .session_memory_note
        .as_deref()
        .expect("session memory note should be merged from topic blocks");
    assert!(merged.contains("### Session Memory Topic: `backend-api`"));
    assert!(merged.contains("/users and /orders finalized"));
    assert!(!merged.contains("showing 12 of 120 chars"));
    assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
    assert!(merged.contains("CTA wording approved"));
}

#[test]
fn apply_system_prompt_contexts_persists_runtime_prompt_metadata() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_TOOL_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        credential_ref: None,
        configured: true,
        description: Some("Runtime test token".to_string()),
    }];
    config_with_env.publish_env_vars();

    let root = tempfile::tempdir().expect("temp dir");
    let workspace = root.path().join("project");
    std::fs::create_dir_all(root.path().join(".git")).expect("git marker");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    std::fs::write(root.path().join("AGENTS.md"), "Workspace policy").expect("agents file");

    let mut session = Session::new("session-1", "model");
    let env_context = crate::runtime::context::build_env_prompt_context().unwrap_or_default();
    session.add_message(Message::system(format!(
        "Base prompt\n\n{}\nWorkspace path: {}\n{}\n{}\n\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
        env_context,
    )));
    let config = crate::runtime::config::AgentLoopConfig::default();
    let skill_context = "## Skill System\nSkill details";
    let tool_guide_context = "## Tool Usage Guidelines\nGuide details";

    let report = super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        skill_context,
        tool_guide_context,
    );

    assert_eq!(report.version, "bamboo.runtime-system-prompt.v3");
    assert_eq!(report.sections.len(), 7);
    assert_eq!(
        session
            .metadata
            .get("runtime_prompt_composer_version")
            .map(String::as_str),
        Some("bamboo.runtime-system-prompt.v3")
    );
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_flags"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_lengths"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_section_layout"));
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].content, "Base prompt");
    assert_eq!(
        session.metadata.get("skill.context").map(String::as_str),
        Some(skill_context)
    );
    for marker in [
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
        crate::runtime::context::ENV_CONTEXT_START_MARKER,
        "<!-- BAMBOO_SKILL_CONTEXT_START -->",
        "<!-- BAMBOO_TOOL_GUIDE_START -->",
    ] {
        assert!(!session.messages[0].content.contains(marker));
    }
    assert!(!session.messages[0]
        .content
        .contains(workspace.to_string_lossy().as_ref()));
    assert!(!session.messages[0].content.contains("Skill details"));
    assert!(!session.messages[0].content.contains("Guide details"));

    let base_prompt = report
        .section("base_prompt")
        .expect("base prompt section should exist");
    let workspace_context = report
        .section("workspace_context")
        .expect("workspace section should exist");
    let instruction_context = report
        .section("instruction_context")
        .expect("instruction section should exist");
    let env_context = report
        .section("env_context")
        .expect("env section should exist");
    assert!(workspace_context
        .content
        .contains(&format!("Workspace path: {}", workspace.display())));
    assert!(instruction_context.content.contains("Workspace policy"));
    assert!(env_context
        .content
        .contains("environment variables were explicitly configured by the user inside Bodhi"));
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};project_context:environment_project:static:0:0;workspace_context:environment_workspace:dynamic:1:{};instruction_context:environment_instruction:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        instruction_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
    );
    assert_eq!(
        session
            .metadata
            .get("runtime_prompt_section_layout")
            .map(String::as_str),
        Some(expected_layout.as_str())
    );
}

#[test]
fn prompt_assembly_report_component_values_match_sections() {
    use super::prompt_setup::{PromptAssemblyReport, PromptLayer, PromptSection};

    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_TOOL_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        credential_ref: None,
        configured: true,
        description: Some("Runtime test token".to_string()),
    }];
    config_with_env.publish_env_vars();

    let base_prompt = "Base prompt";
    let workspace_context = format!(
        "{}\nWorkspace path: /tmp/workspace\n{}\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );
    let instruction_context = format!(
        "{}\n## AGENTS.md\nSource: /tmp/AGENTS.md\n\nWorkspace policy\n{}",
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_END_MARKER,
    );
    let env_context = crate::runtime::context::build_env_prompt_context().unwrap_or_default();
    let skill_context = "## Skill System\nSkill details";
    let tool_guide_context = "## Tool Usage Guidelines\nGuide details";
    let sections = vec![
        PromptSection::new("base_prompt", PromptLayer::CoreStatic, false, base_prompt),
        PromptSection::new(
            "workspace_context",
            PromptLayer::EnvironmentWorkspace,
            true,
            workspace_context.as_str(),
        ),
        PromptSection::new(
            "instruction_context",
            PromptLayer::EnvironmentInstruction,
            true,
            instruction_context.as_str(),
        ),
        PromptSection::new(
            "env_context",
            PromptLayer::EnvironmentConfiguration,
            true,
            env_context.as_str(),
        ),
        PromptSection::new(
            "skill_context",
            PromptLayer::SkillMetadata,
            true,
            skill_context,
        ),
        PromptSection::new(
            "tool_guide_context",
            PromptLayer::CapabilityTool,
            true,
            tool_guide_context,
        ),
    ];
    let final_prompt = format!(
        "{}\n\n{}\n\n{}\n\n{}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n{}\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n{}\n<!-- BAMBOO_TOOL_GUIDE_END -->",
        base_prompt, workspace_context, instruction_context, env_context, skill_context, tool_guide_context
    );

    let report = PromptAssemblyReport::from_sections(sections, &final_prompt);

    let expected_lengths = format!(
        "base={};project={};workspace={};instruction={};env={};skill={};tool_guide={};external_memory={};task_list={};final={}",
        base_prompt.len(),
        0,
        workspace_context.len(),
        instruction_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
        0,
        0,
        final_prompt.len(),
    );
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};workspace_context:environment_workspace:dynamic:1:{};instruction_context:environment_instruction:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        instruction_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
    );

    assert_eq!(
        report.component_flags_value(),
        "project=0;workspace=1;instruction=1;env=1;skill=1;tool_guide=1;external_memory=0;task_list=0"
    );
    assert_eq!(report.component_lengths_value(), expected_lengths);
    assert_eq!(report.section_layout_value(), expected_layout);
}

#[test]
fn build_stable_prompt_frame_contains_only_invariant_system_content() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_PROMPT_ENVELOPE_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        credential_ref: None,
        configured: true,
        description: Some("Prompt envelope token".to_string()),
    }];
    config_with_env.publish_env_vars();

    let workspace = std::env::temp_dir().join("bamboo-prompt-envelope-workspace");
    let system_prompt = format!(
        "Base system\n\n{}\nWorkspace path: {}\n{}\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );

    let config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some(system_prompt),
        ..Default::default()
    };
    let mut session = Session::new("session-stable-frame-1", "model");
    session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
    session.metadata.insert(
        "skill.context".to_string(),
        "## Skill\nUse the skill".to_string(),
    );

    let stable = build_stable_prompt_frame_with_sections(
        &session,
        &config,
        &[],
        &std::collections::BTreeSet::new(),
    )
    .0;

    assert!(stable.stable_instructions.contains("Base system"));
    assert!(!stable.stable_instructions.contains("Workspace path:"));
    assert!(!stable
        .stable_instructions
        .contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
    assert!(!stable
        .stable_instructions
        .contains(crate::runtime::context::PROJECT_CONTEXT_START_MARKER));
    assert!(!stable
        .stable_instructions
        .contains(crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER));
    assert!(!stable
        .stable_instructions
        .contains("environment variables were explicitly configured by the user inside Bodhi"));
    assert!(!stable.stable_instructions.contains("## Skill"));
    assert!(!stable
        .stable_instructions
        .contains("BAMBOO_TOOL_GUIDE_START"));
    // Framework-invariant directives ride on top of even a fully custom override
    // base (`config.system_prompt`), so they are present regardless of the user's
    // base prompt.
    assert!(stable
        .stable_instructions
        .contains("Investigate before you conclude"));
    assert!(stable.stable_instructions.contains("Verify your own work"));
    assert!(stable.stable_prefix_messages.is_empty());
}

#[test]
fn build_stable_prompt_frame_strips_round_dynamic_prompt_blocks() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let workspace = std::env::temp_dir().join("bamboo-prompt-envelope-workspace-dynamic");
    let system_prompt = format!(
        "Base system\n\n{}\nWorkspace path: {}\n{}\n{}\n\n<!-- BAMBOO_TASK_LIST_START -->\n## Current Task List: Agent Tasks\n[/] task-1: do the thing\n<!-- BAMBOO_TASK_LIST_END -->\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\nExternal memory snapshot\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPLAN MODE ACTIVE\n<!-- BAMBOO_PLAN_MODE_END -->\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\nPlan runtime snapshot\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );

    let config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some(system_prompt),
        ..Default::default()
    };
    let mut session = Session::new("session-stable-frame-2", "model");
    session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
    session.metadata.insert(
        "skill.context".to_string(),
        "## Skill\nUse the skill".to_string(),
    );
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: "do the thing".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "Older work was compressed.",
        2,
        80,
    ));

    let stable = build_stable_prompt_frame_with_sections(
        &session,
        &config,
        &[],
        &std::collections::BTreeSet::new(),
    )
    .0;

    assert!(stable.stable_instructions.contains("Base system"));
    assert!(!stable.stable_instructions.contains("Workspace path:"));
    assert!(!stable
        .stable_instructions
        .contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
    assert!(!stable
        .stable_instructions
        .contains(crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER));
    assert!(!stable.stable_instructions.contains("Current Task List"));
    assert!(!stable
        .stable_instructions
        .contains("External memory snapshot"));
    assert!(!stable.stable_instructions.contains("PLAN MODE ACTIVE"));
    assert!(!stable.stable_instructions.contains("Plan runtime snapshot"));
}

#[test]
fn stable_prompt_frame_carries_instructions_and_prefix_messages() {
    // The stable frame is what feeds the IR's system field + StablePrefix run; the
    // Responses-input/chat projections are derived by the IR's lowering methods, not
    // a per-envelope converter.
    let stable =
        StablePromptFrame::new("Stable instructions", vec![Message::user("stable prefix")]);
    assert_eq!(stable.stable_instructions, "Stable instructions");
    assert_eq!(stable.stable_prefix_messages.len(), 1);
    assert_eq!(stable.stable_prefix_messages[0].content, "stable prefix");
}

#[tokio::test]
async fn runtime_skill_context_rejects_another_projects_workspace_overlay() {
    struct CrossProjectSource {
        descriptor: crate::project_context::ProjectDescriptor,
        workspace_owner: bamboo_domain::ProjectId,
    }

    #[async_trait]
    impl crate::project_context::ProjectContextSource for CrossProjectSource {
        async fn find_project(
            &self,
            project_id: &bamboo_domain::ProjectId,
        ) -> Result<
            Option<crate::project_context::ProjectDescriptor>,
            crate::project_context::ProjectContextError,
        > {
            Ok((&self.descriptor.id == project_id).then(|| self.descriptor.clone()))
        }

        async fn find_workspace_owner(
            &self,
            _workspace: &std::path::Path,
        ) -> Result<Option<bamboo_domain::ProjectId>, crate::project_context::ProjectContextError>
        {
            Ok(Some(self.workspace_owner.clone()))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("foreign-workspace");
    let foreign_skill = workspace.join(".bamboo/skills/foreign-only");
    std::fs::create_dir_all(&foreign_skill).expect("foreign skill");
    std::fs::write(
        foreign_skill.join("SKILL.md"),
        "---\nname: foreign-only\ndescription: MUST NOT LOAD FOREIGN OVERLAY\n---\nFOREIGN BODY\n",
    )
    .expect("foreign skill");
    let session_project = bamboo_domain::ProjectId::parse("session-project").expect("Project id");
    let workspace_owner =
        bamboo_domain::ProjectId::parse("workspace-owner").expect("owner Project id");
    let project_home = directory.path().join("projects/session-project");
    std::fs::create_dir_all(project_home.join("skills")).expect("Project skills");
    let descriptor = crate::project_context::ProjectDescriptor {
        id: session_project.clone(),
        name: "Session Project".to_string(),
        project_path: Some(workspace.clone()),
        home: project_home.clone(),
        workspace_bindings: Vec::new(),
        resources: bamboo_domain::ProjectResourceSummary {
            project_id: session_project.clone(),
            resource_revision: 1,
            resources: Vec::new(),
        },
    };
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("global-skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let resolver = Arc::new(crate::project_context::ProjectContextResolver::new(
        Arc::new(CrossProjectSource {
            descriptor,
            workspace_owner,
        }),
    ));
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        project_context_resolver: Some(resolver),
        selected_skill_ids: Some(vec!["foreign-only".to_string()]),
        ..Default::default()
    };
    let mut session = Session::new("cross-project-skill", "model");
    session.set_project_id_meta(session_project.to_string());
    session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());

    let error = super::skill_context::load_skill_context(
        &config,
        &session,
        "cross-project-skill",
        "load foreign-only",
        false,
    )
    .await
    .expect_err("foreign workspace overlay must fail closed before skill discovery");
    assert!(error.contains("belongs to Project"));
    assert!(!error.contains("MUST NOT LOAD FOREIGN OVERLAY"));
    assert!(!session
        .metadata
        .contains_key(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY));

    let mut unassigned = Session::new("unassigned-cross-project-skill", "model");
    unassigned.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
    let error = super::skill_context::load_skill_context(
        &config,
        &unassigned,
        "unassigned-cross-project-skill",
        "load foreign-only",
        false,
    )
    .await
    .expect_err("Unassigned session must not load an owned workspace overlay");
    assert!(error.contains("but the session is Unassigned"));
    assert!(!error.contains("MUST NOT LOAD FOREIGN OVERLAY"));
    assert!(!unassigned
        .metadata
        .contains_key(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY));
}
