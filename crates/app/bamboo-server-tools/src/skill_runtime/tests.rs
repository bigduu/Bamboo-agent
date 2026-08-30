use super::{
    load_skill::plan_allows_dynamic_provider, LoadSkillTool, ReadSkillResourceTool, SkillToolAccess,
};
use bamboo_skills::access_control::{parse_loaded_skill_ids, serialize_loaded_skill_ids};
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_SUMMARY_METADATA_KEY, LAST_RESOURCE_READ_SUMMARY_METADATA_KEY,
    SKILL_RUNTIME_ACTIVATION_GENERATION_KEY, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY, SKILL_RUNTIME_SELECTION_SOURCE_KEY,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    FunctionSchema, Tool, ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome,
    ToolResult, ToolSchema,
};
use bamboo_agent_core::Session;
use bamboo_llm::Config;
use bamboo_skills::{SkillManager, SkillStoreConfig};

fn public_load_skill_receipt(result: &ToolResult) -> serde_json::Value {
    let value: serde_json::Value =
        serde_json::from_str(&result.result).expect("load_skill receipt JSON");
    let keys = value
        .as_object()
        .expect("load_skill receipt object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        std::collections::BTreeSet::from([
            "activation_status",
            "kind",
            "revision",
            "skill_id",
            "source",
        ]),
        "load_skill must expose only its typed public receipt"
    );
    for private_key in [
        "instructions",
        "skill_base_dir",
        "resource_files",
        "dynamic_context",
        "allowed_tools",
        "args",
    ] {
        assert!(
            value.get(private_key).is_none(),
            "public load_skill receipt exposed {private_key}: {value:#}"
        );
    }
    value
}

#[tokio::test]
async fn real_runner_auto_load_skill_survives_final_save_and_restart() {
    use bamboo_agent_core::storage::AttachmentReader;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_engine::{Agent, ExecuteRequestBuilder};
    use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
    use futures::stream;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio_util::sync::CancellationToken;

    struct NoAttachments;
    #[async_trait::async_trait]
    impl AttachmentReader for NoAttachments {
        async fn read_attachment(
            &self,
            _session_id: &str,
            _attachment_id: &str,
        ) -> std::io::Result<Option<(Vec<u8>, String)>> {
            Ok(None)
        }
    }

    struct RecordingProvider {
        queue: AsyncMutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
        requests: AsyncMutex<Vec<Vec<bamboo_agent_core::Message>>>,
    }
    #[async_trait::async_trait]
    impl LLMProvider for RecordingProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.requests.lock().await.push(messages.to_vec());
            let items = self.queue.lock().await.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let skill_dir = directory.path().join("skills/runner-review");
    std::fs::create_dir_all(&skill_dir).expect("skill dir");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: runner-review
description: RUNNER_AUTO_REVIEW_NEEDLE
metadata:
  invocation_policy:
    explicit: true
    automatic: true
---
RUNNER_RUNTIME_INSTRUCTIONS"#,
    )
    .expect("skill");
    std::fs::create_dir_all(skill_dir.join("references")).expect("resource directory");
    std::fs::write(
        skill_dir.join("references/private.txt"),
        "RUNNER_PRIVATE_RESOURCE_SENTINEL",
    )
    .expect("private workflow resource");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("manager");
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let cache = Arc::new(dashmap::DashMap::new());
    let repo = bamboo_engine::SessionRepository::new(cache, storage.clone(), locked.clone());
    let config = Arc::new(RwLock::new(Config::default()));
    let load_skill = LoadSkillTool::new(manager.clone(), config.clone(), repo.clone());
    let tools = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(load_skill)
            .expect("register load_skill")
            .build(),
    );
    let call = ToolCall {
        id: "call-load-runner-review".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "load_skill".to_string(),
            arguments: serde_json::json!({"skill_id":"runner-review"}).to_string(),
        },
    };
    let provider = Arc::new(RecordingProvider {
        queue: AsyncMutex::new(vec![
            vec![Ok(LLMChunk::ToolCalls(vec![call])), Ok(LLMChunk::Done)],
            vec![
                Ok(LLMChunk::Token("first done".to_string())),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(LLMChunk::Token("restart done".to_string())),
                Ok(LLMChunk::Done),
            ],
        ]),
        requests: AsyncMutex::new(Vec::new()),
    });
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            directory.path().join("metrics.db"),
        )),
        7,
    );
    let agent = Agent::builder()
        .storage(storage.clone())
        .persistence(Arc::new(repo.clone()))
        .attachment_reader(Arc::new(NoAttachments))
        .skill_manager(manager)
        .metrics_collector(metrics)
        .config(config)
        .provider(provider.clone())
        .default_tools(tools)
        .build()
        .expect("agent");

    let session_id = "real-runner-auto-load";
    let mut session = Session::new(session_id, "test-model");
    session.title = "PRESERVE_TITLE".to_string();
    session
        .metadata
        .insert("external.metadata".to_string(), "preserve".to_string());
    session.add_message(bamboo_agent_core::Message::system("system"));
    session.add_message(bamboo_agent_core::Message::user(
        "Please use RUNNER_AUTO_REVIEW_NEEDLE",
    ));
    repo.save(&mut session).await.expect("seed session");
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(128);
    agent
        .execute(
            &mut session,
            ExecuteRequestBuilder::new(
                "Please use RUNNER_AUTO_REVIEW_NEEDLE",
                event_tx,
                CancellationToken::new(),
            )
            .model("test-model")
            .build(),
        )
        .await
        .expect("real runner executes");
    assert!(session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    let saved = storage
        .load_session(session_id)
        .await
        .expect("load")
        .expect("saved");
    assert!(saved
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
    assert!(saved
        .metadata
        .contains_key(SKILL_RUNTIME_SELECTION_SOURCE_KEY));
    assert!(saved
        .metadata
        .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY));
    assert_eq!(saved.title, "PRESERVE_TITLE");
    assert_eq!(
        saved.metadata.get("external.metadata").map(String::as_str),
        Some("preserve")
    );
    assert!(saved.messages.iter().any(|message| {
        message
            .tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.function.name == "load_skill"))
    }));
    let durable_messages = serde_json::to_string(&saved.messages).expect("durable messages");
    let private_root = directory.path().to_string_lossy().into_owned();
    for private in [
        "RUNNER_RUNTIME_INSTRUCTIONS",
        "RUNNER_PRIVATE_RESOURCE_SENTINEL",
        private_root.as_str(),
        "skill_base_dir",
        "resource_files",
        "dynamic_context",
    ] {
        assert!(
            !durable_messages.contains(private),
            "durable/public history leaked {private}: {durable_messages}"
        );
    }
    let mut tool_complete = None;
    while let Ok(event) = event_rx.try_recv() {
        if let bamboo_agent_core::AgentEvent::ToolComplete { result, .. } = event {
            tool_complete = Some(result);
        }
    }
    let tool_complete = tool_complete.expect("ToolComplete event");
    let receipt = public_load_skill_receipt(&tool_complete);
    assert_eq!(receipt["skill_id"], "runner-review");
    assert_eq!(receipt["activation_status"], "active");
    let tool_complete_json =
        serde_json::to_string(&tool_complete).expect("serialized ToolComplete result");
    for private in [
        "RUNNER_RUNTIME_INSTRUCTIONS",
        "RUNNER_PRIVATE_RESOURCE_SENTINEL",
        private_root.as_str(),
        "skill_base_dir",
        "resource_files",
        "dynamic_context",
    ] {
        assert!(
            !tool_complete_json.contains(private),
            "live ToolComplete leaked {private}: {tool_complete_json}"
        );
    }

    let requests = provider.requests.lock().await;
    let second = serde_json::to_string(&requests[1]).expect("second request");
    assert_eq!(
        requests[1]
            .iter()
            .filter(|message| serde_json::to_string(message)
                .expect("message")
                .contains("workflow_runtime"))
            .count(),
        1,
        "{second}"
    );
    assert!(second.contains("RUNNER_RUNTIME_INSTRUCTIONS"));
    drop(requests);

    let mut restarted = saved;
    restarted.add_message(bamboo_agent_core::Message::user("continue"));
    let (restart_tx, _restart_rx) = tokio::sync::mpsc::channel(128);
    agent
        .execute(
            &mut restarted,
            ExecuteRequestBuilder::new("continue", restart_tx, CancellationToken::new())
                .model("test-model")
                .build(),
        )
        .await
        .expect("restart active runner");
    let requests = provider.requests.lock().await;
    let restart = serde_json::to_string(&requests[2]).expect("restart request");
    assert_eq!(
        requests[2]
            .iter()
            .filter(|message| serde_json::to_string(message)
                .expect("message")
                .contains("workflow_runtime"))
            .count(),
        1,
        "{restart}"
    );
    assert!(restart.contains("RUNNER_RUNTIME_INSTRUCTIONS"));
}

#[tokio::test]
async fn explicit_fail_closed_dynamic_context_stop_matrix_keeps_main_runner_alive() {
    use bamboo_agent_core::storage::AttachmentReader;
    use bamboo_agent_core::tools::FunctionCall;
    use bamboo_engine::{Agent, ExecuteRequestBuilder};
    use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
    use futures::stream;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio_util::sync::CancellationToken;

    struct NoAttachments;
    #[async_trait::async_trait]
    impl AttachmentReader for NoAttachments {
        async fn read_attachment(
            &self,
            _session_id: &str,
            _attachment_id: &str,
        ) -> std::io::Result<Option<(Vec<u8>, String)>> {
            Ok(None)
        }
    }
    struct CapturingProvider {
        requests: AsyncMutex<Vec<Vec<bamboo_agent_core::Message>>>,
        queue: AsyncMutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
    }
    #[async_trait::async_trait]
    impl LLMProvider for CapturingProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.requests.lock().await.push(messages.to_vec());
            let items = self.queue.lock().await.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    for (id, stop) in [("dynamic-continue", false), ("dynamic-stop", true)] {
        let root = directory.path().join("skills").join(id);
        std::fs::create_dir_all(&root).expect("skill root");
        std::fs::write(
            root.join("SKILL.md"),
            format!(
                "---\nname: {id}\ndescription: {id}\nmetadata:\n  dynamic_context:\n    - id: read\n      tool: Read\n      input: {{path: README.md}}\n      stop_on_failure: {stop}\n---\n{id} instructions"
            ),
        )
        .expect("skill");
    }
    std::fs::write(directory.path().join("README.md"), "workspace").expect("readme");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("manager");
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(
        Arc::new(dashmap::DashMap::new()),
        storage.clone(),
        locked,
    );
    let dynamic_provider = Arc::new(DynamicProviderExecutor {
        mode: DynamicProviderMode::Complete("MUST_NOT_EXECUTE".to_string()),
        calls: AtomicUsize::new(0),
        saw_bypass: AtomicBool::new(false),
        saw_auto: AtomicBool::new(false),
        saw_plan: AtomicBool::new(false),
    });
    let config = Arc::new(RwLock::new(Config::default()));
    let tools = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(
                LoadSkillTool::new(manager.clone(), config.clone(), repo.clone())
                    .with_fail_closed_context_registry(dynamic_provider.clone()),
            )
            .expect("load tool")
            .build(),
    );
    let activation_call = |id: &str, skill_id: &str| ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "load_skill".to_string(),
            arguments: serde_json::json!({"skill_id": skill_id}).to_string(),
        },
    };
    let answer = || {
        vec![
            Ok(LLMChunk::Token("main model continued".to_string())),
            Ok(LLMChunk::Done),
        ]
    };
    let llm = Arc::new(CapturingProvider {
        requests: AsyncMutex::new(Vec::new()),
        queue: AsyncMutex::new(vec![
            vec![
                Ok(LLMChunk::ToolCalls(vec![activation_call(
                    "load-dynamic-continue",
                    "dynamic-continue",
                )])),
                Ok(LLMChunk::Done),
            ],
            answer(),
            vec![
                Ok(LLMChunk::ToolCalls(vec![activation_call(
                    "load-dynamic-stop",
                    "dynamic-stop",
                )])),
                Ok(LLMChunk::Done),
            ],
            answer(),
        ]),
    });
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            directory.path().join("matrix-metrics.db"),
        )),
        7,
    );
    let agent = Agent::builder()
        .storage(storage.clone())
        .persistence(Arc::new(repo.clone()))
        .attachment_reader(Arc::new(NoAttachments))
        .skill_manager(manager)
        .metrics_collector(metrics)
        .config(config)
        .provider(llm.clone())
        .default_tools(tools)
        .build()
        .expect("agent");

    for (index, (id, stop)) in [("dynamic-continue", false), ("dynamic-stop", true)]
        .into_iter()
        .enumerate()
    {
        let session_id = format!("explicit-matrix-{index}");
        let mut session = Session::new(&session_id, "test-model");
        session.set_workspace_path_meta(directory.path().to_string_lossy().into_owned());
        session.add_message(bamboo_agent_core::Message::system("system"));
        session.add_message(bamboo_agent_core::Message::user("run selected workflow"));
        repo.save(&mut session).await.expect("seed");
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(64);
        agent
            .execute(
                &mut session,
                ExecuteRequestBuilder::new(
                    "run selected workflow",
                    event_tx,
                    CancellationToken::new(),
                )
                .model("test-model")
                .selected_skill_ids(vec![id.to_string()])
                .build(),
            )
            .await
            .expect("main model always continues");
        assert_eq!(
            session
                .metadata
                .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY),
            !stop
        );
        assert_eq!(
            session
                .metadata
                .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY),
            !stop
        );
        assert_eq!(
            session
                .messages
                .iter()
                .filter_map(|message| message.tool_calls.as_ref())
                .flatten()
                .filter(|call| call.function.name == "load_skill")
                .count(),
            1,
            "explicit workflow activation must be model-issued exactly once"
        );
        let request_index = index * 2 + 1;
        let request =
            serde_json::to_string(&llm.requests.lock().await[request_index]).expect("request");
        if stop {
            assert!(!request.contains("context_type: workflow_runtime"));
        } else {
            assert!(request.contains("context_type: workflow_runtime"));
            assert!(request.contains("typed_authority_unavailable"));
        }
    }
    assert_eq!(dynamic_provider.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn parse_loaded_skill_ids_supports_json_and_csv() {
    let from_json = parse_loaded_skill_ids(r#"["skill-b","skill-a","skill-a"]"#);
    assert_eq!(from_json.len(), 2);
    assert!(from_json.contains("skill-a"));
    assert!(from_json.contains("skill-b"));

    let from_csv = parse_loaded_skill_ids("skill-c, skill-d , skill-c");
    assert_eq!(from_csv.len(), 2);
    assert!(from_csv.contains("skill-c"));
    assert!(from_csv.contains("skill-d"));
}

#[test]
fn serialize_loaded_skill_ids_is_stable_and_sorted() {
    let mut ids = HashSet::new();
    ids.insert("skill-b".to_string());
    ids.insert("skill-a".to_string());

    assert_eq!(serialize_loaded_skill_ids(&ids), r#"["skill-a","skill-b"]"#);
}

#[tokio::test]
async fn dynamic_context_is_permission_scoped_redacted_cached_and_event_deduped() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let skill_dir = temp_dir.path().join("skills/dynamic-demo");
    std::fs::create_dir_all(&skill_dir).expect("skill dir");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: dynamic-demo
description: Dynamic demo
metadata:
  dynamic_context:
    - id: workspace-read
      tool: Read
      input:
        path: input.txt
      max_chars: 128
      timeout_ms: 1000
      cache_ttl_secs: 60
---
Use the dynamic context."#,
    )
    .expect("skill file");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    manager.initialize().await.expect("manager");
    let session_id = "dynamic-context-session";
    let mut session = Session::new(session_id, "model");
    session.set_workspace_path_meta(temp_dir.path().to_string_lossy().into_owned());
    std::fs::write(temp_dir.path().join("input.txt"), "workspace input").expect("workspace input");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["dynamic-demo"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session).await.expect("save session");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let provider = Arc::new(DynamicProviderExecutor {
        mode: DynamicProviderMode::Complete(
            serde_json::json!({
                "api_key": "super-secret-output",
                "data": "x".repeat(512),
            })
            .to_string(),
        ),
        calls: AtomicUsize::new(0),
        saw_bypass: AtomicBool::new(false),
        saw_auto: AtomicBool::new(false),
        saw_plan: AtomicBool::new(false),
    });
    let tool = LoadSkillTool::new(
        manager.clone(),
        Arc::new(RwLock::new(Config::default())),
        repo.clone(),
    )
    .with_test_context_tools(provider.clone());
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
    let context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "dynamic-load",
        event_tx: Some(&event_tx),
        available_tool_schemas: None,
        bypass_permissions: true,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    for _ in 0..2 {
        let ToolOutcome::Completed(result) = tool
            .invoke(
                serde_json::json!({"skill_id":"dynamic-demo"}),
                context.to_tool_ctx(),
            )
            .await
            .expect("load dynamic workflow")
        else {
            panic!("load must complete")
        };
        let payload = public_load_skill_receipt(&result);
        assert_eq!(payload["activation_status"], "active");
    }
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        2,
        "each load rechecks the current provider permission policy"
    );
    assert!(!provider.saw_bypass.load(Ordering::SeqCst));
    assert!(matches!(
        event_rx.try_recv(),
        Ok(bamboo_agent_core::AgentEvent::WorkflowActivated { ref workflow_id, .. })
            if workflow_id == "dynamic-demo"
    ));
    assert!(
        event_rx.try_recv().is_err(),
        "activation event is deduplicated"
    );
    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("saved session");
    let cache = saved
        .metadata
        .get(bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY)
        .expect("bounded cache metadata");
    assert!(!cache.contains("input.txt"));
    assert!(!cache.contains("super-secret-output"));
    let active = saved
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
        .expect("durable private activation metadata");
    assert!(active.dynamic_context[0].content.contains("[REDACTED]"));
    assert!(!active.dynamic_context[0]
        .content
        .contains("super-secret-output"));
    assert!(active.dynamic_context[0].diagnostic.is_some());

    let production_session_id = "dynamic-context-production-authority";
    let mut production_session = Session::new(production_session_id, "model");
    production_session.set_workspace_path_meta(temp_dir.path().to_string_lossy().into_owned());
    production_session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["dynamic-demo"]"#.to_string(),
    );
    repo.save(&mut production_session)
        .await
        .expect("save production session");
    let production_tool = LoadSkillTool::new(
        manager.clone(),
        Arc::new(RwLock::new(Config::default())),
        repo.clone(),
    )
    .with_fail_closed_context_registry(provider.clone());
    let calls_before = provider.calls.load(Ordering::SeqCst);
    let production_context = ToolExecutionContext {
        session_id: Some(production_session_id),
        root_session_id: None,
        tool_call_id: "production-authority-load",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let ToolOutcome::Completed(production) = production_tool
        .invoke(
            serde_json::json!({"skill_id":"dynamic-demo"}),
            production_context.to_tool_ctx(),
        )
        .await
        .expect("production authority degrades without aborting")
    else {
        panic!("production load completes")
    };
    let production = public_load_skill_receipt(&production);
    assert_eq!(production["activation_status"], "active");
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls_before);
    let production_saved = repo
        .storage()
        .load_session(production_session_id)
        .await
        .expect("load")
        .expect("production saved");
    assert!(production_saved
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    let production_active = production_saved
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
        .expect("private production activation");
    assert_eq!(
        production_active.dynamic_context[0].provenance,
        "typed_authority_unavailable"
    );
    assert_eq!(production_active.dynamic_context[0].content, "");
    assert!(!production_saved
        .metadata
        .contains_key(bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY));

    let typed_session_id = "dynamic-context-typed-authority";
    let mut typed_session = Session::new(typed_session_id, "model");
    typed_session.set_workspace_path_meta(temp_dir.path().to_string_lossy().into_owned());
    typed_session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["dynamic-demo"]"#.to_string(),
    );
    repo.save(&mut typed_session)
        .await
        .expect("save typed-authority session");
    let permission_config = Arc::new(bamboo_tools::permission::PermissionConfig::new());
    permission_config.set_policy_revision(41);
    let permission_checker: Arc<dyn bamboo_tools::permission::PermissionChecker> = Arc::new(
        bamboo_tools::permission::ConfigPermissionChecker::new(permission_config.clone()),
    );
    let runtime_config = Arc::new(RwLock::new(Config::default()));
    let context_tools: Arc<dyn ToolExecutor> = Arc::new(
        bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
            runtime_config.clone(),
            permission_checker,
        ),
    );
    let typed_tool = LoadSkillTool::new(manager, runtime_config, repo.clone())
        .with_permission_checked_context_registry(context_tools, Some(permission_config.clone()));
    let typed_context = ToolExecutionContext {
        session_id: Some(typed_session_id),
        root_session_id: None,
        tool_call_id: "typed-authority-load",
        event_tx: None,
        available_tool_schemas: None,
        // The outer load_skill call may run under bypass. Its dynamic provider
        // must still dispatch with bypass disabled through the #601 checker.
        bypass_permissions: true,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let ToolOutcome::Completed(typed) = typed_tool
        .invoke(
            serde_json::json!({"skill_id":"dynamic-demo"}),
            typed_context.to_tool_ctx(),
        )
        .await
        .expect("typed permission authority executes the provider")
    else {
        panic!("typed authority load completes")
    };
    let typed = public_load_skill_receipt(&typed);
    assert_eq!(typed["activation_status"], "active");
    let typed_saved = repo
        .storage()
        .load_session(typed_session_id)
        .await
        .expect("load typed authority session")
        .expect("typed authority session remains durable");
    let typed_active = typed_saved
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
        .expect("private typed activation");
    assert_eq!(
        typed_active.dynamic_context[0].provenance,
        "registered_tool_permission_checked"
    );
    assert!(typed_active.dynamic_context[0]
        .content
        .contains("workspace input"));
    assert_eq!(permission_config.policy_revision(), 41);
}

#[tokio::test]
async fn dynamic_context_auto_and_plan_flags_survive_read_dispatch_while_invalid_write_fails_closed(
) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let skill_dir = temp_dir.path().join("skills/dynamic-approval");
    std::fs::create_dir_all(&skill_dir).expect("skill dir");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: dynamic-approval
description: Dynamic approval demo
metadata:
  dynamic_context:
    - id: approval-read
      tool: Read
      input: {path: README.md}
      stop_on_failure: true
---
Never activate after an approval pause."#,
    )
    .expect("skill file");
    let mutating_skill_dir = temp_dir.path().join("skills/dynamic-mutation");
    std::fs::create_dir_all(&mutating_skill_dir).expect("mutating skill dir");
    std::fs::write(
        mutating_skill_dir.join("SKILL.md"),
        r#"---
name: dynamic-mutation
description: Dynamic mutating provider
metadata:
  dynamic_context:
    - id: write
      tool: Write
      input: {file_path: blocked.txt, content: must-not-run}
      stop_on_failure: true
---
Plan must block this provider before dispatch."#,
    )
    .expect("mutating skill file");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    manager.initialize().await.expect("manager");
    let session_id = "dynamic-approval-session";
    let mut session = Session::new(session_id, "model");
    session.set_workspace_path_meta(temp_dir.path().to_string_lossy().into_owned());
    std::fs::write(temp_dir.path().join("README.md"), "workspace readme").expect("readme");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["dynamic-approval","dynamic-mutation"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session).await.expect("save session");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let provider = Arc::new(DynamicProviderExecutor {
        mode: DynamicProviderMode::NeedsHuman,
        calls: AtomicUsize::new(0),
        saw_bypass: AtomicBool::new(false),
        saw_auto: AtomicBool::new(false),
        saw_plan: AtomicBool::new(false),
    });
    let tool = LoadSkillTool::new(manager, Arc::new(RwLock::new(Config::default())), repo)
        .with_test_context_tools(provider.clone());
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
    let context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "dynamic-approval-load",
        event_tx: Some(&event_tx),
        available_tool_schemas: None,
        bypass_permissions: true,
        auto_approve_permissions: true,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let ToolOutcome::Completed(result) = tool
        .invoke(
            serde_json::json!({"skill_id":"dynamic-approval"}),
            context.to_tool_ctx(),
        )
        .await
        .expect("degraded result continues main session")
    else {
        panic!("load must return degraded completed payload")
    };
    let payload: serde_json::Value = serde_json::from_str(&result.result).expect("payload json");
    assert_eq!(payload["activation_status"], "degraded");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(!provider.saw_bypass.load(Ordering::SeqCst));
    assert!(provider.saw_auto.load(Ordering::SeqCst));
    assert!(!provider.saw_plan.load(Ordering::SeqCst));
    assert!(event_rx.try_recv().is_err());
    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("saved session");
    assert!(!saved
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));

    let plan_read_context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "dynamic-plan-read-load",
        event_tx: Some(&event_tx),
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: true,
        plan_read_only: true,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let ToolOutcome::Completed(plan_read_result) = tool
        .invoke(
            serde_json::json!({"skill_id":"dynamic-approval"}),
            plan_read_context.to_tool_ctx(),
        )
        .await
        .expect("Plan+Auto read provider degrades without pausing")
    else {
        panic!("Plan+Auto read provider must not return a pause")
    };
    let plan_read_payload = public_load_skill_receipt(&plan_read_result);
    assert_eq!(plan_read_payload["activation_status"], "degraded");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(!provider.saw_bypass.load(Ordering::SeqCst));
    assert!(provider.saw_auto.load(Ordering::SeqCst));
    assert!(provider.saw_plan.load(Ordering::SeqCst));
    assert!(event_rx.try_recv().is_err());

    let invalid_write_context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "dynamic-invalid-write-load",
        event_tx: Some(&event_tx),
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: true,
        plan_read_only: true,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let ToolOutcome::Completed(invalid_write_result) = tool
        .invoke(
            serde_json::json!({"skill_id":"dynamic-mutation"}),
            invalid_write_context.to_tool_ctx(),
        )
        .await
        .expect("invalid mutating provider metadata degrades without dispatch")
    else {
        panic!("invalid mutating provider metadata must not return a pause")
    };
    let invalid_write_payload = public_load_skill_receipt(&invalid_write_result);
    assert_eq!(invalid_write_payload["activation_status"], "degraded");
    let invalid_write_session = storage
        .load_session(session_id)
        .await
        .expect("load invalid provider session")
        .expect("invalid provider session remains durable");
    let invalid_write_context = invalid_write_session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<bamboo_skills::DynamicContextBlock>>(raw).ok())
        .expect("private invalid provider diagnostic");
    assert_eq!(
        invalid_write_context[0].provenance,
        "invalid_workflow_metadata"
    );
    assert!(
        invalid_write_context[0]
            .diagnostic
            .as_ref()
            .is_some_and(|diagnostic| diagnostic.message.contains("invalid")),
        "unexpected invalid metadata diagnostic: {invalid_write_context:#?}"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        2,
        "invalid mutating provider metadata must fail closed before dispatch"
    );
}

#[test]
fn dynamic_context_plan_helper_fail_closes_mutating_and_unknown_tools() {
    assert!(plan_allows_dynamic_provider(true, "Read"));
    assert!(!plan_allows_dynamic_provider(true, "Write"));
    assert!(!plan_allows_dynamic_provider(true, "unregistered_tool"));
    assert!(plan_allows_dynamic_provider(false, "Write"));
}

/// Build a per-session-locked session cache pre-populated with one session.
fn test_session_cache(session_id: &str, session: &Session) -> bamboo_engine::SessionCache {
    let cache = Arc::new(dashmap::DashMap::new());
    cache.insert(
        session_id.to_string(),
        Arc::new(parking_lot::RwLock::new(session.clone())),
    );
    cache
}

enum DynamicProviderMode {
    Complete(String),
    NeedsHuman,
}

struct DynamicProviderExecutor {
    mode: DynamicProviderMode,
    calls: AtomicUsize,
    saw_bypass: AtomicBool,
    saw_auto: AtomicBool,
    saw_plan: AtomicBool,
}

#[async_trait::async_trait]
impl ToolExecutor for DynamicProviderExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        unreachable!("dynamic provider must use outcome-aware dispatch")
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.saw_bypass
            .store(ctx.bypass_permissions, Ordering::SeqCst);
        self.saw_auto
            .store(ctx.auto_approve_permissions, Ordering::SeqCst);
        self.saw_plan.store(ctx.plan_read_only, Ordering::SeqCst);
        Ok(match &self.mode {
            DynamicProviderMode::Complete(output) => {
                ToolOutcome::Completed(ToolResult::text(true, output.clone()))
            }
            DynamicProviderMode::NeedsHuman => ToolOutcome::NeedsHuman {
                question: bamboo_agent_core::PendingQuestion {
                    tool_call_id: call.id.clone(),
                    tool_name: call.function.name.clone(),
                    question: "Approve?".to_string(),
                    options: vec!["yes".to_string(), "no".to_string()],
                    allow_custom: false,
                    source: bamboo_agent_core::PendingQuestionSource::PauseTool,
                },
                result: ToolResult::text(false, "approval required"),
            },
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        ["Read", "Write"]
            .into_iter()
            .map(|name| ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: name.to_string(),
                    description: "dynamic context provider".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            })
            .collect()
    }
}

#[derive(Default)]
struct TestStorage {
    sessions: RwLock<HashMap<String, Session>>,
}

#[async_trait::async_trait]
impl Storage for TestStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        Ok(self.sessions.read().await.get(session_id).cloned())
    }

    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        Ok(self.sessions.write().await.remove(session_id).is_some())
    }
}

#[tokio::test]
async fn load_skill_rejects_globally_disabled_skill() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    std::fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    {
        let mut cfg = config.write().await;
        cfg.skills.disabled = vec!["demo-skill".to_string()];
        cfg.normalize_skill_settings();
    }

    let session_id = "session-1";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");

    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let tool = LoadSkillTool::new(
        skill_manager,
        config,
        bamboo_engine::SessionRepository::new(sessions, storage, persistence),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "tool-call-1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let error = tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            ctx.to_tool_ctx(),
        )
        .await
        .expect_err("disabled skill should be rejected");

    assert!(error
        .to_string()
        .contains("globally disabled in Bamboo settings"));
}

#[tokio::test]
async fn load_skill_accepts_only_runtime_advertised_skill_ids() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    let session_id = "session-runtime-allowlist";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    let mut automatic_run = session.clone();
    automatic_run.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["review"]"#.to_string(),
    );
    repo.save(&mut automatic_run)
        .await
        .expect("publish automatic runtime selection");
    let tool = LoadSkillTool::new(skill_manager, config, repo.clone());
    let context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "tool-call-runtime-allowlist",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    tool.invoke(
        serde_json::json!({ "skill_id": "review" }),
        context.to_tool_ctx(),
    )
    .await
    .expect("advertised review skill should load");

    let error = tool
        .invoke(
            serde_json::json!({ "skill_id": "plan" }),
            context.to_tool_ctx(),
        )
        .await
        .expect_err("manual-only plan must not load in an automatic review session");
    assert!(error.to_string().contains("not selected for this request"));

    let mut explicit_run = repo.load(session_id).await.expect("cached session");
    explicit_run.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["plan"]"#.to_string(),
    );
    explicit_run.metadata.insert(
        SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    repo.save(&mut explicit_run)
        .await
        .expect("publish explicit runtime selection");
    tool.invoke(
        serde_json::json!({ "skill_id": "plan" }),
        context.to_tool_ctx(),
    )
    .await
    .expect("explicitly advertised plan skill should load on the next run");
}

#[tokio::test]
async fn runtime_generation_marker_prevents_stale_metadata_from_repinning_live_catalog() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let skills_dir = temp_dir.path().join("skills");
    for (id, prompt) in [("review-demo", "review N"), ("plan-demo", "plan N")] {
        let root = skills_dir.join(id);
        std::fs::create_dir_all(root.join("references")).expect("skill root");
        std::fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {id}\ndescription: {id}\n---\n{prompt}\n"),
        )
        .expect("skill definition");
        std::fs::write(
            root.join("references/value.txt"),
            format!("{prompt} resource"),
        )
        .expect("skill resource");
    }
    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: skills_dir.clone(),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("initialize manager");
    let session_id = "runtime-pinned-generation";
    let review_ids = vec!["review-demo".to_string()];
    let descriptor = skill_manager
        .store()
        .pin_current_activation(session_id, &review_ids, None)
        .await
        .expect("pin review N");
    let mut session = Session::new(session_id, "model");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        serde_json::to_string(&review_ids).expect("review ids"),
    );
    session.metadata.insert(
        SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        descriptor.catalog_revision.to_string(),
    );
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        serde_json::to_string(&descriptor.skill_revisions).expect("review revisions"),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session).await.expect("save session");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    let tool = LoadSkillTool::new(
        skill_manager.clone(),
        Arc::new(RwLock::new(Config::default())),
        repo.clone(),
    );
    let read_tool = ReadSkillResourceTool::new(
        skill_manager.clone(),
        Arc::new(RwLock::new(Config::default())),
        repo.clone(),
    );
    let context = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "runtime-pinned-load",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    std::fs::write(
        skills_dir.join("review-demo/SKILL.md"),
        "---\nname: review-demo\ndescription: review N+1\n---\nreview N+1\n",
    )
    .expect("publish review N+1");
    std::fs::write(
        skills_dir.join("review-demo/references/value.txt"),
        "review N+1 resource",
    )
    .expect("publish resource N+1");
    skill_manager.store().reload().await.expect("reload N+1");
    let mut stale_edit = repo.load(session_id).await.expect("cached session");
    stale_edit.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["plan-demo"]"#.to_string(),
    );
    repo.save(&mut stale_edit).await.expect("save stale edit");

    let ToolOutcome::Completed(loaded) = tool
        .invoke(
            serde_json::json!({"skill_id": "review-demo"}),
            context.to_tool_ctx(),
        )
        .await
        .expect("pinned review remains authoritative")
    else {
        panic!("load_skill should complete")
    };
    let loaded = public_load_skill_receipt(&loaded);
    assert_eq!(loaded["skill_id"], "review-demo");
    assert_eq!(loaded["activation_status"], "active");
    let ToolOutcome::Completed(resource) = read_tool
        .invoke(
            serde_json::json!({
                "skill_id": "review-demo",
                "resource_path": "references/value.txt"
            }),
            context.to_tool_ctx(),
        )
        .await
        .expect("pinned resource ignores stale allowlist")
    else {
        panic!("read_skill_resource should complete")
    };
    let resource: serde_json::Value =
        serde_json::from_str(&resource.result).expect("resource result");
    assert_eq!(resource["content"], "review N resource");
    let error = tool
        .invoke(
            serde_json::json!({"skill_id": "plan-demo"}),
            context.to_tool_ctx(),
        )
        .await
        .expect_err("stale allowlist must not switch the active generation");
    assert!(error.to_string().contains("does not match"));
}

#[tokio::test]
async fn load_skill_persists_last_loaded_skill_summary() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    std::fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    let session_id = "session-2";
    let mut session = Session::new(session_id, "model");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["demo-skill"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let tool = LoadSkillTool::new(
        skill_manager,
        config,
        bamboo_engine::SessionRepository::new(
            sessions.clone(),
            storage.clone(),
            persistence.clone(),
        ),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "tool-call-2",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let _ = tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            ctx.to_tool_ctx(),
        )
        .await
        .expect("load_skill should succeed");

    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session should succeed")
        .expect("session should exist");
    let summary = saved
        .metadata
        .get(LAST_LOADED_SKILL_SUMMARY_METADATA_KEY)
        .expect("last loaded skill summary should be present");
    assert!(summary.contains("demo-skill"));
}

#[tokio::test]
async fn read_skill_resource_persists_last_resource_read_summary() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    let refs_dir = skill_dir.join("references");
    std::fs::create_dir_all(&refs_dir).expect("references dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");
    std::fs::write(refs_dir.join("policy.md"), "line1\nline2\nline3\n")
        .expect("resource file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    let session_id = "session-3";
    let mut session = Session::new(session_id, "model");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["demo-skill"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let session_repo =
        bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let load_tool = LoadSkillTool::new(skill_manager.clone(), config.clone(), session_repo.clone());
    let read_tool = ReadSkillResourceTool::new(skill_manager, config, session_repo);

    let load_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "tool-call-load",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let read_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id: "tool-call-read",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let _ = load_tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            load_ctx.to_tool_ctx(),
        )
        .await
        .expect("load_skill should succeed");

    let _ = read_tool
        .invoke(
            serde_json::json!({
                "skill_id": "demo-skill",
                "resource_path": "references/policy.md",
                "offset": 1,
                "limit": 1
            }),
            read_ctx.to_tool_ctx(),
        )
        .await
        .expect("read_skill_resource should succeed");

    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session should succeed")
        .expect("session should exist");
    let summary = saved
        .metadata
        .get(LAST_RESOURCE_READ_SUMMARY_METADATA_KEY)
        .expect("last resource read summary should be present");
    assert!(summary.contains("demo-skill"));
    assert!(summary.contains("references/policy.md"));
    assert!(summary.contains("\"offset\":1"));
}

#[tokio::test]
async fn session_workspace_skill_catalog_selection_and_runtime_roots_are_isolated() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let global_skills = temp_dir.path().join("data/skills");
    let workspace_one = temp_dir.path().join("workspace-one");
    let workspace_two = temp_dir.path().join("workspace-two");

    for (workspace, description, instructions, resource, exclusive) in [
        (
            &workspace_one,
            "alpha needle workflow",
            "Alpha workspace instructions.",
            "alpha resource",
            "only-alpha",
        ),
        (
            &workspace_two,
            "beta needle workflow",
            "Beta workspace instructions.",
            "beta resource",
            "only-beta",
        ),
    ] {
        let shared = workspace.join(".bamboo/skills/shared-workflow");
        std::fs::create_dir_all(shared.join("references")).expect("shared resource dir");
        std::fs::write(
            shared.join("SKILL.md"),
            format!(
                "---\nname: shared-workflow\ndescription: {description}\nallowed-tools:\n  - read_file\n---\n{instructions}\n"
            ),
        )
        .expect("shared skill");
        std::fs::write(shared.join("references/scope.txt"), resource).expect("shared resource");
        let exclusive_root = workspace.join(".bamboo/skills").join(exclusive);
        std::fs::create_dir_all(&exclusive_root).expect("exclusive skill dir");
        std::fs::write(
            exclusive_root.join("SKILL.md"),
            format!(
                "---\nname: {exclusive}\ndescription: {exclusive} project skill\n---\n{exclusive} instructions\n"
            ),
        )
        .expect("exclusive skill");
    }

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: global_skills,
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("initialize manager");

    let catalog_one = skill_manager
        .store_for_workspace(Some(&workspace_one))
        .await
        .expect("workspace one store")
        .skill_catalog_snapshot()
        .await;
    let catalog_two = skill_manager
        .store_for_workspace(Some(&workspace_two))
        .await
        .expect("workspace two store")
        .skill_catalog_snapshot()
        .await;
    assert_eq!(
        catalog_one
            .entries
            .iter()
            .find(|entry| entry.id == "shared-workflow")
            .expect("shared one")
            .description,
        "alpha needle workflow"
    );
    assert_eq!(
        catalog_two
            .entries
            .iter()
            .find(|entry| entry.id == "shared-workflow")
            .expect("shared two")
            .description,
        "beta needle workflow"
    );
    assert!(catalog_one
        .entries
        .iter()
        .any(|entry| entry.id == "only-alpha"));
    assert!(!catalog_one
        .entries
        .iter()
        .any(|entry| entry.id == "only-beta"));
    assert!(catalog_two
        .entries
        .iter()
        .any(|entry| entry.id == "only-beta"));
    assert!(!catalog_two
        .entries
        .iter()
        .any(|entry| entry.id == "only-alpha"));

    let disabled = std::collections::BTreeSet::new();
    let explicitly_selected = vec!["shared-workflow".to_string()];
    let selected_one = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_one,
            &disabled,
            Some(&explicitly_selected),
            None,
            None,
        )
        .await
        .expect("explicit selection one");
    let selected_two = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_two,
            &disabled,
            Some(&explicitly_selected),
            None,
            None,
        )
        .await
        .expect("explicit selection two");
    assert_eq!(selected_one[0].prompt, "Alpha workspace instructions.");
    assert_eq!(selected_two[0].prompt, "Beta workspace instructions.");

    let auto_one = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_one,
            &disabled,
            None,
            None,
            Some("alpha needle"),
        )
        .await
        .expect("auto selection one");
    let auto_two = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_two,
            &disabled,
            None,
            None,
            Some("beta needle"),
        )
        .await
        .expect("auto selection two");
    assert_eq!(
        auto_one
            .iter()
            .map(|skill| skill.id.as_str())
            .collect::<Vec<_>>(),
        vec!["shared-workflow"]
    );
    assert_eq!(
        auto_two
            .iter()
            .map(|skill| skill.id.as_str())
            .collect::<Vec<_>>(),
        vec!["shared-workflow"]
    );
    assert_eq!(
        auto_one
            .iter()
            .find(|skill| skill.id == "shared-workflow")
            .expect("auto shared one")
            .description,
        "alpha needle workflow"
    );
    assert_eq!(
        auto_two
            .iter()
            .find(|skill| skill.id == "shared-workflow")
            .expect("auto shared two")
            .description,
        "beta needle workflow"
    );

    let mut session_one = Session::new("workspace-session-one", "model");
    session_one.set_workspace_path_meta(workspace_one.to_string_lossy());
    session_one.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["shared-workflow"]"#.to_string(),
    );
    let mut session_two = Session::new("workspace-session-two", "model");
    session_two.set_workspace_path_meta(workspace_two.to_string_lossy());
    session_two.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["shared-workflow"]"#.to_string(),
    );
    let sessions = Arc::new(dashmap::DashMap::new());
    for session in [&session_one, &session_two] {
        sessions.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
    }
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session_one).await.expect("save one");
    storage.save_session(&session_two).await.expect("save two");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let config = Arc::new(RwLock::new(Config::default()));
    let load_tool = LoadSkillTool::new(skill_manager.clone(), config.clone(), repo.clone());
    let read_tool = ReadSkillResourceTool::new(skill_manager, config, repo);

    for (session_id, expected_instructions, expected_resource, expected_workspace) in [
        (
            "workspace-session-one",
            "Alpha workspace instructions.",
            "alpha resource",
            &workspace_one,
        ),
        (
            "workspace-session-two",
            "Beta workspace instructions.",
            "beta resource",
            &workspace_two,
        ),
    ] {
        let context = ToolExecutionContext {
            session_id: Some(session_id),
            root_session_id: None,
            tool_call_id: "workspace-skill-call",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        };
        let ToolOutcome::Completed(loaded) = load_tool
            .invoke(
                serde_json::json!({ "skill_id": "shared-workflow" }),
                context.to_tool_ctx(),
            )
            .await
            .expect("load workspace skill")
        else {
            panic!("load_skill should complete")
        };
        let loaded = public_load_skill_receipt(&loaded);
        assert_eq!(loaded["skill_id"], "shared-workflow");
        let durable = storage
            .load_session(session_id)
            .await
            .expect("load workspace activation")
            .expect("workspace activation remains durable")
            .metadata
            .get(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
            .and_then(|raw| {
                serde_json::from_str::<bamboo_skills::DurableWorkflowActivation>(raw).ok()
            })
            .expect("private workspace activation snapshot");
        assert_eq!(
            durable
                .snapshot
                .skills
                .get("shared-workflow")
                .expect("workspace activation entry")
                .definition
                .prompt,
            expected_instructions
        );
        let _expected_workspace =
            std::fs::canonicalize(expected_workspace).expect("canonical workspace");
        if session_id == "workspace-session-one" {
            std::fs::remove_dir_all(&workspace_one)
                .expect("delete workspace after immutable activation is loaded");
        }

        let ToolOutcome::Completed(resource) = read_tool
            .invoke(
                serde_json::json!({
                    "skill_id": "shared-workflow",
                    "resource_path": "references/scope.txt"
                }),
                context.to_tool_ctx(),
            )
            .await
            .expect("read workspace resource")
        else {
            panic!("read_skill_resource should complete")
        };
        let resource: serde_json::Value =
            serde_json::from_str(&resource.result).expect("resource result json");
        assert_eq!(resource["content"], expected_resource);
    }
}

#[tokio::test]
async fn runtime_skill_store_keeps_project_home_across_workspace_switches() {
    let directory = tempfile::tempdir().expect("tempdir");
    let global_skills = directory.path().join("global-skills");
    std::fs::create_dir_all(&global_skills).expect("global skills");
    let project_store =
        Arc::new(bamboo_projects::ProjectStore::open(directory.path()).expect("Project store"));
    let project = project_store
        .create("Runtime Skills", None)
        .expect("create Project");
    let project_skill = project_store
        .paths()
        .project_home(&project.id)
        .join("skills/project-shared");
    std::fs::create_dir_all(&project_skill).expect("Project skill");
    std::fs::write(
        project_skill.join("SKILL.md"),
        "---\nname: project-shared\ndescription: Shared Project runtime skill\n---\nPROJECT_SHARED_RUNTIME\n",
    )
    .expect("write Project skill");

    let workspace_one = directory.path().join("workspace-one");
    let workspace_two = directory.path().join("workspace-two");
    for (workspace, skill_id) in [
        (&workspace_one, "workspace-one-only"),
        (&workspace_two, "workspace-two-only"),
    ] {
        let skill = workspace.join(".bamboo/skills").join(skill_id);
        std::fs::create_dir_all(&skill).expect("workspace skill");
        std::fs::write(
            skill.join("SKILL.md"),
            format!(
                "---\nname: {skill_id}\ndescription: Workspace runtime skill\n---\n{skill_id}\n"
            ),
        )
        .expect("write workspace skill");
    }

    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: global_skills,
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize manager");
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let sessions = Arc::new(dashmap::DashMap::new());
    let repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    let mut session = Session::new("project-runtime-skill", "model");
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(workspace_one.to_string_lossy().into_owned());
    repo.save(&mut session)
        .await
        .expect("save assigned session");
    let access = SkillToolAccess::new(
        manager,
        Arc::new(RwLock::new(Config::default())),
        repo.clone(),
    )
    .with_project_store(project_store);

    let first = access
        .skill_store(Some(&session.id))
        .await
        .expect("first runtime store");
    assert!(first.get_skill("project-shared").await.is_ok());
    assert!(first.get_skill("workspace-one-only").await.is_ok());
    assert!(first.get_skill("workspace-two-only").await.is_err());

    session.set_workspace_path_meta(workspace_two.to_string_lossy().into_owned());
    repo.save(&mut session).await.expect("switch workspace");
    let second = access
        .skill_store(Some(&session.id))
        .await
        .expect("second runtime store");
    assert!(second.get_skill("project-shared").await.is_ok());
    assert!(second.get_skill("workspace-one-only").await.is_err());
    assert!(second.get_skill("workspace-two-only").await.is_ok());
}
