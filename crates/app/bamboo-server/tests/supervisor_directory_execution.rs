//! Regression for #2450: production Root surface -> real permission checker ->
//! SessionControlTool -> Host directory. Only the model is a scripted fixture.

use bamboo_agent_core::tools::ExecutingSupervisorObservation;
use bamboo_agent_core::tools::{
    with_executing_session_caller, FunctionCall, ToolCall, ToolError, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolOutcome,
};
use bamboo_domain::{Message, SessionCallerReference};
use bamboo_domain::{Session, SessionAuthorityIdentity, Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_server::{tools::ToolSurface, AppState};
use bamboo_tools::permission::{PermissionConfig, PermissionRule, PermissionType};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Default)]
struct DirectoryModel(Mutex<Option<ToolCall>>);

#[async_trait::async_trait]
impl LLMProvider for DirectoryModel {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let chunk = match self.0.lock().unwrap().take() {
            Some(call) => {
                let schema = &tools
                    .iter()
                    .find(|tool| tool.function.name == "session_control")
                    .expect("directory advertised in this thinking mode")
                    .function
                    .parameters;
                let directory = schema["oneOf"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|variant| variant["properties"]["action"]["const"] == "directory")
                    .expect("production directory variant");
                for field in ["query", "limit", "cursor"] {
                    assert!(directory["properties"][field].is_object());
                }
                LLMChunk::ToolCalls(vec![call])
            }
            None => LLMChunk::Token("Directory observed.".into()),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct Assembly {
    home: tempfile::TempDir,
    state: AppState,
    model: Arc<DirectoryModel>,
}

impl Assembly {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        // Persist an enabled policy before assembly, so the real built-in gate
        // cannot disappear via the legacy no-config behavior. Execution stays
        // explicitly denied; directory must need no execution grant.
        let policy = PermissionConfig::new();
        policy.add_rule(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "*",
            false,
        ));
        bamboo_tools::permission::storage::PermissionStorage::new(home.path())
            .save(&policy)
            .await
            .unwrap();
        let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        let model = Arc::new(DirectoryModel::default());
        let state = AppState::new_with_provider(home.path().to_path_buf(), config, model.clone())
            .await
            .unwrap();
        state
            .session_store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        Self { home, state, model }
    }

    async fn supervisor(&self) -> Session {
        self.state
            .storage
            .load_execution_admission_snapshot(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap()
    }

    async fn call(&self, caller: &Session, args: Value) -> Result<Value, ToolError> {
        let call = tool_call(args);
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let tools = self.state.tools_for(ToolSurface::Root);
        let schemas = tools.list_tools();
        if args_is_directory(&call) {
            assert_eq!(
                tools.call_parallel_classification(&call),
                (bamboo_agent_core::tools::ToolMutability::ReadOnly, true)
            );
        }
        let ctx = ToolExecutionContext::for_dispatch(
            &caller.id,
            &caller.root_session_id,
            &call.id,
            &tx,
            &schemas,
            ToolExecutionSessionFlags::from_session(caller),
            false,
            None,
            None,
        )
        .with_executing_supervisor(
            ExecutingSupervisorObservation::capture_from_executing_session(caller),
        );
        assert!(!ctx.bypass_permissions && !ctx.auto_approve_permissions);
        let outcome = with_executing_session_caller(
            Some(Arc::new(SessionCallerReference::from(caller))),
            tools.execute_with_context_outcome(&call, ctx),
        )
        .await?;
        let ToolOutcome::Completed(result) = outcome else {
            panic!("directory must complete synchronously without approval or suspension")
        };
        assert!(result.success, "{}", result.result);
        Ok(serde_json::from_str(&result.result).unwrap())
    }

    async fn model_call(&self, supervisor: &mut Session, args: Value) -> Value {
        *self.model.0.lock().unwrap() = Some(tool_call(args));
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        self.state
            .agent
            .execute_direct(
                supervisor,
                bamboo_engine::ExecuteRequestBuilder::new(
                    "Read the session directory once",
                    tx,
                    tokio_util::sync::CancellationToken::new(),
                )
                .tools(self.state.tools_for(ToolSurface::Root))
                .provider_override(self.model.clone())
                .model("model")
                .build(),
            )
            .await
            .unwrap();
        drain.await.unwrap();
        let result = supervisor
            .messages
            .iter()
            .rev()
            .find(|message| message.tool_call_id.as_deref() == Some("directory-assembly-call"))
            .expect("native loop retained the tool result");
        // On the original source this assertion prints the reported production
        // failure: Permission check failed: unsupported session_control action.
        assert!(
            !result
                .content
                .contains("unsupported session_control action"),
            "{}",
            result.content
        );
        serde_json::from_str(&result.content).expect("directory page, not a permission rejection")
    }
}

fn tool_call(args: Value) -> ToolCall {
    ToolCall {
        id: "directory-assembly-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: args.to_string(),
        },
    }
}

fn args_is_directory(call: &ToolCall) -> bool {
    serde_json::from_str::<Value>(&call.function.arguments).unwrap()["action"] == "directory"
}

fn ids(page: &Value) -> Vec<&str> {
    page["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["session_id"].as_str().unwrap())
        .collect()
}

#[actix_web::test]
async fn directory_assembled_standard_and_ultra_preserve_read_only_pagination() {
    let f = Box::pin(Assembly::new()).await;
    let a = root(&f.state.session_store, "directory-a", "project-a").await;
    root(&f.state.session_store, "directory-b", "project-b").await;
    root(&f.state.session_store, "unrelated-root", "project-a").await;
    let child = Session::new_child_of("directory-child", &a, "model", "child");
    f.state.storage.save_session(&child).await.unwrap();
    f.state
        .storage
        .save_session(&Session::new("directory-projectless", "model"))
        .await
        .unwrap();
    let targets = [
        "directory-a",
        "directory-b",
        "unrelated-root",
        "directory-child",
    ];
    let mut before = Vec::new();
    for id in targets {
        before.push(
            serde_json::to_value(f.state.storage.load_session(id).await.unwrap().unwrap()).unwrap(),
        );
    }
    let permissions = serde_json::to_value(
        f.state
            .permission_checker
            .permission_config()
            .unwrap()
            .to_serializable(),
    )
    .unwrap();
    let management = serde_json::to_value(f.supervisor().await.supervisor_management).unwrap();
    let mut sessions_before: Vec<_> = f
        .state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    sessions_before.sort();
    for ultra in [false, true] {
        let mut supervisor = f.supervisor().await;
        supervisor.set_root_orchestration_only(ultra).unwrap();
        let workspace = f.home.path().join("supervisor-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        supervisor.set_workspace_path_meta(workspace.to_string_lossy());
        f.state.storage.save_session(&supervisor).await.unwrap();
        let first = f
            .model_call(
                &mut supervisor,
                json!({"action":"directory", "query":" DIRECTORY- ", "limit":1}),
            )
            .await;
        assert_eq!(ids(&first), ["directory-a"]);
        assert_eq!(first["caller_session_id"], DEFAULT_SUPERVISOR_SESSION_ID);
        let cursor = first["next_cursor"].as_str().unwrap();
        let second = f
            .call(
                &supervisor,
                json!({"action":"directory", "query":"directory-", "limit":1, "cursor":cursor}),
            )
            .await
            .unwrap();
        assert_eq!(ids(&second), ["directory-b"]);
        assert!(second["next_cursor"].is_null());
        assert_eq!(second["entries"][0]["linked"], false);
        let all = f
            .call(&supervisor, json!({"action":"directory"}))
            .await
            .unwrap();
        assert_eq!(ids(&all), ["directory-a", "directory-b", "unrelated-root"]);
        // Current-user authority spans Projects; children, the Supervisor and
        // projectless rows remain outside this existing observation surface.
        for changed in [
            json!({"query":"unrelated", "limit":1}),
            json!({"query":"directory-", "limit":2}),
        ] {
            let mut args = changed;
            args["action"] = json!("directory");
            args["cursor"] = json!(cursor);
            assert!(f
                .call(&supervisor, args)
                .await
                .unwrap_err()
                .to_string()
                .contains("cursor_stale"));
        }
        for args in [
            json!({"action":"directory","limit":0}),
            json!({"action":"directory","limit":21}),
            json!({"action":"directory","query":"q".repeat(129)}),
            json!({"action":"directory","cursor":"invalid"}),
        ] {
            assert!(f
                .call(&supervisor, args)
                .await
                .unwrap_err()
                .to_string()
                .contains("session_directory_invalid_request"));
        }
        // Reading an unlinked row neither changes nor bypasses the execution
        // policy for existing create/followup/cancel operations.
        for args in [
            json!({"action":"create","request":{"project_id":"project-a","workspace_path":"/explicit"}}),
            json!({"action":"followup","target_session_id":"directory-a"}),
            json!({"action":"cancel","target_session_id":"directory-a","expected_run_id":"run-directory-guard"}),
        ] {
            let error = f.call(&supervisor, args).await.unwrap_err().to_string();
            assert!(error.contains("Permission denied"), "{error}");
        }
        assert!(f.state.agent_runners.read().await.is_empty());
        assert_eq!(
            serde_json::to_value(f.supervisor().await.supervisor_management).unwrap(),
            management
        );
        eprintln!(
            "directory assembly mode={:?}: native permission/executor/Host PASS",
            supervisor.root_thinking_mode()
        );
    }
    for (id, original) in targets.into_iter().zip(before) {
        assert_eq!(
            serde_json::to_value(f.state.storage.load_session(id).await.unwrap().unwrap()).unwrap(),
            original
        );
    }
    assert_eq!(
        serde_json::to_value(
            f.state
                .permission_checker
                .permission_config()
                .unwrap()
                .to_serializable()
        )
        .unwrap(),
        permissions
    );
    let mut sessions_after: Vec<_> = f
        .state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    sessions_after.sort();
    assert_eq!(
        sessions_after, sessions_before,
        "directory must not create Sessions"
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn directory_assembled_rejects_ordinary_forged_and_expired_supervisors() {
    let f = Box::pin(Assembly::new()).await;
    let original = f.supervisor().await;
    let ordinary = root(
        &f.state.session_store,
        "ordinary-directory-caller",
        "project-a",
    )
    .await;
    let mut forged = ordinary.clone();
    forged.authority_identity = original.authority_identity.clone();
    let mut wrong_incarnation = original.clone();
    wrong_incarnation.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    for caller in [&ordinary, &forged, &wrong_incarnation] {
        let error = f
            .call(caller, json!({"action":"directory"}))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            !error.contains("unsupported session_control action"),
            "{error}"
        );
        assert!(
            error.contains("original executing Supervisor identity")
                || error.contains("session_directory_authority_required"),
            "{error}"
        );
    }
    f.state
        .storage
        .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
    f.state
        .session_store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    assert_ne!(
        original.supervisor_incarnation_id(),
        f.supervisor().await.supervisor_incarnation_id()
    );
    let error = f
        .call(&original, json!({"action":"directory"}))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("session_directory_authority_required"),
        "{error}"
    );
    assert!(f
        .call(&f.supervisor().await, json!({"action":"directory"}))
        .await
        .is_ok());
    f.state.shutdown().await;
}

async fn root(store: &bamboo_storage::SessionStoreV2, id: &str, project: &str) -> Session {
    let mut session = Session::new(id, "model");
    session.set_project_id_meta(project);
    session.set_workspace_path_meta(format!("/workspace/{id}"));
    store.save_session(&session).await.unwrap();
    session
}
