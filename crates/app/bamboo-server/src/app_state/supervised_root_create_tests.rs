//! Host-wired model entry: an independent Root is not a SubAgent child.

use std::sync::Arc;

use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, FunctionCall, ToolCall, ToolExecutionContext, ToolOutcome,
};
use bamboo_domain::{
    RootModeOperationAction, RootModeOperationRequest, SessionKind, SupervisorReference,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_tools::permission::{PermissionRule, PermissionType};
use serde_json::{json, Value};

use super::AppState;

struct Fixture {
    home: tempfile::TempDir,
    state: AppState,
    supervisor: SupervisorReference,
    project_id: bamboo_domain::ProjectId,
    workspace: String,
}

impl Fixture {
    async fn new(ultra: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let state = Box::pin(AppState::new_with_provider(
            home.path().to_path_buf(),
            bamboo_llm::Config::default(),
            Arc::new(super::UnconfiguredProvider {
                message: "no external model is available in this fixture".into(),
            }),
        ))
        .await
        .unwrap();
        let workspace = home.path().join("workspaces").join("explicit-worker");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace.to_string_lossy().into_owned();
        let project = state
            .project_store
            .create_with_project_path("Explicit worker Project", None, &workspace, Vec::new())
            .unwrap();
        let service = SupervisorSessionService::new(state.storage.clone());
        let original = service
            .get_or_create_default("supervisor-private-model")
            .await
            .unwrap();
        let supervisor = SupervisorReference::from(&original);
        service
            .configure_project_scope(&supervisor, 0, [project.id.clone()].into())
            .await
            .unwrap();
        let original = state
            .storage
            .load_root_authority(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        state
            .storage
            .root_mode_operation(&RootModeOperationRequest {
                session_id: original.id.clone(),
                operation_id: format!(
                    "{}:{}",
                    original.root_mode_transition_epoch,
                    uuid::Uuid::new_v4()
                ),
                birth_token: original.root_mode_birth_token(),
                expected_epoch: original.root_mode_transition_epoch,
                requested_enabled: ultra,
                action: RootModeOperationAction::Select,
            })
            .await
            .unwrap();
        let original = state
            .storage
            .load_root_authority(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.root_orchestration_only_enabled(), ultra);
        // A real native permission rule for the fixture operation. No bypass,
        // auto-approve, identity override, or permissions on a worker are used.
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .add_rule(PermissionRule::new(
                PermissionType::ExecuteCommand,
                "session_control create*",
                true,
            ));
        Self {
            home,
            state,
            supervisor,
            project_id: project.id,
            workspace,
        }
    }

    fn request(&self, operation_id: uuid::Uuid) -> Value {
        json!({"action":"create", "request": {
            "operation_id": operation_id,
            "expected_state_revision": 1,
            "project_id": self.project_id,
            "workspace_path": self.workspace,
            "title": "Independent implementation task",
            "message": "Inspect the explicitly assigned workspace and report evidence."
        }})
    }

    async fn invoke(&self, args: Value) -> Value {
        let executing = self
            .state
            .storage
            .load_root_authority(&self.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let call = ToolCall {
            id: "supervised-root-create".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_control".into(),
                arguments: args.to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&executing.id);
        ctx.root_session_id = Some(&executing.id);
        let ctx = ctx.with_executing_supervisor(
            ExecutingSupervisorObservation::capture_from_executing_session(&executing),
        );
        let result = self
            .state
            .tools_for(crate::tools::ToolSurface::Root)
            .execute_with_context_outcome(&call, ctx)
            .await
            .expect("the Host must expose the independent Root creation operation");
        let ToolOutcome::Completed(result) = result else {
            panic!("expected bounded creation/delivery receipt");
        };
        assert!(result.success, "{}", result.result);
        serde_json::from_str(&result.result).unwrap()
    }
}

async fn assert_create_is_independent_and_exact_retry(ultra: bool) {
    let f = Fixture::new(ultra).await;
    let before = f
        .state
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = uuid::Uuid::new_v4();
    let response = f.invoke(f.request(operation_id)).await;
    assert_eq!(response["action"], "create");
    assert_eq!(response["admitted"], true);
    let id = response["target_session_id"].as_str().unwrap();
    let root = f.state.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(root.kind, SessionKind::Root);
    assert_eq!(root.root_session_id, id);
    assert!(root.parent_session_id.is_none());
    assert!(root.authority_identity.is_ordinary());
    assert_eq!(
        root.project_id_meta().as_deref(),
        Some(f.project_id.as_str())
    );
    assert_eq!(
        root.workspace_path_meta().as_deref(),
        Some(f.workspace.as_str())
    );
    assert_eq!(root.supervisor_incarnation_id(), None);
    assert!(!root.root_orchestration_only_enabled());
    assert_ne!(root.model, "supervisor-private-model");
    let link = f
        .state
        .storage
        .inspect_supervisor_link(&f.supervisor, id)
        .await
        .unwrap();
    assert!(link.authorized);
    assert_eq!(link.link.unwrap().revision, 1);
    let replay = f.invoke(f.request(operation_id)).await;
    assert_eq!(replay["target_session_id"], id);
    assert_eq!(replay["receipt_id"], response["receipt_id"]);
    assert_eq!(replay["generation"], response["generation"]);
    assert_eq!(
        f.state.session_inbox.inspect(id).await.unwrap().generation,
        1
    );
    let after = f
        .state
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.authority_identity, before.authority_identity);
    assert_eq!(
        serde_json::to_value(&after.messages).unwrap(),
        serde_json::to_value(&before.messages).unwrap()
    );
    assert_eq!(after.model, before.model);
    assert_eq!(after.root_orchestration_only_enabled(), ultra);
    assert!(f.home.path().is_dir());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_standard_creates_independent_root_and_retries_once() {
    assert_create_is_independent_and_exact_retry(false).await;
}

#[actix_web::test]
async fn supervisor_ultra_creates_independent_root_and_retries_once() {
    assert_create_is_independent_and_exact_retry(true).await;
}
