//! Synthetic persistence/authority cases. No provider, live account or model call.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolError, ToolOutcome};
use bamboo_agent_core::{Message, Session, Storage, Tool, ToolCtx};
use bamboo_domain::{
    PendingQuestion, PendingQuestionSource, SupervisorManagementMutation as Mutation,
    SupervisorManagementRequest, SupervisorReference,
};
use bamboo_server_tools::session_inspector::discovery::{
    AuthorizedSessionInspectRequest, AuthorizedSessionListRequest, AuthorizedSessionPage,
    SessionDiscoveryPort, SessionProgressObservation, SupervisorSessionDiscovery,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    supervisor: Session,
    reference: SupervisorReference,
}

impl Fixture {
    async fn new(ultra: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let receipt = store
            .get_or_create_default_supervisor("fixture-model")
            .await
            .unwrap();
        let reference: SupervisorReference = (&receipt).into();
        let mut supervisor = store
            .load_session(&receipt.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(ultra).unwrap();
        store.save_session(&supervisor).await.unwrap();
        let mut fixture = Self {
            home,
            store,
            supervisor,
            reference,
        };
        fixture
            .mutate(Mutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap(), "project-b".parse().unwrap()]
                    .into(),
            })
            .await;
        fixture.supervisor = fixture
            .store
            .load_execution_admission_snapshot(&fixture.supervisor.id)
            .await
            .unwrap()
            .unwrap();
        fixture
    }

    async fn mutate(&self, mutation: Mutation) {
        let scope = self
            .store
            .inspect_supervisor_scope(&self.reference)
            .await
            .unwrap();
        self.store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: self.reference.clone(),
                expected_state_revision: scope.state_revision,
                mutation,
            })
            .await
            .unwrap();
    }

    async fn root(&self, id: &str, project: &str, attached: bool) -> Session {
        let mut session = Session::new(id, "private-fixture-model");
        session.set_project_id_meta(project);
        session.title = "private title must not appear in a directory".into();
        session.add_message(Message::user(
            "private transcript must not appear in a directory",
        ));
        self.store.save_session(&session).await.unwrap();
        if attached {
            self.mutate(Mutation::Attach {
                target_session_id: id.into(),
            })
            .await;
        }
        session
    }

    fn ctx(&self) -> ToolCtx {
        let mut ctx = ToolCtx::none("discovery-fixture");
        ctx.session_id = Some(Arc::from(self.supervisor.id.as_str()));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(&self.supervisor);
        ctx.executing_caller = Some(Arc::new(bamboo_domain::SessionCallerReference::from(
            &self.supervisor,
        )));
        ctx
    }

    fn tool(&self) -> SessionInspectorTool {
        SessionInspectorTool::new(self.store.clone(), self.store.clone())
    }

    fn authorized_tool(&self) -> SessionInspectorTool {
        SessionInspectorTool::authorized(
            self.store.clone(),
            self.store.clone(),
            Arc::new(SupervisorSessionDiscovery::new(self.store.clone())),
        )
    }

    async fn invoke(&self, args: Value) -> Value {
        result(self.tool().invoke(args, self.ctx()).await.unwrap())
    }
}

fn result(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("discovery cannot pause")
    };
    assert!(result.success);
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn directory_authorizes_before_paging_and_recovers_same_cursor_after_restart() {
    for ultra in [false, true] {
        let mut fixture = Fixture::new(ultra).await;
        let first = fixture.root("aa-unattached", "project-a", false).await;
        fixture.root("bb-authorized", "project-a", true).await;
        fixture.root("cc-detached", "project-a", true).await;
        fixture
            .mutate(Mutation::Detach {
                target_session_id: "cc-detached".into(),
            })
            .await;
        let mut stale = fixture.root("dd-metadata-changed", "project-a", true).await;
        stale.metadata_version += 1;
        fixture.store.save_session(&stale).await.unwrap();
        fixture.root("ee-authorized", "project-b", true).await;
        let before =
            serde_json::to_value(fixture.store.load_session(&first.id).await.unwrap()).unwrap();
        let page = fixture.invoke(json!({"action":"list","limit":1})).await;
        assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
        assert_eq!(page["sessions"][0]["session_id"], first.id);
        assert_eq!(
            page["sessions"][0]["created_at"],
            first
                .created_at
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
        );
        let serialized = page.to_string();
        for forbidden in [
            "private title",
            "private transcript",
            "private-fixture-model",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "directory leaked {forbidden}"
            );
        }
        let cursor = page["next_cursor"].as_str().unwrap().to_owned();
        fixture.store = Arc::new(
            SessionStoreV2::new(fixture.home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let next = fixture
            .invoke(json!({"action":"list","limit":1,"cursor":cursor}))
            .await;
        assert_eq!(next["sessions"][0]["session_id"], "bb-authorized");
        let mut cursor = next["next_cursor"].as_str().map(str::to_owned);
        let mut remaining = Vec::new();
        while let Some(position) = cursor {
            let page = fixture
                .invoke(json!({"action":"list","limit":1,"cursor":position}))
                .await;
            assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
            remaining.push(
                page["sessions"][0]["session_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            cursor = page["next_cursor"].as_str().map(str::to_owned);
        }
        assert_eq!(
            remaining,
            ["cc-detached", "dd-metadata-changed", "ee-authorized"]
        );
        let filtered = fixture
            .invoke(json!({"action":"list","limit":1,"query":"PROJECT-B"}))
            .await;
        assert_eq!(filtered["sessions"][0]["session_id"], "ee-authorized");
        assert_eq!(
            serde_json::to_value(fixture.store.load_session(&first.id).await.unwrap()).unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn directory_cursor_rejects_changed_query_limit_scope_and_replaced_relationship() {
    let fixture = Fixture::new(false).await;
    fixture.root("first", "project-a", true).await;
    fixture.root("second", "project-a", true).await;
    let page = fixture.invoke(json!({"action":"list","limit":1})).await;
    let cursor = page["next_cursor"].as_str().unwrap();
    for request in [
        json!({"action":"list","limit":1,"query":"first","cursor":cursor}),
        json!({"action":"list","limit":2,"cursor":cursor}),
    ] {
        let error = fixture
            .tool()
            .invoke(request, fixture.ctx())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("session_discovery_cursor_stale"));
    }
    fixture
        .mutate(Mutation::Detach {
            target_session_id: "second".into(),
        })
        .await;
    fixture
        .mutate(Mutation::Attach {
            target_session_id: "second".into(),
        })
        .await;
    let error = fixture
        .tool()
        .invoke(
            json!({"action":"list","limit":1,"cursor":cursor}),
            fixture.ctx(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("session_discovery_cursor_stale"));
    let fresh = fixture.invoke(json!({"action":"list","limit":1})).await;
    assert!(fresh["scope_revision"].as_u64().unwrap() > page["scope_revision"].as_u64().unwrap());
}

#[tokio::test]
async fn discovery_denies_missing_forged_and_stale_caller_lifetimes() {
    let fixture = Fixture::new(false).await;
    fixture.root("managed", "project-a", true).await;
    let tool = fixture.authorized_tool();
    let mut missing = fixture.ctx();
    missing.executing_supervisor = None;
    assert!(tool
        .invoke(json!({"action":"list"}), missing)
        .await
        .is_err());
    let mut ordinary = fixture.root("ordinary", "project-a", false).await;
    ordinary.metadata.insert("role".into(), "supervisor".into());
    fixture.store.save_session(&ordinary).await.unwrap();
    let child = Session::new_child_of("child", &ordinary, "model", "child");
    fixture.store.save_session(&child).await.unwrap();
    for id in [&ordinary.id, &child.id] {
        let mut forged = fixture.ctx();
        forged.session_id = Some(Arc::from(id.as_str()));
        let error = tool
            .invoke(json!({"action":"list"}), forged)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("session_discovery_authority_required"));
    }
    fixture
        .store
        .delete_session(&fixture.supervisor.id)
        .await
        .unwrap();
    fixture
        .store
        .get_or_create_default_supervisor("replacement")
        .await
        .unwrap();
    assert!(tool
        .invoke(json!({"action":"list"}), fixture.ctx())
        .await
        .is_err());
}

#[tokio::test]
async fn directory_observes_current_birth_project_and_scope_without_legacy_acl_filtering() {
    for change in ["recreated", "project", "scope"] {
        let fixture = Fixture::new(true).await;
        let mut target = fixture.root("managed", "project-a", true).await;
        let old_selector = fixture
            .store
            .inspect_supervisor_current_user_target(&fixture.reference, &target.id)
            .await
            .unwrap();
        match change {
            "recreated" => {
                fixture.store.delete_session(&target.id).await.unwrap();
                fixture
                    .store
                    .recreate_root_session(&target.id, "replacement")
                    .await
                    .unwrap();
            }
            "project" => {
                target.set_project_id_meta("project-b");
                target.metadata_version += 1;
                fixture.store.save_session(&target).await.unwrap();
            }
            "scope" => {
                fixture
                    .mutate(Mutation::ConfigureProjectScope {
                        allowed_projects: Default::default(),
                    })
                    .await
            }
            _ => unreachable!(),
        }
        let page = fixture.invoke(json!({"action":"list"})).await;
        assert_eq!(page["sessions"].as_array().unwrap().len(), 1, "{change}");
        assert_eq!(page["sessions"][0]["session_id"], target.id);
        let fresh = fixture
            .store
            .inspect_supervisor_current_user_target(&fixture.reference, &target.id)
            .await
            .unwrap();
        if change == "scope" {
            assert_eq!(
                fresh, old_selector,
                "legacy ACL change does not recreate a target"
            );
        } else {
            assert_ne!(
                fresh, old_selector,
                "old birth/metadata must not describe the replacement"
            );
        }
        assert!(
            fixture
                .tool()
                .invoke(
                    json!({"action":"inspect","session_id":target.id}),
                    fixture.ctx()
                )
                .await
                .is_ok(),
            "{change}"
        );
    }
}

#[tokio::test]
async fn corrupted_caller_or_target_authority_is_an_error_without_history_writes() {
    for caller in [false, true] {
        let fixture = Fixture::new(false).await;
        let target = fixture.root("managed", "project-a", true).await;
        let id = if caller {
            &fixture.supervisor.id
        } else {
            &target.id
        };
        let path = fixture
            .store
            .sessions_root_dir()
            .join(id)
            .join("runtime.json");
        tokio::fs::write(&path, b"invalid authority fixture")
            .await
            .unwrap();
        let history = fixture
            .store
            .sessions_root_dir()
            .join(&target.id)
            .join("session.json");
        let before = tokio::fs::read(&history).await.unwrap();
        let error = fixture
            .tool()
            .invoke(json!({"action":"list"}), fixture.ctx())
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("session_discovery_authority_unavailable"));
        assert_eq!(tokio::fs::read(&history).await.unwrap(), before);
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"invalid authority fixture"
        );
    }
}

#[tokio::test]
async fn inspect_bounds_visible_excerpt_and_does_not_claim_input_or_live_completion() {
    let fixture = Fixture::new(false).await;
    let mut target = fixture.root("managed", "project-a", true).await;
    let mut visible = Message::assistant("结果🎋".repeat(2_000), None);
    visible.reasoning = Some("private hidden reasoning must not be exported".into());
    let message_id = visible.id.clone();
    target.add_message(visible);
    let mut hidden = Message::assistant("hidden runtime assistant text must not be exported", None);
    hidden.metadata = Some(json!({"hidden_from_ui":true}));
    target.add_message(hidden);
    target.set_last_run_status("completed");
    target.pending_question = Some(PendingQuestion {
        tool_call_id: "pending-approval".into(),
        tool_name: "Bash".into(),
        question: "private approval prompt".into(),
        options: vec![],
        allow_custom: false,
        source: PendingQuestionSource::PauseTool,
    });
    fixture.store.save_session(&target).await.unwrap();
    let before =
        serde_json::to_value(fixture.store.load_session(&target.id).await.unwrap()).unwrap();
    let view = fixture
        .invoke(json!({"action":"inspect","session_id":target.id,"max_chars":17}))
        .await;
    assert_eq!(
        view["latest_visible_assistant_excerpt"]["content"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        17
    );
    assert_eq!(
        view["latest_visible_assistant_excerpt"]["message_id"],
        message_id
    );
    assert_eq!(view["latest_visible_assistant_excerpt"]["truncated"], true);
    assert_eq!(view["last_persisted_status"], "completed");
    assert_eq!(view["has_pending_question"], true);
    for flag in ["status_is_live", "input_specific", "completion_verified"] {
        assert_eq!(view[flag], false);
    }
    assert!(!view.to_string().contains("hidden reasoning"));
    assert!(!view.to_string().contains("hidden runtime assistant text"));
    assert!(!view.to_string().contains("private approval prompt"));
    assert!(view.get("run_id").is_none());
    assert_eq!(
        serde_json::to_value(fixture.store.load_session(&target.id).await.unwrap()).unwrap(),
        before
    );
    target.set_last_run_status("injected raw status text");
    fixture.store.save_session(&target).await.unwrap();
    assert_eq!(
        fixture
            .invoke(json!({"action":"inspect","session_id":target.id}))
            .await["last_persisted_status"],
        "unknown"
    );
}

#[tokio::test]
async fn authorized_surface_schema_bounds_and_action_fields_match_execution() {
    let fixture = Fixture::new(false).await;
    fixture.root("managed", "project-a", true).await;
    let tool = fixture.authorized_tool();
    let schema = tool.parameters_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for valid in [
        json!({"action":"list","limit":1}),
        json!({"action":"inspect","session_id":"managed","max_chars":1}),
    ] {
        assert!(validator.is_valid(&valid));
        assert!(tool.invoke(valid, fixture.ctx()).await.is_ok());
    }
    for invalid in [
        json!({"action":"list","limit":0}),
        json!({"action":"list","limit":21}),
        json!({"action":"list","offset":0}),
        json!({"action":"list","session_id":"managed"}),
        json!({"action":"list","query":"a".repeat(129)}),
        json!({"action":"list","cursor":"a".repeat(4097)}),
        json!({"action":"inspect"}),
        json!({"action":"inspect","session_id":"managed","max_chars":0}),
        json!({"action":"inspect","session_id":"managed","max_chars":8001}),
        json!({"action":"inspect","session_id":"managed","query":"ignored"}),
        json!({"action":"read_messages","session_id":"managed"}),
    ] {
        assert!(!validator.is_valid(&invalid), "{invalid}");
        assert!(
            tool.invoke(invalid.clone(), fixture.ctx()).await.is_err(),
            "{invalid}"
        );
    }
    for invalid in [
        json!({"action":"list","cursor":"{}"}),
        json!({"action":"inspect","session_id":"../escape"}),
    ] {
        assert!(tool.invoke(invalid, fixture.ctx()).await.is_err());
    }
}

#[tokio::test]
async fn authorized_discovery_fields_survive_actual_provider_schema_normalization() {
    let fixture = Fixture::new(false).await;
    let schema = fixture.authorized_tool().parameters_schema();
    let normalized =
        bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema(
            &schema,
        );
    assert_eq!(normalized["type"], "object");
    assert_eq!(normalized["properties"], schema["properties"]);
    assert_eq!(normalized["required"], json!(["action"]));
    assert_eq!(
        normalized["properties"]["action"]["enum"],
        json!(["list", "inspect"])
    );
    for field in [
        "action",
        "query",
        "limit",
        "cursor",
        "session_id",
        "max_chars",
    ] {
        assert!(
            normalized["properties"].get(field).is_some(),
            "lost provider field: {field}"
        );
    }
    assert_eq!(normalized["properties"]["limit"]["maximum"], 20);
    assert_eq!(normalized["properties"]["max_chars"]["maximum"], 8000);
    assert!(normalized.get("oneOf").is_none());
}

struct DeniedHostAdapter;

#[async_trait]
impl SessionDiscoveryPort for DeniedHostAdapter {
    async fn list(
        &self,
        _ctx: &ToolCtx,
        _request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError> {
        Err(ToolError::Execution("fixture Host grant denied".into()))
    }
    async fn inspect(
        &self,
        _ctx: &ToolCtx,
        _request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError> {
        Err(ToolError::Execution("fixture Host grant denied".into()))
    }
}

#[tokio::test]
async fn ordinary_caller_uses_installed_host_adapter_without_global_index_fallback() {
    let fixture = Fixture::new(false).await;
    let ordinary = fixture.root("ordinary", "project-a", false).await;
    fixture.root("unrelated-root", "project-b", false).await;
    let tool = SessionInspectorTool::authorized(
        fixture.store.clone(),
        fixture.store.clone(),
        Arc::new(DeniedHostAdapter),
    );
    let mut ctx = ToolCtx::none("ordinary-discovery-fixture");
    ctx.session_id = Some(Arc::from(ordinary.id.as_str()));
    let error = tool
        .invoke(json!({"action":"list"}), ctx)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fixture Host grant denied"));
    assert!(!error.to_string().contains("unrelated-root"));
}
