use bamboo_agent_core::tools::ExecutingSupervisorObservation;
use bamboo_agent_core::{Message, Session, Storage, Tool, ToolCtx};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn supervisor_history_refuses_unattached_independent_root() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let receipt = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let supervisor = store
        .load_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let mut target = Session::new("unattached-root", "model");
    target.set_project_id_meta("outside-project");
    target.add_message(Message::user("must not escape explicit management scope"));
    store.save_session(&target).await.unwrap();
    let tool = SessionInspectorTool::new(store.clone(), store.clone());
    let mut ctx = ToolCtx::none("scoped-read");
    ctx.session_id = Some(Arc::from(supervisor.id.as_str()));
    ctx.executing_supervisor =
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
    let result = tool
        .invoke(
            json!({"action":"read_messages", "session_id":target.id,"limit":2}),
            ctx,
        )
        .await;
    assert!(
        result.is_err(),
        "Supervisor read escaped its explicit attach scope"
    );
}

#[tokio::test]
async fn supervisor_history_scoped_readback_survives_modes_and_restart_and_denies_invalid_links() {
    use bamboo_domain::{
        SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
    };
    for ultra in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let receipt = store
            .get_or_create_default_supervisor("model")
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
        let mut target = Session::new("managed-root", "model");
        target.set_project_id_meta("project-a");
        target.add_message(Message::user("authorized history before restart"));
        target.add_message(Message::assistant("bounded result", None));
        store.save_session(&target).await.unwrap();
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 0,
                mutation: SupervisorManagementMutation::ConfigureProjectScope {
                    allowed_projects: ["project-a".parse().unwrap()].into(),
                },
            })
            .await
            .unwrap();
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 1,
                mutation: SupervisorManagementMutation::Attach {
                    target_session_id: target.id.clone(),
                },
            })
            .await
            .unwrap();
        drop(store);
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let tool = SessionInspectorTool::new(store.clone(), store.clone());
        let context = || {
            let mut ctx = ToolCtx::none("scoped-history");
            ctx.session_id = Some(Arc::from(supervisor.id.as_str()));
            ctx.executing_supervisor =
                ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
            ctx
        };
        let args =
            json!({"action":"read_messages","session_id":target.id,"limit":1,"truncate_chars":7});
        let result = tool.invoke(args.clone(), context()).await.unwrap();
        let bamboo_agent_core::ToolOutcome::Completed(result) = result else {
            panic!("readback")
        };
        let result: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(result["slice_count"], 1);
        // Existing viewer convention keeps the requested prefix and appends "...".
        assert_eq!(result["messages"][0]["content"], "bounded...");
        let metadata = tool
            .invoke(
                json!({"action":"get_meta","session_id":target.id}),
                context(),
            )
            .await
            .unwrap();
        let bamboo_agent_core::ToolOutcome::Completed(metadata) = metadata else {
            panic!("metadata")
        };
        let metadata: serde_json::Value = serde_json::from_str(&metadata.result).unwrap();
        assert_eq!(metadata["session"]["id"], target.id);
        assert_eq!(metadata["session"]["project_id"], "project-a");
        assert!(tool
            .invoke(
                json!({"action":"read_messages","session_id":supervisor.id,"limit":1}),
                context()
            )
            .await
            .is_ok());
        assert!(tool
            .invoke(json!({"action":"list"}), context())
            .await
            .is_err());
        assert!(tool
            .invoke(json!({"action":"search","query":"history"}), context())
            .await
            .is_err());
        let before = serde_json::to_value(store.load_session(&target.id).await.unwrap()).unwrap();
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 2,
                mutation: SupervisorManagementMutation::Detach {
                    target_session_id: target.id.clone(),
                },
            })
            .await
            .unwrap();
        assert!(tool.invoke(args.clone(), context()).await.is_err());
        assert_eq!(
            serde_json::to_value(store.load_session(&target.id).await.unwrap()).unwrap(),
            before
        );
        let mut missing = ToolCtx::none("missing-observation");
        missing.session_id = Some(Arc::from(supervisor.id.as_str()));
        assert!(tool.invoke(args.clone(), missing).await.is_err());
        store.delete_session(&reference.session_id).await.unwrap();
        store
            .get_or_create_default_supervisor("new-model")
            .await
            .unwrap();
        assert!(tool.invoke(args, context()).await.is_err());
    }
}

#[tokio::test]
async fn supervisor_history_denies_changed_target_scope_and_corrupt_authority_without_writes() {
    use bamboo_domain::{
        SupervisorManagementMutation as Mutation, SupervisorManagementRequest, SupervisorReference,
    };
    for scenario in [
        "project",
        "metadata",
        "recreated",
        "scope",
        "target-proof",
        "supervisor-proof",
        "caller",
    ] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let receipt = store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let reference: SupervisorReference = (&receipt).into();
        let supervisor = store
            .load_session(&receipt.session_id)
            .await
            .unwrap()
            .unwrap();
        let mut target = Session::new("managed-root", "model");
        target.set_project_id_meta("project-a");
        target.add_message(Message::user("explicitly attached history"));
        store.save_session(&target).await.unwrap();
        for (revision, mutation) in [
            Mutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap()].into(),
            },
            Mutation::Attach {
                target_session_id: target.id.clone(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            store
                .mutate_supervisor_management(&SupervisorManagementRequest {
                    supervisor: reference.clone(),
                    expected_state_revision: revision as u64,
                    mutation,
                })
                .await
                .unwrap();
        }
        match scenario {
            "project" => {
                target.set_project_id_meta("project-b");
                target.metadata_version += 1;
                store.save_session(&target).await.unwrap();
            }
            "metadata" => {
                target.metadata_version += 1;
                store.save_session(&target).await.unwrap();
            }
            "recreated" => {
                store.delete_session(&target.id).await.unwrap();
                store
                    .recreate_root_session(&target.id, "replacement")
                    .await
                    .unwrap();
            }
            "scope" => {
                store
                    .mutate_supervisor_management(&SupervisorManagementRequest {
                        supervisor: reference.clone(),
                        expected_state_revision: 2,
                        mutation: Mutation::ConfigureProjectScope {
                            allowed_projects: Default::default(),
                        },
                    })
                    .await
                    .unwrap();
            }
            "target-proof" | "supervisor-proof" => {
                let id = if scenario == "target-proof" {
                    &target.id
                } else {
                    &supervisor.id
                };
                tokio::fs::write(
                    store.sessions_root_dir().join(id).join("runtime.json"),
                    b"invalid authority",
                )
                .await
                .unwrap();
            }
            "caller" => {}
            _ => unreachable!(),
        }
        let target_path = store.sessions_root_dir().join(&target.id);
        let before = (
            tokio::fs::read(target_path.join("session.json"))
                .await
                .unwrap(),
            tokio::fs::read(target_path.join("runtime.json"))
                .await
                .unwrap(),
        );
        let tool = SessionInspectorTool::new(store.clone(), store.clone());
        let mut ctx = ToolCtx::none("denied-read");
        ctx.session_id = Some(Arc::from(if scenario == "caller" {
            "ordinary-root"
        } else {
            supervisor.id.as_str()
        }));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
        assert!(
            tool.invoke(
                json!({"action":"read_messages","session_id":target.id,"limit":1}),
                ctx
            )
            .await
            .is_err(),
            "{scenario}"
        );
        assert_eq!(
            before,
            (
                tokio::fs::read(target_path.join("session.json"))
                    .await
                    .unwrap(),
                tokio::fs::read(target_path.join("runtime.json"))
                    .await
                    .unwrap(),
            ),
            "denied read changed target files: {scenario}"
        );
    }
}

#[tokio::test]
async fn ordinary_standard_history_viewer_preserves_existing_read_surface() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut target = Session::new("ordinary-target", "model");
    target.add_message(Message::user("existing Standard viewer"));
    store.save_session(&target).await.unwrap();
    let tool = SessionInspectorTool::new(store.clone(), store.clone());
    let mut ctx = ToolCtx::none("ordinary-read");
    ctx.session_id = Some(Arc::from("ordinary-root"));
    assert!(tool
        .invoke(
            json!({"action":"read_messages","session_id":target.id,"limit":1}),
            ctx
        )
        .await
        .is_ok());
}
