//! Focused coverage for the Host-installed Supervisor session directory:
//! authority denial for ordinary callers, links∪index merge with origins,
//! pagination, and engagement mirroring through the tool action.

use std::sync::Arc;

use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolCtx};
use bamboo_domain::{
    AgentRuntimeState, HumanInputMark, Session, SessionAuthorityIdentity, Storage,
    SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_server_tools::session_control::directory::{
    SupervisorDirectoryPort, SupervisorDirectoryRequest,
};

use crate::app_state::supervisor_directory::SupervisorSessionDirectory;
use bamboo_storage::SessionStoreV2;

/// Codex P2: with directory AND root_run both installed (production wiring),
/// the directory oneOf variant must keep query/limit/cursor usable — the
/// root_run install loop must not strip them from the directory variant.
#[tokio::test]
async fn session_control_schema_keeps_directory_params_with_root_run_installed() {
    use bamboo_agent_core::tools::Tool as _;
    use bamboo_domain::SessionActivationDisposition;

    struct NoopActivation;
    #[async_trait::async_trait]
    impl bamboo_domain::SessionActivationPort for NoopActivation {
        async fn request_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationDisposition, bamboo_domain::SessionActivationError> {
            Ok(SessionActivationDisposition::ActivationReserved)
        }
    }
    let (home, store, _reference) = fixture().await;
    let inbox: Arc<dyn bamboo_domain::SessionInboxPort> = Arc::new(
        bamboo_storage::FileSessionInbox::new(store.clone(), Default::default()),
    );
    let activation: Arc<dyn bamboo_domain::SessionActivationPort> = Arc::new(NoopActivation);
    let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
        store.clone(),
        inbox,
        activation,
    ));
    let directory: Arc<dyn SupervisorDirectoryPort> = Arc::new(SupervisorSessionDirectory::new(
        store.clone(),
        store.clone(),
    ));
    let tool = bamboo_server_tools::SessionControlTool::new(messenger).with_directory(directory);
    let schema = tool.parameters_schema();
    // Codex P2: directory-only installation must narrow the top-level
    // requirement to `action` so directory requests are schema-valid.
    assert_eq!(
        schema["required"],
        serde_json::json!(["action"]),
        "directory-only install must require only action at top level"
    );
    let variant = schema["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .find(|variant| variant["properties"]["action"]["const"] == "directory")
        .expect("directory variant present");
    for field in ["query", "limit", "cursor"] {
        assert!(
            variant["properties"][field].is_object(),
            "directory variant must keep {field} usable, got {}",
            variant["properties"][field]
        );
    }
    assert!(home.path().exists());
}

async fn fixture() -> (tempfile::TempDir, Arc<SessionStoreV2>, SupervisorReference) {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let reference: SupervisorReference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    (home, store, reference)
}

fn supervisor_ctx(session: &Session) -> ToolCtx {
    let mut ctx = ToolCtx::none("test-directory-call");
    let id: Arc<str> = Arc::from(session.id.as_str());
    ctx.session_id = Some(id.clone());
    ctx.executing_supervisor = Some(
        ExecutingSupervisorObservation::capture_from_executing_session(session)
            .expect("supervisor identity present"),
    );
    ctx
}

async fn authorized_supervisor(store: &SessionStoreV2, reference: &SupervisorReference) -> Session {
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
    let mut session = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .expect("supervisor persisted");
    session.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: reference.incarnation_id,
    };
    session
}

async fn root(store: &SessionStoreV2, id: &str, project: &str) -> Session {
    let mut session = Session::new(id, "model");
    session.set_project_id_meta(project);
    session.set_workspace_path_meta(format!("/workspace/{id}"));
    store.save_session(&session).await.unwrap();
    session
}

fn with_human_input(session: &mut Session) {
    let mut runtime = AgentRuntimeState::new("run-1");
    runtime.set_last_human_input(HumanInputMark::user(chrono::Utc::now()));
    session.agent_runtime_state = Some(runtime);
}

#[tokio::test]
async fn ordinary_session_has_no_directory_authority() {
    let (_home, store, _reference) = fixture().await;
    let directory = SupervisorSessionDirectory::new(store.clone(), store.clone());
    let mut ordinary = Session::new("ordinary-root", "model");
    ordinary.set_project_id_meta("project-a");
    let mut ctx = ToolCtx::none("ordinary-call");
    ctx.session_id = Some(Arc::from("ordinary-root"));
    let error = directory
        .directory(&ctx, SupervisorDirectoryRequest::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("authority_required"));
}

#[tokio::test]
async fn directory_merges_link_origin_and_user_opened_with_engagement() {
    let (_home, store, reference) = fixture().await;
    let supervisor = authorized_supervisor(&store, &reference).await;

    // Supervisor-created: attach a management link to a saved root.
    let mut created = root(&store, "aaa-created-root", "project-a").await;
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::Attach {
                target_session_id: "aaa-created-root".into(),
            },
        })
        .await
        .unwrap();

    // User-opened in an authorized project, with human engagement; a second
    // one in another project is visible too: the current-user authority
    // domain spans all Projects (legacy allowed_projects is not a gate).
    let mut engaged = root(&store, "zzz-user-engaged", "project-a").await;
    with_human_input(&mut engaged);
    store.save_session(&engaged).await.unwrap();
    root(&store, "other-project-root", "project-b").await;

    let directory = SupervisorSessionDirectory::new(store.clone(), store.clone());
    let page = directory
        .directory(
            &supervisor_ctx(&supervisor),
            SupervisorDirectoryRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.caller_session_id, DEFAULT_SUPERVISOR_SESSION_ID);
    // Supervisor itself never appears as an entry.
    assert!(page
        .entries
        .iter()
        .all(|e| e.session_id != DEFAULT_SUPERVISOR_SESSION_ID));

    // A legacy-attached root keeps origin=user_opened (attachment is not
    // creation provenance) but reports linked=true.
    let created_entry = page
        .entries
        .iter()
        .find(|e| e.session_id == "aaa-created-root")
        .expect("attached entry");
    assert_eq!(
        created_entry.origin,
        bamboo_server_tools::session_control::directory::SessionDirectoryOrigin::UserOpened,
        "attachment must not relabel origin"
    );
    assert!(created_entry.linked);
    assert!(created_entry.last_human_input.is_none());

    let user_entry = page
        .entries
        .iter()
        .find(|e| e.session_id == "zzz-user-engaged")
        .expect("user-opened entry");
    assert_eq!(
        user_entry.origin,
        bamboo_server_tools::session_control::directory::SessionDirectoryOrigin::UserOpened
    );
    assert!(!user_entry.linked);
    assert!(user_entry.last_human_input.is_some());

    assert!(
        page.entries
            .iter()
            .any(|e| e.session_id == "other-project-root"),
        "current-user domain spans all Projects; legacy allowlist is not a gate"
    );
    let _ = &mut created;
}

/// Codex P2 follow-up: a root minted by the Host creation route carries the
/// durable origin mark and is classified supervisor_created even without any
/// management link (create deliberately attaches none).
#[tokio::test]
async fn directory_marks_host_created_roots_by_provenance_not_links() {
    let (_home, store, reference) = fixture().await;
    let supervisor = authorized_supervisor(&store, &reference).await;
    let mut created = root(&store, "mmm-host-created", "project-a").await;
    created
        .metadata
        .insert("session.origin.v1".into(), "supervisor_created".into());
    store.save_session(&created).await.unwrap();
    // No management link: the creation route deliberately mints none.

    let directory = SupervisorSessionDirectory::new(store.clone(), store.clone());
    let page = directory
        .directory(
            &supervisor_ctx(&supervisor),
            SupervisorDirectoryRequest::default(),
        )
        .await
        .unwrap();
    let entry = page
        .entries
        .iter()
        .find(|e| e.session_id == "mmm-host-created")
        .expect("host-created entry without a link");
    assert_eq!(
        entry.origin,
        bamboo_server_tools::session_control::directory::SessionDirectoryOrigin::SupervisorCreated
    );
    assert!(!entry.linked);
}

/// Codex P1: a linked target that was PATCH-moved to an unauthorized Project
/// (stale link keeps the old Project) must not expose its current metadata.
#[tokio::test]
async fn directory_hides_linked_target_moved_out_of_authorized_project() {
    let (_home, store, reference) = fixture().await;
    let supervisor = authorized_supervisor(&store, &reference).await;
    let mut target = root(&store, "linked-moved", "project-a").await;
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::Attach {
                target_session_id: "linked-moved".into(),
            },
        })
        .await
        .unwrap();

    // User moves the session to an unauthorized Project via PATCH: the index
    // now reports project-b while the historical link still says project-a.
    // The PATCH path bumps metadata_version; mirror that here.
    target.set_project_id_meta("project-b");
    target.metadata_version += 1;
    store.save_session(&target).await.unwrap();

    let directory = SupervisorSessionDirectory::new(store.clone(), store.clone());
    let page = directory
        .directory(
            &supervisor_ctx(&supervisor),
            SupervisorDirectoryRequest::default(),
        )
        .await
        .unwrap();
    assert!(
        !page.entries.iter().any(|e| e.session_id == "linked-moved"),
        "a link whose current target binding changed must expose nothing"
    );
}

#[tokio::test]
async fn directory_paginates_with_stable_cursor() {
    let (_home, store, reference) = fixture().await;
    let supervisor = authorized_supervisor(&store, &reference).await;
    root(&store, "page-a", "project-a").await;
    root(&store, "page-b", "project-a").await;
    root(&store, "page-c", "project-a").await;

    let directory = SupervisorSessionDirectory::new(store.clone(), store.clone());
    let ctx = supervisor_ctx(&supervisor);
    let first = directory
        .directory(
            &ctx,
            SupervisorDirectoryRequest {
                limit: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 2);
    let cursor = first.next_cursor.clone().expect("more pages remain");

    let second = directory
        .directory(
            &ctx,
            SupervisorDirectoryRequest {
                limit: Some(2),
                cursor: Some(cursor.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!second.entries.is_empty());
    assert!(
        second
            .entries
            .iter()
            .all(|e| !first.entries.iter().any(|f| f.session_id == e.session_id)),
        "pages do not overlap"
    );

    // Scope change invalidates the cursor binding.
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-b".parse().unwrap()].into(),
            },
        })
        .await
        .unwrap();
    let refreshed = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let stale = directory
        .directory(
            &supervisor_ctx(&refreshed),
            SupervisorDirectoryRequest {
                limit: Some(2),
                cursor: Some(cursor),
                ..Default::default()
            },
        )
        .await;
    match stale {
        Err(error) if error.to_string().contains("cursor_stale") => {}
        Err(error) => panic!("unexpected error: {error}"),
        Ok(page) => panic!("expected stale cursor error, got {page:?}"),
    }
}
