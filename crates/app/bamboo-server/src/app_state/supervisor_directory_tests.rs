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
    // one in an unauthorized project must stay invisible.
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

    let created_entry = page
        .entries
        .iter()
        .find(|e| e.session_id == "aaa-created-root")
        .expect("supervisor-created entry");
    assert_eq!(
        created_entry.origin,
        bamboo_server_tools::session_control::directory::SessionDirectoryOrigin::SupervisorCreated
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
        !page
            .entries
            .iter()
            .any(|e| e.session_id == "other-project-root"),
        "unauthorized Project sessions stay invisible"
    );
    let _ = &mut created;
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
