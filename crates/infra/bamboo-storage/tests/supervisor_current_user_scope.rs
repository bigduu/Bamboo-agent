//! The current-user contract uses canonical storage, not fixture ACL grants.
//! Legacy mutation below only constructs old on-disk data for migration tests.

use bamboo_domain::{
    AgentRuntimeState, Message, Session, Storage, SupervisorManagementMutation,
    SupervisorManagementRequest, SupervisorReference,
};
use bamboo_storage::SessionStoreV2;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn fixture() -> (tempfile::TempDir, SessionStoreV2, SupervisorReference) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let reference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    (home, store, reference)
}

async fn root(store: &SessionStoreV2, id: &str, project: Option<&str>) -> Session {
    let mut session = Session::new(id, "model");
    if let Some(project) = project {
        session.set_project_id_meta(project);
    }
    session.agent_runtime_state = Some(AgentRuntimeState::new(id));
    session.add_message(Message::user(format!("synthetic history for {id}")));
    store.save_session(&session).await.unwrap();
    session
}

async fn canonical_bytes(store: &SessionStoreV2, id: &str) -> (Vec<u8>, Vec<u8>) {
    let directory = store.sessions_root_dir().join(id);
    (
        tokio::fs::read(directory.join("session.json"))
            .await
            .unwrap(),
        tokio::fs::read(directory.join("runtime.json"))
            .await
            .unwrap(),
    )
}

async fn assert_default_read(
    store: &SessionStoreV2,
    supervisor: &SupervisorReference,
    target: &Session,
) {
    let observation = store
        .inspect_supervisor_link(supervisor, &target.id)
        .await
        .unwrap();
    assert!(
        observation.authorized,
        "current-user target {} must not require configure/attach",
        target.id
    );
    let read = store
        .load_supervisor_managed_session(supervisor, &target.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.id, target.id);
    assert_eq!(read.created_at, target.created_at);
    assert_eq!(read.authority_identity, target.authority_identity);
    assert_eq!(read.messages.len(), target.messages.len());
    assert_eq!(read.messages[0].content, target.messages[0].content);
}

#[tokio::test]
async fn current_user_two_projects_and_later_roots_need_no_manual_grants() {
    let (_home, store, reference) = fixture().await;
    let a = root(&store, "current-a", Some("project-a")).await;
    let b = root(&store, "current-b", Some("project-b")).await;
    let before_a = canonical_bytes(&store, &a.id).await;
    let before_b = canonical_bytes(&store, &b.id).await;
    let supervisor = canonical_bytes(&store, &reference.session_id).await;
    for target in [&a, &b] {
        assert_default_read(&store, &reference, target).await;
    }
    let later = root(&store, "current-later", Some("project-later")).await;
    assert_default_read(&store, &reference, &later).await;
    assert_eq!(canonical_bytes(&store, &a.id).await, before_a);
    assert_eq!(canonical_bytes(&store, &b.id).await, before_b);
    assert_eq!(
        canonical_bytes(&store, &reference.session_id).await,
        supervisor
    );
}

#[tokio::test]
async fn legacy_scope_and_detached_link_are_preserved_without_restricting_default_access() {
    let (_home, store, reference) = fixture().await;
    let a = root(&store, "legacy-a", Some("project-a")).await;
    let b = root(&store, "legacy-b", Some("project-b")).await;
    for (revision, mutation) in [
        (
            0,
            SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap()].into(),
            },
        ),
        (
            1,
            SupervisorManagementMutation::Attach {
                target_session_id: a.id.clone(),
            },
        ),
        (
            2,
            SupervisorManagementMutation::Detach {
                target_session_id: a.id.clone(),
            },
        ),
        (
            3,
            SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: Default::default(),
            },
        ),
    ] {
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: revision,
                mutation,
            })
            .await
            .unwrap();
    }
    let before = canonical_bytes(&store, &reference.session_id).await;
    assert_default_read(&store, &reference, &a).await;
    assert_default_read(&store, &reference, &b).await;
    let historical = store
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap()
        .supervisor_management
        .unwrap();
    assert_eq!(historical.revision, 4);
    assert!(historical.allowed_projects.is_empty());
    assert!(!historical.links[&a.id].enabled);
    assert_eq!(canonical_bytes(&store, &reference.session_id).await, before);
}

#[tokio::test]
async fn current_user_no_project_root_and_child_history_need_no_fabricated_project() {
    let (_home, store, reference) = fixture().await;
    let parent = root(&store, "unbound-root", None).await;
    let mut child = Session::new_child_of("unbound-child", &parent, "model", "bounded work");
    child.agent_runtime_state = Some(AgentRuntimeState::new("child"));
    child.add_message(Message::assistant("synthetic child evidence", None));
    store.save_session(&child).await.unwrap();
    assert_default_read(&store, &reference, &parent).await;
    assert_default_read(&store, &reference, &child).await;
    assert!(store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap()
        .project_id_meta()
        .is_none());
}

#[tokio::test]
async fn default_domain_access_does_not_trust_other_store_or_ordinary_identity() {
    let (_home, store, reference) = fixture().await;
    let (_other_home, _other, other_reference) = fixture().await;
    let target = root(&store, "local-target", Some("project-local")).await;
    let ordinary = root(&store, "ordinary", Some("project-local")).await;
    let before = canonical_bytes(&store, &target.id).await;
    for invalid in [
        other_reference,
        SupervisorReference {
            session_id: ordinary.id,
            incarnation_id: reference.incarnation_id,
        },
    ] {
        assert!(store
            .load_supervisor_managed_session(&invalid, &target.id)
            .await
            .is_err());
        assert!(store
            .inspect_supervisor_link(&invalid, &target.id)
            .await
            .is_err());
    }
    assert_eq!(canonical_bytes(&store, &target.id).await, before);
}

#[tokio::test]
async fn canonical_supervisor_publication_works_in_both_modes_without_changing_worker_permissions()
{
    let (_home, store, reference) = fixture().await;
    let worker = root(&store, "worker", Some("project-worker")).await;
    let before = canonical_bytes(&store, &worker.id).await;
    let effects = Arc::new(AtomicUsize::new(0));
    for ultra in [false, true] {
        let mut supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(ultra).unwrap();
        store.save_session(&supervisor).await.unwrap();
        let effect = effects.clone();
        // Zero is the legacy Standard birth revision. Its effective role
        // authority starts at1; existing positive revisions stay unchanged.
        let revision = supervisor.root_tool_authority_revision.max(1);
        store
            .with_supervisor_orchestration_authority(
                &reference,
                revision,
                Box::new(move || {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .await
            .expect("trusted Supervisor business publication must not depend on Ultra");
    }
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    assert_eq!(canonical_bytes(&store, &worker.id).await, before);
}
