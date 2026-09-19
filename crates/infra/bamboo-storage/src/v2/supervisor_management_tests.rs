//! Host-link acceptance against canonical files and independent V2 stores.

use std::collections::BTreeSet;

use super::*;
use bamboo_domain::{
    Message, SessionAuthorityConflict, SupervisorManagementMutation as Mutation,
    SupervisorManagementReceipt, SupervisorManagementRequest, SupervisorReference,
    MAX_SUPERVISOR_LINKS, MAX_SUPERVISOR_PROJECTS,
};

async fn fixture() -> (tempfile::TempDir, Arc<SessionStoreV2>, SupervisorReference) {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let reference = (&store
        .get_or_create_default_supervisor("supervisor-model")
        .await
        .unwrap())
        .into();
    (home, store, reference)
}

fn scope(projects: &[&str]) -> Mutation {
    Mutation::ConfigureProjectScope {
        allowed_projects: projects.iter().map(|id| id.parse().unwrap()).collect(),
    }
}

fn attach(id: &str) -> Mutation {
    Mutation::Attach {
        target_session_id: id.into(),
    }
}
fn detach(id: &str) -> Mutation {
    Mutation::Detach {
        target_session_id: id.into(),
    }
}

async fn change(
    store: &SessionStoreV2,
    reference: &SupervisorReference,
    revision: u64,
    mutation: Mutation,
) -> io::Result<SupervisorManagementReceipt> {
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: revision,
            mutation,
        })
        .await
}

async fn target(store: &SessionStoreV2, id: &str, project: &str) -> Session {
    let mut root = Session::new(id, format!("{id}-model"));
    root.set_project_id_meta(project);
    root.metadata_version = 7;
    root.set_workspace_path_meta(format!("/workspace/{id}"));
    root.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::default());
    root.agent_runtime_state
        .as_mut()
        .unwrap()
        .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
    root.add_message(Message::user(format!("{id} independent conversation")));
    store.save_session(&root).await.unwrap();
    root
}

async fn files(store: &SessionStoreV2, id: &str) -> (Vec<u8>, Vec<u8>) {
    let directory = store.sessions_root_dir().join(id);
    (
        fs::read(directory.join("session.json")).await.unwrap(),
        fs::read(directory.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap(),
    )
}

async fn linked(store: &SessionStoreV2, reference: &SupervisorReference, id: &str) -> bool {
    store
        .inspect_supervisor_link(reference, id)
        .await
        .unwrap()
        .authorized
}

#[tokio::test]
async fn supervisor_management_requires_explicit_scope_and_preserves_two_existing_roots() {
    let (_home, store, reference) = fixture().await;
    let a = target(&store, "target-a", "project-a").await;
    let b = target(&store, "target-b", "project-b").await;
    let before_a = files(&store, &a.id).await;
    let before_b = files(&store, &b.id).await;
    let mut supervisor = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    supervisor.set_project_id_meta("project-a");
    supervisor.metadata_version += 1;
    supervisor
        .metadata
        .insert("allowed_projects".into(), "project-a,project-b".into());
    supervisor.add_message(Message::user("Supervisor history also survives"));
    store.save_session(&supervisor).await.unwrap();
    let empty = store.inspect_supervisor_scope(&reference).await.unwrap();
    assert_eq!(empty.state_revision, 0);
    assert!(empty.allowed_projects.is_empty());
    assert_eq!(
        change(&store, &reference, 0, attach(&a.id))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        change(&store, &reference, 0, scope(&["project-a", "project-b"]))
            .await
            .unwrap()
            .state_revision,
        1
    );
    assert_eq!(
        change(&store, &reference, 1, attach(&a.id))
            .await
            .unwrap()
            .link_revision,
        Some(1)
    );
    let repeated = change(&store, &reference, 2, attach(&a.id)).await.unwrap();
    assert!(!repeated.changed);
    assert_eq!(repeated.state_revision, 2);
    change(&store, &reference, 2, attach(&b.id)).await.unwrap();
    assert!(linked(&store, &reference, &a.id).await);
    assert!(linked(&store, &reference, &b.id).await);
    let observation = store
        .inspect_supervisor_link(&reference, &a.id)
        .await
        .unwrap();
    let link = observation.link.unwrap();
    assert_eq!(link.target_created_at, a.created_at);
    assert_eq!(link.target_metadata_version, a.metadata_version);
    assert_eq!(link.target_project_id.as_str(), "project-a");
    change(&store, &reference, 3, detach(&a.id)).await.unwrap();
    assert!(!linked(&store, &reference, &a.id).await);
    assert!(linked(&store, &reference, &b.id).await);
    assert_eq!(files(&store, &a.id).await, before_a);
    assert_eq!(files(&store, &b.id).await, before_b);
    let full = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        full.messages.last().unwrap().content,
        "Supervisor history also survives"
    );
    assert_eq!(full.metadata_version, supervisor.metadata_version);
    assert_eq!(full.supervisor_management.unwrap().revision, 4);
    let child = Session::new_child_of("target-child", &a, "model", "child");
    store.save_session(&child).await.unwrap();
    assert!(change(&store, &reference, 4, attach(&child.id))
        .await
        .is_err());
    assert!(change(&store, &reference, 4, attach(&reference.session_id))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_management_independent_store_cas_has_one_winner_and_explicit_retry() {
    let (home, first, reference) = fixture().await;
    let second = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        change(&first, &reference, 0, scope(&["project-a"])),
        change(&second, &reference, 0, scope(&["project-b"]))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let stale = if a.is_err() {
        a.unwrap_err()
    } else {
        b.unwrap_err()
    };
    assert_eq!(stale.kind(), io::ErrorKind::WouldBlock);
    let observed = second.inspect_supervisor_scope(&reference).await.unwrap();
    assert_eq!(observed.state_revision, 1);
    let before = files(&first, &reference.session_id).await;
    let retry = change(
        &second,
        &reference,
        1,
        Mutation::ConfigureProjectScope {
            allowed_projects: observed.allowed_projects,
        },
    )
    .await
    .unwrap();
    assert!(!retry.changed);
    assert_eq!(retry.state_revision, 1);
    assert_eq!(files(&first, &reference.session_id).await, before);
    let cold = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(
        cold.inspect_supervisor_scope(&reference)
            .await
            .unwrap()
            .state_revision,
        1
    );
}

#[tokio::test]
async fn supervisor_management_scope_regrant_metadata_aba_and_new_birth_require_reattach() {
    let (_home, store, reference) = fixture().await;
    let mut a = target(&store, "target-a", "project-a").await;
    change(&store, &reference, 0, scope(&["project-a", "project-b"]))
        .await
        .unwrap();
    change(&store, &reference, 1, attach(&a.id)).await.unwrap();
    change(&store, &reference, 2, scope(&["project-b"]))
        .await
        .unwrap();
    let disabled = store
        .inspect_supervisor_link(&reference, &a.id)
        .await
        .unwrap()
        .link
        .unwrap();
    assert!(!disabled.enabled);
    assert_eq!(disabled.revision, 2);
    change(&store, &reference, 3, scope(&["project-a", "project-b"]))
        .await
        .unwrap();
    assert!(!linked(&store, &reference, &a.id).await);
    assert_eq!(
        change(&store, &reference, 4, attach(&a.id))
            .await
            .unwrap()
            .link_revision,
        Some(3)
    );
    for project in ["project-b", "project-a"] {
        a.set_project_id_meta(project);
        a.metadata_version += 1;
        store.save_session(&a).await.unwrap();
        assert!(!linked(&store, &reference, &a.id).await);
    }
    change(&store, &reference, 5, attach(&a.id)).await.unwrap();
    assert!(linked(&store, &reference, &a.id).await);
    a.title = "unrelated metadata change".into();
    a.metadata_version += 1;
    store.save_session(&a).await.unwrap();
    assert!(!linked(&store, &reference, &a.id).await);
    change(&store, &reference, 6, attach(&a.id)).await.unwrap();
    store.delete_session(&a.id).await.unwrap();
    assert!(!linked(&store, &reference, &a.id).await);
    let mut replacement = store
        .recreate_root_session(&a.id, "new-model")
        .await
        .unwrap();
    replacement.set_project_id_meta("project-a");
    replacement.metadata_version += 1;
    store.save_session(&replacement).await.unwrap();
    assert!(!linked(&store, &reference, &a.id).await);
    change(&store, &reference, 7, attach(&a.id)).await.unwrap();
    assert!(linked(&store, &reference, &a.id).await);
    assert_eq!(
        store
            .inspect_supervisor_link(&reference, &a.id)
            .await
            .unwrap()
            .link
            .unwrap()
            .target_created_at,
        replacement.created_at
    );
}

#[tokio::test]
async fn supervisor_management_rejects_forged_callers_and_stale_incarnation() {
    let (_home, store, reference) = fixture().await;
    let root = target(&store, "ordinary", "project-a").await;
    let child = Session::new_child_of("ordinary-child", &root, "model", "child");
    store.save_session(&child).await.unwrap();
    for id in [&root.id, &child.id, "made-up"] {
        let forged = SupervisorReference {
            session_id: id.into(),
            incarnation_id: reference.incarnation_id,
        };
        assert!(store.inspect_supervisor_scope(&forged).await.is_err());
        assert!(change(&store, &forged, 0, scope(&["project-a"]))
            .await
            .is_err());
    }
    change(&store, &reference, 0, scope(&["project-a"]))
        .await
        .unwrap();
    let state = store
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap()
        .supervisor_management;
    for mut forged in [root.clone(), child, Session::new("forged-new", "model")] {
        forged.supervisor_management = state.clone();
        for runtime in [false, true] {
            let result = if runtime {
                store.save_runtime_state(&forged).await
            } else {
                store.save_session(&forged).await
            };
            assert!(result
                .unwrap_err()
                .get_ref()
                .is_some_and(|error| error.is::<SessionAuthorityConflict>()));
        }
        assert!(store
            .validate_root_context_for_save(&forged)
            .await
            .unwrap_err()
            .get_ref()
            .is_some_and(|error| error.is::<SessionAuthorityConflict>()));
    }
    store.delete_session(&reference.session_id).await.unwrap();
    let replacement = SupervisorReference::from(
        &store
            .get_or_create_default_supervisor("replacement")
            .await
            .unwrap(),
    );
    assert_ne!(replacement.incarnation_id, reference.incarnation_id);
    assert!(store.inspect_supervisor_scope(&reference).await.is_err());
    assert!(change(&store, &reference, 0, scope(&["project-a"]))
        .await
        .is_err());
    assert_eq!(
        store
            .inspect_supervisor_scope(&replacement)
            .await
            .unwrap()
            .state_revision,
        0
    );
}

#[tokio::test]
async fn supervisor_management_stale_raw_saves_fail_merges_adopt_and_copies_drop_links() {
    let (_home, store, reference) = fixture().await;
    target(&store, "target-a", "project-a").await;
    change(&store, &reference, 0, scope(&["project-a"]))
        .await
        .unwrap();
    change(&store, &reference, 1, attach("target-a"))
        .await
        .unwrap();
    let stale = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    change(&store, &reference, 2, detach("target-a"))
        .await
        .unwrap();
    let before = files(&store, &reference.session_id).await;
    for runtime in [false, true] {
        let result = if runtime {
            store.save_runtime_state(&stale).await
        } else {
            store.save_session(&stale).await
        };
        assert!(result
            .unwrap_err()
            .get_ref()
            .is_some_and(|error| error.is::<SessionAuthorityConflict>()));
        assert_eq!(files(&store, &reference.session_id).await, before);
    }
    let locked = crate::session_merge::LockedSessionStore::new(store.clone());
    for runtime in [false, true] {
        let mut candidate = stale.clone();
        let expected = store
            .load_root_authority(&reference.session_id)
            .await
            .unwrap()
            .unwrap()
            .supervisor_management;
        let published = std::sync::atomic::AtomicBool::new(false);
        let callback = |saved: &Session| {
            assert_eq!(saved.supervisor_management, expected);
            published.store(true, Ordering::SeqCst);
        };
        if runtime {
            locked
                .save_runtime_only_and_publish(&mut candidate, callback)
                .await
                .unwrap();
        } else {
            locked
                .merge_save_runtime_and_publish(&mut candidate, |saved, committed| {
                    assert!(committed);
                    callback(saved);
                })
                .await
                .unwrap();
        }
        assert!(published.load(Ordering::SeqCst));
        assert_eq!(candidate.supervisor_management, expected);
        assert!(!linked(&store, &reference, "target-a").await);
    }
    let copy = store
        .copy_session(&reference.session_id, "ordinary-copy")
        .await
        .unwrap()
        .unwrap();
    assert!(copy.authority_identity.is_ordinary());
    assert!(copy.supervisor_management.is_none());
}

#[tokio::test]
async fn supervisor_management_damage_fails_closed_but_target_damage_cannot_block_detach() {
    for supervisor_damaged in [false, true] {
        for missing in [false, true] {
            let (_home, store, reference) = fixture().await;
            target(&store, "target-a", "project-a").await;
            change(&store, &reference, 0, scope(&["project-a"]))
                .await
                .unwrap();
            change(&store, &reference, 1, attach("target-a"))
                .await
                .unwrap();
            let damaged_id = if supervisor_damaged {
                &reference.session_id
            } else {
                "target-a"
            };
            let path = store
                .sessions_root_dir()
                .join(damaged_id)
                .join(RUNTIME_SIDECAR_FILE);
            if missing {
                fs::remove_file(&path).await.unwrap();
            } else {
                fs::write(&path, b"{corrupt").await.unwrap();
            }
            assert!(store
                .inspect_supervisor_link(&reference, "target-a")
                .await
                .is_err());
            assert!(change(&store, &reference, 2, attach("target-a"))
                .await
                .is_err());
            let revoked = change(&store, &reference, 2, detach("target-a")).await;
            assert_eq!(revoked.is_err(), supervisor_damaged);
            if !supervisor_damaged {
                assert!(!linked(&store, &reference, "target-a").await);
                let unchanged = change(&store, &reference, 3, detach("target-a"))
                    .await
                    .unwrap();
                assert!(!unchanged.changed);
                assert_eq!(unchanged.link_revision, Some(2));
            }
        }
    }
    // Deletion also cannot prevent detachment of the persisted relation.
    let (_home, store, reference) = fixture().await;
    target(&store, "target-a", "project-a").await;
    change(&store, &reference, 0, scope(&["project-a"]))
        .await
        .unwrap();
    change(&store, &reference, 1, attach("target-a"))
        .await
        .unwrap();
    store.delete_session("target-a").await.unwrap();
    assert!(
        change(&store, &reference, 2, detach("target-a"))
            .await
            .unwrap()
            .changed
    );
}

#[tokio::test]
async fn supervisor_management_rejects_corrupt_unsupported_regressing_and_divergent_state() {
    for damage in [
        "schema",
        "incarnation",
        "zero-revision",
        "oversized-id",
        "scope",
        "link-revision",
        "regressed",
        "diverged",
        "lost-link",
    ] {
        let (_home, store, reference) = fixture().await;
        target(&store, "target-a", "project-a").await;
        change(&store, &reference, 0, scope(&["project-a"]))
            .await
            .unwrap();
        change(&store, &reference, 1, attach("target-a"))
            .await
            .unwrap();
        let full = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        store.save_session(&full).await.unwrap(); // main now holds revision 2.
        let path = store
            .sessions_root_dir()
            .join(&reference.session_id)
            .join(RUNTIME_SIDECAR_FILE);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        let state = &mut value["supervisor_management"];
        match damage {
            "schema" => state["schema_version"] = 2.into(),
            "incarnation" => state["incarnation_id"] = Uuid::new_v4().to_string().into(),
            "zero-revision" => state["revision"] = 0.into(),
            "oversized-id" => {
                let link = state["links"]["target-a"].clone();
                state["links"]["a".repeat(257)] = link;
            }
            "scope" => state["allowed_projects"] = serde_json::json!([]),
            "link-revision" => state["links"]["target-a"]["revision"] = 100.into(),
            "regressed" => state["revision"] = 1.into(),
            "diverged" => state["links"]["target-a"]["enabled"] = false.into(),
            "lost-link" => {
                state["revision"] = 3.into();
                state["links"] = serde_json::json!({});
            }
            _ => unreachable!(),
        }
        fs::write(&path, serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();
        let before = files(&store, &reference.session_id).await;
        assert!(
            store.inspect_supervisor_scope(&reference).await.is_err(),
            "{damage}"
        );
        assert!(
            store.load_session(&reference.session_id).await.is_err(),
            "{damage}"
        );
        assert!(
            change(&store, &reference, 2, detach("target-a"))
                .await
                .is_err(),
            "{damage}"
        );
        assert_eq!(files(&store, &reference.session_id).await, before);
    }
}

#[tokio::test]
async fn supervisor_management_capacity_and_overflow_fail_before_publication() {
    let (_home, store, reference) = fixture().await;
    target(&store, "new-target", "project-a").await;
    let oversized: BTreeSet<_> = (0..=MAX_SUPERVISOR_PROJECTS)
        .map(|i| format!("project-{i}").parse().unwrap())
        .collect();
    let before = files(&store, &reference.session_id).await;
    assert!(change(
        &store,
        &reference,
        0,
        Mutation::ConfigureProjectScope {
            allowed_projects: oversized
        }
    )
    .await
    .is_err());
    assert!(change(&store, &reference, 0, attach(&"a".repeat(257)))
        .await
        .is_err());
    assert_eq!(files(&store, &reference.session_id).await, before);
    change(&store, &reference, 0, scope(&["project-a"]))
        .await
        .unwrap();
    change(&store, &reference, 1, attach("new-target"))
        .await
        .unwrap();
    let mut full = store
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let mut bounded = full.supervisor_management.take().unwrap();
    let mut tombstone = bounded.links.remove("new-target").unwrap();
    tombstone.enabled = false;
    for i in 0..MAX_SUPERVISOR_LINKS {
        bounded
            .links
            .insert(format!("old-target-{i}"), tombstone.clone());
    }
    full.supervisor_management = Some(bounded.clone());
    let path = store
        .sessions_root_dir()
        .join(&reference.session_id)
        .join(RUNTIME_SIDECAR_FILE);
    fs::write(&path, serde_json::to_vec(&full).unwrap())
        .await
        .unwrap();
    let before = files(&store, &reference.session_id).await;
    assert!(change(&store, &reference, 2, attach("new-target"))
        .await
        .is_err());
    assert_eq!(files(&store, &reference.session_id).await, before);
    // Host operations cannot increment either exhausted counter, but a fresh
    // already-satisfied retry remains a valid no-op at the exhausted revision.
    bounded.links.clear();
    bounded.revision = u64::MAX;
    bounded.links.insert(
        "new-target".into(),
        bamboo_domain::SupervisorManagedLink {
            revision: u64::MAX,
            enabled: true,
            ..tombstone
        },
    );
    full.supervisor_management = Some(bounded);
    fs::write(&path, serde_json::to_vec(&full).unwrap())
        .await
        .unwrap();
    let before = files(&store, &reference.session_id).await;
    assert!(change(&store, &reference, u64::MAX, detach("new-target"))
        .await
        .is_err());
    assert!(change(
        &store,
        &reference,
        u64::MAX,
        scope(&["project-a", "project-b"])
    )
    .await
    .is_err());
    assert!(
        !change(&store, &reference, u64::MAX, scope(&["project-a"]))
            .await
            .unwrap()
            .changed
    );
    assert_eq!(files(&store, &reference.session_id).await, before);
}
