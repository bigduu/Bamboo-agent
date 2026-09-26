use super::*;
use bamboo_domain::{
    ActorActivation, ActorActivationClaim, ActorDirectoryPort, ActorLogicalState,
    ActorPlacementClass, ActorPlacementRef, ActorSnapshotError as Error, ActorSnapshotLimits,
    ActorSnapshotPort, ActorSnapshotPrincipal, PublicActorSubtreeSnapshot,
};
use std::os::unix::fs::symlink;

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    store: Arc<SessionStoreV2>,
    root: Session,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        // macOS /var is a symlink; the configured capability root is canonical.
        let home = temp.path().canonicalize().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
        let root = Session::new("snapshot-root", "model");
        store.save_session(&root).await.unwrap();
        Self {
            _temp: temp,
            home,
            store,
            root,
        }
    }
    async fn child(&self, id: &str, parent: &Session) -> Session {
        let child = Session::new_child_of(id, parent, "model", "Child");
        self.store.save_session(&child).await.unwrap();
        child
    }
    async fn snapshot(
        &self,
        subtree: &str,
        limits: ActorSnapshotLimits,
    ) -> Result<PublicActorSubtreeSnapshot, Error> {
        self.store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &self.root.id,
                subtree,
                limits,
            )
            .await
    }
    async fn activate(&self, id: &str) -> ActorActivation {
        let now = Utc::now();
        self.store
            .claim_activation(&ActorActivationClaim {
                actor_id: id.to_owned(),
                run_id: "PRIVATE-RUN".into(),
                lease_owner: "PRIVATE-HOST".into(),
                lease_expires_at: now + chrono::Duration::minutes(5),
                inbox_generation: 42,
                placement_ref: Some(ActorPlacementRef {
                    class: ActorPlacementClass::Docker,
                    lease_id: "PRIVATE-ENDPOINT-LEASE".into(),
                }),
                now,
            })
            .await
            .unwrap()
    }
    fn directory(&self, id: &str) -> PathBuf {
        if id == self.root.id {
            self.home.join("sessions").join(id)
        } else {
            self.home
                .join("sessions")
                .join(&self.root.id)
                .join("children")
                .join(id)
        }
    }
    async fn edit(&self, id: &str, file: &str, update: impl FnOnce(&mut serde_json::Value)) {
        let path = self.directory(id).join(file);
        let mut value = serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        update(&mut value);
        fs::write(path, serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn complete_134_actor_tree_survives_restart_and_leaves_cold_authority_unknown() {
    let f = Fixture::new().await;
    let parent = f.child("nested-parent", &f.root).await;
    for n in 0..132 {
        f.child(
            &format!("node-{n:03}"),
            if n % 2 == 0 { &parent } else { &f.root },
        )
        .await;
    }
    let first = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(first.nodes.len(), 134);
    assert_eq!(first.schema_version, 1);
    assert_eq!(first.stream_cursor, None);
    assert!(first.nodes.iter().all(|n| n.logical_state.is_none()
        && n.placement_class.is_none()
        && n.activation.is_none()
        && n.revision.actor_directory_revision.is_none()));
    let child = first
        .nodes
        .iter()
        .find(|n| n.actor_id == "node-000")
        .unwrap();
    assert_eq!(child.parent_actor_id.as_deref(), Some("nested-parent"));
    assert_eq!(child.root_actor_id, f.root.id);
    assert_eq!(child.depth, 2);
    assert!(!f
        .directory(&f.root.id)
        .join("actor-authority.json")
        .exists());
    assert!(!f
        .directory("node-000")
        .join("actor-authority.initialized.json")
        .exists());
    let reopened = SessionStoreV2::new(f.home.clone()).await.unwrap();
    let restarted = reopened
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &f.root.id,
            &f.root.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(first, restarted);
    let selected = f
        .snapshot(&parent.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(selected.nodes.len(), 67);
    assert!(selected.nodes.iter().all(
        |n| n.actor_id == parent.id || n.parent_actor_id.as_deref() == Some(parent.id.as_str())
    ));
}

#[tokio::test]
async fn live_actor_fence_authorizes_self_and_descendants_but_not_siblings_or_foreign_roots() {
    let f = Fixture::new().await;
    let parent = f.child("parent", &f.root).await;
    let child = f.child("child", &parent).await;
    let sibling = f.child("sibling", &f.root).await;
    let activation = f.activate(&parent.id).await;
    let read = |root: String, subtree: String, fence: bamboo_domain::ActorActivationFence| {
        let store = f.store.clone();
        async move {
            store
                .actor_subtree_snapshot(
                    ActorSnapshotPrincipal::live_actor(fence),
                    &root,
                    &subtree,
                    ActorSnapshotLimits::default(),
                )
                .await
        }
    };
    assert_eq!(
        read(f.root.id.clone(), parent.id.clone(), activation.fence())
            .await
            .unwrap()
            .nodes
            .len(),
        2
    );
    assert_eq!(
        read(f.root.id.clone(), child.id.clone(), activation.fence())
            .await
            .unwrap()
            .nodes
            .len(),
        1
    );
    for subtree in [&f.root.id, &sibling.id] {
        assert_eq!(
            read(f.root.id.clone(), subtree.clone(), activation.fence())
                .await
                .unwrap_err(),
            Error::UnauthorizedScope
        );
    }
    let foreign = Session::new("foreign-root", "model");
    f.store.save_session(&foreign).await.unwrap();
    // A foreign Root must be rejected before parsing even its corrupt body.
    fs::write(
        f.home.join("sessions/foreign-root/session.json"),
        b"PRIVATE invalid body",
    )
    .await
    .unwrap();
    assert_eq!(
        read(foreign.id.clone(), foreign.id.clone(), activation.fence())
            .await
            .unwrap_err(),
        Error::UnauthorizedScope
    );
    for component in 0..7 {
        let mut fence = activation.fence();
        match component {
            0 => fence.schema_version += 1,
            1 => fence.actor_id = sibling.id.clone(),
            2 => fence.activation_id.push('x'),
            3 => fence.attempt += 1,
            4 => fence.run_id.push('x'),
            5 => fence.lease_owner.push('x'),
            _ => fence.lease_epoch += 1,
        }
        assert_eq!(
            read(f.root.id.clone(), parent.id.clone(), fence)
                .await
                .unwrap_err(),
            Error::UnauthorizedScope
        );
    }
    f.edit(&parent.id, "actor-authority.json", |v| {
        v["activation"]["lease_expires_at"] =
            serde_json::json!(Utc::now() - chrono::Duration::seconds(1))
    })
    .await;
    assert_eq!(
        read(f.root.id.clone(), parent.id.clone(), activation.fence())
            .await
            .unwrap_err(),
        Error::UnauthorizedScope
    );
}

#[tokio::test]
async fn durable_lineage_rejects_foreign_parent_cycles_depth_and_cache_forgery() {
    let f = Fixture::new().await;
    let parent = f.child("parent", &f.root).await;
    f.child("child", &parent).await;
    // Rebuildable index cannot turn a foreign/sibling selector into authority.
    {
        let mut index = f.store.index.write().await;
        index.sessions.get_mut("child").unwrap().parent_session_id = Some("forged".into());
        index.sessions.get_mut("child").unwrap().root_session_id = "foreign".into();
    }
    let snapshot = f
        .snapshot("child", ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(snapshot.nodes[0].parent_actor_id.as_deref(), Some("parent"));
    for (parent, depth) in [("foreign", 2), ("child", 2), ("snapshot-root", 2)] {
        for file in ["session.json", "runtime.json"] {
            f.edit("child", file, |v| {
                v["parent_session_id"] = serde_json::json!(parent);
                v["spawn_depth"] = serde_json::json!(depth);
            })
            .await;
        }
        assert_eq!(
            f.snapshot(&f.root.id, ActorSnapshotLimits::default())
                .await
                .unwrap_err(),
            Error::InconsistentAuthority
        );
    }
}

#[tokio::test]
async fn changed_birth_stale_row_and_missing_marker_fail_without_repair() {
    let f = Fixture::new().await;
    let parent = f.child("parent", &f.root).await;
    f.child("child", &parent).await;
    f.store.ensure_actor("child").await.unwrap();
    // A coherent changed birth makes the initialized actor row stale, while
    // a contradictory compact/flat Main is rejected as inconsistent earlier.
    let path = f.directory("parent").join("session.json");
    let mut main: Session = serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
    main.created_at = f.root.created_at;
    fs::write(path, compact_main::serialize_main(&main).unwrap())
        .await
        .unwrap();
    f.edit("parent", "runtime.json", |v| {
        v["created_at"] = serde_json::json!(f.root.created_at)
    })
    .await;
    assert_eq!(
        f.snapshot("child", ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::StaleAuthority
    );
    // A row cannot be recreated after losing its independent initialization marker.
    fs::remove_file(
        f.directory("child")
            .join("actor-authority.initialized.json"),
    )
    .await
    .unwrap();
    assert_eq!(
        f.snapshot("child", ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    assert!(!f
        .directory("child")
        .join("actor-authority.initialized.json")
        .exists());
}

#[tokio::test]
async fn every_budget_rejects_the_whole_view_and_counts_non_candidate_entries() {
    let f = Fixture::new().await;
    f.child("child", &f.root).await;
    let default = ActorSnapshotLimits::default();
    for limits in [
        ActorSnapshotLimits {
            nodes: 1,
            ..default
        },
        ActorSnapshotLimits {
            file_reads: 1,
            ..default
        },
        ActorSnapshotLimits {
            file_bytes: 1,
            ..default
        },
        ActorSnapshotLimits {
            aggregate_read_bytes: 1,
            ..default
        },
        ActorSnapshotLimits {
            response_bytes: 1,
            ..default
        },
        ActorSnapshotLimits {
            nodes: default.nodes + 1,
            ..default
        },
    ] {
        assert_eq!(
            f.snapshot(&f.root.id, limits).await.unwrap_err(),
            Error::BudgetExceeded
        );
    }
    let children = f.directory(&f.root.id).join("children");
    fs::write(children.join("junk-one"), b"not a candidate")
        .await
        .unwrap();
    fs::write(children.join("junk-two"), b"not a candidate")
        .await
        .unwrap();
    assert_eq!(
        f.snapshot(
            &f.root.id,
            ActorSnapshotLimits {
                directory_entries: 1,
                ..default
            }
        )
        .await
        .unwrap_err(),
        Error::BudgetExceeded
    );
}

#[tokio::test]
async fn pending_journals_are_unchanged_and_reader_waits_for_writer_transaction() {
    let f = Fixture::new().await;
    let guard = f
        .store
        .lock_runtime_task_transaction_shared()
        .await
        .unwrap();
    let store = f.store.clone();
    let root = f.root.id.clone();
    let mut read = tokio::spawn(async move {
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root,
                &root,
                ActorSnapshotLimits::default(),
            )
            .await
    });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut read)
        .await
        .is_err());
    drop(guard);
    assert!(read.await.unwrap().is_ok());
    for name in [RUNTIME_TASK_TRANSACTION_DIR, SESSION_COPY_TRANSACTION_DIR] {
        let directory = f.home.join(name);
        fs::create_dir_all(&directory).await.unwrap();
        let journal = directory.join("pending.json");
        fs::write(&journal, b"PRIVATE journal incomplete")
            .await
            .unwrap();
        assert_eq!(
            f.snapshot(&f.root.id, ActorSnapshotLimits::default())
                .await
                .unwrap_err(),
            Error::PendingTransaction
        );
        assert_eq!(
            fs::read(&journal).await.unwrap(),
            b"PRIVATE journal incomplete"
        );
        fs::remove_file(journal).await.unwrap();
    }
}

#[tokio::test]
async fn whitelist_never_serializes_private_payload_or_synthesizes_health_queue_cursor() {
    let f = Fixture::new().await;
    let mut session = f.root.clone();
    session.title = "Public title".into();
    session
        .metadata
        .insert("responsibility".into(), "PRIVATE-PROFILE".into());
    session
        .metadata
        .insert("api_key".into(), "PRIVATE-CREDENTIAL".into());
    session.workspace = Some("/PRIVATE/ENV/PATH".into());
    session
        .messages
        .push(bamboo_domain::Message::user("PRIVATE-TRANSCRIPT"));
    f.store.save_session(&session).await.unwrap();
    let activation = f.activate(&f.root.id).await;
    let snapshot = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let json = serde_json::to_value(&snapshot).unwrap();
    let wire = json.to_string();
    assert!(!wire.contains("PRIVATE"));
    for key in [
        "queue",
        "health",
        "request",
        "wait",
        "global_revision",
        "lease",
        "run_id",
        "project_id",
        "checkpoint",
        "messages",
        "responsibility",
    ] {
        assert!(!wire.contains(key), "{key}");
    }
    assert_eq!(json["stream_cursor"], serde_json::Value::Null);
    assert_eq!(
        snapshot.nodes[0].logical_state,
        Some(ActorLogicalState::Active)
    );
    assert_eq!(
        snapshot.nodes[0].placement_class,
        Some(ActorPlacementClass::Docker)
    );
    assert_eq!(
        snapshot.nodes[0].activation.as_ref().unwrap().activation_id,
        activation.activation_id
    );
    assert!(snapshot.nodes[0].revision.actor_directory_revision.unwrap() > 0);
    assert_eq!(
        json["nodes"][0]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        [
            "actor_id",
            "parent_actor_id",
            "root_actor_id",
            "depth",
            "title",
            "role",
            "logical_state",
            "placement_class",
            "revision",
            "activation"
        ]
        .into_iter()
        .map(String::from)
        .collect()
    );
    for error in [
        Error::InvalidSelector,
        Error::StaleAuthority,
        Error::InconsistentAuthority,
        Error::StorageUnavailable,
    ] {
        assert!(!format!(
            "{error:?} {error} {}",
            serde_json::to_string(&error).unwrap()
        )
        .contains("PRIVATE"));
    }
}

#[tokio::test]
async fn actual_files_and_ancestors_reject_symlink_escape_and_directory_replacement_keeps_fd() {
    let f = Fixture::new().await;
    let root = f.directory(&f.root.id);
    let file = root.join("session.json");
    let outside = f.home.join("outside.json");
    fs::rename(&file, &outside).await.unwrap();
    symlink(&outside, &file).unwrap();
    assert_eq!(
        f.snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    fs::remove_file(&file).await.unwrap();
    fs::rename(&outside, &file).await.unwrap();
    let saved = f.home.join("saved-root");
    fs::rename(&root, &saved).await.unwrap();
    symlink(&saved, &root).unwrap();
    assert_eq!(
        f.snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    fs::remove_file(&root).await.unwrap();
    fs::rename(&saved, &root).await.unwrap();
    let directory = actor_snapshot_reader::Directory::open_absolute(&root).unwrap();
    fs::rename(&root, &saved).await.unwrap();
    fs::create_dir(&root).await.unwrap();
    fs::write(root.join("session.json"), b"PRIVATE replacement")
        .await
        .unwrap();
    let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
    let retained = directory
        .read("session.json", 512 * 1024, &mut budget)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&retained).unwrap()["id"],
        f.root.id
    );
}

#[test]
fn actual_read_growth_is_capped_after_stat_and_fd_does_not_follow_replaced_filename() {
    use std::io::Write;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("file");
    std::fs::write(&path, b"small").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    assert_eq!(file.metadata().unwrap().len(), 5);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[b'x'; 100])
        .unwrap();
    let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits {
        aggregate_read_bytes: 8,
        ..ActorSnapshotLimits::default()
    });
    assert_eq!(
        actor_snapshot_reader::read_content(file, 8, &mut budget).unwrap_err(),
        Error::BudgetExceeded
    );
    let file = std::fs::File::open(&path).unwrap();
    std::fs::rename(&path, temp.path().join("original")).unwrap();
    let outside = temp.path().join("outside");
    std::fs::write(&outside, b"PRIVATE").unwrap();
    symlink(&outside, &path).unwrap();
    let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
    assert!(!actor_snapshot_reader::read_content(file, 200, &mut budget)
        .unwrap()
        .starts_with(b"PRIVATE"));
}

#[tokio::test]
async fn opaque_identity_changes_with_view_without_becoming_global_revision() {
    let f = Fixture::new().await;
    let first = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let mut root = f.root.clone();
    root.title = "Changed public title".into();
    root.metadata_version += 1;
    f.store.save_session(&root).await.unwrap();
    let next = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_ne!(first.snapshot_id, next.snapshot_id);
    assert_eq!(first.nodes[0].actor_id, next.nodes[0].actor_id);
    assert_eq!(
        next.nodes[0].revision.session_metadata_version,
        root.metadata_version
    );
    assert_eq!(next.stream_cursor, None);
}

#[tokio::test]
async fn main_runtime_project_and_authority_divergence_fail_closed() {
    let f = Fixture::new().await;
    f.child("child", &f.root).await;
    f.edit("child", "runtime.json", |v| {
        v["metadata"]["project_id"] = serde_json::json!("project-b");
        v["runtime_metadata"] = serde_json::json!({"project_id": "project-b"});
    })
    .await;
    assert_eq!(
        f.snapshot("child", ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::StaleAuthority
    );
    f.edit("child", "runtime.json", |v| {
        v["metadata"].as_object_mut().unwrap().remove("project_id");
        v["runtime_metadata"] = serde_json::Value::Null;
        v["authority_identity"] =
            serde_json::json!({"kind": "supervisor", "incarnation_id": Uuid::new_v4()});
    })
    .await;
    assert_eq!(
        f.snapshot("child", ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::StaleAuthority
    );
}

#[tokio::test]
async fn unavailable_root_proof_and_revoked_birth_do_not_return_cached_tree() {
    let f = Fixture::new().await;
    let proof = f.directory(&f.root.id).join("root-tool-authority.json");
    let bytes = fs::read(&proof).await.unwrap();
    fs::remove_file(&proof).await.unwrap();
    assert_eq!(
        f.snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    assert!(!proof.exists());
    fs::write(&proof, bytes).await.unwrap();
    let revocations = f.home.join(".root-revocations");
    fs::create_dir_all(&revocations).await.unwrap();
    fs::write(revocations.join(format!("{}.json", f.root.id)), serde_json::to_vec(&serde_json::json!({"version": 1, "session_id": f.root.id, "revoked_through": f.root.created_at})).unwrap()).await.unwrap();
    assert_eq!(
        f.snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::NotFound
    );
}
