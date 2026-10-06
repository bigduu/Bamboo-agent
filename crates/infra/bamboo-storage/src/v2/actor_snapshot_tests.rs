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
                    slot_epoch: None,
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
        let bytes = if file == "session.json" {
            // Intentionally changed authority still uses the host's coherent
            // frame; otherwise this would only test unsupported legacy encoding.
            compact_main::serialize_main(&serde_json::from_value::<Session>(value).unwrap())
                .unwrap()
        } else {
            serde_json::to_vec(&value).unwrap()
        };
        fs::write(path, bytes).await.unwrap();
    }
}

#[tokio::test]
async fn title_save_keeps_initialized_child_ancestor_observation_coherent() {
    let f = Fixture::new().await;
    let child = f.child("title-child", &f.root).await;
    let before = f.store.ensure_actor(&child.id).await.unwrap();
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let mut renamed = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    renamed.title = "Renamed root".into();
    renamed.title_generated = true;
    renamed.title_version += 1;
    renamed.metadata_version += 1;
    renamed.updated_at = Utc::now();
    f.store.save_session(&renamed).await.unwrap();
    let after: bamboo_domain::ActorDirectoryEntry = serde_json::from_slice(
        &fs::read(f.directory(&child.id).join("actor-authority.json"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!f
        .directory(&f.root.id)
        .join("actor-authority.json")
        .exists());
    assert_eq!(
        after.actor.observed_metadata_version,
        child.metadata_version
    );
    eprintln!("title save Root uninitialized; canonical Root metadata {} -> {}; Child own metadata {}; Child ancestor observation {} -> {}",
        f.root.metadata_version, renamed.metadata_version, after.actor.observed_metadata_version,
        before.actor.ancestor_observations[0].metadata_version,
        after.actor.ancestor_observations[0].metadata_version);
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(
        after.actor.ancestor_observations[0].metadata_version,
        renamed.metadata_version
    );
}

fn rename_title(mut session: Session) -> Session {
    session.title = "A new title".into();
    session.title_generated = true;
    session.title_version += 1;
    session.metadata_version += 1;
    session.updated_at = Utc::now();
    session
}

async fn title_row(f: &Fixture, id: &str) -> bamboo_domain::ActorDirectoryEntry {
    serde_json::from_slice(
        &fs::read(f.directory(id).join("actor-authority.json"))
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn title_observations_preserve_live_fences_and_unaffected_siblings() {
    let f = Fixture::new().await;
    let parent = f.child("title-parent", &f.root).await;
    let leaf = f.child("title-leaf", &parent).await;
    let sibling = f.child("title-sibling", &f.root).await;
    f.store.ensure_actor(&f.root.id).await.unwrap();
    f.store.ensure_actor(&sibling.id).await.unwrap();
    let parent_activation = f.activate(&parent.id).await;
    let leaf_activation = f.activate(&leaf.id).await;
    let ids = [&f.root.id, &parent.id, &leaf.id, &sibling.id];
    let mut before = Vec::new();
    for id in ids {
        before.push(title_row(&f, id).await);
    }
    let renamed = rename_title(f.store.load_session(&parent.id).await.unwrap().unwrap());
    f.store.save_session(&renamed).await.unwrap();
    for (index, id) in ids.into_iter().enumerate() {
        let actual = title_row(&f, id).await;
        let mut expected = before[index].clone();
        if *id == parent.id {
            expected.actor.observed_metadata_version = renamed.metadata_version;
            expected.revision += 1;
        } else if *id == leaf.id {
            expected.actor.ancestor_observations[0].metadata_version = renamed.metadata_version;
            expected.revision += 1;
        }
        assert_eq!(
            actual, expected,
            "only exact title observations may change for {id}"
        );
    }
    assert_eq!(
        title_row(&f, &parent.id).await.activation,
        Some(parent_activation)
    );
    assert_eq!(
        title_row(&f, &leaf.id).await.activation,
        Some(leaf_activation)
    );
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let before_noop = durable_tree(&f.home);
    f.store.validate_title_observations(&renamed).await.unwrap();
    assert_eq!(durable_tree(&f.home), before_noop);
}

#[tokio::test]
async fn title_observations_reject_preexisting_damage_without_publication() {
    for damage in ["own", "ancestor", "birth", "project", "marker"] {
        let f = Fixture::new().await;
        let child = f.child("damaged-title-child", &f.root).await;
        f.store.ensure_actor(&f.root.id).await.unwrap();
        f.store.ensure_actor(&child.id).await.unwrap();
        if damage == "marker" {
            fs::remove_file(
                f.directory(&child.id)
                    .join("actor-authority.initialized.json"),
            )
            .await
            .unwrap();
        } else {
            f.edit(&child.id, "actor-authority.json", |v| match damage {
                "own" => v["actor"]["observed_metadata_version"] = 1.into(),
                "ancestor" => v["actor"]["ancestor_observations"][0]["metadata_version"] = 1.into(),
                "birth" => v["actor"]["session_created_at"] = serde_json::json!(Utc::now()),
                "project" => v["actor"]["project_id"] = "different-project".into(),
                _ => unreachable!(),
            })
            .await;
        }
        let current = f.store.load_session(&f.root.id).await.unwrap().unwrap();
        let before = durable_tree(&f.home);
        assert!(
            f.store.validate_title_observations(&current).await.is_err(),
            "no-op {damage}"
        );
        assert!(
            f.store.save_session(&rename_title(current)).await.is_err(),
            "rename {damage}"
        );
        assert_eq!(durable_tree(&f.home), before, "no mutation on {damage}");
        assert!(f
            .snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn title_observations_keep_counter_gaps_and_project_aba_fail_closed() {
    for gap in [2, u64::MAX] {
        let f = Fixture::new().await;
        f.store.ensure_actor(&f.root.id).await.unwrap();
        let mut renamed = rename_title(f.root.clone());
        renamed.metadata_version = gap;
        let before = durable_tree(&f.home);
        assert!(f.store.save_session(&renamed).await.is_err());
        assert_eq!(durable_tree(&f.home), before);
    }
    let f = Fixture::new().await;
    f.activate(&f.root.id).await;
    // Two unseen metadata revisions may hide Project A→B→A. A title
    // operation must not bless that already-stale observation as harmless.
    let mut previous = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    previous.metadata_version += 2;
    f.store.save_session(&previous).await.unwrap();
    let before = durable_tree(&f.home);
    assert!(f
        .store
        .save_session(&rename_title(previous.clone()))
        .await
        .is_err());
    assert!(f
        .store
        .validate_title_observations(&previous)
        .await
        .is_err());
    assert_eq!(durable_tree(&f.home), before);
    assert_eq!(
        f.store.ensure_actor(&f.root.id).await.unwrap_err(),
        bamboo_domain::ActorDirectoryError::ProjectTransitionBlocked
    );
}

#[tokio::test]
async fn title_observations_preserve_absence_and_legacy_root_spelling() {
    let f = Fixture::new().await;
    f.store.validate_title_observations(&f.root).await.unwrap();
    let renamed = rename_title(f.root.clone());
    f.store.save_session(&renamed).await.unwrap();
    assert!(!f
        .directory(&f.root.id)
        .join("actor-authority.json")
        .exists());
    assert!(!f
        .directory(&f.root.id)
        .join("actor-authority.initialized.json")
        .exists());
    for file in ["session.json", RUNTIME_SIDECAR_FILE] {
        f.edit(&f.root.id, file, |v| v["root_session_id"] = "".into())
            .await;
    }
    f.store.ensure_actor(&f.root.id).await.unwrap();
    let legacy = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    assert!(legacy.root_session_id.is_empty());
    f.store.validate_title_observations(&legacy).await.unwrap();
    let mut next = rename_title(legacy);
    next.title = "Legacy renamed".into();
    f.store.save_session(&next).await.unwrap();
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn title_observations_noop_reloads_instead_of_trusting_caller_snapshot() {
    let f = Fixture::new().await;
    f.store.ensure_actor(&f.root.id).await.unwrap();
    let old = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    let next = rename_title(old.clone());
    f.store.save_session(&next).await.unwrap();
    let before = durable_tree(&f.home);
    assert!(f.store.validate_title_observations(&old).await.is_err());
    f.store.validate_title_observations(&next).await.unwrap();
    assert_eq!(durable_tree(&f.home), before);
}

#[tokio::test]
async fn title_observations_report_partial_io_and_validate_noop_truthfully() {
    use super::default_actor_context_tests::DefaultWriteHook;
    for (file, phase, new_title_visible, consistent) in [
        (
            "actor-authority.json",
            DurableWritePhase::AfterReplace,
            true,
            false,
        ),
        (
            RUNTIME_SIDECAR_FILE,
            DurableWritePhase::BeforeReplace,
            false,
            true,
        ),
        (
            "session.json",
            DurableWritePhase::BeforeReplace,
            true,
            false,
        ),
        (
            SEARCH_INDEX_REVISION_FILE,
            DurableWritePhase::BeforeReplace,
            true,
            true,
        ),
    ] {
        let f = Fixture::new().await;
        let baseline = rename_title(f.root.clone());
        f.store.save_session(&baseline).await.unwrap();
        let child = f.child("io-title-child", &baseline).await;
        f.store.ensure_actor(&f.root.id).await.unwrap();
        f.store.ensure_actor(&child.id).await.unwrap();
        let old = f.store.load_session(&f.root.id).await.unwrap().unwrap();
        let mut next = rename_title(old.clone());
        next.title = "Next requested title".into();
        let hook = DefaultWriteHook::install(&f.store, file, phase, true);
        hook.release();
        assert!(
            f.store.save_session(&next).await.is_err(),
            "{file}: must report write failure"
        );
        let current = f.store.load_session(&f.root.id).await.unwrap().unwrap();
        assert_eq!(current.title == next.title, new_title_visible);
        let before_validation = durable_tree(&f.home);
        let validation = f.store.validate_title_observations(&current).await;
        let snapshot = f.snapshot(&f.root.id, ActorSnapshotLimits::default()).await;
        assert_eq!(validation.is_ok(), consistent, "no-op after {file}");
        assert_eq!(snapshot.is_ok(), consistent, "snapshot after {file}");
        assert_eq!(durable_tree(&f.home), before_validation);
        if !consistent {
            // A same-title manual retry checks the validator above. Changing
            // back to the old title is also a fresh, rejected preflight.
            let mut retry = rename_title(current);
            retry.title = old.title.clone();
            assert!(f.store.save_session(&retry).await.is_err());
            assert_eq!(durable_tree(&f.home), before_validation);
        }
    }
}

#[tokio::test]
async fn title_failed_refresh_cannot_hide_later_project_aba_from_live_fences() {
    use super::default_actor_context_tests::DefaultWriteHook;
    for initialize_root in [true, false] {
        let f = Fixture::new().await;
        let child = f.child("title-aba-child", &f.root).await;
        let mut fences = Vec::new();
        if initialize_root {
            fences.push(f.activate(&f.root.id).await.fence());
        }
        fences.push(f.activate(&child.id).await.fence());
        let next = rename_title(f.store.load_session(&f.root.id).await.unwrap().unwrap());
        let hook = DefaultWriteHook::install(
            &f.store,
            "actor-authority.json",
            DurableWritePhase::AfterReplace,
            true,
        );
        hook.release();
        assert!(f.store.save_session(&next).await.is_err());
        let mut current = f.store.load_session(&f.root.id).await.unwrap().unwrap();
        for project in [Some("project-b"), None] {
            match project {
                Some(project) => current.set_project_id_meta(project),
                None => current.clear_project_id_meta(),
            }
            current.metadata_version += 1;
            f.store.save_runtime_state(&current).await.unwrap();
        }
        let before = durable_tree(&f.home);
        for fence in fences {
            assert_eq!(
                f.store
                    .validate_fence(&fence, Utc::now())
                    .await
                    .unwrap_err(),
                bamboo_domain::ActorDirectoryError::ProjectTransitionBlocked
            );
        }
        assert!(f.store.validate_title_observations(&current).await.is_err());
        assert_eq!(durable_tree(&f.home), before);
    }
}

#[tokio::test]
async fn title_commit_rejects_non_title_change_in_loaded_snapshot_window() {
    let f = Fixture::new().await;
    let child = f.child("title-concurrent-child", &f.root).await;
    f.store.ensure_actor(&child.id).await.unwrap();
    let old = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    let mut concurrent = old.clone();
    concurrent.model = "new concurrent model".into();
    f.store.save_runtime_state(&concurrent).await.unwrap();
    let before = durable_tree(&f.home);
    assert!(f.store.save_session(&rename_title(old)).await.is_err());
    assert_eq!(durable_tree(&f.home), before);
    assert_eq!(
        f.store
            .load_session(&f.root.id)
            .await
            .unwrap()
            .unwrap()
            .model,
        concurrent.model
    );
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn title_commit_rejects_equal_nonzero_version_from_another_title_writer() {
    let f = Fixture::new().await;
    f.store.ensure_actor(&f.root.id).await.unwrap();
    let old = f.store.load_session(&f.root.id).await.unwrap().unwrap();
    let mut committed = rename_title(old.clone());
    committed.title = "Other title writer".into();
    f.store.save_session(&committed).await.unwrap();
    let before = durable_tree(&f.home);
    assert!(f.store.save_session(&rename_title(old)).await.is_err());
    assert_eq!(durable_tree(&f.home), before);
    f.snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn title_commit_checks_newer_runtime_even_when_stale_main_matches() {
    use super::default_actor_context_tests::DefaultWriteHook;
    for changed_field in ["title", "pinned"] {
        let f = Fixture::new().await;
        let child = f.child("title-split-child", &f.root).await;
        let first = rename_title(child);
        f.store.save_session(&first).await.unwrap();
        f.store.ensure_actor(&first.id).await.unwrap();
        let mut second = first.clone();
        second.metadata_version += 1;
        if changed_field == "title" {
            second.title = "Newer runtime title".into();
            second.title_version += 1;
        } else {
            second.pinned = true;
        }
        let hook = DefaultWriteHook::install(
            &f.store,
            "session.json",
            DurableWritePhase::BeforeReplace,
            true,
        );
        hook.release();
        assert!(f.store.save_session(&second).await.is_err());
        let current = f.store.load_session(&first.id).await.unwrap().unwrap();
        assert_eq!(current.metadata_version, second.metadata_version);
        let before = durable_tree(&f.home);
        // This stale caller exactly matches Main, but must not roll newer
        // runtime back to its old observation and report false success.
        assert!(
            f.store.save_session(&first).await.is_err(),
            "{changed_field}"
        );
        assert_eq!(durable_tree(&f.home), before);
        assert!(f.store.validate_title_observations(&current).await.is_err());
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
    assert!(first.stream_cursor.is_some());
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
    assert_eq!(
        reopened
            .actor_tree_cursor(&f.root.id)
            .await
            .unwrap()
            .map(|v| v.0),
        first.stream_cursor
    );
    let selected = f
        .snapshot(&parent.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(selected.nodes.len(), 67);
    assert!(selected.nodes.iter().all(
        |n| n.actor_id == parent.id || n.parent_actor_id.as_deref() == Some(parent.id.as_str())
    ));
    let mut hooks = Vec::new();
    for node in &first.nodes {
        let mut session = f.store.load_session(&node.actor_id).await.unwrap().unwrap();
        add_private_history(&mut session);
        f.store.save_session(&session).await.unwrap();
        let path = f.directory(&node.actor_id).join("session.json");
        assert!(std::fs::metadata(&path).unwrap().len() > 512 * 1024);
        hooks.push(MainReadHook::install(&path, false));
    }
    let before = durable_tree(&f.home);
    let large = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(first.snapshot_id, large.snapshot_id);
    assert_eq!(first.nodes, large.nodes);
    assert_eq!(first.stream_cursor, large.stream_cursor);
    let selected_later = f
        .snapshot(&parent.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(selected.snapshot_id, selected_later.snapshot_id);
    assert_eq!(selected.nodes, selected_later.nodes);
    assert_eq!(large.stream_cursor, selected_later.stream_cursor);
    let reopened = SessionStoreV2::new(f.home.clone()).await.unwrap();
    assert_eq!(
        large,
        reopened
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &f.root.id,
                &f.root.id,
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap()
    );
    for (node, hook) in first.nodes.iter().zip(&hooks) {
        let raw = std::fs::read(f.directory(&node.actor_id).join("session.json")).unwrap();
        let section = compact_main::section_length(&raw).unwrap();
        let trace = hook.traces();
        assert_eq!(trace.len(), 3);
        assert!(trace
            .iter()
            .all(|read| read.bytes == section && read.offset == section as u64));
    }
    assert_eq!(durable_tree(&f.home), before);
    assert!(!serde_json::to_string(&large).unwrap().contains("PRIVATE"));
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
    // A coherent changed observed Main-frame/Runtime birth makes the
    // initialized actor's ancestor observation stale.
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
    assert!(json["stream_cursor"].as_str().is_some());
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
    assert!(next.stream_cursor.is_some());
    assert_ne!(first.stream_cursor, next.stream_cursor);
}

#[tokio::test]
async fn legacy_missing_tree_marker_fails_closed_without_creating_one() {
    let f = Fixture::new().await;
    let marker = f.directory(&f.root.id).join(ACTOR_TREE_REVISION_FILE);
    fs::remove_file(&marker).await.unwrap();
    let snapshot = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(snapshot.stream_cursor, None);
    assert_eq!(f.store.actor_tree_cursor(&f.root.id).await.unwrap(), None);
    assert!(!marker.exists());
}

#[tokio::test]
async fn private_session_saves_and_noop_actor_reads_do_not_advance_tree_cursor() {
    let f = Fixture::new().await;
    let initial = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let mut root = f.root.clone();
    add_private_history(&mut root);
    f.store.save_session(&root).await.unwrap();
    root.updated_at = Utc::now();
    f.store.save_runtime_state(&root).await.unwrap();
    let after_private = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(after_private.stream_cursor, initial.stream_cursor);
    assert_eq!(after_private.snapshot_id, initial.snapshot_id);

    f.store.inspect_actor(&f.root.id).await.unwrap();
    let initialized = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_ne!(initialized.stream_cursor, initial.stream_cursor);
    f.store.inspect_actor(&f.root.id).await.unwrap();
    let no_op = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_eq!(no_op.stream_cursor, initialized.stream_cursor);
}

#[tokio::test]
async fn cancelled_child_writer_retains_root_lock_until_physical_commit() {
    use super::default_actor_context_tests::DefaultWriteHook;

    struct Release(Arc<DefaultWriteHook>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    let f = Fixture::new().await;
    let mut first = f.child("tree-first", &f.root).await;
    let mut second = f.child("tree-second", &f.root).await;
    let before = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    first.title = "first committed".into();
    first.metadata_version += 1;
    second.title = "second committed".into();
    second.metadata_version += 1;
    let other = Arc::new(SessionStoreV2::new(f.home.clone()).await.unwrap());
    let hook = DefaultWriteHook::install(
        &f.store,
        "session.json",
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let first_write = tokio::spawn({
        let store = f.store.clone();
        async move { store.save_session(&first).await }
    });
    tokio::task::spawn_blocking({
        let hook = hook.clone();
        move || hook.wait()
    })
    .await
    .unwrap();
    first_write.abort();
    assert!(first_write.await.unwrap_err().is_cancelled());
    let second_write = tokio::spawn(async move { other.save_session(&second).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !second_write.is_finished(),
        "another Session under the same Root overtook an unfinished write"
    );
    hook.release();
    second_write.await.unwrap().unwrap();
    let after = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    assert_ne!(before.stream_cursor, after.stream_cursor);
    assert_eq!(after.nodes.len(), 3);
    assert!(after
        .nodes
        .iter()
        .any(|node| node.title == "first committed"));
    assert!(after
        .nodes
        .iter()
        .any(|node| node.title == "second committed"));
    let marker: ActorTreeRevision = serde_json::from_slice(
        &fs::read(f.directory(&f.root.id).join(ACTOR_TREE_REVISION_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(marker.revision, 5); // Root + two creates + two updates.
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

// Hooks are keyed by the opened inode, not a path check/reopen. Production has
// neither this registry nor the pause; every trace is from a real File read.
use std::io::Seek;
use std::os::unix::fs::MetadataExt;
use std::sync::{Condvar, Mutex as StdMutex, OnceLock, Weak};

#[derive(Debug, Clone)]
struct MainReadTrace {
    bytes: usize,
    offset: u64,
}
#[derive(Debug, Default)]
struct MainReadState {
    entered: bool,
    released: bool,
    trace: Vec<MainReadTrace>,
}
#[derive(Debug)]
pub(super) struct MainReadHook {
    pause: bool,
    state: StdMutex<MainReadState>,
    changed: Condvar,
}
type MainHooks = std::collections::HashMap<(u64, u64), Weak<MainReadHook>>;
static MAIN_HOOKS: OnceLock<StdMutex<MainHooks>> = OnceLock::new();
const MAIN_DEADLINE: Duration = Duration::from_secs(10);

pub(super) fn main_read_hook(file: &std::fs::File) -> Option<Arc<MainReadHook>> {
    let metadata = file.metadata().unwrap();
    MAIN_HOOKS
        .get()?
        .lock()
        .unwrap()
        .get(&(metadata.dev(), metadata.ino()))?
        .upgrade()
}
impl MainReadHook {
    fn install(path: &Path, pause: bool) -> Arc<Self> {
        let metadata = std::fs::metadata(path).unwrap();
        let hook = Arc::new(Self {
            pause,
            state: StdMutex::new(MainReadState::default()),
            changed: Condvar::new(),
        });
        MAIN_HOOKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert((metadata.dev(), metadata.ino()), Arc::downgrade(&hook));
        hook
    }
    pub(super) fn opened(&self) {
        let mut state = self.state.lock().unwrap();
        if state.entered {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while self.pause && !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }
    pub(super) fn finished(&self, file: &mut std::fs::File, bytes: usize) {
        self.state.lock().unwrap().trace.push(MainReadTrace {
            bytes,
            offset: file.stream_position().unwrap(),
        });
        self.changed.notify_all();
    }
    fn entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timed) = self
            .changed
            .wait_timeout_while(state, MAIN_DEADLINE, |s| !s.entered)
            .unwrap();
        assert!(
            state.entered && !timed.timed_out(),
            "read closure never opened Main FD"
        );
    }
    fn completed(&self) {
        let state = self.state.lock().unwrap();
        let (state, timed) = self
            .changed
            .wait_timeout_while(state, MAIN_DEADLINE, |s| s.trace.is_empty())
            .unwrap();
        assert!(
            !state.trace.is_empty() && !timed.timed_out(),
            "Main read never finished"
        );
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
    fn traces(&self) -> Vec<MainReadTrace> {
        self.state.lock().unwrap().trace.clone()
    }
}
struct ReleaseMain(Arc<MainReadHook>);
impl Drop for ReleaseMain {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn durable_tree(home: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    use sha2::{Digest, Sha256};
    fn walk(dir: &Path, output: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                output.insert(path.clone(), vec![]);
                walk(&path, output);
            } else {
                output.insert(
                    path.clone(),
                    Sha256::digest(std::fs::read(path).unwrap()).to_vec(),
                );
            }
        }
    }
    let mut output = Default::default();
    walk(&home.join("sessions"), &mut output);
    output
}

fn add_private_history(session: &mut Session) {
    use bamboo_domain::session::provider_transcript::{
        ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
        ProviderTranscriptOrigin,
    };
    let user = bamboo_domain::Message::user("PRIVATE-MESSAGE".repeat(22_000));
    let anchor = user.id.clone();
    session.messages.push(user);
    session.messages.push(bamboo_domain::Message::tool_result(
        "private-call",
        "PRIVATE-TOOL".repeat(16_000),
    ));
    let payloads = [
        (
            ProviderTranscriptAuthor::Model,
            serde_json::json!({"type":"tool_search_call","id":"private-search-call","execution":"server",
                "call_id":"private-search","status":"completed","arguments":{"query":"private-history"}}),
        ),
        (
            ProviderTranscriptAuthor::ToolResult,
            serde_json::json!({"type":"tool_search_output","id":"private-search-output","execution":"server",
                "call_id":"private-search","status":"completed","tools":[{"type":"function","name":"private_fixture_tool"}]}),
        ),
        (
            ProviderTranscriptAuthor::Model,
            serde_json::json!({"type":"message","id":"private-native","role":"assistant","status":"completed",
                "content":[{"type":"output_text","text":"PRIVATE-NATIVE".repeat(16_000),"annotations":[]}]}),
        ),
    ];
    let items = payloads
        .into_iter()
        .map(|(author, payload)| {
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        })
        .collect();
    session
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &"a".repeat(64),
        )
        .unwrap();
    session
        .append_provider_transcript_group(&anchor, None, items)
        .unwrap();
}

#[tokio::test]
async fn compact_main_budget_counts_each_framing_byte_once_and_stops_at_section() {
    let f = Fixture::new().await;
    let path = f.directory(&f.root.id).join("session.json");
    let mut session = f.root.clone();
    add_private_history(&mut session);
    f.store.save_session(&session).await.unwrap();
    let raw = std::fs::read(&path).unwrap();
    let section = compact_main::section_length(&raw).unwrap();
    let hook = MainReadHook::install(&path, false);
    let dir = actor_snapshot_reader::Directory::open_absolute(&f.directory(&f.root.id)).unwrap();
    // Seed an actual ancillary debit; the Main section exactly fills the rest.
    std::fs::write(f.directory(&f.root.id).join("earlier"), b"paid").unwrap();
    let limits = ActorSnapshotLimits {
        aggregate_read_bytes: section + 4,
        file_bytes: section,
        ..ActorSnapshotLimits::default()
    };
    let mut budget = actor_snapshot_reader::ReadBudget::new(limits);
    assert_eq!(
        dir.read("earlier", 4, &mut budget).unwrap().unwrap(),
        b"paid"
    );
    assert_eq!(
        dir.read_main_section(&mut budget).unwrap().unwrap(),
        raw[..section]
    );
    let trace = hook.traces();
    assert_eq!((trace[0].bytes, trace[0].offset), (section, section as u64));
    assert_eq!(
        dir.read("earlier", 4, &mut budget).unwrap_err(),
        Error::BudgetExceeded
    );
    for limits in [
        ActorSnapshotLimits {
            file_bytes: section - 1,
            ..ActorSnapshotLimits::default()
        },
        ActorSnapshotLimits {
            aggregate_read_bytes: section - 1,
            ..ActorSnapshotLimits::default()
        },
    ] {
        let mut budget = actor_snapshot_reader::ReadBudget::new(limits);
        assert_eq!(
            dir.read_main_section(&mut budget).unwrap_err(),
            Error::BudgetExceeded
        );
    }
    assert!(hook.traces()[1..]
        .iter()
        .all(|trace| trace.bytes == compact_main::HEADER_BYTES
            && trace.offset == compact_main::HEADER_BYTES as u64));
}

#[tokio::test]
async fn compact_observation_does_not_claim_unseen_flat_or_complete_json_integrity() {
    let f = Fixture::new().await;
    let baseline = f
        .snapshot(&f.root.id, ActorSnapshotLimits::default())
        .await
        .unwrap();
    let path = f.directory(&f.root.id).join("session.json");
    let raw = std::fs::read(&path).unwrap();
    let end = compact_main::section_length(&raw).unwrap();
    let mut flat: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    flat.as_object_mut().unwrap().remove(compact_main::MEMBER);
    flat["created_at"] = serde_json::json!(Utc::now() + chrono::Duration::days(1));
    flat["runtime_metadata"] = serde_json::json!({"project_id":"unseen-foreign"});
    let flat = serde_json::to_vec(&flat).unwrap();
    let mut damaged = raw[..end].to_vec();
    damaged.extend_from_slice(&flat[1..]);
    for bytes in [
        damaged,
        [raw[..end].to_vec(), b"invalid JSON suffix".to_vec()].concat(),
    ] {
        assert!(compact_main::validate_full_main(&bytes).is_err());
        std::fs::write(&path, &bytes).unwrap();
        let before = durable_tree(&f.home);
        assert_eq!(
            f.snapshot(&f.root.id, ActorSnapshotLimits::default())
                .await
                .unwrap(),
            baseline
        );
        assert_eq!(durable_tree(&f.home), before);
    }
}

#[tokio::test]
async fn compact_main_malformed_unsupported_missing_and_tight_caps_fail_without_writes() {
    let f = Fixture::new().await;
    let path = f.directory(&f.root.id).join("session.json");
    let raw = std::fs::read(&path).unwrap();
    let end = compact_main::section_length(&raw).unwrap();
    let prefix = compact_main::PREFIX.len();
    let mut bad_digit = raw.clone();
    bad_digit[prefix] = b'x';
    let mut huge = raw.clone();
    huge[prefix..prefix + 10].copy_from_slice(b"9999999999");
    let mut bad_middle = raw.clone();
    bad_middle[prefix + 10] = b'!';
    let mut bad_close = raw.clone();
    bad_close[end - 1] = b'!';
    let mut malformed = raw.clone();
    malformed[compact_main::HEADER_BYTES] = 0xff;
    let payload: serde_json::Value =
        serde_json::from_slice(&raw[compact_main::HEADER_BYTES..end - 2]).unwrap();
    let frame = |payload: &[u8]| {
        let mut bytes = compact_main::PREFIX.to_vec();
        bytes.extend_from_slice(format!("{:010}", payload.len()).as_bytes());
        bytes.extend_from_slice(&raw[prefix + 10..compact_main::HEADER_BYTES]);
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(b"},");
        bytes
    };
    let mut missing = payload.clone();
    missing.as_object_mut().unwrap().remove("project_id");
    let mut unknown = payload.clone();
    unknown["unknown"] = serde_json::json!(true);
    let serialized = serde_json::to_vec(&payload).unwrap();
    let mut duplicate = b"{\"id\":\"duplicate\",".to_vec();
    duplicate.extend_from_slice(&serialized[1..]);
    let mut nested_unknown = payload.clone();
    nested_unknown["authority_identity"]["unknown"] = serde_json::json!(true);
    let nested_duplicate = String::from_utf8(serialized.clone()).unwrap().replace(
        "\"authority_identity\":{",
        "\"authority_identity\":{\"kind\":\"ordinary\",",
    );
    let mut unsupported = raw.clone();
    let version = unsupported
        .windows(b"\"version\":1".len())
        .position(|w| w == b"\"version\":1")
        .unwrap();
    unsupported[version + b"\"version\":".len()] = b'2';
    for (bytes, error) in [
        (bad_digit, Error::InconsistentAuthority),
        (huge, Error::BudgetExceeded),
        (bad_middle, Error::InconsistentAuthority),
        (bad_close, Error::InconsistentAuthority),
        (malformed, Error::InconsistentAuthority),
        (
            raw[..compact_main::HEADER_BYTES - 1].to_vec(),
            Error::InconsistentAuthority,
        ),
        (raw[..end - 1].to_vec(), Error::InconsistentAuthority),
        (
            frame(&serde_json::to_vec(&missing).unwrap()),
            Error::InconsistentAuthority,
        ),
        (
            frame(&serde_json::to_vec(&unknown).unwrap()),
            Error::InconsistentAuthority,
        ),
        (frame(&duplicate), Error::InconsistentAuthority),
        (
            frame(&serde_json::to_vec(&nested_unknown).unwrap()),
            Error::InconsistentAuthority,
        ),
        (
            frame(nested_duplicate.as_bytes()),
            Error::InconsistentAuthority,
        ),
        (unsupported, Error::UnsupportedAuthority),
        (b"{}".to_vec(), Error::UnsupportedAuthority),
        (
            serde_json::to_vec(&f.root).unwrap(),
            Error::UnsupportedAuthority,
        ),
    ] {
        std::fs::write(&path, bytes).unwrap();
        let before = durable_tree(&f.home);
        assert_eq!(
            f.snapshot(&f.root.id, ActorSnapshotLimits::default())
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(durable_tree(&f.home), before);
    }
    std::fs::remove_file(&path).unwrap();
    let before = durable_tree(&f.home);
    assert_eq!(
        f.snapshot(&f.root.id, ActorSnapshotLimits::default())
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    assert_eq!(durable_tree(&f.home), before);
}

#[tokio::test]
async fn compact_main_retains_open_inode_after_filename_replace_and_counts_truncation() {
    let f = Fixture::new().await;
    let path = f.directory(&f.root.id).join("session.json");
    let raw = std::fs::read(&path).unwrap();
    let section = compact_main::section_length(&raw).unwrap();
    let directory =
        actor_snapshot_reader::Directory::open_absolute(&f.directory(&f.root.id)).unwrap();
    let hook = MainReadHook::install(&path, true);
    let _release = ReleaseMain(hook.clone());
    let read = tokio::task::spawn_blocking(move || {
        let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
        directory.read_main_section(&mut budget).unwrap().unwrap()
    });
    let opened = hook.clone();
    tokio::task::spawn_blocking(move || opened.entered())
        .await
        .unwrap();
    std::fs::rename(&path, path.with_extension("retained")).unwrap();
    symlink("/etc/passwd", &path).unwrap();
    hook.release();
    assert_eq!(read.await.unwrap(), raw[..section]);
    assert_eq!(
        (hook.traces()[0].bytes, hook.traces()[0].offset),
        (section, section as u64)
    );
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(path.with_extension("retained"), &path).unwrap();
    let hook = MainReadHook::install(&path, true);
    let _release = ReleaseMain(hook.clone());
    let directory =
        actor_snapshot_reader::Directory::open_absolute(&f.directory(&f.root.id)).unwrap();
    let read = tokio::task::spawn_blocking(move || {
        let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
        directory.read_main_section(&mut budget)
    });
    let opened = hook.clone();
    tokio::task::spawn_blocking(move || opened.entered())
        .await
        .unwrap();
    // Already opened/stat'ed real file becomes shorter; partial bytes are paid.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len((section - 1) as u64)
        .unwrap();
    hook.release();
    assert_eq!(
        read.await.unwrap().unwrap_err(),
        Error::InconsistentAuthority
    );
    assert_eq!(
        (hook.traces()[0].bytes, hook.traces()[0].offset),
        (section - 1, (section - 1) as u64)
    );
}

fn assert_snapshot_physical_guards_held(home: &Path) {
    for name in [
        SESSION_LIFECYCLE_LOCK_FILE,
        RUNTIME_TASK_TRANSACTION_LOCK_FILE,
    ] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(home.join(name))
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }
}
fn competing_snapshot_writer(
    store: Arc<SessionStoreV2>,
    mut root: Session,
) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
    let (done, receiver) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        root.messages
            .push(bamboo_domain::Message::user("after actual reader finished"));
        runtime.block_on(store.save_session(&root)).unwrap();
        done.send(()).unwrap();
    });
    (writer, receiver)
}

#[tokio::test]
async fn compact_started_reader_keeps_actual_guards_after_caller_abort_then_writer_reopens() {
    let f = Fixture::new().await;
    let second = Arc::new(SessionStoreV2::new(f.home.clone()).await.unwrap());
    let hook = MainReadHook::install(&f.directory(&f.root.id).join("session.json"), true);
    let _release = ReleaseMain(hook.clone());
    let (store, root) = (f.store.clone(), f.root.id.clone());
    let reader = tokio::spawn(async move {
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root,
                &root,
                ActorSnapshotLimits::default(),
            )
            .await
    });
    let opened = hook.clone();
    tokio::task::spawn_blocking(move || opened.entered())
        .await
        .unwrap();
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    assert_snapshot_physical_guards_held(&f.home);
    let (writer, done) = competing_snapshot_writer(second, f.root.clone());
    assert!(matches!(
        done.recv_timeout(Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    hook.release();
    done.recv_timeout(MAIN_DEADLINE).unwrap();
    writer.join().unwrap();
    hook.completed();
    let reopened = SessionStoreV2::new(f.home.clone()).await.unwrap();
    let saved = reopened.load_session(&f.root.id).await.unwrap().unwrap();
    assert_eq!(
        saved.messages.last().unwrap().content,
        "after actual reader finished"
    );
    assert_eq!(saved.created_at, f.root.created_at);
}

#[test]
fn compact_started_reader_keeps_guards_after_whole_runtime_shutdown() {
    struct RequestDropped(std::sync::mpsc::Sender<()>);
    impl Drop for RequestDropped {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let f = runtime.block_on(Fixture::new());
    let second = Arc::new(
        runtime
            .block_on(SessionStoreV2::new(f.home.clone()))
            .unwrap(),
    );
    let hook = MainReadHook::install(&f.directory(&f.root.id).join("session.json"), true);
    let _release = ReleaseMain(hook.clone());
    let (dropped, observed) = std::sync::mpsc::channel();
    let (store, root) = (f.store.clone(), f.root.id.clone());
    let request = runtime.spawn(async move {
        let _dropped = RequestDropped(dropped);
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root,
                &root,
                ActorSnapshotLimits::default(),
            )
            .await
    });
    hook.entered();
    runtime.shutdown_background();
    observed.recv_timeout(MAIN_DEADLINE).unwrap(); // The actual inner future dropped.
    assert_snapshot_physical_guards_held(&f.home);
    let (writer, done) = competing_snapshot_writer(second, f.root.clone());
    assert!(matches!(
        done.recv_timeout(Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    hook.release();
    done.recv_timeout(MAIN_DEADLINE).unwrap();
    writer.join().unwrap();
    hook.completed();
    let independent = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    independent.block_on(async {
        assert!(request.await.unwrap_err().is_cancelled());
        let reopened = SessionStoreV2::new(f.home.clone()).await.unwrap();
        let saved = reopened.load_session(&f.root.id).await.unwrap().unwrap();
        assert_eq!(
            saved.messages.last().unwrap().content,
            "after actual reader finished"
        );
        assert_eq!(saved.created_at, f.root.created_at);
    });
}

#[tokio::test]
async fn compact_large_root_mode_and_supervisor_keep_existing_committed_proof_checks() {
    let f = Fixture::new().await;
    let supervisor = f
        .store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    for id in [&f.root.id, &supervisor.session_id] {
        let mut session = f.store.load_session(id).await.unwrap().unwrap();
        add_private_history(&mut session);
        f.store.save_session(&session).await.unwrap();
        let operation = RootModeOperationRequest {
            session_id: id.clone(),
            operation_id: format!("0:{}", Uuid::new_v4()),
            birth_token: session.root_mode_birth_token(),
            expected_epoch: 0,
            requested_enabled: true,
            action: RootModeOperationAction::Select,
        };
        assert!(matches!(
            f.store.root_mode_operation(&operation).await.unwrap(),
            RootModeOperationDecision::Terminal(_)
        ));
        let saved = f.store.load_session(id).await.unwrap().unwrap();
        assert!(saved.root_orchestration_only);
        assert_eq!(saved.root_mode_transition_epoch, 1);
        assert_eq!(saved.root_mode_operations.len(), 1);
        let dir = f.home.join("sessions").join(id);
        assert!(std::fs::metadata(dir.join("session.json")).unwrap().len() > 512 * 1024);
        let before = durable_tree(&f.home);
        assert_eq!(
            f.store
                .actor_subtree_snapshot(
                    ActorSnapshotPrincipal::host_owner(),
                    id,
                    id,
                    ActorSnapshotLimits::default()
                )
                .await
                .unwrap()
                .nodes
                .len(),
            1
        );
        assert_eq!(durable_tree(&f.home), before);
        // A readable proof mismatch still rejects; the Main optimization does
        // not replace the committed Root/Supervisor consistency validators.
        let proof = dir.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
        let original = std::fs::read(&proof).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        assert_eq!(value["root_mode_transition_epoch"], 1);
        value["root_mode_transition_epoch"] = serde_json::json!(99);
        std::fs::write(&proof, serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            f.store
                .actor_subtree_snapshot(
                    ActorSnapshotPrincipal::host_owner(),
                    id,
                    id,
                    ActorSnapshotLimits::default()
                )
                .await
                .unwrap_err(),
            Error::StaleAuthority
        );
        std::fs::write(&proof, original).unwrap();
    }
}
