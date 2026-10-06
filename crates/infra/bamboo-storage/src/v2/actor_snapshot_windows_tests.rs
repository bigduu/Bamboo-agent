//! Native Windows coverage for the handle-bound snapshot reader. These tests
//! intentionally run only on Windows; cross-compilation is not acceptance.

use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, ActorSnapshotError as Error, ActorSnapshotLimits,
    ActorSnapshotPort, ActorSnapshotPrincipal, Session, Storage,
};
use std::ffi::OsStr;
use std::io::{Seek, Write};
use std::os::windows::fs::symlink_file;
use std::process::Command;

#[tokio::test]
async fn nested_snapshot_is_private_read_only_and_stable_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    let mut root = Session::new("windows-snapshot-root", "PRIVATE-MODEL");
    root.title = "Visible root".into();
    let parent = Session::new_child_of("windows-parent", &root, "PRIVATE-MODEL", "Parent");
    let child = Session::new_child_of("windows-child", &parent, "PRIVATE-MODEL", "Child");
    store.save_session(&root).await.unwrap();
    store.save_session(&parent).await.unwrap();
    store.save_session(&child).await.unwrap();
    let source = home.join("sessions/windows-snapshot-root/session.json");
    let original = std::fs::read(&source).unwrap();
    let first = store
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root.id,
            &root.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(first.nodes.len(), 3);
    assert_eq!(
        first
            .nodes
            .iter()
            .map(|node| node.depth)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(first.nodes.iter().all(|node| node.logical_state.is_none()));
    assert!(!serde_json::to_string(&first).unwrap().contains("PRIVATE"));
    assert_eq!(std::fs::read(&source).unwrap(), original);
    assert!(!home
        .join("sessions/windows-snapshot-root/actor-authority.json")
        .exists());
    let reopened = SessionStoreV2::new(home.clone()).await.unwrap();
    let again = reopened
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root.id,
            &root.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(first, again);
    let selected = reopened
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root.id,
            &parent.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(selected.nodes.len(), 2);
    assert_eq!(
        reopened
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root.id,
                "foreign-child",
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap_err(),
        Error::NotFound
    );
}

#[tokio::test]
async fn stale_identity_and_tight_budgets_fail_without_partial_tree() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    let root = Session::new("windows-stale-root", "model");
    let child = Session::new_child_of("windows-stale-child", &root, "model", "Child");
    let sibling = Session::new_child_of("windows-stale-sibling", &root, "model", "Child");
    store.save_session(&root).await.unwrap();
    store.save_session(&child).await.unwrap();
    store.save_session(&sibling).await.unwrap();
    let snapshot = |limits| {
        store.actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root.id,
            &root.id,
            limits,
        )
    };
    let mut limits = ActorSnapshotLimits::default();
    limits.nodes = 1;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let mut limits = ActorSnapshotLimits::default();
    limits.file_reads = 1;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let mut limits = ActorSnapshotLimits::default();
    limits.directory_entries = 1;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let mut limits = ActorSnapshotLimits::default();
    limits.file_bytes = 8;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let mut limits = ActorSnapshotLimits::default();
    limits.aggregate_read_bytes = 8;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let mut limits = ActorSnapshotLimits::default();
    limits.response_bytes = 8;
    assert_eq!(snapshot(limits).await.unwrap_err(), Error::BudgetExceeded);
    let sidecar = home.join("sessions/windows-stale-root/runtime.json");
    let mut side: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
    side["id"] = "foreign-root".into();
    std::fs::write(&sidecar, serde_json::to_vec(&side).unwrap()).unwrap();
    assert_eq!(
        snapshot(ActorSnapshotLimits::default()).await.unwrap_err(),
        Error::StaleAuthority
    );
}

#[test]
fn retained_directory_and_file_handles_reject_reparse_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let outside = temp.path().join("outside");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let home = home.canonicalize().unwrap();
    let outside = outside.canonicalize().unwrap();
    let inside_path = home.join("inside");
    std::fs::create_dir(&inside_path).unwrap();
    std::fs::write(inside_path.join("evidence"), b"retained").unwrap();
    std::fs::write(outside.join("evidence"), b"foreign").unwrap();
    let root = actor_snapshot_reader::Directory::open_absolute(&home).unwrap();
    assert_eq!(
        root.child(OsStr::new("..")).err(),
        Some(Error::InconsistentAuthority)
    );
    let inside = root.child(OsStr::new("inside")).unwrap().unwrap();
    std::fs::rename(&inside_path, home.join("saved")).unwrap();
    junction(&inside_path, &outside);
    assert_eq!(
        root.child(OsStr::new("inside")).err(),
        Some(Error::InconsistentAuthority)
    );
    let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
    assert_eq!(
        inside.read("evidence", 16, &mut budget).unwrap().unwrap(),
        b"retained"
    );
    let saved_file = home.join("saved/evidence");
    std::fs::remove_file(&saved_file).unwrap();
    junction(&saved_file, &outside);
    assert_eq!(
        inside.read("evidence", 16, &mut budget).err(),
        Some(Error::InconsistentAuthority)
    );
    std::fs::remove_dir(&saved_file).unwrap();
    std::fs::write(&saved_file, b"123456789").unwrap();
    assert_eq!(
        inside.read("evidence", 8, &mut budget).err(),
        Some(Error::BudgetExceeded)
    );
}

#[tokio::test]
async fn live_actor_fence_authorizes_self_and_descendants_but_not_siblings_or_foreign_roots() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
    let root = Session::new("windows-live-root", "PRIVATE-MODEL");
    let parent = Session::new_child_of("windows-live-parent", &root, "PRIVATE-MODEL", "Parent");
    let child = Session::new_child_of("windows-live-child", &parent, "PRIVATE-MODEL", "Child");
    let sibling = Session::new_child_of("windows-live-sibling", &root, "PRIVATE-MODEL", "Sibling");
    for session in [&root, &parent, &child, &sibling] {
        store.save_session(session).await.unwrap();
    }
    store.flush_search_index().await;
    let now = Utc::now();
    let activation = store
        .claim_activation(&ActorActivationClaim {
            actor_id: parent.id.clone(),
            run_id: "PRIVATE-RUN".into(),
            lease_owner: "PRIVATE-HOST".into(),
            lease_expires_at: now + chrono::Duration::minutes(5),
            inbox_generation: 42,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    let read = |root: String, subtree: String, fence: bamboo_domain::ActorActivationFence| {
        let store = store.clone();
        let home = home.clone();
        async move {
            let before = durable_files(&home);
            let result = store
                .actor_subtree_snapshot(
                    ActorSnapshotPrincipal::live_actor(fence),
                    &root,
                    &subtree,
                    ActorSnapshotLimits::default(),
                )
                .await;
            store.flush_search_index().await;
            assert_eq!(durable_files(&home), before);
            result
        }
    };
    for (subtree, expected) in [
        (&parent.id, vec![parent.id.as_str(), child.id.as_str()]),
        (&child.id, vec![child.id.as_str()]),
    ] {
        let snapshot = read(root.id.clone(), subtree.clone(), activation.fence())
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .nodes
                .iter()
                .map(|node| node.actor_id.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains("PRIVATE"));
    }
    for subtree in [&root.id, &sibling.id] {
        assert_eq!(
            read(root.id.clone(), subtree.clone(), activation.fence())
                .await
                .unwrap_err(),
            Error::UnauthorizedScope
        );
    }
    let foreign = Session::new("windows-foreign-root", "PRIVATE-MODEL");
    store.save_session(&foreign).await.unwrap();
    store.flush_search_index().await;
    // Reject foreign-root scope before trying to parse its private, corrupt body.
    std::fs::write(
        home.join("sessions/windows-foreign-root/session.json"),
        b"PRIVATE corrupt body",
    )
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
            read(root.id.clone(), parent.id.clone(), fence)
                .await
                .unwrap_err(),
            Error::UnauthorizedScope
        );
    }
    let authority =
        home.join("sessions/windows-live-root/children/windows-live-parent/actor-authority.json");
    let mut row: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&authority).unwrap()).unwrap();
    row["activation"]["lease_expires_at"] =
        serde_json::json!(Utc::now() - chrono::Duration::seconds(1));
    std::fs::write(&authority, serde_json::to_vec(&row).unwrap()).unwrap();
    assert_eq!(
        read(root.id.clone(), parent.id.clone(), activation.fence())
            .await
            .unwrap_err(),
        Error::UnauthorizedScope
    );
}

#[tokio::test]
async fn regular_file_symlink_replacement_is_rejected_and_open_handle_retains_original() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    let root = Session::new("windows-symlink-root", "PRIVATE-MODEL");
    store.save_session(&root).await.unwrap();
    let directory = home.join("sessions/windows-symlink-root");
    let path = directory.join("session.json");
    let original = std::fs::read(&path).unwrap();
    let retained = std::fs::File::open(&path).unwrap();
    let reader = actor_snapshot_reader::Directory::open_absolute(&directory).unwrap();
    std::fs::rename(&path, directory.join("retained.json")).unwrap();
    let outside = home.join("owned-outside.json");
    std::fs::write(&outside, b"PRIVATE foreign replacement").unwrap();
    // Missing Windows symlink capability is an explicit failure, never a skip.
    symlink_file(&outside, &path)
        .expect("native Windows regular-file symlink creation is required");
    assert!(std::fs::symlink_metadata(&path)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(std::fs::metadata(&outside).unwrap().is_file());
    assert_eq!(std::fs::read_link(&path).unwrap(), outside);
    let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits::default());
    assert_eq!(
        reader
            .read("session.json", 512 * 1024, &mut budget)
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    assert_eq!(
        actor_snapshot_reader::read_content(retained, original.len(), &mut budget).unwrap(),
        original
    );
    assert_eq!(
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root.id,
                &root.id,
                ActorSnapshotLimits::default()
            )
            .await
            .unwrap_err(),
        Error::InconsistentAuthority
    );
    assert_eq!(
        std::fs::read(&outside).unwrap(),
        b"PRIVATE foreign replacement"
    );
}

#[test]
fn grow_after_open_is_capped_and_actual_bytes_debit_the_aggregate_budget() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("growing");
    let earlier = temp.path().join("earlier");
    std::fs::write(&earlier, b"paid").unwrap();
    let empty = temp.path().join("empty");
    std::fs::write(&empty, b"").unwrap();
    for aggregate_limit in [12, 32] {
        std::fs::write(&path, b"small").unwrap();
        let retained = std::fs::File::open(&path).unwrap();
        let mut observed = retained.try_clone().unwrap();
        assert_eq!(retained.metadata().unwrap().len(), 5);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[b'x'; 100])
            .unwrap();
        let mut budget = actor_snapshot_reader::ReadBudget::new(ActorSnapshotLimits {
            aggregate_read_bytes: aggregate_limit,
            ..ActorSnapshotLimits::default()
        });
        assert_eq!(
            actor_snapshot_reader::read_content(
                std::fs::File::open(&earlier).unwrap(),
                4,
                &mut budget
            )
            .unwrap(),
            b"paid"
        );
        assert_eq!(
            actor_snapshot_reader::read_content(retained, 8, &mut budget).unwrap_err(),
            Error::BudgetExceeded
        );
        // A cloned Windows handle shares the original cursor. Exactly max + 1
        // bytes were consumed, rather than trusting the stale five-byte stat.
        assert_eq!(observed.stream_position().unwrap(), 9);
        if aggregate_limit == 12 {
            assert_eq!(
                actor_snapshot_reader::read_content(
                    std::fs::File::open(&empty).unwrap(),
                    0,
                    &mut budget
                )
                .unwrap_err(),
                Error::BudgetExceeded
            );
        } else {
            // This exact fill proves that even rejected reads debit all bytes.
            let remaining = aggregate_limit - (4 + 9);
            std::fs::write(temp.path().join("tail"), vec![b't'; remaining]).unwrap();
            assert_eq!(
                actor_snapshot_reader::read_content(
                    std::fs::File::open(temp.path().join("tail")).unwrap(),
                    remaining,
                    &mut budget
                )
                .unwrap()
                .len(),
                remaining
            );
            assert_eq!(
                actor_snapshot_reader::read_content(
                    std::fs::File::open(&earlier).unwrap(),
                    4,
                    &mut budget
                )
                .unwrap_err(),
                Error::BudgetExceeded
            );
        }
    }
}

fn durable_files(home: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn walk(directory: &std::path::Path, files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                files.insert(path.clone(), vec![]);
                walk(&path, files);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                files.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    let mut files = Default::default();
    walk(home, &mut files);
    files
}

fn junction(link: &std::path::Path, target: &std::path::Path) {
    let output = Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(link)
        .arg(target)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "junction setup failed: {:?}",
        output
    );
}
