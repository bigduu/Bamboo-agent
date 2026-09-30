//! Native Windows coverage for the handle-bound snapshot reader. These tests
//! intentionally run only on Windows; cross-compilation is not acceptance.

use super::*;
use bamboo_domain::{
    ActorSnapshotError as Error, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    Session, Storage,
};
use std::ffi::OsStr;
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
