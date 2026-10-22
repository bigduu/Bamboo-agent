//! Node exit alone must not release resources of an unconfirmed writer domain.
use super::*;

const UNKNOWN_WRITERS: &str = ".bamboo-profile-writer-exit-unconfirmed";

fn fixture(
    home: &std::path::Path,
    lifecycle: &BrowserLifecycle,
    executable: &str,
) -> (Arc<BrowserSession>, PathBuf, PathBuf) {
    let root = home.join("profiles");
    let lease = profile::acquire(&root, "writer-domain-chat").unwrap();
    let profile_path = lease.path.clone();
    let scratch = tempfile::Builder::new().prefix("scratch-").tempdir_in(home).unwrap();
    let scratch_path = scratch.path().to_path_buf();
    std::fs::write(scratch_path.join("synthetic-sentinel"), b"owned test data").unwrap();
    let mut command = Command::new(executable);
    if executable == "/bin/sleep" {
        command.arg("5");
    }
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).kill_on_drop(true);
    command.process_group(0);
    let child = command.spawn().unwrap();
    // Deliberately use the production constructor. This fixture supplies no
    // proof about native descendants, even though its own child is synthetic.
    let session = BrowserSession::from_child_tracked(child, Some(scratch), lifecycle).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    (session, profile_path, scratch_path)
}

fn profile_lock_is_held(profile: &std::path::Path) -> bool {
    let file = std::fs::OpenOptions::new().read(true).write(true)
        .open(profile.join(".bamboo-profile.lock")).unwrap();
    fs2::FileExt::try_lock_exclusive(&file).is_err()
}

#[tokio::test]
async fn forced_host_exit_preserves_the_unknown_writer_lease_and_scratch() {
    let home = tempfile::tempdir().unwrap();
    let lifecycle = BrowserLifecycle::default();
    let (session, profile, scratch) = fixture(home.path(), &lifecycle, "/bin/sleep");
    session.signal_stop();
    session.terminate("synthetic forced host exit").await;
    assert!(session.child.lock().unwrap().try_wait().unwrap().is_some());
    assert!(profile_lock_is_held(&profile), "Node reaping must not release a native-writer lease");
    assert!(scratch.join("synthetic-sentinel").is_file(), "unknown writers may still own scratch");
    assert!(profile.join(UNKNOWN_WRITERS).is_file());
}

#[tokio::test]
async fn drop_reaper_preserves_unknown_writer_resources_after_reaping_node() {
    let home = tempfile::tempdir().unwrap();
    let lifecycle = BrowserLifecycle::default();
    let (session, profile, scratch) = fixture(home.path(), &lifecycle, "/bin/sleep");
    let child = session.child.clone();
    let cleanup = session.cleanup.clone();
    // A previous signal flag is not a writer-exit proof. Leave the child alive
    // until the actual Drop reaper takes ownership, avoiding an EOF/Arc race.
    session.kill_issued.store(true, Ordering::Release);
    drop(session);
    tokio::time::timeout(Duration::from_secs(4), cleanup.wait()).await.unwrap();
    assert!(child.lock().unwrap().try_wait().unwrap().is_some());
    assert!(profile_lock_is_held(&profile));
    assert!(scratch.join("synthetic-sentinel").is_file());
    assert!(profile.join(UNKNOWN_WRITERS).is_file());
}

#[tokio::test]
async fn durable_unknown_writer_fence_blocks_reopen_delete_and_reset() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join("profiles");
    let owner = profile::acquire(&root, "writer-domain-chat").unwrap();
    let path = owner.path.clone();
    std::fs::write(path.join(UNKNOWN_WRITERS), b"synthetic unknown writer generation\n").unwrap();
    let marker = path.join("synthetic-sentinel");
    std::fs::write(&marker, b"preserve this state").unwrap();
    drop(owner); // Simulates loss of process-local file descriptors on restart.
    let reopened = profile::acquire(&root, "writer-domain-chat");
    let reopened_allowed = reopened.is_ok();
    drop(reopened);
    let reset = profile::begin_reset(&root);
    let reset_allowed = reset.is_ok();
    drop(reset);
    let deletion_allowed = profile::remove(&root, "writer-domain-chat").is_ok();
    assert!(!reopened_allowed && !reset_allowed && !deletion_allowed,
        "a durable unknown-writer fence must survive the old owner process");
    assert_eq!(std::fs::read(marker).unwrap(), b"preserve this state");
}

#[tokio::test]
async fn ordinary_fulfilled_host_close_keeps_the_existing_cleanup_contract() {
    let home = tempfile::tempdir().unwrap();
    let lifecycle = BrowserLifecycle::default();
    let (session, profile, scratch) = fixture(home.path(), &lifecycle, "/bin/cat");
    session.begin_retirement("synthetic normal EOF").unwrap();
    session.terminate("synthetic normal EOF").await;
    assert!(session.close_confirmed.load(Ordering::Acquire));
    assert!(!profile_lock_is_held(&profile));
    assert!(!scratch.exists());
    assert!(profile::acquire(&home.path().join("profiles"), "writer-domain-chat").is_ok());
}
