//! Node exit alone must not release resources of an unconfirmed writer domain.
use super::*;

const UNKNOWN_WRITERS: &str = ".bamboo-profile-writer-exit-unconfirmed";

#[tokio::test]
async fn managed_group_barrier_refuses_live_members_and_accepts_only_reaped_empty_group() {
    assert!(!managed_group_exit_confirmed(true, None));
    assert!(!managed_group_exit_confirmed(
        true,
        Some(unsafe { libc::getpgrp() })
    ));
    let mut command = Command::new("/bin/sleep");
    command.arg("5").kill_on_drop(true).process_group(0);
    let mut child = command.spawn().unwrap();
    let group = i32::try_from(child.id().unwrap()).unwrap();
    assert!(!managed_group_exit_confirmed(true, Some(group)));
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    assert!(managed_group_exit_confirmed(true, Some(group)));
}

#[tokio::test]
async fn clean_node_exit_with_a_remaining_group_member_retains_active_fence_and_scratch() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join("profiles");
    let mut lease = profile::acquire_shared(&root).unwrap();
    lease.begin_launch().unwrap();
    lease.begin_close().unwrap();
    let path = lease.path.clone();
    let scratch = tempfile::Builder::new().tempdir_in(home.path()).unwrap();
    let scratch_path = scratch.path().to_path_buf();
    std::fs::write(scratch_path.join("sentinel"), b"synthetic").unwrap();
    // This controlled child stands in for Node: it exits 0 while a synthetic
    // member remains in its own group. That member only sleeps and naturally
    // exits; it never writes a profile or any user data.
    let mut command = Command::new("/usr/bin/python3");
    command
        .arg("-c")
        .arg("import os,time\nif os.fork() == 0:\n time.sleep(2)\n os._exit(0)\nos._exit(0)")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .process_group(0);
    let mut child = command.spawn().unwrap();
    let group = i32::try_from(child.id().unwrap()).unwrap();
    assert!(child.wait().await.unwrap().success());
    assert!(!managed_group_exit_confirmed(true, Some(group)));
    let cleanup = Arc::new(BrowserCleanup {
        session: Mutex::new(Weak::new()),
        done: watch::channel(false).0,
        quarantined: Mutex::new(Vec::new()),
        unconfirmed_writers: Arc::new(AtomicBool::new(false)),
        writer_admission: Arc::new(Mutex::new(())),
    });
    BrowserReaper {
        native_launch_unadmitted: false,
        child: Arc::new(Mutex::new(child)),
        cleanup: cleanup.clone(),
        lease: Some(lease),
        temp_dir: Some(scratch),
        stdin: None,
        process_group: Some(group),
        group_exit_required: true,
        graceful: true,
        marked: true,
        clean_host_exit: false,
    }
    .reap();
    assert!(cleanup.is_quarantined());
    assert!(path.join(profile::LAUNCH_ACTIVE).is_file());
    assert!(profile_lock_is_held(&path));
    assert_eq!(
        std::fs::read(scratch_path.join("sentinel")).unwrap(),
        b"synthetic"
    );
    // Let the non-writing fixture finish naturally before dropping temp data.
    tokio::time::sleep(Duration::from_millis(2100)).await;
}

fn fixture(
    home: &std::path::Path,
    lifecycle: &BrowserLifecycle,
    executable: &str,
) -> (Arc<BrowserSession>, PathBuf, PathBuf) {
    let root = home.join("profiles");
    let lease = profile::acquire(&root, "writer-domain-chat").unwrap();
    let profile_path = lease.path.clone();
    let scratch = tempfile::Builder::new()
        .prefix("scratch-")
        .tempdir_in(home)
        .unwrap();
    let scratch_path = scratch.path().to_path_buf();
    std::fs::write(scratch_path.join("synthetic-sentinel"), b"owned test data").unwrap();
    let mut command = Command::new(executable);
    if executable == "/bin/sleep" {
        command.arg("5");
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command.process_group(0);
    let child = command.spawn().unwrap();
    // Deliberately use the production constructor. This fixture supplies no
    // proof about native descendants, even though its own child is synthetic.
    let session = BrowserSession::from_child_tracked(child, Some(scratch), lifecycle).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    (session, profile_path, scratch_path)
}

fn profile_lock_is_held(profile: &std::path::Path) -> bool {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(profile.join(".bamboo-profile.lock"))
        .unwrap();
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
    assert!(
        profile_lock_is_held(&profile),
        "Node reaping must not release a native-writer lease"
    );
    assert!(
        scratch.join("synthetic-sentinel").is_file(),
        "unknown writers may still own scratch"
    );
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
    tokio::time::timeout(Duration::from_secs(4), cleanup.wait())
        .await
        .unwrap();
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
    std::fs::write(
        path.join(UNKNOWN_WRITERS),
        b"synthetic unknown writer generation\n",
    )
    .unwrap();
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
    assert!(
        !reopened_allowed && !reset_allowed && !deletion_allowed,
        "a durable unknown-writer fence must survive the old owner process"
    );
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

#[tokio::test]
async fn unknown_writer_marker_failure_still_fences_manager_admission() {
    let home = tempfile::tempdir().unwrap();
    let lifecycle = BrowserLifecycle::default();
    let (session, profile, scratch) = fixture(home.path(), &lifecycle, "/bin/sleep");
    let invalid_marker = profile.join(UNKNOWN_WRITERS);
    std::fs::create_dir(&invalid_marker).unwrap();
    session.signal_stop();
    session.terminate("synthetic marker failure").await;
    let blocked_before_io_recovery = lifecycle.unconfirmed_writers.load(Ordering::Acquire);
    let lease_retained = profile_lock_is_held(&profile);
    let scratch_retained = scratch.join("synthetic-sentinel").is_file();
    let completion_was_not_published = !*session.cleanup.done.borrow();
    // Repair only this synthetic obstruction before asserting, including RED.
    // Otherwise Drop correctly waits forever for a durable-fence I/O recovery.
    std::fs::remove_dir(invalid_marker).unwrap();
    session.quarantine_resources();
    assert!(
        blocked_before_io_recovery,
        "unknown writers must fence admission before marker I/O"
    );
    assert!(lease_retained && scratch_retained && completion_was_not_published);
}

#[test]
fn queued_open_cannot_launch_after_unknown_writer_fence() {
    const DATA: &str = "BAMBOO_BROWSER_WRITER_ADMISSION_FIXTURE";
    if let Some(home) = std::env::var_os(DATA).map(PathBuf::from) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let browser = BrowserManager::with_fixture_profile_root(home.join("profiles"));
            let gate = browser.session_gate("queued-chat");
            let held_gate = gate.lock().await;
            let mut opening = Box::pin(browser.open("queued-chat"));
            // The first poll passes the initial admission check and then waits
            // at the actual per-chat gate, before any host can be spawned.
            assert!(futures::poll!(opening.as_mut()).is_pending());
            let (session, _, _) = fixture(&home, &browser.lifecycle, "/bin/sleep");
            session.signal_stop();
            session.terminate("synthetic unknown writer").await;
            drop(held_gate);
            let result = opening.await;
            // Observe this fixture's actual account profile as well as a
            // legacy profile; the shared runtime no longer uses a chat key.
            let launched = std::fs::read_dir(home.join("profiles"))
                .unwrap()
                .any(|entry| {
                    let entry = entry.unwrap();
                    entry.file_type().unwrap().is_dir()
                        && entry
                            .path()
                            .join("synthetic-launched")
                            .try_exists()
                            .unwrap()
                });
            // Drain the synthetic EOF host on RED before checking the result.
            browser.shutdown().await;
            assert!(matches!(result, Err(BrowserError::Unavailable(ref message))
                if message == "browser writer exit unconfirmed"));
            assert!(
                !launched,
                "a queued open must not launch a new child after the fence"
            );
        });
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let script = home.path().join("synthetic-host.py");
    std::fs::write(
        &script,
        r#"import json, os, pathlib, sys
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
(profile / 'synthetic-launched').write_text('synthetic only')
for line in sys.stdin:
    request = json.loads(line)
    print(json.dumps({'id': request['id'], 'ok': True, 'result': {'tabs': []}}), flush=True)
"#,
    )
    .unwrap();
    let log_path = home.path().join("fixture.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "browser::writer_lifecycle_tests::queued_open_cannot_launch_after_unknown_writer_fence",
            "--exact",
            "--test-threads=1",
        ])
        .env(DATA, home.path())
        .env(
            "BAMBOO_BROWSER_NODE",
            std::env::var_os("BAMBOO_BROWSER_TEST_PYTHON")
                .unwrap_or_else(|| "/usr/bin/python3".into()),
        )
        .env("BAMBOO_BROWSER_HOST_SCRIPT", script)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .status()
        .unwrap();
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(log_path).unwrap()
    );
}
