//! Regression fixtures own every process, profile and HTTP endpoint they use.

use super::*;

#[cfg(unix)]
#[tokio::test]
async fn clean_reaper_holds_lease_until_close_directory_barrier_recovers() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let lease = profile::acquire(&root, "chat").unwrap();
    let path = lease.path.clone();
    lease.begin_close().unwrap();
    let marker = path.join(".bamboo-profile-close-pending");
    let competing = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join(".bamboo-profile.lock"))
        .unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("cat >/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true);
    command.process_group(0);
    let mut child = command.spawn().unwrap();
    let process_group = child.id().and_then(|id| i32::try_from(id).ok());
    let stdin = child.stdin.take();
    let reaper = BrowserReaper {
        cleanup: None,
        child: Arc::new(Mutex::new(child)),
        lease: Some(lease),
        temp_dir: None,
        stdin,
        process_group,
        graceful: true,
        marked: true,
    };
    // The fence was established before the clean host exit. Unlink can now
    // succeed, while opening the directory to confirm it must fail.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o300)).unwrap();
    let (finished, receiver) = std::sync::mpsc::channel();
    let owner = std::thread::spawn(move || {
        reaper.reap();
        finished.send(()).unwrap();
    });
    let started = Instant::now();
    while marker.exists() && started.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(5));
    }
    let marker_unlinked = !marker.exists();
    let released_before_barrier = receiver.recv_timeout(Duration::from_millis(300)).is_ok();
    let still_leased = fs2::FileExt::try_lock_exclusive(&competing).is_err();
    // Restore permissions and join before assertions, including on RED.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    owner.join().unwrap();
    assert!(
        marker_unlinked,
        "fixture must reach unlink before the barrier fails"
    );
    assert!(
        !released_before_barrier,
        "a clean reaper must wait for confirmation of the directory barrier"
    );
    assert!(
        still_leased,
        "an unconfirmed unlink must retain its profile lease"
    );
    assert!(fs2::FileExt::try_lock_exclusive(&competing).is_ok());
    drop(competing);
    assert!(profile::acquire(&root, "chat").is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn marker_io_failure_keeps_the_live_owner_until_close_can_be_fenced() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let lease = profile::acquire(&root, "chat").unwrap();
    let path = lease.path.clone();
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("cat >/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command.process_group(0);
    let session = BrowserSession::from_child(command.spawn().unwrap(), None).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    let manager = BrowserManager::with_profile_root(root.clone());
    manager
        .sessions
        .lock()
        .await
        .insert("chat".into(), session.clone());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = manager.close("chat").await;
    let preserved =
        session.alive.load(Ordering::Acquire) && manager.sessions.lock().await.contains_key("chat");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    // Restore fixture permissions before asserting, including on a RED build.
    manager.close("chat").await.unwrap();
    assert!(result.is_err());
    assert!(
        preserved,
        "failed marker creation must not detach a live profile owner"
    );
    assert!(profile::acquire(&root, "chat").is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn marker_io_failure_keeps_drop_and_the_lease_until_a_durable_fence_exists() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let lease = profile::acquire(&root, "chat").unwrap();
    let path = lease.path.clone();
    let competing = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join(".bamboo-profile.lock"))
        .unwrap();
    let mut command = Command::new("/bin/sleep");
    command
        .arg("60")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(false);
    command.process_group(0);
    let child = command.spawn().unwrap();
    let pid = child.id().unwrap() as i32;
    let session = BrowserSession::from_child(child, None).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    session.kill_issued.store(true, Ordering::Release);
    let manager = BrowserManager::with_profile_root(root.clone());
    manager.sessions.lock().await.insert("chat".into(), session);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o500)).unwrap();
    let (finished, receiver) = std::sync::mpsc::channel();
    let owner = std::thread::spawn(move || {
        drop(manager);
        finished.send(()).unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let drop_pending = receiver.try_recv().is_err();
    let still_leased = fs2::FileExt::try_lock_exclusive(&competing).is_err();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    // Clean the RED fixture too, without touching any non-fixture process.
    kill_browser_process_group(pid);
    owner.join().unwrap();
    assert!(
        drop_pending,
        "Drop cannot return before creating its durable replay fence"
    );
    assert!(
        still_leased,
        "an I/O failure must not release an unmarked profile"
    );
    assert!(fs2::FileExt::try_lock_exclusive(&competing).is_ok());
    drop(competing);
    assert!(profile::acquire(&root, "chat").is_err());
}

#[cfg(unix)]
#[test]
fn profile_reaper_can_finish_after_the_async_runtime_has_stopped() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (manager, pid, scratch) = runtime.block_on(async {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("cat >/dev/null; sleep 0.15")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        command.process_group(0);
        let child = command.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        let scratch = tempfile::tempdir().unwrap();
        let scratch_path = scratch.path().to_path_buf();
        let session = BrowserSession::from_child(child, Some(scratch)).unwrap();
        *session.profile_lease.lock().unwrap() = Some(profile::acquire(&root, "chat").unwrap());
        let manager = BrowserManager::with_profile_root(root.clone());
        manager.sessions.lock().await.insert("chat".into(), session);
        (manager, pid, scratch_path)
    });
    drop(runtime);
    drop(manager);
    assert!(profile::acquire(&root, "chat").is_err());
    let started = Instant::now();
    loop {
        if let Ok(lease) = profile::acquire(&root, "chat") {
            assert_ne!(unsafe { libc::kill(pid, 0) }, 0);
            assert!(
                !scratch.exists(),
                "scratch cleanup must finish while the lease is still held"
            );
            drop(lease);
            break;
        }
        if started.elapsed() >= Duration::from_secs(7) {
            kill_browser_process_group(pid);
            panic!("reaper did not release a cleanly closed profile after runtime shutdown");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unconfirmed_profile_close_blocks_replay_and_discards_queued_writes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let late_marker = directory.path().join("late-action");
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("IFS= read -r request; : > \"$LATE_MARKER\"; sleep 60")
        .env("LATE_MARKER", &late_marker)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command.process_group(0);
    let session = BrowserSession::from_child(command.spawn().unwrap(), None).unwrap();
    let lease = profile::acquire(&root, "chat").unwrap();
    let checkpoint = lease.path.join(".bamboo-session-cookies-v1.json");
    std::fs::write(&checkpoint, b"synthetic previous checkpoint").unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    let manager = BrowserManager::with_profile_root(root.clone());
    manager
        .sessions
        .lock()
        .await
        .insert("chat".into(), session.clone());
    let stdin = session.stdin.lock().await;
    let queued = {
        let manager = manager.clone();
        tokio::spawn(async move {
            manager
                .command("chat", "input", json!({"text":"synthetic"}))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while session.pending.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        assert!(manager.close("chat").await.is_err());
    })
    .await
    .expect("closing must not wait indefinitely for a blocked write end");
    drop(stdin);
    assert!(queued.await.unwrap().is_err());
    assert!(!late_marker.exists());
    assert!(session.reaped.load(Ordering::Acquire));
    assert!(
        profile::acquire(&root, "chat").is_err(),
        "an unconfirmed close must not replay a stale cookie checkpoint"
    );
    assert_eq!(
        std::fs::read(checkpoint).unwrap(),
        b"synthetic previous checkpoint"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn nonzero_host_exit_cannot_claim_a_confirmed_profile_close() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("cat >/dev/null; exit 23")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command.process_group(0);
    let session = BrowserSession::from_child(command.spawn().unwrap(), None).unwrap();
    *session.profile_lease.lock().unwrap() = Some(profile::acquire(&root, "chat").unwrap());
    let manager = BrowserManager::with_profile_root(root.clone());
    manager
        .sessions
        .lock()
        .await
        .insert("chat".into(), session.clone());
    assert!(manager.close("chat").await.is_err());
    assert!(session.reaped.load(Ordering::Acquire));
    assert!(session.profile_lease.lock().unwrap().is_none());
    assert!(profile::acquire(&root, "chat").is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_manager_keeps_lease_until_a_previously_signalled_child_is_reaped() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let lease = profile::acquire(&root, "drop-chat").unwrap();
    let lock_path = lease.path.join(".bamboo-profile.lock");
    let mut command = Command::new("/bin/sleep");
    command
        .arg("60")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    command.process_group(0).kill_on_drop(false);
    let child = command.spawn().unwrap();
    let pid = child.id().unwrap() as i32;
    let session = BrowserSession::from_child(child, None).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    // Model a stop that has been issued but whose exit has not been observed.
    // A signal flag alone is never evidence that releasing the lease is safe.
    session.kill_issued.store(true, Ordering::Release);
    let manager = BrowserManager::with_profile_root(root);
    manager
        .sessions
        .lock()
        .await
        .insert("drop-chat".into(), session);
    drop(manager);

    let competing = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    let acquired = fs2::FileExt::try_lock_exclusive(&competing).is_ok();
    let still_alive = unsafe { libc::kill(pid, 0) } == 0;
    // Clean only this fixture's process group, even when the assertion fails.
    kill_browser_process_group(pid);
    assert!(
        !acquired || !still_alive,
        "profile lease escaped while its old host was still alive"
    );
    drop(competing);
    tokio::time::timeout(Duration::from_secs(4), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned host must eventually be reaped");
}

#[cfg(unix)]
struct LogoutSite {
    url: String,
    release: Arc<tokio::sync::Notify>,
    observed: Arc<tokio::sync::Notify>,
    logout_cookie_absent: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(unix)]
impl Drop for LogoutSite {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(unix)]
impl LogoutSite {
    async fn start() -> Self {
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let release = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::new(tokio::sync::Notify::new());
        let logout_cookie_absent = Arc::new(AtomicBool::new(false));
        let gate = release.clone();
        let notification = observed.clone();
        let cookie_absent = logout_cookie_absent.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let gate = gate.clone();
                let notification = notification.clone();
                let cookie_absent = cookie_absent.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut chunk = [0; 2048];
                    loop {
                        let Ok(size) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if size == 0 {
                            return;
                        }
                        request.extend_from_slice(&chunk[..size]);
                        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            break;
                        }
                        if request.len() > 16384 {
                            return;
                        }
                    }
                    let request = String::from_utf8_lossy(&request);
                    let endpoint = request.split_whitespace().nth(1).unwrap_or("");
                    let present = request.lines().any(|line| {
                        line.to_ascii_lowercase().starts_with("cookie:")
                            && line.contains("fixture-session=synthetic")
                    });
                    if endpoint == "/gate" {
                        gate.notified().await;
                    }
                    let cookie = match endpoint {
                        "/seed" => "Set-Cookie: fixture-session=synthetic; HttpOnly; SameSite=Strict; Path=/\r\n",
                        "/logout" => "Set-Cookie: fixture-session=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/\r\n",
                        _ => "",
                    };
                    if endpoint == "/observed" {
                        cookie_absent.store(!present, Ordering::Release);
                        notification.notify_one();
                    }
                    let body = if endpoint == "/probe" {
                        present.to_string()
                    } else {
                        "<!doctype html><title>Synthetic logout fixture</title>".into()
                    };
                    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n{cookie}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            url,
            release,
            observed,
            logout_cookie_absent,
            task,
        }
    }
}

#[cfg(unix)]
async fn prepare_immediate_logout() -> (
    LogoutSite,
    tempfile::TempDir,
    PathBuf,
    BrowserManager,
    PathBuf,
) {
    let site = LogoutSite::start().await;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("profiles");
    let manager = BrowserManager::with_profile_root(root.clone());
    let initial = manager.open("logout-chat").await.unwrap();
    let page = manager
        .command(
            "logout-chat",
            "navigate",
            json!({
                "expected_epoch":initial["page_epoch"], "url":format!("{}seed", site.url)
            }),
        )
        .await
        .unwrap();
    assert!(
        manager
            .frame("logout-chat", 0, 2000)
            .await
            .unwrap()
            .is_some(),
        "fixture must have an active output stream before owner loss"
    );
    // The gate opens only AFTER the last successful command's checkpoint.
    // No host command follows the HTTP-only cookie deletion before retirement.
    manager.eval("logout-chat", json!({
        "expected_epoch":page["page_epoch"], "expected_url":format!("{}seed", site.url),
        "code":"void fetch('/gate').then(() => fetch('/logout')).then(() => fetch('/observed')); true"
    })).await.unwrap();
    let checkpoint = profile::directory(&root, "logout-chat")
        .unwrap()
        .join(".bamboo-session-cookies-v1.json");
    site.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), site.observed.notified())
        .await
        .unwrap();
    assert!(
        site.logout_cookie_absent.load(Ordering::Acquire),
        "site must first observe the cookie was actually cleared"
    );
    let saved: Value = serde_json::from_slice(&std::fs::read(&checkpoint).unwrap()).unwrap();
    assert!(saved["cookies"].as_array().unwrap().iter().any(|cookie| cookie["name"] == "fixture-session"),
        "fixture must retire before the background checkpoint; otherwise it does not exercise the gap");
    (site, directory, root, manager, checkpoint)
}

#[cfg(unix)]
async fn immediate_logout_then_retire(mode: &str) {
    let (site, _directory, root, manager, _checkpoint) = prepare_immediate_logout().await;
    match mode {
        "close" => manager.close("logout-chat").await.unwrap(),
        "idle" => {
            *manager.sessions.lock().await["logout-chat"]
                .last_used
                .lock()
                .unwrap() = Instant::now() - BROWSER_IDLE_TTL;
            assert_eq!(manager.sweep_idle(BROWSER_IDLE_TTL).await, 1);
        }
        "drop" => {}
        _ => panic!("unknown fixture mode"),
    }
    drop(manager);
    let reopened = BrowserManager::with_profile_root(root.clone());
    let initial = tokio::time::timeout(BROWSER_CLOSE_DEADLINE + Duration::from_secs(5), async {
        loop {
            match reopened.open("logout-chat").await {
                Ok(state) => break state,
                Err(BrowserError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("unexpected reopen error: {error}"),
            }
        }
    })
    .await
    .expect("controlled retirement should finish and release the profile");
    let page = reopened
        .command(
            "logout-chat",
            "navigate",
            json!({
                "expected_epoch":initial["page_epoch"], "url":site.url
            }),
        )
        .await
        .unwrap();
    let probe = reopened.eval("logout-chat", json!({
        "expected_epoch":page["page_epoch"], "expected_url":site.url,
        "code":"fetch('/probe').then(response => response.text()).then(text => text === 'true')"
    })).await.unwrap();
    reopened.close("logout-chat").await.unwrap();
    assert_eq!(
        probe["value"], false,
        "{mode} resurrected an already cleared HTTP-only session cookie"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "requires the Playwright Chromium runtime"]
fn streaming_profile_finishes_logout_when_the_runtime_and_stdout_reader_stop() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_site, _directory, root, manager, checkpoint) =
        runtime.block_on(prepare_immediate_logout());
    drop(runtime);
    drop(manager);
    let started = Instant::now();
    loop {
        if let Ok(lease) = profile::acquire(&root, "logout-chat") {
            drop(lease);
            break;
        }
        assert!(
            started.elapsed() < BROWSER_CLOSE_DEADLINE + Duration::from_secs(5),
            "streaming host did not finish after its runtime disappeared"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let saved: Value = serde_json::from_slice(&std::fs::read(checkpoint).unwrap()).unwrap();
    assert!(
        !saved["cookies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|cookie| cookie["name"] == "fixture-session"),
        "runtime loss must not skip the final cookie deletion checkpoint"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the Playwright Chromium runtime"]
async fn immediate_cookie_logout_survives_explicit_close() {
    immediate_logout_then_retire("close").await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the Playwright Chromium runtime"]
async fn immediate_cookie_logout_survives_idle_reclamation() {
    immediate_logout_then_retire("idle").await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the Playwright Chromium runtime"]
async fn immediate_cookie_logout_survives_manager_drop() {
    immediate_logout_then_retire("drop").await;
}

#[cfg(unix)]
async fn interrupted_logout_is_not_replayed(cancel: bool) {
    let (site, _directory, root, manager, checkpoint) = prepare_immediate_logout().await;
    let session = manager.sessions.lock().await["logout-chat"].clone();
    let args = json!({
        "expected_epoch":session.frame_epoch.load(Ordering::Acquire),
        "expected_url":format!("{}seed", site.url), "code":"new Promise(() => {})"
    });
    if cancel {
        let pending = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.eval("logout-chat", args).await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.pending.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(4), async {
            while manager.sessions.lock().await.contains_key("logout-chat") {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    } else {
        let result = manager
            .command_with_deadline("logout-chat", "eval", args, Duration::from_millis(20))
            .await;
        assert!(
            matches!(result, Err(BrowserError::Failed(message)) if message.contains("timed out"))
        );
    }
    assert!(session.reaped.load(Ordering::Acquire));
    // Verify this was the vulnerable interval, rather than an already-updated
    // checkpoint. The site observed logout before the controlled interruption.
    let saved: Value = serde_json::from_slice(&std::fs::read(checkpoint).unwrap()).unwrap();
    assert!(saved["cookies"]
        .as_array()
        .unwrap()
        .iter()
        .any(|cookie| cookie["name"] == "fixture-session"));
    assert!(
        profile::acquire(&root, "logout-chat").is_err(),
        "a controlled interruption must block stale-cookie replay"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the Playwright Chromium runtime"]
async fn cookie_logout_before_eval_timeout_blocks_stale_checkpoint_replay() {
    interrupted_logout_is_not_replayed(false).await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the Playwright Chromium runtime"]
async fn cookie_logout_before_caller_cancel_blocks_stale_checkpoint_replay() {
    interrupted_logout_is_not_replayed(true).await;
}
