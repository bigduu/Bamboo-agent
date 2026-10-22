//! Synthetic host contract tests. No Chromium, site data, model or credentials.
use super::*;

const FIXTURE: &str = "BAMBOO_BROWSER_SHARED_MANAGER_FIXTURE";

#[tokio::test]
async fn transport_limits_unterminated_frames_and_preserves_record_boundaries() {
    let bytes = vec![b'x'; shared::MAX_FRAME_BYTES + 1];
    let mut reader = BufReader::new(bytes.as_slice());
    assert_eq!(read_host_line(&mut reader).await.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    let mut reader = BufReader::new(b"{\"a\":1}\n{\"b\":2}\n".as_slice());
    assert_eq!(read_host_line(&mut reader).await.unwrap().unwrap(), b"{\"a\":1}\n");
    assert_eq!(read_host_line(&mut reader).await.unwrap().unwrap(), b"{\"b\":2}\n");
    assert!(read_host_line(&mut reader).await.unwrap().is_none());
    let mut reader = BufReader::new(b"{\"incomplete\":true}".as_slice());
    assert_eq!(read_host_line(&mut reader).await.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
}

fn in_isolated_process(test: &str, run: impl FnOnce(PathBuf)) {
    if let Some(home) = std::env::var_os(FIXTURE).map(PathBuf::from) {
        run(home);
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let script = home.path().join("synthetic-host.py");
    std::fs::write(&script, SYNTHETIC_HOST).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(format!("browser::shared_manager_tests::{test}"))
        .arg("--nocapture")
        .env(FIXTURE, home.path())
        .env("BAMBOO_BROWSER_NODE", "/usr/bin/python3")
        .env("BAMBOO_BROWSER_HOST_SCRIPT", &script)
        .status()
        .unwrap();
    assert!(status.success(), "isolated shared-manager contract failed");
}

#[test]
fn two_chats_share_one_process_and_deleting_one_preserves_its_sibling_and_account_store() {
    in_isolated_process(
        "two_chats_share_one_process_and_deleting_one_preserves_its_sibling_and_account_store",
        |home| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let root = home.join("profiles");
                    let manager = BrowserManager::with_profile_root(root.clone());
                    let (a, b) = tokio::join!(manager.open("canonical-chat-a"), manager.open("canonical-chat-b"));
                    let a = a.unwrap();
                    let b = b.unwrap();
                    let launches = std::fs::read_to_string(home.join("launches")).unwrap();
                    let count = launches.lines().count();
                    // Drain our own processes on RED before asserting.
                    if count != 1 {
                        manager.shutdown().await;
                        assert_eq!(count, 1, "chat open must reuse one physical host");
                    }
                    assert_eq!(a["profile_mode"], "shared");
                    assert_eq!(b["storage_shared"], true);
                    manager.retire_deleted_session("canonical-chat-a").await.unwrap();
                    assert!(manager.open("canonical-chat-a").await.is_err());
                    assert_eq!(manager.state("canonical-chat-b").await.unwrap()["profile_mode"], "shared");
                    let profile_path = PathBuf::from(launches.lines().next().unwrap());
                    assert!(profile_path.join("synthetic-account-marker").is_file());
                    manager.close("canonical-chat-b").await.unwrap();
                    assert!(profile_path.join("synthetic-account-marker").is_file());
                    assert!(matches!(manager.state("canonical-chat-b").await, Err(BrowserError::NotOpen)));
                    manager.shutdown().await;

                    let restarted = BrowserManager::with_profile_root(root);
                    assert!(restarted.open("canonical-chat-a").await.is_err());
                    assert_eq!(restarted.open("canonical-chat-c").await.unwrap()["profile_mode"], "shared");
                    restarted.close("canonical-chat-c").await.unwrap();
                    restarted.shutdown().await;
                    let launches = std::fs::read_to_string(home.join("launches")).unwrap();
                    assert_eq!(launches.lines().count(), 2);
                    assert!(launches.lines().all(|path| path == profile_path.to_str().unwrap()));
                });
        },
    );
}

#[test]
fn active_clean_mode_is_preserved_until_explicit_close_and_reopen() {
    in_isolated_process("active_clean_mode_is_preserved_until_explicit_close_and_reopen", |home| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let manager = BrowserManager::with_profile_root(home.join("profiles"));
            let clean = manager.open_with_mode("canonical-clean", Some(BrowserProfileMode::Clean)).await.unwrap();
            assert_eq!(clean["profile_mode"], "clean");
            assert_eq!(clean["storage_shared"], false);
            assert_eq!(manager.open("canonical-clean").await.unwrap()["profile_mode"], "clean");
            assert!(matches!(manager.open_with_mode("canonical-clean", Some(BrowserProfileMode::Shared)).await, Err(BrowserError::ProfileModeConflict)));
            assert_eq!(manager.state("canonical-clean").await.unwrap()["profile_mode"], "clean");
            manager.close("canonical-clean").await.unwrap();
            assert!(matches!(manager.state("canonical-clean").await, Err(BrowserError::NotOpen)));
            assert_eq!(manager.open_with_mode("canonical-clean", Some(BrowserProfileMode::Shared)).await.unwrap()["profile_mode"], "shared");
            manager.close("canonical-clean").await.unwrap();
            manager.shutdown().await;
        });
    });
}

#[test]
fn cancelling_a_chat_mutation_closes_its_scope_without_killing_the_sibling_host() {
    in_isolated_process("cancelling_a_chat_mutation_closes_its_scope_without_killing_the_sibling_host", |home| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let manager = BrowserManager::with_profile_root(home.join("profiles"));
            manager.open("canonical-chat-a").await.unwrap();
            manager.open("canonical-chat-b").await.unwrap();
            let owned = manager.clone();
            let mutation = tokio::spawn(async move {
                owned.command("canonical-chat-a", "synthetic_slow_mutation", json!({})).await
            });
            tokio::time::timeout(Duration::from_secs(3), async {
                while !home.join("slow-started").exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
            }).await.unwrap();
            mutation.abort();
            assert!(mutation.await.unwrap_err().is_cancelled());
            tokio::time::timeout(Duration::from_secs(3), async {
                while manager.shared.contains("canonical-chat-a") { tokio::time::sleep(Duration::from_millis(5)).await; }
            }).await.unwrap();
            assert_eq!(manager.state("canonical-chat-b").await.unwrap()["profile_mode"], "shared");
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!home.join("late-mutation").exists());
            assert_eq!(std::fs::read_to_string(home.join("launches")).unwrap().lines().count(), 1);
            assert!(!manager.lifecycle.unconfirmed_writers.load(Ordering::Acquire));
            manager.close("canonical-chat-b").await.unwrap();
            manager.shutdown().await;
        });
    });
}

#[test]
fn reset_reuses_the_account_key_but_preserves_deleted_chat_fences() {
    in_isolated_process("reset_reuses_the_account_key_but_preserves_deleted_chat_fences", |home| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let root = home.join("profiles");
            let manager = BrowserManager::with_profile_root(root.clone());
            manager.open("old-canonical-chat").await.unwrap();
            manager.retire_deleted_session("old-canonical-chat").await.unwrap();
            let reset = manager.reset_profiles().await.unwrap();
            reset.purge_profiles().await.unwrap();
            drop(reset);
            assert!(matches!(manager.open("old-canonical-chat").await, Err(BrowserError::NotOpen)));
            assert_eq!(manager.open("new-canonical-chat").await.unwrap()["profile_mode"], "shared");
            manager.close("new-canonical-chat").await.unwrap();
            manager.shutdown().await;
            let launches = std::fs::read_to_string(home.join("launches")).unwrap();
            let paths: Vec<_> = launches.lines().collect();
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0], paths[1], "account reset must not rotate its stable profile identity");
        });
    });
}

#[test]
fn wrong_reply_owner_fences_the_runtime_and_retains_unknown_writer_state() {
    in_isolated_process("wrong_reply_owner_fences_the_runtime_and_retains_unknown_writer_state", |home| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let root = home.join("profiles");
            let manager = BrowserManager::with_profile_root(root);
            manager.open("canonical-chat-a").await.unwrap();
            manager.open("canonical-chat-b").await.unwrap();
            assert!(manager.command("canonical-chat-a", "synthetic_wrong_owner", json!({})).await.is_err());
            assert!(manager.state("canonical-chat-b").await.is_err());
            manager.shutdown().await;
            assert!(manager.lifecycle.unconfirmed_writers.load(Ordering::Acquire));
            let launches = std::fs::read_to_string(home.join("launches")).unwrap();
            let path = PathBuf::from(launches.lines().next().unwrap());
            assert!(path.join(".bamboo-profile-writer-exit-unconfirmed").is_file());
            assert!(path.join("synthetic-account-marker").is_file());
            assert!(manager.reset_profiles().await.is_err());
        });
    });
}

#[test]
fn cancelled_startup_is_owned_and_shutdown_cannot_admit_another_writer() {
    in_isolated_process("cancelled_startup_is_owned_and_shutdown_cannot_admit_another_writer", |home| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            std::fs::write(home.join("delay-handshake"), b"synthetic startup gate").unwrap();
            let manager = BrowserManager::with_profile_root(home.join("profiles"));
            let owned = manager.clone();
            let opening = tokio::spawn(async move { owned.open("canonical-startup-chat").await });
            tokio::time::timeout(Duration::from_secs(3), async {
                while !home.join("handshake-started").exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
            }).await.unwrap();
            let owned = manager.clone();
            let shutdown = tokio::spawn(async move { owned.shutdown().await });
            tokio::task::yield_now().await;
            opening.abort();
            assert!(opening.await.unwrap_err().is_cancelled());
            tokio::time::timeout(Duration::from_secs(4), shutdown).await.unwrap().unwrap();
            assert!(manager.open("canonical-second-chat").await.is_err());
            assert_eq!(std::fs::read_to_string(home.join("launches")).unwrap().lines().count(), 1);
            assert!(manager.lifecycle.unconfirmed_writers.load(Ordering::Acquire));
            assert!(manager.lifecycle.owners.lock().unwrap().iter().all(|owner| *owner.done.borrow()));
        });
    });
}

const SYNTHETIC_HOST: &str = r#"import json, os, pathlib, sys, threading, time
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
home = pathlib.Path(__file__).parent
with (home / 'launches').open('a') as output:
    output.write(str(profile) + '\n')
(profile / 'synthetic-account-marker').write_text('synthetic data only')
generation = 'a' * 24
scopes = {}
output_lock = threading.Lock()
def emit(request, result):
    reply = {'id': request['id'], 'ok': True, 'result': result}
    if request.get('version') == 2:
        reply.update(version=2, host_generation=generation)
        if 'scope_id' in request:
            reply.update(scope_id=request['scope_id'], scope_generation=request['scope_generation'])
    with output_lock:
        print(json.dumps(reply), flush=True)
def slow(request):
    (home / 'slow-started').write_text('synthetic request only')
    time.sleep(0.25)
    if request['scope_id'] in scopes:
        (home / 'late-mutation').write_text('scope was not closed')
    emit(request, {'done': True})
for line in sys.stdin:
    request = json.loads(line)
    action = request['action']
    owner = request.get('scope_id', 'legacy')
    v2 = request.get('version') == 2
    if action == 'synthetic_wrong_owner':
        wrong = dict(request, scope_id='f' * 24)
        emit(wrong, {'done': True})
        continue
    if action == 'runtime_state' and (home / 'delay-handshake').exists():
        (home / 'handshake-started').write_text('synthetic startup only')
        time.sleep(0.25)
    if action == 'synthetic_slow_mutation':
        threading.Thread(target=slow, args=(request,)).start()
        continue
    if action == 'runtime_state':
        result = {'protocol_version': 2, 'host_generation': generation, 'browser_instance_id': 'b' * 24,
                  'scope_count': len(scopes), 'profile_modes': ['shared', 'clean']}
    elif action == 'scope_close':
        scopes.pop(owner, None)
        result = {'closed': True}
    else:
        if action == 'scope_open' or owner == 'legacy':
            scopes.setdefault(owner, request.get('args', {}).get('profile_mode', 'shared'))
        mode = scopes[owner]
        result = {'page_epoch': 1, 'tabs': [], 'active_tab_id': None,
                  'profile_mode': mode, 'storage_shared': mode == 'shared'}
    emit(request, result)
"#;
