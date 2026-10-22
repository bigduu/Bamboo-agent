//! Synthetic host contract tests. No Chromium, site data, model or credentials.
use super::*;

const FIXTURE: &str = "BAMBOO_BROWSER_SHARED_MANAGER_FIXTURE";

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
                    let a = manager.open("canonical-chat-a").await.unwrap();
                    let b = manager.open("canonical-chat-b").await.unwrap();
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

const SYNTHETIC_HOST: &str = r#"import json, os, pathlib, sys
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
home = pathlib.Path(os.environ['BAMBOO_BROWSER_HOST_SCRIPT']).parent
with (home / 'launches').open('a') as output:
    output.write(str(profile) + '\n')
(profile / 'synthetic-account-marker').write_text('synthetic data only')
generation = 'a' * 24
scopes = {}
for line in sys.stdin:
    request = json.loads(line)
    action = request['action']
    owner = request.get('scope_id', 'legacy')
    v2 = request.get('version') == 2
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
    reply = {'id': request['id'], 'ok': True, 'result': result}
    if v2:
        reply.update(version=2, host_generation=generation)
        if 'scope_id' in request:
            reply.update(scope_id=owner, scope_generation=request['scope_generation'])
    print(json.dumps(reply), flush=True)
"#;
