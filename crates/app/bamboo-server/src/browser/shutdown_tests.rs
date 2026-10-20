//! Real process-exit fixtures use a synthetic EOF-checkpointing host only.

use super::*;

const FIXTURE: &str = "BAMBOO_BROWSER_SHUTDOWN_FIXTURE";
const MODE: &str = "BAMBOO_BROWSER_SHUTDOWN_MODE";

#[test]
fn app_state_shutdown_flushes_browser_before_process_exit() {
    check_process_exit("active");
}

#[test]
fn app_state_shutdown_drains_previously_detached_browser_reaper() {
    check_process_exit("detached");
}

#[test]
fn app_state_shutdown_prevents_browser_reopen() {
    check_process_exit("reopen");
}

fn check_process_exit(mode: &str) {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("data");
    let script = directory.path().join("synthetic-host.py");
    std::fs::write(
        &script,
        r#"import json, os, pathlib, sys, time
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
(profile / 'fixture-pid').write_text(str(os.getpid()))
for line in sys.stdin:
    request = json.loads(line)
    print(json.dumps({'id':request['id'], 'ok':True, 'result':{'tabs':[]}}), flush=True)
time.sleep(0.4)
(profile / 'fixture-checkpoint').write_text('synthetic session cookie checkpoint')
"#,
    )
    .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "browser::shutdown_tests::browser_shutdown_process_fixture",
            "--exact",
            "--test-threads=1",
        ])
        .env(FIXTURE, &data)
        .env(MODE, mode)
        .env("BAMBOO_BROWSER_NODE", "/usr/bin/python3")
        .env("BAMBOO_BROWSER_HOST_SCRIPT", script)
        .output()
        .unwrap();
    let root = data.join("browser-profiles");
    let path = profile::directory(&root, "shutdown-chat").unwrap();
    // Capture state at actual executable exit, before cleaning the RED case.
    let checkpoint_at_exit = path.join("fixture-checkpoint").is_file();
    let profile_reusable_at_exit = profile::acquire(&root, "shutdown-chat").is_ok();
    let reopen_refused = data.join("reopen-refused").is_file();
    let pid: i32 = std::fs::read_to_string(path.join("fixture-pid"))
        .unwrap()
        .parse()
        .unwrap();
    let exited_at_return = unsafe { libc::kill(pid, 0) } != 0;
    // The old code can leave the synthetic child briefly alive. Own and clean
    // that process before any assertion, including the expected RED failures.
    let started = Instant::now();
    while unsafe { libc::kill(pid, 0) } == 0 && started.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(10));
    }
    if unsafe { libc::kill(pid, 0) } == 0 {
        kill_browser_process_group(pid);
    }
    assert!(
        output.status.success(),
        "fixture failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(checkpoint_at_exit, "shutdown must await the host's EOF checkpoint");
    assert!(exited_at_return, "shutdown must reap its host before executable exit");
    assert!(profile_reusable_at_exit, "clean shutdown must leave a reusable profile");
    if mode == "reopen" {
        assert!(reopen_refused, "shutdown must fence new browser opens");
    }
}

#[test]
fn browser_shutdown_process_fixture() {
    let Some(data) = std::env::var_os(FIXTURE).map(PathBuf::from) else {
        return;
    };
    let mode = std::env::var(MODE).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = crate::app_state::AppState::new(data.clone()).await.unwrap();
        state.browser.open("shutdown-chat").await.unwrap();
        if mode == "detached" {
            let session = state.browser.sessions.lock().await.remove("shutdown-chat");
            drop(session);
            let path = profile::directory(&data.join("browser-profiles"), "shutdown-chat").unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !path.join(".bamboo-profile-close-pending").is_file() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.unwrap();
        }
        state.shutdown().await;
        if mode == "reopen" && state.browser.open("shutdown-chat").await.is_err() {
            std::fs::write(data.join("reopen-refused"), b"refused").unwrap();
        }
        drop(state);
    });
    // A normal executable must not depend on a detached thread finishing after
    // its last AppState/runtime returns. Do not let libtest wait hide that bug.
    std::process::exit(0);
}
