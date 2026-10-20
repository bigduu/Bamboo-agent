//! Development reset owns only temporary fixture history and synthetic hosts.

use super::*;
use actix_web::web;
use bamboo_agent_core::{Session, Storage};

fn seed_profile(root: &std::path::Path, id: &str) -> PathBuf {
    let lease = profile::acquire(root, id).unwrap();
    let path = lease.path.clone();
    std::fs::create_dir(path.join("Default")).unwrap();
    std::fs::write(path.join("Default/Cookies"), b"synthetic cookie").unwrap();
    std::fs::write(path.join("Default/Local Storage"), b"synthetic storage").unwrap();
    std::fs::write(path.join("Default/IndexedDB"), b"synthetic database").unwrap();
    path
}

#[tokio::test]
async fn dev_reset_purges_closed_and_orphaned_profiles_and_fences_old_ids() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .unwrap(),
    );
    for id in ["reset-closed", "reset-never-opened"] {
        state
            .session_store
            .save_session(&Session::new(id, "synthetic-model"))
            .await
            .unwrap();
    }
    let root = data.path().join("browser-profiles");
    let closed = seed_profile(&root, "reset-closed");
    let orphaned = seed_profile(&root, "orphaned-no-session");
    let response = crate::handlers::agent::dev::reset(state.clone())
        .await
        .unwrap();
    let profiles_absent = !closed.exists() && !orphaned.exists();
    let old_ids_fenced = ["reset-closed", "orphaned-no-session", "reset-never-opened"]
        .into_iter()
        .all(|id| matches!(profile::acquire(&root, id), Err(BrowserError::NotOpen)));
    let canonical_absent = state
        .session_store
        .load_session("reset-closed")
        .await
        .unwrap()
        .is_none();
    state.shutdown().await;
    assert!(response.status().is_success());
    assert!(
        canonical_absent,
        "fixture must exercise the real storage reset"
    );
    assert!(
        profiles_absent,
        "greenfield reset retained persisted site data"
    );
    assert!(
        old_ids_fenced,
        "old requests must not recreate reset profiles"
    );
}

#[tokio::test]
async fn dev_reset_reports_profile_lock_failure_without_claiming_success() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .unwrap(),
    );
    state
        .session_store
        .save_session(&Session::new("reset-blocked", "synthetic-model"))
        .await
        .unwrap();
    let root = data.path().join("browser-profiles");
    let other_owner = profile::acquire(&root, "reset-blocked").unwrap();
    let path = other_owner.path.clone();
    std::fs::write(path.join("Cookies"), b"synthetic other owner").unwrap();
    let failed = crate::handlers::agent::dev::reset(state.clone())
        .await
        .is_err();
    let canonical_retained = state
        .session_store
        .load_session("reset-blocked")
        .await
        .unwrap()
        .is_some();
    let profile_retained = path.join("Cookies").is_file();
    drop(other_owner);
    let retry = crate::handlers::agent::dev::reset(state.clone()).await;
    let retry_purged = !path.exists();
    state.shutdown().await;
    assert!(
        failed,
        "reset cannot report success with a foreign live profile lease"
    );
    assert!(canonical_retained && profile_retained);
    assert!(
        retry.is_ok() && retry_purged,
        "a safe reset retry must finish cleanup"
    );
}

#[cfg(unix)]
#[test]
fn dev_reset_reaps_active_host_fences_old_arcs_and_allows_new_chats() {
    check_owned_host_entry("reset");
}

#[cfg(unix)]
pub(super) fn check_owned_host_entry(mode: &str) {
    let temporary = tempfile::tempdir().unwrap();
    let data = temporary.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let script = temporary.path().join("synthetic-host.py");
    let pids = data.join("owned-host-pids");
    std::fs::write(
        &script,
        format!(
            r#"import json, os, pathlib, sys, time
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
with pathlib.Path({:?}).open('a') as owned:
    owned.write(str(os.getpid()) + '\n')
for line in sys.stdin:
    request = json.loads(line)
    print(json.dumps({{'id':request['id'], 'ok':True, 'result':{{'tabs':[]}}}}), flush=True)
time.sleep(0.2)
(profile / 'fixture-checkpoint').write_text('synthetic checkpoint')
"#,
            pids.to_string_lossy()
        ),
    )
    .unwrap();
    let log = std::fs::File::create(data.join("fixture.log")).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "browser::reset_tests::dev_reset_active_host_fixture",
            "--exact",
            "--test-threads=1",
        ])
        .env("BAMBOO_BROWSER_RESET_FIXTURE", &data)
        .env("BAMBOO_BROWSER_RESET_ENTRY", mode)
        .env("BAMBOO_BROWSER_NODE", "/usr/bin/python3")
        .env("BAMBOO_BROWSER_HOST_SCRIPT", script)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .status()
        .unwrap();
    // Own cleanup even when a regression makes the helper fail before shutdown.
    for line in std::fs::read_to_string(&pids).unwrap_or_default().lines() {
        let pid: i32 = line.parse().unwrap();
        if unsafe { libc::kill(pid, 0) } == 0 {
            kill_browser_process_group(pid);
        }
    }
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(data.join("fixture.log")).unwrap()
    );
    let observed: Value =
        serde_json::from_slice(&std::fs::read(data.join("observed.json")).unwrap()).unwrap();
    assert_eq!(observed["reaped_before_success"], true);
    assert_eq!(observed["old_arc_fenced"], true);
    assert_eq!(observed["profile_purged"], true);
    if mode == "reset" {
        assert_eq!(observed["queued_open_fenced"], true);
    }
    assert_eq!(observed["new_chat_usable"], true);
}

#[cfg(unix)]
#[test]
fn dev_reset_active_host_fixture() {
    let Some(data) = std::env::var_os("BAMBOO_BROWSER_RESET_FIXTURE").map(PathBuf::from) else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let state =
                web::Data::new(crate::app_state::AppState::new(data.clone()).await.unwrap());
            let entry = std::env::var("BAMBOO_BROWSER_RESET_ENTRY").unwrap();
            let parent = Session::new("adapter-parent", "synthetic-model");
            let active = if entry == "subagent" {
                state.session_store.save_session(&parent).await.unwrap();
                Session::new_child_of("reset-active", &parent, "synthetic-model", "fixture child")
            } else {
                Session::new("reset-active", "synthetic-model")
            };
            state.session_store.save_session(&active).await.unwrap();
            state.browser.open("reset-active").await.unwrap();
            let old = state
                .browser
                .sessions
                .lock()
                .await
                .get("reset-active")
                .unwrap()
                .clone();
            let path = profile::directory(&data.join("browser-profiles"), "reset-active").unwrap();
            std::fs::write(path.join("Cookies"), b"synthetic login").unwrap();
            let (succeeded, queued_fenced) = if entry == "subagent" {
                let deleted = super::terminal_entry_tests::delete_via_root_tool(
                    &state, &parent.id, &active.id,
                )
                .await;
                (deleted["deleted"] == true, false)
            } else {
                let resetting = crate::handlers::agent::dev::reset(state.clone());
                tokio::pin!(resetting);
                let (response, queued_fenced) = tokio::select! {
                response = &mut resetting => (response.unwrap(), false),
                _ = async {
                    while !old.retiring.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                } => {
                    // Begin the open while reset holds its temporary gate.
                    // It must refuse after reset, not reconstruct a new lease.
                    let queued = state.browser.open("queued-during-reset");
                    let (response, queued) = tokio::join!(&mut resetting, queued);
                    (response.unwrap(), matches!(queued, Err(BrowserError::NotOpen)))
                }
                };
                (response.status().is_success(), queued_fenced)
            };
            let reaped = old.child.lock().unwrap().try_wait().unwrap().is_some();
            let fenced = old.call("state", json!({})).await.is_err();
            let purged = !path.exists();
            state
                .session_store
                .save_session(&Session::new("new-after-reset", "synthetic-model"))
                .await
                .unwrap();
            let usable = state.browser.open("new-after-reset").await.is_ok();
            state.shutdown().await;
            std::fs::write(
                data.join("observed.json"),
                serde_json::to_vec(&json!({
                    "reaped_before_success": succeeded && reaped,
                    "old_arc_fenced": fenced,
                    "profile_purged": purged,
                "queued_open_fenced": queued_fenced,
                    "new_chat_usable": usable
                }))
                .unwrap(),
            )
            .unwrap();
        });
}
