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
    if mode == "gate" {
        assert_eq!(observed["foreign_new_key_blocked"], true);
        assert_eq!(observed["no_profile_created_while_reset"], true);
        assert_eq!(observed["foreign_new_key_usable_after_release"], true);
        return;
    }
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
            if entry == "gate" {
                let root = data.join("browser-profiles");
                let foreign = BrowserManager::with_profile_root(root.clone());
                let reset = state.browser.reset_profiles().await.unwrap();
                // A distinct manager has its own in-memory open gate/epoch.
                // Only the shared stable file gate can block this new key.
                let blocked = foreign.open("foreign-new-key").await.is_err();
                let unallocated = !profile::has_evidence(&root, "foreign-new-key").unwrap()
                    && std::fs::read_to_string(data.join("owned-host-pids"))
                        .unwrap_or_default()
                        .is_empty();
                drop(reset);
                let usable = foreign.open("foreign-new-key").await.is_ok();
                foreign.shutdown().await;
                state.shutdown().await;
                std::fs::write(
                    data.join("observed.json"),
                    serde_json::to_vec(&json!({
                        "foreign_new_key_blocked":blocked,
                        "no_profile_created_while_reset":unallocated,
                        "foreign_new_key_usable_after_release":usable
                    }))
                    .unwrap(),
                )
                .unwrap();
                return;
            }
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
                let deleted = super::terminal_entry_tests::delete_via_subagent_tool(
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

#[cfg(unix)]
#[test]
fn reset_gate_blocks_a_new_key_in_an_independent_manager() {
    check_owned_host_entry("gate");
}

#[tokio::test]
async fn partial_reset_evicts_revoked_children_even_when_index_publication_fails() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::app_state::AppState::new(data.path().into())
            .await
            .unwrap(),
    );
    let parent = Session::new("partial-reset-parent", "synthetic-model");
    let child = Session::new_child_of("partial-reset-child", &parent, "synthetic-model", "fixture");
    state.session_store.save_session(&parent).await.unwrap();
    state.session_store.save_session(&child).await.unwrap();
    state.session_store.flush_search_index().await;
    state.sessions.insert(
        child.id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(child.clone())),
    );
    let token = tokio_util::sync::CancellationToken::new();
    state
        .cancel_tokens
        .write()
        .await
        .insert(child.id.clone(), token.clone());
    let (sender, _) = tokio::sync::broadcast::channel(8);
    state
        .session_event_senders
        .write()
        .await
        .insert(child.id.clone(), sender);
    let root = data.path().join("browser-profiles");
    let path = seed_profile(&root, &child.id);
    // Actual filesystem error at the final index-publication boundary, after
    // the real canonical Root revocation/tree deletion. Only this temp store.
    let lock_path = data.path().join(".sessions-index.lock");
    std::fs::remove_file(&lock_path).unwrap();
    std::fs::create_dir(&lock_path).unwrap();
    let failed = crate::handlers::agent::dev::reset(state.clone())
        .await
        .is_err();
    std::fs::remove_dir(&lock_path).unwrap();
    let absent = state
        .session_store
        .session_is_canonically_absent(&child.id)
        .await
        .unwrap();
    let evicted = !state.sessions.contains_key(&child.id)
        && !state.cancel_tokens.read().await.contains_key(&child.id)
        && !state
            .session_event_senders
            .read()
            .await
            .contains_key(&child.id)
        && token.is_cancelled();
    let fenced = matches!(
        profile::acquire(&root, &child.id),
        Err(BrowserError::NotOpen)
    );
    state.shutdown().await;
    assert!(
        failed && absent,
        "fixture must fail after real canonical deletion"
    );
    assert!(!path.exists() && fenced);
    assert!(
        evicted,
        "a failed reset retained a logically deleted Child in runtime state"
    );
}

#[tokio::test]
async fn partial_reset_preserves_a_still_canonical_root_and_its_runtime_state() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::app_state::AppState::new(data.path().into())
            .await
            .unwrap(),
    );
    let session = Session::new("partial-reset-survivor", "synthetic-model");
    state.session_store.save_session(&session).await.unwrap();
    state.sessions.insert(
        session.id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(session.clone())),
    );
    let root = data.path().join("browser-profiles");
    let profile = seed_profile(&root, &session.id);
    let children = data
        .path()
        .join("sessions")
        .join(&session.id)
        .join("children");
    // Root persistence already creates this empty container. Replace only
    // that private empty fixture directory, never a real Child tree.
    std::fs::remove_dir(&children).unwrap();
    std::fs::write(&children, b"synthetic census obstruction").unwrap();
    let failed = crate::handlers::agent::dev::reset(state.clone())
        .await
        .is_err();
    let retained =
        profile.join("Default/Cookies").is_file() && state.sessions.contains_key(&session.id);
    // The obstruction intentionally makes canonical observation uncertain.
    // Restore it before independently checking the unchanged live Root.
    std::fs::remove_file(&children).unwrap();
    let canonical = !state
        .session_store
        .session_is_canonically_absent(&session.id)
        .await
        .unwrap();
    state.shutdown().await;
    assert!(
        failed && canonical && retained,
        "partial failure purged still-canonical data"
    );
}

#[tokio::test]
async fn dev_reset_retries_when_the_canonical_sessions_directory_is_missing() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::app_state::AppState::new(data.path().into())
            .await
            .unwrap(),
    );
    let session = Session::new("interrupted-reset", "synthetic-model");
    state.session_store.save_session(&session).await.unwrap();
    let root = data.path().join("browser-profiles");
    let path = seed_profile(&root, &session.id);
    // Establish real canonical revocation, then simulate the missing-directory
    // state of an interrupted reset. This is not a power-cut/fsync claim.
    state.session_store.dev_reset().await.unwrap();
    std::fs::remove_dir(data.path().join("sessions")).unwrap();
    let retried = crate::handlers::agent::dev::reset(state.clone()).await;
    let fenced = matches!(
        profile::acquire(&root, &session.id),
        Err(BrowserError::NotOpen)
    );
    state.shutdown().await;
    assert!(retried.is_ok() && data.path().join("sessions").is_dir());
    assert!(!path.exists() && fenced);
}
