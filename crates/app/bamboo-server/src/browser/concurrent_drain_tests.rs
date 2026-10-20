//! All owned hosts must receive EOF before waiting for any checkpoint.

use super::*;
use bamboo_agent_core::{Session, Storage};

const FIXTURE: &str = "BAMBOO_BROWSER_CONCURRENT_DRAIN_FIXTURE";
const MODE: &str = "BAMBOO_BROWSER_CONCURRENT_DRAIN_MODE";

#[test]
fn app_state_shutdown_overlaps_all_host_checkpoints() {
    check_concurrent_drain("shutdown");
}

#[test]
fn dev_reset_overlaps_all_host_checkpoints() {
    check_concurrent_drain("reset");
}

#[test]
fn recursive_delete_overlaps_all_host_retirements() {
    check_concurrent_drain("delete");
}

#[test]
fn bulk_cleanup_overlaps_all_host_retirements() {
    check_concurrent_drain("bulk");
}

#[test]
fn idle_sweep_overlaps_all_host_checkpoints() {
    check_concurrent_drain("idle");
}

fn check_concurrent_drain(mode: &str) {
    let temporary = tempfile::tempdir().unwrap();
    let data = temporary.path().join("data");
    let events = temporary.path().join("events");
    std::fs::create_dir(&events).unwrap();
    let script = temporary.path().join("synthetic-host.py");
    let owned_pids = temporary.path().join("owned-pids");
    std::fs::write(
        &script,
        format!(
            r#"import json, os, pathlib, sys, time
profile = pathlib.Path(os.environ['BAMBOO_BROWSER_PROFILE_DIR'])
events = pathlib.Path({:?})
with pathlib.Path({:?}).open('a') as owned:
    owned.write(str(os.getpid()) + ' ' + profile.name + '\n')
for line in sys.stdin:
    request = json.loads(line)
    print(json.dumps({{'id':request['id'], 'ok':True, 'result':{{'tabs':[]}}}}), flush=True)
(events / (profile.name + '.eof')).write_text('EOF received')
deadline = time.monotonic() + 2
while len(list(events.glob('*.eof'))) < 2 and time.monotonic() < deadline:
    time.sleep(0.005)
if len(list(events.glob('*.eof'))) == 2:
    (events / (profile.name + '.overlap')).write_text('all EOF before checkpoint')
(profile / 'fixture-checkpoint').write_text('synthetic checkpoint')
(events / (profile.name + '.checkpoint')).write_text('checkpoint finished')
"#,
            events.to_string_lossy(),
            owned_pids.to_string_lossy()
        ),
    )
    .unwrap();
    let log_path = temporary.path().join("fixture.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "browser::concurrent_drain_tests::concurrent_drain_process_fixture",
            "--exact",
            "--test-threads=1",
        ])
        .env(FIXTURE, temporary.path())
        .env(MODE, mode)
        .env(
            "BAMBOO_BROWSER_NODE",
            std::env::var_os("BAMBOO_BROWSER_TEST_PYTHON")
                .unwrap_or_else(|| "/usr/bin/python3".into()),
        )
        .env("BAMBOO_BROWSER_HOST_SCRIPT", &script)
        // Measure executable exit itself, not a pipe held by its children.
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .status()
        .unwrap();
    let records: Vec<_> = std::fs::read_to_string(&owned_pids)
        .unwrap_or_default()
        .lines()
        .map(|line| {
            let (pid, key) = line.split_once(' ').unwrap();
            (pid.parse::<i32>().unwrap(), key.to_string())
        })
        .collect();
    let all_reaped_at_return = records
        .iter()
        .all(|(pid, _)| unsafe { libc::kill(*pid, 0) } != 0);
    let all_overlapped = records
        .iter()
        .all(|(_, key)| events.join(format!("{key}.overlap")).is_file());
    let all_checkpointed = records
        .iter()
        .all(|(_, key)| events.join(format!("{key}.checkpoint")).is_file());
    let profile_root = data.join("browser-profiles");
    let profiles_correct = records.iter().all(|(_, key)| {
        let profile = profile_root.join(key);
        if matches!(mode, "reset" | "delete" | "bulk") {
            !profile.exists()
        } else {
            profile.join("fixture-checkpoint").is_file()
        }
    });
    // Own cleanup even on a RED failure or a broken helper; never touch a PID
    // outside the isolated fixture's own process log.
    for (pid, _) in &records {
        if unsafe { libc::kill(*pid, 0) } == 0 {
            kill_browser_process_group(*pid);
        }
    }
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(&log_path).unwrap()
    );
    assert_eq!(
        records.len(),
        2,
        "fixture must start two actual owned hosts"
    );
    assert!(all_reaped_at_return && all_checkpointed && profiles_correct);
    assert!(temporary.path().join("old-arcs-fenced").is_file());
    assert!(
        all_overlapped,
        "a host waited for another checkpoint before receiving EOF"
    );
}

#[test]
fn concurrent_drain_process_fixture() {
    let Some(fixture) = std::env::var_os(FIXTURE).map(PathBuf::from) else {
        return;
    };
    let mode = std::env::var(MODE).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let state = actix_web::web::Data::new(
                crate::app_state::AppState::new(fixture.join("data"))
                    .await
                    .unwrap(),
            );
            let root = Session::new("concurrent-drain-root", "synthetic-model");
            let deleting = matches!(mode.as_str(), "delete" | "bulk");
            if deleting {
                state.session_store.save_session(&root).await.unwrap();
            }
            let mut old = Vec::new();
            for id in ["concurrent-drain-a", "concurrent-drain-b"] {
                let session = if deleting {
                    Session::new_child_of(id, &root, "synthetic-model", "fixture")
                } else {
                    Session::new(id, "synthetic-model")
                };
                state.session_store.save_session(&session).await.unwrap();
                state.browser.open(id).await.unwrap();
                old.push(state.browser.sessions.lock().await.get(id).unwrap().clone());
            }
            match mode.as_str() {
                "reset" => {
                    crate::handlers::agent::dev::reset(state.clone())
                        .await
                        .unwrap();
                }
                "delete" => {
                    let response = crate::handlers::agent::delete::handler(
                        state.clone(),
                        actix_web::web::Path::from(root.id.clone()),
                    )
                    .await
                    .unwrap();
                    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
                }
                "bulk" => {
                    let response = crate::handlers::agent::sessions::cleanup_sessions(
                        state.clone(),
                        actix_web::web::Json(crate::handlers::agent::sessions::CleanupRequest {
                            mode: "all".into(),
                            keep_pinned: false,
                        }),
                    )
                    .await
                    .unwrap();
                    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
                }
                "idle" => assert_eq!(state.browser.sweep_idle(Duration::ZERO).await, 2),
                "shutdown" => state.shutdown().await,
                other => panic!("unknown isolated fixture mode: {other}"),
            }
            if deleting {
                for id in [root.id.as_str(), "concurrent-drain-a", "concurrent-drain-b"] {
                    assert!(state
                        .session_store
                        .session_is_canonically_absent(id)
                        .await
                        .unwrap());
                }
            }
            for session in old {
                assert!(session.call("state", json!({})).await.is_err());
            }
            std::fs::write(fixture.join("old-arcs-fenced"), b"fenced").unwrap();
            // Unrelated destructors/runtime shutdown must not conceal an
            // executable returning before any owned host is actually reaped.
            std::process::exit(0);
        });
}
