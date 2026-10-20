use super::*;
use crate::tools::WriteStdinTool;
use serde_json::Value;
use tokio::io::{duplex, AsyncWriteExt};

fn context(owner: &str) -> ToolCtx {
    let mut ctx = ToolCtx::none("exec-session-test");
    ctx.session_id = Some(Arc::from(owner));
    ctx
}

fn payload(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("exec sessions must return ordinary completed tool calls");
    };
    assert!(result.success);
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn captures_split_utf8_and_prompts_without_newlines() {
    let state = PolledShellState::new().unwrap();
    let (mut writer, reader) = duplex(16);
    let pump = tokio::spawn(pump_polled_stream(reader, state.clone()));
    writer.write_all(&[0xe6, 0xb1]).await.unwrap();
    tokio::task::yield_now().await;
    writer.write_all(&[0x89, b'>']).await.unwrap();
    drop(writer);
    pump.await.unwrap();
    state.finish("completed", Some(0)).await;
    let result = state.take("unused", None).await;
    assert_eq!(result.output, "汉>");
    assert!(result.session_id.is_none());
}

#[tokio::test]
async fn capture_and_response_limits_report_omissions_and_advance_cursor() {
    let state = PolledShellState::new().unwrap();
    state.push(&vec![b'x'; MAX_CAPTURE_BYTES + 100]).await;
    state.finish("completed", Some(0)).await;
    let result = state.take("unused", Some(1)).await;
    assert_eq!(result.output, "xx");
    assert_eq!(result.output_omitted_bytes, MAX_CAPTURE_BYTES + 98);
    let second = state.take("unused", None).await;
    assert!(second.output.is_empty());
    assert_eq!(second.output_omitted_bytes, 0);
}

#[tokio::test]
async fn response_budget_accounts_for_json_escaping_and_utf8_without_miscounting_omissions() {
    let text = "\0\"\\汉🙂\n\r\t\u{8}\u{c}\u{1f}/\u{7f}\u{2028}\u{2029}".repeat(40);
    for tokens in 1..=64 {
        let state = PolledShellState::new().unwrap();
        state.push(text.as_bytes()).await;
        state.finish("completed", Some(7)).await;
        let result = state.take("unused", Some(tokens)).await;
        let encoded = serde_json::to_string(&result.output).unwrap();
        assert!(encoded.len() <= tokens * 4);
        assert!(text.starts_with(&result.output));
        assert_eq!(
            result.output_omitted_bytes,
            text.len() - result.output.len()
        );
        assert_eq!(result.exit_code, Some(7));
        if let Some(next) = text[result.output.len()..].chars().next() {
            let mut longer = result.output.clone();
            longer.push(next);
            assert!(serde_json::to_string(&longer).unwrap().len() > tokens * 4);
        }
    }
}

#[tokio::test]
async fn tiny_output_budget_preserves_running_session_metadata() {
    let state = PolledShellState::new().unwrap();
    let text = "\0\"\\汉🙂";
    state.push(text.as_bytes()).await;
    let result = state.take("running-handle", Some(1)).await;
    assert!(result.output.is_empty());
    assert_eq!(serde_json::to_string(&result.output).unwrap().len(), 2);
    assert_eq!(result.output_omitted_bytes, text.len());
    let envelope = serde_json::to_value(&result).unwrap();
    assert_eq!(envelope["session_id"], "running-handle");
    assert_eq!(envelope["status"], "running");
    // This cap covers the encoded output string; required protocol metadata is
    // additional even when the requested body budget is only four bytes.
    assert!(serde_json::to_string(&result).unwrap().len() > 4);
}

#[tokio::test(start_paused = true)]
async fn quiet_poll_advisory_requires_sustained_silence_and_repeated_empty_readouts() {
    let state = PolledShellState::new().unwrap();
    tokio::time::advance(QUIET_POLL_ADVISORY_AFTER).await;
    for count in 1..QUIET_POLL_ADVISORY_READOUTS {
        let result = state.take("quiet-handle", None).await;
        let progress = result.polling.unwrap();
        assert_eq!(progress.consecutive_empty_readouts, count);
        assert!(progress.advisory.is_none());
    }
    let result = state.take("quiet-handle", None).await;
    assert_eq!(result.session_id.as_deref(), Some("quiet-handle"));
    assert_eq!(result.status, "running");
    assert!(result.exit_code.is_none());
    let progress = result.polling.unwrap();
    assert_eq!(progress.elapsed_ms, 90_000);
    assert_eq!(progress.output_idle_ms, 90_000);
    assert!(!progress.exit_observed);
    assert_eq!(progress.advisory, Some(QUIET_POLL_ADVISORY));
    assert!(!state.is_complete().await);

    // A legitimate long command remains pending, even beyond the advisory.
    tokio::time::advance(Duration::from_secs(3600)).await;
    let later = state.take("quiet-handle", None).await;
    assert_eq!(later.status, "running");
    assert!(later.exit_code.is_none());
    assert!(later.polling.unwrap().advisory.is_some());
    state.finish("completed", Some(7)).await;
    let terminal = state.take("quiet-handle", None).await;
    assert_eq!(terminal.status, "completed");
    assert_eq!(terminal.exit_code, Some(7));
    assert!(terminal.session_id.is_none());
    assert!(terminal.polling.is_none());
}

#[tokio::test(start_paused = true)]
async fn rapid_empty_polls_wait_for_the_silence_boundary_before_advising() {
    let state = PolledShellState::new().unwrap();
    for _ in 0..100 {
        let result = state.take("short-polls", None).await;
        assert!(result.polling.unwrap().advisory.is_none());
    }
    tokio::time::advance(QUIET_POLL_ADVISORY_AFTER - Duration::from_millis(1)).await;
    let before = state.take("short-polls", None).await;
    assert!(before.polling.unwrap().advisory.is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    let at_boundary = state.take("short-polls", None).await;
    assert!(at_boundary.polling.unwrap().advisory.is_some());
}

#[tokio::test(start_paused = true)]
async fn captured_output_resets_quiet_polling_even_when_the_response_cap_omits_it() {
    let state = PolledShellState::new().unwrap();
    tokio::time::advance(QUIET_POLL_ADVISORY_AFTER).await;
    for _ in 0..QUIET_POLL_ADVISORY_READOUTS {
        state.take("output-resumes", None).await;
    }
    // One token cannot render either character, but capture made progress.
    state.push("\0汉".as_bytes()).await;
    let resumed = state.take("output-resumes", Some(1)).await;
    assert!(resumed.output.is_empty());
    assert_eq!(resumed.output_omitted_bytes, 4);
    let progress = resumed.polling.unwrap();
    assert_eq!(progress.elapsed_ms, 90_000);
    assert_eq!(progress.output_idle_ms, 0);
    assert_eq!(progress.consecutive_empty_readouts, 0);
    assert!(progress.advisory.is_none());

    tokio::time::advance(QUIET_POLL_ADVISORY_AFTER).await;
    let first_empty = state.take("output-resumes", None).await;
    assert!(first_empty.polling.unwrap().advisory.is_none());
    state.push(b"").await;
    let second_empty = state.take("output-resumes", None).await;
    assert_eq!(second_empty.polling.unwrap().consecutive_empty_readouts, 2);
    let third_empty = state.take("output-resumes", None).await;
    assert!(third_empty.polling.unwrap().advisory.is_some());
    state.reset_empty_readouts().await;
    let after_input = state.take("output-resumes", None).await;
    let progress = after_input.polling.unwrap();
    assert_eq!(progress.consecutive_empty_readouts, 1);
    assert!(progress.advisory.is_none());
}

#[tokio::test(start_paused = true)]
async fn exit_observed_during_capture_is_not_a_terminal_result() {
    let state = PolledShellState::new().unwrap();
    let pending = serde_json::to_value(state.take("draining", None).await).unwrap();
    assert_eq!(pending["polling"]["exit_observed"], false);
    assert!(pending.get("exit_code").is_none());
    state.mark_process_exited();
    let draining = serde_json::to_value(state.take("draining", None).await).unwrap();
    assert_eq!(draining["polling"]["exit_observed"], true);
    assert_eq!(draining["status"], "running");
    assert_eq!(draining["session_id"], "draining");
    assert!(draining.get("exit_code").is_none());
    state.push(b"tail").await;
    state.finish("completed", Some(0)).await;
    let terminal = serde_json::to_value(state.take("draining", None).await).unwrap();
    assert_eq!(terminal["status"], "completed");
    assert_eq!(terminal["output"], "tail");
    assert_eq!(terminal["exit_code"], 0);
    assert!(terminal.get("session_id").is_none());
    assert!(terminal.get("polling").is_none());
}

#[test]
fn workdir_accepts_workspace_and_nested_paths_but_rejects_other_directories() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let nested = workspace.join("nested");
    let outside = directory.path().join("workspace-other");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let owner = format!("workdir-{}", uuid::Uuid::new_v4());
    workspace_state::publish_resolved_workspace(&owner, workspace.clone());

    assert_eq!(
        resolve_cwd(&owner, None).unwrap(),
        workspace.canonicalize().unwrap()
    );
    assert_eq!(
        resolve_cwd(&owner, Some("nested")).unwrap(),
        nested.canonicalize().unwrap()
    );
    assert_eq!(
        resolve_cwd(&owner, Some(nested.to_str().unwrap())).unwrap(),
        nested.canonicalize().unwrap()
    );
    for requested in ["../workspace-other", outside.to_str().unwrap()] {
        assert!(matches!(
            resolve_cwd(&owner, Some(requested)),
            Err(ToolError::InvalidArguments(message)) if message.contains("within the current session workspace")
        ));
    }
}

#[cfg(unix)]
#[test]
fn workdir_canonicalizes_workspace_and_rejects_symlink_escape() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let nested = workspace.join("nested");
    let outside = directory.path().join("outside");
    let workspace_alias = directory.path().join("workspace-alias");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&workspace, &workspace_alias).unwrap();
    std::os::unix::fs::symlink(&outside, workspace.join("escape")).unwrap();
    let owner = format!("workdir-symlink-{}", uuid::Uuid::new_v4());
    workspace_state::publish_resolved_workspace(&owner, workspace_alias);

    assert_eq!(
        resolve_cwd(&owner, Some("nested")).unwrap(),
        nested.canonicalize().unwrap()
    );
    assert!(matches!(
        resolve_cwd(&owner, Some("escape")),
        Err(ToolError::InvalidArguments(message)) if message.contains("within the current session workspace")
    ));
}

#[cfg(unix)]
mod process_tests {
    use super::*;
    use bamboo_agent_core::{BashCompletionInfo, BashCompletionSink};
    use bamboo_infrastructure::process::{
        CommandEnvironmentDiagnostics, CommandEnvironmentSource, PythonDiscoveryDiagnostics,
    };
    use bamboo_infrastructure::test_support::{
        override_command_environment, CommandEnvironmentOverrideGuard,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn environment() -> CommandEnvironmentOverrideGuard {
        override_command_environment(
            HashMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            CommandEnvironmentDiagnostics {
                source: CommandEnvironmentSource::InheritedProcess,
                import_shell: None,
                import_error: None,
                path: Some("/usr/bin:/bin".into()),
                path_entries: Some(2),
                python: PythonDiscoveryDiagnostics {
                    configured: None,
                    resolved: None,
                    invocation: None,
                    source: None,
                    tried: Vec::new(),
                    tried_preview: Vec::new(),
                    tried_total: 0,
                    tried_truncated: false,
                    hint: None,
                },
            },
        )
    }

    async fn start(cmd: String, owner: &str, interactive: bool) -> Value {
        payload(
            ExecCommandTool::new()
                .invoke(
                    json!({"cmd":cmd,"yield_time_ms":250,"interactive":interactive}),
                    context(owner),
                )
                .await
                .unwrap(),
        )
    }

    async fn poll(id: &str, owner: &str) -> Value {
        payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id":id,"yield_time_ms":2000}),
                    context(owner),
                )
                .await
                .unwrap(),
        )
    }

    // Initial lifecycle fixture loops also stop on directory removal and
    // after 3000 ten-millisecond sleeps. This emergency fallback outlasts the
    // original observation deadlines and does not supply a cleanup assertion.
    struct InitialProcessCleanup<'a> {
        owner: &'a str,
        release: PathBuf,
    }

    impl Drop for InitialProcessCleanup<'_> {
        fn drop(&mut self) {
            // A failed setup/assertion still releases every test-owned fixture.
            // Product PASS is checked before this fallback is used.
            let _ = std::fs::write(&self.release, b"release");
            for id in bash_runtime::uncollected_polled_shells_for_session(self.owner) {
                if let Some(shell) = bash_runtime::get_shell(&id) {
                    let _ = futures::FutureExt::now_or_never(shell.kill());
                }
            }
        }
    }

    async fn await_initial_cleanup(shell: &bash_runtime::ShellSession) -> (bool, bool) {
        let state = shell.polled.as_ref().unwrap();
        state.wait(10_000).await;
        let reaped = state.process_exited();
        // Capture publication can wake us before the completion owner reaches
        // its registry removal. Keep the actual wait/drain proof separate.
        let removed = timeout(Duration::from_secs(2), async {
            while bash_runtime::get_shell(&shell.id).is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .is_ok();
        // Keep expected-red cleanup distinct from the product observation.
        if !state.is_complete().await {
            shell.kill().await.unwrap();
            state.wait(10_000).await;
        }
        assert!(
            state.process_exited(),
            "test cleanup did not actually reap its owned leader/group"
        );
        assert!(
            state.is_complete().await,
            "test cleanup did not finish capture"
        );
        bash_runtime::remove_shell(&shell.id);
        (reaped, removed)
    }

    #[tokio::test]
    async fn cancelled_initial_exec_terminates_descendants_before_registry_removal() {
        let _environment = environment();
        assert!(crate::LocalTaskResources::current().is_none());
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-tree-{}", uuid::Uuid::new_v4());
        workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: directory.path().join("release"),
        };
        let survivor = start("cat".into(), &owner, true).await;
        let survivor_id = survivor["session_id"].as_str().unwrap();
        let tool = ExecCommandTool::new();
        let mut invocation = tool.invoke(json!({"cmd":
            "sh -c 'fixture_dir=$(pwd -P); remaining=3000; printf started > descendant-started; while [ -d \"$fixture_dir\" ] && [ ! -f release ] && [ \"$remaining\" -gt 0 ]; do remaining=$((remaining - 1)); sleep 0.01; done; printf escaped > late-write' & printf '%s' \"$!\" > descendant-pid; wait"
        }), context(&owner));
        assert!(futures::poll!(invocation.as_mut()).is_pending());
        timeout(Duration::from_secs(3), async {
            while !directory.path().join("descendant-started").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the real descendant must start before cancellation");
        let ids = bash_runtime::uncollected_polled_shells_for_session(&owner);
        let id = ids.iter().find(|id| id.as_str() != survivor_id).unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let descendant = std::fs::read_to_string(directory.path().join("descendant-pid")).unwrap();
        assert!(descendant.parse::<u32>().unwrap() > 0);
        drop(invocation);
        let (reaped_before_fallback, removed_after_cleanup) = await_initial_cleanup(&shell).await;
        std::fs::write(directory.path().join("release"), b"release").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let late_write = directory.path().join("late-write").exists();
        let survivor_result = payload(WriteStdinTool::new().invoke(
            json!({"session_id":survivor_id,"chars":"still alive","eof":true,"yield_time_ms":2000}), context(&owner)
        ).await.unwrap());
        assert_eq!(survivor_result["status"], "completed");
        assert_eq!(survivor_result["exit_code"], 0);
        assert_eq!(survivor_result["output"], "still alive");
        eprintln!("INITIAL_TREE_CLEANUP descendant={descendant} reaped={reaped_before_fallback} removed={removed_after_cleanup} late_write={late_write} survivor_completed=true");
        assert!(
            reaped_before_fallback,
            "initial future cancellation did not reap the owned command tree"
        );
        assert!(
            removed_after_cleanup,
            "completed unpublished entry retained registry capacity"
        );
        assert!(
            !late_write,
            "a cancelled descendant executed after owned cleanup"
        );
    }

    #[tokio::test]
    async fn cancelled_initial_exec_after_natural_exit_retains_leader_until_tree_cleanup() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-natural-exit-{}", uuid::Uuid::new_v4());
        workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: directory.path().join("release"),
        };
        let tool = ExecCommandTool::new();
        let mut invocation = tool.invoke(json!({"cmd":
            "sh -c 'fixture_dir=$(pwd -P); remaining=3000; printf started > descendant-started; while [ -d \"$fixture_dir\" ] && [ ! -f release ] && [ \"$remaining\" -gt 0 ]; do remaining=$((remaining - 1)); sleep 0.01; done; printf escaped > late-write' & printf '%s' \"$!\" > descendant-pid; exit 7"
        }), context(&owner));
        // Do not poll the invoking future again: it cannot deliver while its
        // completion task reaches the real natural-exit/capture-ready boundary.
        assert!(futures::poll!(invocation.as_mut()).is_pending());
        let ids = bash_runtime::uncollected_polled_shells_for_session(&owner);
        assert_eq!(ids.len(), 1);
        let shell = bash_runtime::get_shell(&ids[0]).unwrap();
        let handoff = shell.initial_handoff.as_ref().unwrap();
        timeout(Duration::from_secs(5), handoff.capture_ready())
            .await
            .unwrap();
        assert!(
            handoff.retained_leader_has_exited().unwrap(),
            "WNOWAIT must still observe our unreaped leader"
        );
        assert!(
            !shell.polled.as_ref().unwrap().process_exited(),
            "readiness is not an actual wait receipt"
        );
        assert!(directory.path().join("descendant-started").exists());
        drop(invocation);
        let (reaped_before_fallback, removed_after_cleanup) = await_initial_cleanup(&shell).await;
        std::fs::write(directory.path().join("release"), b"release").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let late_write = directory.path().join("late-write").exists();
        eprintln!("INITIAL_NATURAL_EXIT_CLEANUP reaped={reaped_before_fallback} removed={removed_after_cleanup} late_write={late_write}");
        assert!(reaped_before_fallback);
        assert!(removed_after_cleanup);
        assert!(
            !late_write,
            "naturally orphaned descendant survived initial cancellation"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_initial_exec_before_completion_first_poll_retains_cleanup_request() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-before-poll-{}", uuid::Uuid::new_v4());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: directory.path().join("release"),
        };
        let tool = ExecCommandTool::new();
        let mut invocation = tool.invoke(json!({"cmd":"sleep 30 & wait"}), context(&owner));
        assert!(futures::poll!(invocation.as_mut()).is_pending());
        let ids = bash_runtime::uncollected_polled_shells_for_session(&owner);
        assert_eq!(ids.len(), 1);
        let shell = bash_runtime::get_shell(&ids[0]).unwrap();
        drop(invocation);
        // No await has scheduled the completion task: Drop must retain the
        // unpublished entry until that owner actually waits and drains.
        let retained_before_completion = bash_runtime::get_shell(&shell.id).is_some();
        let reaped_before_completion = shell.polled.as_ref().unwrap().process_exited();
        let (reaped_before_fallback, removed_after_cleanup) = await_initial_cleanup(&shell).await;
        assert!(
            retained_before_completion,
            "initial Drop discarded authority before owned cleanup"
        );
        assert!(
            !reaped_before_completion,
            "completion unexpectedly ran before its first poll"
        );
        assert!(
            reaped_before_fallback,
            "initial cancellation was lost before first owner poll"
        );
        assert!(removed_after_cleanup);
    }

    #[tokio::test]
    async fn cancelled_later_poll_preserves_delivered_exec_handle() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("delivered-later-poll-{}", uuid::Uuid::new_v4());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: directory.path().join("release"),
        };
        let first = start("cat".into(), &owner, true).await;
        let id = first["session_id"].as_str().unwrap();
        let tool = WriteStdinTool::new();
        let mut invocation = tool.invoke(json!({"session_id":id}), context(&owner));
        assert!(futures::poll!(invocation.as_mut()).is_pending());
        drop(invocation);
        let shell = bash_runtime::get_shell(id).unwrap();
        shell.polled.as_ref().unwrap().wait(250).await;
        assert!(!shell.polled.as_ref().unwrap().process_exited());
        let result = payload(
            tool.invoke(
                json!({"session_id":id,"chars":"next round","eof":true,"yield_time_ms":2000}),
                context(&owner),
            )
            .await
            .unwrap(),
        );
        assert_eq!(result["status"], "completed");
        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["output"], "next round");
    }

    #[tokio::test]
    async fn terminal_initial_delivery_reaps_before_returning_exact_exit_status() {
        let _environment = environment();
        let result = start(
            "printf out; printf err >&2; exit 7".into(),
            "initial-terminal-exact",
            false,
        )
        .await;
        assert_eq!(result["status"], "completed");
        assert_eq!(result["exit_code"], 7);
        assert!(result.get("session_id").is_none());
        assert!(result["output"].as_str().unwrap().contains("out"));
        assert!(result["output"].as_str().unwrap().contains("err"));
    }

    async fn initial_snapshot_fixture(
        owner: &str,
        directory: &Path,
    ) -> Arc<bash_runtime::ShellSession> {
        let state = PolledShellState::new().unwrap();
        state.push(b"early/").await;
        state.output.lock().await.omitted_bytes = 5;
        bash_runtime::spawn_polled(
            "fixture_dir=$(pwd -P); remaining=3000; while [ -d \"$fixture_dir\" ] && [ ! -f release ] && [ \"$remaining\" -gt 0 ]; do remaining=$((remaining - 1)); sleep 0.01; done; exit 7",
            directory,
            None,
            owner.to_owned(),
            false,
            state,
        )
        .await
        .unwrap()
    }

    // This unit writer controls the capture-state boundary, not an OS pipe
    // fault. The real child and unchanged completion task supply readiness:
    // nobody in this fixture calls mark_capture_ready or supplies a status.
    fn observe_initial_capture_writer(
        shell: &Arc<bash_runtime::ShellSession>,
        release: PathBuf,
        worker: &mut Option<tokio::task::JoinHandle<()>>,
    ) -> Option<(bool, bool)> {
        let state = shell.polled.as_ref().unwrap().clone();
        let handoff = shell.initial_handoff.as_ref().unwrap().clone();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        *worker = Some(tokio::spawn(async move {
            let written = {
                if let Ok(mut output) = state.output.try_lock() {
                    output.bytes.extend_from_slice(b"tail");
                    output.omitted_bytes += 3;
                    output.error = Some("late capture error".to_owned());
                    true
                } else {
                    false
                }
            };
            let ready = if written {
                std::fs::write(release, b"release").unwrap();
                timeout(Duration::from_secs(5), handoff.capture_ready())
                    .await
                    .is_ok()
            } else {
                false
            };
            let _ = send.send((written, ready));
        }));
        // The second runtime worker executes the try_lock attempt while the
        // invoking worker stays at this exact commit boundary; no sleeps race.
        receive.recv_timeout(Duration::from_secs(6)).ok()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn initial_snapshot_commit_preserves_running_tail_error_and_omissions() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-snapshot-running-{}", uuid::Uuid::new_v4());
        let release = directory.path().join("release");
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: release.clone(),
        };
        let shell = initial_snapshot_fixture(&owner, directory.path()).await;
        let state = shell.polled.as_ref().unwrap();
        let mut delivery = InitialDelivery {
            shell: &shell,
            delivered: false,
        };
        let mut worker = None;
        let mut observation = None;
        let first = take_initial_result(&shell, None, &mut delivery, || {
            observation = observe_initial_capture_writer(&shell, release.clone(), &mut worker);
        })
        .await
        .unwrap();
        let worker_result = worker.unwrap().await;
        // A writer held off by the committed snapshot remains retrievable via
        // the running handle. Use the ordinary capture APIs for its next turn.
        state.push(b"tail").await;
        state.record_error("late capture error".to_owned()).await;
        state.output.lock().await.omitted_bytes += 3;
        std::fs::write(&release, b"release").unwrap();
        let last = payload(poll_result(&shell, 5_000, None).await);
        let (reaped, removed) = await_initial_cleanup(&shell).await;
        assert!(worker_result.is_ok());
        assert_eq!(observation, Some((false, false)));
        assert!(delivery.delivered);
        assert_eq!(first.session_id.as_deref(), Some(shell.id.as_str()));
        assert_eq!(first.status, "running");
        assert_eq!(first.output, "early/");
        assert_eq!(first.output_omitted_bytes, 5);
        assert!(first.error.is_none());
        assert!(last.get("session_id").is_none());
        assert_eq!(last["status"], "error");
        assert_eq!(last["exit_code"], 7);
        assert_eq!(last["output"], "tail");
        assert_eq!(last["output_omitted_bytes"], 3);
        assert_eq!(last["error"], "late capture error");
        assert!(reaped && removed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_initial_snapshot_control_exposes_unretrievable_terminal_tail() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-snapshot-old-split-{}", uuid::Uuid::new_v4());
        let release = directory.path().join("release");
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: release.clone(),
        };
        let shell = initial_snapshot_fixture(&owner, directory.path()).await;
        let state = shell.polled.as_ref().unwrap();
        let mut delivery = InitialDelivery {
            shell: &shell,
            delivered: false,
        };
        // Source-bound negative control: the AE9 split (take, then deliver)
        // releases capture before the real second worker completes its write.
        let mut stale = state.take(&shell.id, None).await;
        let mut worker = None;
        let observation = observe_initial_capture_writer(&shell, release, &mut worker);
        let terminal = shell.deliver_initial().unwrap().unwrap();
        stale.session_id = None;
        stale.exit_code = terminal.code();
        stale.status = if stale.error.is_some() {
            "error"
        } else if terminal.code().is_none() {
            "killed"
        } else {
            "completed"
        };
        delivery.delivered = true;
        bash_runtime::remove_shell(&shell.id);
        let worker_result = worker.unwrap().await;
        let (reaped, removed) = await_initial_cleanup(&shell).await;
        let unreachable_tail = state.take(&shell.id, None).await;
        assert!(worker_result.is_ok());
        assert_eq!(observation, Some((true, true)));
        assert!(stale.session_id.is_none());
        assert_eq!(stale.output, "early/");
        assert_eq!(stale.output_omitted_bytes, 5);
        assert!(stale.error.is_none());
        assert_eq!(unreachable_tail.output, "tail");
        assert_eq!(unreachable_tail.output_omitted_bytes, 3);
        assert_eq!(
            unreachable_tail.error.as_deref(),
            Some("late capture error")
        );
        assert!(bash_runtime::get_shell(&shell.id).is_none());
        assert!(reaped && removed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_initial_snapshot_returns_complete_terminal_capture() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-snapshot-ready-{}", uuid::Uuid::new_v4());
        let release = directory.path().join("release");
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: release.clone(),
        };
        let shell = initial_snapshot_fixture(&owner, directory.path()).await;
        let state = shell.polled.as_ref().unwrap();
        state.push(b"tail").await;
        state.record_error("late capture error".to_owned()).await;
        state.output.lock().await.omitted_bytes += 3;
        std::fs::write(release, b"release").unwrap();
        timeout(
            Duration::from_secs(5),
            shell.initial_handoff.as_ref().unwrap().capture_ready(),
        )
        .await
        .unwrap();
        let reaped_before_delivery = state.process_exited();
        let last = payload(poll_initial_result(&shell, 250, None).await.unwrap());
        let (reaped, removed) = await_initial_cleanup(&shell).await;
        assert!(!reaped_before_delivery);
        assert!(last.get("session_id").is_none());
        assert_eq!(last["status"], "error");
        assert_eq!(last["exit_code"], 7);
        assert_eq!(last["output"], "early/tail");
        assert_eq!(last["output_omitted_bytes"], 8);
        assert_eq!(last["error"], "late capture error");
        assert!(reaped && removed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_initial_snapshot_keeps_output_cursor_until_cleanup() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("initial-snapshot-cancelled-{}", uuid::Uuid::new_v4());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: directory.path().join("release"),
        };
        let shell = initial_snapshot_fixture(&owner, directory.path()).await;
        let state = shell.polled.as_ref().unwrap();
        shell.cancel_initial();
        let mut delivery = InitialDelivery {
            shell: &shell,
            delivered: false,
        };
        let result = take_initial_result(&shell, None, &mut delivery, || {}).await;
        drop(delivery);
        let (reaped, removed) = await_initial_cleanup(&shell).await;
        let retained = state.take(&shell.id, None).await;
        assert!(matches!(result, Err(ToolError::Execution(_))));
        assert_eq!(retained.output, "early/");
        assert_eq!(retained.output_omitted_bytes, 5);
        assert!(reaped && removed);
    }

    #[tokio::test]
    async fn fast_completion_drains_stdout_and_stderr_before_returning_final_result() {
        let _environment = environment();
        let result = start("printf 'out'; printf 'err' >&2".into(), "fast-exec", false).await;
        assert_eq!(result["status"], "completed");
        assert_eq!(result["exit_code"], 0);
        assert!(result.get("session_id").is_none());
        let output = result["output"].as_str().unwrap();
        assert!(output.contains("out"));
        assert!(output.contains("err"));
    }

    #[tokio::test]
    async fn exec_command_runs_in_an_authorized_workspace_subdirectory() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let owner = format!("workdir-process-{}", uuid::Uuid::new_v4());
        workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
        let result = payload(
            ExecCommandTool::new()
                .invoke(
                    json!({"cmd":"pwd","workdir":"nested","yield_time_ms":2000}),
                    context(&owner),
                )
                .await
                .unwrap(),
        );
        assert_eq!(result["status"], "completed");
        assert_eq!(result["exit_code"], 0);
        assert_eq!(
            result["output"].as_str().unwrap().trim(),
            nested.canonicalize().unwrap().to_str().unwrap()
        );
    }

    #[tokio::test]
    async fn silent_command_advisory_keeps_the_real_process_and_final_handle() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("quiet-command-{}", uuid::Uuid::new_v4());
        let release = directory.path().join("release");
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: release.clone(),
        };
        workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
        let first = start(
            "fixture_dir=$(pwd -P); remaining=3000; while [ -d \"$fixture_dir\" ] && [ ! -f release ] && [ \"$remaining\" -gt 0 ]; do remaining=$((remaining - 1)); sleep 0.01; done; printf tail; exit 7".into(),
            &owner,
            true,
        ).await;
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let state = shell.polled.as_ref().unwrap();
        // Age only the advisory clock; the real gated process and its actual
        // wait/capture owner remain unchanged and supply the terminal result.
        {
            let mut output = state.output.lock().await;
            output.started_at = Instant::now() - QUIET_POLL_ADVISORY_AFTER;
            output.last_output_at = output.started_at;
        }
        for _ in 0..QUIET_POLL_DIAGNOSIS_READOUTS {
            let result = payload(
                WriteStdinTool::new()
                    .invoke(
                        json!({"session_id":id,"chars":"","yield_time_ms":250}),
                        context(&owner),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(result["status"], "running");
            assert_eq!(result["session_id"], id);
            assert_eq!(result["polling"]["exit_observed"], false);
            assert!(result.get("exit_code").is_none());
        }
        assert!(!state.is_complete().await);
        assert!(bash_runtime::get_shell(id).is_some());
        let last_running = payload(poll_result(&shell, 0, None).await);
        assert!(last_running["polling"]["advisory"].is_string());
        assert_eq!(last_running["polling"]["state"], "needs_diagnosis");
        assert_eq!(last_running["polling"]["terminal_result_ttl_ms"], 300_000);
        // Successful input/EOF interrupts the empty-readout streak even when
        // this command makes no stdout/stderr response to the input.
        for request in [
            json!({"session_id":id,"chars":"probe","yield_time_ms":250}),
            json!({"session_id":id,"eof":true,"yield_time_ms":250}),
        ] {
            let after_input = payload(
                WriteStdinTool::new()
                    .invoke(request, context(&owner))
                    .await
                    .unwrap(),
            );
            assert_eq!(after_input["status"], "running");
            assert_eq!(after_input["polling"]["consecutive_empty_readouts"], 1);
            assert!(after_input["polling"].get("advisory").is_none());
        }
        std::fs::write(release, b"release").unwrap();
        let terminal = poll(id, &owner).await;
        assert_eq!(terminal["status"], "completed");
        assert_eq!(terminal["output"], "tail");
        assert_eq!(terminal["exit_code"], 7);
        assert!(terminal.get("session_id").is_none());
        assert!(terminal.get("polling").is_none());
        assert!(bash_runtime::get_shell(id).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_terminal_cache_expiry_releases_capture_without_inventing_an_outcome() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let owner = format!("quiet-cache-{}", uuid::Uuid::new_v4());
        let release = directory.path().join("release");
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: release.clone(),
        };
        workspace_state::publish_resolved_workspace(&owner, directory.path().to_path_buf());
        let first = start(
            "while [ ! -f release ]; do sleep 0.01; done; printf tail; exit 7".into(),
            &owner,
            false,
        )
        .await;
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let capture = Arc::downgrade(shell.polled.as_ref().unwrap());
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(
            bash_runtime::get_shell(id).is_some(),
            "running handles do not use the terminal-cache TTL"
        );
        assert!(WriteStdinTool::new()
            .invoke(json!({"session_id":id}), context("different-owner"))
            .await
            .is_err());
        tokio::time::resume();
        std::fs::write(release, b"release").unwrap();
        shell.polled.as_ref().unwrap().wait(5_000).await;
        assert!(shell.polled.as_ref().unwrap().is_complete().await);
        assert!(
            bash_runtime::get_shell(id).is_some(),
            "terminal result is initially readable"
        );
        tokio::time::pause();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(
            bash_runtime::COMPLETED_SESSION_TTL_SECS + 1,
        ))
        .await;
        tokio::task::yield_now().await;
        assert!(bash_runtime::get_shell(id).is_none());
        let expired = WriteStdinTool::new()
            .invoke(json!({"session_id":id}), context(&owner))
            .await;
        assert!(
            matches!(expired, Err(ToolError::Execution(ref text)) if text.contains("expire") && text.contains("Host restart"))
        );
        drop(shell);
        assert!(
            capture.upgrade().is_none(),
            "expired capture releases its retained-session permit"
        );
    }

    #[tokio::test]
    async fn yielded_command_poll_returns_only_new_output_and_final_exit() {
        let _environment = environment();
        let gate = tempfile::tempdir().unwrap();
        let release = gate.path().join("release");
        let cmd = format!(
            "printf first; while [ ! -f '{}' ]; do sleep 0.01; done; printf second",
            release.display()
        );
        let first = start(cmd, "incremental-exec", false).await;
        assert_eq!(first["status"], "running");
        let id = first["session_id"].as_str().unwrap();
        std::fs::write(release, b"release").unwrap();
        let last = poll(id, "incremental-exec").await;
        assert_eq!(last["status"], "completed");
        assert_eq!(last["exit_code"], 0);
        assert!(last.get("session_id").is_none());
        assert_eq!(
            format!(
                "{}{}",
                first["output"].as_str().unwrap(),
                last["output"].as_str().unwrap()
            ),
            "firstsecond"
        );
        assert!(bash_runtime::get_shell(id).is_none());
    }

    #[tokio::test]
    async fn nonzero_exit_is_a_completed_command_outcome() {
        let _environment = environment();
        let result = start("printf problem >&2; exit 7".into(), "nonzero-exec", false).await;
        assert_eq!(result["status"], "completed");
        assert_eq!(result["exit_code"], 7);
        assert_eq!(result["output"], "problem");
    }

    #[tokio::test]
    async fn only_owner_can_read_or_write_handle() {
        let _environment = environment();
        let first = start("cat".into(), "owner-exec", true).await;
        let id = first["session_id"].as_str().unwrap();
        let denied = WriteStdinTool::new()
            .invoke(
                json!({"session_id":id,"chars":"stolen\n","eof":true}),
                context("other-exec"),
            )
            .await;
        assert!(matches!(denied, Err(ToolError::Execution(_))));
        let last = payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id":id,"chars":"owned\n","eof":true}),
                    context("owner-exec"),
                )
                .await
                .unwrap(),
        );
        assert_eq!(last["output"], "owned\n");
        assert_eq!(last["exit_code"], 0);
    }

    #[tokio::test]
    async fn legacy_tools_cannot_read_write_or_kill_polled_handles() {
        let _environment = environment();
        let first = start("cat".into(), "legacy-boundary-exec", true).await;
        let id = first["session_id"].as_str().unwrap();
        for owner in ["legacy-boundary-exec", "other-exec"] {
            let read = crate::tools::BashOutputTool::new()
                .invoke(json!({"bash_id":id}), context(owner))
                .await;
            let write = crate::tools::BashInputTool::new()
                .invoke(json!({"bash_id":id,"input":"injected"}), context(owner))
                .await;
            let kill = crate::tools::KillShellTool::new()
                .invoke(json!({"shell_id":id}), context(owner))
                .await;
            assert!(matches!(read, Err(ToolError::Execution(_))));
            assert!(matches!(write, Err(ToolError::Execution(_))));
            assert!(matches!(kill, Err(ToolError::Execution(_))));
        }
        let last = payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id":id,"chars":"owned","eof":true}),
                    context("legacy-boundary-exec"),
                )
                .await
                .unwrap(),
        );
        assert_eq!(last["output"], "owned");
        assert_eq!(last["exit_code"], 0);
    }

    #[tokio::test]
    async fn input_after_process_exit_returns_final_output_and_exit_code() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let release = directory.path().join("release");
        let first = start(
            format!(
                "while [ ! -f '{}' ]; do sleep 0.01; done; printf final; exit 7",
                release.display()
            ),
            "exited-input-exec",
            true,
        )
        .await;
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        std::fs::write(release, b"release").unwrap();
        timeout(Duration::from_secs(2), async {
            while !shell.polled.as_ref().unwrap().is_complete().await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Exercise the completed-process/no-stdin branch deterministically.
        shell.close_stdin().await;
        let last = payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id": id, "chars": "late input"}),
                    context("exited-input-exec"),
                )
                .await
                .unwrap(),
        );
        assert_eq!(last["status"], "completed");
        assert_eq!(last["exit_code"], 7);
        assert_eq!(last["output"], "final");
        assert!(last.get("session_id").is_none());
    }

    #[tokio::test]
    async fn broken_stdin_during_descendant_pipe_drain_keeps_short_yield_and_final_outcome() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let release = directory.path().join("release");
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::write(&self.0, b"release");
            }
        }
        let _cleanup = Cleanup(release.clone());
        let first = start(
            format!(
                "sleep 30 </dev/null & \
                 while [ ! -f '{}' ]; do sleep 0.01; done; printf final; exit 7",
                release.display()
            ),
            "broken-stdin-drain-exec",
            true,
        )
        .await;
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let state = shell.polled.as_ref().unwrap();
        std::fs::write(release, b"release").unwrap();
        timeout(Duration::from_secs(1), async {
            while !state.process_exited() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!state.is_complete().await);
        // The real parent has exited and the descendant has no stdin reader;
        // its inherited output pipes keep completion unpublished for two seconds.
        let error = shell.write_stdin("probe", false).await.unwrap_err();
        assert!(error.contains("Failed to write to stdin"), "{error}");
        let yielded = payload(
            timeout(
                Duration::from_secs(1),
                WriteStdinTool::new().invoke(
                    json!({"session_id":id,"chars":"late input","yield_time_ms":250}),
                    context("broken-stdin-drain-exec"),
                ),
            )
            .await
            .unwrap()
            .unwrap(),
        );
        assert_eq!(yielded["status"], "running");
        assert_eq!(yielded["session_id"], id);
        let last = timeout(Duration::from_secs(5), poll(id, "broken-stdin-drain-exec"))
            .await
            .unwrap();
        assert_eq!(last["status"], "error");
        assert_eq!(last["exit_code"], 7);
        assert!(last["error"]
            .as_str()
            .unwrap()
            .contains("captured output is incomplete"));
        assert!(last.get("session_id").is_none());
        assert_eq!(
            format!(
                "{}{}{}",
                first["output"].as_str().unwrap(),
                yielded["output"].as_str().unwrap(),
                last["output"].as_str().unwrap()
            ),
            "final"
        );
    }

    #[tokio::test]
    async fn live_stdin_errors_stay_errors_and_do_not_consume_unread_output() {
        let _environment = environment();
        let directory = tempfile::tempdir().unwrap();
        let release = directory.path().join("release");
        let finish = directory.path().join("finish");
        struct Release(PathBuf, PathBuf);
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = std::fs::write(&self.0, b"release");
                let _ = std::fs::write(&self.1, b"finish");
            }
        }
        let _release = Release(release.clone(), finish.clone());
        let first = start(
            format!(
                "while [ ! -f '{}' ]; do sleep 0.01; done; printf unread; \
                 while [ ! -f '{}' ]; do sleep 0.01; done; printf done",
                release.display(),
                finish.display()
            ),
            "live-stdin-error-exec",
            false,
        )
        .await;
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let state = shell.polled.as_ref().unwrap();
        std::fs::write(release, b"release").unwrap();
        timeout(Duration::from_secs(1), async {
            while state.output.lock().await.bytes != b"unread" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = timeout(
            Duration::from_secs(1),
            WriteStdinTool::new().invoke(
                json!({"session_id":id,"chars":"not accepted","yield_time_ms":250}),
                context("live-stdin-error-exec"),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error, ToolError::Execution(message) if message.contains("no interactive stdin"))
        );
        assert!(!state.process_exited());
        assert!(!state.is_complete().await);
        assert_eq!(state.output.lock().await.bytes, b"unread");
        std::fs::write(finish, b"finish").unwrap();
        let last = poll(id, "live-stdin-error-exec").await;
        assert_eq!(last["status"], "completed");
        assert_eq!(last["exit_code"], 0);
        assert_eq!(last["output"], "unreaddone");
    }

    #[tokio::test]
    async fn interactive_bytes_and_eof_finish_without_implicit_newline() {
        let _environment = environment();
        let first = start("cat".into(), "interactive-exec", true).await;
        let id = first["session_id"].as_str().unwrap();
        let last = payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id":id,"chars":"answer","eof":true}),
                    context("interactive-exec"),
                )
                .await
                .unwrap(),
        );
        assert_eq!(last["status"], "completed");
        assert_eq!(last["output"], "answer");
        assert_eq!(last["exit_code"], 0);
    }

    #[tokio::test]
    async fn ctrl_c_stops_existing_command_and_returns_drained_result() {
        let _environment = environment();
        let first = start("exec sleep 30".into(), "interrupt-exec", false).await;
        let id = first["session_id"].as_str().unwrap();
        let last = payload(
            WriteStdinTool::new()
                .invoke(
                    json!({"session_id":id,"chars":"\u{3}","yield_time_ms":2000}),
                    context("interrupt-exec"),
                )
                .await
                .unwrap(),
        );
        assert_eq!(last["status"], "killed");
        assert!(last.get("session_id").is_none());
    }

    #[tokio::test]
    async fn inherited_descendant_pipes_have_bounded_drain_and_explicit_incomplete_result() {
        let _environment = environment();
        let cmd = "sleep 30 & printf done".to_owned();
        let first = start(cmd, "inherited-pipes-exec", false).await;
        let id = first["session_id"].as_str().unwrap();
        let last = timeout(Duration::from_secs(5), poll(id, "inherited-pipes-exec"))
            .await
            .unwrap();
        assert_eq!(last["status"], "error");
        assert!(last.get("session_id").is_none());
        assert!(last["error"]
            .as_str()
            .unwrap()
            .contains("captured output is incomplete"));
        assert_eq!(
            format!(
                "{}{}",
                first["output"].as_str().unwrap(),
                last["output"].as_str().unwrap()
            ),
            "done"
        );
    }

    #[derive(Default)]
    struct CompletionCounter(AtomicUsize);
    impl BashCompletionSink for CompletionCounter {
        fn on_bash_completed(&self, _info: BashCompletionInfo) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn polled_handles_preserve_owner_without_legacy_wait_or_completion_push() {
        let _environment = environment();
        let sink = Arc::new(CompletionCounter::default());
        let mut ctx = context("pull-exec");
        ctx.can_async_resume = true;
        ctx.bash_completion_sink = Some(sink.clone());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
        ctx.event_tx = Some(event_tx);
        let first = payload(
            ExecCommandTool::new()
                .invoke(
                    json!({"cmd":"cat","interactive":true,"yield_time_ms":250}),
                    ctx,
                )
                .await
                .unwrap(),
        );
        let id = first["session_id"].as_str().unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        assert_eq!(shell.session_id.as_deref(), Some("pull-exec"));
        assert!(bash_runtime::running_shells_for_session("pull-exec").is_empty());
        payload(
            WriteStdinTool::new()
                .invoke(json!({"session_id":id,"eof":true}), context("pull-exec"))
                .await
                .unwrap(),
        );
        assert_eq!(sink.0.load(Ordering::SeqCst), 0);
        assert!(timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn local_task_drain_retains_final_output_until_the_model_polls() {
        let _environment = environment();
        let resources =
            crate::LocalTaskResources::new("local-exec", "local-run", chrono::Utc::now());
        let first = resources
            .scope(start(
                "printf before; exec sleep 30".into(),
                "local-exec",
                false,
            ))
            .await;
        let id = first["session_id"].as_str().unwrap();
        timeout(Duration::from_secs(5), resources.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert!(
            bash_runtime::get_shell(id).is_some(),
            "finished output must survive resource finalization"
        );
        let last = poll(id, "local-exec").await;
        assert!(last.get("session_id").is_none());
        assert_eq!(last["status"], "killed");
        assert_eq!(
            format!(
                "{}{}",
                first["output"].as_str().unwrap(),
                last["output"].as_str().unwrap()
            ),
            "before"
        );
    }
    // Re-execute this synchronous test in a real OS child. Its retained
    // std::process::Child is the sleeper's sole reap/kill authority; unlike a
    // shell job table, it cannot asynchronously reap and recycle that PID.
    const FORK_HELPER_TEST: &str =
        "tools::exec_command::tests::process_tests::initial_exec_fork_fixture_process";

    #[test]
    fn initial_exec_fork_fixture_process() {
        let Ok(directory) = std::env::var("BAMBOO_INITIAL_EXEC_FIXTURE_DIR") else {
            return;
        };
        let directory = PathBuf::from(directory);
        let name = std::env::var("BAMBOO_INITIAL_EXEC_FIXTURE_NAME").unwrap();
        assert!(["target", "survivor", "unrelated"].contains(&name.as_str()));
        if !directory.exists() {
            return;
        }
        struct Sleeper(Option<std::process::Child>);
        impl Sleeper {
            fn stop(&mut self) -> std::io::Result<()> {
                let Some(mut child) = self.0.take() else {
                    return Ok(());
                };
                // Child retains the unreaped identity even if sleep exited.
                // Consume it once: an uncertain wait never permits a PID retry.
                let killed = child.kill();
                let waited = child.wait();
                killed?;
                waited.map(|_| ())
            }
        }
        impl Drop for Sleeper {
            fn drop(&mut self) {
                if let Err(error) = self.stop() {
                    eprintln!("fork helper cleanup unconfirmed: {error}");
                }
            }
        }
        let child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let mut sleeper = Sleeper(Some(child));
        std::fs::write(
            directory.join(format!("{name}.sleep")),
            sleeper.0.as_ref().unwrap().id().to_string(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("{name}.helper")),
            std::process::id().to_string(),
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while directory.exists()
            && !directory.join(format!("{name}.release")).exists()
            && std::time::Instant::now() < deadline
        {
            if directory.join(format!("{name}.probe")).exists() {
                let _ = std::fs::write(directory.join(format!("{name}.alive")), b"alive");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        sleeper
            .stop()
            .expect("fork helper must kill and reap its owned sleeper");
        let _ = std::fs::write(directory.join(format!("{name}.cleaned")), b"cleaned");
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    struct ForkFixture {
        directory: tempfile::TempDir,
    }

    impl ForkFixture {
        fn new() -> Self {
            Self {
                directory: tempfile::tempdir().unwrap(),
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        fn helper_command(&self, name: &str) -> String {
            format!("BAMBOO_INITIAL_EXEC_FIXTURE_DIR={} BAMBOO_INITIAL_EXEC_FIXTURE_NAME={} {} --exact {} --test-threads=1 --nocapture",
                shell_quote(self.directory.path().to_str().unwrap()), shell_quote(name),
                shell_quote(std::env::current_exe().unwrap().to_str().unwrap()), shell_quote(FORK_HELPER_TEST))
        }

        fn command(&self, early_exit: bool) -> String {
            format!(
                "printf '%s' \"$$\" > target.leader; {} & {}",
                self.helper_command("target"),
                if early_exit { "exit 7" } else { "wait" }
            )
        }

        fn release(&self) {
            for name in ["target", "survivor", "unrelated"] {
                let _ = std::fs::write(self.path(&format!("{name}.release")), b"release");
            }
        }

        fn ready(&self, name: &str) -> bool {
            ["sleep", "helper"].into_iter().all(|suffix| {
                std::fs::read_to_string(self.path(&format!("{name}.{suffix}")))
                    .ok()
                    .and_then(|text| text.parse::<u32>().ok())
                    .is_some_and(|pid| pid > 0)
            })
        }

        fn pid(&self, name: &str) -> u32 {
            std::fs::read_to_string(self.path(name))
                .unwrap()
                .parse()
                .unwrap()
        }
    }

    impl Drop for ForkFixture {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct UnrelatedFork {
        child: std::process::Child,
        release: PathBuf,
    }

    impl Drop for UnrelatedFork {
        fn drop(&mut self) {
            let _ = std::fs::write(&self.release, b"release");
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                match self.child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!(
                            "unrelated fixture cleanup could not confirm child ownership: {error}"
                        );
                        return;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            // The actual Child remains unreaped and was created in this
            // fixture's private group. Never signal any marker-file PID.
            if unsafe { libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL) } != 0 {
                eprintln!(
                    "unrelated fixture group cleanup request failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            if let Err(error) = self.child.wait() {
                eprintln!("unrelated fixture reap unconfirmed: {error}");
            }
        }
    }

    fn fork_process_running(pid: u32) -> bool {
        // Read-only observation works on Darwin and Linux and rejects zombies.
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&output.stdout);
        !state.trim().is_empty() && !state.trim_start().starts_with(['Z', 'X'])
    }

    async fn fork_ready(fixture: &ForkFixture, name: &str) {
        timeout(Duration::from_secs(2), async {
            while !fixture.ready(name) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("real forked descendant did not start");
    }

    async fn cancelled_initial_fork(
        early_exit: bool,
        before_first_poll: bool,
        after_prepared: bool,
    ) {
        let _environment = environment();
        assert!(crate::LocalTaskResources::current().is_none());
        let fixture = ForkFixture::new();
        let owner = format!("initial-fork-{}", uuid::Uuid::new_v4());
        let _cleanup = InitialProcessCleanup {
            owner: &owner,
            release: fixture.path("target.release"),
        };
        workspace_state::publish_resolved_workspace(&owner, fixture.directory.path().to_path_buf());
        let survivor = start(
            format!("{} & wait", fixture.helper_command("survivor")),
            &owner,
            false,
        )
        .await;
        let survivor_id = survivor["session_id"].as_str().unwrap();
        fork_ready(&fixture, "survivor").await;
        use std::os::unix::process::CommandExt;
        let mut unrelated = UnrelatedFork {
            child: std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    FORK_HELPER_TEST,
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env("BAMBOO_INITIAL_EXEC_FIXTURE_DIR", fixture.directory.path())
                .env("BAMBOO_INITIAL_EXEC_FIXTURE_NAME", "unrelated")
                .process_group(0)
                .spawn()
                .unwrap(),
            release: fixture.path("unrelated.release"),
        };
        fork_ready(&fixture, "unrelated").await;

        let tool = ExecCommandTool::new();
        let mut invocation =
            tool.invoke(json!({"cmd":fixture.command(early_exit)}), context(&owner));
        assert!(futures::poll!(invocation.as_mut()).is_pending());
        if before_first_poll {
            // Do not yield this current-thread runtime. OS children can fork,
            // but neither product supervisor nor completion task can poll yet.
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !fixture.ready("target") && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(fixture.ready("target"));
        } else {
            fork_ready(&fixture, "target").await;
        }
        let ids = bash_runtime::uncollected_polled_shells_for_session(&owner);
        let id = ids.iter().find(|id| id.as_str() != survivor_id).unwrap();
        let shell = bash_runtime::get_shell(id).unwrap();
        let state = shell.polled.as_ref().unwrap();
        let target_sleep = fixture.pid("target.sleep");
        let target_helper = fixture.pid("target.helper");
        let survivor_sleep = fixture.pid("survivor.sleep");
        let unrelated_sleep = fixture.pid("unrelated.sleep");
        if early_exit {
            timeout(Duration::from_secs(2), async {
                while fork_process_running(fixture.pid("target.leader")) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("leader did not exit naturally");
        }
        if after_prepared {
            // Leave invoke unpolled after its first suspension. Its real
            // completion task reaches the existing two-second pipe timeout,
            // but this caller still has not committed initial delivery.
            timeout(Duration::from_secs(3), async {
                while state.output.lock().await.error.is_none() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("bounded incomplete result was not prepared");
        }
        let descendant_started = fork_process_running(target_sleep);
        drop(invocation);
        state.wait(2000).await;
        let cancelled_sleep_stopped = !fork_process_running(target_sleep);
        let cancelled_helper_stopped = !fork_process_running(target_helper);
        let registry_removed = bash_runtime::get_shell(id).is_none();
        let actually_reaped = state.process_exited();

        // Cancelling a later poll must not acquire initial cancellation power.
        let poll_tool = WriteStdinTool::new();
        let mut later = poll_tool.invoke(json!({"session_id":survivor_id}), context(&owner));
        assert!(futures::poll!(later.as_mut()).is_pending());
        drop(later);
        std::fs::write(fixture.path("survivor.probe"), b"probe").unwrap();
        std::fs::write(fixture.path("unrelated.probe"), b"probe").unwrap();
        let controls_answered = timeout(Duration::from_secs(2), async {
            while !fixture.path("survivor.alive").exists()
                || !fixture.path("unrelated.alive").exists()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .is_ok();
        let controls_running =
            fork_process_running(survivor_sleep) && fork_process_running(unrelated_sleep);

        // Always complete RED/control cleanup before asserting the captured
        // product failure. Each Rust helper owns and waits its own sleeper.
        fixture.release();
        let cleanup_stopped = timeout(Duration::from_secs(3), async {
            while [target_sleep, target_helper, survivor_sleep, unrelated_sleep]
                .into_iter()
                .any(fork_process_running)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        state.wait(2000).await;
        let cleanup_capture_complete = state.is_complete().await;
        let survivor_result = poll(survivor_id, &owner).await;
        let unrelated_status = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(status) = unrelated.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("unrelated control was not reaped");
        bash_runtime::remove_shell(id);
        assert!(
            cleanup_stopped && cleanup_capture_complete,
            "fork fixture cleanup did not finish"
        );
        assert_eq!(survivor_result["status"], "completed");
        assert!(survivor_result.get("session_id").is_none());
        assert!(unrelated_status.success());
        eprintln!("INITIAL_EXEC_FORK_CLEANUP_CONFIRMED early_exit={early_exit} before_first_poll={before_first_poll} after_prepared={after_prepared} all_sleepers_stopped=true survivor_reaped=true unrelated_reaped=true capture_complete=true");
        assert!(
            descendant_started,
            "fixture did not exercise a live long-lived descendant"
        );
        assert!(
            controls_answered && controls_running,
            "initial cancellation stopped a delivered or unrelated sibling"
        );
        assert!(
            registry_removed && actually_reaped,
            "initial cancellation did not remove/reap its leader"
        );
        assert!(
            cancelled_sleep_stopped && cancelled_helper_stopped,
            "cancelled initial exec left a forked descendant alive"
        );
    }

    #[tokio::test]
    async fn cancelled_initial_exec_stops_long_lived_fork_without_stopping_siblings() {
        cancelled_initial_fork(false, false, false).await;
    }

    #[tokio::test]
    async fn cancelled_initial_exec_fork_before_supervisor_first_poll() {
        cancelled_initial_fork(false, true, false).await;
    }

    #[tokio::test]
    async fn cancelled_initial_exec_fork_after_natural_leader_exit() {
        cancelled_initial_fork(true, false, false).await;
    }

    #[tokio::test]
    async fn cancelled_initial_exec_fork_after_incomplete_result_preparation() {
        cancelled_initial_fork(true, false, true).await;
    }
}
