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
            "sh -c 'printf started > descendant-started; while [ ! -f release ]; do sleep 0.01; done; printf escaped > late-write' & printf '%s' \"$!\" > descendant-pid; wait"
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
            "sh -c 'printf started > descendant-started; while [ ! -f release ]; do sleep 0.01; done; printf escaped > late-write' & printf '%s' \"$!\" > descendant-pid; exit 7"
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
}
