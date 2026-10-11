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
    assert_eq!(result.output, "xxxx");
    assert_eq!(result.output_omitted_bytes, MAX_CAPTURE_BYTES + 96);
    let second = state.take("unused", None).await;
    assert!(second.output.is_empty());
    assert_eq!(second.output_omitted_bytes, 0);
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
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("descendant.pid");
        struct CleanupDescendant(PathBuf);
        impl Drop for CleanupDescendant {
            fn drop(&mut self) {
                if let Ok(pid) = std::fs::read_to_string(&self.0) {
                    if let Ok(pid) = pid.trim().parse::<i32>() {
                        unsafe {
                            libc::kill(pid, libc::SIGKILL);
                        }
                    }
                }
            }
        }
        let _cleanup = CleanupDescendant(pid_file.clone());
        let cmd = format!(
            "sleep 30 & printf '%s' $! > '{}'; printf done",
            pid_file.display()
        );
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
