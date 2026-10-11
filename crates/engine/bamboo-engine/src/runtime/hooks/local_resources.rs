//! Native hook lifetimes use the same process and blocking-work owner as tools.
use std::{future::Future, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use bamboo_hooks::{
    CapturedHookOutput, HookCommandExecutor, HookCommandOutput, HOOK_COMMAND_OUTPUT_LIMIT_BYTES,
};
use bamboo_tools::local_task_resources::{LocalTaskChild, LocalTaskResources};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

/// Unlike a tool invocation guard, this guard cannot be disarmed by a nested
/// builtin tool. A custom hook may start unknown work after that tool returns.
struct HookInvocationGuard {
    resources: LocalTaskResources,
    completed: bool,
}
impl Drop for HookInvocationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.resources.record_unconfirmed(
                "hook invocation was interrupted; external or blocking work stop is unconfirmed",
            );
        }
    }
}

pub(super) async fn run<F: Future>(future: F) -> F::Output {
    let Some(resources) = LocalTaskResources::current() else {
        return future.await;
    };
    let mut guard = HookInvocationGuard {
        resources: resources.clone(),
        completed: false,
    };
    let output = bamboo_hooks::with_hook_command_executor(
        Arc::new(OwnedHookCommandExecutor(resources)),
        future,
    )
    .await;
    guard.completed = true;
    output
}

struct OwnedHookCommandExecutor(LocalTaskResources);
#[async_trait]
impl HookCommandExecutor for OwnedHookCommandExecutor {
    async fn execute(
        &self,
        mut command: Command,
        input: Vec<u8>,
        timeout: Duration,
    ) -> io::Result<HookCommandOutput> {
        let resources = self.0.clone();
        // spawn_owned rebinds LocalTaskResources inside its task. The caller
        // only holds a receiver, so dropping the hook cannot drop the capture
        // task or its process supervisor.
        let result = self.0.spawn_owned(async move {
            // Failed admission/spawn has not run a hook, and preserves NotFound
            // for the configured script runner's executable fallback.
            let child = LocalTaskChild::spawn(&mut command)?;
            let result = capture(child, input, timeout).await;
            if let Err(error) = &result {
                resources.record_unconfirmed(format!(
                    "owned hook resources could not be finalized: {error}"
                ));
            }
            result
        })?;
        result.await.map_err(io::Error::other)?
    }
}

async fn capture(
    mut child: LocalTaskChild,
    input: Vec<u8>,
    timeout: Duration,
) -> io::Result<HookCommandOutput> {
    let mut stdin = child
        .take_stdin()
        .ok_or_else(|| io::Error::other("owned lifecycle hook stdin is missing"))?;
    let stdout = child
        .take_stdout()
        .ok_or_else(|| io::Error::other("owned lifecycle hook stdout is missing"))?;
    let stderr = child
        .take_stderr()
        .ok_or_else(|| io::Error::other("owned lifecycle hook stderr is missing"))?;

    // No detached pump tasks: all pipe work remains inside the registry-owned
    // capture future. Keep it alive across the command timeout so we still
    // establish EOF after killing/draining the owned process group or Job.
    let io = async move {
        let (input_result, stdout, stderr) = tokio::join!(
            async move {
                let result = stdin.write_all(&input).await;
                let _ = stdin.shutdown().await;
                result
            },
            read_capped(stdout),
            read_capped(stderr),
        );
        if let Err(error) = input_result {
            // Hooks may intentionally ignore their input and close stdin.
            if error.kind() != io::ErrorKind::BrokenPipe {
                return Err(error);
            }
        }
        Ok::<_, io::Error>((stdout?, stderr?))
    };
    tokio::pin!(io);
    let mut io_result = None;
    let completion = tokio::time::timeout(timeout, async {
        tokio::join!(child.wait(), async {
            io_result = Some((&mut io).await);
        })
    })
    .await;
    let (exit_code, timed_out, stdout, stderr) = match completion {
        Ok((status, ())) => {
            let (stdout, stderr) = io_result.expect("joined hook I/O result")?;
            (status?.code(), false, stdout, stderr)
        }
        Err(_) => {
            // A kill request is not completion. This joins the same process
            // supervisor the resource domain will drain before any receipt.
            child.kill().await?;
            // An escaped pipe holder must never turn a timeout into successful
            // resource proof. Bound this cleanup, return an error, and poison.
            let (stdout, stderr) = match io_result {
                Some(result) => result?,
                None => tokio::time::timeout(Duration::from_secs(1), &mut io)
                    .await
                    .map_err(|_| io::Error::other("owned hook pipe holders did not stop"))??,
            };
            (None, true, stdout, stderr)
        }
    };
    Ok(HookCommandOutput {
        exit_code,
        stdout,
        stderr,
        timed_out,
    })
}

async fn read_capped(mut reader: impl AsyncRead + Unpin) -> io::Result<CapturedHookOutput> {
    let mut captured = CapturedHookOutput::default();
    let mut chunk = [0; 8192];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            return Ok(captured);
        }
        let keep = HOOK_COMMAND_OUTPUT_LIMIT_BYTES
            .saturating_sub(captured.bytes.len())
            .min(count);
        captured.bytes.extend_from_slice(&chunk[..keep]);
        captured.truncated |= keep < count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{AgentHook, Session};
    use bamboo_domain::{AgentHookPoint, AgentRuntimeState, HookPayload, HookResult};
    use chrono::Utc;

    struct PendingHook(AgentHookPoint);
    #[async_trait]
    impl AgentHook for PendingHook {
        fn point(&self) -> AgentHookPoint {
            self.0
        }
        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn local_hook_resources_cancelled_custom_and_observer_hooks_deny_receipts() {
        for point in [
            AgentHookPoint::BeforeToolExecution,
            AgentHookPoint::AfterToolExecution,
            AgentHookPoint::AfterSessionEnd,
        ] {
            let resources = LocalTaskResources::new("hook", "cancelled", Utc::now());
            let mut runner = super::super::HookRunner::new();
            runner.register(Arc::new(PendingHook(point)));
            let session = Session::new("hook", "model");
            let mut state = AgentRuntimeState::new("run");
            let payload = HookPayload::Finalize {
                stop_hook_active: false,
            };
            let invocation = async {
                if point == AgentHookPoint::AfterSessionEnd {
                    runner
                        .run_observer_hooks(point, &payload, &session, &mut state, None)
                        .await;
                } else {
                    runner
                        .run_hooks(point, &payload, &session, &mut state, None)
                        .await;
                }
            };
            assert!(
                tokio::time::timeout(Duration::from_millis(10), resources.scope(invocation),)
                    .await
                    .is_err()
            );
            let error = resources.close_and_drain().await.unwrap_err();
            assert!(error
                .to_string()
                .contains("hook invocation was interrupted"));
        }
    }

    #[tokio::test]
    async fn local_hook_resources_completed_inline_hook_allows_receipt() {
        let resources = LocalTaskResources::new("hook", "completed", Utc::now());
        assert_eq!(resources.scope(run(async { 42 })).await, 42);
        resources.close_and_drain().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hook_resources_normal_exit_drains_redirected_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let resources = LocalTaskResources::new("hook", "descendants", Utc::now());
        let output = resources
            .scope(run(bamboo_hooks::test_lifecycle_shell_command(
                "PreToolUse",
                "(sleep 0.3; touch late-write) >/dev/null 2>&1 & printf owned",
                2_000,
                Some(temp.path().to_owned()),
            )))
            .await
            .unwrap();
        assert_eq!(output.exit_code, Some(0));
        assert_eq!(output.stdout, "owned");
        assert!(!output.timed_out);
        resources.close_and_drain().await.unwrap();
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(!temp.path().join("late-write").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hook_resources_timeout_drains_process_and_pipe_work() {
        let resources = LocalTaskResources::new("hook", "timeout", Utc::now());
        let output = resources
            .scope(run(bamboo_hooks::test_lifecycle_shell_command(
                "PreToolUse",
                "printf before-timeout; sleep 30",
                50,
                None,
            )))
            .await
            .unwrap();
        assert!(output.timed_out);
        assert_eq!(output.stdout, "before-timeout");
        resources.close_and_drain().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hook_resources_cancelled_command_still_drains_owned_processes() {
        let temp = tempfile::tempdir().unwrap();
        let resources = LocalTaskResources::new("hook", "cancelled-command", Utc::now());
        let mut invocation = Box::pin(resources.scope(run(
            bamboo_hooks::test_lifecycle_shell_command(
                "PreToolUse",
                "(sleep 0.4; touch late-write) >/dev/null 2>&1 & touch started; sleep 30",
                30_000,
                Some(temp.path().to_owned()),
            ),
        )));
        let started = async {
            loop {
                if temp.path().join("started").exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            _ = &mut invocation => panic!("hook unexpectedly completed"),
            result = tokio::time::timeout(Duration::from_secs(5), started) => result.unwrap(),
        }
        drop(invocation);
        assert!(resources.close_and_drain().await.is_err());
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(!temp.path().join("late-write").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hook_resources_timeout_after_pipe_eof_does_not_repoll_completed_io() {
        let resources = LocalTaskResources::new("hook", "closed-pipes", Utc::now());
        let output = resources
            .scope(run(bamboo_hooks::test_lifecycle_shell_command(
                "PreToolUse",
                "exec 0<&- 1>&- 2>&-; sleep 30",
                50,
                None,
            )))
            .await
            .unwrap();
        assert!(output.timed_out);
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_hook_resources_output_capture_is_bounded() {
        let bytes = vec![b'x'; HOOK_COMMAND_OUTPUT_LIMIT_BYTES + 9];
        let output = read_capped(bytes.as_slice()).await.unwrap();
        assert!(output.truncated);
        assert_eq!(output.bytes.len(), HOOK_COMMAND_OUTPUT_LIMIT_BYTES);
    }
}
