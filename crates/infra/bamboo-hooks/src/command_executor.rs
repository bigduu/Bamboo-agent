//! Optional host-owned command execution. Standalone/legacy hooks keep their
//! existing runner; native hosts bind this scope without a reverse dependency.
use std::{future::Future, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::process::Command;

pub const HOOK_COMMAND_OUTPUT_LIMIT_BYTES: usize = 64 * 1024;

#[derive(Debug, Default)]
pub struct CapturedHookOutput {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Debug)]
pub struct HookCommandOutput {
    pub exit_code: Option<i32>,
    pub stdout: CapturedHookOutput,
    pub stderr: CapturedHookOutput,
    pub timed_out: bool,
}

/// Executes a command under the host's resource lifetime. These values are
/// command output only; this interface cannot issue a resource stop receipt.
#[async_trait]
pub trait HookCommandExecutor: Send + Sync {
    async fn execute(
        &self,
        command: Command,
        input: Vec<u8>,
        timeout: Duration,
    ) -> io::Result<HookCommandOutput>;
}

tokio::task_local! {
    static EXECUTOR: Arc<dyn HookCommandExecutor>;
}

pub async fn with_hook_command_executor<F: Future>(
    executor: Arc<dyn HookCommandExecutor>,
    future: F,
) -> F::Output {
    EXECUTOR.scope(executor, future).await
}

pub(crate) fn current() -> Option<Arc<dyn HookCommandExecutor>> {
    EXECUTOR.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct StubExecutor(Mutex<Vec<String>>);
    #[async_trait]
    impl HookCommandExecutor for StubExecutor {
        async fn execute(
            &self,
            command: Command,
            _input: Vec<u8>,
            _timeout: Duration,
        ) -> io::Result<HookCommandOutput> {
            let program = command
                .as_std()
                .get_program()
                .to_string_lossy()
                .into_owned();
            self.0.lock().unwrap().push(program.clone());
            if program == "node" {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            Ok(HookCommandOutput {
                exit_code: Some(0),
                stdout: CapturedHookOutput {
                    bytes: b"owned executor".to_vec(),
                    truncated: false,
                },
                stderr: CapturedHookOutput::default(),
                timed_out: false,
            })
        }
    }

    #[tokio::test]
    async fn owned_hook_executor_covers_shell_script_fallback_and_portable_commands() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("hook.js");
        std::fs::write(
            &path,
            "throw new Error('must not run through legacy runner');",
        )
        .unwrap();
        let executor = Arc::new(StubExecutor(Mutex::new(Vec::new())));
        with_hook_command_executor(executor.clone(), async {
            let shell = crate::test_lifecycle_shell_command(
                "PreToolUse",
                "must-not-run",
                100,
                Some(temp.path().to_owned()),
            )
            .await
            .unwrap();
            assert_eq!(shell.stdout, "owned executor");
            let script = crate::test_lifecycle_handler(
                "PreToolUse",
                &bamboo_config::LifecycleHookHandler::script(
                    path.to_string_lossy(),
                    bamboo_config::LifecycleScriptRunner::Auto,
                    100,
                ),
                Some(temp.path().to_owned()),
            )
            .await
            .unwrap();
            assert_eq!(script.stdout, "owned executor");
            let portable = crate::configured::execute_portable_command(
                "must-not-run",
                vec![],
                1,
                temp.path(),
                temp.path(),
                temp.path(),
            )
            .await
            .unwrap();
            assert_eq!(portable.stdout, "owned executor");
        })
        .await;
        let programs = executor.0.lock().unwrap();
        assert_eq!(programs.len(), 4);
        assert_eq!(programs[1], "node");
        assert_eq!(programs[2], "bun");
        assert!(current().is_none());
    }
}
