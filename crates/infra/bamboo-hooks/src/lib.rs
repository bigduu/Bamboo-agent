//! Lifecycle hook registry and configured handler runtimes.
//!
//! The agent engine owns lifecycle seams and applies returned decisions. This
//! crate owns handler matching, deterministic dispatch, command execution, and
//! external script runtime selection.

mod command_executor;
mod configured;
mod dispatcher;
pub mod portable;
mod shell_compatibility;

pub use bamboo_config::LifecycleScriptRunner;
pub use configured::{
    test_lifecycle_handler, test_lifecycle_shell_command, LifecycleHookEvent,
    LifecycleHookTestOutput, ScriptHook, ShellCommandHook, ShellHookEvent, ShellHookTestOutput,
};
pub use dispatcher::{HookDispatchReport, HookDispatcher, HookExecution, HookRunOutcome};

pub use command_executor::{
    with_hook_command_executor, CapturedHookOutput, HookCommandExecutor, HookCommandOutput,
    HOOK_COMMAND_OUTPUT_LIMIT_BYTES,
};

pub use shell_compatibility::requires_complete_shell_policy_checks;
