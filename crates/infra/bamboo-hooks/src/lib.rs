//! Lifecycle hook registry and configured handler runtimes.
//!
//! The agent engine owns lifecycle seams and applies returned decisions. This
//! crate owns handler matching, deterministic dispatch, command execution, and
//! external script runtime selection.

mod configured;
mod dispatcher;

pub use bamboo_config::LifecycleScriptRunner;
pub use configured::{
    test_lifecycle_handler, test_lifecycle_shell_command, LifecycleHookEvent,
    LifecycleHookTestOutput, ScriptHook, ShellCommandHook, ShellHookEvent, ShellHookTestOutput,
};
pub use dispatcher::{HookDispatchReport, HookDispatcher, HookExecution, HookRunOutcome};
