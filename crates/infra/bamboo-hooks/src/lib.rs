//! Lifecycle hook registry and configured handler runtimes.
//!
//! The agent engine owns lifecycle seams and applies returned decisions. This
//! crate owns handler matching, deterministic dispatch, command execution, and
//! the isolated JavaScript runtime.

mod configured;
mod dispatcher;

pub use configured::{
    test_lifecycle_handler, test_lifecycle_shell_command, JavaScriptHook, LifecycleHookEvent,
    LifecycleHookTestOutput, ShellCommandHook, ShellHookEvent, ShellHookTestOutput,
};
pub use dispatcher::{HookDispatchReport, HookDispatcher, HookExecution, HookRunOutcome};
