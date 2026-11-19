//! Permission selection for the concrete current-session loop overlay.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

use bamboo_agent_core::{ToolCall, ToolError};
use serde_json::Value;

pub const SESSION_LOOP_EXECUTION_CONTRACT: &str = "bamboo.session_loop.current_human.v1";

struct SelectedLoop {
    call: ToolCall,
    args: Value,
    evaluated: AtomicBool,
}

tokio::task_local! {
    static SELECTED_LOOP: SelectedLoop;
}

pub fn is_session_loop_mutation(args: &Value) -> bool {
    matches!(
        args.get("action").and_then(Value::as_str),
        Some("start" | "stop" | "resume")
    )
}

/// Wrap the existing permission chain for the exact Host overlay and call.
/// This selects a policy evaluation and grants no input or Session authority.
pub async fn with_framework_session_loop_permission<F, T>(
    call: &ToolCall,
    args: &Value,
    execution_contract: Option<&'static str>,
    check: F,
) -> Result<T, ToolError>
where
    F: Future<Output = Result<T, ToolError>>,
{
    if execution_contract != Some(SESSION_LOOP_EXECUTION_CONTRACT)
        || !is_session_loop_mutation(args)
    {
        return Err(ToolError::Execution(
            "Current-session loop permission owner unavailable".into(),
        ));
    }
    SELECTED_LOOP
        .scope(
            SelectedLoop {
                call: call.clone(),
                args: args.clone(),
                evaluated: AtomicBool::new(false),
            },
            async {
                let result = check.await?;
                if !SELECTED_LOOP.with(|selection| selection.evaluated.load(Ordering::SeqCst)) {
                    return Err(ToolError::Execution(
                        "Current-session loop requires the Host permission policy".into(),
                    ));
                }
                Ok(result)
            },
        )
        .await
}

pub(crate) fn selected_framework_session_loop(
    call: &ToolCall,
    execution_name: &str,
    args: &Value,
) -> bool {
    execution_name == "session_loop"
        && SELECTED_LOOP
            .try_with(|selection| selection.call == *call && selection.args == *args)
            .unwrap_or(false)
}

pub(crate) fn record_framework_session_loop_evaluation(call: &ToolCall, args: &Value) {
    if selected_framework_session_loop(call, "session_loop", args) {
        let _ = SELECTED_LOOP.try_with(|selection| {
            selection.evaluated.store(true, Ordering::SeqCst);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::FunctionCall;
    use serde_json::json;

    #[tokio::test]
    async fn session_loop_permission_requires_the_exact_owner_call_and_checker() {
        let args = json!({"action":"start","prompt":"private task"});
        let call = ToolCall {
            id: "loop-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_loop".into(),
                arguments: args.to_string(),
            },
        };
        let unauthenticated =
            with_framework_session_loop_permission(&call, &args, None, async { Ok(()) }).await;
        assert!(unauthenticated.is_err());

        let outcome = with_framework_session_loop_permission(
            &call,
            &args,
            Some(SESSION_LOOP_EXECUTION_CONTRACT),
            async {
                let mut other = call.clone();
                other.id = "other-call".into();
                let other_args = json!({"action":"stop"});
                assert!(!selected_framework_session_loop(
                    &other,
                    "session_loop",
                    &args
                ));
                assert!(!selected_framework_session_loop(
                    &call,
                    "session_loop",
                    &other_args
                ));
                assert!(!selected_framework_session_loop(
                    &call,
                    "custom::session_loop",
                    &args
                ));
                assert!(selected_framework_session_loop(
                    &call,
                    "session_loop",
                    &args
                ));
                record_framework_session_loop_evaluation(&call, &args);
                Ok(())
            },
        )
        .await;
        assert!(outcome.is_ok());

        let opaque = with_framework_session_loop_permission(
            &call,
            &args,
            Some(SESSION_LOOP_EXECUTION_CONTRACT),
            async { Ok(()) },
        )
        .await;
        assert!(opaque.is_err());
    }
}
