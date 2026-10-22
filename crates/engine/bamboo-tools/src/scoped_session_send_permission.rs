//! Restriction-only context for a selected concrete framework overlay owner.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

use bamboo_agent_core::{ToolCall, ToolError};
use serde_json::Value;

pub const SCOPED_SESSION_SEND_EXECUTION_CONTRACT: &str = "bamboo.session_message.scoped_send.v1";

struct SelectedSend {
    call: ToolCall,
    resolved_args: Value,
    contract: &'static str,
    native_evaluated: AtomicBool,
}

tokio::task_local! {
    static SELECTED_SEND: SelectedSend;
}

/// Retain the normal base permission chain while selecting its one native
/// evaluation's additional Deny/Ask context. This cannot authorize a send.
/// Selection must come from the concrete exact owner, never an alias/catalog.
/// The complete original call, original argument bytes and resolved arguments
/// bind the scope; another nested invocation does not inherit this selection.
pub async fn with_framework_scoped_session_send_restriction<F, T>(
    call: &ToolCall,
    resolved_args: &Value,
    selected_contract: Option<&'static str>,
    check: F,
) -> Result<T, ToolError>
where
    F: Future<Output = Result<T, ToolError>>,
{
    if selected_contract != Some(SCOPED_SESSION_SEND_EXECUTION_CONTRACT) {
        return Err(ToolError::Execution(
            "scoped_session_send_permission_owner_unavailable".into(),
        ));
    }
    SELECTED_SEND
        .scope(
            SelectedSend {
                call: call.clone(),
                resolved_args: resolved_args.clone(),
                contract: SCOPED_SESSION_SEND_EXECUTION_CONTRACT,
                native_evaluated: AtomicBool::new(false),
            },
            async {
                let outcome = check.await?;
                if !SELECTED_SEND
                    .with(|selection| selection.native_evaluated.load(Ordering::SeqCst))
                {
                    // An opaque/configless base's default allow cannot stand
                    // in for the required single canonical policy evaluation.
                    return Err(ToolError::Execution(
                        "scoped_session_send_permission_config_unsupported".into(),
                    ));
                }
                Ok(outcome)
            },
        )
        .await
}

pub(crate) fn record_framework_scoped_session_send_evaluation(
    call: &ToolCall,
    resolved_args: &Value,
) {
    if selected_framework_scoped_session_send(call, "session_message", resolved_args) {
        let _ = SELECTED_SEND.try_with(|selection| {
            selection.native_evaluated.store(true, Ordering::SeqCst);
        });
    }
}

pub(crate) fn selected_framework_scoped_session_send(
    call: &ToolCall,
    execution_name: &str,
    resolved_args: &Value,
) -> bool {
    execution_name == "session_message"
        && SELECTED_SEND
            .try_with(|selection| {
                selection.contract == SCOPED_SESSION_SEND_EXECUTION_CONTRACT
                    && selection.call == *call
                    && selection.resolved_args == *resolved_args
            })
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::FunctionCall;
    use serde_json::json;

    fn call() -> ToolCall {
        ToolCall {
            id: "original".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_message".into(),
                arguments: r#"{"action":"send","target_session_id":"private"}"#.into(),
            },
        }
    }

    #[tokio::test]
    async fn restriction_selection_binds_full_call_resolved_args_and_concrete_owner() {
        let original = call();
        let args = json!({"action":"send", "target_session_id":"private"});
        with_framework_scoped_session_send_restriction(
            &original,
            &args,
            Some(SCOPED_SESSION_SEND_EXECUTION_CONTRACT),
            async {
                assert!(selected_framework_scoped_session_send(
                    &original,
                    "session_message",
                    &args
                ));
                for field in ["id", "type", "name", "bytes"] {
                    let mut nested = original.clone();
                    match field {
                        "id" => nested.id = "new-nested-call".into(),
                        "type" => nested.tool_type = "other".into(),
                        "name" => nested.function.name = "custom::session_message".into(),
                        _ => nested.function.arguments.push(' '),
                    }
                    assert!(!selected_framework_scoped_session_send(
                        &nested,
                        "session_message",
                        &args
                    ));
                }
                assert!(!selected_framework_scoped_session_send(
                    &original,
                    "custom::session_message",
                    &args
                ));
                let changed = json!({"action":"send", "target_session_id":"changed"});
                assert!(!selected_framework_scoped_session_send(
                    &original,
                    "session_message",
                    &changed
                ));
                record_framework_scoped_session_send_evaluation(&original, &args);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert!(!selected_framework_scoped_session_send(
            &original,
            "session_message",
            &args
        ));
        assert!(
            with_framework_scoped_session_send_restriction(&original, &args, None, async {
                Ok(())
            })
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn opaque_base_or_unrelated_nested_evaluation_does_not_attest_the_original_gate() {
        let original = call();
        let args = json!({"action":"send"});
        for nested in [false, true] {
            let error = with_framework_scoped_session_send_restriction(
                &original,
                &args,
                Some(SCOPED_SESSION_SEND_EXECUTION_CONTRACT),
                async {
                    if nested {
                        let mut other = original.clone();
                        other.id = "nested-call".into();
                        record_framework_scoped_session_send_evaluation(&other, &args);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("permission_config_unsupported"));
        }
    }
}
