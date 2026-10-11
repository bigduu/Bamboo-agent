//! Narrow hook projections for the terminal API migration. Callable tool names
//! and the stored transcript keep their original identities.

use bamboo_agent_core::Session;
use bamboo_domain::HookPayload;
use serde_json::Value;

pub(crate) struct LegacyShellProjection {
    pub payload: HookPayload,
    pub input: Value,
}

pub(crate) fn legacy_shell_projection(
    payload: &HookPayload,
    session: &Session,
    original_input: Option<&Value>,
) -> Option<LegacyShellProjection> {
    let tool_name = match payload {
        HookPayload::ToolExecution { tool_name, .. }
        | HookPayload::ToolResult { tool_name, .. } => tool_name,
        _ => return None,
    };
    // The engine supplies resolved builtin identities. Never guess a builtin
    // from an unresolved namespace: exact custom names may share this suffix.
    if !matches!(tool_name.as_str(), "exec_command" | "write_stdin") {
        return None;
    }
    let (tool_call_id, input) = match payload {
        HookPayload::ToolExecution {
            tool_call_id,
            parsed_args,
            ..
        } => (tool_call_id, Some(parsed_args.clone())),
        HookPayload::ToolResult { tool_call_id, .. } => {
            let input = original_input.cloned().or_else(|| {
                session
                    .messages
                    .iter()
                    .rev()
                    .filter_map(|message| message.tool_calls.as_ref())
                    .flatten()
                    .find(|call| call.id == *tool_call_id)
                    .and_then(|call| serde_json::from_str(&call.function.arguments).ok())
            });
            (tool_call_id, input)
        }
        _ => return None,
    };
    let mut input = match input {
        Some(input) => input,
        None if tool_name == "exec_command" => serde_json::json!({}),
        None => return None,
    };
    let args = input.as_object_mut()?;
    let legacy_name = match tool_name.as_str() {
        "exec_command" => {
            if let Some(command) = args.remove("cmd") {
                args.insert("command".into(), command);
            }
            "Bash"
        }
        "write_stdin" => {
            let chars = args.get("chars").and_then(Value::as_str).unwrap_or("");
            let legacy_name = if chars == "\u{3}" {
                "KillShell"
            } else if !chars.is_empty() || args.get("eof").and_then(Value::as_bool) == Some(true) {
                "BashInput"
            } else {
                "BashOutput"
            };
            if let Some(id) = args.remove("session_id") {
                args.insert("bash_id".into(), id);
            }
            if let Some(chars) = args.remove("chars") {
                args.insert("input".into(), chars);
            }
            args.insert("append_newline".into(), Value::Bool(false));
            legacy_name
        }
        _ => return None,
    };
    let payload = match payload {
        HookPayload::ToolExecution { .. } => HookPayload::ToolExecution {
            tool_name: legacy_name.into(),
            tool_call_id: tool_call_id.clone(),
            parsed_args: input.clone(),
        },
        HookPayload::ToolResult { outcome, .. } => HookPayload::ToolResult {
            tool_name: legacy_name.into(),
            tool_call_id: tool_call_id.clone(),
            outcome: outcome.clone(),
        },
        _ => unreachable!("only tool hook payloads are projected"),
    };
    Some(LegacyShellProjection { payload, input })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stdin_compatibility_selects_legacy_operation_and_preserves_input_bytes() {
        let session = Session::new("test", "model");
        for (chars, eof, name) in [
            ("", false, "BashOutput"),
            ("yes\n", false, "BashInput"),
            ("", true, "BashInput"),
            ("\u{3}", false, "KillShell"),
        ] {
            let payload = HookPayload::ToolExecution {
                tool_name: "write_stdin".into(),
                tool_call_id: "stdin-call".into(),
                parsed_args: json!({"session_id":"exec-owned", "chars":chars, "eof":eof}),
            };
            let projected = legacy_shell_projection(&payload, &session, None).unwrap();
            assert!(
                matches!(projected.payload, HookPayload::ToolExecution { ref tool_name, .. } if tool_name == name)
            );
            assert_eq!(projected.input["bash_id"], "exec-owned");
            assert_eq!(projected.input["input"], chars);
            assert_eq!(projected.input["append_newline"], false);
            assert!(projected.input.get("session_id").is_none());
            assert!(projected.input.get("chars").is_none());
        }
    }

    #[test]
    fn shell_projection_requires_exact_resolved_builtin_names() {
        let session = Session::new("test", "model");
        let payload = HookPayload::ToolExecution {
            tool_name: "exec_command".into(),
            tool_call_id: "exec-call".into(),
            parsed_args: json!({"cmd":"cargo test"}),
        };
        assert_eq!(
            legacy_shell_projection(&payload, &session, None)
                .unwrap()
                .input,
            json!({"command":"cargo test"})
        );
        for name in [
            "default::exec_command",
            "default::write_stdin",
            "vendor::exec_command",
            "vendor::write_stdin",
            "mcp__custom__exec_command",
        ] {
            let payload = HookPayload::ToolExecution {
                tool_name: name.into(),
                tool_call_id: "foreign-call".into(),
                parsed_args: json!({"cmd":"cargo test", "session_id":"exec", "chars":"yes"}),
            };
            assert!(legacy_shell_projection(&payload, &session, None).is_none());
        }
    }
}
