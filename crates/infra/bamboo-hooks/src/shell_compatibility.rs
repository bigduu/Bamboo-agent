//! Narrow hook projections for the terminal API migration. Callable tool names
//! and the stored transcript keep their original identities.

use bamboo_agent_core::Session;
use bamboo_domain::HookPayload;
use serde_json::Value;

pub(crate) struct LegacyShellProjection {
    pub payload: HookPayload,
    pub input: Value,
    pub restriction_only: bool,
}

pub(crate) fn foreground_bash_hook(payload: &HookPayload) -> bool {
    matches!(
        payload,
        HookPayload::ToolExecution { tool_name, .. } | HookPayload::ToolResult { tool_name, .. }
            if tool_name == "bash"
    )
}

/// Check all relevant shell policies before an Ask can suspend dispatch.
pub fn requires_complete_shell_policy_checks(
    point: bamboo_domain::AgentHookPoint,
    payload: &HookPayload,
    session: &Session,
    original_input: Option<&Value>,
) -> bool {
    point == bamboo_domain::AgentHookPoint::BeforeToolExecution
        && (foreground_bash_hook(payload)
            || legacy_shell_projections(payload, session, original_input)
                .iter()
                .any(|projected| projected.restriction_only))
}

/// Foreground commands carry forward both prior start-command policies.
/// A continuation also checks any separate restriction on reading output.
pub(crate) fn legacy_shell_projections(
    payload: &HookPayload,
    session: &Session,
    original_input: Option<&Value>,
) -> Vec<LegacyShellProjection> {
    let Some(primary) = legacy_shell_projection(payload, session, original_input) else {
        return Vec::new();
    };
    if foreground_bash_hook(payload) {
        let mut args = primary
            .input
            .as_object()
            .expect("shell projection is an object")
            .clone();
        if let Some(command) = args.remove("command") {
            args.insert("cmd".into(), command);
        }
        args.remove("timeout");
        // An Ask in one compatibility policy must not hide a separate
        // restriction that matched the previous model entry point.
        return vec![
            project_shell_hook(payload, "exec_command", Value::Object(args), false),
            primary,
        ];
    }
    let secondary_payload = match &primary.payload {
        HookPayload::ToolExecution {
            tool_name,
            tool_call_id,
            ..
        } if matches!(tool_name.as_str(), "BashInput" | "KillShell") => {
            Some(HookPayload::ToolExecution {
                tool_name: "BashOutput".into(),
                tool_call_id: tool_call_id.clone(),
                parsed_args: serde_json::json!({"bash_id": primary.input["bash_id"]}),
            })
        }
        HookPayload::ToolResult {
            tool_name,
            tool_call_id,
            outcome,
        } if matches!(tool_name.as_str(), "BashInput" | "KillShell") => {
            Some(HookPayload::ToolResult {
                tool_name: "BashOutput".into(),
                tool_call_id: tool_call_id.clone(),
                outcome: outcome.clone(),
            })
        }
        _ => None,
    };
    let mut projections = Vec::with_capacity(2);
    if let Some(payload) = secondary_payload {
        let input = serde_json::json!({"bash_id": primary.input["bash_id"]});
        projections.push(primary);
        projections.push(LegacyShellProjection {
            payload,
            input,
            restriction_only: true,
        });
    } else {
        projections.push(primary);
    }
    projections
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
    if !matches!(tool_name.as_str(), "bash" | "exec_command" | "write_stdin") {
        return None;
    }
    let input = match payload {
        HookPayload::ToolExecution { parsed_args, .. } => Some(parsed_args.clone()),
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
            input
        }
        _ => return None,
    };
    let mut input = match input {
        Some(input) => input,
        None if matches!(tool_name.as_str(), "bash" | "exec_command") => serde_json::json!({}),
        None => return None,
    };
    let args = input.as_object_mut()?;
    args.remove("yield_time_ms");
    args.remove("max_output_tokens");
    let legacy_name = match tool_name.as_str() {
        "bash" => {
            if let Some(seconds) = args.remove("timeout").and_then(|value| value.as_f64()) {
                if let Some(milliseconds) = serde_json::Number::from_f64(seconds * 1000.0) {
                    args.insert("timeout".into(), milliseconds.into());
                }
            }
            "Bash"
        }
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
            let chars = args
                .remove("chars")
                .unwrap_or_else(|| Value::String(String::new()));
            if legacy_name == "BashInput" {
                args.insert("input".into(), chars);
                args.insert("append_newline".into(), Value::Bool(false));
            }
            if legacy_name != "BashInput" || args.get("eof") == Some(&Value::Bool(false)) {
                args.remove("eof");
            }
            legacy_name
        }
        _ => return None,
    };
    Some(project_shell_hook(payload, legacy_name, input, false))
}

fn project_shell_hook(
    payload: &HookPayload,
    legacy_name: &str,
    input: Value,
    restriction_only: bool,
) -> LegacyShellProjection {
    let payload = match payload {
        HookPayload::ToolExecution { tool_call_id, .. } => HookPayload::ToolExecution {
            tool_name: legacy_name.into(),
            tool_call_id: tool_call_id.clone(),
            parsed_args: input.clone(),
        },
        HookPayload::ToolResult {
            tool_call_id,
            outcome,
            ..
        } => HookPayload::ToolResult {
            tool_name: legacy_name.into(),
            tool_call_id: tool_call_id.clone(),
            outcome: outcome.clone(),
        },
        _ => unreachable!("only tool hook payloads are projected"),
    };
    LegacyShellProjection {
        payload,
        input,
        restriction_only,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn foreground_projects_both_previous_command_policies() {
        let session = Session::new("foreground-hook", "model");
        let payload = HookPayload::ToolExecution {
            tool_name: "bash".into(),
            tool_call_id: "bash-call".into(),
            parsed_args: json!({"command":"printf ok", "timeout":2.5}),
        };
        let aliases = legacy_shell_projections(&payload, &session, None);
        assert_eq!(aliases.len(), 2);
        assert!(
            matches!(&aliases[0].payload, HookPayload::ToolExecution { tool_name, .. }
            if tool_name == "exec_command")
        );
        assert_eq!(aliases[0].input, json!({"cmd":"printf ok"}));
        assert!(
            matches!(&aliases[1].payload, HookPayload::ToolExecution { tool_name, .. }
            if tool_name == "Bash")
        );
        assert_eq!(
            aliases[1].input,
            json!({"command":"printf ok", "timeout":2500.0})
        );
    }

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
            if name == "BashInput" {
                assert_eq!(projected.input["input"], chars);
                assert_eq!(projected.input["append_newline"], false);
            } else {
                assert_eq!(projected.input, json!({"bash_id":"exec-owned"}));
            }
            assert!(projected.input.get("session_id").is_none());
            assert!(projected.input.get("chars").is_none());
        }
    }

    #[test]
    fn continuation_projects_output_side_and_omits_transport_tuning_and_false_defaults() {
        let session = Session::new("test", "model");
        for (chars, eof, primary_name) in [
            ("", false, "BashOutput"),
            ("yes\n", false, "BashInput"),
            ("", true, "BashInput"),
            ("\u{3}", true, "KillShell"),
        ] {
            let payload = HookPayload::ToolExecution {
                tool_name: "write_stdin".into(),
                tool_call_id: "stdin-call".into(),
                parsed_args: json!({"session_id":"exec-owned", "chars":chars, "eof":eof, "yield_time_ms":250, "max_output_tokens":10}),
            };
            let projected = legacy_shell_projections(&payload, &session, None);
            assert!(
                matches!(&projected[0].payload, HookPayload::ToolExecution { tool_name, .. } if tool_name == primary_name)
            );
            assert!(!projected[0].restriction_only);
            assert!(projected[0].input.get("yield_time_ms").is_none());
            assert!(projected[0].input.get("max_output_tokens").is_none());
            if primary_name == "BashInput" && eof {
                assert_eq!(projected[0].input["eof"], true);
            } else {
                assert!(projected[0].input.get("eof").is_none());
            }
            assert_eq!(
                projected.len(),
                if primary_name == "BashOutput" { 1 } else { 2 }
            );
            let output = projected.last().unwrap();
            assert!(
                matches!(&output.payload, HookPayload::ToolExecution { tool_name, .. } if tool_name == "BashOutput")
            );
            assert_eq!(output.input, json!({"bash_id":"exec-owned"}));
            assert_eq!(output.restriction_only, primary_name != "BashOutput");
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
