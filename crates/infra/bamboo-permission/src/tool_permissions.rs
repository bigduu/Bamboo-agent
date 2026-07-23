use serde_json::Value;

use crate::bash_security;
use crate::hierarchy::PermissionRuleSet;
use crate::{PermissionContext, PermissionError, PermissionType};

const DELETE_COMMANDS: [&str; 7] = ["rm", "rmdir", "del", "erase", "unlink", "rd", "remove-item"];

pub fn check_permissions(
    tool_name: &str,
    args: &Value,
) -> Result<Option<Vec<PermissionContext>>, PermissionError> {
    match tool_name {
        "Write" | "Edit" | "apply_patch" => {
            let path = required_string_arg(args, "file_path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("{} file: {}", tool_name, path),
            )]))
        }
        "NotebookEdit" => {
            let path = required_string_arg(args, "notebook_path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("Notebook edit: {}", path),
            )]))
        }
        "Bash" => {
            let command = required_string_arg(args, "command")?.trim();
            if command.is_empty() {
                return Err(PermissionError::CheckFailed(
                    "Missing or invalid 'command' parameter".to_string(),
                ));
            }

            // AST-based security analysis (bash_security). #560: exactly ONE
            // context per Bash call — the resource is always the bare
            // command, never mangled with a "SECURITY:" prefix, so whitelist
            // patterns and session grants (which glob-match against
            // `ctx.resource`) match uniformly regardless of whether the
            // analysis happened to notice a benign construct (`${…}`, an
            // `if`, a heredoc). The "Dangerous shell pattern" framing is
            // reserved for an actual `Deny` verdict; Allow-level warnings
            // (parameter expansion, control flow, command substitution, …)
            // are folded into the description as an informational note
            // instead of changing the request's identity/severity.
            let security = bash_security::analyze_command(command);
            // #556: AST-based (argv[0] basename), not a substring scan — see
            // `is_delete_command`.
            let is_delete = is_delete_command(command);

            let permission_type = if is_delete {
                PermissionType::DeleteOperation
            } else {
                PermissionType::ExecuteCommand
            };

            let mut description = if is_delete {
                format!("Delete operation via shell: {}", command)
            } else {
                format!("Execute command: {}", command)
            };
            if security.verdict == bash_security::BashVerdict::Deny {
                description = format!(
                    "Dangerous shell pattern detected: {} — {}",
                    security.summary(),
                    description
                );
            } else if security.is_dangerous() {
                description = format!("{} (note: {})", description, security.summary());
            }

            Ok(Some(vec![PermissionContext::new(
                permission_type,
                command,
                description,
            )]))
        }
        "session_note" | "memory_note" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            if matches!(action.as_str(), "append" | "replace" | "clear") {
                let notes_dir = bamboo_config::paths::bamboo_dir()
                    .join("memory")
                    .join("v1")
                    .join("sessions");
                let notes_path = bamboo_config::paths::path_to_display_string(&notes_dir);
                Ok(Some(vec![PermissionContext::new(
                    PermissionType::WriteFile,
                    notes_path.clone(),
                    format!("{} action={} in {}", tool_name, action, notes_path),
                )]))
            } else {
                Ok(None)
            }
        }
        "memory" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            let bamboo_dir = bamboo_config::paths::bamboo_dir();
            let session_memory_dir = bamboo_config::paths::path_to_display_string(
                &bamboo_dir.join("memory").join("v1").join("sessions"),
            );
            let global_memory_dir = bamboo_config::paths::path_to_display_string(
                &bamboo_dir
                    .join("memory")
                    .join("v1")
                    .join("scopes")
                    .join("global"),
            );
            let project_memory_dir = bamboo_config::paths::path_to_display_string(
                // The permission classifier has no trusted session context, so
                // it cannot resolve the exact opaque Project id. Gate durable
                // writes at the conservative first-class Project root; runtime
                // resolution narrows assigned writes to
                // projects/<id>/memory/v1 and rejects Unassigned Project writes.
                &bamboo_dir.join("projects"),
            );
            let context = |resource: String| {
                PermissionContext::new(
                    PermissionType::WriteFile,
                    resource.clone(),
                    format!("{} action={} in {}", tool_name, action, resource),
                )
            };
            let ambiguous_durable_contexts = || {
                vec![
                    context(global_memory_dir.clone()),
                    context(project_memory_dir.clone()),
                ]
            };
            let scoped_durable_contexts = || match args
                .get("scope")
                .and_then(Value::as_str)
                .map(str::trim)
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("global") => vec![context(global_memory_dir.clone())],
                Some("project") => vec![context(project_memory_dir.clone())],
                // Invalid/missing scope fails closed at the permission boundary.
                // The tool may later reject the arguments, but no durable write
                // can be authorized by a resource from only one possible scope.
                _ => ambiguous_durable_contexts(),
            };
            let write_contexts = match action.as_str() {
                "session_append" | "session_replace" | "session_clear" => {
                    Some(vec![context(session_memory_dir)])
                }
                "write" | "rebuild" => Some(scoped_durable_contexts()),
                // These actions identify existing memories by id(s), so the
                // classifier cannot know whether execution will mutate the
                // Global store or the assigned Project store.
                "merge" | "split" | "consolidate" => Some(ambiguous_durable_contexts()),
                "purge"
                    if args
                        .get("id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| !id.trim().is_empty()) =>
                {
                    Some(ambiguous_durable_contexts())
                }
                "purge" => Some(scoped_durable_contexts()),
                _ => None,
            };
            Ok(write_contexts)
        }
        "BashInput" => {
            let bash_id = required_string_arg(args, "bash_id")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                bash_id,
                format!("Write to interactive shell stdin: {}", bash_id),
            )]))
        }
        "BashOutput" => {
            let bash_id = required_string_arg(args, "bash_id")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                bash_id,
                format!("Read shell output: {}", bash_id),
            )]))
        }
        "KillShell" => {
            let shell_id = first_present_string_arg(args, &["shell_id", "bash_id"])?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                shell_id,
                format!("Kill shell: {}", shell_id),
            )]))
        }
        "WebFetch" => {
            let url = required_string_arg(args, "url")?;
            let resource = extract_domain(url);
            Ok(Some(vec![PermissionContext::new(
                PermissionType::HttpRequest,
                resource,
                format!("Web fetch: {}", url),
            )]))
        }
        "WebSearch" => {
            let query = required_string_arg(args, "query")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::HttpRequest,
                "duckduckgo.com",
                format!("Web search query: {}", query),
            )]))
        }
        "js_repl" => {
            let code = required_string_arg(args, "code")?;
            let preview: String = code.chars().take(80).collect();
            let preview = if code.chars().count() > 80 {
                format!("{}...", preview)
            } else {
                preview
            };
            // Key the grant on the CODE, not a constant `"node"`: session grants
            // are recorded per-resource, so a constant resource means the first
            // approved js_repl call silently grants ANY future code for the
            // session-grant window (defeating the js_repl force-ask backstop).
            // Mirror Bash's `SECURITY: {command}` so a grant only ever covers a
            // re-run of the exact same code.
            Ok(Some(vec![PermissionContext::new(
                PermissionType::ExecuteCommand,
                format!("SECURITY: js_repl {code}"),
                format!("Execute JavaScript: {}", preview),
            )]))
        }
        // ── Server/overlay tools (#395) ────────────────────────────────────
        // #393 wired these through the permission gate, but without a
        // classification here `check_permissions` returned `Ok(None)` → no
        // context → ungated in EVERY mode (Default never prompts, user
        // `deploy_agent(*)` ask-rules never fire). Classify the compute-spinning
        // and schedule-mutating actions so they actually reach the gate; read
        // actions stay ungated.
        "deploy_agent" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            // deploy/stop spin up or tear down local/Docker/SSH workers.
            if matches!(action.as_str(), "deploy" | "stop") {
                Ok(Some(vec![PermissionContext::new(
                    PermissionType::ExecuteCommand,
                    format!("deploy_agent {action}"),
                    format!("deploy_agent {action}: spin up/stop a worker process"),
                )]))
            } else {
                Ok(None) // list → read-only
            }
        }
        "cluster" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            // deploy/stop a worker onto a managed node; list/describe/status read.
            if matches!(action.as_str(), "deploy" | "stop") {
                let node = args.get("node").and_then(|v| v.as_str()).unwrap_or("");
                Ok(Some(vec![PermissionContext::new(
                    PermissionType::ExecuteCommand,
                    format!("cluster {action} {node}").trim_end().to_string(),
                    format!("cluster {action} on node '{node}'"),
                )]))
            } else {
                Ok(None)
            }
        }
        "Project" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            if matches!(action.as_str(), "bind_workspace" | "unbind_workspace") {
                let path = required_string_arg(args, "path")?;
                Ok(Some(vec![PermissionContext::new(
                    PermissionType::WriteFile,
                    path,
                    format!("Project {action}: mutate the Project workspace binding for {path}"),
                )]))
            } else {
                // inspect/list_resources are strictly redacted read operations.
                Ok(None)
            }
        }
        "SubAgent" => {
            // Legacy calls omit `action` and mean `create`; the tool defaults it
            // that way INSIDE `invoke`, which runs AFTER this gate — so default it
            // here too rather than hard-failing on a missing field (that would
            // abort a legacy call before it reaches the tool). #395.
            let action = args
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("create")
                .trim()
                .to_ascii_lowercase();
            match action.as_str() {
                // Spawn / run / drive a child agent = independent compute + full toolset.
                "create" | "run" | "send_message" => Ok(Some(vec![PermissionContext::new(
                    PermissionType::ExecuteCommand,
                    format!("SubAgent {action}"),
                    format!("SubAgent {action}: spawn/run a child agent session"),
                )])),
                // Mutate an already-created child session.
                "update" | "cancel" => Ok(Some(vec![PermissionContext::new(
                    PermissionType::WriteFile,
                    format!("SubAgent {action}"),
                    format!("SubAgent {action}: modify a child session"),
                )])),
                "delete" => Ok(Some(vec![PermissionContext::new(
                    PermissionType::DeleteOperation,
                    "SubAgent delete",
                    "SubAgent delete: remove a child session",
                )])),
                // wait / list / get / list_models → passive or read-only.
                _ => Ok(None),
            }
        }
        "scheduler" => {
            let action = required_string_arg(args, "action")?
                .trim()
                .to_ascii_lowercase();
            let schedule_id = args.get("schedule_id").and_then(|v| v.as_str());
            match action.as_str() {
                // Immediately mints + executes a fresh session → like a command.
                "run_now" => Ok(Some(vec![PermissionContext::new(
                    PermissionType::ExecuteCommand,
                    format!("scheduler run_now {}", schedule_id.unwrap_or(""))
                        .trim_end()
                        .to_string(),
                    "scheduler run_now: execute a schedule immediately".to_string(),
                )])),
                // Create/modify/remove a schedule that later auto-executes sessions.
                "create" | "patch" | "delete" => Ok(Some(vec![PermissionContext::new(
                    PermissionType::WriteFile,
                    format!("scheduler {action} {}", schedule_id.unwrap_or(""))
                        .trim_end()
                        .to_string(),
                    format!("scheduler {action}: modify an auto-executing schedule"),
                )])),
                _ => Ok(None), // list / list_sessions → read-only
            }
        }
        // Read-only: session_inspector (list / get_meta / read_messages) and the
        // session_history viewer never mutate or spin up compute. #395.
        "session_inspector" | "session_history" => Ok(None),
        // `notify` fires an outbound OS popup / push notification but mutates
        // nothing in the session or workspace, so it is explicitly ungated
        // (auto-approved) by design: a reminder/alert tool that itself
        // prompts for permission is useless in headless/scheduled runs — the
        // whole point is surfacing something to the human without them
        // having to already be watching. A user `deny notify(*)` ask-rule can
        // still target it by name if they want to opt out. Listed explicitly
        // (not left to the catch-all) per the #395 lesson: an unlisted tool
        // being silently ungated is a bug, an intentionally-ungated one
        // documented here is a decision.
        "notify" => Ok(None),
        _ => Ok(None),
    }
}

fn extract_domain(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
        .unwrap_or_else(|| url.to_string())
}

/// True if some command actually INVOKED by `command` (i.e. some top-level
/// `argv[0]` basename, AST-based — not a raw substring/keyword scan) is a
/// delete command. #556: a delete keyword that merely appears in a comment, a
/// quoted string, or as a substring of an unrelated word (`cat model.json`,
/// `# rm cleanup`, `git commit -m "rm helper"`, `git grep 'rm -rf'`) never
/// matches, because those bytes are never an `argv[0]`. Fails CLOSED (returns
/// `true`) when the command can't be parsed, mirroring
/// [`bash_security::is_compound_command`]'s poisoned-lock/unparseable
/// handling — an unverifiable command is never mistaken for a non-delete one.
pub fn is_delete_command(command: &str) -> bool {
    match bash_security::top_level_command_basenames(command) {
        Some(names) => names
            .iter()
            .any(|name| DELETE_COMMANDS.contains(&name.as_str())),
        None => true,
    }
}

fn required_string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, PermissionError> {
    args.get(key)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            PermissionError::CheckFailed(format!("Missing or invalid '{}' parameter", key))
        })
}

fn first_present_string_arg<'a>(
    args: &'a Value,
    keys: &[&str],
) -> Result<&'a str, PermissionError> {
    for key in keys {
        if let Some(value) = args.get(key).and_then(|value| value.as_str()) {
            return Ok(value);
        }
    }
    Err(PermissionError::CheckFailed(format!(
        "Missing or invalid parameter (expected one of: {})",
        keys.join(", ")
    )))
}

/// Check tool rules against allowed and denied tool patterns.
///
/// Deny rules take precedence over allow rules.
/// Returns `Some(true)` if allowed, `Some(false)` if denied, `None` if no rules match.
pub fn check_tool_rules(
    tool_name: &str,
    args: &Value,
    allowed_tools: &[String],
    denied_tools: &[String],
) -> Option<bool> {
    let rule_set = PermissionRuleSet::from_rules(allowed_tools, denied_tools);
    rule_set.match_tool_call(tool_name, args)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn check_permissions_write() {
        let args = json!({"file_path": "/tmp/test.txt"});
        let contexts = check_permissions("Write", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
    }

    // ── Server/overlay tool classification (#395) ──────────────────────────
    #[test]
    fn overlay_tools_gate_compute_spinning_actions() {
        // deploy_agent / cluster deploy+stop and SubAgent create spin up compute →
        // ExecuteCommand; scheduler run_now executes immediately → ExecuteCommand.
        for (tool, args) in [
            (
                "deploy_agent",
                json!({"action": "deploy", "role": "worker"}),
            ),
            ("deploy_agent", json!({"action": "stop"})),
            ("cluster", json!({"action": "deploy", "node": "n1"})),
            ("cluster", json!({"action": "stop", "node": "n1"})),
            ("SubAgent", json!({"action": "create", "prompt": "x"})),
            ("SubAgent", json!({"action": "run", "session_id": "c1"})),
            (
                "SubAgent",
                json!({"action": "send_message", "session_id": "c1"}),
            ),
            // Legacy call with no `action` defaults to create (back-compat).
            ("SubAgent", json!({"prompt": "x"})),
            (
                "scheduler",
                json!({"action": "run_now", "schedule_id": "s1"}),
            ),
        ] {
            let contexts = check_permissions(tool, &args)
                .unwrap()
                .unwrap_or_else(|| panic!("{tool} {args} must be gated, not ungated"));
            assert_eq!(
                contexts[0].permission_type,
                PermissionType::ExecuteCommand,
                "{tool} {args} should gate as ExecuteCommand"
            );
        }
    }

    #[test]
    fn scheduler_mutations_gate_as_writefile() {
        for action in ["create", "patch", "delete"] {
            let args = json!({"action": action, "schedule_id": "s1", "name": "n"});
            let contexts = check_permissions("scheduler", &args).unwrap().unwrap();
            assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
        }
    }

    #[test]
    fn project_binding_mutations_gate_as_writefile() {
        for action in ["bind_workspace", "unbind_workspace"] {
            let path = "/workspace/project";
            let contexts = check_permissions(
                "Project",
                &json!({"action": action, "path": path, "expected_revision": 1}),
            )
            .unwrap()
            .expect("Project binding mutation must be permission gated");
            assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
            assert_eq!(contexts[0].resource, path);
        }
        assert!(check_permissions("Project", &json!({"action": "inspect"}))
            .unwrap()
            .is_none());
        assert!(
            check_permissions("Project", &json!({"action": "list_resources"}))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn subagent_mutations_gate() {
        // update/cancel modify an active child → WriteFile; delete → DeleteOperation.
        for action in ["update", "cancel"] {
            let args = json!({"action": action, "session_id": "c1"});
            let contexts = check_permissions("SubAgent", &args).unwrap().unwrap();
            assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
        }
        let del = json!({"action": "delete", "session_id": "c1"});
        let contexts = check_permissions("SubAgent", &del).unwrap().unwrap();
        assert_eq!(contexts[0].permission_type, PermissionType::DeleteOperation);
    }

    #[test]
    fn overlay_read_actions_stay_ungated() {
        // Read-only actions must NOT produce a permission context (Ok(None)).
        for (tool, args) in [
            ("deploy_agent", json!({"action": "list"})),
            ("cluster", json!({"action": "list"})),
            ("cluster", json!({"action": "status", "node": "n1"})),
            ("SubAgent", json!({"action": "list"})),
            ("SubAgent", json!({"action": "wait"})),
            ("SubAgent", json!({"action": "get", "session_id": "c1"})),
            ("SubAgent", json!({"action": "list_models"})),
            ("scheduler", json!({"action": "list"})),
            (
                "scheduler",
                json!({"action": "list_sessions", "schedule_id": "s1"}),
            ),
            (
                "session_inspector",
                json!({"action": "read_messages", "session_id": "x"}),
            ),
        ] {
            assert!(
                check_permissions(tool, &args).unwrap().is_none(),
                "{tool} {args} should stay ungated"
            );
        }
    }

    #[test]
    fn notify_is_explicitly_ungated() {
        // `notify` must hit the EXPLICIT `"notify" => Ok(None)` arm, not the
        // catch-all — asserting a specific-enough shape (empty args) still
        // pass through confirms it isn't relying on `required_string_arg`
        // machinery that would otherwise error first.
        let contexts = check_permissions(
            "notify",
            &json!({"title": "Reminder", "message": "Stand up", "priority": "high"}),
        )
        .unwrap();
        assert!(
            contexts.is_none(),
            "notify must be auto-approved (ungated) by design"
        );
    }

    #[test]
    fn check_permissions_apply_patch() {
        let args = json!({"file_path": "/tmp/test.txt", "patch": "..."});
        let contexts = check_permissions("apply_patch", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
    }

    #[test]
    fn check_permissions_bash_delete() {
        // #560: exactly ONE context per Bash call, not two sequential prompts
        // for a single `rm x` — DeleteOperation carries the elevated risk
        // classification directly rather than being a second context.
        let args = json!({"command": "rm -rf /tmp/a"});
        let contexts = check_permissions("Bash", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::DeleteOperation);
        // The resource stays the bare command — never "SECURITY: …" — so a
        // whitelist/session-grant pattern matches it uniformly.
        assert_eq!(contexts[0].resource, "rm -rf /tmp/a");
    }

    #[test]
    fn check_permissions_bash_resource_never_security_prefixed() {
        // #560: an Allow-level warning (benign `${…}` parameter expansion)
        // must not rewrite the resource, or a `Bash(cargo *)`-style whitelist
        // pattern could never match this call.
        let args = json!({"command": "echo ${HOME}"});
        let contexts = check_permissions("Bash", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::ExecuteCommand);
        assert_eq!(contexts[0].resource, "echo ${HOME}");
        assert!(!contexts[0].resource.starts_with("SECURITY:"));
    }

    #[test]
    fn check_permissions_bash_deny_verdict_gets_security_framing() {
        // A real Deny verdict (eval-like builtin) still surfaces the
        // "Dangerous shell pattern" framing — in the description, not the
        // resource.
        let args = json!({"command": "eval 'cat /etc/passwd'"});
        let contexts = check_permissions("Bash", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].resource, "eval 'cat /etc/passwd'");
        assert!(contexts[0]
            .operation_description
            .contains("Dangerous shell pattern detected"));
    }

    #[test]
    fn check_permissions_bash_delete_false_positives_not_gated_as_delete() {
        // #556: a delete keyword that only appears in a comment, a quoted
        // string, or as a substring of an unrelated word must NOT classify as
        // DeleteOperation.
        for cmd in [
            "cat model.json",
            "python format.py",
            "git diff --word-diff",
            "ls orders/",
            "grep -n herd file.txt",
            "echo hyperderive",
            "cargo build",
            "git commit -m \"remove dead code, rm helper\"",
            "git grep -n 'rm -rf'",
            "echo \"please delete me\"",
            "man rm",
            "which rm",
        ] {
            let args = json!({"command": cmd});
            let contexts = check_permissions("Bash", &args).unwrap().unwrap();
            assert_eq!(
                contexts[0].permission_type,
                PermissionType::ExecuteCommand,
                "`{cmd}` must not classify as DeleteOperation"
            );
        }
    }

    #[test]
    fn check_permissions_bash_real_deletes_still_gated_as_delete() {
        for cmd in ["rm -rf /tmp/a", "rmdir /tmp/b", "unlink /tmp/c", "rm x"] {
            let args = json!({"command": cmd});
            let contexts = check_permissions("Bash", &args).unwrap().unwrap();
            assert_eq!(
                contexts[0].permission_type,
                PermissionType::DeleteOperation,
                "`{cmd}` must classify as DeleteOperation"
            );
        }
    }

    #[test]
    fn check_permissions_web_fetch() {
        let args = json!({"url": "https://example.com/path"});
        let contexts = check_permissions("WebFetch", &args).unwrap().unwrap();
        assert_eq!(contexts[0].permission_type, PermissionType::HttpRequest);
        assert_eq!(contexts[0].resource, "example.com");
    }

    #[test]
    fn check_permissions_bash_trims_command() {
        let args = json!({"command": "   ls -la   "});
        let contexts = check_permissions("Bash", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].resource, "ls -la");
    }

    #[test]
    fn check_permissions_session_note_write_actions_require_write_context() {
        let append = check_permissions("session_note", &json!({"action": "append"}))
            .unwrap()
            .unwrap();
        assert_eq!(append.len(), 1);
        assert_eq!(append[0].permission_type, PermissionType::WriteFile);

        let read = check_permissions("session_note", &json!({"action": "read"})).unwrap();
        assert!(read.is_none());
    }

    #[test]
    fn check_permissions_memory_action_scopes_read_vs_write() {
        let session_read = check_permissions("memory", &json!({"action": "session_read"})).unwrap();
        assert!(session_read.is_none());

        let query = check_permissions("memory", &json!({"action": "query"})).unwrap();
        assert!(query.is_none());

        let session_append = check_permissions("memory", &json!({"action": "session_append"}))
            .unwrap()
            .unwrap();
        assert_eq!(session_append.len(), 1);
        assert_eq!(session_append[0].permission_type, PermissionType::WriteFile);
        assert!(session_append[0].resource.contains("/memory/v1/sessions"));

        let global_write =
            check_permissions("memory", &json!({"action": "write", "scope": "global"}))
                .unwrap()
                .unwrap();
        assert_eq!(global_write.len(), 1);
        assert_eq!(global_write[0].permission_type, PermissionType::WriteFile);
        assert!(global_write[0]
            .resource
            .ends_with("/memory/v1/scopes/global"));

        let project_write =
            check_permissions("memory", &json!({"action": "write", "scope": "project"}))
                .unwrap()
                .unwrap();
        assert_eq!(project_write.len(), 1);
        assert_eq!(project_write[0].permission_type, PermissionType::WriteFile);
        assert!(project_write[0].resource.ends_with("/projects"));

        let ambiguous_write = check_permissions("memory", &json!({"action": "write"}))
            .unwrap()
            .unwrap();
        assert_eq!(ambiguous_write.len(), 2);
    }

    #[test]
    fn check_permissions_memory_mutating_actions_all_gated() {
        // Every mutating durable-memory action must produce a WriteFile context so
        // it hits the permission gate. `split` and `consolidate` were previously
        // omitted (issue #341) even though `MemoryTool::classify` treats them as
        // mutating, so a `memory(*)` ask-rule / Default-mode prompt never fired for
        // them. Guards the full set stays in lockstep with the tool's classifier.
        for (action, args, expected_contexts) in [
            ("write", json!({"action": "write", "scope": "global"}), 1),
            ("merge", json!({"action": "merge", "id": "memory-1"}), 2),
            ("split", json!({"action": "split", "id": "memory-1"}), 2),
            (
                "consolidate",
                json!({"action": "consolidate", "ids": ["memory-1", "memory-2"]}),
                2,
            ),
            ("purge", json!({"action": "purge", "id": "memory-1"}), 2),
            (
                "rebuild",
                json!({"action": "rebuild", "scope": "project"}),
                1,
            ),
        ] {
            let contexts = check_permissions("memory", &args)
                .unwrap_or_else(|_| panic!("memory action {action} should classify"))
                .unwrap_or_else(|| panic!("memory action {action} must require a WriteFile gate"));
            assert_eq!(contexts.len(), expected_contexts, "action {action}");
            assert!(contexts
                .iter()
                .all(|context| context.permission_type == PermissionType::WriteFile));
        }

        let ambiguous = check_permissions(
            "memory",
            &json!({"action": "consolidate", "ids": ["a", "b"]}),
        )
        .unwrap()
        .expect("ambiguous mutation must be gated");
        assert_eq!(ambiguous.len(), 2);
        assert!(ambiguous
            .iter()
            .any(|context| context.resource.ends_with("/memory/v1/scopes/global")));
        assert!(ambiguous
            .iter()
            .any(|context| context.resource.ends_with("/projects")));

        let scoped_purge =
            check_permissions("memory", &json!({"action": "purge", "scope": "global"}))
                .unwrap()
                .expect("scoped purge must be gated");
        assert_eq!(scoped_purge.len(), 1);
        assert!(scoped_purge[0]
            .resource
            .ends_with("/memory/v1/scopes/global"));

        // Read-only actions stay ungated.
        for action in [
            "session_read",
            "session_list_topics",
            "query",
            "get",
            "find_duplicates",
            "inspect",
            "scan_blobs",
            "scan_duplicates",
        ] {
            assert!(
                check_permissions("memory", &json!({"action": action}))
                    .unwrap()
                    .is_none(),
                "read-only memory action {action} must not be gated"
            );
        }
    }

    #[test]
    fn check_permissions_kill_shell_accepts_bash_id_alias() {
        let args = json!({"bash_id": "abc-123"});
        let contexts = check_permissions("KillShell", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::TerminalSession);
        assert_eq!(contexts[0].resource, "abc-123");
    }

    #[test]
    fn check_permissions_bash_input_classified_as_terminal_session() {
        let args = json!({"bash_id": "abc-123", "input": "y"});
        let contexts = check_permissions("BashInput", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::TerminalSession);
        assert_eq!(contexts[0].resource, "abc-123");
        assert!(contexts[0]
            .operation_description
            .contains("Write to interactive shell stdin"));
    }

    #[test]
    fn check_permissions_js_repl() {
        let args = json!({"code": "console.log('hello')"});
        let contexts = check_permissions("js_repl", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::ExecuteCommand);
        // Resource is keyed on the code (not a constant `"node"`) so a session
        // grant only ever covers a re-run of the same code.
        assert_eq!(
            contexts[0].resource,
            "SECURITY: js_repl console.log('hello')"
        );
        assert!(contexts[0]
            .operation_description
            .contains("console.log('hello')"));
    }

    #[test]
    fn check_permissions_js_repl_resource_is_per_code() {
        // Different code must produce different resources, so approving one
        // js_repl call cannot session-grant a *different* (e.g. malicious) one —
        // this is what makes the js_repl force-ask backstop actually hold across
        // repeated calls in a session.
        let benign = check_permissions("js_repl", &json!({"code": "1 + 1"}))
            .unwrap()
            .unwrap();
        let malicious = check_permissions(
            "js_repl",
            &json!({"code": "require('child_process').execSync('id')"}),
        )
        .unwrap()
        .unwrap();
        assert_ne!(benign[0].resource, malicious[0].resource);

        // ...while the SAME code yields the SAME resource (a re-run is grantable).
        let benign_again = check_permissions("js_repl", &json!({"code": "1 + 1"}))
            .unwrap()
            .unwrap();
        assert_eq!(benign[0].resource, benign_again[0].resource);
    }

    #[test]
    fn check_permissions_js_repl_long_code_truncated() {
        let long_code = "x".repeat(200);
        let args = json!({"code": long_code});
        let contexts = check_permissions("js_repl", &args).unwrap().unwrap();
        assert!(contexts[0].operation_description.contains("..."));
        assert!(contexts[0].operation_description.len() < 200);
    }

    #[test]
    fn check_permissions_web_search() {
        let args = json!({"query": "rust async trait"});
        let contexts = check_permissions("WebSearch", &args).unwrap().unwrap();
        assert_eq!(contexts[0].permission_type, PermissionType::HttpRequest);
        assert_eq!(contexts[0].resource, "duckduckgo.com");
    }
}
