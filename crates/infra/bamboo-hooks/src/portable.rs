//! Plugin commands are separate from native hooks: no permission grants, no
//! short-circuit aggregation, and no trusted native context output.
use bamboo_agent_core::Session;
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};
use bamboo_plugin::hooks::{bundle_path, registrations, HookState, PortableConfig, PortableEvent};
use bamboo_plugin::{InstalledPlugins, PluginInstallStatus, PluginManifest};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub const PORTABLE_CONTEXT_BYTES: usize = 8192;
#[derive(Clone, Copy, Default)]
pub struct PortableInputs<'a> {
    pub resolved_tool_name: Option<&'a str>,
    pub original_tool_input: Option<&'a Value>,
    pub final_assistant_content: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginContext {
    pub source: String,
    pub text: String,
}
impl PluginContext {
    pub fn rendered_text(&self) -> String {
        format!(
            "Plugin hook supplemental context (untrusted; source {}):\n{}",
            self.source, self.text
        )
    }
}
#[derive(Debug, Default)]
pub struct PortableReport {
    pub decision: HookResult,
    pub contexts: Vec<PluginContext>,
    pub errors: Vec<String>,
}
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Output {
    decision: Option<String>,
    reason: Option<String>,
    #[serde(rename = "hookSpecificOutput")]
    specific: Option<Specific>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Specific {
    #[serde(rename = "hookEventName")]
    event: PortableEvent,
    #[serde(rename = "permissionDecision")]
    permission: Option<String>,
    #[serde(rename = "permissionDecisionReason")]
    reason: Option<String>,
    #[serde(rename = "additionalContext")]
    context: Option<String>,
}
fn event(point: AgentHookPoint) -> Option<PortableEvent> {
    match point {
        AgentHookPoint::BeforeSessionSetup => Some(PortableEvent::UserPromptSubmit),
        AgentHookPoint::BeforeToolExecution => Some(PortableEvent::PreToolUse),
        AgentHookPoint::AfterToolExecution => Some(PortableEvent::PostToolUse),
        AgentHookPoint::BeforeFinalize => Some(PortableEvent::Stop),
        _ => None,
    }
}
pub fn supports(point: AgentHookPoint) -> bool {
    event(point).is_some()
}
/// Scheduling hint only: execution still rechecks current trust under the
/// operation lock. Empty, unreviewed and event-unrelated registrations
/// must not disable the host's normal parallel tool scheduling.
pub fn has_active_hooks_for(root: &Path, point: AgentHookPoint) -> bool {
    let Some(event) = event(point) else {
        return false;
    };
    let Some(store) = std::fs::read(root.join("installed.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<InstalledPlugins>(&bytes).ok())
    else {
        return false;
    };
    store.plugins.iter().any(|plugin| {
        if plugin.status != PluginInstallStatus::Installed
            || store.plugins.iter().filter(|p| p.id == plugin.id).count() != 1
            || !plugin
                .registered
                .hooks
                .iter()
                .any(|h| h.enabled && h.trusted_digest.as_deref() == Some(&h.digest))
        {
            return false;
        }
        let Some(manifest) = std::fs::read(plugin.plugin_dir.join("plugin.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PluginManifest>(&bytes).ok())
        else {
            return false;
        };
        if manifest.id != plugin.id
            || manifest.version != plugin.version
            || manifest.validate().is_err()
            || manifest.platforms.as_ref().is_some_and(|ps| {
                !bamboo_plugin::Platform::current().is_some_and(|p| ps.contains(&p))
            })
        {
            return false;
        }
        plugin.registered.hooks.iter().any(|h| {
            h.plugin_id == plugin.id
                && h.version == plugin.version
                && h.state(&h.digest) == HookState::Active
                && bundle_path(&plugin.plugin_dir, &h.config)
                    .ok()
                    .and_then(|path| {
                        use std::io::Read;
                        let mut bytes = Vec::new();
                        std::fs::File::open(path)
                            .ok()?
                            .take(65537)
                            .read_to_end(&mut bytes)
                            .ok()?;
                        Some(bytes)
                    })
                    .and_then(|bytes| PortableConfig::parse(&bytes).ok())
                    .is_some_and(|c| c.hooks.get(&event).is_some_and(|groups| !groups.is_empty()))
        })
    })
}

pub fn envelope(
    event: PortableEvent,
    payload: &HookPayload,
    session: &Session,
    cwd: &Path,
) -> Result<Value, String> {
    envelope_with_tool_input(event, payload, session, cwd, None, None)
}

fn envelope_with_tool_input(
    event: PortableEvent,
    payload: &HookPayload,
    session: &Session,
    cwd: &Path,
    original_tool_input: Option<&Value>,
    final_assistant_content: Option<&str>,
) -> Result<Value, String> {
    let mut value = json!({"session_id":session.id,"transcript_path":null,"cwd":cwd,"hook_event_name":event,"permission_mode":"default"});
    match (event, payload) {
        (
            PortableEvent::UserPromptSubmit,
            HookPayload::SessionSetup {
                initial_message, ..
            },
        ) => value["prompt"] = json!(initial_message),
        (PortableEvent::UserPromptSubmit, HookPayload::Prompt { prompt }) => {
            value["prompt"] = json!(prompt)
        }
        (
            PortableEvent::PreToolUse,
            HookPayload::ToolExecution {
                tool_name,
                tool_call_id,
                parsed_args,
            },
        ) => {
            value["tool_name"] = json!(tool_name);
            value["tool_use_id"] = json!(tool_call_id);
            value["tool_input"] = parsed_args.clone();
        }
        (
            PortableEvent::PostToolUse,
            HookPayload::ToolResult {
                tool_name,
                tool_call_id,
                outcome,
            },
        ) => {
            value["tool_name"] = json!(tool_name);
            value["tool_use_id"] = json!(tool_call_id);
            value["tool_input"] = if let Some(input) = original_tool_input {
                input.clone()
            } else {
                let call = session
                    .messages
                    .iter()
                    .rev()
                    .filter_map(|m| m.tool_calls.as_ref())
                    .flatten()
                    .find(|call| call.id == *tool_call_id)
                    .ok_or("PostToolUse original input unavailable")?;
                serde_json::from_str(&call.function.arguments)
                    .map_err(|e| format!("invalid original tool arguments: {e}"))?
            };
            value["tool_response"] = json!(outcome.result.as_ref().or(outcome.error.as_ref()));
        }
        (PortableEvent::Stop, HookPayload::Finalize { stop_hook_active }) => {
            value["stop_hook_active"] = json!(stop_hook_active);
            value["last_assistant_message"] = json!(final_assistant_content
                .map(str::to_owned)
                .unwrap_or_else(|| session
                    .messages
                    .iter()
                    .rev()
                    .find(|m| matches!(m.role, bamboo_agent_core::Role::Assistant))
                    .map(|m| &m.content)
                    .cloned()
                    .unwrap_or_default()));
        }
        _ => return Err("portable event/payload mismatch".into()),
    }
    Ok(value)
}
fn interpret(
    event: PortableEvent,
    output: crate::LifecycleHookTestOutput,
    stop_active: bool,
) -> Result<(HookResult, Option<String>), String> {
    if output.timed_out || output.stdout_truncated || output.stderr_truncated {
        return Err("hook timed out or exceeded output cap".into());
    }
    if output.exit_code == Some(2) {
        return Ok((block(event, output.stderr, stop_active)?, None));
    }
    if output.exit_code != Some(0) {
        return Err(format!(
            "hook failed: {:?}: {}",
            output.exit_code, output.stderr
        ));
    }
    if output.stdout.trim().is_empty() {
        return Ok((HookResult::Continue, None));
    }
    // Plain stdout is context only for prompt submission. JSON-shaped malformed
    // output must never fall back to text.
    if !output.stdout.trim_start().starts_with(['{', '[']) {
        if event == PortableEvent::UserPromptSubmit {
            return Ok((HookResult::Continue, Some(output.stdout)));
        }
        return Ok((HookResult::Continue, None));
    }
    let parsed: Output = serde_json::from_str(&output.stdout)
        .map_err(|e| format!("unsupported hook output: {e}"))?;
    let mut result = HookResult::Continue;
    if let Some(decision) = parsed.decision {
        if decision != "block" || event == PortableEvent::PreToolUse {
            return Err("unsupported decision/event combination".into());
        }
        result = block(
            event,
            parsed.reason.ok_or("block requires reason")?,
            stop_active,
        )?;
    } else if parsed.reason.is_some() {
        return Err("reason without decision".into());
    }
    let mut context = None;
    if let Some(specific) = parsed.specific {
        if specific.event != event {
            return Err("hookEventName mismatch".into());
        }
        if let Some(permission) = specific.permission {
            if event != PortableEvent::PreToolUse {
                return Err("permissionDecision only supported for PreToolUse".into());
            }
            match permission.as_str() {
                "deny" => {
                    result = block(
                        event,
                        specific
                            .reason
                            .ok_or("deny requires permissionDecisionReason")?,
                        stop_active,
                    )?;
                }
                "allow" => {} // Deliberately no authority beyond the host's existing policy.
                _ => return Err("unsupported permission decision".into()),
            }
        } else if specific.reason.is_some() {
            return Err("permission reason without decision".into());
        }
        if event == PortableEvent::Stop && specific.context.is_some() {
            return Err("Stop additionalContext unsupported".into());
        }
        context = specific.context;
    }
    Ok((result, context))
}
fn block(event: PortableEvent, reason: String, stop_active: bool) -> Result<HookResult, String> {
    if reason.trim().is_empty() {
        return Err("block requires nonempty reason".into());
    }
    let mut reason = reason;
    let mut end = reason.len().min(PORTABLE_CONTEXT_BYTES);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason.truncate(end);
    Ok(match event {
        PortableEvent::UserPromptSubmit => HookResult::Abort { reason },
        PortableEvent::Stop if stop_active => HookResult::Continue,
        // Existing Stop deny seam performs a single bounded continuation.
        _ => HookResult::Deny { reason },
    })
}
/// Load provenance on every seam, so uninstall/update/recovery cannot leave a
/// frozen active hook in a previously-created runner.
pub async fn run(
    root: &Path,
    point: AgentHookPoint,
    payload: &HookPayload,
    session: &Session,
) -> PortableReport {
    run_with_tool_input(root, point, payload, session, None).await
}

pub async fn run_with_tool_input(
    root: &Path,
    point: AgentHookPoint,
    payload: &HookPayload,
    session: &Session,
    original_tool_input: Option<&Value>,
) -> PortableReport {
    run_with_inputs(root, point, payload, session, original_tool_input, None).await
}

pub async fn run_with_inputs(
    root: &Path,
    point: AgentHookPoint,
    payload: &HookPayload,
    session: &Session,
    original_tool_input: Option<&Value>,
    final_assistant_content: Option<&str>,
) -> PortableReport {
    let mut report = PortableReport::default();
    let Some(event) = event(point) else {
        return report;
    };
    let store = match InstalledPlugins::load(&root.join("installed.json")).await {
        Ok(s) => s,
        Err(e) => {
            report.errors.push(e.to_string());
            return report;
        }
    };
    let mut remaining = PORTABLE_CONTEXT_BYTES;
    for plugin in &store.plugins {
        if plugin.status != PluginInstallStatus::Installed
            || store.plugins.iter().filter(|p| p.id == plugin.id).count() != 1
        {
            continue;
        }
        let manifest: PluginManifest = match tokio::fs::read(plugin.plugin_dir.join("plugin.json"))
            .await
            .and_then(|b| {
                serde_json::from_slice::<PluginManifest>(&b).map_err(std::io::Error::other)
            }) {
            Ok(m) if m.id == plugin.id && m.version == plugin.version => m,
            _ => {
                report
                    .errors
                    .push(format!("{}: invalid hook manifest identity", plugin.id));
                continue;
            }
        };
        if plugin.registered.hooks.is_empty() {
            continue;
        }
        if let Err(error) = manifest.validate() {
            report.errors.push(error.to_string());
            continue;
        }
        if manifest.platforms.as_ref().is_some_and(|platforms| {
            !bamboo_plugin::Platform::current().is_some_and(|p| platforms.contains(&p))
        }) {
            continue;
        }
        for registered in &plugin.registered.hooks {
            if registered.plugin_id != plugin.id || registered.version != plugin.version {
                continue;
            }
            if registered.state(&registered.digest) != HookState::Active {
                continue;
            }
            let reviewed = ReviewedHookConfig {
                registry_path: root.join("installed.json"),
                receipt: registered,
                root: &plugin.plugin_dir,
                data: root.join(".hook-data").join(&plugin.id),
                config: &registered.config,
                source: format!(
                    "{}@{}:{}:{event:?}",
                    plugin.id, plugin.version, registered.config
                ),
                digest: &registered.digest,
            };
            let result = run_config(
                event,
                payload,
                session,
                &reviewed,
                &mut remaining,
                original_tool_input,
                final_assistant_content,
            )
            .await;
            match result {
                Ok(mut next) => {
                    // Denial is sticky; every matched handler still runs.
                    if !matches!(
                        report.decision,
                        HookResult::Deny { .. } | HookResult::Abort { .. }
                    ) {
                        report.decision = next.decision;
                    }
                    report.contexts.append(&mut next.contexts);
                    report.errors.append(&mut next.errors);
                }
                Err(e) => report.errors.push(format!("{}: {e}", plugin.id)),
            }
        }
    }
    if event == PortableEvent::UserPromptSubmit
        && matches!(report.decision, HookResult::Abort { .. })
    {
        report.contexts.clear();
    }
    report
}
struct ReviewedHookConfig<'a> {
    registry_path: PathBuf,
    receipt: &'a bamboo_plugin::hooks::HookRegistration,
    root: &'a Path,
    data: PathBuf,
    config: &'a str,
    source: String,
    digest: &'a str,
}
async fn run_config(
    event: PortableEvent,
    payload: &HookPayload,
    session: &Session,
    reviewed: &ReviewedHookConfig<'_>,
    remaining: &mut usize,
    original_tool_input: Option<&Value>,
    final_assistant_content: Option<&str>,
) -> Result<PortableReport, String> {
    let path = bundle_path(reviewed.root, reviewed.config).map_err(|e| e.to_string())?;
    let config_bytes = tokio::fs::read(path).await.map_err(|e| e.to_string())?;
    run_config_snapshot(
        event,
        payload,
        session,
        reviewed,
        remaining,
        (original_tool_input, final_assistant_content),
        &config_bytes,
    )
    .await
}

async fn run_config_snapshot(
    event: PortableEvent,
    payload: &HookPayload,
    session: &Session,
    reviewed: &ReviewedHookConfig<'_>,
    remaining: &mut usize,
    inputs: (Option<&Value>, Option<&str>),
    config_bytes: &[u8],
) -> Result<PortableReport, String> {
    let (original_tool_input, final_assistant_content) = inputs;
    let root = reviewed.root;
    let data = &reviewed.data;
    let source = &reviewed.source;
    let reviewed_digest = reviewed.digest;
    let config = PortableConfig::parse(config_bytes).map_err(|e| e.to_string())?;
    let cwd = session
        .workspace
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir().map_err(|e| e.to_string())?);
    let input = envelope_with_tool_input(
        event,
        payload,
        session,
        &cwd,
        original_tool_input,
        final_assistant_content,
    )?;
    let stop_active = input["stop_hook_active"].as_bool().unwrap_or(false);
    let bytes = serde_json::to_vec(&input).map_err(|e| e.to_string())?;
    let mut report = PortableReport::default();
    if let Some(groups) = config.hooks.get(&event) {
        for group in groups {
            if let Some(matcher) = group
                .matcher
                .as_deref()
                .filter(|s| !s.is_empty() && *s != "*")
            {
                if !regex::Regex::new(matcher)
                    .map_err(|e| e.to_string())?
                    .is_match(input["tool_name"].as_str().unwrap_or(""))
                {
                    continue;
                }
            }
            for command in &group.hooks {
                let _operation = bamboo_plugin::registry::PLUGIN_OPERATION_LOCK.lock().await;
                // A preceding command may have changed its bundle. Recheck at
                // every spawn boundary, not merely once at the lifecycle seam.
                let review_check: Result<(), String> = async {
                    let store = InstalledPlugins::load(&reviewed.registry_path)
                        .await
                        .map_err(|e| e.to_string())?;
                    let installed = store
                        .get_unique(&reviewed.receipt.plugin_id)
                        .map_err(|e| e.to_string())?
                        .ok_or("plugin uninstalled before execution")?;
                    if installed.status != PluginInstallStatus::Installed
                        || installed.version != reviewed.receipt.version
                        || !installed.registered.hooks.iter().any(|hook| {
                            hook.config == reviewed.receipt.config
                                && hook.plugin_id == installed.id
                                && hook.version == installed.version
                                && hook.state(reviewed_digest) == HookState::Active
                        })
                    {
                        return Err("plugin hook execution trust revoked before spawn".into());
                    }
                    let current_manifest: PluginManifest = serde_json::from_slice(
                        &tokio::fs::read(root.join("plugin.json"))
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                    .map_err(|e| e.to_string())?;
                    let bundle = root.to_owned();
                    let current = tokio::task::spawn_blocking(move || {
                        current_manifest.validate()?;
                        registrations(&current_manifest, &bundle)
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                    if !current.iter().any(|r| r.digest == reviewed_digest) {
                        return Err("reviewed plugin bytes changed before command execution".into());
                    }
                    // The parsed snapshot may have come from a replacement
                    // bundle while a managed update held this lock. If that
                    // update rolled back, the old receipt/hash is valid again,
                    // but commands captured from its candidate are not trusted.
                    let current_path =
                        bundle_path(root, reviewed.config).map_err(|e| e.to_string())?;
                    let current_bytes = tokio::fs::read(current_path)
                        .await
                        .map_err(|e| e.to_string())?;
                    if current_bytes != config_bytes {
                        return Err("hook configuration changed before command execution".into());
                    }
                    tokio::fs::create_dir_all(data)
                        .await
                        .map_err(|e| e.to_string())
                }
                .await;
                if let Err(error) = review_check {
                    report.errors.push(error);
                    return Ok(report);
                }
                let execution = crate::configured::execute_portable_command(
                    &command.command,
                    bytes.clone(),
                    command.timeout,
                    &cwd,
                    root,
                    data,
                )
                .await;
                match execution.and_then(|o| interpret(event, o, stop_active)) {
                    Ok((decision, context)) => {
                        if !matches!(
                            report.decision,
                            HookResult::Deny { .. } | HookResult::Abort { .. }
                        ) {
                            report.decision = decision;
                        }
                        if let Some(mut text) = context {
                            let source: String = source.chars().take(512).collect();
                            let overhead = PluginContext {
                                source: source.clone(),
                                text: String::new(),
                            }
                            .rendered_text()
                            .len();
                            if *remaining <= overhead {
                                continue;
                            }
                            let mut end = text.len().min(*remaining - overhead);
                            while !text.is_char_boundary(end) {
                                end -= 1;
                            }
                            text.truncate(end);
                            *remaining -= end + overhead;
                            if !text.trim().is_empty() {
                                report.contexts.push(PluginContext { source, text });
                            }
                        }
                    }
                    Err(e) => report.errors.push(e),
                }
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_plugin::{InstalledPlugin, PluginSource, RegisteredCapabilities};
    fn output(code: i32, stdout: &str, stderr: &str) -> crate::LifecycleHookTestOutput {
        crate::LifecycleHookTestOutput {
            exit_code: Some(code),
            stdout: stdout.into(),
            stderr: stderr.into(),
            timed_out: false,
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }
    #[test]
    fn golden_decisions_and_permission_ceiling() {
        assert_eq!(interpret(PortableEvent::PreToolUse,output(0,r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}}"#,""),false).unwrap().0,HookResult::Continue);
        assert_eq!(interpret(PortableEvent::PreToolUse,output(0,r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"blocked"}}"#,""),false).unwrap().0,HookResult::Deny{reason:"blocked".into()});
        assert!(matches!(
            interpret(
                PortableEvent::UserPromptSubmit,
                output(2, "", "blocked"),
                false
            )
            .unwrap()
            .0,
            HookResult::Abort { .. }
        ));
        assert_eq!(
            interpret(
                PortableEvent::Stop,
                output(0, r#"{"decision":"block","reason":"continue"}"#, ""),
                true
            )
            .unwrap()
            .0,
            HookResult::Continue
        );
        assert!(matches!(
            interpret(PortableEvent::Stop, output(2, "", "continue"), false)
                .unwrap()
                .0,
            HookResult::Deny { .. }
        ));
        assert_eq!(
            interpret(PortableEvent::Stop, output(0, "", ""), false)
                .unwrap()
                .0,
            HookResult::Continue
        );
    }
    #[test]
    fn unsupported_outputs_are_errors() {
        for stdout in [
            r#"{"continue":false}"#,
            r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"ask"}}"#,
            r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","updatedInput":{}}}"#,
            r#"{"decision":"approve"}"#,
            r#"{"hookSpecificOutput":{"hookEventName":"Stop"}}"#,
            "{bad",
            "[1]",
        ] {
            assert!(
                interpret(PortableEvent::PreToolUse, output(0, stdout, ""), false).is_err(),
                "{stdout}"
            );
        }
        assert!(interpret(PortableEvent::PreToolUse, output(1, "", "failure"), false).is_err());
    }
    #[test]
    fn golden_input_retains_real_tool_name_and_arguments() {
        let session = Session::new("test", "model");
        let args = json!({"patch":"*** Begin Patch\n*** End Patch"});
        let input = envelope(
            PortableEvent::PreToolUse,
            &HookPayload::ToolExecution {
                tool_name: "apply_patch".into(),
                tool_call_id: "call-1".into(),
                parsed_args: args.clone(),
            },
            &session,
            Path::new("/workspace/path with spaces"),
        )
        .unwrap();
        assert_eq!(
            input,
            json!({"session_id":session.id,"transcript_path":null,"cwd":"/workspace/path with spaces","hook_event_name":"PreToolUse","permission_mode":"default","tool_name":"apply_patch","tool_use_id":"call-1","tool_input":args})
        );
    }
    #[test]
    fn golden_prompt_post_and_stop_inputs() {
        let session = Session::new("fixture", "model");
        let prompt = envelope(
            PortableEvent::UserPromptSubmit,
            &HookPayload::Prompt {
                prompt: "hello".into(),
            },
            &session,
            Path::new("/work"),
        )
        .unwrap();
        assert_eq!(prompt["prompt"], "hello");
        assert_eq!(prompt["hook_event_name"], "UserPromptSubmit");
        let original = json!({"patch":"actual patch"});
        let payload = HookPayload::ToolResult {
            tool_name: "apply_patch".into(),
            tool_call_id: "call".into(),
            outcome: bamboo_domain::HookToolOutcome {
                success: true,
                result: Some("done".into()),
                error: None,
                needs_human: false,
                duration_ms: 1,
            },
        };
        let post = envelope_with_tool_input(
            PortableEvent::PostToolUse,
            &payload,
            &session,
            Path::new("/work"),
            Some(&original),
            None,
        )
        .unwrap();
        assert_eq!(post["tool_input"], original);
        assert_eq!(post["tool_name"], "apply_patch");
        assert_eq!(post["tool_response"], "done");
        assert_eq!(post["tool_use_id"], "call");
        let stop = envelope(
            PortableEvent::Stop,
            &HookPayload::Finalize {
                stop_hook_active: true,
            },
            &session,
            Path::new("/work"),
        )
        .unwrap();
        assert_eq!(stop["stop_hook_active"], true);
        assert_eq!(stop["last_assistant_message"], "");
    }
    #[test]
    fn failed_tool_response_and_pending_stop_content_are_current() {
        let mut session = Session::new("fixture", "model");
        session.messages.push(bamboo_agent_core::Message::assistant(
            "previous reply",
            None,
        ));
        let payload = HookPayload::ToolResult {
            tool_name: "apply_patch".into(),
            tool_call_id: "call".into(),
            outcome: bamboo_domain::HookToolOutcome {
                success: false,
                result: None,
                error: Some("patch failed".into()),
                needs_human: false,
                duration_ms: 1,
            },
        };
        let post = envelope_with_tool_input(
            PortableEvent::PostToolUse,
            &payload,
            &session,
            Path::new("/work"),
            Some(&json!({"patch":"original"})),
            None,
        )
        .unwrap();
        assert_eq!(post["tool_response"], "patch failed");
        let stop = envelope_with_tool_input(
            PortableEvent::Stop,
            &HookPayload::Finalize {
                stop_hook_active: false,
            },
            &session,
            Path::new("/work"),
            None,
            Some("pending final reply"),
        )
        .unwrap();
        assert_eq!(stop["last_assistant_message"], "pending final reply");
        assert_eq!(session.messages.last().unwrap().content, "previous reply");
    }

    #[tokio::test]
    async fn scheduling_hint_requires_current_active_event() {
        let (temp, session, payload) =
            fixture(vec![json!({"type":"command","command":"true","timeout":1})]).await;
        let root = temp.path();
        assert!(!has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
        trust(root).await;
        assert!(has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
        assert!(!has_active_hooks_for(root, AgentHookPoint::BeforeFinalize));
        let path = root.join("installed.json");
        let mut store = InstalledPlugins::load(&path).await.unwrap();
        store.plugins[0].registered.hooks[0].enabled = false;
        store.save(&path).await.unwrap();
        assert!(!has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
        trust(root).await;
        tokio::fs::write(root.join("plugin with spaces/script.sh"), "changed")
            .await
            .unwrap();
        // A scheduling hint may conservatively remain true after bytes change;
        // only the blocking-safe locked spawn check decides execution trust.
        assert!(has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
        let report = run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(!report.errors.is_empty());
        assert!(matches!(report.decision, HookResult::Continue));
        let mut manifest: PluginManifest = serde_json::from_slice(
            &tokio::fs::read(root.join("plugin with spaces/plugin.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        tokio::fs::write(root.join("plugin with spaces/hooks.json"), serde_json::to_vec(&json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"true","timeout":1}]}]}})).unwrap()).await.unwrap();
        store.plugins[0].registered.hooks =
            registrations(&manifest, &store.plugins[0].plugin_dir).unwrap();
        store.save(&path).await.unwrap();
        trust(root).await;
        assert!(has_active_hooks_for(root, AgentHookPoint::BeforeFinalize));
        assert!(!has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
        manifest.provides.hooks.clear();
        tokio::fs::write(
            root.join("plugin with spaces/plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
        store.plugins[0].registered.hooks.clear();
        store.save(&path).await.unwrap();
        assert!(!has_active_hooks_for(
            root,
            AgentHookPoint::BeforeToolExecution
        ));
    }

    async fn fixture(commands: Vec<Value>) -> (tempfile::TempDir, Session, HookPayload) {
        let temp = tempfile::tempdir().unwrap();
        let bundle = temp.path().join("plugin with spaces");
        tokio::fs::create_dir_all(&bundle).await.unwrap();
        let manifest:PluginManifest=serde_json::from_value(json!({"id":"fixture","name":"Fixture","version":"0.1.0","provides":{"hooks":[{"config":"hooks.json","scripts":["script.sh"]}]}})).unwrap();
        tokio::fs::write(
            bundle.join("plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(bundle.join("script.sh"), "# reviewed fixture\n")
            .await
            .unwrap();
        tokio::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&json!({"hooks":{"PreToolUse":[{"hooks":commands}]}})).unwrap(),
        )
        .await
        .unwrap();
        let registered = registrations(&manifest, &bundle).unwrap();
        let entry = InstalledPlugin {
            id: manifest.id,
            version: manifest.version,
            source: PluginSource::LocalDir {
                path: bundle.clone(),
            },
            plugin_dir: bundle,
            installed_at: chrono::Utc::now(),
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                hooks: registered,
                ..Default::default()
            },
        };
        InstalledPlugins {
            plugins: vec![entry],
        }
        .save(&temp.path().join("installed.json"))
        .await
        .unwrap();
        let mut session = Session::new("test", "model");
        session.workspace = Some(temp.path().to_string_lossy().into());
        let payload = HookPayload::ToolExecution {
            tool_name: "apply_patch".into(),
            tool_call_id: "call".into(),
            parsed_args: json!({"patch":"original"}),
        };
        (temp, session, payload)
    }
    #[tokio::test]
    async fn persistent_data_does_not_overlap_a_plugin_named_data() {
        let (temp, session, payload) = fixture(vec![
            json!({"type":"command","command":"touch \"$PLUGIN_DATA/state\"","timeout":1}),
        ])
        .await;
        let root = temp.path();
        let other = root.join("data");
        std::fs::create_dir(&other).unwrap();
        for name in ["plugin.json", "script.sh", "hooks.json"] {
            std::fs::copy(root.join("plugin with spaces").join(name), other.join(name)).unwrap();
        }
        let mut manifest: PluginManifest =
            serde_json::from_slice(&std::fs::read(other.join("plugin.json")).unwrap()).unwrap();
        manifest.id = "data".into();
        std::fs::write(
            other.join("plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let before = registrations(&manifest, &other).unwrap();
        let mut store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        let mut data_entry = store.plugins[0].clone();
        data_entry.id = "data".into();
        data_entry.plugin_dir = other.clone();
        data_entry.registered.hooks = before.clone();
        store.plugins.push(data_entry);
        store.save(&root.join("installed.json")).await.unwrap();
        // Only approve the original fixture, never the data plugin.
        let digest = store.plugins[0].registered.hooks[0].digest.clone();
        store.plugins[0].registered.hooks[0]
            .confirm_review(&digest)
            .unwrap();
        store.save(&root.join("installed.json")).await.unwrap();
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(root.join(".hook-data/fixture/state").exists());
        assert_eq!(registrations(&manifest, &other).unwrap(), before);
        manifest.id = ".hook-data".into();
        assert!(manifest.validate().is_err());
    }

    async fn trust(root: &Path) {
        let path = root.join("installed.json");
        let mut store = InstalledPlugins::load(&path).await.unwrap();
        for hook in &mut store.plugins[0].registered.hooks {
            hook.confirm_review(&hook.digest.clone()).unwrap();
        }
        store.save(&path).await.unwrap();
    }
    #[tokio::test]
    async fn rolled_back_update_cannot_execute_a_candidate_config_snapshot() {
        let (temp, session, payload) =
            fixture(vec![json!({"type":"command","command":"true","timeout":1})]).await;
        let root = temp.path();
        trust(root).await;
        let store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        let installed = &store.plugins[0];
        let receipt = &installed.registered.hooks[0];
        let reviewed = ReviewedHookConfig {
            registry_path: root.join("installed.json"),
            receipt,
            root: &installed.plugin_dir,
            data: root.join(".hook-data/fixture"),
            config: &receipt.config,
            source: "fixture@0.1.0:hooks.json:PreToolUse".into(),
            digest: &receipt.digest,
        };
        // The on-disk bundle and receipt have already rolled back. This is the
        // candidate config an in-flight invocation captured before the lock.
        let candidate = serde_json::to_vec(&json!({"hooks":{"PreToolUse":[{"hooks":[{
            "type":"command","command":"touch \"$PLUGIN_DATA/unreviewed\"","timeout":1
        }]}]}}))
        .unwrap();
        let mut remaining = PORTABLE_CONTEXT_BYTES;
        let report = run_config_snapshot(
            PortableEvent::PreToolUse,
            &payload,
            &session,
            &reviewed,
            &mut remaining,
            (None, None),
            &candidate,
        )
        .await
        .unwrap();
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("configuration changed")));
        assert!(!reviewed.data.join("unreviewed").exists());
        // Restored, reviewed commands remain usable after rejecting the stale snapshot.
        let report = run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    #[tokio::test]
    async fn install_update_uninstall_and_incomplete_execution_boundaries() {
        let (temp, session, payload) = fixture(vec![
            json!({"type":"command","command":"touch \"$PLUGIN_DATA/executed\"","timeout":1}),
        ])
        .await;
        let root = temp.path();
        let marker = root.join(".hook-data/fixture/executed");
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(!marker.exists());
        trust(root).await;
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(marker.exists());
        tokio::fs::remove_file(&marker).await.unwrap();
        tokio::fs::write(root.join("plugin with spaces/script.sh"), "changed")
            .await
            .unwrap();
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(!marker.exists());
        tokio::fs::write(
            root.join("plugin with spaces/script.sh"),
            "# reviewed fixture\n",
        )
        .await
        .unwrap();
        let path = root.join("installed.json");
        let mut store = InstalledPlugins::load(&path).await.unwrap();
        store.plugins[0].status = PluginInstallStatus::Installing;
        store.save(&path).await.unwrap();
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(!marker.exists());
        InstalledPlugins::default().save(&path).await.unwrap();
        run(
            root,
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(!marker.exists());
    }
    #[tokio::test]
    async fn deny_wins_over_noop_in_both_orders_and_all_handlers_run() {
        let deny = json!({"type":"command","command":"echo '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"blocked\"}}'","timeout":1});
        let allow = json!({"type":"command","command":"echo '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"allow\"}}'; touch \"$PLUGIN_DATA/observer\"","timeout":1});
        for commands in [vec![deny.clone(), allow.clone()], vec![allow, deny]] {
            let (temp, session, payload) = fixture(commands).await;
            trust(temp.path()).await;
            let report = run(
                temp.path(),
                AgentHookPoint::BeforeToolExecution,
                &payload,
                &session,
            )
            .await;
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(matches!(report.decision, HookResult::Deny { .. }));
            assert!(temp.path().join(".hook-data/fixture/observer").exists());
        }
    }
    #[tokio::test]
    async fn installation_boundary_blocks_spawn_and_observes_revocation() {
        let (temp, session, payload) = fixture(vec![
            json!({"type":"command","command":"touch \"$PLUGIN_DATA/should-not-run\"","timeout":1}),
        ])
        .await;
        trust(temp.path()).await;
        let guard = bamboo_plugin::registry::PLUGIN_OPERATION_LOCK.lock().await;
        let root = temp.path().to_path_buf();
        let task = tokio::spawn(async move {
            run(
                &root,
                AgentHookPoint::BeforeToolExecution,
                &payload,
                &session,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!temp
            .path()
            .join(".hook-data/fixture/should-not-run")
            .exists());
        let path = temp.path().join("installed.json");
        let mut store = InstalledPlugins::load(&path).await.unwrap();
        store.plugins[0].registered.hooks[0].enabled = false;
        store.save(&path).await.unwrap();
        drop(guard);
        let report = task.await.unwrap();
        assert!(!temp
            .path()
            .join(".hook-data/fixture/should-not-run")
            .exists());
        assert!(matches!(report.decision, HookResult::Continue));
    }
    #[tokio::test]
    async fn in_seam_bundle_change_skips_later_commands_without_losing_denial() {
        let first = json!({"type":"command","command":"echo changed > \"$PLUGIN_ROOT/script.sh\"; echo '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"blocked\"}}'","timeout":1});
        let second =
            json!({"type":"command","command":"touch \"$PLUGIN_DATA/should-not-run\"","timeout":1});
        let (temp, session, payload) = fixture(vec![first, second]).await;
        trust(temp.path()).await;
        let report = run(
            temp.path(),
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(matches!(report.decision, HookResult::Deny { .. }));
        assert!(!report.errors.is_empty());
        assert!(!temp
            .path()
            .join(".hook-data/fixture/should-not-run")
            .exists());
    }
    #[tokio::test]
    async fn contexts_share_one_budget_across_matching_commands() {
        let command = json!({"type":"command","command":"printf '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"additionalContext\":\"'; head -c 6000 /dev/zero | tr '\\0' x; printf '\"}}'","timeout":2});
        let (temp, session, payload) = fixture(vec![command.clone(), command]).await;
        trust(temp.path()).await;
        let report = run(
            temp.path(),
            AgentHookPoint::BeforeToolExecution,
            &payload,
            &session,
        )
        .await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            report
                .contexts
                .iter()
                .map(|c| c.rendered_text().len())
                .sum::<usize>(),
            PORTABLE_CONTEXT_BYTES
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_background_descendants_and_handles_spaces() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root with spaces");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let marker = root.join("escaped");
        let output = crate::configured::execute_portable_command(
            "(sleep 2; touch \"$PLUGIN_ROOT/escaped\") & wait",
            b"{}".to_vec(),
            1,
            &root,
            &root,
            &root,
        )
        .await
        .unwrap();
        assert!(output.timed_out);
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert!(!marker.exists());
    }
}
