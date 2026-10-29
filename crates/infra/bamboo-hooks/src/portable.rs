//! Plugin commands are separate from native hooks: no permission grants, no
//! short-circuit aggregation, and no trusted native context output.
use bamboo_agent_core::Session;
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};
use bamboo_plugin::hooks::{bundle_path, registrations, HookState, PortableConfig, PortableEvent};
use bamboo_plugin::{InstalledPlugins, PluginInstallStatus, PluginManifest};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const PORTABLE_CONTEXT_BYTES: usize = 8192;
// The existing bundle hashing budget is 64 MiB and PortableConfig::parse
// accepts at most 64 KiB. Apply equivalent limits before reading for the
// best-effort usage observer, including its installed-plugin registry.
const USAGE_PLUGIN_METADATA_BYTES: usize = 64 * 1024 * 1024;
const USAGE_HOOK_CONFIG_BYTES: usize = 64 * 1024;

// A cancelled spawn_blocking task can continue its filesystem work. Keep its
// permit inside the blocking closure so repeated usage-observer timeouts cannot
// create an unbounded number of these usage-specific tasks.
fn usage_review_slot() -> Result<OwnedSemaphorePermit, String> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(4)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| "model usage plugin review budget exhausted".to_string())
}

fn spawn_review<T: Send + 'static>(
    permit: Option<OwnedSemaphorePermit>,
    work: impl FnOnce() -> T + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
}

async fn resolve_bundle_path(
    event: PortableEvent,
    root: &Path,
    relative: &str,
) -> Result<PathBuf, String> {
    if event != PortableEvent::BambooModelCallUsage {
        return bundle_path(root, relative).map_err(|error| error.to_string());
    }
    let permit = usage_review_slot()?;
    let root = root.to_owned();
    let relative = relative.to_owned();
    spawn_review(Some(permit), move || {
        bundle_path(&root, &relative).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn resolve_working_directory(
    event: PortableEvent,
    workspace: Option<&str>,
) -> Result<PathBuf, String> {
    if event == PortableEvent::BambooModelCallUsage {
        if let Some(workspace) = workspace {
            return Ok(PathBuf::from(workspace));
        }
        let permit = usage_review_slot()?;
        return spawn_review(Some(permit), std::env::current_dir)
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string());
    }
    Ok(workspace
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir().map_err(|error| error.to_string())?))
}

async fn load_plugins(event: PortableEvent, path: &Path) -> Result<InstalledPlugins, String> {
    if event != PortableEvent::BambooModelCallUsage {
        return InstalledPlugins::load(path)
            .await
            .map_err(|error| error.to_string());
    }
    let permit = usage_review_slot()?;
    let path = path.to_owned();
    spawn_review(Some(permit), move || {
        if !path.try_exists().map_err(|error| error.to_string())? {
            return Ok(InstalledPlugins::default());
        }
        let bytes = read_usage_file(&path, USAGE_PLUGIN_METADATA_BYTES)?;
        let raw = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
        if raw.trim().is_empty() {
            return Ok(InstalledPlugins::default());
        }
        serde_json::from_str(raw).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
}

fn read_usage_file(path: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > max_bytes {
        return Err("model usage plugin metadata exceeds read limit".to_string());
    }
    Ok(bytes)
}

async fn read_plugin_file(
    event: PortableEvent,
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    if event != PortableEvent::BambooModelCallUsage {
        return tokio::fs::read(path)
            .await
            .map_err(|error| error.to_string());
    }
    let permit = usage_review_slot()?;
    let path = path.to_owned();
    spawn_review(Some(permit), move || read_usage_file(&path, max_bytes))
        .await
        .map_err(|error| error.to_string())?
}

async fn create_plugin_data_dir(event: PortableEvent, path: &Path) -> Result<(), String> {
    if event != PortableEvent::BambooModelCallUsage {
        return tokio::fs::create_dir_all(path)
            .await
            .map_err(|error| error.to_string());
    }
    let permit = usage_review_slot()?;
    let path = path.to_owned();
    spawn_review(Some(permit), move || {
        std::fs::create_dir_all(path).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
}

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
        AgentHookPoint::AfterModelCallUsage => Some(PortableEvent::BambooModelCallUsage),
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
    if let (PortableEvent::BambooModelCallUsage, HookPayload::ModelCallUsage { observation }) =
        (event, payload)
    {
        return Ok(
            json!({"schema_version":1,"hook_event_name":event,"session_id":session.id,"observation":observation}),
        );
    }
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
    let store = match load_plugins(event, &root.join("installed.json")).await {
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
        let manifest: PluginManifest = match read_plugin_file(
            event,
            &plugin.plugin_dir.join("plugin.json"),
            USAGE_PLUGIN_METADATA_BYTES,
        )
        .await
        .and_then(|b| {
            serde_json::from_slice::<PluginManifest>(&b).map_err(|error| error.to_string())
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
    let path = resolve_bundle_path(event, reviewed.root, reviewed.config).await?;
    let config_bytes = read_plugin_file(event, &path, USAGE_HOOK_CONFIG_BYTES).await?;
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
    let cwd = resolve_working_directory(event, session.workspace.as_deref()).await?;
    let input = envelope_with_tool_input(
        event,
        payload,
        session,
        &cwd,
        original_tool_input,
        final_assistant_content,
    )?;
    let legacy_inputs =
        crate::shell_compatibility::legacy_shell_projections(payload, session, original_tool_input)
            .into_iter()
            .map(|projected| {
                envelope_with_tool_input(
                    event,
                    &projected.payload,
                    session,
                    &cwd,
                    Some(&projected.input),
                    final_assistant_content,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
    let stop_active = input["stop_hook_active"].as_bool().unwrap_or(false);
    let mut report = PortableReport::default();
    if let Some(groups) = config.hooks.get(&event) {
        for group in groups {
            let matcher = group
                .matcher
                .as_deref()
                .filter(|s| !s.is_empty() && *s != "*")
                .map(regex::Regex::new)
                .transpose()
                .map_err(|e| e.to_string())?;
            let matches = |input: &Value| {
                matcher.as_ref().is_none_or(|matcher| {
                    matcher.is_match(input["tool_name"].as_str().unwrap_or(""))
                })
            };
            // Canonical/wildcard groups run once. Legacy-only groups evaluate
            // both operations of a combined continuation so an output-side
            // restriction is not skipped after matching its stdin/kill side.
            let selected_inputs: Vec<_> = if matches(&input) {
                vec![&input]
            } else {
                legacy_inputs
                    .iter()
                    .filter(|legacy| matches(legacy))
                    .collect()
            };
            for selected_input in selected_inputs {
                let bytes = serde_json::to_vec(selected_input).map_err(|e| e.to_string())?;
                for command in &group.hooks {
                    let _operation = bamboo_plugin::registry::PLUGIN_OPERATION_LOCK.lock().await;
                    // A preceding command may have changed its bundle. Recheck at
                    // every spawn boundary, not merely once at the lifecycle seam.
                    let review_check: Result<(), String> = async {
                        let store = load_plugins(event, &reviewed.registry_path).await?;
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
                            &read_plugin_file(
                                event,
                                &root.join("plugin.json"),
                                USAGE_PLUGIN_METADATA_BYTES,
                            )
                            .await?,
                        )
                        .map_err(|e| e.to_string())?;
                        let bundle = root.to_owned();
                        let permit = if event == PortableEvent::BambooModelCallUsage {
                            Some(usage_review_slot()?)
                        } else {
                            None
                        };
                        let current = spawn_review(permit, move || {
                            current_manifest.validate()?;
                            registrations(&current_manifest, &bundle)
                        })
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                        if !current.iter().any(|r| r.digest == reviewed_digest) {
                            return Err(
                                "reviewed plugin bytes changed before command execution".into()
                            );
                        }
                        // The parsed snapshot may have come from a replacement
                        // bundle while a managed update held this lock. If that
                        // update rolled back, the old receipt/hash is valid again,
                        // but commands captured from its candidate are not trusted.
                        let current_path =
                            resolve_bundle_path(event, root, reviewed.config).await?;
                        let current_bytes =
                            read_plugin_file(event, &current_path, USAGE_HOOK_CONFIG_BYTES).await?;
                        if current_bytes != config_bytes {
                            return Err(
                                "hook configuration changed before command execution".into()
                            );
                        }
                        create_plugin_data_dir(event, data).await
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
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_plugin::{InstalledPlugin, PluginSource, RegisteredCapabilities};

    #[tokio::test]
    async fn cancelled_usage_review_keeps_its_slot_until_blocking_work_finishes() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().try_acquire_owned().unwrap();
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let task = spawn_review(Some(permit), move || {
            let _ = started.send(());
            let _ = wait.recv();
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), observed)
            .await
            .expect("blocking review did not start")
            .unwrap();
        // The outer observer future may be dropped while the blocking task is
        // still executing. Its slot must not be returned at that point.
        drop(task);
        assert!(slots.clone().try_acquire_owned().is_err());
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(permit) = slots.clone().try_acquire_owned() {
                    drop(permit);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("completed blocking review did not release its slot");
    }

    #[tokio::test]
    async fn usage_bundle_resolution_preserves_the_existing_path_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hooks.json");
        std::fs::write(&file, b"{}").unwrap();
        let event = PortableEvent::BambooModelCallUsage;
        assert_eq!(
            resolve_bundle_path(event, dir.path(), "hooks.json")
                .await
                .unwrap(),
            file.canonicalize().unwrap()
        );
        assert!(resolve_bundle_path(event, dir.path(), "../outside")
            .await
            .is_err());
        assert_eq!(
            resolve_working_directory(event, Some("synthetic-workspace"))
                .await
                .unwrap(),
            PathBuf::from("synthetic-workspace")
        );
    }

    #[tokio::test]
    async fn usage_filesystem_helpers_preserve_registry_and_io_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let registry = dir.path().join("installed.json");
        let event = PortableEvent::BambooModelCallUsage;
        assert!(load_plugins(event, &registry)
            .await
            .unwrap()
            .plugins
            .is_empty());
        std::fs::write(&registry, b" ").unwrap();
        assert!(load_plugins(event, &registry)
            .await
            .unwrap()
            .plugins
            .is_empty());
        std::fs::write(&registry, b"invalid json").unwrap();
        assert!(load_plugins(event, &registry).await.is_err());
        std::fs::write(&registry, br#"{"plugins":[]}"#).unwrap();
        assert!(load_plugins(event, &registry)
            .await
            .unwrap()
            .plugins
            .is_empty());
        assert_eq!(
            read_plugin_file(event, &registry, USAGE_PLUGIN_METADATA_BYTES)
                .await
                .unwrap(),
            br#"{"plugins":[]}"#
        );
        assert!(read_usage_file(&registry, 4).is_err());
        assert!(read_plugin_file(event, &registry, 4).await.is_err());
        let config = dir.path().join("hooks.json");
        std::fs::write(&config, vec![b' '; USAGE_HOOK_CONFIG_BYTES + 1]).unwrap();
        assert!(read_plugin_file(event, &config, USAGE_HOOK_CONFIG_BYTES)
            .await
            .is_err());
        let data = dir.path().join("nested").join("data");
        create_plugin_data_dir(event, &data).await.unwrap();
        assert!(data.is_dir());
    }

    #[test]
    fn model_usage_envelope_omits_workspace_and_preserves_unknown_values() {
        let mut session = Session::new("usage-test", "synthetic-model");
        session.workspace = Some("/PRIVATE_WORKSPACE_SENTINEL".into());
        let payload = HookPayload::ModelCallUsage {
            observation: bamboo_domain::ModelCallUsage {
                schema_version: 1,
                event_id: "event-1".into(),
                call_id: "call-1".into(),
                session_id: session.id.clone(),
                run_id: None,
                provider: Some("openai".into()),
                model: Some("synthetic-model".into()),
                status: bamboo_domain::ModelCallStatus::Success,
                trace_id: None,
                span_id: None,
                usage: Some(bamboo_domain::ModelTokenUsage {
                    cache_read_input_tokens: Some(0),
                    ..Default::default()
                }),
            },
        };
        let value = envelope(
            PortableEvent::BambooModelCallUsage,
            &payload,
            &session,
            std::path::Path::new("/PRIVATE_CWD_SENTINEL"),
        )
        .unwrap();
        assert_eq!(value["observation"]["usage"]["cache_read_input_tokens"], 0);
        assert!(value["observation"]["usage"]["cache_creation_input_tokens"].is_null());
        assert!(!value.to_string().contains("PRIVATE_"));
        assert!(value.get("cwd").is_none());
    }
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

    struct RecordingTerminalExecutor(std::sync::Mutex<Vec<Value>>);
    #[async_trait::async_trait]
    impl crate::HookCommandExecutor for RecordingTerminalExecutor {
        async fn execute(
            &self,
            _command: tokio::process::Command,
            input: Vec<u8>,
            _timeout: std::time::Duration,
        ) -> std::io::Result<crate::HookCommandOutput> {
            let input: Value = serde_json::from_slice(&input).unwrap();
            let deny = (input["tool_name"] == "Bash" || input["tool_name"] == "BashOutput")
                && input["hook_event_name"] == "PreToolUse";
            self.0.lock().unwrap().push(input);
            Ok(crate::HookCommandOutput {
                exit_code: Some(0),
                stdout: crate::CapturedHookOutput {
                    bytes: if deny {
                        br#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"legacy policy"}}"#.to_vec()
                    } else {
                        Vec::new()
                    },
                    truncated: false,
                },
                stderr: crate::CapturedHookOutput::default(),
                timed_out: false,
            })
        }
    }

    #[tokio::test]
    async fn reviewed_portable_usage_extension_receives_only_the_snapshot() {
        let command = json!({"type":"command", "command":"record-envelope", "timeout":1});
        let (temp, session, _) = fixture(vec![command.clone()]).await;
        let root = temp.path();
        let bundle = root.join("plugin with spaces");
        tokio::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&json!({"hooks": {
                "BambooModelCallUsage":[{"hooks":[command]}]
            }}))
            .unwrap(),
        )
        .await
        .unwrap();
        let manifest: PluginManifest =
            serde_json::from_slice(&tokio::fs::read(bundle.join("plugin.json")).await.unwrap())
                .unwrap();
        let mut store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        store.plugins[0].registered.hooks = registrations(&manifest, &bundle).unwrap();
        store.save(&root.join("installed.json")).await.unwrap();
        trust(root).await;
        let payload = HookPayload::ModelCallUsage {
            observation: bamboo_domain::ModelCallUsage {
                schema_version: 1,
                event_id: "event-1".into(),
                call_id: "call-1".into(),
                session_id: session.id.clone(),
                run_id: None,
                provider: Some("openai".into()),
                model: Some("synthetic-model".into()),
                status: bamboo_domain::ModelCallStatus::Success,
                trace_id: None,
                span_id: None,
                usage: Some(bamboo_domain::ModelTokenUsage {
                    cache_read_input_tokens: Some(0),
                    ..Default::default()
                }),
            },
        };
        let recorder =
            std::sync::Arc::new(RecordingTerminalExecutor(std::sync::Mutex::new(Vec::new())));
        let report = crate::with_hook_command_executor(
            recorder.clone(),
            run(
                root,
                AgentHookPoint::AfterModelCallUsage,
                &payload,
                &session,
            ),
        )
        .await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let seen = recorder.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0]["hook_event_name"], "BambooModelCallUsage");
        assert_eq!(
            seen[0]["observation"]["usage"]["cache_read_input_tokens"],
            0
        );
        assert!(seen[0].get("cwd").is_none());
        assert!(!seen[0]
            .to_string()
            .contains(&root.to_string_lossy().to_string()));
    }

    #[tokio::test]
    async fn portable_legacy_only_overlap_evaluates_output_policy_after_input_eof_or_kill() {
        let command = json!({"type":"command", "command":"record-envelope", "timeout":1});
        let (temp, session, _) = fixture(vec![command.clone()]).await;
        let root = temp.path();
        let bundle = root.join("plugin with spaces");
        let groups: Vec<_> = [
            "^write_stdin$",
            "*",
            "^(write_stdin|BashInput|KillShell|BashOutput)$",
            "^(BashInput|KillShell|BashOutput)$",
        ]
        .into_iter()
        .map(|matcher| json!({"matcher":matcher,"hooks":[command.clone()]}))
        .collect();
        std::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&json!({"hooks":{"PreToolUse":groups}})).unwrap(),
        )
        .unwrap();
        let manifest: PluginManifest =
            serde_json::from_slice(&std::fs::read(bundle.join("plugin.json")).unwrap()).unwrap();
        let mut store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        store.plugins[0].registered.hooks = registrations(&manifest, &bundle).unwrap();
        store.save(&root.join("installed.json")).await.unwrap();
        trust(root).await;

        for (args, primary_name) in [
            (json!({"session_id":"exec", "chars":"yes\n"}), "BashInput"),
            (json!({"session_id":"exec", "eof":true}), "BashInput"),
            (json!({"session_id":"exec", "chars":"\u{3}"}), "KillShell"),
        ] {
            let recorder =
                std::sync::Arc::new(RecordingTerminalExecutor(std::sync::Mutex::new(Vec::new())));
            let payload = HookPayload::ToolExecution {
                tool_name: "write_stdin".into(),
                tool_call_id: "overlap-call".into(),
                parsed_args: args.clone(),
            };
            let report = crate::with_hook_command_executor(
                recorder.clone(),
                run_with_tool_input(
                    root,
                    AgentHookPoint::BeforeToolExecution,
                    &payload,
                    &session,
                    Some(&args),
                ),
            )
            .await;
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(matches!(report.decision, HookResult::Deny { .. }));
            let seen = recorder.0.lock().unwrap();
            assert_eq!(seen.len(), 5);
            for input in &seen[..3] {
                assert_eq!(input["tool_name"], "write_stdin");
                assert_eq!(input["tool_input"], args);
            }
            assert_eq!(seen[3]["tool_name"], primary_name);
            assert_eq!(seen[4]["tool_name"], "BashOutput");
            assert_eq!(seen[4]["tool_input"], json!({"bash_id":"exec"}));
        }
    }

    #[tokio::test]
    async fn portable_legacy_shell_hooks_keep_command_policy_and_primary_hooks_run_once() {
        let (temp, session, _) = fixture(vec![
            json!({"type":"command", "command":"record-envelope", "timeout":1}),
        ])
        .await;
        let root = temp.path();
        let bundle = root.join("plugin with spaces");
        let command = json!({"type":"command", "command":"record-envelope", "timeout":1});
        let config = json!({"hooks": {
            "PreToolUse": [
                {"matcher":"^exec_command$", "hooks":[command.clone()]},
                {"matcher":"*", "hooks":[command.clone()]},
                {"matcher":"^(exec_command|Bash)$", "hooks":[command.clone()]},
                {"matcher":"^Bash$", "hooks":[command.clone()]}
            ],
            "PostToolUse": [
                {"matcher":"^Bash$", "hooks":[command.clone()]},
                {"matcher":"^exec_command$", "hooks":[command]}
            ]
        }});
        std::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        let manifest: PluginManifest =
            serde_json::from_slice(&std::fs::read(bundle.join("plugin.json")).unwrap()).unwrap();
        let mut store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        store.plugins[0].registered.hooks = registrations(&manifest, &bundle).unwrap();
        store.save(&root.join("installed.json")).await.unwrap();
        trust(root).await;
        let recorder =
            std::sync::Arc::new(RecordingTerminalExecutor(std::sync::Mutex::new(Vec::new())));
        let args = json!({"cmd":"cargo test"});
        let payload = HookPayload::ToolExecution {
            tool_name: "exec_command".into(),
            tool_call_id: "exec-call".into(),
            parsed_args: args.clone(),
        };
        let report = crate::with_hook_command_executor(
            recorder.clone(),
            run(
                root,
                AgentHookPoint::BeforeToolExecution,
                &payload,
                &session,
            ),
        )
        .await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            matches!(report.decision, HookResult::Deny { ref reason } if reason == "legacy policy")
        );
        let post = HookPayload::ToolResult {
            tool_name: "exec_command".into(),
            tool_call_id: "exec-call".into(),
            outcome: bamboo_domain::HookToolOutcome {
                success: true,
                result: Some("done".into()),
                error: None,
                needs_human: false,
                duration_ms: 1,
            },
        };
        let report = crate::with_hook_command_executor(
            recorder.clone(),
            run_with_tool_input(
                root,
                AgentHookPoint::AfterToolExecution,
                &post,
                &session,
                Some(&args),
            ),
        )
        .await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let seen = recorder.0.lock().unwrap();
        assert_eq!(seen.len(), 6);
        for input in &seen[..3] {
            assert_eq!(input["tool_name"], "exec_command");
            assert_eq!(input["tool_input"], args);
        }
        for input in [&seen[3], &seen[4]] {
            assert_eq!(input["tool_name"], "Bash");
            assert_eq!(input["tool_input"], json!({"command":"cargo test"}));
        }
        assert_eq!(seen[4]["tool_response"], "done");
        assert_eq!(seen[5]["tool_name"], "exec_command");
    }

    #[tokio::test]
    async fn portable_output_policy_covers_input_eof_and_kill_without_duplicate_canonical_groups() {
        let command = json!({"type":"command", "command":"record-envelope", "timeout":1});
        let (temp, session, _) = fixture(vec![command.clone()]).await;
        let root = temp.path();
        let bundle = root.join("plugin with spaces");
        let groups: Vec<_> = [
            "^write_stdin$",
            "*",
            "^(write_stdin|BashOutput)$",
            "^(BashInput|KillShell|BashOutput)$",
            "^BashOutput$",
        ]
        .into_iter()
        .map(|matcher| json!({"matcher":matcher,"hooks":[command.clone()]}))
        .collect();
        std::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&json!({"hooks":{"PreToolUse":groups,"PostToolUse":groups}}))
                .unwrap(),
        )
        .unwrap();
        let manifest: PluginManifest =
            serde_json::from_slice(&std::fs::read(bundle.join("plugin.json")).unwrap()).unwrap();
        let mut store = InstalledPlugins::load(&root.join("installed.json"))
            .await
            .unwrap();
        store.plugins[0].registered.hooks = registrations(&manifest, &bundle).unwrap();
        store.save(&root.join("installed.json")).await.unwrap();
        trust(root).await;

        for (args, primary) in [
            (
                json!({"session_id":"exec", "chars":"yes\n", "eof":false, "yield_time_ms":250, "max_output_tokens":10}),
                "BashInput",
            ),
            (json!({"session_id":"exec", "eof":true}), "BashInput"),
            (json!({"session_id":"exec", "chars":"\u{3}"}), "KillShell"),
        ] {
            for (point, payload) in [
                (
                    AgentHookPoint::BeforeToolExecution,
                    HookPayload::ToolExecution {
                        tool_name: "write_stdin".into(),
                        tool_call_id: "combined-call".into(),
                        parsed_args: args.clone(),
                    },
                ),
                (
                    AgentHookPoint::AfterToolExecution,
                    HookPayload::ToolResult {
                        tool_name: "write_stdin".into(),
                        tool_call_id: "combined-call".into(),
                        outcome: bamboo_domain::HookToolOutcome {
                            success: true,
                            result: Some("output".into()),
                            error: None,
                            needs_human: false,
                            duration_ms: 1,
                        },
                    },
                ),
            ] {
                let recorder = std::sync::Arc::new(RecordingTerminalExecutor(
                    std::sync::Mutex::new(Vec::new()),
                ));
                let report = crate::with_hook_command_executor(
                    recorder.clone(),
                    run_with_tool_input(root, point, &payload, &session, Some(&args)),
                )
                .await;
                assert!(report.errors.is_empty(), "{:?}", report.errors);
                if point == AgentHookPoint::BeforeToolExecution {
                    assert!(matches!(report.decision, HookResult::Deny { .. }));
                }
                let seen = recorder.0.lock().unwrap();
                assert_eq!(seen.len(), 6);
                for input in &seen[..3] {
                    assert_eq!(input["tool_name"], "write_stdin");
                    assert_eq!(input["tool_input"], args);
                }
                assert_eq!(seen[3]["tool_name"], primary);
                assert!(seen[3]["tool_input"].get("yield_time_ms").is_none());
                assert!(seen[3]["tool_input"].get("max_output_tokens").is_none());
                for output in [&seen[4], &seen[5]] {
                    assert_eq!(output["tool_name"], "BashOutput");
                    assert_eq!(output["tool_input"], json!({"bash_id":"exec"}));
                    if point == AgentHookPoint::AfterToolExecution {
                        assert_eq!(output["tool_response"], "output");
                    }
                }
            }
        }
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
