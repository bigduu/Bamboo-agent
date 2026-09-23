use std::sync::OnceLock;
use std::{fs, path::Path};

use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sha2_11::Sha256 as Sha256V11;

use crate::bash_security;
use crate::hierarchy::PermissionRuleSet;
use crate::{PermissionContext, PermissionError, PermissionType};

const DELETE_COMMANDS: [&str; 7] = ["rm", "rmdir", "del", "erase", "unlink", "rd", "remove-item"];

/// Maximum number of independent permission contexts that can be carried by
/// one proactive request. This matches the bounded replay ledger so every
/// schema-valid request can eventually complete.
pub const MAX_PROACTIVE_PERMISSION_BATCH: usize = 64;

/// Keep selector-bound `fill` grants tied to exact bytes without putting
/// potentially sensitive browser input into permission resources or logs.
/// Those remembered grants are process-local, so this salt can be too.
fn browser_type_fingerprint(text: &str) -> String {
    static SALT: OnceLock<[u8; 16]> = OnceLock::new();
    let salt = SALT.get_or_init(|| *uuid::Uuid::new_v4().as_bytes());
    let mut digest = Sha256::new();
    digest.update(salt);
    digest.update(text.as_bytes());
    format!("{:x}", digest.finalize())
}

/// A focused `type` approval may be accepted after a daemon restart. Its
/// one-shot receipt therefore needs the same private resource fingerprint in
/// both processes. Never fall back to an ephemeral or unkeyed digest: that
/// would strand the approved replay or disclose low-entropy typed input.
fn browser_focused_type_fingerprint(text: &str) -> Result<String, PermissionError> {
    browser_persistent_fingerprint("focused-type-v1", text)
}

/// Reusable private fingerprint for browser actions whose approval may replay
/// after a daemon restart. Callers must use a distinct fixed purpose for each
/// resource shape and must not substitute a process-local or unkeyed digest.
pub(crate) fn browser_persistent_fingerprint(
    purpose: &'static str,
    input: &str,
) -> Result<String, PermissionError> {
    let data_dir = bamboo_config::paths::bamboo_dir();
    let key = bamboo_config::encryption::get_encryption_key();
    let env_key = std::env::var("BAMBOO_CONFIG_ENCRYPTION_KEY").ok();
    let key = verified_persistent_browser_key(&key, &data_dir, env_key.as_deref())?;
    fs::create_dir_all(&data_dir).map_err(|_| browser_fingerprint_key_error())?;
    let canonical_dir = fs::canonicalize(&data_dir).map_err(|_| browser_fingerprint_key_error())?;
    Ok(browser_persistent_fingerprint_with_key(
        purpose,
        input,
        key,
        &canonical_dir,
    ))
}

fn browser_fingerprint_key_error() -> PermissionError {
    PermissionError::CheckFailed(
        "browser permission requires a stable private fingerprint key".into(),
    )
}

fn verified_persistent_browser_key<'a>(
    key: &'a [u8],
    data_dir: &Path,
    configured_env_key: Option<&str>,
) -> Result<&'a [u8], PermissionError> {
    if key.len() != 32 {
        return Err(browser_fingerprint_key_error());
    }
    let configured_key = configured_env_key
        .and_then(|value| hex::decode(value).ok())
        .filter(|value| value.len() == 32);
    if let Some(configured_key) = configured_key {
        return (configured_key == key)
            .then_some(key)
            .ok_or_else(browser_fingerprint_key_error);
    }
    let persisted = fs::read_to_string(data_dir.join(".bamboo_encryption_key"))
        .ok()
        .and_then(|value| hex::decode(value.trim()).ok());
    if persisted.as_deref() == Some(key) {
        Ok(key)
    } else {
        Err(browser_fingerprint_key_error())
    }
}

fn browser_persistent_fingerprint_with_key(
    purpose: &str,
    input: &str,
    key: &[u8],
    data_dir: &Path,
) -> String {
    type HmacSha256 = Hmac<Sha256V11>;
    let mut derivation = HmacSha256::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    derivation.update(b"bamboo-browser-permission-key-v1\0");
    derivation.update(&(purpose.len() as u32).to_be_bytes());
    derivation.update(purpose.as_bytes());
    derivation.update(data_dir.as_os_str().as_encoded_bytes());
    let derived_key = derivation.finalize().into_bytes();
    let mut fingerprint =
        HmacSha256::new_from_slice(&derived_key).expect("HMAC accepts derived keys");
    fingerprint.update(input.as_bytes());
    hex::encode(fingerprint.finalize().into_bytes())
}

/// Stable across restarts so a durable approval for the same visible target
/// continues to match. Focused text uses the persistent keyed fingerprint;
/// selector-bound fill still uses the separate process-local salt above.
fn browser_target_fingerprint(target: &str) -> String {
    format!("{:x}", Sha256::digest(target.as_bytes()))
}

fn browser_press_key(args: &Value) -> Result<&str, PermissionError> {
    let key = required_string_arg(args, "key")?;
    if key.trim().is_empty()
        || key.encode_utf16().count() > 128
        || key.chars().any(char::is_control)
    {
        return Err(PermissionError::CheckFailed(
            "browser press key must be nonempty and at most 128 UTF-16 code units without control characters"
                .into(),
        ));
    }
    Ok(key)
}

fn browser_pointer_coordinate(
    args: &Value,
    name: &str,
    maximum: f64,
) -> Result<f64, PermissionError> {
    args.get(name)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0 && *value < maximum)
        .ok_or_else(|| {
            PermissionError::CheckFailed(format!("browser {name} must be within the viewport"))
        })
}

fn browser_pointer_selector<'a>(args: &'a Value, name: &str) -> Result<&'a str, PermissionError> {
    let selector = required_string_arg(args, name)?;
    if selector.trim().is_empty() || selector.encode_utf16().count() > 512 {
        return Err(PermissionError::CheckFailed(format!(
            "browser {name} must be 1..512 UTF-16 code units"
        )));
    }
    Ok(selector)
}

fn browser_pointer_button(args: &Value) -> Result<&str, PermissionError> {
    let button = match args.get("button") {
        None => "left",
        Some(value) => value
            .as_str()
            .ok_or_else(|| PermissionError::CheckFailed("invalid browser pointer button".into()))?,
    };
    if !matches!(button, "left" | "right" | "middle") {
        return Err(PermissionError::CheckFailed(
            "invalid browser pointer button".into(),
        ));
    }
    Ok(button)
}

fn browser_pointer_target(action: &str, args: &Value) -> Result<(String, String), PermissionError> {
    let invalid =
        || PermissionError::CheckFailed("browser pointer target is ambiguous or incomplete".into());
    match action {
        "hover" => {
            if args.get("target").is_some_and(|value| !value.is_null())
                || args.get("button").is_some()
                || args.get("source_selector").is_some()
                || args.get("target_selector").is_some()
                || args.get("to_x").is_some()
                || args.get("to_y").is_some()
            {
                return Err(invalid());
            }
            if args.get("selector").is_some_and(|value| !value.is_null()) {
                if args.get("x").is_some() || args.get("y").is_some() {
                    return Err(invalid());
                }
                let selector = browser_pointer_selector(args, "selector")?;
                Ok((
                    format!("css:{}", browser_target_fingerprint(selector)),
                    format!("Hover browser selector {selector:?}"),
                ))
            } else {
                let x = browser_pointer_coordinate(args, "x", 1200.0)?;
                let y = browser_pointer_coordinate(args, "y", 1000.0)?;
                Ok((
                    format!("point:{x},{y}"),
                    format!("Hover browser at {x},{y}"),
                ))
            }
        }
        "drag" => {
            if args.get("target").is_some_and(|value| !value.is_null())
                || args.get("selector").is_some()
            {
                return Err(invalid());
            }
            let button = browser_pointer_button(args)?;
            if args.get("source_selector").is_some() || args.get("target_selector").is_some() {
                if args.get("x").is_some()
                    || args.get("y").is_some()
                    || args.get("to_x").is_some()
                    || args.get("to_y").is_some()
                    || button != "left"
                {
                    return Err(invalid());
                }
                let source = browser_pointer_selector(args, "source_selector")?;
                let target = browser_pointer_selector(args, "target_selector")?;
                let identity = serde_json::json!([source, target]).to_string();
                Ok((
                    format!("css:{}", browser_target_fingerprint(&identity)),
                    format!("Drag browser selector {source:?} to {target:?}"),
                ))
            } else {
                let x = browser_pointer_coordinate(args, "x", 1200.0)?;
                let y = browser_pointer_coordinate(args, "y", 1000.0)?;
                let to_x = browser_pointer_coordinate(args, "to_x", 1200.0)?;
                let to_y = browser_pointer_coordinate(args, "to_y", 1000.0)?;
                Ok((
                    format!("point:{x},{y}:{to_x},{to_y}:{button}"),
                    format!("Drag browser from {x},{y} to {to_x},{to_y} with {button} button"),
                ))
            }
        }
        _ => Err(invalid()),
    }
}

/// Focus is mutable without a page navigation or epoch change. A remembered
/// resource grant cannot safely authorize another focused keyboard invocation.
pub fn is_focused_browser_input(tool_name: &str, args: &Value) -> bool {
    if !tool_name.eq_ignore_ascii_case("browser") {
        return false;
    }
    match args.get("action").and_then(Value::as_str) {
        Some("type" | "key") => true,
        // A dialog response targets a transient page prompt and may contain
        // private text. It uses the same one-shot approval/display boundary.
        Some("dialog_respond") => true,
        Some("press") => {
            matches!(args.get("target"), None | Some(Value::Null))
                && matches!(args.get("selector"), None | Some(Value::Null))
        }
        _ => false,
    }
}

/// The selected option values can encode private page data. Approval displays
/// use this classifier to project only the action while execution keeps the
/// exact values.
pub fn is_native_browser_select(tool_name: &str, args: &Value) -> bool {
    tool_name
        .trim()
        .rsplit("::")
        .next()
        .is_some_and(|name| name.trim().eq_ignore_ascii_case("browser"))
        && args.get("action").and_then(Value::as_str) == Some("select_option")
}

/// A semantic locator's grant identity is independent of JSON key order and
/// never contains page text. The description remains readable at approval.
fn browser_semantic_target(args: &Value) -> Result<Option<(String, String)>, PermissionError> {
    let selector = args.get("selector").filter(|value| !value.is_null());
    let Some(target) = args.get("target").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let invalid = || PermissionError::CheckFailed("invalid browser semantic target".into());
    if selector.is_some() {
        return Err(PermissionError::CheckFailed(
            "browser selector and target are mutually exclusive".into(),
        ));
    }
    let object = target.as_object().ok_or_else(invalid)?;
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let string = |name: &str, maximum: usize| -> Result<Option<&str>, PermissionError> {
        match object.get(name) {
            None => Ok(None),
            Some(value) => value
                .as_str()
                .filter(|value| !value.trim().is_empty() && value.encode_utf16().count() <= maximum)
                .map(Some)
                .ok_or_else(invalid),
        }
    };
    let frame = string("frame_selector", 512)?;
    let exact = match object.get("exact") {
        None => true,
        Some(value) => value.as_bool().ok_or_else(invalid)?,
    };
    let (role, name, value, description, allowed): (
        Option<&str>,
        Option<&str>,
        Option<&str>,
        String,
        &[&str],
    ) = match kind {
        "role" => {
            let role = string("role", 64)?.ok_or_else(invalid)?;
            if !role
                .chars()
                .all(|character| character.is_ascii_lowercase() || character == '-')
            {
                return Err(invalid());
            }
            let name = string("name", 256)?;
            let description = match name {
                Some(name) => format!("role {role} named {name:?}"),
                None => format!("role {role}"),
            };
            (
                Some(role),
                name,
                None,
                description,
                &["kind", "role", "name", "exact", "frame_selector"],
            )
        }
        "label" | "text" => {
            let value = string("value", 256)?.ok_or_else(invalid)?;
            (
                None,
                None,
                Some(value),
                format!("{kind} {value:?}"),
                &["kind", "value", "exact", "frame_selector"],
            )
        }
        _ => return Err(invalid()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid());
    }
    let canonical = serde_json::json!([
        kind,
        role.unwrap_or(""),
        name.unwrap_or(""),
        value.unwrap_or(""),
        exact,
        frame.unwrap_or("")
    ]);
    let identity = browser_target_fingerprint(&canonical.to_string());
    let description = match frame {
        Some(frame) => format!("{description} in iframe {frame:?}"),
        None => description,
    };
    Ok(Some((format!("semantic:{identity}"), description)))
}

pub fn check_permissions(
    tool_name: &str,
    args: &Value,
) -> Result<Option<Vec<PermissionContext>>, PermissionError> {
    match tool_name {
        "request_permissions" => {
            let reason = required_string_arg(args, "reason")?.trim();
            if reason.is_empty() {
                return Err(PermissionError::CheckFailed(
                    "Missing or invalid 'reason' parameter".to_string(),
                ));
            }
            let permissions = args
                .get("permissions")
                .and_then(Value::as_array)
                .filter(|permissions| !permissions.is_empty())
                .ok_or_else(|| {
                    PermissionError::CheckFailed(
                        "Missing or invalid 'permissions' array".to_string(),
                    )
                })?;
            if permissions.len() > MAX_PROACTIVE_PERMISSION_BATCH {
                return Err(PermissionError::CheckFailed(format!(
                    "'permissions' array cannot contain more than {MAX_PROACTIVE_PERMISSION_BATCH} items"
                )));
            }
            let mut contexts = Vec::with_capacity(permissions.len());
            for (index, permission) in permissions.iter().enumerate() {
                let permission_type = permission
                    .get("type")
                    .and_then(Value::as_str)
                    .and_then(parse_requested_permission_type)
                    .ok_or_else(|| {
                        PermissionError::CheckFailed(format!(
                            "permissions[{index}] has an invalid permission type"
                        ))
                    })?;
                let resource = permission
                    .get("resource")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|resource| !resource.is_empty())
                    .ok_or_else(|| {
                        PermissionError::CheckFailed(format!(
                            "permissions[{index}] is missing a non-empty resource"
                        ))
                    })?;
                let description = permission
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|description| !description.is_empty())
                    .unwrap_or_else(|| permission_type.description());
                contexts.push(PermissionContext::new(
                    permission_type,
                    resource,
                    format!("Proactive request: {reason} — {description}"),
                ));
            }
            Ok(Some(contexts))
        }
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
        "browser_eval" => {
            if args.get("session_id").is_some() {
                return Err(PermissionError::CheckFailed(
                    "browser_eval session is bound to the current chat".into(),
                ));
            }
            let code = required_string_arg(args, "code")?;
            if code.trim().is_empty() || code.len() > 8 * 1024 {
                return Err(PermissionError::CheckFailed(
                    "browser_eval code must be nonempty and at most 8 KiB".into(),
                ));
            }
            let epoch = args
                .get("expected_epoch")
                .and_then(Value::as_u64)
                .filter(|epoch| *epoch < (1_u64 << 53))
                .ok_or_else(|| {
                    PermissionError::CheckFailed(
                        "browser_eval requires a safe expected_epoch".into(),
                    )
                })?;
            let expected_url = required_string_arg(args, "expected_url")?;
            if expected_url.len() > 8 * 1024 {
                return Err(PermissionError::CheckFailed(
                    "browser_eval expected_url exceeds 8 KiB".into(),
                ));
            }
            let display_origin = if expected_url == "about:blank" {
                "about:blank".to_string()
            } else {
                let parsed = url::Url::parse(expected_url).map_err(|error| {
                    PermissionError::CheckFailed(format!("invalid browser_eval URL: {error}"))
                })?;
                if !matches!(parsed.scheme(), "http" | "https")
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                {
                    return Err(PermissionError::CheckFailed(
                        "browser_eval requires an http(s) URL without credentials or about:blank"
                            .into(),
                    ));
                }
                parsed.origin().ascii_serialization()
            };
            // JSON tuple encoding keeps the URL and source unambiguous even if
            // either contains delimiter bytes. The persistent keyed digest
            // survives a parked one-shot approval across daemon restarts.
            let fingerprint_input = serde_json::to_string(&(expected_url, code))
                .expect("two strings always serialize as JSON");
            let fingerprint =
                browser_persistent_fingerprint("browser-eval-v1", &fingerprint_input)?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::BrowserInteraction,
                format!("browser_eval:{epoch}:{fingerprint}"),
                format!("Execute browser page JavaScript on {display_origin}; can send page data and requests to other websites"),
            )]))
        }
        "browser" => {
            let action = required_string_arg(args, "action")?;
            match action {
                "navigate" => {
                    let raw = required_string_arg(args, "url")?;
                    let url = url::Url::parse(raw).map_err(|error| {
                        PermissionError::CheckFailed(format!("invalid browser URL: {error}"))
                    })?;
                    if !matches!(url.scheme(), "http" | "https")
                        || !url.username().is_empty()
                        || url.password().is_some()
                    {
                        return Err(PermissionError::CheckFailed(
                            "browser navigation requires an http(s) URL without credentials".into(),
                        ));
                    }
                    Ok(Some(vec![PermissionContext::new(
                        PermissionType::HttpRequest,
                        url.as_str(),
                        format!("Navigate browser to {}", url.origin().ascii_serialization()),
                    )]))
                }
                "hover" | "drag" => {
                    let epoch = args
                        .get("expected_epoch")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            PermissionError::CheckFailed(
                                "browser interaction requires expected_epoch from a snapshot"
                                    .into(),
                            )
                        })?;
                    let (target, description) = browser_pointer_target(action, args)?;
                    Ok(Some(vec![PermissionContext::new(
                        PermissionType::BrowserInteraction,
                        format!("browser:{epoch}:{action}:{target}"),
                        description,
                    )]))
                }
                "dialog_respond" => {
                    let epoch = args
                        .get("expected_epoch")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            PermissionError::CheckFailed(
                                "browser dialog response requires expected_epoch".into(),
                            )
                        })?;
                    let dialog_id = required_string_arg(args, "dialog_id")?;
                    if dialog_id.len() != 24
                        || !dialog_id
                            .bytes()
                            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                    {
                        return Err(PermissionError::CheckFailed(
                            "browser dialog_id must be a 24-character lowercase hex ID".into(),
                        ));
                    }
                    let accept = args.get("accept").and_then(Value::as_bool).ok_or_else(|| {
                        PermissionError::CheckFailed(
                            "browser dialog response requires accept".into(),
                        )
                    })?;
                    let text = match args.get("text") {
                        None | Some(Value::Null) => None,
                        Some(value) => Some(
                            value
                                .as_str()
                                .filter(|text| text.encode_utf16().count() <= 4096)
                                .ok_or_else(|| {
                                    PermissionError::CheckFailed(
                                        "invalid browser dialog text".into(),
                                    )
                                })?,
                        ),
                    };
                    if !accept && text.is_some() {
                        return Err(PermissionError::CheckFailed(
                            "dismissed browser dialog cannot include text".into(),
                        ));
                    }
                    let choice = if accept { "accept" } else { "dismiss" };
                    let text_identity = match text {
                        Some(text) => browser_persistent_fingerprint("dialog-response-v1", text)?,
                        None => "none".to_string(),
                    };
                    Ok(Some(vec![PermissionContext::new(
                        PermissionType::BrowserInteraction,
                        format!(
                            "browser:{epoch}:dialog_respond:{dialog_id}:{choice}:{text_identity}"
                        ),
                        "Answer pending browser dialog",
                    )]))
                }
                "download" => {
                    let epoch = args
                        .get("expected_epoch")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            PermissionError::CheckFailed(
                                "browser interaction requires expected_epoch from a snapshot"
                                    .into(),
                            )
                        })?;
                    let fields = args.as_object().ok_or_else(|| {
                        PermissionError::CheckFailed(
                            "browser download accepts only a CSS selector and expected_epoch"
                                .into(),
                        )
                    })?;
                    if fields.keys().any(|key| {
                        !matches!(key.as_str(), "action" | "selector" | "expected_epoch")
                    }) {
                        return Err(PermissionError::CheckFailed(
                            "browser download accepts only a CSS selector and expected_epoch"
                                .into(),
                        ));
                    }
                    let selector = browser_pointer_selector(args, "selector")?;
                    let fingerprint =
                        browser_persistent_fingerprint("download-selector-v1", selector)?;
                    Ok(Some(vec![PermissionContext::new(
                        PermissionType::BrowserInteraction,
                        format!("browser:{epoch}:download:css:{fingerprint}"),
                        "Download from selected browser element",
                    )]))
                }
                "click" | "click_at" | "fill" | "select_option" | "type" | "press" | "key"
                | "scroll" | "history" | "viewport" | "new_tab" | "activate_tab" | "close_tab" => {
                    let focused_input = is_focused_browser_input(tool_name, args);
                    // Bind remembered grants to the page generation. Navigation
                    // increments the epoch, so a selector approved on one site
                    // cannot silently carry authority to the next site.
                    let epoch = args
                        .get("expected_epoch")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            PermissionError::CheckFailed(
                                "browser interaction requires expected_epoch from a snapshot"
                                    .into(),
                            )
                        })?;
                    let semantic = if matches!(action, "click" | "fill" | "press") {
                        browser_semantic_target(args)?
                    } else {
                        None
                    };
                    let target = match action {
                        "new_tab" => "new".to_string(),
                        "activate_tab" | "close_tab" => browser_tab_id_arg(args)?.to_string(),
                        "click" | "fill" => match &semantic {
                            Some((identity, _)) => identity.clone(),
                            None => required_string_arg(args, "selector")?.to_string(),
                        },
                        "select_option" => {
                            if args.get("target").is_some_and(|value| !value.is_null()) {
                                return Err(PermissionError::CheckFailed(
                                    "browser select_option accepts only a CSS selector".into(),
                                ));
                            }
                            let selector = required_string_arg(args, "selector")?;
                            if selector.trim().is_empty() || selector.encode_utf16().count() > 512 {
                                return Err(PermissionError::CheckFailed(
                                    "browser select requires a bounded CSS selector".into(),
                                ));
                            }
                            let values = args
                                .get("values")
                                .and_then(Value::as_array)
                                .filter(|values| (1..=16).contains(&values.len()))
                                .ok_or_else(|| {
                                    PermissionError::CheckFailed(
                                        "browser select requires 1..16 option values".into(),
                                    )
                                })?;
                            if values
                                .iter()
                                .any(|value| value.as_str().is_none_or(|value| value.len() > 512))
                            {
                                return Err(PermissionError::CheckFailed(
                                    "browser select option values must be bounded strings".into(),
                                ));
                            }
                            let payload = serde_json::json!([selector, values]).to_string();
                            let fingerprint =
                                browser_persistent_fingerprint("select-option-v1", &payload)?;
                            format!("options:{fingerprint}")
                        }
                        "press" => match &semantic {
                            Some((identity, _)) => identity.clone(),
                            None => match args.get("selector") {
                                None | Some(Value::Null) => "focused".to_string(),
                                Some(value) => value
                                    .as_str()
                                    .filter(|selector| !selector.trim().is_empty())
                                    .ok_or_else(|| {
                                        PermissionError::CheckFailed(
                                            "browser press selector must be nonempty".into(),
                                        )
                                    })?
                                    .to_string(),
                            },
                        },
                        "scroll" => args
                            .get("selector")
                            .and_then(Value::as_str)
                            .unwrap_or("page")
                            .to_string(),
                        "history" => {
                            let direction = required_string_arg(args, "direction")?;
                            if !matches!(direction, "back" | "forward" | "reload") {
                                return Err(PermissionError::CheckFailed(
                                    "invalid browser history direction".into(),
                                ));
                            }
                            direction.to_string()
                        }
                        "viewport" => {
                            let width = args.get("width").and_then(Value::as_u64);
                            let height = args.get("height").and_then(Value::as_u64);
                            match (width, height) {
                                (Some(width @ 320..=1200), Some(height @ 240..=1000)) => {
                                    format!("{width}x{height}")
                                }
                                _ => {
                                    return Err(PermissionError::CheckFailed(
                                        "browser viewport must be within 320..1200 by 240..1000"
                                            .into(),
                                    ));
                                }
                            }
                        }
                        "click_at" => {
                            let coordinate = |name| {
                                args.get(name)
                                    .and_then(Value::as_f64)
                                    .filter(|value| value.is_finite() && *value >= 0.0)
                                    .ok_or_else(|| {
                                        PermissionError::CheckFailed(format!(
                                            "browser requires nonnegative {name}"
                                        ))
                                    })
                            };
                            let x = coordinate("x")?;
                            let y = coordinate("y")?;
                            let button = match args.get("button") {
                                None => "left",
                                Some(value) => value.as_str().ok_or_else(|| {
                                    PermissionError::CheckFailed("invalid browser button".into())
                                })?,
                            };
                            if !matches!(button, "left" | "right" | "middle") {
                                return Err(PermissionError::CheckFailed(
                                    "invalid browser button".into(),
                                ));
                            }
                            format!("{x},{y},{button}")
                        }
                        "type" => {
                            let text = required_string_arg(args, "text")?;
                            format!("focused:{}", browser_focused_type_fingerprint(text)?)
                        }
                        "key" => {
                            let key = browser_press_key(args)?;
                            browser_persistent_fingerprint("focused-key-v1", key)?
                        }
                        _ => unreachable!(),
                    };
                    let description = if action == "select_option" {
                        "Select native browser options".to_string()
                    } else if action == "type" {
                        "Type into focused browser element".to_string()
                    } else if focused_input && action == "key" {
                        "Send key to focused browser element".to_string()
                    } else if focused_input && action == "press" {
                        "Press key on focused browser element".to_string()
                    } else if let Some((_, semantic_description)) = &semantic {
                        format!("Browser {action} on {semantic_description}")
                    } else {
                        format!("Browser {action} on {target}")
                    };
                    let target = if action == "fill" && semantic.is_some() {
                        let text = required_string_arg(args, "text")?;
                        format!("{target}:text:{}", browser_type_fingerprint(text))
                    } else if action == "press" {
                        let key = browser_press_key(args)?;
                        if focused_input {
                            format!(
                                "{target}:key:{}",
                                browser_persistent_fingerprint("focused-press-v1", key)?
                            )
                        } else if semantic.is_none() {
                            // CSS selectors are arbitrary text. Prefix their
                            // resource namespace so a selector literally
                            // named `focused` cannot impersonate the private
                            // focused-input marker after restart.
                            format!("css:{target}:key:{}", browser_target_fingerprint(key))
                        } else {
                            format!("{target}:key:{}", browser_target_fingerprint(key))
                        }
                    } else {
                        target
                    };
                    Ok(Some(vec![PermissionContext::new(
                        PermissionType::BrowserInteraction,
                        format!("browser:{epoch}:{action}:{target}"),
                        description,
                    )]))
                }
                "tabs" | "snapshot" | "screenshot" => Ok(None),
                _ => Err(PermissionError::CheckFailed(
                    "unknown browser action".into(),
                )),
            }
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
        "session_control" => {
            let action = required_string_arg(args, "action")?;
            if action != "followup" {
                return Err(PermissionError::CheckFailed(
                    "unsupported session_control action".into(),
                ));
            }
            let target = required_string_arg(args, "target_session_id")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::ExecuteCommand,
                format!("session_control followup {target}"),
                "session_control followup: continue an existing independent Root",
            )]))
        }
        // Read-only: session_inspector (list / get_meta / read_messages), the
        // legacy session_history viewer, and exact self-only current history
        // never mutate or spin up compute. #395.
        "session_inspector" | "session_history" | "session_history_current" => Ok(None),
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

fn parse_requested_permission_type(value: &str) -> Option<PermissionType> {
    match value {
        "write_file" | "WriteFile" => Some(PermissionType::WriteFile),
        "execute_command" | "ExecuteCommand" => Some(PermissionType::ExecuteCommand),
        "git_write" | "GitWrite" => Some(PermissionType::GitWrite),
        "http_request" | "HttpRequest" => Some(PermissionType::HttpRequest),
        "delete_operation" | "DeleteOperation" => Some(PermissionType::DeleteOperation),
        "terminal_session" | "TerminalSession" => Some(PermissionType::TerminalSession),
        "browser_interaction" | "BrowserInteraction" => Some(PermissionType::BrowserInteraction),
        _ => None,
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

fn browser_tab_id_arg(args: &Value) -> Result<&str, PermissionError> {
    let tab_id = required_string_arg(args, "tab_id")?;
    if tab_id.len() != 24
        || !tab_id
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(PermissionError::CheckFailed(
            "browser tab_id must be a 24-character lowercase hex ID".into(),
        ));
    }
    Ok(tab_id)
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
    use std::process::Command;

    use super::*;

    #[test]
    fn browser_eval_fingerprint_child_process() {
        let Some(output_path) = std::env::var_os("BAMBOO_EVAL_TEST_OUTPUT") else {
            return;
        };
        let mut args = json!({
            "code":"document.querySelector('#password').value = 'secret-value'",
            "expected_epoch":17,
            "expected_url":"https://example.com/account?token=private-query"
        });
        match std::env::var("BAMBOO_EVAL_TEST_CASE").as_deref() {
            Ok("base") => {}
            Ok("code") => args["code"] = json!("document.title"),
            Ok("url") => args["expected_url"] = json!("https://example.com/other"),
            Ok("epoch") => args["expected_epoch"] = json!(18),
            Ok("blank") => {
                args["code"] = json!("1");
                args["expected_url"] = json!("about:blank");
            }
            other => panic!("unexpected eval fingerprint fixture: {other:?}"),
        }
        let context = check_permissions("browser_eval", &args)
            .unwrap()
            .unwrap()
            .remove(0);
        assert_eq!(context.permission_type, PermissionType::BrowserInteraction);
        fs::write(
            output_path,
            json!({
                "resource":context.resource,
                "description":context.operation_description,
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn browser_eval_grants_bind_exact_code_url_and_epoch_after_restart() {
        let data_dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let run = |case: &str, data_dir: &Path, output_path: &Path| {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tool_permissions::tests::browser_eval_fingerprint_child_process",
                ])
                .env("BAMBOO_DATA_DIR", data_dir)
                .env("BAMBOO_EVAL_TEST_CASE", case)
                .env("BAMBOO_EVAL_TEST_OUTPUT", output_path)
                .env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY")
                .output()
                .unwrap();
            assert!(output.status.success(), "eval fingerprint child failed");
            serde_json::from_slice::<Value>(&fs::read(output_path).unwrap()).unwrap()
        };
        let base = run("base", data_dir.path(), &data_dir.path().join("base.json"));
        let restarted = run(
            "base",
            data_dir.path(),
            &data_dir.path().join("restarted.json"),
        );
        let resource = base["resource"].as_str().unwrap();
        assert!(resource.starts_with("browser_eval:17:"));
        assert_eq!(base, restarted);
        assert_eq!(
            base["description"],
            "Execute browser page JavaScript on https://example.com; can send page data and requests to other websites"
        );
        for secret in ["#password", "secret-value", "private-query"] {
            assert!(!base.to_string().contains(secret));
        }

        let config = crate::PermissionConfig::default();
        config
            .grant_once_for_generation(
                "chat",
                "call",
                "generation",
                PermissionType::BrowserInteraction,
                resource.to_string(),
            )
            .unwrap();
        assert!(config.consume_once_for_generation(
            "chat",
            "call",
            "generation",
            PermissionType::BrowserInteraction,
            restarted["resource"].as_str().unwrap(),
        ));
        assert!(!config.consume_once_for_generation(
            "chat",
            "call",
            "generation",
            PermissionType::BrowserInteraction,
            resource,
        ));
        for case in ["code", "url", "epoch", "blank"] {
            let changed = run(
                case,
                data_dir.path(),
                &data_dir.path().join(format!("{case}.json")),
            );
            assert_ne!(resource, changed["resource"], "{case}");
            config
                .grant_once_for_generation(
                    "chat",
                    case,
                    "generation",
                    PermissionType::BrowserInteraction,
                    resource.to_string(),
                )
                .unwrap();
            assert!(!config.consume_once_for_generation(
                "chat",
                case,
                "generation",
                PermissionType::BrowserInteraction,
                changed["resource"].as_str().unwrap(),
            ));
        }
        let other_install = run(
            "base",
            other_dir.path(),
            &other_dir.path().join("base.json"),
        );
        assert_ne!(resource, other_install["resource"]);
        assert_ne!(
            serde_json::to_string(&("a\0b", "c")).unwrap(),
            serde_json::to_string(&("a", "b\0c")).unwrap(),
            "URL and code must have an unambiguous fingerprint input"
        );
    }

    #[test]
    fn browser_eval_rejects_unbounded_or_unsafe_requests_before_approval() {
        for args in [
            json!({"code":" ","expected_epoch":17,"expected_url":"about:blank"}),
            json!({"code":"x".repeat(8193),"expected_epoch":17,"expected_url":"about:blank"}),
            json!({"code":"1","expected_url":"about:blank"}),
            json!({"code":"1","expected_epoch":17,"expected_url":"file:///secret"}),
            json!({"code":"1","expected_epoch":17,"expected_url":"https://user:password@example.com/"}),
            json!({"code":"1","expected_epoch":17,"expected_url":"about:blank","session_id":"other"}),
        ] {
            assert!(check_permissions("browser_eval", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn browser_navigation_and_interaction_use_distinct_scoped_permissions() {
        let navigation = check_permissions(
            "browser",
            &json!({"action":"navigate","url":"http://127.0.0.1:53495/"}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(navigation[0].permission_type, PermissionType::HttpRequest);
        assert_eq!(navigation[0].resource, "http://127.0.0.1:53495/");

        let click = check_permissions(
            "browser",
            &json!({"action":"click","selector":"#increment","expected_epoch":17}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(click[0].permission_type, PermissionType::BrowserInteraction);
        assert_eq!(click[0].resource, "browser:17:click:#increment");
        let later = check_permissions(
            "browser",
            &json!({"action":"click","selector":"#increment","expected_epoch":18}),
        )
        .unwrap()
        .unwrap();
        assert_ne!(click[0].resource, later[0].resource);
        assert!(check_permissions(
            "browser",
            &json!({"action":"click","selector":"#increment"})
        )
        .is_err());
        assert!(check_permissions("browser", &json!({"action":"snapshot"}))
            .unwrap()
            .is_none());
    }

    #[test]
    fn browser_host_controls_use_action_specific_epoch_scoped_interaction_permissions() {
        let cases = [
            (
                json!({"action":"history","direction":"back","expected_epoch":17}),
                "browser:17:history:back",
            ),
            (
                json!({"action":"viewport","width":640,"height":480,"expected_epoch":17}),
                "browser:17:viewport:640x480",
            ),
            (
                json!({"action":"click_at","x":12.5,"y":20,"button":"right","expected_epoch":17}),
                "browser:17:click_at:12.5,20,right",
            ),
        ];
        for (args, expected_resource) in cases {
            let context = check_permissions("browser", &args).unwrap().unwrap();
            assert_eq!(context.len(), 1);
            assert_eq!(
                context[0].permission_type,
                PermissionType::BrowserInteraction
            );
            assert_eq!(context[0].resource, expected_resource);
            let mut later = args;
            later["expected_epoch"] = json!(18);
            assert_ne!(
                check_permissions("browser", &later).unwrap().unwrap()[0].resource,
                expected_resource
            );
        }
    }

    #[test]
    fn browser_pointer_actions_bind_permission_to_epoch_and_exact_target() {
        let cases = [
            json!({"action":"hover","selector":"#tip","expected_epoch":17}),
            json!({"action":"hover","x":12.5,"y":20,"expected_epoch":17}),
            json!({"action":"drag","source_selector":"#source","target_selector":"#drop","expected_epoch":17}),
            json!({"action":"drag","x":10,"y":20,"to_x":30,"to_y":40,"button":"right","expected_epoch":17}),
        ];
        for args in cases {
            let context = check_permissions("browser", &args).unwrap().unwrap();
            assert_eq!(context.len(), 1);
            assert_eq!(
                context[0].permission_type,
                PermissionType::BrowserInteraction
            );
            let original = &context[0].resource;
            assert!(original.starts_with("browser:17:"), "{original}");
            let mut later = args.clone();
            later["expected_epoch"] = json!(18);
            assert_ne!(
                original,
                &check_permissions("browser", &later).unwrap().unwrap()[0].resource
            );
            let mut different = args.clone();
            match args["action"].as_str().unwrap() {
                "hover" if args.get("selector").is_some() => {
                    different["selector"] = json!("#other")
                }
                "hover" => different["x"] = json!(13.5),
                "drag" if args.get("source_selector").is_some() => {
                    different["target_selector"] = json!("#other")
                }
                "drag" => different["to_x"] = json!(31),
                _ => unreachable!(),
            }
            assert_ne!(
                original,
                &check_permissions("browser", &different).unwrap().unwrap()[0].resource
            );
        }
    }

    #[test]
    fn browser_pointer_permission_rejects_ambiguous_and_out_of_bounds_targets() {
        for (action, args) in [
            ("hover", json!({"selector":"#tip","x":10,"y":20})),
            ("hover", json!({"selector":" "})),
            ("hover", json!({"x":1200,"y":20})),
            ("hover", json!({"x":10,"y":1000})),
            ("drag", json!({"source_selector":"#source"})),
            (
                "drag",
                json!({"source_selector":"#source","target_selector":"#drop","x":10}),
            ),
            (
                "drag",
                json!({"source_selector":"#source","target_selector":"#drop","button":"right"}),
            ),
            ("drag", json!({"x":10,"y":20,"to_x":1200,"to_y":40})),
            (
                "drag",
                json!({"x":10,"y":20,"to_x":30,"to_y":40,"button":"invalid"}),
            ),
        ] {
            let mut request = args.clone();
            request["action"] = json!(action);
            request["expected_epoch"] = json!(17);
            assert!(
                check_permissions("browser", &request).is_err(),
                "{action}: {args}"
            );
        }
        assert!(
            check_permissions("browser", &json!({"action":"hover","selector":"#tip"})).is_err()
        );
    }

    #[test]
    fn focused_key_and_press_use_stable_private_exact_resources() {
        let context = |action: &str, key: &str, epoch: u64| {
            check_permissions(
                "browser",
                &json!({"action":action,"key":key,"expected_epoch":epoch}),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        for (action, prefix) in [
            ("key", "browser:17:key:"),
            ("press", "browser:17:press:focused:key:"),
        ] {
            let first = context(action, "private-key", 17);
            assert!(first.resource.starts_with(prefix));
            assert!(!first.resource.contains("private-key"));
            assert!(!first.operation_description.contains("private-key"));
            assert_eq!(first.resource, context(action, "private-key", 17).resource);
            assert_ne!(first.resource, context(action, "other-key", 17).resource);
            assert_ne!(first.resource, context(action, "private-key", 18).resource);
            assert!(crate::PermissionRequest::is_focused_browser_resource(
                "browser",
                &first.resource
            ));
        }
        assert!(check_permissions(
            "browser",
            &json!({"action":"key","key":"x".repeat(129),"expected_epoch":17})
        )
        .is_err());
    }

    #[test]
    fn browser_tab_mutations_bind_grants_to_epoch_and_opaque_tab_id() {
        assert!(check_permissions("browser", &json!({"action":"tabs"}))
            .unwrap()
            .is_none());
        for (args, resource) in [
            (
                json!({"action":"new_tab","expected_epoch":17}),
                "browser:17:new_tab:new",
            ),
            (
                json!({"action":"activate_tab","tab_id":"aaaaaaaaaaaaaaaaaaaaaaaa","expected_epoch":17}),
                "browser:17:activate_tab:aaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                json!({"action":"close_tab","tab_id":"aaaaaaaaaaaaaaaaaaaaaaaa","expected_epoch":17}),
                "browser:17:close_tab:aaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        ] {
            let context = check_permissions("browser", &args).unwrap().unwrap();
            assert_eq!(
                context[0].permission_type,
                PermissionType::BrowserInteraction
            );
            assert_eq!(context[0].resource, resource);
            let mut stale = args;
            stale["expected_epoch"] = json!(18);
            assert_ne!(
                check_permissions("browser", &stale).unwrap().unwrap()[0].resource,
                resource
            );
        }
        for args in [
            json!({"action":"new_tab"}),
            json!({"action":"activate_tab","expected_epoch":17}),
            json!({"action":"close_tab","expected_epoch":17}),
            json!({"action":"activate_tab","tab_id":"short","expected_epoch":17}),
            json!({"action":"close_tab","tab_id":"AAAAAAAAAAAAAAAAAAAAAAAA","expected_epoch":17}),
            json!({"action":"activate_tab","tab_id":"a".repeat(10000),"expected_epoch":17}),
        ] {
            assert!(check_permissions("browser", &args).is_err());
        }
    }

    #[test]
    fn browser_select_fingerprint_child_process() {
        let Some(output_path) = std::env::var_os("BAMBOO_SELECT_TEST_OUTPUT") else {
            return;
        };
        let cases = [
            json!({"action":"select_option","selector":"#choice","values":["private-red"],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":["private-blue"],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#other","values":["private-red"],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":["private-red"],"expected_epoch":18}),
        ];
        let resources: Vec<String> = cases
            .iter()
            .map(|args| {
                let mut contexts = check_permissions("browser", args).unwrap().unwrap();
                let context = contexts.remove(0);
                assert_eq!(context.permission_type, PermissionType::BrowserInteraction);
                assert_eq!(
                    context.operation_description,
                    "Select native browser options"
                );
                context.resource
            })
            .collect();
        fs::write(output_path, serde_json::to_vec(&resources).unwrap()).unwrap();
    }

    #[test]
    fn browser_select_grants_bind_values_selector_and_epoch_without_plaintext() {
        let select = json!({"action":"select_option","values":["private-red"]});
        assert!(is_native_browser_select("browser", &select));
        assert!(is_native_browser_select("default::browser", &select));
        assert!(!is_native_browser_select("default::other", &select));
        let data_dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let run = |dir: &Path, output_path: &Path| {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tool_permissions::tests::browser_select_fingerprint_child_process",
                ])
                .env("BAMBOO_DATA_DIR", dir)
                .env("BAMBOO_SELECT_TEST_OUTPUT", output_path)
                .env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "select fingerprint child process failed"
            );
            let bytes = fs::read(output_path).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("private-red"));
            assert!(!String::from_utf8_lossy(&bytes).contains("private-blue"));
            serde_json::from_slice::<Vec<String>>(&bytes).unwrap()
        };
        let first = run(data_dir.path(), &data_dir.path().join("first.json"));
        let restarted = run(data_dir.path(), &data_dir.path().join("restarted.json"));
        let other = run(other_dir.path(), &other_dir.path().join("other.json"));
        assert_eq!(first, restarted, "same installation needs stable grants");
        assert_ne!(first, other, "another installation needs separate grants");
        assert_eq!(first.len(), 4);
        assert!(first[0].starts_with("browser:17:select_option:options:"));
        assert_ne!(first[0], first[1], "another value needs another grant");
        assert_ne!(first[0], first[2], "another selector needs another grant");
        assert_ne!(first[0], first[3], "another page epoch needs another grant");

        for args in [
            json!({"action":"select_option","selector":"#choice","values":["private-red"]}),
            json!({"action":"select_option","selector":" ","values":["private-red"],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","target":{"kind":"role","role":"combobox"},"values":["private-red"],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":[],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":[7],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":["x".repeat(513)],"expected_epoch":17}),
            json!({"action":"select_option","selector":"#choice","values":vec!["red"; 17],"expected_epoch":17}),
        ] {
            assert!(check_permissions("browser", &args).is_err());
        }
    }

    #[test]
    fn browser_download_fingerprint_child_process() {
        let Some(output_path) = std::env::var_os("BAMBOO_DOWNLOAD_TEST_OUTPUT") else {
            return;
        };
        let cases = [
            json!({"action":"download","selector":"a[data-secret='private-a']","expected_epoch":17}),
            json!({"action":"download","selector":"a[data-secret='private-b']","expected_epoch":17}),
            json!({"action":"download","selector":"a[data-secret='private-a']","expected_epoch":18}),
        ];
        let resources: Vec<String> = cases
            .iter()
            .map(|args| {
                let mut contexts = check_permissions("browser", args).unwrap().unwrap();
                let context = contexts.remove(0);
                assert_eq!(context.permission_type, PermissionType::BrowserInteraction);
                assert_eq!(
                    context.operation_description,
                    "Download from selected browser element"
                );
                context.resource
            })
            .collect();
        fs::write(output_path, serde_json::to_vec(&resources).unwrap()).unwrap();
    }

    #[test]
    fn browser_download_grants_bind_selector_and_epoch_without_plaintext() {
        let data_dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let run = |dir: &Path, output_path: &Path| {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tool_permissions::tests::browser_download_fingerprint_child_process",
                ])
                .env("BAMBOO_DATA_DIR", dir)
                .env("BAMBOO_DOWNLOAD_TEST_OUTPUT", output_path)
                .env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "download fingerprint child process failed"
            );
            let bytes = fs::read(output_path).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("private-a"));
            assert!(!String::from_utf8_lossy(&bytes).contains("private-b"));
            serde_json::from_slice::<Vec<String>>(&bytes).unwrap()
        };
        let first = run(data_dir.path(), &data_dir.path().join("first.json"));
        let restarted = run(data_dir.path(), &data_dir.path().join("restarted.json"));
        let other = run(other_dir.path(), &other_dir.path().join("other.json"));
        assert_eq!(first, restarted, "same installation needs stable grants");
        assert_ne!(first, other, "another installation needs separate grants");
        assert_eq!(first.len(), 3);
        assert!(first[0].starts_with("browser:17:download:css:"));
        assert_ne!(first[0], first[1], "another selector needs another grant");
        assert_ne!(first[0], first[2], "another epoch needs another grant");

        for args in [
            json!({"action":"download","selector":"#file"}),
            json!({"action":"download","selector":" ","expected_epoch":17}),
            json!({"action":"download","selector":"x".repeat(513),"expected_epoch":17}),
            json!({"action":"download","selector":"#file","url":"https://example.test/file","expected_epoch":17}),
            json!({"action":"download","selector":"#file","path":"/tmp/file","expected_epoch":17}),
            json!({"action":"download","selector":"#file","target":{"kind":"text","value":"Save"},"expected_epoch":17}),
        ] {
            assert!(check_permissions("browser", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn focused_browser_input_classifier_distinguishes_missing_and_explicit_selectors() {
        for args in [
            json!({"action":"type","text":"secret"}),
            json!({"action":"key","key":"Tab"}),
            json!({"action":"press","key":"Enter"}),
            json!({"action":"press","selector":null,"key":"Enter"}),
        ] {
            assert!(is_focused_browser_input("browser", &args), "{args}");
        }
        for args in [
            json!({"action":"press","selector":"","key":"Enter"}),
            json!({"action":"press","selector":"page","key":"Enter"}),
            json!({"action":"press","target":{"kind":"role","role":"button","name":"Save"},"key":"Enter"}),
            json!({"action":"fill","selector":"#name","text":"secret"}),
        ] {
            assert!(!is_focused_browser_input("browser", &args), "{args}");
        }
        assert!(!is_focused_browser_input(
            "request_permissions",
            &json!({"action":"type","text":"secret"})
        ));
        assert!(check_permissions(
            "browser",
            &json!({"action":"press","selector":"","key":"Enter","expected_epoch":17})
        )
        .is_err());
    }

    #[test]
    fn focused_browser_key_must_be_persistent_and_is_scoped_to_data_dir() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let key = [7u8; 32];
        let other_key = [8u8; 32];
        assert!(verified_persistent_browser_key(&key, first_dir.path(), None).is_err());
        fs::write(
            first_dir.path().join(".bamboo_encryption_key"),
            hex::encode(key),
        )
        .unwrap();
        assert!(verified_persistent_browser_key(&key, first_dir.path(), None).is_ok());
        assert!(verified_persistent_browser_key(&other_key, first_dir.path(), None).is_err());
        assert!(
            verified_persistent_browser_key(&key, second_dir.path(), Some(&hex::encode(key)))
                .is_ok()
        );

        let original = browser_persistent_fingerprint_with_key(
            "focused-type-v1",
            "fixture input",
            &key,
            first_dir.path(),
        );
        assert_eq!(
            original,
            browser_persistent_fingerprint_with_key(
                "focused-type-v1",
                "fixture input",
                &key,
                first_dir.path()
            )
        );
        assert_ne!(
            original,
            browser_persistent_fingerprint_with_key(
                "focused-type-v1",
                "other input",
                &key,
                first_dir.path()
            )
        );
        assert_ne!(
            original,
            browser_persistent_fingerprint_with_key(
                "focused-type-v1",
                "fixture input",
                &key,
                second_dir.path()
            )
        );
        assert_ne!(
            original,
            browser_persistent_fingerprint_with_key(
                "focused-type-v1",
                "fixture input",
                &other_key,
                first_dir.path()
            )
        );
        assert_ne!(
            original,
            browser_persistent_fingerprint_with_key(
                "browser-eval-v1",
                "fixture input",
                &key,
                first_dir.path()
            )
        );
        for purpose in ["focused-key-v1", "focused-press-v1"] {
            let before_restart = browser_persistent_fingerprint_with_key(
                purpose,
                "fixture input",
                &key,
                first_dir.path(),
            );
            let after_restart = browser_persistent_fingerprint_with_key(
                purpose,
                "fixture input",
                &key,
                first_dir.path(),
            );
            assert_eq!(before_restart, after_restart);
            assert_ne!(before_restart, original);
        }

        let css_fill = check_permissions(
            "browser",
            &json!({"action":"fill","selector":"#name","text":"fixture input","expected_epoch":17}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(css_fill[0].resource, "browser:17:fill:#name");
        let semantic_fill = check_permissions(
            "browser",
            &json!({"action":"fill","target":{"kind":"role","role":"textbox","name":"Name"},"text":"fixture input","expected_epoch":17}),
        )
        .unwrap()
        .unwrap();
        assert!(semantic_fill[0].resource.ends_with(&format!(
            ":text:{}",
            browser_type_fingerprint("fixture input")
        )));
    }

    #[test]
    fn focused_type_fingerprint_child_process() {
        let Some(output_path) = std::env::var_os("BAMBOO_FOCUSED_TYPE_TEST_OUTPUT") else {
            return;
        };
        let result = check_permissions(
            "browser",
            &json!({"action":"type","text":"fixture input","expected_epoch":17}),
        );
        let outcome = match result {
            Ok(Some(mut contexts)) => format!("ok:{}", contexts.remove(0).resource),
            Err(error) => format!("error:{error}"),
            Ok(None) => panic!("focused type needs a permission context"),
        };
        fs::write(output_path, outcome).unwrap();
    }

    #[test]
    fn focused_type_one_shot_receipt_matches_after_process_restart() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let blocked_file_name = "bamboo-encryption-test-key";
        let run = |data_dir: &Path, output_path: &Path, env_key: Option<&str>| {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tool_permissions::tests::focused_type_fingerprint_child_process",
                ])
                .env("BAMBOO_DATA_DIR", data_dir)
                .env("BAMBOO_FOCUSED_TYPE_TEST_OUTPUT", output_path);
            if let Some(env_key) = env_key {
                command.env("BAMBOO_CONFIG_ENCRYPTION_KEY", env_key);
            } else {
                command.env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY");
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "fingerprint child process failed");
            fs::read_to_string(output_path).unwrap()
        };
        let first = run(first_dir.path(), &first_dir.path().join("first.txt"), None);
        let restarted = run(
            first_dir.path(),
            &first_dir.path().join("restarted.txt"),
            None,
        );
        let other_install = run(
            second_dir.path(),
            &second_dir.path().join("other.txt"),
            None,
        );
        assert!(first.starts_with("ok:browser:17:type:focused:"));
        assert_eq!(
            first, restarted,
            "same data dir must replay the exact resource"
        );
        assert_ne!(first, other_install, "different data dirs must be isolated");

        let config = crate::PermissionConfig::default();
        let old_resource = first.strip_prefix("ok:").unwrap();
        let current_resource = restarted.strip_prefix("ok:").unwrap();
        config
            .grant_once_for_generation(
                "chat",
                "call",
                "generation",
                PermissionType::BrowserInteraction,
                old_resource.to_string(),
            )
            .unwrap();
        assert!(config.consume_once_for_generation(
            "chat",
            "call",
            "generation",
            PermissionType::BrowserInteraction,
            current_resource,
        ));
        assert!(!config.consume_once_for_generation(
            "chat",
            "call",
            "generation",
            PermissionType::BrowserInteraction,
            current_resource,
        ));

        let env_key_before = hex::encode([9u8; 32]);
        let env_key_after = hex::encode([10u8; 32]);
        let env_only_dir = first_dir.path().join("env-only");
        let env_first = run(
            &env_only_dir,
            &first_dir.path().join("env-first.txt"),
            Some(&env_key_before),
        );
        let env_restarted = run(
            &env_only_dir,
            &first_dir.path().join("env-restarted.txt"),
            Some(&env_key_before),
        );
        let env_rotated = run(
            &env_only_dir,
            &first_dir.path().join("env-rotated.txt"),
            Some(&env_key_after),
        );
        assert!(env_only_dir.is_dir(), "valid env key creates the data dir");
        assert_eq!(env_first, env_restarted);
        assert_ne!(
            env_first, env_rotated,
            "key rotation invalidates old receipts"
        );
        config
            .grant_once_for_generation(
                "chat",
                "rotated-call",
                "generation",
                PermissionType::BrowserInteraction,
                env_first.strip_prefix("ok:").unwrap().to_string(),
            )
            .unwrap();
        assert!(!config.consume_once_for_generation(
            "chat",
            "rotated-call",
            "generation",
            PermissionType::BrowserInteraction,
            env_rotated.strip_prefix("ok:").unwrap(),
        ));

        let blocked_dir = first_dir.path().join(blocked_file_name);
        fs::write(&blocked_dir, "not a directory").unwrap();
        let blocked = run(&blocked_dir, &first_dir.path().join("blocked.txt"), None);
        assert_eq!(
            blocked,
            "error:Permission check failed: browser permission requires a stable private fingerprint key"
        );
    }

    #[test]
    fn browser_type_grant_is_bound_to_input_without_exposing_text() {
        let context = |text: &str, epoch| {
            check_permissions(
                "browser",
                &json!({"action":"type","text":text,"expected_epoch":epoch}),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        let original = context("private input", 17);
        assert_eq!(original.permission_type, PermissionType::BrowserInteraction);
        assert!(original.resource.starts_with("browser:17:type:focused:"));
        assert!(!original.resource.contains("private input"));
        assert_eq!(
            original.operation_description,
            "Type into focused browser element"
        );
        assert_eq!(original.resource, context("private input", 17).resource);
        let different_text = context("other input", 17);
        assert_ne!(original.resource, different_text.resource);
        assert_ne!(original.resource, context("private input", 18).resource);

        let config = crate::PermissionConfig::default();
        let matcher =
            crate::conservative_matchers(PermissionType::BrowserInteraction, &original.resource)
                .remove(0);
        config
            .grant_typed_scoped_session_permission(
                "chat",
                PermissionType::BrowserInteraction,
                matcher,
            )
            .unwrap();
        assert!(config.is_scoped_session_granted(
            "chat",
            PermissionType::BrowserInteraction,
            &original.resource
        ));
        assert!(!config.is_scoped_session_granted(
            "chat",
            PermissionType::BrowserInteraction,
            &different_text.resource
        ));
    }

    #[test]
    fn browser_dialog_response_permission_binds_id_epoch_choice_and_private_text() {
        let dialog_id = "a".repeat(24);
        let context = |id: &str, epoch: u64, accept: bool, text: Option<&str>| {
            check_permissions(
                "browser",
                &json!({
                    "action":"dialog_respond","dialog_id":id,
                    "expected_epoch":epoch,"accept":accept,"text":text
                }),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        let original = context(&dialog_id, 17, true, Some("private answer"));
        assert_eq!(original.permission_type, PermissionType::BrowserInteraction);
        assert!(original
            .resource
            .starts_with(&format!("browser:17:dialog_respond:{dialog_id}:accept:")));
        assert!(!original.resource.contains("private answer"));
        assert_eq!(
            original.operation_description,
            "Answer pending browser dialog"
        );
        assert!(crate::PermissionRequest::is_focused_browser_resource(
            "browser",
            &original.resource
        ));
        assert!(is_focused_browser_input(
            "browser",
            &json!({"action":"dialog_respond","dialog_id":dialog_id})
        ));
        assert_eq!(
            original.resource,
            context(&dialog_id, 17, true, Some("private answer")).resource
        );
        assert_ne!(
            original.resource,
            context(&dialog_id, 18, true, Some("private answer")).resource
        );
        assert_ne!(
            original.resource,
            context(&"b".repeat(24), 17, true, Some("private answer")).resource
        );
        assert_ne!(
            original.resource,
            context(&dialog_id, 17, true, Some("other answer")).resource
        );
        assert_ne!(
            original.resource,
            context(&dialog_id, 17, false, None).resource
        );
        for args in [
            json!({"action":"dialog_respond","dialog_id":"short","accept":true,"expected_epoch":17}),
            json!({"action":"dialog_respond","dialog_id":dialog_id,"accept":true}),
            json!({"action":"dialog_respond","dialog_id":dialog_id,"expected_epoch":17}),
            json!({"action":"dialog_respond","dialog_id":dialog_id,"accept":false,"text":"private answer","expected_epoch":17}),
            json!({"action":"dialog_respond","dialog_id":dialog_id,"accept":true,"text":"x".repeat(4097),"expected_epoch":17}),
        ] {
            assert!(check_permissions("browser", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn browser_semantic_grants_bind_target_frame_epoch_and_fill_text() {
        let context = |action: &str, target: Value, epoch: u64, text: &str| {
            check_permissions(
                "browser",
                &json!({"action":action,"target":target,"expected_epoch":epoch,"text":text,"key":"Enter"}),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        let role =
            json!({"kind":"role","role":"button","name":"Save","frame_selector":"iframe#checkout"});
        let original = context("click", role.clone(), 17, "");
        assert_eq!(original.permission_type, PermissionType::BrowserInteraction);
        assert!(original.resource.starts_with("browser:17:click:semantic:"));
        assert!(!original.resource.contains("Save"));
        assert_eq!(
            original.resource,
            "browser:17:click:semantic:164a3f6e8b4b57fc37043624be24dc835d53ed605c301b27aa9d7ca9c0fe031e"
        );
        assert!(original
            .operation_description
            .contains("role button named \"Save\""));
        assert!(original.operation_description.contains("iframe#checkout"));
        assert_eq!(
            original.resource,
            context("click", json!({"frame_selector":"iframe#checkout","name":"Save","role":"button","kind":"role","exact":true}), 17, "").resource
        );
        assert_ne!(
            original.resource,
            context("click", role.clone(), 18, "").resource
        );
        assert_ne!(original.resource, context("click", json!({"kind":"role","role":"button","name":"Save","frame_selector":"iframe#other"}), 17, "").resource);
        assert_ne!(original.resource, context("click", json!({"kind":"role","role":"button","name":"Cancel","frame_selector":"iframe#checkout"}), 17, "").resource);
        assert_ne!(original.resource, context("click", json!({"kind":"role","role":"button","name":"Save","frame_selector":"iframe#checkout","exact":false}), 17, "").resource);
        assert_ne!(original.resource, context("press", role, 17, "").resource);
        let press = |key: &str| {
            check_permissions(
                "browser",
                &json!({"action":"press","target":{"kind":"role","role":"button","name":"Save","frame_selector":"iframe#checkout"},"key":key,"expected_epoch":17}),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        assert_ne!(press("Tab").resource, press("Enter").resource);
        assert_eq!(press("Tab").resource, press("Tab").resource);
        assert!(!press("Control+A").resource.contains("Control+A"));

        let label = json!({"kind":"label","value":"Secret field"});
        let first_fill = context("fill", label.clone(), 17, "private value");
        let second_fill = context("fill", label, 17, "other value");
        assert_ne!(first_fill.resource, second_fill.resource);
        assert!(!first_fill.resource.contains("Secret field"));
        assert!(!first_fill.resource.contains("private value"));
        assert!(first_fill.operation_description.contains("Secret field"));
        assert!(!first_fill.operation_description.contains("private value"));
        assert_ne!(
            first_fill.resource,
            context(
                "fill",
                json!({"kind":"label","value":"Secret field"}),
                17,
                ""
            )
            .resource
        );
    }

    #[test]
    fn browser_css_press_grants_bind_the_key_selector_and_epoch() {
        let context = |selector: &str, key: &str, epoch: u64| {
            check_permissions(
                "browser",
                &json!({"action":"press","selector":selector,"key":key,"expected_epoch":epoch}),
            )
            .unwrap()
            .unwrap()
            .remove(0)
        };
        let enter = context("#save", "Enter", 17);
        assert_eq!(enter.permission_type, PermissionType::BrowserInteraction);
        assert!(enter
            .resource
            .starts_with("browser:17:press:css:#save:key:"));
        assert_eq!(enter.operation_description, "Browser press on #save");
        assert!(!enter.resource.contains("Enter"));
        assert_eq!(enter.resource, context("#save", "Enter", 17).resource);
        assert_ne!(enter.resource, context("#save", "Control+A", 17).resource);
        assert_ne!(enter.resource, context("#other", "Enter", 17).resource);
        assert_ne!(enter.resource, context("#save", "Enter", 18).resource);
        assert!(context("page", "Enter", 17)
            .resource
            .starts_with("browser:17:press:css:page:key:"));
        let literal_focused = context("focused", "Enter", 17);
        assert!(literal_focused
            .resource
            .starts_with("browser:17:press:css:focused:key:"));
        assert!(!crate::PermissionRequest::is_focused_browser_resource(
            "browser",
            &literal_focused.resource
        ));

        let semantic = check_permissions(
            "browser",
            &json!({"action":"press","target":{"kind":"role","role":"button","name":"Save"},"key":"Enter","expected_epoch":17}),
        )
        .unwrap()
        .unwrap()
        .remove(0);
        let key_digest = enter.resource.rsplit(":key:").next().unwrap();
        assert_eq!(key_digest.len(), 64);
        assert!(semantic.resource.ends_with(key_digest));
        assert!(semantic.resource.contains(":press:semantic:"));
    }

    #[test]
    fn remembered_css_enter_approval_does_not_authorize_control_a() {
        use crate::{
            PermissionConfig, PermissionDecisionKind, PermissionDecisionSource,
            PermissionEvaluation, PermissionOutcome, RiskLevel,
        };

        let args = |key: &str| json!({"action":"press","selector":"focused","key":key,"expected_epoch":17});
        let enter_args = args("Enter");
        let control_args = args("Control+A");
        let enter = check_permissions("browser", &enter_args)
            .unwrap()
            .unwrap()
            .remove(0);
        let control = check_permissions("browser", &control_args)
            .unwrap()
            .unwrap()
            .remove(0);
        assert!(!crate::PermissionRequest::is_focused_browser_resource(
            "browser",
            &enter.resource
        ));
        let config = PermissionConfig::new();
        let matcher =
            crate::conservative_matchers(enter.permission_type, &enter.resource).remove(0);
        config
            .grant_typed_scoped_session_permission("chat", enter.permission_type, matcher)
            .unwrap();
        let evaluation = |context: &PermissionContext, tool_args: Value| PermissionEvaluation {
            request_id: "browser-press".into(),
            session_id: "chat".into(),
            workspace_path: None,
            tool_name: "browser".into(),
            tool_args,
            permission_type: context.permission_type,
            resource: context.resource.clone(),
            operation_summary: context.operation_description.clone(),
            risk_level: RiskLevel::High,
            bypass_requested: false,
            auto_approve_requested: false,
            platform_hard_deny: None,
            consume_once: true,
            supported_decisions: PermissionDecisionKind::all_supported(),
        };
        assert!(matches!(
            config.evaluate(evaluation(&enter, enter_args)),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::RememberedSession,
                ..
            }
        ));
        assert!(matches!(
            config.evaluate(evaluation(&control, control_args)),
            PermissionOutcome::Ask(_)
        ));
    }

    #[test]
    fn browser_press_rejects_invalid_keys_before_approval() {
        for key in [
            json!(null),
            json!(7),
            json!(""),
            json!(" "),
            json!("Control\nA"),
            json!("a".repeat(129)),
        ] {
            assert!(
                check_permissions(
                    "browser",
                    &json!({"action":"press","selector":"#save","key":key,"expected_epoch":17}),
                )
                .is_err(),
                "{key}"
            );
        }
        for selector in [json!(""), json!(" "), json!(7)] {
            assert!(check_permissions(
                "browser",
                &json!({"action":"press","selector":selector,"key":"Enter","expected_epoch":17}),
            )
            .is_err());
        }
    }

    #[test]
    fn browser_semantic_targets_reject_invalid_approval_requests() {
        for args in [
            json!({"action":"click","expected_epoch":17,"target":{"kind":"role","role":"button"},"selector":"#save"}),
            json!({"action":"click","expected_epoch":17,"target":{"kind":"label"}}),
            json!({"action":"click","expected_epoch":17,"target":{"kind":"text","value":"Save","exact":"yes"}}),
            json!({"action":"click","expected_epoch":17,"target":{"kind":"role","role":"BUTTON"}}),
            json!({"action":"click","expected_epoch":17,"target":{"kind":"text","value":"Save","frame_selector":" "}}),
            json!({"action":"click","expected_epoch":17}),
            json!({"action":"press","expected_epoch":17,"target":{"kind":"text","value":"Save"}}),
        ] {
            assert!(check_permissions("browser", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn browser_host_controls_reject_missing_epoch_and_invalid_arguments_before_approval() {
        let valid_without_epoch = [
            json!({"action":"history","direction":"back"}),
            json!({"action":"viewport","width":640,"height":480}),
            json!({"action":"click_at","x":12,"y":20}),
            json!({"action":"type","text":"Lotus"}),
            json!({"action":"key","key":"Enter"}),
        ];
        for args in valid_without_epoch {
            assert!(check_permissions("browser", &args).is_err(), "{args}");
        }
        let invalid = [
            json!({"action":"history","direction":"sideways","expected_epoch":17}),
            json!({"action":"viewport","width":319,"height":480,"expected_epoch":17}),
            json!({"action":"click_at","x":-1,"y":20,"expected_epoch":17}),
            json!({"action":"click_at","x":12,"y":20,"button":"invalid","expected_epoch":17}),
            json!({"action":"type","expected_epoch":17}),
            json!({"action":"key","key":"","expected_epoch":17}),
        ];
        for args in invalid {
            assert!(check_permissions("browser", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn browser_permission_rejects_unsafe_navigation_schemes_and_credentials() {
        for url in [
            "file:///tmp/a",
            "javascript:alert(1)",
            "http://user:pass@example.com/",
        ] {
            assert!(
                check_permissions("browser", &json!({"action":"navigate","url":url})).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn check_permissions_write() {
        let args = json!({"file_path": "/tmp/test.txt"});
        let contexts = check_permissions("Write", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
    }

    #[test]
    fn request_permissions_batch_becomes_independent_typed_contexts() {
        let contexts = check_permissions(
            "request_permissions",
            &serde_json::json!({
                "reason": "Deploy the service",
                "permissions": [
                    {"type": "execute_command", "resource": "docker compose up -d"},
                    {
                        "type": "http_request",
                        "resource": "registry.example.com",
                        "description": "pull images"
                    }
                ]
            }),
        )
        .unwrap()
        .expect("typed permission contexts");
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].permission_type, PermissionType::ExecuteCommand);
        assert_eq!(contexts[0].resource, "docker compose up -d");
        assert_eq!(contexts[1].permission_type, PermissionType::HttpRequest);
        assert_eq!(contexts[1].resource, "registry.example.com");
        assert!(contexts
            .iter()
            .all(|context| context.operation_description.contains("Deploy the service")));
    }

    #[test]
    fn request_permissions_batch_enforces_replay_ledger_boundary() {
        let permission = || json!({"type": "write_file", "resource": "/workspace/file"});
        let at_limit = (0..MAX_PROACTIVE_PERMISSION_BATCH)
            .map(|_| permission())
            .collect::<Vec<_>>();
        let contexts = check_permissions(
            "request_permissions",
            &json!({"reason": "Prepare files", "permissions": at_limit}),
        )
        .unwrap()
        .expect("64 contexts fit the replay ledger");
        assert_eq!(contexts.len(), MAX_PROACTIVE_PERMISSION_BATCH);

        let over_limit = (0..=MAX_PROACTIVE_PERMISSION_BATCH)
            .map(|_| permission())
            .collect::<Vec<_>>();
        let error = check_permissions(
            "request_permissions",
            &json!({"reason": "Prepare files", "permissions": over_limit}),
        )
        .unwrap_err();
        assert!(error.to_string().contains("more than 64 items"));
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
                "session_control",
                json!({"action": "followup", "target_session_id": "root-a", "operation_id": "op-a", "message": "continue"}),
            ),
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
            (
                "session_history_current",
                json!({"action": "read_current", "limit": 5}),
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
