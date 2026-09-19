//! Pure, host-owned projection policy for plugin [`ToolEventV1`] delivery.
//!
//! This module deliberately performs no filesystem or async I/O. Tool
//! publication is a synchronous hot path, so path handling is lexical and
//! fail-closed: relative, traversal, control-character, drive-relative, and
//! extended-device paths are redacted. The mutation tools separately prove
//! filesystem provenance before they publish a successful event.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use bamboo_plugin::{
    EventSinkPermissionGrants, ObservationPermissionId, PluginError, PluginManifest, PluginResult,
    RegisteredCapabilities, MAX_EVENT_SINKS_PER_PLUGIN, MAX_EVENT_SINK_ID_BYTES,
    MAX_EVENT_SINK_PERMISSIONS, MAX_EVENT_SINK_PERMISSION_ID_BYTES, OBSERVE_CONTENT_PERMISSION,
    OBSERVE_DIFF_PERMISSION, OBSERVE_METADATA_PERMISSION, OBSERVE_PATHS_PERMISSION,
    OBSERVE_TOOL_NAME_PERMISSION,
};
use bamboo_plugin_protocol::{
    ProjectedFileChangedV1, ProjectedToolEventContextV1, ProjectedToolEventV1, ToolEventBuildError,
    ToolEventV1, MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES, MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES,
    TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED, TOOL_EVENT_PATH_REDACTION_SENSITIVE,
    TOOL_EVENT_PATH_REDACTION_UNSAFE, TOOL_EVENT_V1_SCHEMA_VERSION,
};
use serde::Deserialize;
use serde_json::{Map, Value};

pub const TOOL_EVENT_DIFF_FIELD: &str = "diff";
pub const TOOL_EVENT_CONTENT_FIELD: &str = "content";
pub const TOOL_EVENT_DIFF_TRUNCATED_FIELD: &str = "diff_truncated";
pub const TOOL_EVENT_CONTENT_TRUNCATED_FIELD: &str = "content_truncated";
pub const TOOL_EVENT_PATH_REDACTION_REASON_FIELD: &str = "path_redaction_reason";
pub const TOOL_EVENT_POLICY_GENERATION_FIELD: &str = "observation_policy_generation";

pub const PATH_REDACTION_PERMISSION_NOT_GRANTED: &str =
    TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED;
pub const PATH_REDACTION_SENSITIVE: &str = TOOL_EVENT_PATH_REDACTION_SENSITIVE;
pub const PATH_REDACTION_UNSAFE: &str = TOOL_EVENT_PATH_REDACTION_UNSAFE;

const TRUNCATION_MARKER: &str = "\u{2026}";

const KNOWN_PERMISSIONS: &[&str] = &[
    OBSERVE_METADATA_PERMISSION,
    OBSERVE_TOOL_NAME_PERMISSION,
    OBSERVE_PATHS_PERMISSION,
    OBSERVE_DIFF_PERMISSION,
    OBSERVE_CONTENT_PERMISSION,
];

/// Runtime host policy. Manifest permissions are requests only; a field is
/// projected when its request and this grant set both contain the permission.
/// Metadata is always retained as the minimum routable/safe envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolEventObservationPolicy {
    granted: BTreeSet<String>,
}

impl Default for ToolEventObservationPolicy {
    fn default() -> Self {
        Self::metadata_only()
    }
}

impl ToolEventObservationPolicy {
    pub fn metadata_only() -> Self {
        Self {
            granted: BTreeSet::from([OBSERVE_METADATA_PERMISSION.to_string()]),
        }
    }

    /// Construct a policy from an already validated, exact host grant set.
    /// This intentionally does not add metadata: corrupt persisted authority
    /// must fail closed in the resolver instead of being silently repaired on
    /// the delivery hot path.
    pub(crate) fn from_validated_permission_ids<I, S>(permissions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut policy = Self {
            granted: BTreeSet::new(),
        };
        for permission in permissions {
            let permission = permission.as_ref();
            if KNOWN_PERMISSIONS.contains(&permission) {
                policy.granted.insert(permission.to_string());
            }
        }
        policy
    }

    #[cfg(test)]
    pub(crate) fn all_v1() -> Self {
        Self::from_validated_permission_ids(KNOWN_PERMISSIONS.iter().copied())
    }

    fn grants(&self, permission: &str) -> bool {
        self.granted.contains(permission)
    }

    pub(crate) fn grant_requested(
        &self,
        requested: &[ObservationPermissionId],
    ) -> GrantedObservation {
        let requested: BTreeSet<&str> = requested.iter().map(|id| id.as_str()).collect();
        let has = |permission| requested.contains(permission) && self.grants(permission);
        GrantedObservation {
            metadata: has(OBSERVE_METADATA_PERMISSION),
            tool_name: has(OBSERVE_TOOL_NAME_PERMISSION),
            paths: has(OBSERVE_PATHS_PERMISSION),
            diff: has(OBSERVE_DIFF_PERMISSION),
            content: has(OBSERVE_CONTENT_PERMISSION),
        }
    }
}

/// One explicit request-side grant record. HTTP deliberately uses a list,
/// rather than a JSON object, so duplicate sink ids are observable and can be
/// rejected instead of silently taking a parser's last value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSinkGrantRequest {
    pub sink_id: String,
    pub granted_permissions: Vec<ObservationPermissionId>,
}

/// Resolve request authority into the canonical map persisted in both the
/// Installing and Installed rows.
///
/// `Some(records)` is a complete target for all ToolEventV1 sinks: listed
/// sinks receive exactly the validated permissions, while omitted v1 sinks
/// receive metadata only. `None` is context-sensitive by design: a fresh
/// install starts metadata-only; an update preserves only prior grants that
/// the new manifest still requests. Newly requested permissions are never
/// granted implicitly.
pub(crate) fn resolve_event_sink_grants(
    manifest: &PluginManifest,
    previous: Option<&RegisteredCapabilities>,
    requested: Option<&[EventSinkGrantRequest]>,
) -> PluginResult<EventSinkPermissionGrants> {
    manifest.validate()?;

    if let Some(records) = requested {
        if records.len() > MAX_EVENT_SINKS_PER_PLUGIN {
            return invalid_grants(format!(
                "event sink grant request exceeds the per-plugin limit of {MAX_EVENT_SINKS_PER_PLUGIN}"
            ));
        }
        for record in records {
            validate_external_grant_record(record)?;
        }
        let mut explicit = BTreeMap::new();
        for record in records {
            if explicit
                .insert(record.sink_id.clone(), record.granted_permissions.clone())
                .is_some()
            {
                return invalid_grants(format!(
                    "event sink grant request repeats sink '{}'",
                    record.sink_id
                ));
            }
            let Some(sink) = manifest
                .provides
                .event_sinks
                .iter()
                .find(|sink| sink.id == record.sink_id)
            else {
                return invalid_grants(format!(
                    "event sink grant request names unknown sink '{}'",
                    record.sink_id
                ));
            };
            if !is_v1_sink(sink) {
                return invalid_grants(format!(
                    "event sink '{}' uses an unsupported protocol version for host grants",
                    record.sink_id
                ));
            }
        }

        let mut resolved = BTreeMap::new();
        for sink in manifest
            .provides
            .event_sinks
            .iter()
            .filter(|sink| is_v1_sink(sink))
        {
            let fallback = [ObservationPermissionId::new(OBSERVE_METADATA_PERMISSION)];
            let candidate = explicit
                .get(&sink.id)
                .map(Vec::as_slice)
                .unwrap_or(&fallback);
            resolved.insert(sink.id.clone(), canonicalize_sink_grants(sink, candidate)?);
        }
        return Ok(resolved);
    }

    validate_previous_grant_shape(previous)?;
    let previous_is_legacy =
        previous.is_none_or(|registered| registered.event_sink_grants.is_empty());
    let mut resolved = BTreeMap::new();
    for sink in manifest
        .provides
        .event_sinks
        .iter()
        .filter(|sink| is_v1_sink(sink))
    {
        let retained = previous
            .is_some_and(|registered| registered.event_sink_ids.iter().any(|id| id == &sink.id));
        let candidate = if retained && !previous_is_legacy {
            let Some(grants) =
                previous.and_then(|registered| registered.event_sink_grants.get(&sink.id))
            else {
                return invalid_grants(format!(
                    "persisted host grants omit retained ToolEventV1 sink '{}'",
                    sink.id
                ));
            };
            grants
                .iter()
                .filter(|permission| {
                    sink.requested_permissions
                        .iter()
                        .any(|requested| requested == *permission)
                })
                .cloned()
                .collect::<Vec<_>>()
        } else {
            vec![ObservationPermissionId::new(OBSERVE_METADATA_PERMISSION)]
        };
        resolved.insert(sink.id.clone(), canonicalize_sink_grants(sink, &candidate)?);
    }
    Ok(resolved)
}

/// Validate/canonicalize durable authority at the boot/router boundary.
/// Only an entirely empty legacy map is migrated to metadata-only. Once any
/// grant is persisted, every ToolEventV1 sink must have an exact entry.
pub(crate) fn canonicalize_persisted_event_sink_grants(
    manifest: &PluginManifest,
    persisted: &EventSinkPermissionGrants,
) -> PluginResult<EventSinkPermissionGrants> {
    manifest.validate()?;
    if persisted.is_empty() {
        return resolve_event_sink_grants(manifest, None, None);
    }

    let mut resolved = BTreeMap::new();
    for (sink_id, permissions) in persisted {
        validate_permission_set_shape(sink_id, permissions)?;
        let Some(sink) = manifest
            .provides
            .event_sinks
            .iter()
            .find(|sink| &sink.id == sink_id)
        else {
            return invalid_grants(format!(
                "persisted host grants name unknown sink '{sink_id}'"
            ));
        };
        if !is_v1_sink(sink) {
            return invalid_grants(format!(
                "persisted host grants target unsupported sink '{sink_id}'"
            ));
        }
        resolved.insert(
            sink_id.clone(),
            canonicalize_sink_grants(sink, permissions)?,
        );
    }
    for sink in manifest
        .provides
        .event_sinks
        .iter()
        .filter(|sink| is_v1_sink(sink))
    {
        if !resolved.contains_key(&sink.id) {
            return invalid_grants(format!(
                "persisted host grants omit ToolEventV1 sink '{}'",
                sink.id
            ));
        }
    }
    Ok(resolved)
}

fn validate_previous_grant_shape(previous: Option<&RegisteredCapabilities>) -> PluginResult<()> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if previous.event_sink_grants.is_empty() {
        return Ok(());
    }
    let owned: HashSet<&str> = previous.event_sink_ids.iter().map(String::as_str).collect();
    for (sink_id, permissions) in &previous.event_sink_grants {
        validate_permission_set_shape(sink_id, permissions)?;
        if !owned.contains(sink_id.as_str()) {
            return invalid_grants(format!(
                "persisted host grants name sink '{sink_id}' outside registered event-sink provenance"
            ));
        }
    }
    Ok(())
}

fn canonicalize_sink_grants(
    sink: &bamboo_plugin::EventSinkManifestEntry,
    permissions: &[ObservationPermissionId],
) -> PluginResult<Vec<ObservationPermissionId>> {
    validate_permission_set_shape(&sink.id, permissions)?;
    for permission in permissions {
        if !sink
            .requested_permissions
            .iter()
            .any(|requested| requested == permission)
        {
            return invalid_grants(format!(
                "event sink '{}' host grant '{}' was not requested by its manifest",
                sink.id,
                permission.as_str()
            ));
        }
    }
    Ok(KNOWN_PERMISSIONS
        .iter()
        .filter(|known| {
            permissions
                .iter()
                .any(|granted| granted.as_str() == **known)
        })
        .map(|known| ObservationPermissionId::new(*known))
        .collect())
}

fn validate_permission_set_shape(
    sink_id: &str,
    permissions: &[ObservationPermissionId],
) -> PluginResult<()> {
    if sink_id.trim().is_empty() || sink_id.len() > MAX_EVENT_SINK_ID_BYTES {
        return invalid_grants("event sink host grant has an invalid bounded sink id".to_string());
    }
    if permissions.is_empty() || permissions.len() > MAX_EVENT_SINK_PERMISSIONS {
        return invalid_grants(format!(
            "event sink '{sink_id}' host grants must contain 1..={MAX_EVENT_SINK_PERMISSIONS} permissions"
        ));
    }
    let mut seen = HashSet::new();
    for permission in permissions {
        let value = permission.as_str();
        if value.trim().is_empty() || value.len() > MAX_EVENT_SINK_PERMISSION_ID_BYTES {
            return invalid_grants(format!(
                "event sink '{sink_id}' has an invalid bounded host grant id"
            ));
        }
        if !KNOWN_PERMISSIONS.contains(&value) {
            return invalid_grants(format!(
                "event sink '{sink_id}' has an unknown ToolEventV1 host grant"
            ));
        }
        if !seen.insert(value) {
            return invalid_grants(format!(
                "event sink '{sink_id}' repeats host grant '{value}'"
            ));
        }
    }
    if !seen.contains(OBSERVE_METADATA_PERMISSION) {
        return invalid_grants(format!(
            "event sink '{sink_id}' host grants must include '{OBSERVE_METADATA_PERMISSION}'"
        ));
    }
    if (seen.contains(OBSERVE_DIFF_PERMISSION) || seen.contains(OBSERVE_CONTENT_PERMISSION))
        && !seen.contains(OBSERVE_PATHS_PERMISSION)
    {
        return invalid_grants(format!(
            "event sink '{sink_id}' grants diff/content without the required paths grant"
        ));
    }
    Ok(())
}

fn validate_external_grant_record(record: &EventSinkGrantRequest) -> PluginResult<()> {
    if record.sink_id.trim().is_empty() || record.sink_id.len() > MAX_EVENT_SINK_ID_BYTES {
        return invalid_grants(
            "event sink grant request contains an invalid bounded sink id".to_string(),
        );
    }
    if record.granted_permissions.is_empty()
        || record.granted_permissions.len() > MAX_EVENT_SINK_PERMISSIONS
    {
        return invalid_grants(format!(
            "event sink grant request permissions must contain 1..={MAX_EVENT_SINK_PERMISSIONS} entries"
        ));
    }
    for permission in &record.granted_permissions {
        let value = permission.as_str();
        if value.trim().is_empty() || value.len() > MAX_EVENT_SINK_PERMISSION_ID_BYTES {
            return invalid_grants(
                "event sink grant request contains an invalid bounded permission id".to_string(),
            );
        }
    }
    Ok(())
}

fn is_v1_sink(sink: &bamboo_plugin::EventSinkManifestEntry) -> bool {
    sink.protocol.version == TOOL_EVENT_V1_SCHEMA_VERSION
}

fn invalid_grants<T>(message: String) -> PluginResult<T> {
    Err(PluginError::InvalidManifest(message))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GrantedObservation {
    metadata: bool,
    tool_name: bool,
    paths: bool,
    diff: bool,
    content: bool,
}

impl GrantedObservation {
    pub(crate) fn can_observe_tool_name(self) -> bool {
        self.tool_name
    }

    pub(crate) fn permission_ids(self) -> Vec<ObservationPermissionId> {
        [
            (self.metadata, OBSERVE_METADATA_PERMISSION),
            (self.tool_name, OBSERVE_TOOL_NAME_PERMISSION),
            (self.paths, OBSERVE_PATHS_PERMISSION),
            (self.diff, OBSERVE_DIFF_PERMISSION),
            (self.content, OBSERVE_CONTENT_PERMISSION),
        ]
        .into_iter()
        .filter(|(granted, _)| *granted)
        .map(|(_, permission)| ObservationPermissionId::new(permission))
        .collect()
    }
}

/// Build the only object that may cross a per-sink queue. Producer extensions
/// are deny-by-default: v1 copies only the two explicitly permissioned payload
/// fields and synthesizes bounded host metadata/redaction markers.
pub(crate) fn project_tool_event(
    event: &ToolEventV1,
    grants: GrantedObservation,
    policy_generation: u64,
) -> Result<ProjectedToolEventV1, ToolEventBuildError> {
    let source = event
        .data
        .as_object()
        .ok_or(ToolEventBuildError::DataMustBeObject)?;
    let source_path = source.get("path").and_then(Value::as_str).ok_or_else(|| {
        ToolEventBuildError::InvalidKnownPayload(
            "file_changed data.path must be a string".to_string(),
        )
    })?;

    // Validated v1 manifests must request metadata. Keep this explicit and
    // fail closed if a corrupt/runtime-constructed registration bypasses that
    // invariant rather than projecting authority-bearing ids unexpectedly.
    if !grants.metadata {
        return Err(ToolEventBuildError::InvalidKnownPayload(
            "metadata permission is required for ToolEventV1 projection".to_string(),
        ));
    }

    let context = ProjectedToolEventContextV1 {
        session_id: event.context.session_id.clone(),
        root_session_id: event.context.root_session_id.clone(),
        tool_name: grants.tool_name.then(|| event.context.tool_name.clone()),
        tool_call_id: event.context.tool_call_id.clone(),
    };

    let path = project_path(source_path, grants.paths);
    let mut data = ProjectedFileChangedV1 {
        path: path.value,
        path_redaction_reason: path.redaction_reason.map(str::to_string),
        ..ProjectedFileChangedV1::default()
    };

    // Sensitive/unsafe paths never carry payload, even if a caller grants
    // content. This ordering prevents the path policy and payload policy from
    // being evaluated as independent, accidentally leaky decisions.
    if path.redaction_reason.is_none() {
        (data.diff, data.diff_truncated) = project_bounded_string_extension(
            source,
            TOOL_EVENT_DIFF_FIELD,
            grants.diff,
            MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES,
        )?;
        (data.content, data.content_truncated) = project_bounded_string_extension(
            source,
            TOOL_EVENT_CONTENT_FIELD,
            grants.content,
            MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES,
        )?;
    }

    Ok(ProjectedToolEventV1::file_changed(
        context,
        data,
        policy_generation,
    ))
}

fn project_bounded_string_extension(
    source: &Map<String, Value>,
    field: &str,
    granted: bool,
    max_bytes: usize,
) -> Result<(Option<String>, bool), ToolEventBuildError> {
    if !granted {
        return Ok((None, false));
    }
    let Some(value) = source.get(field) else {
        return Ok((None, false));
    };
    let value = value.as_str().ok_or_else(|| {
        ToolEventBuildError::InvalidKnownPayload(format!(
            "file_changed data.{field} must be a string"
        ))
    })?;
    let (value, truncated) = truncate_utf8(value, max_bytes);
    Ok((Some(value), truncated))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let budget = max_bytes.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = budget.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut bounded = String::with_capacity(max_bytes);
    bounded.push_str(&value[..end]);
    if TRUNCATION_MARKER.len() <= max_bytes {
        bounded.push_str(TRUNCATION_MARKER);
    }
    (bounded, true)
}

#[derive(Debug)]
struct ProjectedPath {
    value: Option<String>,
    redaction_reason: Option<&'static str>,
}

fn project_path(raw: &str, granted: bool) -> ProjectedPath {
    if !granted {
        return redacted_path(PATH_REDACTION_PERMISSION_NOT_GRANTED);
    }
    let Some(normalized) = normalize_absolute_path(raw) else {
        return redacted_path(PATH_REDACTION_UNSAFE);
    };
    if is_sensitive_normalized_path(&normalized) {
        return redacted_path(PATH_REDACTION_SENSITIVE);
    }
    ProjectedPath {
        value: Some(normalized),
        redaction_reason: None,
    }
}

fn redacted_path(reason: &'static str) -> ProjectedPath {
    ProjectedPath {
        value: None,
        redaction_reason: Some(reason),
    }
}

/// Pure cross-platform lexical normalization. Ambiguity is redacted rather
/// than guessed: `..`, relative/drive-relative paths, device namespaces, and
/// control characters return `None`.
fn normalize_absolute_path(raw: &str) -> Option<String> {
    if raw.is_empty() || raw.chars().any(char::is_control) {
        return None;
    }
    if raw.starts_with('\\') && !raw.starts_with("\\\\") {
        // A single leading backslash is Windows drive-relative/rooted syntax
        // (and also covers NT object-manager spellings such as `\Device` and
        // `\??`). It is not an absolute, portable identity and must not be
        // reinterpreted as a Unix path after separator normalization.
        return None;
    }
    let slash = raw.replace('\\', "/");
    let raw = slash.as_str();
    let lower = raw.to_ascii_lowercase();
    let namespace = lower.trim_start_matches('/');
    if lower.starts_with("//?/")
        || lower.starts_with("//./")
        || namespace.starts_with("??/")
        || namespace.starts_with("device/")
        || namespace.starts_with("global??/")
    {
        // Windows device/extended namespaces can change path interpretation
        // (including component normalization); never reinterpret them here.
        return None;
    }

    #[derive(Clone, Copy)]
    enum Root<'a> {
        Unix(&'a str),
        Drive(char, &'a str),
        Unc(&'a str),
    }

    let root = if raw.starts_with("//") {
        Root::Unc(raw.trim_start_matches('/'))
    } else if let Some(rest) = raw.strip_prefix('/') {
        Root::Unix(rest)
    } else if raw.len() >= 3
        && raw.as_bytes()[0].is_ascii_alphabetic()
        && raw.as_bytes()[1] == b':'
        && raw.as_bytes()[2] == b'/'
    {
        Root::Drive(
            char::from(raw.as_bytes()[0]).to_ascii_uppercase(),
            &raw[3..],
        )
    } else {
        return None;
    };

    let rest = match root {
        Root::Unix(rest) | Root::Drive(_, rest) | Root::Unc(rest) => rest,
    };
    if let Root::Unc(rest) = root {
        let mut authority = rest.split('/');
        let server = authority.next()?;
        let share = authority.next()?;
        if server.is_empty()
            || share.is_empty()
            || matches!(server, "." | "..")
            || matches!(share, "." | "..")
            || windows_component_is_ambiguous(server)
            || windows_component_is_ambiguous(share)
        {
            return None;
        }
    }
    let windows_semantics = matches!(root, Root::Drive(..) | Root::Unc(..));
    let mut components = Vec::new();
    for component in rest.split('/') {
        if windows_semantics && windows_component_is_ambiguous(component) {
            return None;
        }
        match component {
            "" | "." => {}
            ".." => return None,
            value => components.push(value),
        }
    }

    match root {
        Root::Unix(_) => Some(if components.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", components.join("/"))
        }),
        Root::Drive(drive, _) => Some(if components.is_empty() {
            format!("{drive}:/")
        } else {
            format!("{drive}:/{}", components.join("/"))
        }),
        Root::Unc(_) => {
            // A UNC authority needs both server and share. Anything shorter is
            // ambiguous and therefore not observable.
            (components.len() >= 2).then(|| format!("//{}", components.join("/")))
        }
    }
}

fn windows_component_is_ambiguous(component: &str) -> bool {
    if component.is_empty() || matches!(component, "." | "..") {
        return false;
    }
    if component.contains(':') || component.ends_with([' ', '.']) {
        return true;
    }
    let basename = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_ascii_uppercase();
    matches!(basename.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
        || basename
            .strip_prefix("COM")
            .or_else(|| basename.strip_prefix("LPT"))
            .is_some_and(|number| number.len() == 1 && matches!(number.as_bytes()[0], b'1'..=b'9'))
}

fn is_sensitive_normalized_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let is_unc = lower.starts_with("//");
    let mut components: Vec<&str> = lower
        .trim_start_matches('/')
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    if components
        .first()
        .is_some_and(|component| component.ends_with(':'))
    {
        components.remove(0);
    }
    if components.is_empty() {
        return false;
    }

    // Windows administrative shares (`C$`, `ADMIN$`, `IPC$`) and other
    // hidden shares intentionally expose privileged/hidden namespaces. Once
    // a UNC path names one, never project its authority, descendants, or
    // payload even when paths/content were explicitly granted.
    if is_unc && components.get(1).is_some_and(|share| share.ends_with('$')) {
        return true;
    }

    // System credential/config namespaces on Unix and Windows.
    if matches!(
        components.as_slice(),
        ["etc", ..] | ["proc", ..] | ["sys", ..] | ["dev", ..] | ["boot", ..]
    ) || matches!(components.as_slice(), ["private", "etc", ..])
        || matches!(components.as_slice(), ["windows", "system32", ..])
        || matches!(components.as_slice(), ["programdata", ..])
    {
        return true;
    }

    const SENSITIVE_COMPONENTS: &[&str] = &[
        ".ssh",
        ".aws",
        ".gnupg",
        ".kube",
        ".bamboo",
        ".codex",
        ".claude",
        ".agents",
        ".config",
        ".git",
        "appdata",
        "keychains",
        "secrets",
    ];
    if components
        .iter()
        .any(|component| SENSITIVE_COMPONENTS.contains(component))
    {
        return true;
    }
    let basename = components.last().copied().unwrap_or_default();
    basename == ".env"
        || basename.starts_with(".env.")
        || matches!(
            basename,
            ".netrc"
                | ".npmrc"
                | ".pypirc"
                | ".git-credentials"
                | "credentials"
                | "credentials.json"
                | "secrets.json"
                | "id_rsa"
                | "id_ed25519"
                | "authorized_keys"
                | "agents.md"
                | "claude.md"
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_plugin_protocol::{
        FileChangedV1, ToolEventContextV1, FILE_CHANGED_SUBSCRIPTION_ID_V1,
        TOOL_EVENT_PROTOCOL_NAME,
    };
    use serde_json::json;

    fn event(path: &str) -> ToolEventV1 {
        let mut data = FileChangedV1::bounded(path).unwrap();
        for (key, value) in [
            (TOOL_EVENT_DIFF_FIELD, "diff-sentinel"),
            (TOOL_EVENT_CONTENT_FIELD, "content-sentinel"),
            ("config", "config-sentinel"),
            ("prompt", "prompt-sentinel"),
            ("credentials", "credential-sentinel"),
        ] {
            data.extensions.insert(key.to_string(), json!(value));
        }
        let mut event = ToolEventV1::file_changed(
            ToolEventContextV1::bounded("session-safe", "root-safe", "Write", "call-safe").unwrap(),
            data,
        )
        .unwrap();
        event
            .extensions
            .insert("prompt_dump".to_string(), json!("envelope-prompt-sentinel"));
        event.context.extensions.insert(
            "credential_dump".to_string(),
            json!("context-credential-sentinel"),
        );
        event
    }

    fn requested_all() -> Vec<ObservationPermissionId> {
        KNOWN_PERMISSIONS
            .iter()
            .map(|permission| ObservationPermissionId::new(*permission))
            .collect()
    }

    fn grant_manifest(sinks: &[(&str, &[&str])]) -> PluginManifest {
        let sinks: Vec<Value> = sinks
            .iter()
            .map(|(sink_id, permissions)| {
                json!({
                    "id": sink_id,
                    "service_id": "events-service",
                    "protocol": {
                        "name": TOOL_EVENT_PROTOCOL_NAME,
                        "version": TOOL_EVENT_V1_SCHEMA_VERSION
                    },
                    "subscriptions": [{"id": FILE_CHANGED_SUBSCRIPTION_ID_V1}],
                    "requested_permissions": permissions
                })
            })
            .collect();
        serde_json::from_value(json!({
            "id": "grant-test-plugin",
            "name": "Grant Test Plugin",
            "version": "1.0.0",
            "provides": {
                "services": [{
                    "id": "events-service",
                    "enabled": false,
                    "command": "${platform_bin}",
                    "input_protocol": "ndjson_v1"
                }],
                "event_sinks": sinks
            }
        }))
        .unwrap()
    }

    fn ids(values: &[&str]) -> Vec<ObservationPermissionId> {
        values
            .iter()
            .map(|value| ObservationPermissionId::new(*value))
            .collect()
    }

    #[test]
    fn fresh_and_explicit_grants_are_metadata_safe_and_canonical() {
        let manifest = grant_manifest(&[
            (
                "first",
                &["content", "tool_name", "metadata", "paths", "diff"],
            ),
            ("second", &["paths", "metadata"]),
        ]);
        let fresh = resolve_event_sink_grants(&manifest, None, None).unwrap();
        assert_eq!(fresh.get("first"), Some(&ids(&["metadata"])));
        assert_eq!(fresh.get("second"), Some(&ids(&["metadata"])));

        let explicit = resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "first".to_string(),
                granted_permissions: ids(&["content", "paths", "metadata"]),
            }]),
        )
        .unwrap();
        assert_eq!(
            explicit.get("first"),
            Some(&ids(&["metadata", "paths", "content"]))
        );
        assert_eq!(explicit.get("second"), Some(&ids(&["metadata"])));
    }

    #[test]
    fn update_omission_preserves_only_prior_requested_intersection() {
        let manifest = grant_manifest(&[
            (
                "retained",
                &["metadata", "paths", "diff", "content", "tool_name"],
            ),
            ("new", &["metadata", "paths", "content"]),
        ]);
        let previous = RegisteredCapabilities {
            event_sink_ids: vec!["retained".to_string(), "removed".to_string()],
            event_sink_grants: BTreeMap::from([
                ("retained".to_string(), ids(&["metadata", "paths", "diff"])),
                ("removed".to_string(), ids(&["metadata", "tool_name"])),
            ]),
            ..RegisteredCapabilities::default()
        };

        let resolved = resolve_event_sink_grants(&manifest, Some(&previous), None).unwrap();
        assert_eq!(
            resolved.get("retained"),
            Some(&ids(&["metadata", "paths", "diff"]))
        );
        assert_eq!(resolved.get("new"), Some(&ids(&["metadata"])));
        assert!(!resolved.contains_key("removed"));
        assert!(!resolved["new"]
            .iter()
            .any(|permission| permission.as_str() == "content"));
    }

    #[test]
    fn explicit_grants_reject_duplicate_sink_permission_and_escalation() {
        let manifest = grant_manifest(&[("sink", &["metadata", "paths", "content"])]);
        let duplicate_sink = [
            EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata"]),
            },
            EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata"]),
            },
        ];
        assert!(resolve_event_sink_grants(&manifest, None, Some(&duplicate_sink)).is_err());
        assert!(resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata", "metadata"]),
            }]),
        )
        .is_err());

        let oversized = "x".repeat(MAX_EVENT_SINK_ID_BYTES + 1);
        let error = resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: oversized.clone(),
                granted_permissions: ids(&["metadata"]),
            }]),
        )
        .unwrap_err()
        .to_string();
        assert!(!error.contains(&oversized));
        let oversized_permission = "p".repeat(MAX_EVENT_SINK_PERMISSION_ID_BYTES + 1);
        let error = resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: vec![ObservationPermissionId::new(
                    oversized_permission.clone(),
                )],
            }]),
        )
        .unwrap_err()
        .to_string();
        assert!(!error.contains(&oversized_permission));
        assert!(resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata", "tool_name"]),
            }]),
        )
        .is_err());
        let unknown_permission = "credential-value-must-not-enter-error";
        let error = resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata", unknown_permission]),
            }]),
        )
        .unwrap_err()
        .to_string();
        assert!(!error.contains(unknown_permission));
        assert!(resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "sink".to_string(),
                granted_permissions: ids(&["metadata", "content"]),
            }]),
        )
        .is_err());
        assert!(resolve_event_sink_grants(
            &manifest,
            None,
            Some(&[EventSinkGrantRequest {
                sink_id: "unknown".to_string(),
                granted_permissions: ids(&["metadata"]),
            }]),
        )
        .is_err());
    }

    #[test]
    fn boot_accepts_only_whole_map_legacy_or_exact_v1_authority() {
        let manifest =
            grant_manifest(&[("first", &["metadata", "paths"]), ("second", &["metadata"])]);
        let legacy = canonicalize_persisted_event_sink_grants(&manifest, &BTreeMap::new()).unwrap();
        assert_eq!(legacy.len(), 2);
        assert_eq!(legacy["first"], ids(&["metadata"]));

        let missing = BTreeMap::from([("first".to_string(), ids(&["metadata"]))]);
        assert!(canonicalize_persisted_event_sink_grants(&manifest, &missing).is_err());
        let unknown = BTreeMap::from([
            ("first".to_string(), ids(&["metadata"])),
            ("second".to_string(), ids(&["metadata"])),
            ("unknown".to_string(), ids(&["metadata"])),
        ]);
        assert!(canonicalize_persisted_event_sink_grants(&manifest, &unknown).is_err());
    }

    #[test]
    fn metadata_only_is_safe_and_unknown_fields_never_enter_projection() {
        let source = event("/workspace/src/lib.rs");
        let policy = ToolEventObservationPolicy::default();
        let projected =
            project_tool_event(&source, policy.grant_requested(&requested_all()), 1).unwrap();
        let wire = serde_json::to_string(&projected).unwrap();

        assert_eq!(projected.context.tool_name, None);
        assert_eq!(projected.data.path, None);
        assert_eq!(
            projected.data.path_redaction_reason.as_deref(),
            Some(PATH_REDACTION_PERMISSION_NOT_GRANTED)
        );
        for sentinel in [
            "diff-sentinel",
            "content-sentinel",
            "config-sentinel",
            "prompt-sentinel",
            "credential-sentinel",
            "envelope-prompt-sentinel",
            "context-credential-sentinel",
        ] {
            assert!(!wire.contains(sentinel), "leaked sentinel: {sentinel}");
        }
    }

    #[test]
    fn requested_and_granted_permissions_are_both_required() {
        let source = event("C:\\workspace\\src\\lib.rs");
        let policy = ToolEventObservationPolicy::all_v1();
        let requested = vec![
            ObservationPermissionId::new(OBSERVE_METADATA_PERMISSION),
            ObservationPermissionId::new(OBSERVE_PATHS_PERMISSION),
        ];
        let projected = project_tool_event(&source, policy.grant_requested(&requested), 7).unwrap();

        assert_eq!(projected.context.tool_name, None);
        assert_eq!(
            projected.data.path.as_deref(),
            Some("C:/workspace/src/lib.rs")
        );
        assert_eq!(projected.data.diff, None);
        assert_eq!(projected.data.content, None);
        assert_eq!(projected.observation_policy_generation, Some(7));
    }

    #[test]
    fn sensitive_unix_and_windows_paths_have_one_stable_reason_and_no_payload() {
        let policy = ToolEventObservationPolicy::all_v1();
        let grants = policy.grant_requested(&requested_all());
        for path in [
            "/home/alice/.ssh/id_ed25519",
            "/workspace/.env.production",
            "/Users/alice/.bamboo/config.json",
            "/workspace/.codex/config.toml",
            "/workspace/.claude/settings.json",
            "/workspace/.agents/skills/private.md",
            "/home/alice/.config/gh/hosts.yml",
            "/workspace/AGENTS.md",
            "/workspace/CLAUDE.md",
            r"C:\Users\alice\.aws\credentials",
            r"C:\Users\alice\AppData\Roaming\Codex\config.json",
            r"C:\Windows\System32\config\SAM",
            r"\\server\C$\Windows\System32\config\SAM",
            r"\\server\ADMIN$\System32\config\SAM",
            r"\\server\IPC$\pipe",
        ] {
            let projected = project_tool_event(&event(path), grants, 2).unwrap();
            assert_eq!(projected.data.path, None, "{path}");
            assert_eq!(
                projected.data.path_redaction_reason.as_deref(),
                Some(PATH_REDACTION_SENSITIVE),
                "{path}"
            );
            assert_eq!(projected.data.diff, None);
            assert_eq!(projected.data.content, None);
        }
    }

    #[test]
    fn traversal_relative_and_ambiguous_paths_fail_closed_without_io() {
        let policy = ToolEventObservationPolicy::all_v1();
        let grants = policy.grant_requested(&requested_all());
        for path in [
            "relative/file.rs",
            "/workspace/src/../secret.txt",
            r"C:relative\file.rs",
            r"C:\workspace\src\..\secret.txt",
            r"\\?\C:\workspace\file.rs",
            r"C:\workspace\file.txt:secret",
            "C:\\workspace\\.ssh \\id_rsa",
            r"C:\workspace\NUL",
            r"\\server\share\COM1.txt",
            r"\workspace\file.rs",
            r"\Device\HarddiskVolume1\workspace\file.rs",
            r"\??\C:\workspace\file.rs",
            "/??/C:/workspace/file.rs",
            "/Device/HarddiskVolume1/workspace/file.rs",
            "/GLOBAL??/C:/workspace/file.rs",
            r"\\server\.\share\file.rs",
            r"\\server\\share\file.rs",
            "//server-only",
        ] {
            let projected = project_tool_event(&event(path), grants, 2).unwrap();
            assert_eq!(projected.data.path, None, "{path}");
            assert_eq!(
                projected.data.path_redaction_reason.as_deref(),
                Some(PATH_REDACTION_UNSAFE),
                "{path}"
            );
            assert_eq!(projected.data.content, None);
        }
    }

    #[test]
    fn path_normalization_is_stable_across_supported_syntaxes() {
        assert_eq!(
            normalize_absolute_path("/workspace//src/./lib.rs"),
            Some("/workspace/src/lib.rs".to_string())
        );
        assert_eq!(
            normalize_absolute_path(r"c:\workspace\src\.\lib.rs"),
            Some("C:/workspace/src/lib.rs".to_string())
        );
        assert_eq!(
            normalize_absolute_path(r"\\server\share\dir\file.rs"),
            Some("//server/share/dir/file.rs".to_string())
        );
    }

    #[test]
    fn diff_and_content_truncate_on_utf8_boundaries_with_explicit_markers() {
        let mut source = event("/workspace/src/lib.rs");
        let data = source.data.as_object_mut().unwrap();
        data.insert(
            TOOL_EVENT_DIFF_FIELD.to_string(),
            json!("é".repeat(MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES)),
        );
        data.insert(
            TOOL_EVENT_CONTENT_FIELD.to_string(),
            json!("界".repeat(MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES)),
        );
        // Rebuild through serde so flattened data fields become extensions in
        // the same form as a producer-created event.
        let source: ToolEventV1 =
            serde_json::from_value(serde_json::to_value(source).unwrap()).unwrap();
        let policy = ToolEventObservationPolicy::all_v1();
        let projected =
            project_tool_event(&source, policy.grant_requested(&requested_all()), 4).unwrap();

        let diff = projected.data.diff.as_deref().unwrap();
        let content = projected.data.content.as_deref().unwrap();
        assert!(diff.len() <= MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES);
        assert!(content.len() <= MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES);
        assert!(diff.ends_with(TRUNCATION_MARKER));
        assert!(content.ends_with(TRUNCATION_MARKER));
        assert!(projected.data.diff_truncated);
        assert!(projected.data.content_truncated);
    }

    #[test]
    fn malformed_authorized_payload_is_rejected_but_unauthorized_payload_is_never_read() {
        for field in [TOOL_EVENT_DIFF_FIELD, TOOL_EVENT_CONTENT_FIELD] {
            let mut source = event("/workspace/src/lib.rs");
            source.data.as_object_mut().unwrap().insert(
                field.to_string(),
                json!({"secret": "malformed-payload-sentinel"}),
            );

            let metadata_only = ToolEventObservationPolicy::default();
            let projected =
                project_tool_event(&source, metadata_only.grant_requested(&requested_all()), 9)
                    .expect("an unauthorized extension is omitted without inspecting its value");
            assert!(!serde_json::to_string(&projected)
                .unwrap()
                .contains("malformed-payload-sentinel"));

            let error = project_tool_event(
                &source,
                ToolEventObservationPolicy::all_v1().grant_requested(&requested_all()),
                9,
            )
            .expect_err("an authorized known field must retain its string contract");
            assert!(matches!(error, ToolEventBuildError::InvalidKnownPayload(_)));
            assert!(!error.to_string().contains("malformed-payload-sentinel"));
        }
    }
}
