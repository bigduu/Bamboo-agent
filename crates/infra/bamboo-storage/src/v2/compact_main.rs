//! Main-only framing. This full-buffer compatibility pass is not a compact
//! authority grant or proof of any skipped message/native payload semantics.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::io::{self, Write};

use bamboo_domain::{
    RootModeOperationReceipt, Session, SessionAuthorityIdentity, SessionKind,
    SupervisorManagementState, ROOT_MODE_OPERATION_HISTORY_LIMIT,
};
use chrono::{DateTime, Utc};
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{value::RawValue, Value};

pub(super) const MEMBER: &str = "_bamboo_main_authority";
pub(super) const SECTION_CAP: usize = 512 * 1024;
pub(super) const PREFIX: &[u8] = b"{\"_bamboo_main_authority\":{\"version\":1,\"payload_bytes\":\"";
const MIDDLE: &[u8] = b"\",\"payload\":";
const CLOSE: &[u8] = b"},";
pub(super) const HEADER_BYTES: usize = PREFIX.len() + 10 + MIDDLE.len();
const FRAME_BYTES: usize = HEADER_BYTES + CLOSE.len();

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid compact Main authority")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompactMainAuthority {
    id: String,
    created_at: DateTime<Utc>,
    kind: SessionKind,
    parent_session_id: Option<String>,
    root_session_id: String,
    spawn_depth: u32,
    authority_identity: SessionAuthorityIdentity,
    metadata_version: u64,
    project_id: Option<String>,
    root_orchestration_only: bool,
    root_tool_authority_revision: u64,
    root_mode_transition_epoch: u64,
    root_mode_operations: Vec<RootModeOperationReceipt>,
    supervisor_management: Option<SupervisorManagementState>,
    title_label: String,
}

pub(super) fn public_title(title: &str) -> String {
    title
        .chars()
        .filter(|c| {
            !c.is_control() && !matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        .take(160)
        .collect()
}

impl CompactMainAuthority {
    fn from_session(session: &Session) -> Self {
        Self {
            id: session.id.clone(),
            created_at: session.created_at,
            kind: session.kind,
            parent_session_id: session.parent_session_id.clone(),
            root_session_id: session.root_session_id.clone(),
            spawn_depth: session.spawn_depth,
            authority_identity: session.authority_identity.clone(),
            metadata_version: session.metadata_version,
            project_id: session.project_id_meta(),
            root_orchestration_only: session.root_orchestration_only,
            root_tool_authority_revision: session.root_tool_authority_revision,
            root_mode_transition_epoch: session.root_mode_transition_epoch,
            root_mode_operations: session.root_mode_operations.clone(),
            supervisor_management: session.supervisor_management.clone(),
            title_label: public_title(&session.title),
        }
    }

    /// Only the observed public-graph identity; no skipped flat-body grant.
    pub(super) fn into_snapshot_session(self) -> Session {
        let mut session = Session::new(self.id, "");
        session.created_at = self.created_at;
        session.title = self.title_label;
        session.kind = self.kind;
        session.parent_session_id = self.parent_session_id;
        session.root_session_id = self.root_session_id;
        session.spawn_depth = self.spawn_depth;
        session.authority_identity = self.authority_identity;
        session.metadata_version = self.metadata_version;
        session.root_orchestration_only = self.root_orchestration_only;
        session.root_tool_authority_revision = self.root_tool_authority_revision;
        session.root_mode_transition_epoch = self.root_mode_transition_epoch;
        session.root_mode_operations = self.root_mode_operations;
        session.supervisor_management = self.supervisor_management;
        if let Some(project) = self.project_id {
            session.set_project_id_meta(project);
        }
        session
    }

    fn validate(&self) -> io::Result<()> {
        if self.title_label != public_title(&self.title_label)
            || self.root_mode_operations.len() > ROOT_MODE_OPERATION_HISTORY_LIMIT
        {
            return Err(invalid());
        }
        if let Some(management) = &self.supervisor_management {
            let SessionAuthorityIdentity::Supervisor { incarnation_id } = self.authority_identity
            else {
                return Err(invalid());
            };
            management.validate(incarnation_id).map_err(|_| invalid())?;
        }
        Ok(())
    }
}

struct BoundedPayload(Vec<u8>);
impl Write for BoundedPayload {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let len = self.0.len().checked_add(bytes.len()).ok_or_else(invalid)?;
        if len > SECTION_CAP - FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compact Main authority exceeds 512 KiB section budget",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Prepare before the named publisher starts its new state mutations. Runtime
/// encoders remain separate, and no caller-provided header is trusted.
pub(super) fn serialize_main(session: &Session) -> io::Result<Vec<u8>> {
    let authority = CompactMainAuthority::from_session(session);
    authority.validate()?;
    let mut payload = BoundedPayload(Vec::new());
    serde_json::to_writer(&mut payload, &authority).map_err(io::Error::other)?;
    let section_len = FRAME_BYTES
        .checked_add(payload.0.len())
        .ok_or_else(invalid)?;
    if section_len > SECTION_CAP {
        return Err(invalid());
    }
    let flat = serde_json::to_vec_pretty(session).map_err(io::Error::other)?;
    let suffix = flat.strip_prefix(b"{").ok_or_else(invalid)?;
    let capacity = section_len.checked_add(suffix.len()).ok_or_else(invalid)?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(PREFIX);
    output.extend_from_slice(format!("{:010}", payload.0.len()).as_bytes());
    output.extend_from_slice(MIDDLE);
    output.extend_from_slice(&payload.0);
    output.extend_from_slice(CLOSE);
    output.extend_from_slice(suffix);
    Ok(output)
}

/// Checked framing shared by full-buffer compatibility and retained-FD readers.
/// This observes only the fixed header; callers enforce their own section budget.
pub(super) fn section_length(header: &[u8]) -> io::Result<usize> {
    if !header.starts_with(PREFIX) {
        return Err(invalid());
    }
    let digits_end = PREFIX.len().checked_add(10).ok_or_else(invalid)?;
    let digits = header.get(PREFIX.len()..digits_end).ok_or_else(invalid)?;
    let mut length = 0usize;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return Err(invalid());
        }
        length = length
            .checked_mul(10)
            .and_then(|n| n.checked_add((digit - b'0') as usize))
            .ok_or_else(invalid)?;
    }
    if header.get(digits_end..HEADER_BYTES) != Some(MIDDLE) {
        return Err(invalid());
    }
    FRAME_BYTES.checked_add(length).ok_or_else(invalid)
}

/// Proves only the observed section, never the unseen flat suffix.
pub(super) fn decode_v1_section(bytes: &[u8], cap: usize) -> io::Result<CompactMainAuthority> {
    let section_end = section_length(bytes)?;
    if section_end > cap.min(SECTION_CAP)
        || bytes.get(section_end - CLOSE.len()..section_end) != Some(CLOSE)
    {
        return Err(invalid());
    }
    let raw = bytes
        .get(HEADER_BYTES..section_end - CLOSE.len())
        .ok_or_else(invalid)?;
    let value: UniqueValue = serde_json::from_slice(raw).map_err(|_| invalid())?;
    let authority: CompactMainAuthority =
        serde_json::from_value(value.0.clone()).map_err(|_| invalid())?;
    // Presence of nullable members, unknown nested fields, collapsed duplicate
    // sets and noncanonical numeric shapes cannot disappear through typed serde.
    if serde_json::to_value(&authority).map_err(|_| invalid())? != value.0 {
        return Err(invalid());
    }
    authority.validate()?;
    Ok(authority)
}

/// Preserve legacy absence. Present malformed/moved/escaped/duplicate sections
/// reject before any original reader uses authority or publishes a projection.
pub(super) fn validate_full_main(bytes: &[u8]) -> io::Result<Option<CompactMainAuthority>> {
    let fields: MainFields<'_> = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if !fields.values.contains_key(MEMBER) {
        return Ok(None);
    }
    if fields.duplicate {
        return Err(invalid());
    }
    let authority = decode_v1_section(bytes, SECTION_CAP)?;
    let flat = FlatAuthority::from_fields(&fields.values)?;
    if authority != flat {
        return Err(invalid());
    }
    Ok(Some(authority))
}

// Raw borrowed values keep private messages/native/context out of allocations.
struct MainFields<'a> {
    values: BTreeMap<String, &'a RawValue>,
    duplicate: bool,
}
impl<'de> Deserialize<'de> for MainFields<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = MainFields<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a Main object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = MainFields {
                    values: BTreeMap::new(),
                    duplicate: false,
                };
                while let Some(key) = map.next_key::<String>()? {
                    if matches!(
                        key.as_str(),
                        MEMBER
                            | "id"
                            | "created_at"
                            | "kind"
                            | "parent_session_id"
                            | "root_session_id"
                            | "spawn_depth"
                            | "authority_identity"
                            | "metadata_version"
                            | "root_orchestration_only"
                            | "root_tool_authority_revision"
                            | "root_mode_transition_epoch"
                            | "root_mode_operations"
                            | "supervisor_management"
                            | "title"
                            | "metadata"
                            | "runtime_metadata"
                    ) {
                        fields.duplicate |= fields.values.insert(key, map.next_value()?).is_some();
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(fields)
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

#[derive(Default, Deserialize)]
struct ProjectMetadata {
    #[serde(default)]
    project_id: Option<String>,
}

/// Census-only ordinary identity and raw Project, without compatibility trimming.
pub(super) fn census_project(bytes: &[u8]) -> io::Result<Option<bamboo_domain::ProjectId>> {
    let fields: MainFields<'_> = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if fields.duplicate {
        return Err(invalid());
    }
    if let Some(raw) = fields.values.get("authority_identity") {
        let identity: UniqueValue = serde_json::from_str(raw.get()).map_err(|_| invalid())?;
        if identity.0 != serde_json::json!({"kind":"ordinary"}) {
            return Err(invalid());
        }
    }
    let metadata: ProjectMetadata = fields
        .values
        .get("metadata")
        .map(|value| serde_json::from_str::<ProjectMetadata>(value.get()).map_err(|_| invalid()))
        .transpose()?
        .unwrap_or_default();
    let runtime: Option<ProjectMetadata> = fields
        .values
        .get("runtime_metadata")
        .map(|value| {
            serde_json::from_str::<Option<ProjectMetadata>>(value.get()).map_err(|_| invalid())
        })
        .transpose()?
        .flatten();
    runtime
        .and_then(|value| value.project_id)
        .or(metadata.project_id)
        .map(|raw| {
            let project = bamboo_domain::ProjectId::parse(raw.clone()).map_err(|_| invalid())?;
            if project.as_str() != raw {
                return Err(invalid());
            }
            Ok(project)
        })
        .transpose()
}
struct FlatAuthority;
impl FlatAuthority {
    fn from_fields(fields: &BTreeMap<String, &RawValue>) -> io::Result<CompactMainAuthority> {
        fn get<T: serde::de::DeserializeOwned>(
            fields: &BTreeMap<String, &RawValue>,
            key: &str,
        ) -> io::Result<T> {
            serde_json::from_str(fields.get(key).ok_or_else(invalid)?.get()).map_err(|_| invalid())
        }
        fn default<T: serde::de::DeserializeOwned + Default>(
            fields: &BTreeMap<String, &RawValue>,
            key: &str,
        ) -> io::Result<T> {
            fields
                .get(key)
                .map(|raw| serde_json::from_str(raw.get()).map_err(|_| invalid()))
                .unwrap_or_else(|| Ok(T::default()))
        }
        let metadata: ProjectMetadata = default(fields, "metadata")?;
        let runtime: Option<ProjectMetadata> = default(fields, "runtime_metadata")?;
        Ok(CompactMainAuthority {
            id: get(fields, "id")?,
            created_at: get(fields, "created_at")?,
            kind: default(fields, "kind")?,
            parent_session_id: default(fields, "parent_session_id")?,
            root_session_id: default(fields, "root_session_id")?,
            spawn_depth: default(fields, "spawn_depth")?,
            authority_identity: get_or_unique_default(fields, "authority_identity")?,
            metadata_version: default(fields, "metadata_version")?,
            project_id: runtime
                .and_then(|meta| meta.project_id)
                .or(metadata.project_id),
            root_orchestration_only: default(fields, "root_orchestration_only")?,
            root_tool_authority_revision: default(fields, "root_tool_authority_revision")?,
            root_mode_transition_epoch: default(fields, "root_mode_transition_epoch")?,
            root_mode_operations: get_or_unique_default(fields, "root_mode_operations")?,
            supervisor_management: get_or_unique_default(fields, "supervisor_management")?,
            title_label: public_title(&default::<String>(fields, "title")?),
        })
    }
}

fn get_or_unique_default<T: serde::de::DeserializeOwned + Serialize + Default>(
    fields: &BTreeMap<String, &RawValue>,
    key: &str,
) -> io::Result<T> {
    let Some(raw) = fields.get(key) else {
        return Ok(T::default());
    };
    let value: UniqueValue = serde_json::from_str(raw.get()).map_err(|_| invalid())?;
    let typed: T = serde_json::from_value(value.0.clone()).map_err(|_| invalid())?;
    if let Some(projects) = value.0.get("allowed_projects").and_then(Value::as_array) {
        let encoded = serde_json::to_value(&typed).map_err(|_| invalid())?;
        if encoded
            .get("allowed_projects")
            .and_then(Value::as_array)
            .map(Vec::len)
            != Some(projects.len())
        {
            return Err(invalid());
        }
    }
    Ok(typed)
}

// Closed bounded payload parser: duplicate objects are rejected before a Map,
// BTreeSet or BTreeMap can erase evidence. Numbers retain serde_json shape.
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("unique JSON")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut seen = HashSet::new();
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate authority member"));
                    }
                    values.insert(key, map.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(values)))
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(v.into())))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(v.into())))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v)))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}
