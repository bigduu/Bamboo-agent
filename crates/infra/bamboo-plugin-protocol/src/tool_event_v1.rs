use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

/// Wire schema carried by every [`ToolEventV1`].
pub const TOOL_EVENT_V1_SCHEMA_VERSION: u16 = 1;
/// Stable event-type identifier for a successful supported file mutation.
pub const FILE_CHANGED_EVENT_TYPE_V1: &str = "file_changed";
/// Stable manifest subscription identifier for [`FILE_CHANGED_EVENT_TYPE_V1`].
pub const FILE_CHANGED_SUBSCRIPTION_ID_V1: &str = "tool.file_changed.v1";

// Bounds apply to UTF-8 wire bytes, not Unicode scalar counts. Rejecting an
// oversize event is preferable to silently changing an authority-bearing id or
// path; projection/truncation policy belongs to the later policy layer.
pub const MAX_TOOL_EVENT_SESSION_ID_BYTES: usize = 256;
pub const MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES: usize = 256;
pub const MAX_TOOL_EVENT_TYPE_BYTES: usize = 128;
pub const MAX_TOOL_EVENT_SUBSCRIPTION_ID_BYTES: usize = 128;
pub const MAX_TOOL_EVENT_TOOL_NAME_BYTES: usize = 128;
pub const MAX_TOOL_EVENT_CALL_ID_BYTES: usize = 256;
pub const MAX_TOOL_EVENT_PATH_BYTES: usize = 4096;
pub const MAX_TOOL_EVENT_JSON_BYTES: usize = 16 * 1024;

/// Forward-compatible event type. Unknown strings are retained verbatim.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolEventTypeV1(String);

impl ToolEventTypeV1 {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn file_changed() -> Self {
        Self(FILE_CHANGED_EVENT_TYPE_V1.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_file_changed(&self) -> bool {
        self.0 == FILE_CHANGED_EVENT_TYPE_V1
    }
}

/// Forward-compatible subscription identifier used by plugin manifests.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolEventSubscriptionId(String);

impl ToolEventSubscriptionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn file_changed_v1() -> Self {
        Self(FILE_CHANGED_SUBSCRIPTION_ID_V1.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_file_changed_v1(&self) -> bool {
        self.0 == FILE_CHANGED_SUBSCRIPTION_ID_V1
    }
}

/// Stable identity attached to every tool event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolEventContextV1 {
    pub session_id: String,
    /// Authoritative root-session identity for the executing session tree.
    pub root_session_id: String,
    /// Stable canonical tool name (for example `Edit`, even for `apply_patch`).
    pub tool_name: String,
    /// Original model-provided tool-call identifier.
    pub tool_call_id: String,
    /// Additive context fields added by a future compatible producer.
    #[serde(default, flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl ToolEventContextV1 {
    /// Validate borrowed fields before allocating owned wire DTO strings.
    pub fn bounded_from(
        session_id: &str,
        root_session_id: &str,
        tool_name: &str,
        tool_call_id: &str,
    ) -> Result<Self, ToolEventBuildError> {
        validate_required(
            "context.session_id",
            session_id,
            MAX_TOOL_EVENT_SESSION_ID_BYTES,
        )?;
        validate_required(
            "context.root_session_id",
            root_session_id,
            MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
        )?;
        validate_required(
            "context.tool_name",
            tool_name,
            MAX_TOOL_EVENT_TOOL_NAME_BYTES,
        )?;
        validate_required(
            "context.tool_call_id",
            tool_call_id,
            MAX_TOOL_EVENT_CALL_ID_BYTES,
        )?;
        Ok(Self {
            session_id: session_id.to_string(),
            root_session_id: root_session_id.to_string(),
            tool_name: tool_name.to_string(),
            tool_call_id: tool_call_id.to_string(),
            extensions: BTreeMap::new(),
        })
    }

    pub fn bounded(
        session_id: impl Into<String>,
        root_session_id: impl Into<String>,
        tool_name: impl Into<String>,
        tool_call_id: impl Into<String>,
    ) -> Result<Self, ToolEventBuildError> {
        let context = Self {
            session_id: session_id.into(),
            root_session_id: root_session_id.into(),
            tool_name: tool_name.into(),
            tool_call_id: tool_call_id.into(),
            extensions: BTreeMap::new(),
        };
        context.validate_bounds()?;
        Ok(context)
    }

    fn validate_bounds(&self) -> Result<(), ToolEventBuildError> {
        validate_extension_keys(
            "context",
            &self.extensions,
            &["session_id", "root_session_id", "tool_name", "tool_call_id"],
        )?;
        validate_required(
            "context.session_id",
            &self.session_id,
            MAX_TOOL_EVENT_SESSION_ID_BYTES,
        )?;
        validate_required(
            "context.root_session_id",
            &self.root_session_id,
            MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
        )?;
        validate_required(
            "context.tool_name",
            &self.tool_name,
            MAX_TOOL_EVENT_TOOL_NAME_BYTES,
        )?;
        validate_required(
            "context.tool_call_id",
            &self.tool_call_id,
            MAX_TOOL_EVENT_CALL_ID_BYTES,
        )
    }
}

/// Stable payload for a successful `file_changed` event.
///
/// It intentionally contains no content, diff, permission, or redaction data.
/// Those projections are owned by later policy/routing layers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChangedV1 {
    pub path: String,
    /// Additive payload fields from a future compatible producer.
    #[serde(default, flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl FileChangedV1 {
    /// Validate a borrowed path before allocating its owned wire representation.
    pub fn bounded_from(path: &str) -> Result<Self, ToolEventBuildError> {
        validate_required("data.path", path, MAX_TOOL_EVENT_PATH_BYTES)?;
        Ok(Self {
            path: path.to_string(),
            extensions: BTreeMap::new(),
        })
    }

    pub fn bounded(path: impl Into<String>) -> Result<Self, ToolEventBuildError> {
        let data = Self {
            path: path.into(),
            extensions: BTreeMap::new(),
        };
        data.validate_bounds()?;
        Ok(data)
    }

    fn validate_bounds(&self) -> Result<(), ToolEventBuildError> {
        validate_extension_keys("data", &self.extensions, &["path"])?;
        validate_required("data.path", &self.path, MAX_TOOL_EVENT_PATH_BYTES)
    }
}

/// Versioned, forward-compatible event envelope delivered to plugin sinks.
///
/// `event_type` and `subscription_id` are open string types and `data` is kept
/// as JSON on the envelope, so an older host can deserialize and forward an
/// unknown compatible variant without discarding its payload. Known consumers
/// use [`ToolEventV1::file_changed_data`] for the stable typed DTO.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolEventV1 {
    pub schema_version: u16,
    pub event_type: ToolEventTypeV1,
    pub subscription_id: ToolEventSubscriptionId,
    pub context: ToolEventContextV1,
    pub data: Value,
    /// Additive envelope fields from a future compatible producer.
    #[serde(default, flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl ToolEventV1 {
    pub fn file_changed(
        context: ToolEventContextV1,
        data: FileChangedV1,
    ) -> Result<Self, ToolEventBuildError> {
        // Validate before flatten serialization so a programmatically supplied
        // extension cannot create a duplicate reserved JSON key.
        context.validate_bounds()?;
        data.validate_bounds()?;
        let event = Self {
            schema_version: TOOL_EVENT_V1_SCHEMA_VERSION,
            event_type: ToolEventTypeV1::file_changed(),
            subscription_id: ToolEventSubscriptionId::file_changed_v1(),
            context,
            data: serde_json::to_value(data)
                .map_err(|error| ToolEventBuildError::Serialization(error.to_string()))?,
            extensions: BTreeMap::new(),
        };
        event.validate_bounds()?;
        Ok(event)
    }

    /// Decode the stable payload only when this is the known v1 variant.
    /// Unknown variants remain available through the public `data` field.
    pub fn file_changed_data(&self) -> Option<Result<FileChangedV1, serde_json::Error>> {
        (self.schema_version == TOOL_EVENT_V1_SCHEMA_VERSION
            && self.event_type.is_file_changed()
            && self.subscription_id.is_file_changed_v1())
        .then(|| serde_json::from_value(self.data.clone()))
    }

    /// Revalidate an event before crossing a bounded publisher boundary.
    pub fn validate_bounds(&self) -> Result<(), ToolEventBuildError> {
        if self.schema_version != TOOL_EVENT_V1_SCHEMA_VERSION {
            return Err(ToolEventBuildError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                supported: TOOL_EVENT_V1_SCHEMA_VERSION,
            });
        }
        validate_extension_keys(
            "envelope",
            &self.extensions,
            &[
                "schema_version",
                "event_type",
                "subscription_id",
                "context",
                "data",
            ],
        )?;
        self.context.validate_bounds()?;
        validate_required(
            "event_type",
            self.event_type.as_str(),
            MAX_TOOL_EVENT_TYPE_BYTES,
        )?;
        validate_required(
            "subscription_id",
            self.subscription_id.as_str(),
            MAX_TOOL_EVENT_SUBSCRIPTION_ID_BYTES,
        )?;

        if !self.data.is_object() {
            return Err(ToolEventBuildError::DataMustBeObject);
        }

        let event_known = self.event_type.is_file_changed();
        let subscription_known = self.subscription_id.is_file_changed_v1();
        if event_known != subscription_known {
            return Err(ToolEventBuildError::KnownVariantMismatch);
        }
        if event_known {
            let data: FileChangedV1 = serde_json::from_value(self.data.clone())
                .map_err(|error| ToolEventBuildError::InvalidKnownPayload(error.to_string()))?;
            data.validate_bounds()?;
        }

        let actual = serde_json::to_vec(self)
            .map_err(|error| ToolEventBuildError::Serialization(error.to_string()))?
            .len();
        if actual > MAX_TOOL_EVENT_JSON_BYTES {
            return Err(ToolEventBuildError::EventTooLarge {
                actual,
                max: MAX_TOOL_EVENT_JSON_BYTES,
            });
        }
        Ok(())
    }
}

fn validate_required(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ToolEventBuildError> {
    if value.trim().is_empty() {
        return Err(ToolEventBuildError::EmptyField { field });
    }
    let actual = value.len();
    if actual > max {
        return Err(ToolEventBuildError::FieldTooLarge { field, actual, max });
    }
    Ok(())
}

fn validate_extension_keys(
    location: &'static str,
    extensions: &BTreeMap<String, Value>,
    reserved: &[&str],
) -> Result<(), ToolEventBuildError> {
    if let Some(key) = reserved.iter().find(|key| extensions.contains_key(**key)) {
        return Err(ToolEventBuildError::ReservedExtensionKey {
            location,
            key: (*key).to_string(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolEventBuildError {
    #[error("tool event field `{field}` must not be empty")]
    EmptyField { field: &'static str },
    #[error("tool event field `{field}` is {actual} bytes; maximum is {max}")]
    FieldTooLarge {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    #[error("tool event is {actual} bytes; maximum is {max}")]
    EventTooLarge { actual: usize, max: usize },
    #[error("tool event data must be a JSON object")]
    DataMustBeObject,
    #[error("unsupported tool event schema version {actual}; supported version is {supported}")]
    UnsupportedSchemaVersion { actual: u16, supported: u16 },
    #[error("file_changed event type and subscription id must be paired")]
    KnownVariantMismatch,
    #[error("invalid known tool event payload: {0}")]
    InvalidKnownPayload(String),
    #[error("tool event {location} extension conflicts with reserved key `{key}`")]
    ReservedExtensionKey { location: &'static str, key: String },
    #[error("failed to serialize tool event: {0}")]
    Serialization(String),
}

/// Public JSON Schema for the v1 wire envelope.
///
/// The discriminants are deliberately open strings. Known `file_changed`
/// payloads are typed by the `if/then` branch while unknown compatible variants
/// retain arbitrary object data and additive fields.
pub fn tool_event_v1_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://bamboo.dev/schemas/plugin/tool-event-v1.schema.json",
        "title": "ToolEventV1",
        "type": "object",
        "x-bamboo-maxJsonBytes": MAX_TOOL_EVENT_JSON_BYTES,
        "required": ["schema_version", "event_type", "subscription_id", "context", "data"],
        "properties": {
            "schema_version": { "const": TOOL_EVENT_V1_SCHEMA_VERSION },
            "event_type": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_TYPE_BYTES },
            "subscription_id": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_SUBSCRIPTION_ID_BYTES },
            "context": {
                "type": "object",
                "required": ["session_id", "root_session_id", "tool_name", "tool_call_id"],
                "properties": {
                    "session_id": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_SESSION_ID_BYTES },
                    "root_session_id": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES },
                    "tool_name": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_TOOL_NAME_BYTES },
                    "tool_call_id": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_CALL_ID_BYTES }
                },
                "additionalProperties": true
            },
            "data": { "type": "object" }
        },
        "allOf": [{
            "if": {
                "properties": {
                    "event_type": { "const": FILE_CHANGED_EVENT_TYPE_V1 }
                },
                "required": ["event_type"]
            },
            "then": {
                "properties": {
                    "subscription_id": { "const": FILE_CHANGED_SUBSCRIPTION_ID_V1 },
                    "data": {
                        "type": "object",
                        "required": ["path"],
                        "properties": {
                            "path": { "type": "string", "minLength": 1, "x-bamboo-maxUtf8Bytes": MAX_TOOL_EVENT_PATH_BYTES }
                        },
                        "additionalProperties": true
                    }
                }
            }
        }, {
            "if": {
                "properties": {
                    "subscription_id": { "const": FILE_CHANGED_SUBSCRIPTION_ID_V1 }
                },
                "required": ["subscription_id"]
            },
            "then": {
                "properties": {
                    "event_type": { "const": FILE_CHANGED_EVENT_TYPE_V1 }
                }
            }
        }],
        "additionalProperties": true
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_event() -> ToolEventV1 {
        ToolEventV1::file_changed(
            ToolEventContextV1::bounded("session-1", "root-session-1", "Write", "call-1").unwrap(),
            FileChangedV1::bounded("/workspace/zenith/src/lib.rs").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn file_changed_wire_json_matches_golden() {
        let json = serde_json::to_string(&fixture_event()).unwrap();
        assert_eq!(
            json,
            include_str!("../tests/golden/tool_event_v1.file_changed.json").trim()
        );
    }

    #[test]
    fn schema_matches_golden() {
        let expected: Value =
            serde_json::from_str(include_str!("../tests/golden/tool_event_v1.schema.json"))
                .unwrap();
        assert_eq!(tool_event_v1_schema(), expected);
    }

    #[test]
    fn golden_event_validates_against_public_schema() {
        let schema = tool_event_v1_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/golden/tool_event_v1.file_changed.json"
        ))
        .unwrap();
        assert!(validator.validate(&fixture).is_ok());

        let mut mismatched = fixture;
        mismatched["subscription_id"] = json!("tool.future.v1");
        assert!(validator.validate(&mismatched).is_err());
    }

    #[test]
    fn unknown_compatible_variant_and_extensions_round_trip() {
        let raw = json!({
            "schema_version": 1,
            "event_type": "future_event",
            "subscription_id": "tool.future_event.v1",
            "context": {
                "session_id": "session-1",
                "root_session_id": "root-session-1",
                "tool_name": "FutureTool",
                "tool_call_id": "call-future",
                "trace_hint": "kept"
            },
            "data": { "future": [1, 2, 3] },
            "producer_hint": { "also": "kept" }
        });
        let decoded: ToolEventV1 = serde_json::from_value(raw.clone()).unwrap();

        assert_eq!(decoded.event_type.as_str(), "future_event");
        assert!(decoded.file_changed_data().is_none());
        assert_eq!(serde_json::to_value(decoded).unwrap(), raw);
    }

    #[test]
    fn required_and_size_bounds_are_explicit() {
        assert_eq!(
            ToolEventContextV1::bounded("", "root-session", "Write", "call").unwrap_err(),
            ToolEventBuildError::EmptyField {
                field: "context.session_id"
            }
        );
        assert!(matches!(
            FileChangedV1::bounded("x".repeat(MAX_TOOL_EVENT_PATH_BYTES + 1)),
            Err(ToolEventBuildError::FieldTooLarge {
                field: "data.path",
                ..
            })
        ));
    }

    #[test]
    fn string_bounds_count_exact_utf8_bytes() {
        let exact = "é".repeat(MAX_TOOL_EVENT_PATH_BYTES / "é".len());
        assert_eq!(exact.len(), MAX_TOOL_EVENT_PATH_BYTES);
        assert!(FileChangedV1::bounded(exact).is_ok());

        let over = "é".repeat(MAX_TOOL_EVENT_PATH_BYTES / "é".len() + 1);
        assert_eq!(
            FileChangedV1::bounded(over),
            Err(ToolEventBuildError::FieldTooLarge {
                field: "data.path",
                actual: MAX_TOOL_EVENT_PATH_BYTES + "é".len(),
                max: MAX_TOOL_EVENT_PATH_BYTES,
            })
        );

        let schema = tool_event_v1_schema();
        assert_eq!(schema["properties"]["data"]["type"], json!("object"));
        assert_eq!(
            schema["allOf"][0]["then"]["properties"]["data"]["properties"]["path"]
                ["x-bamboo-maxUtf8Bytes"],
            json!(MAX_TOOL_EVENT_PATH_BYTES)
        );
        assert!(
            schema["allOf"][0]["then"]["properties"]["data"]["properties"]["path"]
                .get("maxLength")
                .is_none()
        );
    }

    #[test]
    fn total_wire_json_bound_is_enforced_after_field_bounds() {
        let mut event = fixture_event();
        event.extensions.insert(
            "future_payload".to_string(),
            json!("x".repeat(MAX_TOOL_EVENT_JSON_BYTES)),
        );

        assert!(matches!(
            event.validate_bounds(),
            Err(ToolEventBuildError::EventTooLarge {
                actual,
                max: MAX_TOOL_EVENT_JSON_BYTES,
            }) if actual > MAX_TOOL_EVENT_JSON_BYTES
        ));
    }

    #[test]
    fn all_variants_require_object_data_and_known_identifiers_stay_paired() {
        let mut event = fixture_event();
        event.data = json!("not-an-object");
        assert_eq!(
            event.validate_bounds(),
            Err(ToolEventBuildError::DataMustBeObject)
        );

        let mut event = fixture_event();
        event.subscription_id = ToolEventSubscriptionId::new("tool.future.v1");
        assert_eq!(
            event.validate_bounds(),
            Err(ToolEventBuildError::KnownVariantMismatch)
        );

        let mut event = fixture_event();
        event.event_type = ToolEventTypeV1::new("future_event");
        assert_eq!(
            event.validate_bounds(),
            Err(ToolEventBuildError::KnownVariantMismatch)
        );

        let mut event = fixture_event();
        event.schema_version = TOOL_EVENT_V1_SCHEMA_VERSION + 1;
        assert_eq!(
            event.validate_bounds(),
            Err(ToolEventBuildError::UnsupportedSchemaVersion {
                actual: TOOL_EVENT_V1_SCHEMA_VERSION + 1,
                supported: TOOL_EVENT_V1_SCHEMA_VERSION,
            })
        );
    }

    #[test]
    fn flattened_extensions_cannot_shadow_reserved_wire_fields() {
        let mut context = fixture_event().context;
        context
            .extensions
            .insert("session_id".to_string(), json!("spoofed-session"));
        assert_eq!(
            context.validate_bounds(),
            Err(ToolEventBuildError::ReservedExtensionKey {
                location: "context",
                key: "session_id".to_string(),
            })
        );

        let mut data = FileChangedV1::bounded("/bounded/file.txt").unwrap();
        data.extensions
            .insert("path".to_string(), json!("/spoofed/file.txt"));
        assert_eq!(
            data.validate_bounds(),
            Err(ToolEventBuildError::ReservedExtensionKey {
                location: "data",
                key: "path".to_string(),
            })
        );

        let mut event = fixture_event();
        event
            .extensions
            .insert("schema_version".to_string(), json!(999));
        assert_eq!(
            event.validate_bounds(),
            Err(ToolEventBuildError::ReservedExtensionKey {
                location: "envelope",
                key: "schema_version".to_string(),
            })
        );
    }
}
