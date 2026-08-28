use serde::{Deserialize, Serialize};

use crate::{
    ToolEventBuildError, ToolEventSubscriptionId, ToolEventTypeV1, MAX_TOOL_EVENT_CALL_ID_BYTES,
    MAX_TOOL_EVENT_JSON_BYTES, MAX_TOOL_EVENT_PATH_BYTES, MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
    MAX_TOOL_EVENT_SESSION_ID_BYTES, MAX_TOOL_EVENT_TOOL_NAME_BYTES, TOOL_EVENT_V1_SCHEMA_VERSION,
};

/// Per-field payload bounds applied by the host projection before it performs
/// the exact complete-event serialization/limit check.
pub const MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES: usize = 4 * 1024;
pub const MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES: usize = 8 * 1024;
pub const TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED: &str = "permission_not_granted";
pub const TOOL_EVENT_PATH_REDACTION_SENSITIVE: &str = "sensitive_path";
pub const TOOL_EVENT_PATH_REDACTION_UNSAFE: &str = "unsafe_path";

/// Permission-aware context delivered to plugin services. Metadata is the
/// safe baseline; `tool_name` is absent unless both requested and host-granted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedToolEventContextV1 {
    pub session_id: String,
    pub root_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub tool_call_id: String,
}

/// Permission-aware `file_changed` data. An absent/redacted path never leaves
/// a placeholder path field; the stable reason is safe metadata. Diff/content
/// are bounded strings and carry an explicit truncation bit when shortened.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedFileChangedV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_redaction_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub diff_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub content_truncated: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Irreversible host projection admitted to a plugin sink queue. This is
/// intentionally a distinct type from producer [`crate::ToolEventV1`], making
/// raw-event queueing a type error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedToolEventV1 {
    pub schema_version: u16,
    pub event_type: ToolEventTypeV1,
    pub subscription_id: ToolEventSubscriptionId,
    pub context: ProjectedToolEventContextV1,
    pub data: ProjectedFileChangedV1,
    /// Present on host-projected delivery. Optional during deserialization so
    /// the original full-observation v1 golden remains backward compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_policy_generation: Option<u64>,
}

impl ProjectedToolEventV1 {
    pub fn file_changed(
        context: ProjectedToolEventContextV1,
        data: ProjectedFileChangedV1,
        observation_policy_generation: u64,
    ) -> Self {
        Self {
            schema_version: TOOL_EVENT_V1_SCHEMA_VERSION,
            event_type: ToolEventTypeV1::file_changed(),
            subscription_id: ToolEventSubscriptionId::file_changed_v1(),
            context,
            data,
            observation_policy_generation: Some(observation_policy_generation),
        }
    }

    /// Revalidate a received projection against the public v1 contract.
    pub fn validate_bounds(&self) -> Result<(), ToolEventBuildError> {
        if self.schema_version != TOOL_EVENT_V1_SCHEMA_VERSION {
            return Err(ToolEventBuildError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                supported: TOOL_EVENT_V1_SCHEMA_VERSION,
            });
        }
        if !self.event_type.is_file_changed() || !self.subscription_id.is_file_changed_v1() {
            return Err(ToolEventBuildError::KnownVariantMismatch);
        }
        validate_projected_field(
            "context.session_id",
            &self.context.session_id,
            MAX_TOOL_EVENT_SESSION_ID_BYTES,
        )?;
        validate_projected_field(
            "context.root_session_id",
            &self.context.root_session_id,
            MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
        )?;
        validate_projected_field(
            "context.tool_call_id",
            &self.context.tool_call_id,
            MAX_TOOL_EVENT_CALL_ID_BYTES,
        )?;
        if let Some(tool_name) = &self.context.tool_name {
            validate_projected_field(
                "context.tool_name",
                tool_name,
                MAX_TOOL_EVENT_TOOL_NAME_BYTES,
            )?;
        }
        match (&self.data.path, &self.data.path_redaction_reason) {
            (Some(path), None) => {
                validate_projected_field("data.path", path, MAX_TOOL_EVENT_PATH_BYTES)?;
            }
            (None, Some(reason))
                if matches!(
                    reason.as_str(),
                    TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED
                        | TOOL_EVENT_PATH_REDACTION_SENSITIVE
                        | TOOL_EVENT_PATH_REDACTION_UNSAFE
                ) =>
            {
                if self.data.diff.is_some()
                    || self.data.content.is_some()
                    || self.data.diff_truncated
                    || self.data.content_truncated
                {
                    return Err(ToolEventBuildError::InvalidKnownPayload(
                        "redacted path projection must not carry diff/content".to_string(),
                    ));
                }
            }
            _ => {
                return Err(ToolEventBuildError::InvalidKnownPayload(
                    "file_changed projection requires exactly one of path or path_redaction_reason"
                        .to_string(),
                ));
            }
        }
        if let Some(diff) = &self.data.diff {
            validate_projected_field("data.diff", diff, MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES)?;
        } else if self.data.diff_truncated {
            return Err(ToolEventBuildError::InvalidKnownPayload(
                "diff_truncated requires diff".to_string(),
            ));
        }
        if let Some(content) = &self.data.content {
            validate_projected_field(
                "data.content",
                content,
                MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES,
            )?;
        } else if self.data.content_truncated {
            return Err(ToolEventBuildError::InvalidKnownPayload(
                "content_truncated requires content".to_string(),
            ));
        }
        if self.observation_policy_generation == Some(0) {
            return Err(ToolEventBuildError::InvalidKnownPayload(
                "observation_policy_generation must be positive".to_string(),
            ));
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

fn validate_projected_field(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ToolEventBuildError> {
    if value.trim().is_empty() {
        return Err(ToolEventBuildError::EmptyField { field });
    }
    if value.len() > max {
        return Err(ToolEventBuildError::FieldTooLarge {
            field,
            actual: value.len(),
            max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolEventSubscriptionId;

    #[test]
    fn absent_observation_fields_are_absent_on_the_wire() {
        let projected = ProjectedToolEventV1::file_changed(
            ProjectedToolEventContextV1 {
                session_id: "session".to_string(),
                root_session_id: "root".to_string(),
                tool_name: None,
                tool_call_id: "call".to_string(),
            },
            ProjectedFileChangedV1 {
                path_redaction_reason: Some("permission_not_granted".to_string()),
                ..ProjectedFileChangedV1::default()
            },
            1,
        );
        let value = serde_json::to_value(&projected).unwrap();
        assert!(value["context"].get("tool_name").is_none());
        assert!(value["data"].get("path").is_none());
        assert_eq!(
            value["data"]["path_redaction_reason"],
            "permission_not_granted"
        );
        projected.validate_bounds().unwrap();
    }

    #[test]
    fn original_full_observation_golden_remains_deserializable() {
        let projected: ProjectedToolEventV1 = serde_json::from_str(include_str!(
            "../tests/golden/tool_event_v1.file_changed.json"
        ))
        .unwrap();
        assert_eq!(projected.context.tool_name.as_deref(), Some("Write"));
        assert_eq!(
            projected.data.path.as_deref(),
            Some("/workspace/zenith/src/lib.rs")
        );
        assert_eq!(projected.observation_policy_generation, None);
        projected.validate_bounds().unwrap();
    }

    #[test]
    fn invalid_identifier_and_mixed_path_projection_are_rejected() {
        let mut projected: ProjectedToolEventV1 = serde_json::from_str(include_str!(
            "../tests/golden/tool_event_v1.file_changed.json"
        ))
        .unwrap();
        projected.subscription_id = ToolEventSubscriptionId::new("tool.future.v1");
        assert_eq!(
            projected.validate_bounds(),
            Err(ToolEventBuildError::KnownVariantMismatch)
        );

        projected.subscription_id = ToolEventSubscriptionId::file_changed_v1();
        projected.data.path_redaction_reason =
            Some(TOOL_EVENT_PATH_REDACTION_SENSITIVE.to_string());
        assert!(matches!(
            projected.validate_bounds(),
            Err(ToolEventBuildError::InvalidKnownPayload(_))
        ));
    }
}
