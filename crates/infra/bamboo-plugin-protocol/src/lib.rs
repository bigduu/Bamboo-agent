//! Dependency-light public wire protocols for Bamboo plugin services.
//!
//! This crate deliberately depends only on serialization/error crates. Runtime,
//! server, plugin-manifest, process-supervision, and tool-result types do not
//! cross this boundary.

mod projected_tool_event_v1;
mod publisher;
mod tool_event_v1;

pub use projected_tool_event_v1::{
    ProjectedFileChangedV1, ProjectedToolEventContextV1, ProjectedToolEventV1,
    MAX_PROJECTED_TOOL_EVENT_CONTENT_BYTES, MAX_PROJECTED_TOOL_EVENT_DIFF_BYTES,
    TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED, TOOL_EVENT_PATH_REDACTION_SENSITIVE,
    TOOL_EVENT_PATH_REDACTION_UNSAFE,
};
pub use publisher::{
    InMemoryToolEventRecorder, NoopToolEventPublisher, ToolEventPublishError, ToolEventPublisher,
};
pub use tool_event_v1::{
    tool_event_v1_schema, FileChangedV1, ToolEventBuildError, ToolEventContextV1,
    ToolEventSubscriptionId, ToolEventTypeV1, ToolEventV1, FILE_CHANGED_EVENT_TYPE_V1,
    FILE_CHANGED_SUBSCRIPTION_ID_V1, MAX_TOOL_EVENT_CALL_ID_BYTES, MAX_TOOL_EVENT_JSON_BYTES,
    MAX_TOOL_EVENT_PATH_BYTES, MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
    MAX_TOOL_EVENT_SESSION_ID_BYTES, MAX_TOOL_EVENT_SUBSCRIPTION_ID_BYTES,
    MAX_TOOL_EVENT_TOOL_NAME_BYTES, MAX_TOOL_EVENT_TYPE_BYTES, TOOL_EVENT_PROTOCOL_NAME,
    TOOL_EVENT_V1_SCHEMA_VERSION,
};
