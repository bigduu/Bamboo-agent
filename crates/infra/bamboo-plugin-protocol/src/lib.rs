//! Dependency-light public wire protocols for Bamboo plugin services.
//!
//! This crate deliberately depends only on serialization/error crates. Runtime,
//! server, plugin-manifest, process-supervision, and tool-result types do not
//! cross this boundary.

mod publisher;
mod tool_event_v1;

pub use publisher::{
    InMemoryToolEventRecorder, NoopToolEventPublisher, ToolEventPublishError, ToolEventPublisher,
};
pub use tool_event_v1::{
    tool_event_v1_schema, FileChangedV1, ToolEventBuildError, ToolEventContextV1,
    ToolEventSubscriptionId, ToolEventTypeV1, ToolEventV1, FILE_CHANGED_EVENT_TYPE_V1,
    FILE_CHANGED_SUBSCRIPTION_ID_V1, MAX_TOOL_EVENT_CALL_ID_BYTES, MAX_TOOL_EVENT_JSON_BYTES,
    MAX_TOOL_EVENT_PATH_BYTES, MAX_TOOL_EVENT_ROOT_SESSION_ID_BYTES,
    MAX_TOOL_EVENT_SESSION_ID_BYTES, MAX_TOOL_EVENT_SUBSCRIPTION_ID_BYTES,
    MAX_TOOL_EVENT_TOOL_NAME_BYTES, MAX_TOOL_EVENT_TYPE_BYTES, TOOL_EVENT_V1_SCHEMA_VERSION,
};
