use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Agent Card
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub supported_interfaces: Vec<AgentInterface>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    pub capabilities: AgentCapabilities,
    #[serde(default)]
    pub security_schemes: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub security_requirements: Vec<serde_json::Value>,
    #[serde(default)]
    pub default_input_modes: Vec<String>,
    #[serde(default)]
    pub default_output_modes: Vec<String>,
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
    #[serde(default)]
    pub signatures: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentInterface {
    pub url: String,
    pub protocol_binding: String, // "JSONRPC", "GRPC", "HTTP+JSON"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub protocol_version: String, // e.g. "1.0"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streaming: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_notifications: Option<bool>,
    #[serde(default)]
    pub extensions: Vec<AgentExtension>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extended_agent_card: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentExtension {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub examples: Vec<String>,
    #[serde(default)]
    pub input_modes: Vec<String>,
    #[serde(default)]
    pub output_modes: Vec<String>,
    #[serde(default)]
    pub security_requirements: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

// ---------------------------------------------------------------------------
// Message / Part
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum A2ARole {
    #[serde(rename = "ROLE_UNSPECIFIED")]
    Unspecified,
    #[serde(rename = "ROLE_USER")]
    User,
    #[serde(rename = "ROLE_AGENT")]
    Agent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: A2ARole,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub reference_task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    #[serde(flatten)]
    pub content: PartContentWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PartContentWire {
    Text { text: String },
    Raw { raw: String },
    Url { url: String },
    Data { data: serde_json::Value },
}

impl PartContentWire {
    pub fn text<S: Into<String>>(s: S) -> Self {
        Self::Text { text: s.into() }
    }

    pub fn data<V: Into<serde_json::Value>>(v: V) -> Self {
        Self::Data { data: v.into() }
    }
}

// ---------------------------------------------------------------------------
// Task / Status / Artifact / StreamResponse
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    #[serde(rename = "TASK_STATE_UNSPECIFIED")]
    Unspecified,
    #[serde(rename = "TASK_STATE_SUBMITTED")]
    Submitted,
    #[serde(rename = "TASK_STATE_WORKING")]
    Working,
    #[serde(rename = "TASK_STATE_INPUT_REQUIRED")]
    InputRequired,
    #[serde(rename = "TASK_STATE_COMPLETED")]
    Completed,
    #[serde(rename = "TASK_STATE_CANCELED")]
    Canceled,
    #[serde(rename = "TASK_STATE_FAILED")]
    Failed,
    #[serde(rename = "TASK_STATE_REJECTED")]
    Rejected,
    #[serde(rename = "TASK_STATE_AUTH_REQUIRED")]
    AuthRequired,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Canceled | TaskState::Failed | TaskState::Rejected
        )
    }

    pub fn as_proto_str(&self) -> &'static str {
        match self {
            TaskState::Unspecified => "TASK_STATE_UNSPECIFIED",
            TaskState::Submitted => "TASK_STATE_SUBMITTED",
            TaskState::Working => "TASK_STATE_WORKING",
            TaskState::InputRequired => "TASK_STATE_INPUT_REQUIRED",
            TaskState::Completed => "TASK_STATE_COMPLETED",
            TaskState::Canceled => "TASK_STATE_CANCELED",
            TaskState::Failed => "TASK_STATE_FAILED",
            TaskState::Rejected => "TASK_STATE_REJECTED",
            TaskState::AuthRequired => "TASK_STATE_AUTH_REQUIRED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    /// ProtoJSON timestamp as ISO 8601 string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    pub status: TaskStatus,
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
    #[serde(default)]
    pub history: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub extensions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent {
    pub task_id: String,
    pub context_id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent {
    pub task_id: String,
    pub context_id: String,
    pub artifact: Artifact,
    #[serde(default)]
    pub append: bool,
    #[serde(default)]
    pub last_chunk: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// StreamResponse uses Option fields for each payload kind.
///
/// This is more robust than a flattened untagged enum because:
/// - It tolerates agents that return multiple fields at once.
/// - It allows easy detection of "no payload" as a protocol error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_update: Option<TaskStatusUpdateEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_update: Option<TaskArtifactUpdateEvent>,
}

impl StreamResponse {
    /// Returns the payload kind as an enum reference for pattern matching.
    pub fn payload_kind(&self) -> Result<StreamPayloadRef<'_>, super::error::A2AClientError> {
        let mut found = Vec::new();
        if self.task.is_some() {
            found.push("task");
        }
        if self.message.is_some() {
            found.push("message");
        }
        if self.status_update.is_some() {
            found.push("status_update");
        }
        if self.artifact_update.is_some() {
            found.push("artifact_update");
        }
        match found.len() {
            0 => Err(super::error::A2AClientError::InvalidStreamResponse(
                "StreamResponse has no payload".to_string(),
            )),
            1 => Ok(()),
            _ => Err(super::error::A2AClientError::InvalidStreamResponse(
                format!("StreamResponse has multiple payloads: {}", found.join(", ")),
            )),
        }?;
        if let Some(ref t) = self.task {
            return Ok(StreamPayloadRef::Task(t));
        }
        if let Some(ref m) = self.message {
            return Ok(StreamPayloadRef::Message(m));
        }
        if let Some(ref s) = self.status_update {
            return Ok(StreamPayloadRef::StatusUpdate(s));
        }
        if let Some(ref a) = self.artifact_update {
            return Ok(StreamPayloadRef::ArtifactUpdate(a));
        }
        unreachable!()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StreamPayloadRef<'a> {
    Task(&'a Task),
    Message(&'a Message),
    StatusUpdate(&'a TaskStatusUpdateEvent),
    ArtifactUpdate(&'a TaskArtifactUpdateEvent),
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub message: Message,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<SendMessageConfiguration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_output_modes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_length: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking: Option<bool>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_length: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelTaskRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_card_deserializes_v1_fields() {
        let json = serde_json::json!({
            "name": "Test Agent",
            "description": "A test agent",
            "supportedInterfaces": [
                {
                    "url": "https://example.com/rpc",
                    "protocolBinding": "JSONRPC",
                    "protocolVersion": "1.0"
                }
            ],
            "version": "1.0.0",
            "capabilities": {
                "streaming": true,
                "extensions": []
            },
            "defaultInputModes": ["text/plain"],
            "defaultOutputModes": ["text/plain"],
            "skills": [],
            "securitySchemes": {},
            "securityRequirements": [],
            "signatures": []
        });
        let card: AgentCard = serde_json::from_value(json).unwrap();
        assert_eq!(card.name, "Test Agent");
        assert_eq!(card.supported_interfaces.len(), 1);
        assert_eq!(card.supported_interfaces[0].protocol_binding, "JSONRPC");
        assert_eq!(card.supported_interfaces[0].protocol_version, "1.0");
        assert_eq!(card.capabilities.streaming, Some(true));
    }

    #[test]
    fn stream_response_deserializes_status_update_camel_case() {
        let json = serde_json::json!({
            "statusUpdate": {
                "taskId": "task-1",
                "contextId": "ctx-1",
                "status": {
                    "state": "TASK_STATE_WORKING"
                }
            }
        });
        let resp: StreamResponse = serde_json::from_value(json).unwrap();
        assert!(resp.status_update.is_some());
        assert!(resp.task.is_none());
        assert!(resp.message.is_none());
        assert!(resp.artifact_update.is_none());
        let update = resp.status_update.unwrap();
        assert_eq!(update.task_id, "task-1");
        assert_eq!(update.context_id, "ctx-1");
        assert!(matches!(update.status.state, TaskState::Working));
    }

    #[test]
    fn stream_response_deserializes_artifact_update_camel_case() {
        let json = serde_json::json!({
            "artifactUpdate": {
                "taskId": "task-1",
                "contextId": "ctx-1",
                "artifact": {
                    "artifactId": "art-1",
                    "parts": [
                        { "text": "diff content" }
                    ]
                },
                "append": false,
                "lastChunk": true
            }
        });
        let resp: StreamResponse = serde_json::from_value(json).unwrap();
        assert!(resp.artifact_update.is_some());
        let update = resp.artifact_update.unwrap();
        assert_eq!(update.artifact.artifact_id, "art-1");
        assert!(!update.append);
        assert!(update.last_chunk);
    }

    #[test]
    fn task_state_is_terminal_for_completed() {
        assert!(TaskState::Completed.is_terminal());
        assert!(TaskState::Canceled.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(TaskState::Rejected.is_terminal());
        assert!(!TaskState::Working.is_terminal());
        assert!(!TaskState::InputRequired.is_terminal());
        assert!(!TaskState::AuthRequired.is_terminal());
    }

    #[test]
    fn part_content_wire_text_roundtrip() {
        let part = PartContentWire::Text {
            text: "hello".to_string(),
        };
        let json = serde_json::to_string(&part).unwrap();
        assert_eq!(json, r#"{"text":"hello"}"#);
        let decoded: PartContentWire = serde_json::from_str(&json).unwrap();
        match decoded {
            PartContentWire::Text { text } => assert_eq!(text, "hello"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn message_roundtrip_with_reference_task_ids() {
        let msg = Message {
            message_id: "msg-1".to_string(),
            context_id: Some("ctx-1".to_string()),
            task_id: Some("task-1".to_string()),
            role: A2ARole::User,
            parts: vec![Part {
                content: PartContentWire::text("hello"),
                metadata: None,
                filename: None,
                media_type: Some("text/plain".to_string()),
            }],
            metadata: None,
            extensions: vec![],
            reference_task_ids: vec!["task-old".to_string()],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("referenceTaskIds"));
        let decoded: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.reference_task_ids, vec!["task-old"]);
    }
}
