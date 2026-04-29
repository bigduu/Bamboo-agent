use std::collections::HashMap;

use bamboo_agent_core::{AgentEvent, TokenUsage};
use bamboo_infrastructure::a2a::types::{
    A2ARole, PartContentWire, StreamResponse, TaskState, TaskStatus,
};

/// Events and metadata updates produced by mapping a single A2A StreamResponse.
pub struct A2AMappedEvents {
    pub events: Vec<AgentEvent>,
    pub metadata_updates: HashMap<String, String>,
}

/// Stateful mapper that tracks the latest A2A task state across a stream.
pub struct A2AEventMapper {
    terminal_sent: bool,
    latest_task_id: Option<String>,
    context_id: Option<String>,
    final_text: String,
}

impl A2AEventMapper {
    pub fn new() -> Self {
        Self {
            terminal_sent: false,
            latest_task_id: None,
            context_id: None,
            final_text: String::new(),
        }
    }

    pub fn latest_task_id(&self) -> Option<&str> {
        self.latest_task_id.as_deref()
    }

    pub fn context_id(&self) -> Option<&str> {
        self.context_id.as_deref()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal_sent
    }

    pub fn final_text(&self) -> &str {
        &self.final_text
    }

    /// Map a single A2A StreamResponse to Bamboo AgentEvents and metadata.
    pub fn map_stream_response(&mut self, response: StreamResponse) -> A2AMappedEvents {
        let mut events = Vec::new();
        let mut metadata = HashMap::new();

        if let Some(task) = response.task {
            self.latest_task_id = Some(task.id.clone());
            if let Some(ctx) = task.context_id.clone() {
                self.context_id = Some(ctx);
            }
            metadata.insert("a2a.latest_task_id".to_string(), task.id.clone());
            if let Some(ctx) = &task.context_id {
                metadata.insert("a2a.context_id".to_string(), ctx.clone());
            }
            metadata.insert(
                "a2a.last_state".to_string(),
                task.status.state.as_proto_str().to_string(),
            );
            events.extend(self.map_status(&task.id, task.context_id.as_deref(), task.status));
        }

        if let Some(message) = response.message {
            if message.role == A2ARole::Agent {
                let text = text_from_parts(&message.parts);
                if !text.is_empty() {
                    self.final_text.push_str(&text);
                    events.push(AgentEvent::Token { content: text });
                }
            }
        }

        if let Some(update) = response.status_update {
            self.latest_task_id = Some(update.task_id.clone());
            self.context_id = Some(update.context_id.clone());
            metadata.insert("a2a.latest_task_id".to_string(), update.task_id.clone());
            metadata.insert("a2a.context_id".to_string(), update.context_id.clone());
            metadata.insert(
                "a2a.last_state".to_string(),
                update.status.state.as_proto_str().to_string(),
            );
            events.extend(self.map_status(
                &update.task_id,
                Some(&update.context_id),
                update.status,
            ));
        }

        if let Some(update) = response.artifact_update {
            let preview =
                handle_artifact_update(&update.artifact, update.append, update.last_chunk);
            if !preview.is_empty() {
                events.push(AgentEvent::Token {
                    content: preview.clone(),
                });
                self.final_text.push_str(&preview);
            }
            metadata.insert(
                "a2a.last_artifacts_summary".to_string(),
                serde_json::json!({
                    "artifact_id": update.artifact.artifact_id,
                    "name": update.artifact.name,
                    "append": update.append,
                    "last_chunk": update.last_chunk,
                })
                .to_string(),
            );
        }

        A2AMappedEvents {
            events,
            metadata_updates: metadata,
        }
    }

    fn map_status(
        &mut self,
        _task_id: &str,
        _context_id: Option<&str>,
        status: TaskStatus,
    ) -> Vec<AgentEvent> {
        let mut events = Vec::new();

        match &status.state {
            TaskState::Submitted => {
                // Optionally emit a brief status token
            }
            TaskState::Working => {
                if let Some(msg) = &status.message {
                    let text = text_from_parts(&msg.parts);
                    if !text.is_empty() {
                        self.final_text.push_str(&text);
                        events.push(AgentEvent::Token { content: text });
                    }
                }
            }
            TaskState::InputRequired => {
                let question = question_from_status(&status);
                events.push(AgentEvent::NeedClarification {
                    question,
                    options: None,
                    tool_call_id: None,
                    allow_custom: true,
                });
            }
            TaskState::AuthRequired => {
                let question = question_from_status(&status);
                events.push(AgentEvent::NeedClarification {
                    question,
                    options: None,
                    tool_call_id: None,
                    allow_custom: true,
                });
            }
            TaskState::Completed => {
                self.terminal_sent = true;
                if let Some(msg) = &status.message {
                    let text = text_from_parts(&msg.parts);
                    if !text.is_empty() {
                        self.final_text.push_str(&text);
                        events.push(AgentEvent::Token { content: text });
                    }
                }
                events.push(AgentEvent::Complete {
                    usage: TokenUsage::default(),
                });
            }
            TaskState::Failed => {
                self.terminal_sent = true;
                let error_msg = status
                    .message
                    .as_ref()
                    .map(|m| text_from_parts(&m.parts))
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "External agent reported failure".to_string());
                events.push(AgentEvent::Error { message: error_msg });
            }
            TaskState::Canceled => {
                self.terminal_sent = true;
                events.push(AgentEvent::Error {
                    message: "External agent task was cancelled".to_string(),
                });
            }
            TaskState::Rejected => {
                self.terminal_sent = true;
                events.push(AgentEvent::Error {
                    message: "External agent rejected the task".to_string(),
                });
            }
            TaskState::Unspecified => {}
        }

        events
    }
}

/// Extract plain text from a slice of Parts.
pub fn text_from_parts(parts: &[bamboo_infrastructure::a2a::types::Part]) -> String {
    parts
        .iter()
        .filter_map(|part| match &part.content {
            PartContentWire::Text { text } => Some(text.as_str()),
            PartContentWire::Data { data } => data.get("summary").and_then(|v| v.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Build a human-readable question from a TaskStatus that requires input/auth.
fn question_from_status(status: &TaskStatus) -> String {
    status
        .message
        .as_ref()
        .map(|m| text_from_parts(&m.parts))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| match status.state {
            TaskState::InputRequired => "External agent requires additional input.".to_string(),
            TaskState::AuthRequired => {
                "External agent requires authentication or authorization.".to_string()
            }
            _ => format!("External agent state: {:?}", status.state),
        })
}

/// Build a preview string from an artifact update.
fn handle_artifact_update(
    artifact: &bamboo_infrastructure::a2a::types::Artifact,
    _append: bool,
    _last_chunk: bool,
) -> String {
    let text = text_from_parts(&artifact.parts);
    if text.is_empty() {
        if let Some(name) = &artifact.name {
            format!("[Artifact: {}]", name)
        } else {
            format!("[Artifact: {}]", artifact.artifact_id)
        }
    } else {
        let header = artifact
            .name
            .as_ref()
            .map(|n| format!("--- Artifact: {} ---\n", n))
            .unwrap_or_default();
        format!("{}{}", header, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure::a2a::types::{
        A2ARole, Message, Part, Task, TaskStatus, TaskStatusUpdateEvent,
    };

    #[test]
    fn a2a_message_text_maps_to_token() {
        let mut mapper = A2AEventMapper::new();
        let response = StreamResponse {
            task: None,
            message: Some(Message {
                message_id: "m1".to_string(),
                context_id: None,
                task_id: None,
                role: A2ARole::Agent,
                parts: vec![Part {
                    content: PartContentWire::text("hello world"),
                    metadata: None,
                    filename: None,
                    media_type: Some("text/plain".to_string()),
                }],
                metadata: None,
                extensions: vec![],
                reference_task_ids: vec![],
            }),
            status_update: None,
            artifact_update: None,
        };
        let mapped = mapper.map_stream_response(response);
        assert_eq!(mapped.events.len(), 1);
        match &mapped.events[0] {
            AgentEvent::Token { content } => assert_eq!(content, "hello world"),
            other => panic!("expected Token, got {:?}", other),
        }
    }

    #[test]
    fn a2a_completed_status_maps_to_complete_and_metadata() {
        let mut mapper = A2AEventMapper::new();
        let response = StreamResponse {
            task: Some(Task {
                id: "task-1".to_string(),
                context_id: Some("ctx-1".to_string()),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: None,
                },
                artifacts: vec![],
                history: vec![],
                metadata: None,
            }),
            message: None,
            status_update: None,
            artifact_update: None,
        };
        let mapped = mapper.map_stream_response(response);
        assert!(mapper.is_terminal());
        assert_eq!(
            mapped.metadata_updates.get("a2a.latest_task_id"),
            Some(&"task-1".to_string())
        );
        assert_eq!(
            mapped.metadata_updates.get("a2a.context_id"),
            Some(&"ctx-1".to_string())
        );
        assert_eq!(
            mapped.metadata_updates.get("a2a.last_state"),
            Some(&"TASK_STATE_COMPLETED".to_string())
        );
        match &mapped.events[0] {
            AgentEvent::Complete { .. } => {}
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn a2a_failed_status_maps_to_error() {
        let mut mapper = A2AEventMapper::new();
        let response = StreamResponse {
            task: None,
            message: None,
            status_update: Some(TaskStatusUpdateEvent {
                task_id: "task-1".to_string(),
                context_id: "ctx-1".to_string(),
                status: TaskStatus {
                    state: TaskState::Failed,
                    message: Some(Message {
                        message_id: "m1".to_string(),
                        context_id: None,
                        task_id: None,
                        role: A2ARole::Agent,
                        parts: vec![Part {
                            content: PartContentWire::text("Something went wrong"),
                            metadata: None,
                            filename: None,
                            media_type: None,
                        }],
                        metadata: None,
                        extensions: vec![],
                        reference_task_ids: vec![],
                    }),
                    timestamp: None,
                },
                metadata: None,
            }),
            artifact_update: None,
        };
        let mapped = mapper.map_stream_response(response);
        assert!(mapper.is_terminal());
        match &mapped.events[0] {
            AgentEvent::Error { message } => assert_eq!(message, "Something went wrong"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn a2a_input_required_maps_to_need_clarification() {
        let mut mapper = A2AEventMapper::new();
        let response = StreamResponse {
            task: None,
            message: None,
            status_update: Some(TaskStatusUpdateEvent {
                task_id: "task-1".to_string(),
                context_id: "ctx-1".to_string(),
                status: TaskStatus {
                    state: TaskState::InputRequired,
                    message: Some(Message {
                        message_id: "m1".to_string(),
                        context_id: None,
                        task_id: None,
                        role: A2ARole::Agent,
                        parts: vec![Part {
                            content: PartContentWire::text("What is your API key?"),
                            metadata: None,
                            filename: None,
                            media_type: None,
                        }],
                        metadata: None,
                        extensions: vec![],
                        reference_task_ids: vec![],
                    }),
                    timestamp: None,
                },
                metadata: None,
            }),
            artifact_update: None,
        };
        let mapped = mapper.map_stream_response(response);
        assert!(!mapper.is_terminal());
        match &mapped.events[0] {
            AgentEvent::NeedClarification { question, .. } => {
                assert_eq!(question, "What is your API key?");
            }
            other => panic!("expected NeedClarification, got {:?}", other),
        }
    }
}
