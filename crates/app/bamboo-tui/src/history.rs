//! History→transcript mapping for session resume (WP3).
//!
//! Kept as a pure function, separate from `app.rs`'s event-loop plumbing, so
//! the id-pairing/skip rules below can be unit tested directly against
//! `HistoryMessage` fixtures without spinning up an `App`.

use crate::api::types::HistoryMessage;
use crate::app::{ChatMessage, MessageRole, SubAgentDisplay, ToolCallDisplay};

/// Map a session's raw history (`GET /api/v1/history/{id}`) into the chat
/// transcript the Chat tab renders.
///
/// - `system` messages are dropped — the TUI never displays them.
/// - `user` messages become a plain `ChatMessage`.
/// - `assistant` messages become a `ChatMessage` carrying their tool calls
///   (installed with `phase: "complete"`, no result yet — a paired `tool`
///   message fills that in below).
/// - `tool` messages are not appended as their own transcript entry; instead
///   the matching `ToolCallDisplay` (by `tool_call_id`) is located in the
///   *most recent* assistant message that has one and updated in place
///   (`result`/`error` + terminal `phase`). A tool result with no matching
///   call anywhere is dropped silently — there's nothing sensible to attach
///   it to.
/// - A mapped message with empty content, no reasoning, no tool calls, and no
///   reconstructable child rows is dropped (nothing to render).
pub fn map_history(messages: Vec<HistoryMessage>) -> Vec<ChatMessage> {
    let mut out: Vec<ChatMessage> = Vec::new();

    for (index, msg) in messages.into_iter().enumerate() {
        let message_id = if msg.id.is_empty() {
            format!("history:{index}:{}", msg.role)
        } else {
            msg.id.clone()
        };
        match msg.role.as_str() {
            "system" => continue,
            "user" => {
                if msg.content.is_empty() {
                    continue;
                }
                out.push(ChatMessage {
                    id: message_id,
                    role: MessageRole::User,
                    content: msg.content,
                    tool_calls: Vec::new(),
                    reasoning: None,
                    sub_agents: Vec::new(),
                    terminal_status: None,
                });
            }
            "assistant" => {
                let tool_calls: Vec<ToolCallDisplay> = msg
                    .tool_calls
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .map(|(tool_index, tc)| ToolCallDisplay {
                        id: if tc.id.is_empty() {
                            format!("{message_id}:tool:{tool_index}")
                        } else {
                            tc.id
                        },
                        tool_name: tc.function.name,
                        arguments: tc.function.arguments,
                        result: None,
                        stream_output: String::new(),
                        error: None,
                        phase: "complete".to_string(),
                    })
                    .collect();
                let reasoning = msg.reasoning.filter(|r| !r.is_empty());
                let sub_agents = sub_agents_from_metadata(msg.metadata.as_ref());
                if msg.content.is_empty()
                    && reasoning.is_none()
                    && tool_calls.is_empty()
                    && sub_agents.is_empty()
                {
                    continue;
                }
                out.push(ChatMessage {
                    id: message_id,
                    role: MessageRole::Assistant,
                    content: msg.content,
                    tool_calls,
                    reasoning,
                    sub_agents,
                    terminal_status: None,
                });
            }
            "tool" => {
                let Some(tool_call_id) = msg.tool_call_id.as_deref() else {
                    continue;
                };
                let child = sub_agent_from_tool_result(&msg.content, msg.tool_success);
                // Scan already-built output back-to-front so a repeated id
                // across turns pairs with the *nearest preceding* assistant
                // message, not the first one in the whole transcript.
                let parent_index = out.iter().rposition(|message| {
                    matches!(message.role, MessageRole::Assistant)
                        && message
                            .tool_calls
                            .iter()
                            .any(|tool| tool.id == tool_call_id)
                });
                if let Some(parent_index) = parent_index {
                    let tc = out[parent_index]
                        .tool_calls
                        .iter_mut()
                        .find(|tool| tool.id == tool_call_id)
                        .expect("parent index was selected by this tool id");
                    if msg.tool_success == Some(false) {
                        tc.phase = "error".to_string();
                        tc.error = Some(msg.content);
                    } else {
                        tc.phase = "complete".to_string();
                        tc.result = Some(msg.content);
                    }
                }
                if let Some(child) = child {
                    if let Some(existing) = out
                        .iter_mut()
                        .rev()
                        .flat_map(|message| message.sub_agents.iter_mut())
                        .find(|existing| existing.child_session_id == child.child_session_id)
                    {
                        if child.title.is_some() {
                            existing.title = child.title;
                        }
                        existing.status = child.status;
                        if child.error.is_some() {
                            existing.error = child.error;
                        }
                    } else {
                        let attach_index = parent_index.or_else(|| {
                            out.iter()
                                .rposition(|message| matches!(message.role, MessageRole::Assistant))
                        });
                        if let Some(attach_index) = attach_index {
                            out[attach_index].sub_agents.push(child);
                        }
                    }
                }
                // The tool message itself is never rendered as a separate
                // transcript row. A reconstructable child summary above may
                // still be retained even when its tool-call pair is absent.
            }
            _ => {}
        }
    }

    out
}

fn sub_agents_from_metadata(metadata: Option<&serde_json::Value>) -> Vec<SubAgentDisplay> {
    metadata
        .and_then(|value| value.get("sub_agents").or_else(|| value.get("subagents")))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|child| {
            let child_session_id = child
                .get("child_session_id")
                .or_else(|| child.get("id"))?
                .as_str()?
                .to_string();
            Some(SubAgentDisplay {
                child_session_id,
                title: child
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                status: child
                    .get("status")
                    .or_else(|| child.get("last_run_status"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| {
                        if child
                            .get("is_running")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                        {
                            "running"
                        } else {
                            "completed"
                        }
                    })
                    .to_string(),
                error: child
                    .get("error")
                    .or_else(|| child.get("last_run_error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
}

fn sub_agent_from_tool_result(content: &str, success: Option<bool>) -> Option<SubAgentDisplay> {
    let value: serde_json::Value = serde_json::from_str(content).ok()?;
    let child_session_id = value.get("child_session_id")?.as_str()?.to_string();
    Some(SubAgentDisplay {
        child_session_id,
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        status: if success == Some(false) {
            "error".to_string()
        } else {
            value
                .get("status")
                .or_else(|| value.get("last_run_status"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| {
                    if value
                        .get("is_running")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                    {
                        "running"
                    } else {
                        "completed"
                    }
                })
                .to_string()
        },
        error: value
            .get("error")
            .or_else(|| value.get("last_run_error"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{HistoryFunctionCall, HistoryToolCall};

    fn user(content: &str) -> HistoryMessage {
        HistoryMessage {
            role: "user".to_string(),
            content: content.to_string(),
            ..Default::default()
        }
    }

    fn assistant(content: &str, tool_calls: Vec<(&str, &str, &str)>) -> HistoryMessage {
        let tool_calls = if tool_calls.is_empty() {
            None
        } else {
            Some(
                tool_calls
                    .into_iter()
                    .map(|(id, name, args)| HistoryToolCall {
                        id: id.to_string(),
                        function: HistoryFunctionCall {
                            name: name.to_string(),
                            arguments: args.to_string(),
                        },
                    })
                    .collect(),
            )
        };
        HistoryMessage {
            role: "assistant".to_string(),
            content: content.to_string(),
            tool_calls,
            ..Default::default()
        }
    }

    fn tool(tool_call_id: &str, content: &str, success: Option<bool>) -> HistoryMessage {
        HistoryMessage {
            role: "tool".to_string(),
            content: content.to_string(),
            tool_call_id: Some(tool_call_id.to_string()),
            tool_success: success,
            ..Default::default()
        }
    }

    #[test]
    fn system_messages_are_skipped() {
        let mut sys = HistoryMessage {
            role: "system".to_string(),
            content: "you are a helpful assistant".to_string(),
            ..Default::default()
        };
        sys.id = "sys1".to_string();
        let out = map_history(vec![sys, user("hi")]);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].role, MessageRole::User));
    }

    #[test]
    fn user_and_assistant_map_straightforwardly() {
        let out = map_history(vec![user("hello"), assistant("hi there", vec![])]);
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0].role, MessageRole::User));
        assert_eq!(out[0].content, "hello");
        assert!(matches!(out[1].role, MessageRole::Assistant));
        assert_eq!(out[1].content, "hi there");
    }

    #[test]
    fn tool_result_pairs_with_matching_call_and_success_sets_complete_phase() {
        let out = map_history(vec![
            user("read the file"),
            assistant("", vec![("t1", "Read", "{\"path\":\"a.txt\"}")]),
            tool("t1", "file contents", Some(true)),
        ]);
        assert_eq!(out.len(), 2, "the tool message is not its own entry");
        let asst = &out[1];
        assert_eq!(asst.tool_calls.len(), 1);
        let tc = &asst.tool_calls[0];
        assert_eq!(tc.tool_name, "Read");
        assert_eq!(tc.result.as_deref(), Some("file contents"));
        assert!(tc.error.is_none());
        assert_eq!(tc.phase, "complete");
    }

    #[test]
    fn tool_success_false_sets_error_phase_and_error_field() {
        let out = map_history(vec![
            assistant("", vec![("t1", "Bash", "{}")]),
            tool("t1", "command not found", Some(false)),
        ]);
        let tc = &out[0].tool_calls[0];
        assert_eq!(tc.phase, "error");
        assert_eq!(tc.error.as_deref(), Some("command not found"));
        assert!(tc.result.is_none());
    }

    #[test]
    fn tool_success_none_defaults_to_complete() {
        let out = map_history(vec![
            assistant("", vec![("t1", "Bash", "{}")]),
            tool("t1", "ok", None),
        ]);
        let tc = &out[0].tool_calls[0];
        assert_eq!(tc.phase, "complete");
        assert_eq!(tc.result.as_deref(), Some("ok"));
    }

    #[test]
    fn unmatched_tool_result_is_dropped_silently() {
        let out = map_history(vec![
            assistant("", vec![("t1", "Bash", "{}")]),
            tool("ghost", "orphan result", Some(true)),
        ]);
        assert_eq!(out.len(), 1);
        assert!(out[0].tool_calls[0].result.is_none());
    }

    #[test]
    fn out_of_order_tool_result_pairs_with_most_recent_assistant() {
        // Two assistant turns both declare a tool call id "t1" (e.g. after a
        // retried/looped call) — the result must land on the LATER one.
        let out = map_history(vec![
            assistant("", vec![("t1", "Bash", "{}")]),
            user("try again"),
            assistant("", vec![("t1", "Bash", "{}")]),
            tool("t1", "second result", Some(true)),
        ]);
        assert_eq!(out.len(), 3);
        assert!(
            out[0].tool_calls[0].result.is_none(),
            "the earlier assistant's call must be untouched"
        );
        assert_eq!(
            out[2].tool_calls[0].result.as_deref(),
            Some("second result")
        );
    }

    #[test]
    fn empty_assistant_message_is_skipped() {
        // No content, no reasoning, no tool calls — nothing to render.
        let out = map_history(vec![assistant("", vec![])]);
        assert!(out.is_empty());
    }

    #[test]
    fn assistant_reasoning_only_is_kept() {
        let mut msg = assistant("", vec![]);
        msg.reasoning = Some("thinking it through".to_string());
        let out = map_history(vec![msg]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reasoning.as_deref(), Some("thinking it through"));
    }

    #[test]
    fn empty_user_message_is_skipped() {
        let out = map_history(vec![user("")]);
        assert!(out.is_empty());
    }

    #[test]
    fn history_preserves_stable_message_tool_and_child_ids_when_available() {
        let mut message = assistant("done", vec![("tool-stable", "sub_agent", "{}")]);
        message.id = "message-stable".to_string();
        message.metadata = Some(serde_json::json!({
            "sub_agents": [{
                "child_session_id": "child-from-metadata",
                "title": "research",
                "status": "completed"
            }]
        }));
        let out = map_history(vec![message]);
        assert_eq!(out[0].id, "message-stable");
        assert_eq!(out[0].tool_calls[0].id, "tool-stable");
        assert_eq!(out[0].sub_agents[0].child_session_id, "child-from-metadata");

        let out = map_history(vec![
            assistant("", vec![("spawn", "sub_agent", "{}")]),
            tool(
                "spawn",
                r#"{"child_session_id":"child-from-result","title":"review"}"#,
                Some(true),
            ),
        ]);
        assert_eq!(out[0].sub_agents[0].child_session_id, "child-from-result");
    }

    #[test]
    fn history_synthesizes_missing_tool_ids_and_merges_child_updates() {
        let mut message = assistant("", vec![("", "SubAgent", "{}")]);
        message.id = "assistant-1".to_string();
        let out = map_history(vec![message]);
        assert_eq!(out[0].tool_calls[0].id, "assistant-1:tool:0");

        let mut message = assistant("", vec![("spawn", "SubAgent", "{}")]);
        message.metadata = Some(serde_json::json!({
            "sub_agents": [{
                "child_session_id": "child-1",
                "title": "initial",
                "status": "running"
            }]
        }));
        let out = map_history(vec![
            message,
            tool(
                "spawn",
                r#"{"child_session_id":"child-1","title":"final","last_run_status":"completed"}"#,
                Some(true),
            ),
        ]);
        assert_eq!(out[0].sub_agents.len(), 1);
        assert_eq!(out[0].sub_agents[0].title.as_deref(), Some("final"));
        assert_eq!(out[0].sub_agents[0].status, "completed");
    }

    #[test]
    fn history_and_live_turns_expose_equivalent_structured_block_shapes() {
        use crate::api::BambooClient;
        use crate::app::{App, ConversationBlockKind};

        let mut stored = assistant("answer", vec![("call-1", "Read", "{}")]);
        stored.id = "message-1".to_string();
        stored.reasoning = Some("reasoning".to_string());
        let mapped = map_history(vec![stored, tool("call-1", "result", Some(true))]);

        let mut history_app = App::new(BambooClient::new("http://127.0.0.1:0"));
        history_app.chat.messages = mapped.clone();
        let mut live_app = App::new(BambooClient::new("http://127.0.0.1:0"));
        live_app.chat.current_turn_id = Some("message-1".to_string());
        live_app.chat.current_response = "answer".to_string();
        live_app.chat.current_reasoning = "reasoning".to_string();
        live_app.chat.current_tool_calls = mapped[0].tool_calls.clone();

        let shape = |app: &App| {
            app.conversation_blocks()
                .into_iter()
                .map(|block| match block.kind {
                    ConversationBlockKind::AssistantMarkdown { .. } => "assistant",
                    ConversationBlockKind::Reasoning { .. } => "reasoning",
                    ConversationBlockKind::ToolCall { .. } => "tool",
                    ConversationBlockKind::UserMessage(_) => "user",
                    ConversationBlockKind::SubAgent { .. } => "subagent",
                    ConversationBlockKind::Question { .. } => "question",
                    ConversationBlockKind::TerminalStatus(_) => "terminal",
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(&history_app), shape(&live_app));
        assert_eq!(
            history_app.conversation_blocks()[2].id,
            live_app.conversation_blocks()[2].id,
            "tool_call_id is the stable cross-shape UI key"
        );
    }
}
