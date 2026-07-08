//! History→transcript mapping for session resume (WP3).
//!
//! Kept as a pure function, separate from `app.rs`'s event-loop plumbing, so
//! the id-pairing/skip rules below can be unit tested directly against
//! `HistoryMessage` fixtures without spinning up an `App`.

use crate::api::types::HistoryMessage;
use crate::app::{ChatMessage, MessageRole, ToolCallDisplay};

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
/// - A mapped message with empty content, no reasoning, and no tool calls is
///   dropped (nothing to render).
pub fn map_history(messages: Vec<HistoryMessage>) -> Vec<ChatMessage> {
    let mut out: Vec<ChatMessage> = Vec::new();

    for msg in messages {
        match msg.role.as_str() {
            "system" => continue,
            "user" => {
                if msg.content.is_empty() {
                    continue;
                }
                out.push(ChatMessage {
                    role: MessageRole::User,
                    content: msg.content,
                    tool_calls: Vec::new(),
                    reasoning: None,
                });
            }
            "assistant" => {
                let tool_calls: Vec<ToolCallDisplay> = msg
                    .tool_calls
                    .unwrap_or_default()
                    .into_iter()
                    .map(|tc| ToolCallDisplay {
                        id: tc.id,
                        tool_name: tc.function.name,
                        arguments: tc.function.arguments,
                        result: None,
                        error: None,
                        phase: "complete".to_string(),
                    })
                    .collect();
                let reasoning = msg.reasoning.filter(|r| !r.is_empty());
                if msg.content.is_empty() && reasoning.is_none() && tool_calls.is_empty() {
                    continue;
                }
                out.push(ChatMessage {
                    role: MessageRole::Assistant,
                    content: msg.content,
                    tool_calls,
                    reasoning,
                });
            }
            "tool" => {
                let Some(tool_call_id) = msg.tool_call_id.as_deref() else {
                    continue;
                };
                // Scan already-built output back-to-front so a repeated id
                // across turns pairs with the *nearest preceding* assistant
                // message, not the first one in the whole transcript.
                let found = out.iter_mut().rev().find_map(|m| {
                    if !matches!(m.role, MessageRole::Assistant) {
                        return None;
                    }
                    m.tool_calls.iter_mut().find(|tc| tc.id == tool_call_id)
                });
                if let Some(tc) = found {
                    if msg.tool_success == Some(false) {
                        tc.phase = "error".to_string();
                        tc.error = Some(msg.content);
                    } else {
                        tc.phase = "complete".to_string();
                        tc.result = Some(msg.content);
                    }
                }
                // No matching call anywhere: dropped silently.
            }
            _ => {}
        }
    }

    out
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
}
