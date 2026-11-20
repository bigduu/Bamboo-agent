use std::collections::HashMap;

use bamboo_domain::{Message, Role};
use serde_json::{json, Value};

/// Limit model-visible file mutation receipts while retaining the original
/// session messages for review and replay. Historical receipts carry no owner
/// marker, so require the exact call name and the matching file-change format.
pub fn project_file_change_results(messages: &mut [Message]) {
    let mut calls = HashMap::new();
    for (index, message) in messages.iter().enumerate() {
        if message.role != Role::Assistant {
            continue;
        }
        for call in message.tool_calls.iter().flatten() {
            if call.id.is_empty() {
                continue;
            }
            let operation = match call.function.name.as_str() {
                "Write" => Some("Write"),
                "Edit" => Some("Edit"),
                _ => None,
            };
            calls
                .entry(call.id.clone())
                .and_modify(|entry| *entry = None)
                .or_insert(operation.map(|operation| (index, operation)));
        }
    }

    for (index, message) in messages.iter_mut().enumerate() {
        if message.role != Role::Tool
            || message.tool_success == Some(false)
            || message.content_parts.is_some()
        {
            continue;
        }
        let Some(&(call_index, operation)) = message
            .tool_call_id
            .as_ref()
            .and_then(|id| calls.get(id))
            .and_then(Option::as_ref)
        else {
            continue;
        };
        if call_index < index {
            if let Some(projected) = compact_file_change_receipt(&message.content, operation) {
                message.content = projected;
            }
        }
    }
}

fn compact_file_change_receipt(content: &str, operation: &str) -> Option<String> {
    let mut values = serde_json::Deserializer::from_str(content).into_iter::<Value>();
    let mut receipt = values.next()?.ok()?;
    let suffix = &content[values.byte_offset()..];
    // Preserve the exact hook feedback that the runtime appends after JSON.
    if !suffix.trim().is_empty()
        && !(suffix.starts_with("\n\n<post_tool_use_feedback>\n")
            && suffix.ends_with("\n</post_tool_use_feedback>"))
    {
        return None;
    }
    if receipt.get("operation")?.as_str()? != operation {
        return None;
    }
    let path = receipt.get("file_path")?.as_str()?;
    let message = receipt.get("message")?.as_str()?;
    let workspace = receipt.get("workspace")?.as_str()?;
    if path.is_empty() || message.is_empty() || workspace.is_empty() {
        return None;
    }
    let checkpoint = receipt.get("checkpoint")?;
    checkpoint.get("created")?.as_bool()?;
    let diff = receipt.get("diff")?;
    if diff.get("format")?.as_str()? != "unified" {
        return None;
    }
    diff.get("unified")?.as_str()?;
    // Keep the original small metadata, including the file parent used by
    // model-facing tools. File lengths stay accurate across distant edits.
    let old_line_count = diff.get("old_line_count")?.as_u64()?;
    let new_line_count = diff.get("new_line_count")?.as_u64()?;
    let fields = receipt.as_object_mut()?;
    fields.remove("diff");
    fields.insert("old_line_count".into(), json!(old_line_count));
    fields.insert("new_line_count".into(), json!(new_line_count));
    let mut projected = receipt.to_string();
    projected.push_str(suffix);
    Some(projected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{FunctionCall, ToolCall};

    fn call(name: &str, id: &str) -> Message {
        Message::assistant(
            "",
            Some(vec![ToolCall {
                id: id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: name.to_string(),
                    arguments: "{}".to_string(),
                },
            }]),
        )
    }

    fn new_file_receipt() -> Value {
        json!({
            "operation": "Write",
            "file_path": "fixture.txt",
            "workspace": ".",
            "message": "Wrote file: fixture.txt",
            "checkpoint": {"created": false, "reason": "file_did_not_exist"},
            "diff": {"format": "unified", "unified": "+file content", "old_line_count": 0,
                     "new_line_count": 1}
        })
    }

    #[test]
    fn new_file_projection_includes_checkpoint_status_and_line_counts() {
        let content = new_file_receipt().to_string();
        let mut messages = [
            call("Write", "write"),
            Message::tool_result("write", &content),
        ];
        project_file_change_results(&mut messages);
        let summary: Value = serde_json::from_str(&messages[1].content).unwrap();
        assert_eq!(summary["checkpoint"]["created"], false);
        assert_eq!(summary["workspace"], ".");
        assert_eq!(summary["old_line_count"], 0);
        assert_eq!(summary["new_line_count"], 1);
        assert!(summary.get("diff").is_none());
    }

    #[test]
    fn projection_preserves_failed_ambiguous_and_other_tool_results() {
        let content = new_file_receipt().to_string();
        let mut messages = [
            call("mcp::Write", "qualified"),
            Message::tool_result("qualified", &content),
            call("Write", "failed"),
            Message::tool_result_with_status("failed", &content, false),
            call("Write", "duplicate"),
            Message::tool_result("duplicate", &content),
            call("Read", "duplicate"),
            Message::tool_result("orphan", &content),
            call("Edit", "other_operation"),
            Message::tool_result("other_operation", &content),
        ];
        project_file_change_results(&mut messages);
        for message in messages.iter().filter(|message| message.role == Role::Tool) {
            assert_eq!(message.content, content);
        }
    }
}
