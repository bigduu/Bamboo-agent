use crate::config::GoldConfig;
use crate::runtime::gold_evaluation::GoldEvaluationResult;
use crate::TaskLoopContext;
use bamboo_agent_core::{FunctionSchema, Message, PendingQuestion, Role, Session, ToolSchema};
use serde_json::json;

use super::decision::normalized_pending_tool_name;
use super::GOLD_AUTO_ANSWER_TOOL_NAME;

pub(crate) fn build_gold_auto_answer_messages(
    session: &Session,
    pending: &PendingQuestion,
    state_evaluation: &GoldEvaluationResult,
    gold_config: &GoldConfig,
) -> Vec<Message> {
    let mut messages = Vec::new();

    let mut system_prompt = String::from(
        "You are a cautious Gold auto-answer evaluator. Decide whether the server should automatically answer the pending clarification for the agent.\n\nRules:\n1. This is Phase 2 low-risk auto-answer only.\n2. Only auto-answer if the answer is clearly derivable from the current session and task context.\n3. Never auto-answer permissions, credentials, secrets, external auth, or ambiguous high-risk questions.\n4. If options are provided, choose one of the provided options verbatim.\n5. If you are unsure, return apply=false.\n6. Use confidence=high only when the answer is clearly low-risk.\n7. Call report_gold_auto_answer exactly once."
    );

    if let Some(extra) = gold_config
        .evaluation_prompt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        system_prompt.push_str("\n\nAdditional instructions:\n");
        system_prompt.push_str(extra);
    }

    messages.push(Message::system(system_prompt));

    let task_summary = TaskLoopContext::from_session(session)
        .map(|context| context.format_for_prompt())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "## Current Task List\nNo task list available.".to_string());

    let options_summary = if pending.options.is_empty() {
        "none".to_string()
    } else {
        pending
            .options
            .iter()
            .map(|option| format!("- {option}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let recent_messages = format_recent_messages(session, 6);

    let user_prompt = format!(
        "## Pending Question\nquestion={}\ntool={}\nnormalized_tool={}\nsource={:?}\nallow_custom={}\n\n## Options\n{}\n\n## Gold State Evaluation\ndecision={}\nconfidence={}\nreasoning={}\n\n{}\n\n## Recent Conversation\n{}\n\n## Instruction\nReturn apply=true only if you can safely choose the answer right now. When options exist, answer with the exact option text.",
        pending.question,
        pending.tool_name,
        normalized_pending_tool_name(&pending.tool_name),
        pending.source,
        pending.allow_custom,
        options_summary,
        state_evaluation.decision.as_str(),
        state_evaluation.confidence.as_str(),
        state_evaluation.reasoning,
        task_summary,
        recent_messages,
    );

    messages.push(Message::user(user_prompt));
    messages
}

fn format_recent_messages(session: &Session, limit: usize) -> String {
    let start = session.messages.len().saturating_sub(limit);
    let mut lines = Vec::new();

    for message in session.messages.iter().skip(start) {
        let role = match message.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        };

        let mut content = message.content.trim().replace('\n', " ");
        if content.chars().count() > 240 {
            content = format!("{}…", content.chars().take(240).collect::<String>());
        }
        if content.is_empty() {
            content = "<empty>".to_string();
        }

        lines.push(format!("- [{role}] {content}"));
    }

    if lines.is_empty() {
        "- <no messages>".to_string()
    } else {
        lines.join("\n")
    }
}

pub(crate) fn get_gold_auto_answer_tools() -> Vec<ToolSchema> {
    vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: GOLD_AUTO_ANSWER_TOOL_NAME.to_string(),
            description: "Report whether Gold should auto-answer the pending clarification"
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "apply": {
                        "type": "boolean"
                    },
                    "answer": {
                        "type": "string",
                        "description": "The exact answer text to submit when apply=true. Use an empty string when apply=false."
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["low", "medium", "high"]
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Short concrete reasoning for the decision"
                    }
                },
                "required": ["apply", "answer", "confidence", "reasoning"],
                "additionalProperties": false
            }),
        },
    }]
}
