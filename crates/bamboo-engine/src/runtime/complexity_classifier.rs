//! Task complexity classifier for dynamic model routing.
//!
//! Uses the fast model (via `MiniLoopExecutor`) to classify the current task
//! complexity at the end of each agent round. The classification result drives
//! per-round model selection in the agent pipeline.

use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, Session};

use crate::runtime::managers::mini_loop::MiniLoopExecutor;

/// Classification of task complexity for model routing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskComplexity {
    /// File reading, grep, listing, formatting — can use the cheapest model.
    Simple,
    /// Standard coding, debugging, writing tests — use the default model.
    Standard,
    /// Architecture decisions, multi-file refactoring, planning — use the most capable model.
    Complex,
}

/// Classifies task complexity using the fast model via `MiniLoopExecutor`.
pub struct ComplexityClassifier;

impl ComplexityClassifier {
    /// Classify task complexity based on the current round's tool calls.
    ///
    /// Returns `TaskComplexity::Standard` as a safe default when classification fails.
    pub async fn classify(
        executor: &dyn MiniLoopExecutor,
        session: &Session,
        tool_calls: &[ToolCall],
        round: usize,
    ) -> Result<TaskComplexity, AgentError> {
        let prompt = build_complexity_prompt(tool_calls, round);
        let context = extract_recent_context(session);
        let decision = executor.decide(session, &prompt, &context).await?;

        let answer = decision.answer.to_lowercase();
        if answer.contains("simple") {
            Ok(TaskComplexity::Simple)
        } else if answer.contains("complex") {
            Ok(TaskComplexity::Complex)
        } else {
            Ok(TaskComplexity::Standard)
        }
    }
}

fn build_complexity_prompt(tool_calls: &[ToolCall], round: usize) -> String {
    let tool_names: Vec<&str> = tool_calls
        .iter()
        .map(|tc| tc.function.name.as_str())
        .collect();
    format!(
        "Classify the current task complexity based on the last tool calls.\n\
         Tool calls this round: {}\n\
         Round number: {}\n\n\
         Respond with exactly one word: simple, standard, or complex.\n\
         - simple: file reading, grep, listing, formatting\n\
         - standard: editing code, debugging, writing tests\n\
         - complex: architectural decisions, multi-file refactoring, planning",
        tool_names.join(", "),
        round
    )
}

/// Extract the last few user/assistant messages as context for classification.
fn extract_recent_context(session: &Session) -> String {
    use bamboo_agent_core::Role;

    session
        .messages
        .iter()
        .rev()
        .filter(|m| matches!(m.role, Role::User | Role::Assistant))
        .take(4)
        .map(|m| {
            let role = match m.role {
                Role::User => "User",
                Role::Assistant => "Assistant",
                _ => "Other",
            };
            format!("{}: {}", role, truncate(&m.content, 200))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        // Find a safe boundary to avoid panicking on multi-byte chars.
        let mut end = max_len;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        &s[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_exact() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_long() {
        assert_eq!(truncate("hello world", 5), "hello");
    }

    #[test]
    fn test_build_complexity_prompt() {
        use bamboo_domain::session::tool_types::FunctionCall;
        let tc = ToolCall {
            id: "1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: "{}".to_string(),
            },
        };
        let prompt = build_complexity_prompt(&[tc], 3);
        assert!(prompt.contains("Read"));
        assert!(prompt.contains("3"));
        assert!(prompt.contains("simple"));
        assert!(prompt.contains("complex"));
    }
}
