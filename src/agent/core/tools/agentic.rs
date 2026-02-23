//! Agentic tool execution framework.
//!
//! This module provides a framework for implementing autonomous agent tools
//! that can iterate and interact with the environment through tool calls.
//!
//! # Overview
//!
//! The agentic system allows tools to:
//! - Execute multiple iterations toward a goal
//! - Maintain state across iterations
//! - Record interactions between user, assistant, and tools
//! - Perform autonomous code review and refinement
//!
//! # Key Types
//!
//! - [`ToolGoal`] - Defines the objective and parameters for an agentic tool
//! - [`AgenticContext`] - Execution context with state and interaction history
//! - [`Interaction`] - Records of user/assistant/tool exchanges
//! - [`ToolExecutor`] - Trait for executing tool calls
//! - [`AgenticTool`] - Trait for autonomous agentic tools
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::tools::agentic::*;
//!
//! let goal = ToolGoal::new("Review code for bugs", json!({"file": "main.rs"}));
//! let context = AgenticContext::new(executor);
//! let tool = SmartCodeReviewTool::new();
//! let result = tool.execute(goal, context).await?;
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::agent::core::tools::{FunctionCall, ToolCall, ToolError};

/// Represents the objective and parameters for an agentic tool execution.
///
/// A `ToolGoal` defines what an agentic tool should accomplish, including
/// the intent, input parameters, and iteration limits.
///
/// # Fields
///
/// * `intent` - High-level description of what the tool should achieve
/// * `params` - JSON parameters for the tool execution
/// * `max_iterations` - Maximum number of iterations before stopping (default: 10)
///
/// # Example
///
/// ```rust,ignore
/// let goal = ToolGoal::new("Fix all bugs", json!({"files": ["main.rs", "lib.rs"]}))
///     .with_max_iterations(20);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolGoal {
    pub intent: String,
    /// Input parameters for the tool execution
    pub params: Value,
    /// Maximum iterations before stopping
    pub max_iterations: usize,
}

impl ToolGoal {
    /// Create a new tool goal with intent and parameters.
    ///
    /// Uses the default maximum iteration count of 10.
    ///
    /// # Arguments
    ///
    /// * `intent` - High-level description of the goal
    /// * `params` - JSON parameters for execution
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let goal = ToolGoal::new("Review code", json!({"path": "src/main.rs"}));
    /// ```
    pub fn new(intent: impl Into<String>, params: Value) -> Self {
        Self {
            intent: intent.into(),
            params,
            max_iterations: 10,
        }
    }

    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InteractionRole {
    User,
    Assistant,
    Tool,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Interaction {
    User {
        message: String,
        timestamp: DateTime<Utc>,
    },
    Assistant {
        message: String,
        metadata: Option<Value>,
        timestamp: DateTime<Utc>,
    },
    ToolAction {
        call: ToolCall,
        timestamp: DateTime<Utc>,
    },
    ToolObservation {
        tool_name: String,
        output: String,
        timestamp: DateTime<Utc>,
    },
    System {
        message: String,
        timestamp: DateTime<Utc>,
    },
}

impl Interaction {
    fn from_role(
        role: InteractionRole,
        content: impl Into<String>,
        metadata: Option<Value>,
    ) -> Self {
        let content = content.into();
        let timestamp = Utc::now();

        match role {
            InteractionRole::User => Self::User {
                message: content,
                timestamp,
            },
            InteractionRole::Assistant => Self::Assistant {
                message: content,
                metadata,
                timestamp,
            },
            InteractionRole::Tool => Self::ToolObservation {
                tool_name: "agentic_tool".to_string(),
                output: content,
                timestamp,
            },
            InteractionRole::System => Self::System {
                message: content,
                timestamp,
            },
        }
    }
}

pub struct AgenticContext {
    pub state: Arc<RwLock<Value>>,
    pub interaction_history: Vec<Interaction>,
    pub base_executor: Arc<dyn ToolExecutor>,
    iteration_count: usize,
}

impl AgenticContext {
    pub fn new(base_executor: Arc<dyn ToolExecutor>) -> Self {
        Self {
            state: Arc::new(RwLock::new(serde_json::json!({}))),
            interaction_history: Vec::new(),
            base_executor,
            iteration_count: 0,
        }
    }

    pub fn with_state(base_executor: Arc<dyn ToolExecutor>, initial_state: Value) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
            interaction_history: Vec::new(),
            base_executor,
            iteration_count: 0,
        }
    }

    pub fn record_interaction(&mut self, role: InteractionRole, content: impl Into<String>) {
        self.interaction_history
            .push(Interaction::from_role(role, content, None));
    }

    pub fn record_interaction_with_metadata(
        &mut self,
        role: InteractionRole,
        content: impl Into<String>,
        metadata: Value,
    ) {
        self.interaction_history
            .push(Interaction::from_role(role, content, Some(metadata)));
    }

    pub fn record_tool_action(&mut self, call: ToolCall) {
        self.interaction_history.push(Interaction::ToolAction {
            call,
            timestamp: Utc::now(),
        });
    }

    pub fn record_tool_observation(
        &mut self,
        tool_name: impl Into<String>,
        output: impl Into<String>,
    ) {
        self.interaction_history.push(Interaction::ToolObservation {
            tool_name: tool_name.into(),
            output: output.into(),
            timestamp: Utc::now(),
        });
    }

    pub fn increment_iteration(&mut self, max_iterations: usize) -> bool {
        self.iteration_count += 1;
        self.iteration_count > max_iterations
    }

    pub fn iteration_count(&self) -> usize {
        self.iteration_count
    }

    pub fn is_first_iteration(&self) -> bool {
        self.iteration_count == 0
    }

    pub async fn read_state(&self) -> tokio::sync::RwLockReadGuard<'_, Value> {
        self.state.read().await
    }

    pub async fn write_state(&self) -> tokio::sync::RwLockWriteGuard<'_, Value> {
        self.state.write().await
    }

    pub async fn update_state(&self, new_state: Value) {
        let mut state = self.state.write().await;
        *state = new_state;
    }

    pub async fn merge_state(&self, partial: Value) {
        let mut state = self.state.write().await;
        if let Value::Object(ref mut existing) = *state {
            if let Value::Object(partial_map) = partial {
                for (key, value) in partial_map {
                    existing.insert(key, value);
                }
            }
        }
    }
}

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ToolResult {
    Success {
        result: String,
    },
    Error {
        error: String,
    },
    NeedClarification {
        question: String,
        options: Option<Vec<String>>,
    },
    NeedMoreActions {
        actions: Vec<ToolCall>,
        reason: String,
    },
}

pub type AgenticToolResult = ToolResult;

impl ToolResult {
    pub fn success(result: impl Into<String>) -> Self {
        Self::Success {
            result: result.into(),
        }
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
        }
    }

    pub fn need_clarification(question: impl Into<String>) -> Self {
        Self::NeedClarification {
            question: question.into(),
            options: None,
        }
    }

    pub fn need_clarification_with_options(
        question: impl Into<String>,
        options: Vec<String>,
    ) -> Self {
        Self::NeedClarification {
            question: question.into(),
            options: Some(options),
        }
    }

    pub fn need_more_actions(actions: Vec<ToolCall>, reason: impl Into<String>) -> Self {
        Self::NeedMoreActions {
            actions,
            reason: reason.into(),
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    pub fn needs_clarification(&self) -> bool {
        matches!(self, Self::NeedClarification { .. })
    }

    pub fn needs_more_actions(&self) -> bool {
        matches!(self, Self::NeedMoreActions { .. })
    }
}

#[async_trait]
pub trait AgenticTool: Send + Sync {
    fn name(&self) -> &str;

    fn description(&self) -> &str;

    async fn execute(
        &self,
        goal: ToolGoal,
        context: &mut AgenticContext,
    ) -> Result<ToolResult, ToolError>;
}

pub fn convert_to_standard_result(
    agentic_result: ToolResult,
) -> crate::agent::core::tools::types::ToolResult {
    match agentic_result {
        ToolResult::Success { result } => crate::agent::core::tools::types::ToolResult {
            success: true,
            result,
            display_preference: None,
        },
        ToolResult::Error { error } => crate::agent::core::tools::types::ToolResult {
            success: false,
            result: error,
            display_preference: Some("error".to_string()),
        },
        ToolResult::NeedClarification { question, .. } => {
            crate::agent::core::tools::types::ToolResult {
                success: true,
                result: question,
                display_preference: Some("clarification".to_string()),
            }
        }
        ToolResult::NeedMoreActions { reason, .. } => {
            crate::agent::core::tools::types::ToolResult {
                success: true,
                result: reason,
                display_preference: Some("actions_needed".to_string()),
            }
        }
    }
}

pub fn convert_from_standard_result(
    standard_result: crate::agent::core::tools::types::ToolResult,
) -> ToolResult {
    if standard_result.success {
        match standard_result.display_preference.as_deref() {
            Some("clarification") => ToolResult::NeedClarification {
                question: standard_result.result,
                options: None,
            },
            Some("actions_needed") => ToolResult::NeedMoreActions {
                actions: Vec::new(),
                reason: standard_result.result,
            },
            _ => ToolResult::Success {
                result: standard_result.result,
            },
        }
    } else {
        ToolResult::Error {
            error: standard_result.result,
        }
    }
}

pub struct SmartCodeReviewTool {
    name: String,
    description: String,
}

impl Default for SmartCodeReviewTool {
    fn default() -> Self {
        Self {
            name: "smart_code_review".to_string(),
            description: "Autonomous reviewer that chooses review strategy, asks clarifying questions, and plans follow-up tool actions.".to_string(),
        }
    }
}

impl SmartCodeReviewTool {
    pub fn new() -> Self {
        Self::default()
    }

    fn collect_findings(&self, content: &str) -> (Vec<String>, bool) {
        let mut findings = Vec::new();
        let mut has_critical = false;

        if content.contains("unsafe ") {
            has_critical = true;
            findings.push("critical: unsafe block detected".to_string());
        }

        if content.contains("unwrap()") {
            findings.push("warning: unwrap() detected".to_string());
        }

        if content.contains("todo!") || content.contains("TODO") {
            findings.push("warning: unresolved TODO detected".to_string());
        }

        let long_line_count = content.lines().filter(|line| line.len() > 120).count();
        if long_line_count > 0 {
            findings.push(format!(
                "warning: {} line(s) exceed 120 characters",
                long_line_count
            ));
        }

        (findings, has_critical)
    }

    fn choose_strategy(&self, content: &str, findings: &[String]) -> &'static str {
        let line_count = content.lines().count();

        if line_count > 300 || findings.len() > 3 {
            "deep"
        } else if line_count > 80 || !findings.is_empty() {
            "standard"
        } else {
            "quick"
        }
    }

    fn build_actions(&self, file_path: Option<&str>) -> Vec<ToolCall> {
        let path_hint = file_path.unwrap_or("<unknown-file>");

        vec![
            ToolCall {
                id: "agentic-action-1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "execute_command".to_string(),
                    arguments: serde_json::json!({
                        "command": format!("cargo clippy --all-targets --all-features -- {}", path_hint)
                    })
                    .to_string(),
                },
            },
            ToolCall {
                id: "agentic-action-2".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "execute_command".to_string(),
                    arguments: serde_json::json!({
                        "command": "cargo test"
                    })
                    .to_string(),
                },
            },
        ]
    }
}

#[async_trait]
impl AgenticTool for SmartCodeReviewTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn execute(
        &self,
        goal: ToolGoal,
        context: &mut AgenticContext,
    ) -> Result<ToolResult, ToolError> {
        context.record_interaction(
            InteractionRole::System,
            format!("Start smart review for intent: {}", goal.intent),
        );

        let content = goal.params.get("content").and_then(Value::as_str);
        if content.is_none() {
            return Ok(ToolResult::need_clarification_with_options(
                "I need the source code content to run a review. Please provide `content`."
                    .to_string(),
                vec![
                    "Paste the full file content".to_string(),
                    "Provide a trimmed snippet and target concerns".to_string(),
                ],
            ));
        }

        let content = content.unwrap_or_default();
        if content.trim().is_empty() {
            return Ok(ToolResult::need_clarification(
                "The provided content is empty. Please share non-empty code.".to_string(),
            ));
        }

        let first_iteration = context.is_first_iteration();

        if context.increment_iteration(goal.max_iterations) {
            return Ok(ToolResult::error(format!(
                "max_iterations reached: {}",
                goal.max_iterations
            )));
        }

        let file_path = goal.params.get("file_path").and_then(Value::as_str);
        let (findings, has_critical) = self.collect_findings(content);
        let strategy = self.choose_strategy(content, &findings).to_string();

        context
            .merge_state(serde_json::json!({
                "intent": goal.intent,
                "strategy": strategy,
                "findings": findings,
                "line_count": content.lines().count(),
            }))
            .await;

        if has_critical && first_iteration {
            return Ok(ToolResult::need_clarification_with_options(
                "I found critical safety signals (for example `unsafe`). Should I continue with defensive recommendations or stop for manual review?".to_string(),
                vec![
                    "Continue with defensive recommendations".to_string(),
                    "Stop and return only critical issues".to_string(),
                ],
            ));
        }

        if strategy == "deep" {
            let actions = self.build_actions(file_path);
            let auto_execute = goal
                .params
                .get("auto_execute_actions")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            if auto_execute {
                if let Some(first_action) = actions.first() {
                    context.record_tool_action(first_action.clone());
                    let action_result = context.base_executor.execute(first_action).await?;
                    context.record_tool_observation(
                        first_action.function.name.as_str(),
                        format!("{:?}", action_result),
                    );
                }
            } else {
                return Ok(ToolResult::need_more_actions(
                    actions,
                    "Deep review requires lint/test evidence before final verdict.",
                ));
            }
        }

        let summary = serde_json::json!({
            "strategy": strategy,
            "finding_count": findings.len(),
            "findings": findings,
        });

        Ok(ToolResult::success(
            serde_json::to_string_pretty(&summary).unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor;

    #[async_trait]
    impl ToolExecutor for MockExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            Ok(ToolResult::success("mock-executed"))
        }
    }

    #[test]
    fn tool_goal_should_store_intent_params_and_iteration_limit() {
        let goal = ToolGoal::new(
            "review rust",
            serde_json::json!({ "content": "fn main() {}" }),
        )
        .with_max_iterations(3);

        assert_eq!(goal.intent, "review rust");
        assert_eq!(goal.max_iterations, 3);
        assert_eq!(goal.params["content"], "fn main() {}");
    }

    #[test]
    fn interaction_history_should_use_enum_variants() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        context.record_interaction(InteractionRole::User, "Review this function");

        assert_eq!(context.interaction_history.len(), 1);
        assert!(matches!(
            &context.interaction_history[0],
            Interaction::User { message, .. } if message == "Review this function"
        ));
    }

    #[test]
    fn tool_result_should_support_new_agentic_variants() {
        let clarification = ToolResult::need_clarification_with_options(
            "Which standard should I follow?",
            vec!["Rust style".to_string(), "Project style".to_string()],
        );
        assert!(clarification.needs_clarification());

        let actions = ToolResult::need_more_actions(
            vec![ToolCall {
                id: "1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "execute_command".to_string(),
                    arguments: "{}".to_string(),
                },
            }],
            "Need more evidence",
        );
        assert!(actions.needs_more_actions());
    }

    #[tokio::test]
    async fn smart_review_should_ask_for_content_when_missing() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        let goal = ToolGoal::new("review", serde_json::json!({}));
        let result = tool.execute(goal, &mut context).await.unwrap();

        assert!(result.needs_clarification());
    }

    #[tokio::test]
    async fn smart_review_should_request_more_actions_for_deep_review() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        let large_code = (0..360)
            .map(|idx| format!("fn f{}() {{ println!(\"{}\") }}", idx, idx))
            .collect::<Vec<_>>()
            .join("\n");

        let goal = ToolGoal::new(
            "review",
            serde_json::json!({
                "file_path": "src/lib.rs",
                "content": large_code,
            }),
        );

        let result = tool.execute(goal, &mut context).await.unwrap();
        assert!(result.needs_more_actions());
    }

    #[tokio::test]
    async fn smart_review_should_return_success_for_small_clean_code() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        let goal = ToolGoal::new(
            "review",
            serde_json::json!({
                "content": "fn sum(a: i32, b: i32) -> i32 { a + b }",
            }),
        )
        .with_max_iterations(5);

        let result = tool.execute(goal, &mut context).await.unwrap();
        assert!(result.is_success());
    }
}
