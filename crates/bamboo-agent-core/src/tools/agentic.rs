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
//! - [`AgenticToolExecutor`] - Trait for executing tool calls
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

use crate::tools::{FunctionCall, ToolCall, ToolError};

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

    /// Set custom maximum iteration count.
    ///
    /// # Arguments
    ///
    /// * `max_iterations` - Maximum iterations allowed
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let goal = ToolGoal::new("Complex task", params)
    ///     .with_max_iterations(50);
    /// ```
    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }
}

/// Role of an interaction participant.
///
/// Identifies who generated an interaction in the agentic loop.
///
/// # Variants
///
/// * `User` - Human user input
/// * `Assistant` - AI assistant response
/// * `Tool` - Tool execution result
/// * `System` - System message
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InteractionRole {
    User,
    Assistant,
    Tool,
    System,
}

/// Represents a single interaction in the agentic execution loop.
///
/// Records exchanges between user, assistant, and tools during
/// autonomous agent execution.
///
/// # Variants
///
/// * `User` - User message with timestamp
/// * `Assistant` - Assistant response with optional metadata
/// * `ToolAction` - Tool call initiated
/// * `ToolObservation` - Tool execution result
/// * `System` - System message
///
/// # Example
///
/// ```rust,ignore
/// let user_msg = Interaction::User {
///     message: "Fix the bug".to_string(),
///     timestamp: Utc::now(),
/// };
///
/// let tool_action = Interaction::ToolAction {
///     call: tool_call,
///     timestamp: Utc::now(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Interaction {
    /// User message
    User {
        /// Message content from user
        message: String,
        /// When the message was sent
        timestamp: DateTime<Utc>,
    },
    /// Assistant response
    Assistant {
        /// Response message
        message: String,
        /// Optional metadata (e.g., confidence, reasoning)
        metadata: Option<Value>,
        /// When the response was generated
        timestamp: DateTime<Utc>,
    },
    /// Tool call initiated
    ToolAction {
        /// The tool call being made
        call: ToolCall,
        /// When the call was initiated
        timestamp: DateTime<Utc>,
    },
    /// Tool execution result
    ToolObservation {
        /// Name of the tool that was executed
        tool_name: String,
        /// Output/result from the tool
        output: String,
        /// When the observation was made
        timestamp: DateTime<Utc>,
    },
    /// System message
    System {
        /// System message content
        message: String,
        /// When the message was generated
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

/// Execution context for agentic tools.
///
/// Maintains state, interaction history, and tool executor reference
/// for autonomous agent execution loops.
///
/// # Fields
///
/// * `state` - Shared mutable state for cross-iteration persistence
/// * `interaction_history` - Record of all interactions
/// * `base_executor` - Tool executor for running tools
///
/// # Thread Safety
///
/// The `state` field uses `Arc<RwLock<Value>>` for thread-safe
/// concurrent access from multiple async tasks.
///
/// # Example
///
/// ```rust,ignore
/// let executor = Arc::new(BuiltinToolExecutor::new());
/// let context = AgenticContext::new(executor);
///
/// // Record interactions
/// context.record_interaction(InteractionRole::User, "Hello");
///
/// // Update state
/// context.update_state(json!({"step": 1})).await;
///
/// // Check iterations
/// if !context.increment_iteration(10) {
///     // Continue execution
/// }
/// ```
pub struct AgenticContext {
    /// Shared mutable state (thread-safe)
    pub state: Arc<RwLock<Value>>,
    /// History of all interactions
    pub interaction_history: Vec<Interaction>,
    /// Tool executor reference
    pub base_executor: Arc<dyn AgenticToolExecutor>,
    /// Current iteration count
    iteration_count: usize,
}

impl AgenticContext {
    /// Create a new agentic context with empty state.
    ///
    /// # Arguments
    ///
    /// * `base_executor` - Tool executor for running tools
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let executor = Arc::new(MyExecutor::new());
    /// let context = AgenticContext::new(executor);
    /// ```
    pub fn new(base_executor: Arc<dyn AgenticToolExecutor>) -> Self {
        Self {
            state: Arc::new(RwLock::new(serde_json::json!({}))),
            interaction_history: Vec::new(),
            base_executor,
            iteration_count: 0,
        }
    }

    /// Create context with custom initial state.
    ///
    /// # Arguments
    ///
    /// * `base_executor` - Tool executor
    /// * `initial_state` - Initial state value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = json!({"files": ["main.rs"]});
    /// let context = AgenticContext::with_state(executor, state);
    /// ```
    pub fn with_state(base_executor: Arc<dyn AgenticToolExecutor>, initial_state: Value) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
            interaction_history: Vec::new(),
            base_executor,
            iteration_count: 0,
        }
    }

    /// Record a simple interaction without metadata.
    ///
    /// # Arguments
    ///
    /// * `role` - Role of the interaction source
    /// * `content` - Interaction content
    pub fn record_interaction(&mut self, role: InteractionRole, content: impl Into<String>) {
        self.interaction_history
            .push(Interaction::from_role(role, content, None));
    }

    /// Record an interaction with metadata.
    ///
    /// # Arguments
    ///
    /// * `role` - Role of the interaction source
    /// * `content` - Interaction content
    /// * `metadata` - Additional metadata
    pub fn record_interaction_with_metadata(
        &mut self,
        role: InteractionRole,
        content: impl Into<String>,
        metadata: Value,
    ) {
        self.interaction_history
            .push(Interaction::from_role(role, content, Some(metadata)));
    }

    /// Record a tool call action.
    ///
    /// # Arguments
    ///
    /// * `call` - Tool call to record
    pub fn record_tool_action(&mut self, call: ToolCall) {
        self.interaction_history.push(Interaction::ToolAction {
            call,
            timestamp: Utc::now(),
        });
    }

    /// Record a tool execution result.
    ///
    /// # Arguments
    ///
    /// * `tool_name` - Name of the tool
    /// * `output` - Tool execution output
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

    /// Increment iteration count and check limit.
    ///
    /// # Arguments
    ///
    /// * `max_iterations` - Maximum allowed iterations
    ///
    /// # Returns
    ///
    /// `true` if limit exceeded, `false` if within limit
    pub fn increment_iteration(&mut self, max_iterations: usize) -> bool {
        self.iteration_count += 1;
        self.iteration_count > max_iterations
    }

    /// Get current iteration count.
    pub fn iteration_count(&self) -> usize {
        self.iteration_count
    }

    /// Check if this is the first iteration.
    pub fn is_first_iteration(&self) -> bool {
        self.iteration_count == 0
    }

    /// Read shared state asynchronously.
    ///
    /// Returns a read guard that releases when dropped.
    pub async fn read_state(&self) -> tokio::sync::RwLockReadGuard<'_, Value> {
        self.state.read().await
    }

    /// Write shared state asynchronously.
    ///
    /// Returns a write guard that releases when dropped.
    pub async fn write_state(&self) -> tokio::sync::RwLockWriteGuard<'_, Value> {
        self.state.write().await
    }

    /// Replace state with a new value.
    ///
    /// # Arguments
    ///
    /// * `new_state` - New state value
    pub async fn update_state(&self, new_state: Value) {
        let mut state = self.state.write().await;
        *state = new_state;
    }

    /// Merge partial state into existing state.
    ///
    /// Only updates keys that exist in the partial value.
    /// Requires both existing and partial states to be JSON objects.
    ///
    /// # Arguments
    ///
    /// * `partial` - Partial state to merge
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

/// Trait for executing tool calls in agentic context.
///
/// Implement this trait to provide custom tool execution logic
/// for agentic tools.
///
/// # Required Methods
///
/// - `execute` - Execute a tool call
///
/// # Example
///
/// ```rust,ignore
/// struct MyExecutor;
///
/// #[async_trait]
/// impl AgenticToolExecutor for MyExecutor {
///     async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
///         // Execute tool and return result
///         Ok(ToolResult::success("Done"))
///     }
/// }
/// ```
#[async_trait]
pub trait AgenticToolExecutor: Send + Sync {
    /// Execute a tool call.
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError>;
}

/// Result from agentic tool execution.
///
/// Indicates the outcome of an agentic tool's execution step.
///
/// # Variants
///
/// * `Success` - Task completed successfully
/// * `Error` - Execution failed
/// * `NeedClarification` - Need user input
/// * `NeedMoreActions` - More tool calls needed
///
/// # Example
///
/// ```rust,ignore
/// let result = ToolResult::success("Task completed");
/// let error = ToolResult::error("Failed to execute");
/// let clarify = ToolResult::need_clarification("Which file?");
/// let more = ToolResult::need_more_actions(vec![tool_call], "Need to read file");
/// ```
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
        /// Tool calls to execute
        actions: Vec<ToolCall>,
        /// Reason for needing more actions
        reason: String,
    },
}

/// Type alias for agentic tool results.
pub type AgenticToolResult = ToolResult;

impl ToolResult {
    /// Create a success result.
    pub fn success(result: impl Into<String>) -> Self {
        Self::Success {
            result: result.into(),
        }
    }

    /// Create an error result.
    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
        }
    }

    /// Create a clarification request without options.
    pub fn need_clarification(question: impl Into<String>) -> Self {
        Self::NeedClarification {
            question: question.into(),
            options: None,
        }
    }

    /// Create a clarification request with predefined options.
    pub fn need_clarification_with_options(
        question: impl Into<String>,
        options: Vec<String>,
    ) -> Self {
        Self::NeedClarification {
            question: question.into(),
            options: Some(options),
        }
    }

    /// Request more tool executions.
    pub fn need_more_actions(actions: Vec<ToolCall>, reason: impl Into<String>) -> Self {
        Self::NeedMoreActions {
            actions,
            reason: reason.into(),
        }
    }

    /// Check if result is successful.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Check if result is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Check if clarification is needed.
    pub fn needs_clarification(&self) -> bool {
        matches!(self, Self::NeedClarification { .. })
    }

    /// Check if more actions are needed.
    pub fn needs_more_actions(&self) -> bool {
        matches!(self, Self::NeedMoreActions { .. })
    }
}

/// Trait for implementing autonomous agentic tools.
///
/// Agentic tools can execute multiple iterations, maintain state,
/// and make autonomous decisions about which tools to call.
///
/// # Required Methods
///
/// - `name()` - Tool identifier
/// - `description()` - Tool description
/// - `execute()` - Execute the agentic tool
///
/// # Difference from Regular Tools
///
/// Unlike regular [`Tool`](crate::tools::registry::Tool) trait:
/// - Receives a [`ToolGoal`] instead of direct arguments
/// - Has access to [`AgenticContext`] for state and history
/// - Can execute multiple iterations
/// - Returns [`ToolResult`] with continuation options
///
/// # Example
///
/// ```rust,ignore
/// struct SmartCodeReviewTool;
///
/// #[async_trait]
/// impl AgenticTool for SmartCodeReviewTool {
///     fn name(&self) -> &str {
///         "smart_code_review"
///     }
///
///     fn description(&self) -> &str {
///         "Autonomously review and fix code issues"
///     }
///
///     async fn execute(
///         &self,
///         goal: ToolGoal,
///         context: &mut AgenticContext,
///     ) -> Result<ToolResult, ToolError> {
///         // Iterate until goal achieved
///         loop {
///             if context.increment_iteration(goal.max_iterations) {
///                 return Ok(ToolResult::error("Max iterations reached"));
///             }
///
///             // Analyze code and decide actions
///             // Execute tools via context.base_executor
///             // Update state and record interactions
///
///             if goal_achieved(&context).await {
///                 return Ok(ToolResult::success("Review complete"));
///             }
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait AgenticTool: Send + Sync {
    /// Get tool name.
    fn name(&self) -> &str;

    /// Get tool description.
    fn description(&self) -> &str;

    /// Execute the agentic tool with goal and context.
    ///
    /// # Arguments
    ///
    /// * `goal` - Execution goal and parameters
    /// * `context` - Execution context with state and history
    ///
    /// # Returns
    ///
    /// Tool execution result indicating success, error, or need for clarification.
    async fn execute(
        &self,
        goal: ToolGoal,
        context: &mut AgenticContext,
    ) -> Result<ToolResult, ToolError>;
}

/// Convert agentic tool result to standard tool result.
///
/// Maps [`ToolResult`] from this module to the standard
/// [`crate::tools::types::ToolResult`] format.
///
/// # Arguments
///
/// * `agentic_result` - Agentic tool result to convert
///
/// # Returns
///
/// Standard tool result with appropriate display preferences.
///
/// # Mapping
///
/// - `Success` → `success: true, display_preference: None`
/// - `Error` → `success: false, display_preference: "error"`
/// - `NeedClarification` → `success: true, display_preference: "clarification"`
/// - `NeedMoreActions` → `success: true, display_preference: "continuation"`
pub fn convert_to_standard_result(
    agentic_result: ToolResult,
) -> crate::tools::types::ToolResult {
    match agentic_result {
        ToolResult::Success { result } => crate::tools::types::ToolResult {
            success: true,
            result,
            display_preference: None,
        },
        ToolResult::Error { error } => crate::tools::types::ToolResult {
            success: false,
            result: error,
            display_preference: Some("error".to_string()),
        },
        ToolResult::NeedClarification { question, .. } => {
            crate::tools::types::ToolResult {
                success: true,
                result: question,
                display_preference: Some("clarification".to_string()),
            }
        }
        ToolResult::NeedMoreActions { reason, .. } => {
            crate::tools::types::ToolResult {
                success: true,
                result: reason,
                display_preference: Some("actions_needed".to_string()),
            }
        }
    }
}

/// Convert standard tool result to agentic tool result.
///
/// Inverse of [`convert_to_standard_result`].
///
/// # Arguments
///
/// * `standard_result` - Standard tool result to convert
///
/// # Returns
///
/// Agentic tool result based on success and display preference.
///
/// # Mapping
///
/// - `success: true` + `"clarification"` → `NeedClarification`
/// - `success: true` + `"actions_needed"` → `NeedMoreActions`
/// - `success: true` + other → `Success`
/// - `success: false` → `Error`
pub fn convert_from_standard_result(
    standard_result: crate::tools::types::ToolResult,
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

/// Smart code review tool with autonomous execution.
///
/// An agentic tool that autonomously reviews code, identifies issues,
/// and plans fixes using multiple tool calls.
///
/// # Capabilities
///
/// - Chooses appropriate review strategy
/// - Asks clarifying questions when needed
/// - Plans and executes follow-up actions
/// - Tracks progress across iterations
///
/// # Example
///
/// ```rust,ignore
/// use bamboo_agent::agent::core::tools::agentic::*;
///
/// let tool = SmartCodeReviewTool::new();
/// let executor = Arc::new(BuiltinToolExecutor::new());
/// let mut context = AgenticContext::new(executor);
///
/// let goal = ToolGoal::new(
///     "Review code for bugs",
///     json!({"files": ["src/main.rs"]})
/// );
///
/// let result = tool.execute(goal, &mut context).await?;
/// ```
pub struct SmartCodeReviewTool {
    /// Tool name
    name: String,
    /// Tool description
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
    /// Create a new smart code review tool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect code findings from content analysis.
    ///
    /// Analyzes code for common issues and patterns.
    ///
    /// # Arguments
    ///
    /// * `content` - Code content to analyze
    ///
    /// # Returns
    ///
    /// Tuple of (findings list, has_critical_issues)
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

    /// Choose review strategy based on content and findings.
    ///
    /// # Arguments
    ///
    /// * `content` - Code content
    /// * `findings` - List of findings
    ///
    /// # Returns
    ///
    /// Strategy name: "quick", "standard", or "deep"
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

    /// Build follow-up tool actions for code review.
    ///
    /// Creates tool calls for running linters and tests.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Optional file path to review
    ///
    /// # Returns
    ///
    /// List of tool calls to execute
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
    impl AgenticToolExecutor for MockExecutor {
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
        let executor: Arc<dyn AgenticToolExecutor> = Arc::new(MockExecutor);
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
        let executor: Arc<dyn AgenticToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        let goal = ToolGoal::new("review", serde_json::json!({}));
        let result = tool.execute(goal, &mut context).await.unwrap();

        assert!(result.needs_clarification());
    }

    #[tokio::test]
    async fn smart_review_should_request_more_actions_for_deep_review() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn AgenticToolExecutor> = Arc::new(MockExecutor);
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
        let executor: Arc<dyn AgenticToolExecutor> = Arc::new(MockExecutor);
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
