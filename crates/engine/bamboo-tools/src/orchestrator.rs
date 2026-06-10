//! Tool execution orchestrator.
//!
//! Inspired by Codex's `ToolOrchestrator`, this module provides a structured
//! pipeline for tool execution: approval → execute → retry on failure.
//!
//! The orchestrator is designed to sit between the agent loop and the
//! `ToolExecutor`, adding safety controls without modifying individual tools.

use std::sync::Arc;
use std::time::Duration;

use crate::events::ToolEmitter;
pub use bamboo_agent_core::{classify_tool, ToolMutability};
use bamboo_agent_core::{ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult};

/// Configuration for the orchestrator.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Maximum number of retry attempts on transient failures.
    pub max_retries: usize,
    /// Delay between retry attempts.
    pub retry_delay: Duration,
    /// Whether to auto-approve read-only tools.
    pub auto_approve_readonly: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            retry_delay: Duration::from_millis(500),
            auto_approve_readonly: true,
        }
    }
}

/// Outcome of an orchestrated tool execution.
#[derive(Debug)]
pub struct OrchestratorResult {
    /// The tool result (if execution succeeded).
    pub result: Result<ToolResult, ToolError>,
    /// Number of attempts made.
    pub attempts: usize,
    /// Whether the execution was auto-approved.
    pub auto_approved: bool,
    /// The emitter with all lifecycle events recorded.
    pub emitter: ToolEmitter,
}

/// Determine if an error is transient and worth retrying.
fn is_transient_error(error: &ToolError) -> bool {
    match error {
        ToolError::Execution(msg) => {
            msg.contains("timed out")
                || msg.contains("connection refused")
                || msg.contains("temporarily unavailable")
        }
        _ => false,
    }
}

/// The tool orchestrator manages the approval → execute → retry pipeline.
///
/// It wraps a `ToolExecutor` and adds:
/// - Tool mutability classification
/// - Auto-approval for read-only tools
/// - Retry logic for transient failures
/// - Structured event emission via `ToolEmitter`
pub struct ToolOrchestrator {
    config: OrchestratorConfig,
}

impl ToolOrchestrator {
    pub fn new() -> Self {
        Self {
            config: OrchestratorConfig::default(),
        }
    }

    pub fn with_config(config: OrchestratorConfig) -> Self {
        Self { config }
    }

    /// Execute a tool call through the orchestrated pipeline.
    ///
    /// Steps:
    /// 1. Classify tool mutability
    /// 2. Auto-approve read-only tools (or check permissions for mutating ones)
    /// 3. Execute with retry on transient errors
    /// 4. Record all lifecycle events
    pub async fn run(
        &self,
        call: &ToolCall,
        executor: &Arc<dyn ToolExecutor>,
        ctx: ToolExecutionContext<'_>,
    ) -> OrchestratorResult {
        let tool_name = call.function.name.trim();
        let mutability = executor.call_mutability(call);
        let is_mutating = mutability == ToolMutability::Mutating;
        let auto_approved = !is_mutating && self.config.auto_approve_readonly;

        let mut emitter = ToolEmitter::new(&call.id, tool_name, is_mutating);
        emitter.set_auto_approved(auto_approved);
        emitter.begin();

        let mut attempts = 0;
        let mut last_result: Result<ToolResult, ToolError> = Err(ToolError::Execution(
            "No execution attempt made".to_string(),
        ));

        for attempt in 0..=self.config.max_retries {
            attempts = attempt + 1;

            match executor.execute_with_context(call, ctx).await {
                Ok(result) => {
                    emitter.finish(Some(format!("Completed in {} attempt(s)", attempts)));
                    return OrchestratorResult {
                        result: Ok(result),
                        attempts,
                        auto_approved,
                        emitter,
                    };
                }
                Err(err) => {
                    if attempt < self.config.max_retries && is_transient_error(&err) {
                        tracing::warn!(
                            tool_name = tool_name,
                            call_id = call.id.as_str(),
                            attempt = attempt + 1,
                            max_retries = self.config.max_retries,
                            error = %err,
                            "Transient tool error, retrying..."
                        );
                        tokio::time::sleep(self.config.retry_delay).await;
                        continue;
                    }
                    last_result = Err(err);
                    break;
                }
            }
        }

        // Record error
        if let Err(ref err) = last_result {
            emitter.error(format!("{}", err));
        }

        OrchestratorResult {
            result: last_result,
            attempts,
            auto_approved,
            emitter,
        }
    }
}

impl Default for ToolOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::ToolEventPhase;
    use crate::BuiltinToolExecutor;
    use async_trait::async_trait;
    use bamboo_agent_core::{FunctionCall, ToolSchema};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "test_call".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    struct MockExecutor {
        call_count: AtomicUsize,
        fail_first_n: usize,
        error_msg: String,
    }

    impl MockExecutor {
        fn new(fail_first_n: usize) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                fail_first_n,
                error_msg: "timed out".to_string(),
            }
        }

        fn permanent_fail() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                fail_first_n: 999,
                error_msg: "permanent failure".to_string(),
            }
        }
    }

    #[async_trait]
    impl ToolExecutor for MockExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.execute_with_context(_call, ToolExecutionContext::none("test"))
                .await
        }

        async fn execute_with_context(
            &self,
            _call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            if count < self.fail_first_n {
                Err(ToolError::Execution(self.error_msg.clone()))
            } else {
                Ok(ToolResult {
                    success: true,
                    result: "ok".to_string(),
                    display_preference: None,
                    images: Vec::new(),
                })
            }
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![]
        }
    }

    #[test]
    fn test_classify_readonly() {
        assert_eq!(classify_tool("Read"), ToolMutability::ReadOnly);
        assert_eq!(classify_tool("Grep"), ToolMutability::ReadOnly);
        assert_eq!(classify_tool("Glob"), ToolMutability::ReadOnly);
        assert_eq!(classify_tool("WebSearch"), ToolMutability::ReadOnly);
        assert_eq!(classify_tool("Sleep"), ToolMutability::ReadOnly);
    }

    #[test]
    fn test_classify_mutating() {
        assert_eq!(classify_tool("Write"), ToolMutability::Mutating);
        assert_eq!(classify_tool("Edit"), ToolMutability::Mutating);
        assert_eq!(classify_tool("Bash"), ToolMutability::Mutating);
        assert_eq!(classify_tool("KillShell"), ToolMutability::Mutating);
    }

    #[tokio::test]
    async fn test_orchestrator_success_first_try() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor::new(0));
        let orch = ToolOrchestrator::new();
        let call = make_call("Read", json!({"file_path": "/tmp/test"}));
        let ctx = ToolExecutionContext::none("test");

        let result = orch.run(&call, &executor, ctx).await;
        assert!(result.result.is_ok());
        assert_eq!(result.attempts, 1);
        assert!(result.auto_approved); // Read is read-only
        assert_eq!(result.emitter.events().len(), 2); // begin + finish
    }

    #[tokio::test]
    async fn test_orchestrator_retry_on_transient_error() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor::new(1));
        let config = OrchestratorConfig {
            max_retries: 2,
            retry_delay: Duration::from_millis(10),
            auto_approve_readonly: true,
        };
        let orch = ToolOrchestrator::with_config(config);
        let call = make_call("Bash", json!({"command": "echo hi"}));
        let ctx = ToolExecutionContext::none("test");

        let result = orch.run(&call, &executor, ctx).await;
        assert!(result.result.is_ok());
        assert_eq!(result.attempts, 2); // first fail + second success
        assert!(!result.auto_approved); // Bash is mutating
    }

    #[tokio::test]
    async fn test_orchestrator_permanent_failure_no_retry() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor::permanent_fail());
        let config = OrchestratorConfig {
            max_retries: 2,
            retry_delay: Duration::from_millis(10),
            auto_approve_readonly: true,
        };
        let orch = ToolOrchestrator::with_config(config);
        let call = make_call("Write", json!({"file_path": "/tmp/test", "content": "x"}));
        let ctx = ToolExecutionContext::none("test");

        let result = orch.run(&call, &executor, ctx).await;
        assert!(result.result.is_err());
        assert_eq!(result.attempts, 1); // no retry for permanent failure
        let events = result.emitter.events();
        assert_eq!(events.last().unwrap().phase, ToolEventPhase::Error);
    }

    #[tokio::test]
    async fn test_orchestrator_exhaust_retries() {
        // All attempts fail with transient error
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor::new(999));
        let config = OrchestratorConfig {
            max_retries: 2,
            retry_delay: Duration::from_millis(10),
            auto_approve_readonly: true,
        };
        let orch = ToolOrchestrator::with_config(config);
        let call = make_call("Bash", json!({"command": "timeout cmd"}));
        let ctx = ToolExecutionContext::none("test");

        let result = orch.run(&call, &executor, ctx).await;
        assert!(result.result.is_err());
        assert_eq!(result.attempts, 3); // 1 initial + 2 retries
    }

    #[tokio::test]
    async fn test_orchestrator_workspace_set_is_not_auto_approved() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
        let orch = ToolOrchestrator::new();
        let call = make_call("Workspace", json!({"path": "/tmp"}));
        let ctx = ToolExecutionContext::none("test");

        let result = orch.run(&call, &executor, ctx).await;
        assert!(!result.auto_approved);
        assert!(result.result.is_err());
    }

    #[test]
    fn test_is_transient_error() {
        assert!(is_transient_error(&ToolError::Execution(
            "request timed out".to_string()
        )));
        assert!(is_transient_error(&ToolError::Execution(
            "connection refused".to_string()
        )));
        assert!(!is_transient_error(&ToolError::InvalidArguments(
            "bad args".to_string()
        )));
        assert!(!is_transient_error(&ToolError::NotFound(
            "no tool".to_string()
        )));
    }
}
