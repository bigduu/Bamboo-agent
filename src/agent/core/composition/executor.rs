//! Workflow executor for tool composition DSL
//!
//! This module provides the execution engine for running tool composition
//! workflows defined using the ToolExpr DSL.

use crate::agent::core::tools::{normalize_tool_name, ToolError, ToolRegistry, ToolResult};
use futures::future::join_all;
use regex::Regex;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use super::condition::Condition;
use super::context::ExecutionContext;
use super::expr::ToolExpr;
use super::parallel::ParallelWait;

/// Executor for running tool composition workflows
///
/// The executor takes a tool registry and executes composition expressions,
/// managing context, variables, and control flow.
pub struct CompositionExecutor {
    /// Tool registry for looking up and executing tools
    registry: Arc<ToolRegistry>,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl CompositionExecutor {
    /// Create a new composition executor with the given tool registry
    pub fn new(registry: Arc<ToolRegistry>) -> Self {
        Self { registry }
    }

    /// Execute a tool expression with the given context
    ///
    /// This method executes the expression and updates the context with
    /// the result, binding it to the `_last` variable.
    pub async fn execute(
        &self,
        expr: &ToolExpr,
        ctx: &mut ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let result = self.execute_internal(expr, ctx).await;

        ctx.log_step(Self::expr_name(expr).to_string(), result.clone());

        if let Ok(value) = &result {
            ctx.bind("_last".to_string(), value.clone());
        }

        result
    }

    fn execute_internal<'a>(
        &'a self,
        expr: &'a ToolExpr,
        ctx: &'a mut ExecutionContext,
    ) -> BoxFuture<'a, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            match expr {
                ToolExpr::Call { tool, args } => self.execute_call(tool, args).await,
                ToolExpr::Sequence { steps, fail_fast } => {
                    self.execute_sequence(steps, *fail_fast, ctx).await
                }
                ToolExpr::Parallel { branches, wait } => {
                    self.execute_parallel(branches, wait, ctx).await
                }
                ToolExpr::Choice {
                    condition,
                    then_branch,
                    else_branch,
                } => {
                    self.execute_choice(condition, then_branch, else_branch.as_deref(), ctx)
                        .await
                }
                ToolExpr::Retry {
                    expr,
                    max_attempts,
                    delay_ms,
                } => {
                    self.execute_retry(expr, *max_attempts, *delay_ms, ctx)
                        .await
                }
                ToolExpr::Let { var, expr, body } => self.execute_let(var, expr, body, ctx).await,
                ToolExpr::Var(name) => self.execute_var(name, ctx),
            }
        })
    }

    async fn execute_call(
        &self,
        tool: &str,
        args: &serde_json::Value,
    ) -> Result<ToolResult, ToolError> {
        let normalized = normalize_tool_name(tool);
        let tool_impl = self
            .registry
            .get(normalized)
            .ok_or_else(|| ToolError::NotFound(format!("Tool '{}' not found", normalized)))?;

        tool_impl.execute(args.clone()).await
    }

    async fn execute_sequence(
        &self,
        steps: &[ToolExpr],
        fail_fast: bool,
        ctx: &mut ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let mut last_result = Self::default_result("empty sequence", true);

        for step in steps {
            match self.execute_internal(step, ctx).await {
                Ok(result) => {
                    ctx.bind("_last".to_string(), result.clone());
                    let should_stop = fail_fast && !result.success;
                    last_result = result;

                    if should_stop {
                        return Ok(last_result);
                    }
                }
                Err(error) => {
                    if fail_fast {
                        return Err(error);
                    }

                    let failure = Self::default_result(error.to_string(), false);
                    ctx.bind("_last".to_string(), failure.clone());
                    last_result = failure;
                }
            }
        }

        Ok(last_result)
    }

    async fn execute_parallel(
        &self,
        branches: &[ToolExpr],
        wait: &ParallelWait,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        if branches.is_empty() {
            return Ok(Self::default_result("empty parallel", true));
        }

        let futures = branches.iter().map(|branch| {
            let mut branch_ctx = ctx.clone();
            async move { self.execute_internal(branch, &mut branch_ctx).await }
        });

        let results = join_all(futures).await;

        match wait {
            ParallelWait::All => self.resolve_parallel_all(results),
            ParallelWait::Any => self.resolve_parallel_any(results),
            ParallelWait::N(target) => self.resolve_parallel_n(results, branches.len(), *target),
        }
    }

    fn resolve_parallel_all(
        &self,
        results: Vec<Result<ToolResult, ToolError>>,
    ) -> Result<ToolResult, ToolError> {
        let mut last_success = None;

        for result in results {
            match result {
                Ok(tool_result) => {
                    if !tool_result.success {
                        return Ok(tool_result);
                    }
                    last_success = Some(tool_result);
                }
                Err(error) => return Err(error),
            }
        }

        Ok(last_success.unwrap_or_else(|| Self::default_result("all branches completed", true)))
    }

    fn resolve_parallel_any(
        &self,
        results: Vec<Result<ToolResult, ToolError>>,
    ) -> Result<ToolResult, ToolError> {
        let mut first_failure = None;
        let mut last_error = None;

        for result in results {
            match result {
                Ok(tool_result) if tool_result.success => return Ok(tool_result),
                Ok(tool_result) => {
                    if first_failure.is_none() {
                        first_failure = Some(tool_result);
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }

        if let Some(failure) = first_failure {
            return Ok(failure);
        }

        Err(last_error
            .unwrap_or_else(|| ToolError::Execution("no parallel branch succeeded".to_string())))
    }

    fn resolve_parallel_n(
        &self,
        results: Vec<Result<ToolResult, ToolError>>,
        branch_count: usize,
        target: usize,
    ) -> Result<ToolResult, ToolError> {
        let mut success_count = 0;
        let mut last_success = None;

        for result in results {
            match result {
                Ok(tool_result) => {
                    if tool_result.success {
                        success_count += 1;
                        last_success = Some(tool_result);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        if success_count >= target {
            return Ok(last_success
                .unwrap_or_else(|| Self::default_result("required branches succeeded", true)));
        }

        Ok(Self::default_result(
            format!("only {success_count} of {branch_count} branches succeeded; required {target}"),
            false,
        ))
    }

    async fn execute_choice(
        &self,
        condition: &Condition,
        then_branch: &ToolExpr,
        else_branch: Option<&ToolExpr>,
        ctx: &mut ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let last_result = ctx
            .lookup("_last")
            .cloned()
            .unwrap_or_else(|| Self::default_result("{}", true));

        if self.evaluate_condition(condition, &last_result) {
            self.execute_internal(then_branch, ctx).await
        } else if let Some(else_expr) = else_branch {
            self.execute_internal(else_expr, ctx).await
        } else {
            Ok(Self::default_result("condition not met", true))
        }
    }

    async fn execute_retry(
        &self,
        expr: &ToolExpr,
        max_attempts: u32,
        delay_ms: u64,
        ctx: &mut ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let attempts = max_attempts.max(1);
        let mut last_error = None;

        for attempt in 0..attempts {
            match self.execute_internal(expr, ctx).await {
                Ok(result) if result.success => return Ok(result),
                Ok(result) => last_error = Some(ToolError::Execution(result.result)),
                Err(error) => last_error = Some(error),
            }

            if attempt + 1 < attempts && delay_ms > 0 {
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        Err(last_error
            .unwrap_or_else(|| ToolError::Execution("retry attempts exhausted".to_string())))
    }

    async fn execute_let(
        &self,
        var: &str,
        expr: &ToolExpr,
        body: &ToolExpr,
        ctx: &mut ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let value = self.execute_internal(expr, ctx).await?;
        ctx.bind(var.to_string(), value.clone());
        ctx.bind("_last".to_string(), value);
        self.execute_internal(body, ctx).await
    }

    fn execute_var(&self, name: &str, ctx: &ExecutionContext) -> Result<ToolResult, ToolError> {
        ctx.lookup(name)
            .cloned()
            .ok_or_else(|| ToolError::Execution(format!("Variable not found: {}", name)))
    }

    fn evaluate_condition(&self, condition: &Condition, result: &ToolResult) -> bool {
        match condition {
            Condition::Success => result.success,
            Condition::Contains { path, value } => {
                Self::extract_value_at_path(&result.result, path)
                    .map(|current| current.contains(value))
                    .unwrap_or(false)
            }
            Condition::Matches { path, pattern } => {
                Self::extract_value_at_path(&result.result, path)
                    .map(|current| {
                        Regex::new(pattern)
                            .map(|regex| regex.is_match(&current))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            }
            Condition::And { conditions } => conditions
                .iter()
                .all(|inner| self.evaluate_condition(inner, result)),
            Condition::Or { conditions } => conditions
                .iter()
                .any(|inner| self.evaluate_condition(inner, result)),
        }
    }

    fn extract_value_at_path(payload: &str, path: &str) -> Option<String> {
        let parsed: serde_json::Value = serde_json::from_str(payload).ok()?;

        if path.is_empty() {
            return Some(Self::value_as_string(&parsed));
        }

        let mut current = &parsed;

        for segment in path.split('.') {
            if let Ok(index) = segment.parse::<usize>() {
                current = current.get(index)?;
            } else {
                current = current.get(segment)?;
            }
        }

        Some(Self::value_as_string(current))
    }

    fn value_as_string(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(inner) => inner.clone(),
            _ => value.to_string(),
        }
    }

    fn expr_name(expr: &ToolExpr) -> &'static str {
        match expr {
            ToolExpr::Call { .. } => "call",
            ToolExpr::Sequence { .. } => "sequence",
            ToolExpr::Parallel { .. } => "parallel",
            ToolExpr::Choice { .. } => "choice",
            ToolExpr::Retry { .. } => "retry",
            ToolExpr::Let { .. } => "let",
            ToolExpr::Var(_) => "var",
        }
    }

    fn default_result(result: impl Into<String>, success: bool) -> ToolResult {
        ToolResult {
            success,
            result: result.into(),
            display_preference: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::Tool;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoArgsTool;

    #[async_trait]
    impl Tool for EchoArgsTool {
        fn name(&self) -> &str {
            "echo_args"
        }

        fn description(&self) -> &str {
            "echoes input args"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: args.to_string(),
                display_preference: None,
            })
        }
    }

    struct StaticTool {
        name: &'static str,
        success: bool,
        result: &'static str,
    }

    #[async_trait]
    impl Tool for StaticTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            "static tool"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: self.success,
                result: self.result.to_string(),
                display_preference: None,
            })
        }
    }

    struct ErrorTool {
        name: &'static str,
    }

    #[async_trait]
    impl Tool for ErrorTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            "always errors"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Err(ToolError::Execution(format!("{} failed", self.name)))
        }
    }

    struct FlakyTool {
        attempts: Arc<AtomicUsize>,
        fail_until: usize,
    }

    #[async_trait]
    impl Tool for FlakyTool {
        fn name(&self) -> &str {
            "flaky"
        }

        fn description(&self) -> &str {
            "fails until a threshold"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.fail_until {
                return Err(ToolError::Execution("transient failure".to_string()));
            }

            Ok(ToolResult {
                success: true,
                result: format!("attempt-{attempt}"),
                display_preference: None,
            })
        }
    }

    fn setup_executor() -> (CompositionExecutor, Arc<AtomicUsize>) {
        let registry = Arc::new(ToolRegistry::new());
        let attempts = Arc::new(AtomicUsize::new(0));

        registry.register(EchoArgsTool).unwrap();
        registry
            .register(StaticTool {
                name: "ok",
                success: true,
                result: "ok-result",
            })
            .unwrap();
        registry
            .register(StaticTool {
                name: "status_ready",
                success: true,
                result: r#"{"status":"ready","email":"agent@example.com"}"#,
            })
            .unwrap();
        registry
            .register(StaticTool {
                name: "then_branch",
                success: true,
                result: "then",
            })
            .unwrap();
        registry
            .register(StaticTool {
                name: "else_branch",
                success: true,
                result: "else",
            })
            .unwrap();
        registry
            .register(StaticTool {
                name: "soft_fail",
                success: false,
                result: "not-good",
            })
            .unwrap();
        registry.register(ErrorTool { name: "hard_fail" }).unwrap();
        registry
            .register(FlakyTool {
                attempts: Arc::clone(&attempts),
                fail_until: 2,
            })
            .unwrap();

        (CompositionExecutor::new(registry), attempts)
    }

    #[tokio::test]
    async fn executes_call_variant() {
        let (executor, _) = setup_executor();
        let mut ctx = ExecutionContext::new();

        let expr = ToolExpr::call("echo_args", json!({ "value": 42 }));
        let result = executor.execute(&expr, &mut ctx).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result, r#"{"value":42}"#);
    }

    #[tokio::test]
    async fn executes_sequence_with_continue_on_error() {
        let (executor, _) = setup_executor();
        let mut ctx = ExecutionContext::new();

        let expr = ToolExpr::sequence_with_fail_fast(
            vec![
                ToolExpr::call("hard_fail", json!({})),
                ToolExpr::call("ok", json!({})),
            ],
            false,
        );

        let result = executor.execute(&expr, &mut ctx).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result, "ok-result");
    }

    #[tokio::test]
    async fn executes_parallel_and_choice_variants() {
        let (executor, _) = setup_executor();
        let mut ctx = ExecutionContext::new();

        let parallel = ToolExpr::parallel_with_wait(
            vec![
                ToolExpr::call("soft_fail", json!({})),
                ToolExpr::call("ok", json!({})),
            ],
            ParallelWait::Any,
        );

        let parallel_result = executor.execute(&parallel, &mut ctx).await.unwrap();
        assert!(parallel_result.success);
        assert_eq!(parallel_result.result, "ok-result");

        let choice = ToolExpr::sequence(vec![
            ToolExpr::call("status_ready", json!({})),
            ToolExpr::choice_with_else(
                Condition::Contains {
                    path: "status".to_string(),
                    value: "ready".to_string(),
                },
                ToolExpr::call("then_branch", json!({})),
                ToolExpr::call("else_branch", json!({})),
            ),
        ]);

        let choice_result = executor.execute(&choice, &mut ctx).await.unwrap();
        assert_eq!(choice_result.result, "then");
    }

    #[tokio::test]
    async fn executes_retry_and_let_var_variants() {
        let (executor, attempts) = setup_executor();
        let mut ctx = ExecutionContext::new();

        let retry_expr = ToolExpr::retry_with_params(ToolExpr::call("flaky", json!({})), 3, 0);
        let retry_result = executor.execute(&retry_expr, &mut ctx).await.unwrap();
        assert!(retry_result.success);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        let let_expr = ToolExpr::let_binding(
            "saved",
            ToolExpr::call("ok", json!({})),
            ToolExpr::var("saved"),
        );
        let let_result = executor.execute(&let_expr, &mut ctx).await.unwrap();
        assert_eq!(let_result.result, "ok-result");

        let missing = ToolExpr::var("missing");
        let error = executor.execute(&missing, &mut ctx).await.unwrap_err();
        assert!(matches!(error, ToolError::Execution(_)));
    }

    #[test]
    fn evaluates_nested_conditions() {
        let executor = CompositionExecutor::new(Arc::new(ToolRegistry::new()));
        let result = ToolResult {
            success: true,
            result: r#"{"status":"ready","email":"agent@example.com"}"#.to_string(),
            display_preference: None,
        };

        let condition = Condition::And {
            conditions: vec![
                Condition::Success,
                Condition::Or {
                    conditions: vec![
                        Condition::Contains {
                            path: "status".to_string(),
                            value: "ready".to_string(),
                        },
                        Condition::Matches {
                            path: "email".to_string(),
                            pattern: ".+@example\\.com".to_string(),
                        },
                    ],
                },
            ],
        };

        assert!(executor.evaluate_condition(&condition, &result));
    }
}
