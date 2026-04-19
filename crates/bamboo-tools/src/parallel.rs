//! Parallel tool execution runtime.
//!
//! Inspired by Codex's `ToolCallRuntime`, this module provides a concurrency
//! manager for tool calls using an RwLock strategy:
//!
//! - **Read-only tools** (Read, Grep, Glob, etc.) acquire a *read* lock and
//!   can execute concurrently with other read-only tools.
//! - **Mutating tools** (Write, Edit, Bash, etc.) acquire a *write* lock and
//!   run exclusively — blocking other tools until they finish.
//!
//! This ensures that multiple reads can happen in parallel while mutations
//! are safely serialized.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;

use bamboo_agent_core::{
    ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult,
};
use crate::orchestrator::ToolMutability;

/// The parallel tool call runtime.
///
/// Wraps a `ToolExecutor` and adds concurrency control via RwLock.
/// Clone is cheap — all state is behind Arc.
#[derive(Clone)]
pub struct ToolCallRuntime {
    executor: Arc<dyn ToolExecutor>,
    /// RwLock for concurrency control:
    /// - Read lock = parallel-safe (multiple readers)
    /// - Write lock = exclusive (single writer)
    parallel_lock: Arc<RwLock<()>>,
}

/// Result of a tool call execution with timing metadata.
#[derive(Debug)]
pub struct ToolCallResult {
    pub call_id: String,
    pub tool_name: String,
    pub result: Result<ToolResult, ToolError>,
    pub elapsed_ms: u64,
    pub was_parallel: bool,
}

impl ToolCallRuntime {
    /// Create a new runtime wrapping the given executor.
    pub fn new(executor: Arc<dyn ToolExecutor>) -> Self {
        Self {
            executor,
            parallel_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Determine if a tool supports parallel execution.
    pub fn supports_parallel(executor: &Arc<dyn ToolExecutor>, call: &ToolCall) -> bool {
        executor.call_mutability(call) == ToolMutability::ReadOnly
            && executor.call_concurrency_safe(call)
    }

    /// Execute a single tool call with appropriate concurrency control.
    pub async fn execute(&self, call: &ToolCall, ctx: ToolExecutionContext<'_>) -> ToolCallResult {
        let tool_name = call.function.name.trim().to_string();
        let parallel = Self::supports_parallel(&self.executor, call);
        let started = Instant::now();

        let result = if parallel {
            // Read lock — allows concurrent execution with other readers
            let _guard = self.parallel_lock.read().await;
            self.executor.execute_with_context(call, ctx).await
        } else {
            // Write lock — exclusive execution
            let _guard = self.parallel_lock.write().await;
            self.executor.execute_with_context(call, ctx).await
        };

        ToolCallResult {
            call_id: call.id.clone(),
            tool_name,
            result,
            elapsed_ms: started.elapsed().as_millis() as u64,
            was_parallel: parallel,
        }
    }

    /// Execute multiple tool calls with automatic parallel/sequential scheduling.
    ///
    /// Tool calls are partitioned into batches:
    /// - Consecutive read-only tools run concurrently
    /// - Mutating tools run one at a time
    /// - Order is preserved (mutating tools act as barriers)
    pub async fn execute_batch(
        &self,
        calls: Vec<(ToolCall, ToolExecutionContext<'_>)>,
    ) -> Vec<ToolCallResult> {
        if calls.is_empty() {
            return Vec::new();
        }

        // Split into groups: consecutive parallel-safe calls are batched
        let mut results = Vec::with_capacity(calls.len());
        let mut parallel_batch: Vec<(ToolCall, ToolExecutionContext<'_>)> = Vec::new();

        for (call, ctx) in calls {
            if Self::supports_parallel(&self.executor, &call) {
                parallel_batch.push((call, ctx));
            } else {
                // Flush any pending parallel batch first
                if !parallel_batch.is_empty() {
                    let batch_results = self.execute_parallel_group(parallel_batch).await;
                    results.extend(batch_results);
                    parallel_batch = Vec::new();
                }
                // Execute the mutating tool sequentially
                let result = self.execute(&call, ctx).await;
                results.push(result);
            }
        }

        // Flush remaining parallel batch
        if !parallel_batch.is_empty() {
            let batch_results = self.execute_parallel_group(parallel_batch).await;
            results.extend(batch_results);
        }

        results
    }

    /// Execute a group of parallel-safe tool calls concurrently.
    async fn execute_parallel_group(
        &self,
        calls: Vec<(ToolCall, ToolExecutionContext<'_>)>,
    ) -> Vec<ToolCallResult> {
        let futures: Vec<_> = calls
            .into_iter()
            .map(|(call, ctx)| {
                let runtime = self.clone();
                async move { runtime.execute(&call, ctx).await }
            })
            .collect();

        futures::future::join_all(futures).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{FunctionCall, ToolSchema};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn make_call(name: &str) -> ToolCall {
        ToolCall {
            id: format!("call_{}", name),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    struct CountingExecutor {
        call_count: AtomicUsize,
        max_concurrent: Arc<std::sync::Mutex<usize>>,
        current_concurrent: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl CountingExecutor {
        fn new(delay: Duration) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                max_concurrent: Arc::new(std::sync::Mutex::new(0)),
                current_concurrent: Arc::new(AtomicUsize::new(0)),
                delay,
            }
        }
    }

    #[async_trait]
    impl ToolExecutor for CountingExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.execute_with_context(_call, ToolExecutionContext::none("test"))
                .await
        }

        async fn execute_with_context(
            &self,
            _call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            // Track concurrency
            let current = self.current_concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            {
                let mut max = self.max_concurrent.lock().unwrap();
                if current > *max {
                    *max = current;
                }
            }

            if self.delay > Duration::ZERO {
                tokio::time::sleep(self.delay).await;
            }

            self.current_concurrent.fetch_sub(1, Ordering::SeqCst);

            Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![]
        }
    }

    #[test]
    fn test_supports_parallel() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(CountingExecutor::new(Duration::ZERO));
        assert!(ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Read")
        ));
        assert!(ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Grep")
        ));
        assert!(ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Glob")
        ));
        assert!(!ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Bash")
        ));
        assert!(!ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Write")
        ));
        assert!(!ToolCallRuntime::supports_parallel(
            &executor,
            &make_call("Edit")
        ));
    }

    #[tokio::test]
    async fn test_single_call_works() {
        let executor = Arc::new(CountingExecutor::new(Duration::ZERO));
        let runtime = ToolCallRuntime::new(executor.clone());
        let call = make_call("Read");
        let ctx = ToolExecutionContext::none("test");

        let result = runtime.execute(&call, ctx).await;
        assert!(result.result.is_ok());
        assert!(result.was_parallel);
        assert_eq!(result.tool_name, "Read");
        assert_eq!(executor.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_mutating_call_is_sequential() {
        let executor = Arc::new(CountingExecutor::new(Duration::ZERO));
        let runtime = ToolCallRuntime::new(executor.clone());
        let call = make_call("Bash");
        let ctx = ToolExecutionContext::none("test");

        let result = runtime.execute(&call, ctx).await;
        assert!(result.result.is_ok());
        assert!(!result.was_parallel);
    }

    #[tokio::test]
    async fn test_parallel_reads_are_concurrent() {
        let executor = Arc::new(CountingExecutor::new(Duration::from_millis(50)));
        let runtime = ToolCallRuntime::new(executor.clone());

        // Execute 3 reads concurrently
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let rt = runtime.clone();
                let call = make_call("Read");
                tokio::spawn(
                    async move { rt.execute(&call, ToolExecutionContext::none("test")).await },
                )
            })
            .collect();

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should succeed
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert!(results.iter().all(|r| r.was_parallel));

        // Max concurrency should be > 1 (parallel execution)
        let max_conc = *executor.max_concurrent.lock().unwrap();
        assert!(
            max_conc >= 2,
            "Expected parallel execution, got max_concurrent={}",
            max_conc
        );
    }

    #[tokio::test]
    async fn test_batch_empty() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(CountingExecutor::new(Duration::ZERO));
        let runtime = ToolCallRuntime::new(executor);
        let results = runtime.execute_batch(vec![]).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_batch_mixed() {
        let executor: Arc<dyn ToolExecutor> = Arc::new(CountingExecutor::new(Duration::ZERO));
        let runtime = ToolCallRuntime::new(executor);

        let calls: Vec<_> = vec![
            (make_call("Read"), ToolExecutionContext::none("test")),
            (make_call("Grep"), ToolExecutionContext::none("test")),
            (make_call("Bash"), ToolExecutionContext::none("test")), // barrier
            (make_call("Glob"), ToolExecutionContext::none("test")),
        ];

        let results = runtime.execute_batch(calls).await;
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.result.is_ok()));
    }
}
