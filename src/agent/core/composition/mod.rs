//! Tool Composition DSL for building complex tool workflows
//!
//! This module provides composable primitives for building tool execution workflows:
//! - Sequence: Execute tools in sequence, passing results between them
//! - Parallel: Execute tools in parallel
//! - Choice: Conditional execution based on predicate
//! - Retry: Retry execution with backoff
//! - Map: Transform results
//!
//! # New Expression DSL
//!
//! The module also provides a serializable expression DSL (`ToolExpr`) that can be
//! defined in YAML/JSON and executed by `CompositionExecutor`.
//!
//! ## Example YAML:
//! ```yaml
//! type: sequence
//! steps:
//!   - type: call
//!     tool: read_file
//!     args:
//!       path: /tmp/input.txt
//!   - type: parallel
//!     branches:
//!       - type: call
//!         tool: process_a
//!         args: {}
//!       - type: call
//!         tool: process_b
//!         args: {}
//!     wait: all
//! ```

use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use futures::future::join_all;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// New expression DSL modules
pub mod condition;
pub mod context;
pub mod executor;
pub mod expr;
pub mod parallel;

// Re-export new DSL types
pub use condition::Condition;
pub use context::ExecutionContext;
pub use executor::CompositionExecutor;
pub use expr::{CompositionError, ToolExpr};
pub use parallel::ParallelWait;

/// Result of a composition execution
#[derive(Debug, Clone)]
pub struct CompositionResult {
    pub success: bool,
    pub result: ToolResult,
    pub context: ExecutionContext,
}

/// Trait for composable tool operations
#[async_trait]
pub trait Composition: Send + Sync {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError>;
}

/// Sequence composition - executes tools in order
pub struct Sequence {
    steps: Vec<Box<dyn Composition>>,
}

impl Sequence {
    pub fn new(steps: Vec<Box<dyn Composition>>) -> Self {
        Self { steps }
    }

    pub fn builder() -> SequenceBuilder {
        SequenceBuilder::new()
    }
}

#[async_trait]
impl Composition for Sequence {
    async fn execute(&self, mut ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        let mut last_result = ToolResult {
            success: true,
            result: String::new(),
            display_preference: None,
        };

        for step in &self.steps {
            let result = step.execute(ctx.clone()).await?;
            ctx = result.context;
            last_result = result.result;

            if !last_result.success {
                return Ok(CompositionResult {
                    success: false,
                    result: last_result,
                    context: ctx,
                });
            }
        }

        Ok(CompositionResult {
            success: true,
            result: last_result,
            context: ctx,
        })
    }
}

pub struct SequenceBuilder {
    steps: Vec<Box<dyn Composition>>,
}

impl SequenceBuilder {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn step(mut self, composition: impl Composition + 'static) -> Self {
        self.steps.push(Box::new(composition));
        self
    }

    pub fn build(self) -> Sequence {
        Sequence::new(self.steps)
    }
}

impl Default for SequenceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parallel composition - executes tools concurrently
pub struct Parallel {
    branches: Vec<Box<dyn Composition>>,
}

impl Parallel {
    pub fn new(branches: Vec<Box<dyn Composition>>) -> Self {
        Self { branches }
    }

    pub fn builder() -> ParallelBuilder {
        ParallelBuilder::new()
    }
}

#[async_trait]
impl Composition for Parallel {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        let futures: Vec<_> = self
            .branches
            .iter()
            .map(|branch| branch.execute(ctx.clone()))
            .collect();

        let results = join_all(futures).await;

        let mut all_success = true;
        let mut combined_results = Vec::new();

        for result in results {
            match result {
                Ok(comp_result) => {
                    combined_results.push(comp_result.result.result);
                    if !comp_result.success {
                        all_success = false;
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(CompositionResult {
            success: all_success,
            result: ToolResult {
                success: all_success,
                result: serde_json::to_string(&combined_results).unwrap_or_default(),
                display_preference: None,
            },
            context: ctx,
        })
    }
}

pub struct ParallelBuilder {
    branches: Vec<Box<dyn Composition>>,
}

impl ParallelBuilder {
    pub fn new() -> Self {
        Self {
            branches: Vec::new(),
        }
    }

    pub fn branch(mut self, composition: impl Composition + 'static) -> Self {
        self.branches.push(Box::new(composition));
        self
    }

    pub fn build(self) -> Parallel {
        Parallel::new(self.branches)
    }
}

impl Default for ParallelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Choice composition - conditional execution
pub struct Choice {
    predicate: Arc<dyn Fn(&ExecutionContext) -> bool + Send + Sync>,
    if_true: Box<dyn Composition>,
    if_false: Option<Box<dyn Composition>>,
}

impl Choice {
    pub fn new(
        predicate: impl Fn(&ExecutionContext) -> bool + Send + Sync + 'static,
        if_true: impl Composition + 'static,
    ) -> Self {
        Self {
            predicate: Arc::new(predicate),
            if_true: Box::new(if_true),
            if_false: None,
        }
    }

    pub fn with_else(mut self, if_false: impl Composition + 'static) -> Self {
        self.if_false = Some(Box::new(if_false));
        self
    }
}

#[async_trait]
impl Composition for Choice {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        if (self.predicate)(&ctx) {
            self.if_true.execute(ctx).await
        } else if let Some(ref else_branch) = self.if_false {
            else_branch.execute(ctx).await
        } else {
            Ok(CompositionResult {
                success: true,
                result: ToolResult {
                    success: true,
                    result: "Condition was false, no else branch".to_string(),
                    display_preference: None,
                },
                context: ctx,
            })
        }
    }
}

/// Retry composition - retry with backoff
pub struct Retry {
    composition: Box<dyn Composition>,
    max_attempts: u32,
    backoff_ms: u64,
}

impl Retry {
    pub fn new(composition: impl Composition + 'static, max_attempts: u32) -> Self {
        Self {
            composition: Box::new(composition),
            max_attempts,
            backoff_ms: 100,
        }
    }

    pub fn with_backoff(mut self, backoff_ms: u64) -> Self {
        self.backoff_ms = backoff_ms;
        self
    }
}

#[async_trait]
impl Composition for Retry {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        let mut last_error = None;

        for attempt in 0..self.max_attempts {
            match self.composition.execute(ctx.clone()).await {
                Ok(result) if result.success => return Ok(result),
                Ok(result) => {
                    if attempt == self.max_attempts - 1 {
                        return Ok(result);
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.max_attempts - 1 {
                        sleep(Duration::from_millis(
                            self.backoff_ms * (attempt as u64 + 1),
                        ))
                        .await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| ToolError::Execution("Max retries exceeded".to_string())))
    }
}

/// Tool wrapper - wraps a Tool into a Composition
pub struct ToolComposition {
    tool: Arc<dyn Tool>,
    args: Value,
    output_variable: Option<String>,
}

impl ToolComposition {
    pub fn new(tool: Arc<dyn Tool>, args: Value) -> Self {
        Self {
            tool,
            args,
            output_variable: None,
        }
    }

    pub fn with_output_variable(mut self, var_name: impl Into<String>) -> Self {
        self.output_variable = Some(var_name.into());
        self
    }
}

#[async_trait]
impl Composition for ToolComposition {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        // Merge context variables into args
        let mut final_args = self.args.clone();
        if let Value::Object(ref mut map) = final_args {
            for (key, value) in &ctx.variables {
                if !map.contains_key(key) {
                    map.insert(key.clone(), value.clone());
                }
            }
        }

        let result = self.tool.execute(final_args).await?;
        let success = result.success;

        let mut new_ctx = ctx;
        if let Some(ref var_name) = self.output_variable {
            new_ctx.set_variable(
                var_name.clone(),
                serde_json::to_value(&result).unwrap_or_default(),
            );
        }
        new_ctx.last_result = Some(result.clone());

        Ok(CompositionResult {
            success,
            result,
            context: new_ctx,
        })
    }
}

/// Map composition - transform results
pub struct Map {
    composition: Box<dyn Composition>,
    transform: Arc<dyn Fn(ToolResult) -> ToolResult + Send + Sync>,
}

impl Map {
    pub fn new(
        composition: impl Composition + 'static,
        transform: impl Fn(ToolResult) -> ToolResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            composition: Box::new(composition),
            transform: Arc::new(transform),
        }
    }
}

#[async_trait]
impl Composition for Map {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        let result = self.composition.execute(ctx).await?;
        let transformed = (self.transform)(result.result);
        let success = transformed.success;

        Ok(CompositionResult {
            success,
            result: transformed,
            context: result.context,
        })
    }
}

#[cfg(test)]
mod tests;
