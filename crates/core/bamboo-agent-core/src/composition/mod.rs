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

use crate::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use futures::future::join_all;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub use bamboo_domain::composition::Condition;

// New expression DSL modules
/// Execution context and state management
pub mod context;
/// Workflow executor implementation
pub mod executor;
/// Tool expression AST types
pub mod expr;
/// Parallel execution strategies
pub mod parallel;

// Re-export new DSL types
pub use context::ExecutionContext;
pub use executor::CompositionExecutor;
pub use expr::{CompositionError, ToolExpr};
pub use parallel::ParallelWait;

/// Result of a composition execution
///
/// Contains the final tool result, success status, and updated execution context
/// after running a composition workflow.
#[derive(Debug, Clone)]
pub struct CompositionResult {
    /// Whether the composition completed successfully
    pub success: bool,

    /// The final result from the last executed tool
    pub result: ToolResult,

    /// The execution context with accumulated variables and state
    pub context: ExecutionContext,
}

/// Trait for composable tool operations
///
/// This trait defines the interface for all composition primitives that can be
/// combined to build complex tool workflows. Compositions can be nested and
/// combined to create sophisticated execution patterns.
///
/// # Implementations
///
/// - `Sequence`: Execute tools in order, passing results between them
/// - `Parallel`: Execute tools concurrently
/// - `Choice`: Conditional execution based on a predicate
/// - `Retry`: Retry execution with exponential backoff
/// - `ToolComposition`: Wrap a single tool as a composition
/// - `Map`: Transform results with a function
///
/// # Example
///
/// ```ignore
/// use bamboo_agent::agent::core::composition::{Sequence, Parallel, ToolComposition};
///
/// let workflow = Sequence::builder()
///     .step(ToolComposition::new(tool1, args))
///     .step(Parallel::builder()
///         .branch(ToolComposition::new(tool2, args2))
///         .branch(ToolComposition::new(tool3, args3))
///         .build())
///     .build();
///
/// let result = workflow.execute(ctx).await?;
/// ```
#[async_trait]
pub trait Composition: Send + Sync {
    /// Executes the composition with the given context
    ///
    /// # Arguments
    ///
    /// * `ctx` - Execution context containing variables and state
    ///
    /// # Returns
    ///
    /// The composition result with success status, final result, and updated context
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError>;
}

/// Sequence composition - executes tools in order
///
/// Runs a series of compositions sequentially, passing the execution context
/// from one step to the next. Stops early if any step fails.
///
/// # Example
///
/// ```ignore
/// let sequence = Sequence::builder()
///     .step(read_file_composition)
///     .step(process_composition)
///     .step(write_file_composition)
///     .build();
/// ```
pub struct Sequence {
    steps: Vec<Box<dyn Composition>>,
}

impl Sequence {
    /// Creates a new sequence with the given steps
    pub fn new(steps: Vec<Box<dyn Composition>>) -> Self {
        Self { steps }
    }

    /// Creates a new sequence builder
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
            images: Vec::new(),
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

/// Builder for creating sequence compositions
pub struct SequenceBuilder {
    steps: Vec<Box<dyn Composition>>,
}

impl SequenceBuilder {
    /// Creates a new empty builder
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Adds a step to the sequence
    pub fn step(mut self, composition: impl Composition + 'static) -> Self {
        self.steps.push(Box::new(composition));
        self
    }

    /// Builds the final sequence
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
///
/// Runs multiple compositions in parallel using `futures::future::join_all`.
/// All branches execute simultaneously and results are collected.
///
/// # Example
///
/// ```ignore
/// let parallel = Parallel::builder()
///     .branch(analyze_file_composition)
///     .branch(run_tests_composition)
///     .branch(check_lint_composition)
///     .build();
/// ```
pub struct Parallel {
    branches: Vec<Box<dyn Composition>>,
}

impl Parallel {
    /// Creates a new parallel composition with the given branches
    pub fn new(branches: Vec<Box<dyn Composition>>) -> Self {
        Self { branches }
    }

    /// Creates a new parallel builder
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
                images: Vec::new(),
            },
            context: ctx,
        })
    }
}

/// Builder for creating parallel compositions
pub struct ParallelBuilder {
    branches: Vec<Box<dyn Composition>>,
}

impl ParallelBuilder {
    /// Creates a new empty builder
    pub fn new() -> Self {
        Self {
            branches: Vec::new(),
        }
    }

    /// Adds a branch to execute in parallel
    pub fn branch(mut self, composition: impl Composition + 'static) -> Self {
        self.branches.push(Box::new(composition));
        self
    }

    /// Builds the final parallel composition
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
///
/// Executes one of two branches based on a predicate function.
/// If the predicate returns true, the `if_true` branch executes;
/// otherwise, the `if_false` branch executes (if provided).
///
/// # Example
///
/// ```ignore
/// let choice = Choice::new(
///     |ctx| ctx.get_variable("dry_run").is_some(),
///     dry_run_composition,
/// ).with_else(real_execution_composition);
/// ```
pub struct Choice {
    /// Predicate function that determines which branch to execute
    predicate: Arc<dyn Fn(&ExecutionContext) -> bool + Send + Sync>,

    /// Branch to execute if predicate returns true
    if_true: Box<dyn Composition>,

    /// Optional branch to execute if predicate returns false
    if_false: Option<Box<dyn Composition>>,
}

impl Choice {
    /// Creates a new choice composition
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that evaluates the context to choose a branch
    /// * `if_true` - Composition to execute if predicate returns true
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

    /// Adds an else branch to execute if the predicate returns false
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
                    images: Vec::new(),
                },
                context: ctx,
            })
        }
    }
}

/// Retry composition - retry with backoff
///
/// Retries a composition up to a maximum number of attempts with
/// configurable backoff delay between attempts.
///
/// # Example
///
/// ```ignore
/// let retry = Retry::new(flaky_composition, 3)
///     .with_backoff(200); // 200ms base backoff
/// ```
pub struct Retry {
    /// The composition to retry
    composition: Box<dyn Composition>,

    /// Maximum number of attempts
    max_attempts: u32,

    /// Base backoff delay in milliseconds (multiplied by attempt number)
    backoff_ms: u64,
}

impl Retry {
    /// Creates a new retry composition
    ///
    /// # Arguments
    ///
    /// * `composition` - The composition to retry
    /// * `max_attempts` - Maximum number of attempts (including the first)
    pub fn new(composition: impl Composition + 'static, max_attempts: u32) -> Self {
        Self {
            composition: Box::new(composition),
            max_attempts,
            backoff_ms: 100,
        }
    }

    /// Sets a custom backoff delay
    ///
    /// The actual delay is `backoff_ms * (attempt + 1)` for exponential backoff
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
///
/// This adapter allows individual tools to be used within composition workflows.
/// Optionally stores the result in a context variable for use by subsequent steps.
///
/// # Example
///
/// ```ignore
/// let tool_comp = ToolComposition::new(tool, json!({"path": "/src/main.rs"}))
///     .with_output_variable("file_content");
/// ```
pub struct ToolComposition {
    /// The tool to execute
    tool: Arc<dyn Tool>,

    /// Arguments to pass to the tool
    args: Value,

    /// Optional variable name to store the result in the context
    output_variable: Option<String>,
}

impl ToolComposition {
    /// Creates a new tool composition
    ///
    /// # Arguments
    ///
    /// * `tool` - The tool to wrap
    /// * `args` - Arguments to pass to the tool (JSON value)
    pub fn new(tool: Arc<dyn Tool>, args: Value) -> Self {
        Self {
            tool,
            args,
            output_variable: None,
        }
    }

    /// Sets the variable name to store the result in the context
    ///
    /// Subsequent steps can access this result via `ctx.get_variable(var_name)`
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
///
/// Applies a transformation function to the result of a composition,
/// allowing post-processing of tool outputs.
///
/// # Example
///
/// ```ignore
/// let map = Map::new(
///     read_file_composition,
///     |result| {
///         ToolResult {
///             success: result.success,
///             result: result.result.to_uppercase(),
///             display_preference: None,
///         }
///     },
/// );
/// ```
pub struct Map {
    /// The composition to transform
    composition: Box<dyn Composition>,

    /// Transformation function to apply to the result
    transform: Arc<dyn Fn(ToolResult) -> ToolResult + Send + Sync>,
}

impl Map {
    /// Creates a new map composition
    ///
    /// # Arguments
    ///
    /// * `composition` - The composition to transform
    /// * `transform` - Function to apply to the result
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
