//! Tests for Tool Composition DSL

use super::*;
use crate::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Test tool implementations
struct MockTool {
    name: String,
    result_value: String,
    should_fail: bool,
    call_count: AtomicUsize,
}

impl MockTool {
    fn new(name: &str, result_value: &str) -> Self {
        Self {
            name: name.to_string(),
            result_value: result_value.to_string(),
            should_fail: false,
            call_count: AtomicUsize::new(0),
        }
    }

    fn failing(name: &str) -> Self {
        Self {
            name: name.to_string(),
            result_value: String::new(),
            should_fail: true,
            call_count: AtomicUsize::new(0),
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Tool for MockTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        "Mock tool for testing"
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "input": { "type": "string" }
            }
        })
    }

    async fn execute(&self, _args: Value) -> Result<ToolResult, ToolError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        if self.should_fail {
            Err(ToolError::Execution("Mock failure".to_string()))
        } else {
            Ok(ToolResult {
                success: true,
                result: self.result_value.clone(),
                display_preference: None,
            })
        }
    }
}

// A simple composition that returns a constant value
struct ConstantComposition {
    value: String,
}

#[async_trait]
impl Composition for ConstantComposition {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        Ok(CompositionResult {
            success: true,
            result: ToolResult {
                success: true,
                result: self.value.clone(),
                display_preference: None,
            },
            context: ctx,
        })
    }
}

// A composition that fails
struct FailingComposition;

#[async_trait]
impl Composition for FailingComposition {
    async fn execute(&self, ctx: ExecutionContext) -> Result<CompositionResult, ToolError> {
        Ok(CompositionResult {
            success: false,
            result: ToolResult {
                success: false,
                result: "Failed".to_string(),
                display_preference: None,
            },
            context: ctx,
        })
    }
}

mod sequence_tests {
    use super::*;

    #[tokio::test]
    async fn test_sequence_execution() {
        let seq = Sequence::builder()
            .step(ConstantComposition {
                value: "step1".to_string(),
            })
            .step(ConstantComposition {
                value: "step2".to_string(),
            })
            .step(ConstantComposition {
                value: "step3".to_string(),
            })
            .build();

        let result = seq.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "step3"); // Last result
    }

    #[tokio::test]
    async fn test_sequence_stops_on_failure() {
        let seq = Sequence::builder()
            .step(ConstantComposition {
                value: "step1".to_string(),
            })
            .step(FailingComposition)
            .step(ConstantComposition {
                value: "step3".to_string(),
            }) // Should not execute
            .build();

        let result = seq.execute(ExecutionContext::new()).await.unwrap();

        assert!(!result.success);
        assert_eq!(result.result.result, "Failed");
    }

    #[tokio::test]
    async fn test_empty_sequence() {
        let seq = Sequence::new(vec![]);
        let result = seq.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert!(result.result.result.is_empty());
    }

    #[tokio::test]
    async fn test_sequence_context_propagation() {
        let tool1 = Arc::new(MockTool::new("tool1", "output1"));
        let tool2 = Arc::new(MockTool::new("tool2", "output2"));

        let seq = Sequence::builder()
            .step(ToolComposition::new(
                tool1.clone(),
                json!({"input": "test"}),
            ))
            .step(ToolComposition::new(
                tool2.clone(),
                json!({"input": "test"}),
            ))
            .build();

        seq.execute(ExecutionContext::new()).await.unwrap();

        assert_eq!(tool1.call_count(), 1);
        assert_eq!(tool2.call_count(), 1);
    }

    #[tokio::test]
    async fn test_sequence_propagates_tool_error() {
        let failing_tool = Arc::new(MockTool::failing("tool1"));
        let seq = Sequence::builder()
            .step(ToolComposition::new(failing_tool, json!({"input": "test"})))
            .build();

        let err = seq.execute(ExecutionContext::new()).await.unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }
}

mod parallel_tests {
    use super::*;

    #[tokio::test]
    async fn test_parallel_execution() {
        let parallel = Parallel::builder()
            .branch(ConstantComposition {
                value: "branch1".to_string(),
            })
            .branch(ConstantComposition {
                value: "branch2".to_string(),
            })
            .branch(ConstantComposition {
                value: "branch3".to_string(),
            })
            .build();

        let result = parallel.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        // Results should be combined
        let results: Vec<String> = serde_json::from_str(&result.result.result).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&"branch1".to_string()));
        assert!(results.contains(&"branch2".to_string()));
        assert!(results.contains(&"branch3".to_string()));
    }

    #[tokio::test]
    async fn test_parallel_failure_handling() {
        let parallel = Parallel::builder()
            .branch(ConstantComposition {
                value: "success".to_string(),
            })
            .branch(FailingComposition)
            .build();

        let result = parallel.execute(ExecutionContext::new()).await.unwrap();

        assert!(!result.success); // Overall failure because one branch failed
    }

    #[tokio::test]
    async fn test_empty_parallel() {
        let parallel = Parallel::new(vec![]);
        let result = parallel.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        let results: Vec<String> = serde_json::from_str(&result.result.result).unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_parallel_concurrent_execution() {
        let tool1 = Arc::new(MockTool::new("tool1", "result1"));
        let tool2 = Arc::new(MockTool::new("tool2", "result2"));

        let parallel: Parallel = Parallel::builder()
            .branch(ToolComposition::new(tool1.clone(), json!({})))
            .branch(ToolComposition::new(tool2.clone(), json!({})))
            .build();

        let start = std::time::Instant::now();
        parallel.execute(ExecutionContext::new()).await.unwrap();
        let elapsed = start.elapsed();

        // Both should execute concurrently, so should be fast
        assert!(elapsed.as_millis() < 100);
        assert_eq!(tool1.call_count(), 1);
        assert_eq!(tool2.call_count(), 1);
    }
}

mod choice_tests {
    use super::*;

    #[tokio::test]
    async fn test_choice_true_branch() {
        let choice = Choice::new(
            |_ctx| true,
            ConstantComposition {
                value: "true_branch".to_string(),
            },
        );

        let result = choice.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "true_branch");
    }

    #[tokio::test]
    async fn test_choice_false_branch() {
        let choice = Choice::new(
            |_ctx| false,
            ConstantComposition {
                value: "true_branch".to_string(),
            },
        )
        .with_else(ConstantComposition {
            value: "false_branch".to_string(),
        });

        let result = choice.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "false_branch");
    }

    #[tokio::test]
    async fn test_choice_no_else_branch() {
        let choice = Choice::new(
            |_ctx| false,
            ConstantComposition {
                value: "true_branch".to_string(),
            },
        );

        let result = choice.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "Condition was false, no else branch");
    }

    #[tokio::test]
    async fn test_choice_with_context() {
        let ctx = ExecutionContext::new().with_variable("should_execute", json!(true));

        let choice = Choice::new(
            |ctx| {
                ctx.get_variable("should_execute")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            },
            ConstantComposition {
                value: "executed".to_string(),
            },
        );

        let result = choice.execute(ctx).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "executed");
    }
}

mod retry_tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let retry = Retry::new(
            ConstantComposition {
                value: "success".to_string(),
            },
            3,
        );

        let result = retry.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "success");
    }

    #[tokio::test]
    async fn test_retry_with_backoff() {
        // This test verifies retry structure works
        // A more sophisticated test would count attempts
        let retry = Retry::new(
            ConstantComposition {
                value: "success".to_string(),
            },
            3,
        )
        .with_backoff(10);

        let result = retry.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
    }
}

mod tool_composition_tests {
    use super::*;

    #[tokio::test]
    async fn test_tool_composition_execution() {
        let tool = Arc::new(MockTool::new("test_tool", "test_output"));
        let composition = ToolComposition::new(tool.clone(), json!({"input": "test"}));

        let result = composition.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "test_output");
        assert_eq!(tool.call_count(), 1);
    }

    #[tokio::test]
    async fn test_tool_composition_with_output_variable() {
        let tool = Arc::new(MockTool::new("test_tool", "test_output"));
        let composition =
            ToolComposition::new(tool.clone(), json!({})).with_output_variable("result");

        let result = composition.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.context.get_variable("result").is_some());
    }

    #[tokio::test]
    async fn test_tool_composition_context_merge() {
        let tool = Arc::new(MockTool::new("test_tool", "output"));
        let composition = ToolComposition::new(tool.clone(), json!({"explicit": "value"}));

        let ctx = ExecutionContext::new().with_variable("from_context", json!("context_value"));

        composition.execute(ctx).await.unwrap();

        // Tool should have been called with merged args
        assert_eq!(tool.call_count(), 1);
    }
}

mod map_tests {
    use super::*;

    #[tokio::test]
    async fn test_map_transformation() {
        let inner = ConstantComposition {
            value: "original".to_string(),
        };
        let map = Map::new(inner, |result| ToolResult {
            success: result.success,
            result: format!("transformed: {}", result.result),
            display_preference: result.display_preference,
        });

        let result = map.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "transformed: original");
    }

    #[tokio::test]
    async fn test_map_failure_preservation() {
        let map = Map::new(FailingComposition, |result| ToolResult {
            success: result.success,
            result: format!("mapped: {}", result.result),
            display_preference: None,
        });

        let result = map.execute(ExecutionContext::new()).await.unwrap();

        assert!(!result.success);
        assert_eq!(result.result.result, "mapped: Failed");
    }
}

mod variable_binding_tests {
    use super::*;

    #[tokio::test]
    async fn test_variable_get_set() {
        let mut ctx = ExecutionContext::new();
        ctx.set_variable("key", json!("value"));

        assert_eq!(
            ctx.get_variable("key").and_then(|v| v.as_str()),
            Some("value")
        );
    }

    #[tokio::test]
    async fn test_variable_builder_pattern() {
        let ctx = ExecutionContext::new()
            .with_variable("a", json!(1))
            .with_variable("b", json!(2));

        assert_eq!(ctx.get_variable("a").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(ctx.get_variable("b").and_then(|v| v.as_i64()), Some(2));
    }

    #[tokio::test]
    async fn test_last_result_tracking() {
        let ctx = ExecutionContext::new().with_last_result(ToolResult {
            success: true,
            result: "previous".to_string(),
            display_preference: None,
        });

        assert!(ctx.last_result.is_some());
        assert_eq!(ctx.last_result.unwrap().result, "previous");
    }

    #[tokio::test]
    async fn test_sequence_variable_accumulation() {
        let tool1 = Arc::new(MockTool::new("tool1", "output1"));
        let tool2 = Arc::new(MockTool::new("tool2", "output2"));

        let seq = Sequence::builder()
            .step(
                ToolComposition::new(tool1.clone(), json!({})).with_output_variable("step1_result"),
            )
            .step(
                ToolComposition::new(tool2.clone(), json!({})).with_output_variable("step2_result"),
            )
            .build();

        let result = seq.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.context.get_variable("step1_result").is_some());
        assert!(result.context.get_variable("step2_result").is_some());
    }
}

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_complex_workflow() {
        // A complex workflow: sequence with parallel step and conditional
        let parallel_step = Parallel::builder()
            .branch(ConstantComposition {
                value: "a".to_string(),
            })
            .branch(ConstantComposition {
                value: "b".to_string(),
            })
            .build();

        let conditional_step = Choice::new(
            |_ctx| true,
            ConstantComposition {
                value: "conditional_true".to_string(),
            },
        );

        let workflow = Sequence::builder()
            .step(ConstantComposition {
                value: "start".to_string(),
            })
            .step(parallel_step)
            .step(conditional_step)
            .step(ConstantComposition {
                value: "end".to_string(),
            })
            .build();

        let result = workflow.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "end");
    }

    #[tokio::test]
    async fn test_nested_composition() {
        let inner_seq = Sequence::builder()
            .step(ConstantComposition {
                value: "inner1".to_string(),
            })
            .step(ConstantComposition {
                value: "inner2".to_string(),
            })
            .build();

        let outer_seq = Sequence::builder()
            .step(ConstantComposition {
                value: "outer_start".to_string(),
            })
            .step(inner_seq)
            .step(ConstantComposition {
                value: "outer_end".to_string(),
            })
            .build();

        let result = outer_seq.execute(ExecutionContext::new()).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result.result, "outer_end");
    }
}
