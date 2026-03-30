//! Tool execution helpers for the agent loop runner.

use std::sync::Arc;

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolExecutor, ToolSchema};
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::metrics::{MetricsCollector, RoundStatus as MetricsRoundStatus};

mod clarification;
mod events;
mod execution_paths;
mod loop_state;
mod output_compressor;
mod per_call;
mod policy;
mod task;
pub(crate) mod tool_error_collector;

use loop_state::RoundExecutionState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolSchedulingMode {
    ParallelSafe,
    Sequential,
}

fn scheduling_mode_for_tool_call(tool_call: &ToolCall) -> ToolSchedulingMode {
    let normalized = crate::agent::tools::normalize_tool_ref(&tool_call.function.name)
        .unwrap_or_else(|| tool_call.function.name.trim().to_string());

    // Resolve aliases to canonical names (e.g. FileExists → GetFileInfo)
    // so the classifier sees the canonical tool name.
    let canonical = crate::agent::tools::resolve_alias(&normalized)
        .map(|s| s.to_string())
        .unwrap_or(normalized);

    // Delegate to the centralized classification in orchestrator.rs
    // which also powers ToolCallRuntime's concurrency control.
    if crate::agent::tools::parallel::ToolCallRuntime::supports_parallel(&canonical) {
        ToolSchedulingMode::ParallelSafe
    } else {
        ToolSchedulingMode::Sequential
    }
}

pub(super) struct RoundToolExecutionResult {
    pub awaiting_clarification: bool,
    pub round_status: MetricsRoundStatus,
    pub round_error: Option<String>,
}

struct SingleToolExecutionControl {
    should_break: bool,
    stop_round: bool,
}

#[allow(clippy::too_many_arguments)]
async fn execute_and_apply_single_tool_call(
    tool_call: &ToolCall,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    round: usize,
    session: &mut Session,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
    state: &mut RoundExecutionState,
    policy_guard: &mut policy::ToolPolicyGuard,
    reserved_calls: usize,
) -> SingleToolExecutionControl {
    let mut stop_round = false;
    let outcome = match policy_guard.check_before_execution(tool_call, reserved_calls) {
        Ok(()) => {
            if let Err(policy_error) = policy::validate_tool_call_context(tool_call, session) {
                tracing::warn!(
                    "[{}][round:{}] Tool call blocked by context policy before ToolStart: tool_call_id={}, tool_name={}, error={}",
                    session_id,
                    round,
                    tool_call.id,
                    tool_call.function.name,
                    policy_error
                );
                per_call::ToolExecutionOutcome {
                    result: Err(policy_error),
                    tool_duration: std::time::Duration::ZERO,
                }
            } else {
                per_call::execute_tool_call_only(per_call::ToolExecutionOnlyContext {
                    tool_call,
                    event_tx,
                    metrics_collector,
                    session_id,
                    round_id,
                    round,
                    tools,
                    config,
                })
                .await
            }
        }
        Err(violation) => {
            stop_round = violation.should_stop_round();
            let message = violation.into_message();
            tracing::warn!(
                "[{}][round:{}] Tool call blocked by policy before execution: tool_call_id={}, tool_name={}, error={}",
                session_id,
                round,
                tool_call.id,
                tool_call.function.name,
                message
            );
            per_call::ToolExecutionOutcome {
                result: Err(message),
                tool_duration: std::time::Duration::ZERO,
            }
        }
    };

    policy_guard.observe_outcome(tool_call, &outcome.result);

    // Compress tool output before applying
    let outcome = output_compressor::maybe_compress(
        &tool_call.function.name,
        &tool_call.function.arguments,
        session_id,
        outcome,
    )
    .await;

    let should_break = per_call::apply_tool_execution_outcome(
        per_call::ToolExecutionApplyContext {
            tool_call,
            event_tx,
            metrics_collector,
            session_id,
            round_id,
            round,
            session,
            tools,
            config,
            task_context,
            state,
        },
        outcome,
    )
    .await;

    SingleToolExecutionControl {
        should_break,
        stop_round,
    }
}

#[allow(clippy::too_many_arguments)]
async fn maybe_apply_mid_turn_context_compression_after_tool(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model_name: Option<&str>,
    tool_schemas: &[ToolSchema],
) -> Result<(), AgentError> {
    let Some(model_name) = model_name else {
        return Ok(());
    };

    if super::round_lifecycle::maybe_apply_mid_turn_context_compression(
        session,
        config,
        llm,
        event_tx,
        session_id,
        model_name,
        tool_schemas,
    )
    .await?
    {
        tracing::debug!(
            "[{}] Applied mid-turn host context compression after single tool result",
            session_id
        );
    }
    Ok(())
}

pub(super) async fn execute_round_tool_calls(
    tool_calls: &[ToolCall],
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    round: usize,
    session: &mut Session,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
    llm: &Arc<dyn LLMProvider>,
    compression_model_name: Option<&str>,
    tool_schemas: &[ToolSchema],
) -> Result<RoundToolExecutionResult, AgentError> {
    let mut state = RoundExecutionState::default();
    let mut policy_guard = policy::ToolPolicyGuard::default();

    let mut next_index = 0usize;
    'tool_calls: while next_index < tool_calls.len() {
        let tool_call = &tool_calls[next_index];

        if scheduling_mode_for_tool_call(tool_call) == ToolSchedulingMode::ParallelSafe {
            let batch_start = next_index;
            while next_index < tool_calls.len()
                && scheduling_mode_for_tool_call(&tool_calls[next_index])
                    == ToolSchedulingMode::ParallelSafe
            {
                next_index += 1;
            }

            let batch = &tool_calls[batch_start..next_index];

            let policy_precheck_error = batch
                .iter()
                .enumerate()
                .find_map(|(offset, call)| policy_guard.check_before_execution(call, offset).err());

            if policy_precheck_error.is_some() {
                for batch_call in batch {
                    let control = execute_and_apply_single_tool_call(
                        batch_call,
                        event_tx,
                        metrics_collector,
                        session_id,
                        round_id,
                        round,
                        session,
                        tools,
                        config,
                        task_context,
                        &mut state,
                        &mut policy_guard,
                        0,
                    )
                    .await;

                    maybe_apply_mid_turn_context_compression_after_tool(
                        session,
                        config,
                        llm,
                        event_tx,
                        session_id,
                        compression_model_name,
                        tool_schemas,
                    )
                    .await?;

                    if control.should_break || control.stop_round {
                        break 'tool_calls;
                    }
                }
                continue;
            }

            // Single parallel-safe tool: execute directly, skip join_all overhead
            if batch.len() == 1 {
                let control = execute_and_apply_single_tool_call(
                    &batch[0],
                    event_tx,
                    metrics_collector,
                    session_id,
                    round_id,
                    round,
                    session,
                    tools,
                    config,
                    task_context,
                    &mut state,
                    &mut policy_guard,
                    0,
                )
                .await;

                maybe_apply_mid_turn_context_compression_after_tool(
                    session,
                    config,
                    llm,
                    event_tx,
                    session_id,
                    compression_model_name,
                    tool_schemas,
                )
                .await?;

                if control.should_break || control.stop_round {
                    break 'tool_calls;
                }
                continue;
            }

            let tool_names: Vec<&str> = batch.iter().map(|tc| tc.function.name.as_str()).collect();
            tracing::info!(
                "[{}][round:{}] ⚡ Executing {} parallel-safe tool calls concurrently: {:?}",
                session_id,
                round,
                batch.len(),
                tool_names
            );

            let parallel_start = std::time::Instant::now();
            let outcomes = join_all(batch.iter().map(|batch_call| {
                per_call::execute_tool_call_only(per_call::ToolExecutionOnlyContext {
                    tool_call: batch_call,
                    event_tx,
                    metrics_collector,
                    session_id,
                    round_id,
                    round,
                    tools,
                    config,
                })
            }))
            .await;
            let parallel_elapsed = parallel_start.elapsed();

            // Log individual tool durations to confirm parallelism
            let individual_durations: Vec<String> = batch
                .iter()
                .zip(outcomes.iter())
                .map(|(tc, o)| format!("{}={:?}", tc.function.name, o.tool_duration))
                .collect();
            let sum_sequential: std::time::Duration =
                outcomes.iter().map(|o| o.tool_duration).sum();
            tracing::info!(
                "[{}][round:{}] ⚡ Parallel batch completed in {:?} (sequential would be {:?}, speedup {:.1}x): [{}]",
                session_id,
                round,
                parallel_elapsed,
                sum_sequential,
                if parallel_elapsed.as_millis() > 0 {
                    sum_sequential.as_millis() as f64 / parallel_elapsed.as_millis() as f64
                } else {
                    1.0
                },
                individual_durations.join(", ")
            );

            for (batch_call, mut outcome) in batch.iter().zip(outcomes.into_iter()) {
                policy_guard.observe_outcome(batch_call, &outcome.result);

                // Compress tool output before applying
                outcome = output_compressor::maybe_compress(
                    &batch_call.function.name,
                    &batch_call.function.arguments,
                    session_id,
                    outcome,
                )
                .await;

                let should_break = per_call::apply_tool_execution_outcome(
                    per_call::ToolExecutionApplyContext {
                        tool_call: batch_call,
                        event_tx,
                        metrics_collector,
                        session_id,
                        round_id,
                        round,
                        session,
                        tools,
                        config,
                        task_context,
                        state: &mut state,
                    },
                    outcome,
                )
                .await;

                maybe_apply_mid_turn_context_compression_after_tool(
                    session,
                    config,
                    llm,
                    event_tx,
                    session_id,
                    compression_model_name,
                    tool_schemas,
                )
                .await?;

                if should_break {
                    break 'tool_calls;
                }
            }

            continue;
        }

        let control = execute_and_apply_single_tool_call(
            tool_call,
            event_tx,
            metrics_collector,
            session_id,
            round_id,
            round,
            session,
            tools,
            config,
            task_context,
            &mut state,
            &mut policy_guard,
            0,
        )
        .await;

        next_index += 1;

        maybe_apply_mid_turn_context_compression_after_tool(
            session,
            config,
            llm,
            event_tx,
            session_id,
            compression_model_name,
            tool_schemas,
        )
        .await?;

        if control.should_break || control.stop_round {
            break;
        }
    }

    Ok(state.into_result())
}

#[cfg(test)]
mod tests {
    use super::{scheduling_mode_for_tool_call, ToolSchedulingMode};
    use crate::agent::core::tools::{FunctionCall, ToolCall};

    fn tool_call(name: &str) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn read_tools_are_parallel_safe() {
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("Read")),
            ToolSchedulingMode::ParallelSafe
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("read_file")),
            ToolSchedulingMode::ParallelSafe
        );
    }

    #[test]
    fn all_parallel_safe_tools_are_classified_correctly() {
        // Canonical read-only tools (from orchestrator::READ_ONLY_TOOLS)
        let parallel_tools = [
            "GetFileInfo",
            "Glob",
            "Grep",
            "Read",
            "WebFetch",
            "WebSearch",
            "Workspace",
            "BashOutput",
            "tool_search",
            "memory_note",
            "recall",
            "Sleep",
        ];
        for name in &parallel_tools {
            assert_eq!(
                scheduling_mode_for_tool_call(&tool_call(name)),
                ToolSchedulingMode::ParallelSafe,
                "{name} should be parallel-safe"
            );
        }
    }

    #[test]
    fn aliases_resolve_to_parallel_safe() {
        let aliases = [
            "read_file",       // alias for Read
            "file_exists",     // alias → FileExists → GetFileInfo
            "fileExists",      // alias → FileExists → GetFileInfo
            "list_directory",  // alias for Glob
            "get_file_info",   // alias for GetFileInfo
            "getFileInfo",     // alias for GetFileInfo
            "get_current_dir", // alias → GetCurrentDir → Workspace
            "getCurrentDir",   // alias → GetCurrentDir → Workspace
        ];
        for alias in &aliases {
            assert_eq!(
                scheduling_mode_for_tool_call(&tool_call(alias)),
                ToolSchedulingMode::ParallelSafe,
                "alias {alias} should resolve to a parallel-safe tool"
            );
        }
    }

    #[test]
    fn side_effect_tools_remain_sequential() {
        let sequential_tools = [
            "Write",
            "Edit",
            "Bash",
            "conclusion_with_options",
            "Task",
            "NotebookEdit",
            "KillShell",
            "scheduler",
            "SubSession",
        ];
        for name in &sequential_tools {
            assert_eq!(
                scheduling_mode_for_tool_call(&tool_call(name)),
                ToolSchedulingMode::Sequential,
                "{name} should be sequential"
            );
        }
    }

    #[test]
    fn mcp_tools_are_sequential() {
        // MCP tool names use __ separator, not ::
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("mcp__playwright__browser_snapshot")),
            ToolSchedulingMode::Sequential,
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("mcp__some_server__some_tool")),
            ToolSchedulingMode::Sequential,
        );
    }

    #[test]
    fn unknown_tools_are_sequential() {
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("totally_unknown_tool")),
            ToolSchedulingMode::Sequential,
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("")),
            ToolSchedulingMode::Sequential,
        );
    }
}
