//! Tool execution helpers for the agent loop runner.

use std::sync::Arc;

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolExecutor};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::metrics::{MetricsCollector, RoundStatus as MetricsRoundStatus};

mod clarification;
mod events;
mod execution_paths;
mod loop_state;
mod per_call;
mod task;
pub(crate) mod tool_error_collector;

use loop_state::RoundExecutionState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolSchedulingMode {
    ParallelSafe,
    Sequential,
}

fn is_parallel_safe_tool_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "FileExists"
            | "Glob"
            | "GetCurrentDir"
            | "GetFileInfo"
            | "Grep"
            | "Read"
            | "WebFetch"
            | "WebSearch"
            | "session_inspector"
    )
}

fn scheduling_mode_for_tool_call(tool_call: &ToolCall) -> ToolSchedulingMode {
    let normalized = crate::agent::tools::normalize_tool_ref(&tool_call.function.name)
        .unwrap_or_else(|| tool_call.function.name.trim().to_string());

    if is_parallel_safe_tool_name(normalized.as_str()) {
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
) -> RoundToolExecutionResult {
    let mut state = RoundExecutionState::default();

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

            // Single parallel-safe tool: execute directly, skip join_all overhead
            if batch.len() == 1 {
                let outcome =
                    per_call::execute_tool_call_only(per_call::ToolExecutionOnlyContext {
                        tool_call: &batch[0],
                        event_tx,
                        metrics_collector,
                        session_id,
                        round_id,
                        round,
                        tools,
                        config,
                    })
                    .await;

                let should_break = per_call::apply_tool_execution_outcome(
                    per_call::ToolExecutionApplyContext {
                        tool_call: &batch[0],
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

                if should_break {
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

            for (batch_call, outcome) in batch.iter().zip(outcomes.into_iter()) {
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

                if should_break {
                    break 'tool_calls;
                }
            }

            continue;
        }

        let should_break = per_call::execute_single_tool_call(per_call::PerToolExecutionContext {
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
            state: &mut state,
        })
        .await;

        next_index += 1;

        if should_break {
            break;
        }
    }

    state.into_result()
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
        let parallel_tools = [
            "FileExists",
            "Glob",
            "GetCurrentDir",
            "GetFileInfo",
            "Grep",
            "Read",
            "WebFetch",
            "WebSearch",
            "session_inspector",
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
            "file_exists",     // alias for FileExists
            "fileExists",      // alias for FileExists
            "list_directory",  // alias for Glob
            "get_file_info",   // alias for GetFileInfo
            "getFileInfo",     // alias for GetFileInfo
            "get_current_dir", // alias for GetCurrentDir
            "getCurrentDir",   // alias for GetCurrentDir
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
            "ask_user",
            "SetWorkspace",
            "Sleep",
            "Task",
            "NotebookEdit",
            "KillShell",
            "memory_note",
            "schedule_tasks",
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
