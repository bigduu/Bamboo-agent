//! Tool execution helpers for the agent loop runner.

use std::sync::Arc;

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::metrics::{MetricsCollector, RoundStatus as MetricsRoundStatus};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_infrastructure::config::PermissionMode;
use bamboo_infrastructure::LLMProvider;

fn build_context_pressure(session: &Session) -> Option<output_compressor::ContextPressure> {
    let usage = session.token_usage.as_ref()?;
    let budget = session.token_budget.as_ref()?;
    let trigger = budget.compression_trigger_context_tokens();
    if trigger == 0 {
        return None;
    }
    let remaining = trigger.saturating_sub(usage.total_tokens);
    let percent = ((usage.total_tokens as f64 / trigger as f64) * 100.0).min(100.0) as u8;
    Some(output_compressor::ContextPressure {
        usage_percent: percent,
        remaining_tokens: remaining,
    })
}

/// Tools exempt from plan-mode blocking (UI pause/clarification tools).
const PLAN_MODE_EXEMPT_TOOLS: &[&str] = &[
    "EnterPlanMode",
    "ExitPlanMode",
    "request_permissions",
    "conclusion_with_options",
    "compact_context",
];

mod clarification;
mod events;
mod execution_paths;
mod loop_state;
mod output_compressor;
mod per_call;
mod policy;
mod task;
pub(in crate::runtime::runner) use task::persist_shared_task_list;
pub(crate) mod tool_error_collector;

use loop_state::RoundExecutionState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolSchedulingMode {
    ParallelSafe,
    Sequential,
}

fn scheduling_mode_for_tool_call(
    tool_call: &ToolCall,
    tools: &Arc<dyn ToolExecutor>,
) -> ToolSchedulingMode {
    let normalized = bamboo_tools::normalize_tool_ref(&tool_call.function.name)
        .unwrap_or_else(|| tool_call.function.name.trim().to_string());

    let canonical = bamboo_tools::resolve_alias(&normalized)
        .map(|s: &str| s.to_string())
        .unwrap_or(normalized);

    let mut effective_call = tool_call.clone();
    effective_call.function.name = canonical;

    if bamboo_tools::parallel::ToolCallRuntime::supports_parallel(tools, &effective_call) {
        ToolSchedulingMode::ParallelSafe
    } else {
        ToolSchedulingMode::Sequential
    }
}

pub(crate) struct RoundToolExecutionResult {
    pub awaiting_clarification: bool,
    pub waiting_for_children: bool,
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
    // Plan mode gate: block mutating tools (except pause/clarification tools)
    if config.permission_mode == Some(PermissionMode::Plan) {
        let tool_name = tool_call.function.name.trim();
        if bamboo_tools::orchestrator::classify_tool(tool_name)
            == bamboo_tools::orchestrator::ToolMutability::Mutating
            && !PLAN_MODE_EXEMPT_TOOLS.contains(&tool_name)
        {
            tracing::warn!(
                "[{}][round:{}] Plan mode blocked mutating tool: tool_call_id={}, tool_name={}",
                session_id,
                round,
                tool_call.id,
                tool_name
            );
            let outcome = per_call::ToolExecutionOutcome {
                result: Err(format!("Plan mode: {} operation blocked", tool_name)),
                tool_duration: std::time::Duration::ZERO,
            };
            policy_guard.observe_outcome(tool_call, &outcome.result);
            let outcome = output_compressor::maybe_compress(
                &tool_call.function.name,
                &tool_call.function.arguments,
                session_id,
                outcome,
                session
                    .token_budget
                    .as_ref()
                    .map(|b| b.max_tool_output_tokens)
                    .unwrap_or(0),
                build_context_pressure(session),
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
            return SingleToolExecutionControl {
                should_break,
                stop_round: false,
            };
        }
    }

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
        session
            .token_budget
            .as_ref()
            .map(|b| b.max_tool_output_tokens)
            .unwrap_or(0),
        build_context_pressure(session),
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

/// Check if the most recent tool result is from `compact_context` and set
/// the manual compression flag on the session so the next compression check
/// forces a compression cycle regardless of threshold.
///
/// Detection is based on the tool call name in the assistant message, not the
/// tool result content — this avoids fragility if the tool output text changes.
fn detect_manual_compression_request(session: &mut Session) {
    if session.force_manual_compression.is_some() {
        return;
    }

    // Find the most recent assistant message containing a compact_context tool call.
    let Some((call_id, instructions)) = session
        .messages
        .iter()
        .rev()
        .take(6)
        .find(|m| {
            m.role == bamboo_agent_core::Role::Assistant
                && m.tool_calls
                    .as_ref()
                    .is_some_and(|calls| calls.iter().any(|c| c.function.name == "compact_context"))
        })
        .and_then(|m| {
            m.tool_calls.as_ref().and_then(|calls| {
                let call = calls
                    .iter()
                    .find(|c| c.function.name == "compact_context")?;
                let instructions =
                    serde_json::from_str::<serde_json::Value>(&call.function.arguments)
                        .ok()
                        .and_then(|args| args.get("instructions").cloned())
                        .and_then(|v| v.as_str().map(String::from));
                Some((call.id.clone(), instructions))
            })
        })
    else {
        return;
    };

    // Verify the corresponding tool result exists (call completed, not in-flight).
    let result_exists = session.messages.iter().rev().take(4).any(|m| {
        m.role == bamboo_agent_core::Role::Tool
            && m.tool_call_id.as_deref() == Some(call_id.as_str())
    });

    if result_exists {
        tracing::info!("detected compact_context tool call, flagging for manual compression");
        session.force_manual_compression = Some(instructions.unwrap_or_default());
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
    _compression_model_provider: Option<&Arc<dyn LLMProvider>>,
    tool_schemas: &[ToolSchema],
) -> Result<(), AgentError> {
    let Some(model_name) = model_name else {
        return Ok(());
    };

    detect_manual_compression_request(session);

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

pub(crate) async fn execute_round_tool_calls(
    tool_calls: &[ToolCall],
    frame: &crate::runtime::runner::round_frame::RoundFrame<'_>,
    session: &mut Session,
    task_context: &mut Option<TaskLoopContext>,
    compression_model_name: Option<&str>,
    compression_model_provider: Option<&Arc<dyn LLMProvider>>,
    tool_schemas: &[ToolSchema],
) -> Result<RoundToolExecutionResult, AgentError> {
    // Bind frame fields as locals so the rest of the function body stays unchanged.
    let event_tx = frame.event_tx;
    let metrics_collector = frame.metrics_collector;
    let session_id = frame.session_id;
    let round_id = frame.round_id;
    let round = frame.turn;
    let tools = frame.tools;
    let config = frame.config;
    let llm = frame.llm;

    let mut state = RoundExecutionState::default();
    let mut policy_guard = policy::ToolPolicyGuard::new(
        config.max_tool_calls_per_round,
        config.max_consecutive_failures_per_tool,
    );

    // Pre-classify all tool calls to avoid repeated normalization.
    let scheduling_modes: Vec<ToolSchedulingMode> = tool_calls
        .iter()
        .map(|tc| scheduling_mode_for_tool_call(tc, tools))
        .collect();

    let mut next_index = 0usize;
    'tool_calls: while next_index < tool_calls.len() {
        let tool_call = &tool_calls[next_index];

        if scheduling_modes[next_index] == ToolSchedulingMode::ParallelSafe {
            let batch_start = next_index;
            while next_index < tool_calls.len()
                && scheduling_modes[next_index] == ToolSchedulingMode::ParallelSafe
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
                        compression_model_provider,
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
                    compression_model_provider,
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
            let per_tool_timeout = std::time::Duration::from_secs(config.per_tool_timeout_secs);
            let batch_timeout = std::time::Duration::from_secs(config.parallel_batch_timeout_secs);
            let outcomes = tokio::time::timeout(
                batch_timeout,
                join_all(batch.iter().map(|tool_call| {
                    let timeout = per_tool_timeout;
                    async move {
                        tokio::time::timeout(
                            timeout,
                            per_call::execute_tool_call_only(per_call::ToolExecutionOnlyContext {
                                tool_call,
                                event_tx,
                                metrics_collector,
                                session_id,
                                round_id,
                                round,
                                tools,
                                config,
                            }),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            per_call::ToolExecutionOutcome {
                                result: Err(format!(
                                    "Tool '{}' timed out after {:?}",
                                    tool_call.function.name, timeout
                                )),
                                tool_duration: timeout,
                            }
                        })
                    }
                })),
            )
            .await
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "[{}][round:{}] Parallel batch timed out after {:?}",
                    session_id,
                    round,
                    batch_timeout
                );
                batch
                    .iter()
                    .map(|_batch_call| per_call::ToolExecutionOutcome {
                        result: Err(format!(
                            "Parallel batch timed out after {:?}",
                            batch_timeout
                        )),
                        tool_duration: batch_timeout,
                    })
                    .collect::<Vec<_>>()
            });
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

            // Compress all outcomes in parallel before applying sequentially.
            let max_tool_tokens = session
                .token_budget
                .as_ref()
                .map(|b| b.max_tool_output_tokens)
                .unwrap_or(0);
            let pressure = build_context_pressure(session);
            let compressed: Vec<_> =
                join_all(batch.iter().zip(outcomes).map(|(batch_call, outcome)| {
                    let tool_name = batch_call.function.name.clone();
                    let args = batch_call.function.arguments.clone();
                    let sid = session_id.to_string();
                    let pressure = pressure
                        .as_ref()
                        .map(|p| output_compressor::ContextPressure {
                            usage_percent: p.usage_percent,
                            remaining_tokens: p.remaining_tokens,
                        });
                    async move {
                        output_compressor::maybe_compress(
                            &tool_name,
                            &args,
                            &sid,
                            outcome,
                            max_tool_tokens,
                            pressure,
                        )
                        .await
                    }
                }))
                .await;

            for (batch_call, outcome) in batch.iter().zip(compressed) {
                policy_guard.observe_outcome(batch_call, &outcome.result);

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
                    compression_model_provider,
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
            compression_model_provider,
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
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutor};
    use bamboo_tools::BuiltinToolExecutor;
    use serde_json::json;
    use std::sync::Arc;

    fn tool_call(name: &str) -> ToolCall {
        tool_call_with_args(name, json!({}))
    }

    fn tool_call_with_args(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    fn builtin_tools() -> Arc<dyn ToolExecutor> {
        Arc::new(BuiltinToolExecutor::new())
    }

    #[test]
    fn read_tools_are_parallel_safe() {
        let tools = builtin_tools();
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("Read"), &tools),
            ToolSchedulingMode::ParallelSafe
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("read_file"), &tools),
            ToolSchedulingMode::ParallelSafe
        );
    }

    #[test]
    fn all_parallel_safe_tools_are_classified_correctly() {
        let tools = builtin_tools();
        let parallel_tools = [
            "GetFileInfo",
            "Glob",
            "Grep",
            "Read",
            "WebFetch",
            "WebSearch",
            "Workspace",
            "BashOutput",
            "session_history",
            "Sleep",
        ];
        for name in &parallel_tools {
            assert_eq!(
                scheduling_mode_for_tool_call(&tool_call(name), &tools),
                ToolSchedulingMode::ParallelSafe,
                "{name} should be parallel-safe"
            );
        }

        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("session_note", json!({"action": "read"})),
                &tools
            ),
            ToolSchedulingMode::ParallelSafe,
            "session_note read action should be parallel-safe"
        );
        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("session_note", json!({"action": "list_topics"})),
                &tools
            ),
            ToolSchedulingMode::ParallelSafe,
            "session_note list_topics action should be parallel-safe"
        );
        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("session_note", json!({"action": "append", "content": "x"})),
                &tools
            ),
            ToolSchedulingMode::Sequential,
            "session_note append action should be sequential"
        );
    }

    #[test]
    fn aliases_resolve_to_parallel_safe() {
        let tools = builtin_tools();
        let aliases = [
            "read_file",
            "file_exists",
            "fileExists",
            "list_directory",
            "get_file_info",
            "getFileInfo",
            "get_current_dir",
            "getCurrentDir",
        ];
        for alias in &aliases {
            assert_eq!(
                scheduling_mode_for_tool_call(&tool_call(alias), &tools),
                ToolSchedulingMode::ParallelSafe,
                "alias {alias} should resolve to a parallel-safe tool"
            );
        }

        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("memory_note", json!({"action": "read"})),
                &tools
            ),
            ToolSchedulingMode::ParallelSafe,
            "memory_note read alias should be parallel-safe"
        );
        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("memory_note", json!({"action": "list_topics"})),
                &tools
            ),
            ToolSchedulingMode::ParallelSafe,
            "memory_note list_topics alias should be parallel-safe"
        );
        assert_eq!(
            scheduling_mode_for_tool_call(
                &tool_call_with_args("memory_note", json!({"action": "append", "content": "x"})),
                &tools
            ),
            ToolSchedulingMode::Sequential,
            "memory_note append alias should be sequential"
        );
    }

    #[test]
    fn side_effect_tools_remain_sequential() {
        let tools = builtin_tools();
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
                scheduling_mode_for_tool_call(&tool_call(name), &tools),
                ToolSchedulingMode::Sequential,
                "{name} should be sequential"
            );
        }
    }

    #[test]
    fn mcp_tools_are_sequential() {
        let tools = builtin_tools();
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("mcp__playwright__browser_snapshot"), &tools),
            ToolSchedulingMode::Sequential,
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("mcp__some_server__some_tool"), &tools),
            ToolSchedulingMode::Sequential,
        );
    }

    #[test]
    fn unknown_tools_are_sequential() {
        let tools = builtin_tools();
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call("totally_unknown_tool"), &tools),
            ToolSchedulingMode::Sequential,
        );
        assert_eq!(
            scheduling_mode_for_tool_call(&tool_call(""), &tools),
            ToolSchedulingMode::Sequential,
        );
    }

    #[test]
    fn plan_mode_exempt_tools_are_correct() {
        use super::PLAN_MODE_EXEMPT_TOOLS;

        // These tools must NOT be blocked by plan mode
        assert!(PLAN_MODE_EXEMPT_TOOLS.contains(&"EnterPlanMode"));
        assert!(PLAN_MODE_EXEMPT_TOOLS.contains(&"ExitPlanMode"));
        assert!(PLAN_MODE_EXEMPT_TOOLS.contains(&"request_permissions"));
        assert!(PLAN_MODE_EXEMPT_TOOLS.contains(&"conclusion_with_options"));
        assert!(PLAN_MODE_EXEMPT_TOOLS.contains(&"compact_context"));
    }

    #[test]
    fn plan_mode_blocks_mutating_tools_via_classify() {
        use super::PLAN_MODE_EXEMPT_TOOLS;
        use bamboo_infrastructure::config::PermissionMode;

        let mut config = crate::runtime::config::AgentLoopConfig::default();
        config.permission_mode = Some(PermissionMode::Plan);

        let mutating_tools = ["Write", "Edit", "Bash", "NotebookEdit", "KillShell"];
        for name in &mutating_tools {
            let is_mutating = bamboo_tools::orchestrator::classify_tool(name)
                == bamboo_tools::orchestrator::ToolMutability::Mutating;
            let is_exempt = PLAN_MODE_EXEMPT_TOOLS.contains(name);
            assert!(
                is_mutating && !is_exempt,
                "{name} should be blocked in plan mode"
            );
        }
    }

    #[test]
    fn plan_mode_allows_read_only_tools_via_classify() {
        // Read-only tools should pass through plan mode
        let read_only_tools = [
            "Read",
            "GetFileInfo",
            "Glob",
            "Grep",
            "WebFetch",
            "WebSearch",
            "BashOutput",
            "session_history",
            "Sleep",
        ];
        for name in &read_only_tools {
            let is_readonly = bamboo_tools::orchestrator::classify_tool(name)
                == bamboo_tools::orchestrator::ToolMutability::ReadOnly;
            assert!(
                is_readonly,
                "{name} should be read-only (allowed in plan mode)"
            );
        }
    }

    #[tokio::test]
    async fn plan_mode_gate_blocks_write_in_pipeline() {
        use super::{execute_and_apply_single_tool_call, loop_state::RoundExecutionState, policy};
        use bamboo_agent_core::Session;
        use bamboo_infrastructure::config::PermissionMode;
        use tokio::sync::mpsc;

        let (event_tx, _event_rx) = mpsc::channel(100);
        let mut session = Session::new("test-session", "test-model");
        let tools = builtin_tools();
        let mut config = crate::runtime::config::AgentLoopConfig::default();
        config.permission_mode = Some(PermissionMode::Plan);

        let mut state = RoundExecutionState::default();
        let mut policy_guard = policy::ToolPolicyGuard::new(80, 3);

        let tool_call = tool_call_with_args(
            "Write",
            json!({"file_path": "/tmp/plan_mode_test.txt", "content": "test"}),
        );

        let control = execute_and_apply_single_tool_call(
            &tool_call,
            &event_tx,
            None,
            "test-session",
            "test-round-1",
            0,
            &mut session,
            &tools,
            &config,
            &mut None,
            &mut state,
            &mut policy_guard,
            0,
        )
        .await;

        // Should not break the round, just block the tool
        assert!(!control.should_break);
        assert!(!control.stop_round);

        // The session should have a tool result message with the plan mode error
        let last_msg = session.messages.last().expect("should have a tool result");
        assert!(
            last_msg.content.contains("Plan mode"),
            "Tool result should contain 'Plan mode' error, got: {}",
            last_msg.content
        );
    }

    #[tokio::test]
    async fn plan_mode_gate_allows_read_in_pipeline() {
        use super::{execute_and_apply_single_tool_call, loop_state::RoundExecutionState, policy};
        use bamboo_agent_core::Session;
        use bamboo_infrastructure::config::PermissionMode;
        use tokio::sync::mpsc;

        let (event_tx, _event_rx) = mpsc::channel(100);
        let mut session = Session::new("test-session", "test-model");
        let tools = builtin_tools();
        let mut config = crate::runtime::config::AgentLoopConfig::default();
        config.permission_mode = Some(PermissionMode::Plan);

        let mut state = RoundExecutionState::default();
        let mut policy_guard = policy::ToolPolicyGuard::new(80, 3);

        let temp_dir = std::env::temp_dir().join("bamboo_plan_mode_read_test");
        std::fs::create_dir_all(&temp_dir).ok();
        let file_path = temp_dir.join("test.txt");
        std::fs::write(&file_path, "hello").ok();

        let tool_call =
            tool_call_with_args("Read", json!({"file_path": file_path.to_str().unwrap()}));

        let control = execute_and_apply_single_tool_call(
            &tool_call,
            &event_tx,
            None,
            "test-session",
            "test-round-1",
            0,
            &mut session,
            &tools,
            &config,
            &mut None,
            &mut state,
            &mut policy_guard,
            0,
        )
        .await;

        assert!(!control.should_break);
        assert!(!control.stop_round);

        let last_msg = session.messages.last().expect("should have a tool result");
        assert!(
            !last_msg.content.contains("Plan mode"),
            "Read should not be blocked in plan mode, got: {}",
            last_msg.content
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn plan_mode_gate_allows_exit_plan_mode_tool() {
        use super::{execute_and_apply_single_tool_call, loop_state::RoundExecutionState, policy};
        use bamboo_agent_core::Session;
        use bamboo_infrastructure::config::PermissionMode;
        use tokio::sync::mpsc;

        let (event_tx, _event_rx) = mpsc::channel(100);
        let mut session = Session::new("test-session", "test-model");
        let tools = builtin_tools();
        let mut config = crate::runtime::config::AgentLoopConfig::default();
        config.permission_mode = Some(PermissionMode::Plan);

        let mut state = RoundExecutionState::default();
        let mut policy_guard = policy::ToolPolicyGuard::new(80, 3);

        let tool_call = tool_call_with_args("ExitPlanMode", json!({"plan": "test plan"}));

        let control = execute_and_apply_single_tool_call(
            &tool_call,
            &event_tx,
            None,
            "test-session",
            "test-round-1",
            0,
            &mut session,
            &tools,
            &config,
            &mut None,
            &mut state,
            &mut policy_guard,
            0,
        )
        .await;

        assert!(!control.stop_round);
        let last_msg = session.messages.last().expect("should have a tool result");
        assert!(
            !last_msg
                .content
                .contains("Plan mode: ExitPlanMode operation blocked"),
            "ExitPlanMode should be exempt from plan mode gate, got: {}",
            last_msg.content
        );
    }

    #[tokio::test]
    async fn default_mode_does_not_block_write() {
        use super::{execute_and_apply_single_tool_call, loop_state::RoundExecutionState, policy};
        use bamboo_agent_core::Session;
        use tokio::sync::mpsc;

        let (event_tx, _event_rx) = mpsc::channel(100);
        let mut session = Session::new("test-session", "test-model");
        let tools = builtin_tools();
        let config = crate::runtime::config::AgentLoopConfig::default();

        let mut state = RoundExecutionState::default();
        let mut policy_guard = policy::ToolPolicyGuard::new(80, 3);

        let temp_dir = std::env::temp_dir().join("bamboo_default_mode_test");
        std::fs::create_dir_all(&temp_dir).ok();
        let file_path = temp_dir.join("test.txt");

        let tool_call = tool_call_with_args(
            "Write",
            json!({"file_path": file_path.to_str().unwrap(), "content": "test"}),
        );

        let control = execute_and_apply_single_tool_call(
            &tool_call,
            &event_tx,
            None,
            "test-session",
            "test-round-1",
            0,
            &mut session,
            &tools,
            &config,
            &mut None,
            &mut state,
            &mut policy_guard,
            0,
        )
        .await;

        assert!(!control.stop_round);
        let last_msg = session.messages.last().expect("should have a tool result");
        assert!(
            !last_msg.content.contains("Plan mode"),
            "Write should work in default mode, got: {}",
            last_msg.content
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn detect_manual_compression_request_sets_flag_when_tool_result_exists() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s1", "m1");

        // Assistant message with compact_context tool call
        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "compact_context".to_string(),
                arguments: r#"{"instructions":"keep API signatures"}"#.to_string(),
            },
        }]);
        session.messages.push(assistant);

        // Tool result message
        let mut tool_result = Message::tool_result("call-1", "Context compression requested");
        tool_result.id = "msg-2".to_string();
        session.messages.push(tool_result);

        assert!(session.force_manual_compression.is_none());
        detect_manual_compression_request(&mut session);
        assert_eq!(
            session.force_manual_compression.as_deref(),
            Some("keep API signatures")
        );
    }

    #[test]
    fn detect_manual_compression_request_extracts_empty_when_no_instructions() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s2", "m2");

        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![ToolCall {
            id: "call-2".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "compact_context".to_string(),
                arguments: "{}".to_string(),
            },
        }]);
        session.messages.push(assistant);

        let mut tool_result = Message::tool_result("call-2", "ok");
        tool_result.id = "msg-2".to_string();
        session.messages.push(tool_result);

        detect_manual_compression_request(&mut session);
        assert!(session.force_manual_compression.is_some());
        assert_eq!(session.force_manual_compression.as_deref(), Some(""));
    }

    #[test]
    fn detect_manual_compression_request_skips_if_flag_already_set() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s3", "m3");
        session.force_manual_compression = Some("already set".to_string());

        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![ToolCall {
            id: "call-3".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "compact_context".to_string(),
                arguments: r#"{"instructions":"new instructions"}"#.to_string(),
            },
        }]);
        session.messages.push(assistant);

        let mut tool_result = Message::tool_result("call-3", "ok");
        tool_result.id = "msg-2".to_string();
        session.messages.push(tool_result);

        detect_manual_compression_request(&mut session);
        assert_eq!(
            session.force_manual_compression.as_deref(),
            Some("already set")
        );
    }

    #[test]
    fn detect_manual_compression_request_does_nothing_without_tool_result() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s4", "m4");

        // Only the assistant tool call — no tool result yet (tool is in-flight)
        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![ToolCall {
            id: "call-4".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "compact_context".to_string(),
                arguments: "{}".to_string(),
            },
        }]);
        session.messages.push(assistant);

        detect_manual_compression_request(&mut session);
        assert!(session.force_manual_compression.is_none());
    }

    #[test]
    fn detect_manual_compression_request_does_nothing_for_other_tools() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s5", "m5");

        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![ToolCall {
            id: "call-5".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: r#"{"file_path":"/tmp/test"}"#.to_string(),
            },
        }]);
        session.messages.push(assistant);

        let mut tool_result = Message::tool_result("call-5", "file contents");
        tool_result.id = "msg-2".to_string();
        session.messages.push(tool_result);

        detect_manual_compression_request(&mut session);
        assert!(session.force_manual_compression.is_none());
    }

    #[test]
    fn detect_manual_compression_request_finds_call_among_parallel_tool_calls() {
        use super::detect_manual_compression_request;
        use bamboo_agent_core::tools::FunctionCall;
        use bamboo_agent_core::tools::ToolCall;
        use bamboo_agent_core::{Message, Session};

        let mut session = Session::new("s6", "m6");

        // Multiple tool calls in one assistant turn, including compact_context
        let mut assistant = Message::assistant("", None);
        assistant.id = "msg-1".to_string();
        assistant.tool_calls = Some(vec![
            ToolCall {
                id: "call-read".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Read".to_string(),
                    arguments: r#"{"file_path":"/tmp/a"}"#.to_string(),
                },
            },
            ToolCall {
                id: "call-compact".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "compact_context".to_string(),
                    arguments: r#"{"instructions":"preserve error traces"}"#.to_string(),
                },
            },
        ]);
        session.messages.push(assistant);

        let mut read_result = Message::tool_result("call-read", "file a");
        read_result.id = "msg-2".to_string();
        session.messages.push(read_result);

        let mut compact_result = Message::tool_result("call-compact", "ok");
        compact_result.id = "msg-3".to_string();
        session.messages.push(compact_result);

        detect_manual_compression_request(&mut session);
        assert_eq!(
            session.force_manual_compression.as_deref(),
            Some("preserve error traces")
        );
    }
}
