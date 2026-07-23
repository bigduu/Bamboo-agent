use bamboo_agent_core::tools::{handle_tool_result_with_agentic_support, ToolHandlingOutcome};
use bamboo_agent_core::AgentEvent;

use super::super::{clarification, events, task, tool_error_collector};
use super::{goal, workspace, SuccessPathContext};

const WORKFLOW_TOOL_METADATA_KEYS: &[&str] = &[
    bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY,
    bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
    bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
    bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
    bamboo_skills::runtime_metadata::LOADED_SKILL_IDS_METADATA_KEY,
    bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_ID_METADATA_KEY,
    bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
];

async fn refresh_workflow_tool_side_effects(ctx: &mut SuccessPathContext<'_>) {
    if !ctx.result.success {
        return;
    }
    let keys: &[&str] = match ctx.tool_call.function.name.as_str() {
        "load_skill" => WORKFLOW_TOOL_METADATA_KEYS,
        "workflow_run" => &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
        _ => return,
    };
    let Some(persistence) = ctx.config.persistence.as_ref() else {
        return;
    };
    match persistence.load_runtime_session(ctx.session_id).await {
        Ok(Some(latest)) => {
            for key in keys {
                if let Some(value) = latest.metadata.get(*key) {
                    ctx.session
                        .metadata
                        .insert((*key).to_string(), value.clone());
                } else {
                    ctx.session.metadata.remove(*key);
                }
            }
        }
        Ok(None) => tracing::warn!(
            "[{}] load_skill completed but no repository session was available for activation refresh",
            ctx.session_id
        ),
        Err(error) => tracing::warn!(
            "[{}] load_skill activation refresh failed: {}",
            ctx.session_id,
            error
        ),
    }
}

pub(super) async fn handle_successful_tool_result(mut ctx: SuccessPathContext<'_>) -> bool {
    // Server tools mutate a repository-owned Session clone. Pull only the
    // workflow activation namespace into the runner's live Session before the
    // next round and before any later runtime save can overwrite those changes.
    refresh_workflow_tool_side_effects(&mut ctx).await;
    task::track_task_progress(
        ctx.task_context,
        ctx.event_tx,
        ctx.session_id,
        ctx.tool_call,
        ctx.result,
        ctx.round,
    )
    .await;

    task::maybe_handle_taskwrite(
        ctx.tool_call,
        ctx.result,
        ctx.session,
        ctx.session_id,
        ctx.event_tx,
        ctx.config,
        ctx.task_context,
    )
    .await;

    workspace::maybe_apply_workspace_update(
        ctx.session,
        ctx.tool_call,
        ctx.result,
        ctx.session_id,
        ctx.config.project_context_resolver.as_deref(),
        ctx.event_tx,
    )
    .await;

    goal::maybe_apply_goal_update(
        ctx.session,
        ctx.tool_call,
        ctx.result,
        ctx.config,
        ctx.round,
    );

    if clarification::maybe_handle_user_question_tool(
        ctx.tool_call,
        ctx.result,
        ctx.session,
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        ctx.config,
    )
    .await
    {
        ctx.state.mark_awaiting_clarification();
        return true;
    }

    events::send_event_with_metrics(
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        AgentEvent::ToolComplete {
            tool_call_id: ctx.tool_call.id.clone(),
            result: ctx.result.clone(),
        },
    )
    .await;

    if !ctx.result.success {
        ctx.state
            .mark_unsuccessful_tool(&ctx.tool_call.function.name);

        // Fire-and-forget: persist soft failure for offline analysis.
        tool_error_collector::append_tool_error(tool_error_collector::soft_failure_record(
            ctx.session_id,
            ctx.round,
            &ctx.tool_call.function.name,
            &ctx.tool_call.id,
            &ctx.tool_call.function.arguments,
            &ctx.result.result,
        ))
        .await;
    }

    tracing::debug!(
        "[{}] tool_complete: {}",
        ctx.session_id,
        serde_json::json!({
            "tool_name": ctx.tool_call.function.name,
            "tool_call_id": ctx.tool_call.id,
            "duration_ms": ctx.tool_duration.as_millis(),
            "success": ctx.result.success,
        })
    );

    let outcome = handle_tool_result_with_agentic_support(
        ctx.result,
        ctx.tool_call,
        ctx.event_tx,
        ctx.session,
        ctx.tools.as_ref(),
        // Legacy ad-hoc CompositionExecutor dispatch is intentionally disabled
        // in the agent runtime. Catalog-pinned workflow_run is the single
        // production orchestration authority (#578).
        None,
    )
    .await;

    match outcome {
        ToolHandlingOutcome::AwaitingClarification => {
            ctx.state.mark_awaiting_clarification();
            true
        }
        ToolHandlingOutcome::WaitingForChildren => {
            ctx.state.mark_waiting_for_children();
            // Do NOT break — remaining tool calls in this round may spawn
            // additional children. The pipeline will suspend the root agent
            // after ALL tool calls finish, once `waiting_for_children` is set.
            false
        }
        ToolHandlingOutcome::Continue => false,
    }
}
