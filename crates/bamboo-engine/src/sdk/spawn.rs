//! Canonical child-session spawn core (anti-fork single implementation).
//!
//! [`run_child_spawn`] is the **one** place that loads a child session, reserves
//! its runner, wires the event forwarder + heartbeat + watchdog, builds the full
//! [`ExecuteRequest`], runs the child loop, and publishes the terminal child
//! completion. Both the background scheduler (`run_spawn_job`) and the ergonomic
//! [`crate::sdk::runner::ProfileRunner`] delegate here so behavior — event
//! ordering, status strings, and field set — stays identical across entry points.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentEvent, Role, SessionKind};

use crate::runtime::execution::event_forwarder::create_event_forwarder;
use crate::runtime::execution::runner_lifecycle::{
    finalize_runner, try_reserve_runner, RunnerReservation,
};
use crate::runtime::execution::session_events::get_or_create_event_sender;
use crate::runtime::execution::spawn::{
    publish_child_completion_parts, resolve_child_provider_override, watch_child_liveness,
    watchdog_policy_for_session, SpawnContext, SpawnJob,
};
use crate::runtime::ExecuteRequestBuilder;

/// Launch a single child spawn job.
///
/// This sets up the child run and **spawns the actual execution onto a
/// background task**, returning `Ok(())` once the run has been *started* — not
/// when it completes. Completion (and persistence finalize) is observed via the
/// `SubAgentCompleted` event on the parent stream, not via this return value.
/// `Err` is only returned for synchronous setup failures (e.g. child session
/// not found) before the background task is spawned.
///
/// Preserves EXACTLY the canonical behavior:
/// - `SubAgentStarted` is emitted by the *adapter* before enqueue (not here).
/// - Event forwarder + 5s heartbeat tasks, watchdog, runner reservation.
/// - Full real [`ExecuteRequest`] field set incl. split provider fields.
/// - Terminal status strings `completed | cancelled | error | skipped | timeout`.
pub async fn run_child_spawn(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    // Ensure both session event streams exist.
    let parent_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.parent_session_id).await;
    let child_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.child_session_id).await;

    // Load child session.
    let mut session = match ctx
        .agent
        .storage()
        .load_session(&job.child_session_id)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            let error = "child session not found".to_string();
            publish_child_completion_parts(
                &parent_tx,
                ctx.completion_handler.clone(),
                job.parent_session_id.clone(),
                job.child_session_id.clone(),
                "error".to_string(),
                Some(error.clone()),
            )
            .await;
            return Err(error);
        }
        Err(e) => {
            let error = format!("failed to load child session: {e}");
            publish_child_completion_parts(
                &parent_tx,
                ctx.completion_handler.clone(),
                job.parent_session_id.clone(),
                job.child_session_id.clone(),
                "error".to_string(),
                Some(error.clone()),
            )
            .await;
            return Err(error);
        }
    };

    // Register the child's workspace in the global state so tools
    // (Read, Glob, Grep, Bash, etc.) can resolve relative paths.
    if let Some(ref ws) = session.workspace {
        bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            std::path::PathBuf::from(ws),
        );
    }

    if session.kind != SessionKind::Child {
        let error = "spawn job child session is not kind=child".to_string();
        publish_child_completion_parts(
            &parent_tx,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "error".to_string(),
            Some(error.clone()),
        )
        .await;
        return Err(error);
    }

    // Ensure last message is user (otherwise nothing to do).
    let last_is_user = session
        .messages
        .last()
        .map(|m| matches!(m.role, Role::User))
        .unwrap_or(false);
    if !last_is_user {
        session.set_last_run_status("skipped");
        session.set_last_run_error("No pending message to execute");
        let _ = ctx
            .agent
            .persistence()
            .save_runtime_session(&mut session)
            .await;
        {
            let mut sessions = ctx.sessions_cache.write().await;
            sessions.insert(job.child_session_id.clone(), session);
        }
        publish_child_completion_parts(
            &parent_tx,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "skipped".to_string(),
            Some("No pending message to execute".to_string()),
        )
        .await;
        return Ok(());
    }

    // Persist a running marker early so list_sessions can reconstruct status.
    session.set_last_run_status("running");
    session.clear_last_run_error();
    let _ = ctx
        .agent
        .persistence()
        .save_runtime_session(&mut session)
        .await;

    // Insert runner status.
    let Some(RunnerReservation { cancel_token, .. }) =
        try_reserve_runner(&ctx.agent_runners, &job.child_session_id, &child_tx).await
    else {
        return Ok(());
    };

    // Forward ALL child events to parent.
    let forwarder_done = CancellationToken::new();
    {
        let mut rx = child_tx.subscribe();
        let parent_tx = parent_tx.clone();
        let job_clone = job.clone();
        let done = forwarder_done.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = done.cancelled() => break,
                    evt = rx.recv() => {
                        match evt {
                            Ok(event) => {
                                let _ = parent_tx.send(AgentEvent::SubAgentEvent {
                                    parent_session_id: job_clone.parent_session_id.clone(),
                                    child_session_id: job_clone.child_session_id.clone(),
                                    event: Box::new(event),
                                });
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
    }
    {
        let parent_tx = parent_tx.clone();
        let job_clone = job.clone();
        let done = forwarder_done.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = done.cancelled() => break,
                    _ = ticker.tick() => {
                        let _ = parent_tx.send(AgentEvent::SubAgentHeartbeat {
                            parent_session_id: job_clone.parent_session_id.clone(),
                            child_session_id: job_clone.child_session_id.clone(),
                            timestamp: Utc::now(),
                        });
                    }
                }
            }
        });
    }

    // Create mpsc channel for agent loop → session events sender.
    let (mpsc_tx, _forwarder_handle) = create_event_forwarder(
        job.child_session_id.clone(),
        child_tx.clone(),
        ctx.agent_runners.clone(),
        ctx.account_feed_inbox.clone(),
    );

    // Child liveness is owned by the child runner. The parent wait state can
    // have a longer lease, but it should not poll or terminate children.
    let timeout_reason = Arc::new(RwLock::new(None::<String>));
    let watchdog_policy = watchdog_policy_for_session(&session);
    tokio::spawn(watch_child_liveness(
        job.parent_session_id.clone(),
        job.child_session_id.clone(),
        ctx.agent_runners.clone(),
        cancel_token.clone(),
        timeout_reason.clone(),
        forwarder_done.clone(),
        watchdog_policy,
    ));

    // Run child loop via unified spawn_session_execution.
    let model = job.model.clone();
    let session_id_clone = job.child_session_id.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let sessions_cache = ctx.sessions_cache.clone();
    let agent = ctx.agent.clone();
    let tools = ctx.tools.clone();
    let external_runner = ctx.external_child_runner.clone();
    let done = forwarder_done.clone();
    let parent_tx_for_done = parent_tx.clone();
    let parent_id_for_done = job.parent_session_id.clone();
    let child_id_for_done = job.child_session_id.clone();
    let session_event_senders = ctx.session_event_senders.clone();
    let provider_router = ctx.provider_router.clone();
    let completion_handler = ctx.completion_handler.clone();
    let app_data_dir = ctx.app_data_dir.clone();

    tokio::spawn(async move {
        // Set the child model via the single authoritative pre-execution
        // mutation point. The child session's system prompt is already in place
        // (loaded from storage), so pass `None` for `system_prompt`.
        crate::session_app::execution_prep::prepare_session_for_execution(
            &mut session,
            None,
            Some(&model),
        );

        let wants_external = session
            .metadata
            .get("runtime.kind")
            .is_some_and(|v| v == "external");

        let result: crate::runtime::runner::Result<()> = if wants_external {
            if let Some(runner) = external_runner {
                if runner.should_handle(&session).await {
                    runner
                        .execute_external_child(&mut session, &job, mpsc_tx, cancel_token.clone())
                        .await
                } else {
                    Err(bamboo_agent_core::AgentError::LLM(format!(
                        "No external runner matched child session runtime metadata: agent_id={:?}, protocol={:?}",
                        session.metadata.get("external.agent_id"),
                        session.metadata.get("external.protocol"),
                    )))
                }
            } else {
                Err(bamboo_agent_core::AgentError::LLM(
                    "Child session requires external runtime, but no external runner is configured"
                        .to_string(),
                ))
            }
        } else {
            let (provider_override, provider_name, provider_type) =
                resolve_child_provider_override(provider_router.as_ref(), &session, &model);
            let disabled_tools: Option<std::collections::BTreeSet<String>> =
                job.disabled_tools.map(|v| v.into_iter().collect());

            // Build via the canonical `ExecuteRequestBuilder` so the child spawn
            // doesn't duplicate the full `ExecuteRequest` field list. The child
            // only sets the primary model/provider and tools; every other field
            // (auxiliary roles, reasoning effort, skill selection, image
            // fallback, gold config) stays at the builder's `None` default —
            // identical to the previous explicit literal.
            // `initial_message` is empty: the child agent loop sources it itself.
            let mut builder =
                ExecuteRequestBuilder::new(String::new(), mpsc_tx, cancel_token.clone())
                    .tools(tools)
                    .model_roster(crate::runtime::ModelRoster {
                        model: Some(model.clone()),
                        provider_name,
                        provider_type,
                        fast: None,
                        background: None,
                        summarization: None,
                    });
            if let Some(provider_override) = provider_override {
                builder = builder.provider_override(provider_override);
            }
            if let Some(disabled_tools) = disabled_tools {
                builder = builder.disabled_tools(disabled_tools);
            }
            if let Some(app_data_dir) = app_data_dir {
                builder = builder.app_data_dir(app_data_dir);
            }

            agent.execute(&mut session, builder.build()).await
        };

        let timeout_error = timeout_reason.read().await.clone();
        let (status, error) = if let Some(reason) = timeout_error {
            ("timeout".to_string(), Some(reason))
        } else {
            match &result {
                Ok(_) => ("completed".to_string(), None),
                Err(e @ bamboo_agent_core::AgentError::Cancelled) => {
                    ("cancelled".to_string(), Some(e.to_string()))
                }
                Err(e) => ("error".to_string(), Some(e.to_string())),
            }
        };

        finalize_runner(&agent_runners_for_status, &session_id_clone, &result).await;

        // Merge any queued injected messages that the pipeline didn't pick up
        // (e.g. if the loop exited before the next turn boundary).
        crate::runtime::runner::state_bridge::merge_pending_injected_messages(
            &mut session,
            Some(agent.storage()),
            Some(agent.persistence()),
        )
        .await;

        // Persist final session snapshot.
        session.set_last_run_status(status.clone());
        if let Some(err) = &error {
            session.set_last_run_error(err.clone());
        } else {
            session.clear_last_run_error();
        }
        let _ = agent.persistence().save_runtime_session(&mut session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }

        // Stop forwarding/heartbeats and emit terminal child status through the
        // same durable completion path used by success/error/cancel/timeout.
        done.cancel();
        publish_child_completion_parts(
            &parent_tx_for_done,
            completion_handler,
            parent_id_for_done,
            child_id_for_done,
            status,
            error,
        )
        .await;

        // Allow dead code: session_event_senders keeps the sender alive during the task.
        drop(session_event_senders);
    });

    Ok(())
}
