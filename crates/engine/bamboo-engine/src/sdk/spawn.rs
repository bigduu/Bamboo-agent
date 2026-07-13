//! Canonical child-session spawn core (anti-fork single implementation).
//!
//! [`run_child_spawn`] is the **one** place that loads a child session, reserves
//! its runner, wires the event forwarder + heartbeat + watchdog, builds the full
//! [`ExecuteRequest`], runs the child loop, and publishes the terminal child
//! completion. Both the background scheduler (`run_spawn_job`) and the ergonomic
//! [`crate::sdk::runner::ChildRunner`] delegate here so behavior — event
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
    publish_child_completion_parts, watch_child_liveness, watchdog_policy_for_session,
    SpawnContext, SpawnJob,
};

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
    // (Read, Glob, Grep, Bash, etc.) can resolve relative paths. `set_workspace`
    // returns the FINAL stored path, which may differ from the requested one
    // when workspace-root confinement (#217) relocated it — store that back
    // onto the domain field so `session.workspace` never diverges from where
    // tools actually run.
    if let Some(ref ws) = session.workspace {
        let stored = bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            std::path::PathBuf::from(ws),
        );
        session.workspace = Some(stored.to_string_lossy().to_string());
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
        ctx.sessions_cache.insert(
            job.child_session_id.clone(),
            Arc::new(parking_lot::RwLock::new(session)),
        );
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
    let Some(RunnerReservation { cancel_token, .. }) = try_reserve_runner(
        &ctx.agent_runners,
        &ctx.session_event_senders,
        &job.child_session_id,
        &child_tx,
    )
    .await
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
    let external_runner = ctx.external_child_runner.clone();
    let done = forwarder_done.clone();
    let parent_tx_for_done = parent_tx.clone();
    let parent_id_for_done = job.parent_session_id.clone();
    let child_id_for_done = job.child_session_id.clone();
    let session_event_senders = ctx.session_event_senders.clone();
    let completion_handler = ctx.completion_handler.clone();

    tokio::spawn(async move {
        // Set the child model via the single authoritative pre-execution
        // mutation point. The child session's system prompt is already in place
        // (loaded from storage), so pass `None` for `system_prompt`.
        crate::session_app::execution_prep::prepare_session_for_execution(
            &mut session,
            None,
            Some(&model),
        );

        // Sub-agents always run as actors (the in-process runtime was removed):
        // dispatch to the composite child runner. The built-in local actor
        // handles the default case; expert `externalAgents` profiles handle
        // roles pinned to other agents. `should_handle` selects the right one.
        //
        // The child's `AgentLoopConfig` is assembled by the external runner, not
        // here, and does not currently wire `bash_resume_hook`. The end-of-turn
        // bash suspend gate is therefore inert for children — graceful
        // degradation: a child that leaves a `run_in_background` shell running
        // simply completes; the shell keeps running detached and stays readable
        // via BashOutput. No strand can occur because the gate refuses to
        // suspend without the hook. (The parent-resume path re-wires the hook,
        // so a RESUMED run is covered; only the initial child run is not.)
        let result: crate::runtime::runner::Result<()> =
            if external_runner.should_handle(&session).await {
                external_runner
                    .execute_external_child(&mut session, &job, mpsc_tx, cancel_token.clone())
                    .await
            } else {
                Err(bamboo_agent_core::AgentError::LLM(format!(
                "No child runner matched session runtime metadata: agent_id={:?}, protocol={:?}",
                session.metadata.get("external.agent_id"),
                session.metadata.get("external.protocol"),
            )))
            };

        let timeout_error = timeout_reason.read().await.clone();
        // Phase 2: a child that suspended awaiting the PARENT's approval of a
        // gated tool is NOT done — publish a NON-terminal "suspended" status so
        // the parent's completion coordinator does not resume the parent
        // prematurely. The child is resumed once the human decides, then runs to
        // a real terminal completion that re-enters this path.
        //
        // Issue #84 Phase 2b: a child that suspended mid-background-Bash is
        // likewise NOT done — it must not publish a premature terminal
        // "completed" to its parent while a `run_in_background` shell is still
        // running for it.
        let suspended_non_terminal = session
            .metadata
            .get("runtime.suspend_reason")
            .map(|reason| {
                matches!(
                    reason.as_str(),
                    "awaiting_parent_approval" | "waiting_for_bash"
                )
            })
            .unwrap_or(false);
        let (status, error) = if let Some(reason) = timeout_error {
            ("timeout".to_string(), Some(reason))
        } else if suspended_non_terminal && result.is_ok() {
            ("suspended".to_string(), None)
        } else {
            match &result {
                Ok(_) => ("completed".to_string(), None),
                Err(e @ bamboo_agent_core::AgentError::Cancelled) => {
                    ("cancelled".to_string(), Some(e.to_string()))
                }
                Err(e) => ("error".to_string(), Some(e.to_string())),
            }
        };

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
        // Flip the runner registry to a terminal status (which makes session
        // summaries report `is_running: false`) ONLY AFTER the final
        // `last_run_status` is persisted above. Doing it earlier opens a window
        // where the summary reports `{is_running: false, last_run_status: null}`,
        // which the frontend's optimistic-race window reads as "still settling",
        // leaving a phantom "thinking" indicator for a few seconds after the
        // reply has already completed (notably on a session's first turn).
        finalize_runner(&agent_runners_for_status, &session_id_clone, &result).await;
        sessions_cache.insert(
            session_id_clone.clone(),
            Arc::new(parking_lot::RwLock::new(session)),
        );

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
