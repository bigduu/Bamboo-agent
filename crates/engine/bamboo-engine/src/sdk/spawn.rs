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
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentEvent, Role, SessionKind};
use bamboo_domain::{
    AgentRuntimeState, AgentStatusState, SuspensionState, WaitingForBashState,
    WaitingForChildrenState,
};

use crate::runtime::execution::event_forwarder::create_event_forwarder;
use crate::runtime::execution::runner_lifecycle::{finalize_runner, try_reserve_runner};
use crate::runtime::execution::session_events::get_or_create_event_sender;
use crate::runtime::execution::spawn::{
    publish_child_completion_parts, watch_child_liveness, watchdog_policy_for_session,
    SpawnContext, SpawnJob,
};
use crate::runtime::execution::SessionExecutionReservation;

type HostWaitSnapshot = (Option<WaitingForChildrenState>, Option<WaitingForBashState>);

fn host_wait_snapshot(session: &bamboo_agent_core::Session) -> HostWaitSnapshot {
    crate::runtime::runner::state_bridge::read_runtime_state(session)
        .map(|state| (state.waiting_for_children, state.waiting_for_bash))
        .unwrap_or((None, None))
}

/// Reconcile actor execution with the host-owned durable wait control plane.
///
/// `Some` means the durable read succeeded and is authoritative, including
/// `(None, None)` (a completion cleared the wait). `None` means the read was
/// unavailable; preserve the prepared in-memory wait and fail closed by
/// re-suspending rather than publishing a false terminal completion.
fn reconcile_actor_host_wait(
    session: &mut bamboo_agent_core::Session,
    durable: Option<HostWaitSnapshot>,
    run_id: &str,
) {
    if session
        .metadata
        .get("runtime.suspend_reason")
        .is_some_and(|reason| !reason.trim().is_empty())
    {
        return;
    }

    let durable_available = durable.is_some();
    let mut runtime_state = crate::runtime::runner::state_bridge::read_runtime_state(session)
        .unwrap_or_else(|| AgentRuntimeState::new(run_id));
    if let Some((waiting_for_children, waiting_for_bash)) = durable {
        runtime_state.waiting_for_children = waiting_for_children;
        runtime_state.waiting_for_bash = waiting_for_bash;
    }
    let reason = if runtime_state.waiting_for_children.is_some() {
        Some(("waiting_for_children", "ChildCompletion"))
    } else if runtime_state.waiting_for_bash.is_some() {
        Some(("waiting_for_bash", "BashCompletion"))
    } else {
        None
    };
    if let Some((reason, hook_point)) = reason {
        runtime_state.status = AgentStatusState::Suspended;
        runtime_state.suspension = Some(SuspensionState {
            reason: reason.to_string(),
            suspended_at: Utc::now(),
            resumable: true,
            hook_point: Some(hook_point.to_string()),
        });
        session
            .metadata
            .insert("runtime.suspend_reason".to_string(), reason.to_string());
    } else if durable_available
        && runtime_state.suspension.as_ref().is_some_and(|suspension| {
            matches!(
                suspension.reason.as_str(),
                "waiting_for_children" | "waiting_for_bash"
            )
        })
    {
        // A successful durable read that cleared the wait wins over a stale
        // prepared snapshot.
        runtime_state.suspension = None;
        if runtime_state.status == AgentStatusState::Suspended {
            runtime_state.status = AgentStatusState::Idle;
        }
    }
    crate::runtime::runner::state_bridge::write_runtime_state(session, &runtime_state);
    // Keep the rolling-upgrade/raw-reader mirror coherent with the typed
    // control plane. A stale legacy Idle must never mask typed Suspended.
    if let Ok(serialized) = serde_json::to_string(&runtime_state) {
        session
            .metadata
            .insert("agent.runtime.state".to_string(), serialized);
    }
}

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
/// - Launch wrappers own announcements: [`crate::execution::SpawnScheduler`]
///   publishes before queued/reserved jobs become runnable, while
///   [`crate::sdk::runner::ChildRunner`] announces direct SDK launches.
/// - Event forwarder + 5s heartbeat tasks, watchdog, runner reservation.
/// - Full real [`ExecuteRequest`] field set incl. split provider fields.
/// - Terminal status strings `completed | cancelled | error | skipped | timeout`.
pub async fn run_child_spawn(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    run_child_spawn_inner(ctx, job, None).await
}

/// Canonical child activation using a runner slot already reserved by the
/// logical-session activation router.
pub(crate) async fn run_child_spawn_reserved(
    ctx: SpawnContext,
    job: SpawnJob,
    reservation: SessionExecutionReservation,
) -> Result<(), String> {
    run_child_spawn_inner(ctx, job, Some(reservation)).await
}

async fn run_child_spawn_inner(
    ctx: SpawnContext,
    job: SpawnJob,
    reserved: Option<SessionExecutionReservation>,
) -> Result<(), String> {
    // Ensure both session event streams exist.
    let parent_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.parent_session_id).await;
    let parent_event_publisher = ctx.replayable_event_publisher();
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
                &parent_event_publisher,
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
                &parent_event_publisher,
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

    // A reserved activation was published by the logical-session router before
    // this task became runnable. Validate that exact ownership and the durable
    // authorized prefix before mutating workspace/control state or writing a
    // false running marker.
    let reserved_identity = reserved.as_ref().map(|reservation| {
        (
            reservation.run_id().to_string(),
            reservation.matches_execution_target(
                &job.child_session_id,
                &session.id,
                &ctx.agent_runners,
            ),
        )
    });
    let reserved_activation = if let Some((reserved_run_id, exact_target)) = reserved_identity {
        let exact_runner = ctx
            .agent_runners
            .read()
            .await
            .get(&job.child_session_id)
            .is_some_and(|runner| {
                runner.run_id == reserved_run_id
                    && matches!(
                        runner.status,
                        crate::runtime::execution::AgentStatus::Running
                    )
            });
        let exact_owner = match ctx.agent.activation_router() {
            Some(router) => {
                router
                    .owns_run(&job.child_session_id, &reserved_run_id)
                    .await
            }
            None => false,
        };
        let authorized_prefix = match ctx.agent.session_inbox() {
            Some(inbox) => inbox
                .inspect(&job.child_session_id)
                .await
                .map_err(|error| {
                    format!(
                        "inspect reserved child SessionInbox {}: {error}",
                        job.child_session_id
                    )
                })?
                .activation_pending(),
            None => false,
        };
        if !exact_target || !exact_runner || !exact_owner || !authorized_prefix {
            return Err(format!(
                "reserved child activation {} failed ownership/prefix validation for {}",
                reserved_run_id, job.child_session_id
            ));
        }
        true
    } else {
        false
    };

    if session.kind != SessionKind::Child {
        let error = "spawn job child session is not kind=child".to_string();
        // Persist the terminal status so the session index never reports this
        // child as pending/running — a wait registered over it later (or the
        // orphan safety net) would otherwise hang on a status that no future
        // completion can ever advance.
        if !reserved_activation {
            session.set_last_run_status("error");
            session.set_last_run_error(error.clone());
            let _ = ctx
                .agent
                .persistence()
                .save_runtime_session(&mut session)
                .await;
        }
        publish_child_completion_parts(
            &parent_event_publisher,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "error".to_string(),
            Some(error.clone()),
        )
        .await;
        return Err(error);
    }

    // Clear suspend leftovers from a PRIOR run (issue #546): a child that
    // suspended (clarification/approval), was answered through the parent via
    // `send_message`/`run`, and is now re-enqueued still carries the old
    // `runtime.suspend_reason` + pending question — none of the re-run entry
    // points perform respond.rs's clear. The terminal mapping below must only
    // observe suspend state stamped by THIS run, otherwise a genuinely
    // completed re-run is published as non-terminal "suspended" and the
    // parent stalls until the wait lease expires.
    session.metadata.remove("runtime.suspend_reason");
    session.clear_pending_question();

    // Ensure last message is user (otherwise nothing to do).
    let last_is_user = session
        .messages
        .last()
        .map(|m| matches!(m.role, Role::User))
        .unwrap_or(false);
    if !last_is_user && !reserved_activation {
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
            &parent_event_publisher,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "skipped".to_string(),
            Some("No pending message to execute".to_string()),
        )
        .await;
        return Ok(());
    }

    // Own runner and router registration as one RAII handoff before workspace
    // publication, durable running markers, launch hooks, or actor execution.
    let mut execution_reservation = if let Some(reservation) = reserved {
        reservation
    } else {
        let Some(reservation) = try_reserve_runner(
            &ctx.agent_runners,
            &ctx.session_event_senders,
            &job.child_session_id,
            &child_tx,
        )
        .await
        else {
            // A Running runner already exists for this child (duplicate enqueue,
            // or a stale entry left by a dead task). Publish nothing: if the
            // earlier run is alive its own completion will fire; if it is a
            // stale entry the child-wait watchdog will detect the dead child and
            // synthesize an error completion.
            tracing::warn!(
                parent_session_id = %job.parent_session_id,
                child_session_id = %job.child_session_id,
                "child spawn skipped: runner already registered for this child"
            );
            return Ok(());
        };
        SessionExecutionReservation::from_pending_registration(
            job.child_session_id.clone(),
            reservation,
            ctx.agent.activation_router().cloned(),
            ctx.agent_runners.clone(),
        )
    };
    let run_id = execution_reservation.run_id().to_string();
    if let Err(error) = execution_reservation.ensure_registered().await {
        let same_run_duplicate = error.existing_run_id() == run_id;
        tracing::warn!(
            parent_session_id = %job.parent_session_id,
            child_session_id = %job.child_session_id,
            run_id = %run_id,
            %error,
            "child runner registration collided with an existing logical-session owner"
        );
        if same_run_duplicate {
            return Ok(());
        }
        let setup_error = error.to_string();
        publish_child_completion_parts(
            &parent_event_publisher,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "error".to_string(),
            Some(setup_error.clone()),
        )
        .await;
        return Err(setup_error);
    }
    let (cancel_token, mut activation_registration) = execution_reservation.disarm_for_execution();

    // Publish the child's process-global workspace only after this task owns
    // both the exact runner slot and logical-session router. A rejected
    // cross-entry caller must not overwrite a live owner's tool root or
    // materialize a directory it was never authorized to use.
    if let Some(ref ws) = session.workspace {
        let stored = bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            std::path::PathBuf::from(ws),
        );
        session.workspace = Some(stored.to_string_lossy().to_string());
    }

    // Persist a running marker only after this task owns both the runner slot
    // and the logical-session router. A distinct cross-entry collision is a
    // synchronous setup failure and must never leave a false durable `running`
    // state behind.
    session.set_last_run_status("running");
    session.clear_last_run_error();
    let _ = ctx
        .agent
        .persistence()
        .save_runtime_session(&mut session)
        .await;

    // Parent projection is intentionally lifecycle-only. The child's complete
    // event stream already lives on `agent.{child_session_id}` and can be
    // opened independently by the frontend. Recursively wrapping every token
    // in `SubAgentEvent` multiplies one busy child stream into every ancestor
    // channel (O(depth × tokens)) and makes 200-way fan-out untenable.
    //
    // Keep only the low-rate Started/Heartbeat/Completed projection on the
    // parent. `SubAgentStarted` is published by the spawn admission path and
    // `SubAgentCompleted` by the durable completion path below.
    let forwarder_done = CancellationToken::new();
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
        run_id.clone(),
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
    let parent_event_publisher_for_done = parent_event_publisher.clone();
    let parent_id_for_done = job.parent_session_id.clone();
    let child_id_for_done = job.child_session_id.clone();
    let session_event_senders = ctx.session_event_senders.clone();
    let completion_handler = ctx.completion_handler.clone();
    let activation_run_id = run_id;

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
        // Panic containment: this task's terminal block below is the ONLY thing
        // that publishes the child's completion — if execution panics and the
        // task unwinds, the parent waits forever on a wake that can never fire.
        // Catch the unwind and map it to a terminal error instead. The session
        // may have been partially mutated by the panicking run; that is
        // acceptable — we persist what we have and surface the panic as the
        // child's error so the parent can decide how to proceed.
        let result: crate::runtime::runner::Result<()> =
            if external_runner.should_handle(&session).await {
                use futures::FutureExt;
                match std::panic::AssertUnwindSafe(external_runner.execute_external_child(
                    &mut session,
                    &job,
                    mpsc_tx,
                    cancel_token.clone(),
                ))
                .catch_unwind()
                .await
                {
                    Ok(result) => result,
                    Err(panic) => {
                        let message = panic
                            .downcast_ref::<&str>()
                            .map(|s| (*s).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "non-string panic payload".to_string());
                        tracing::error!(
                            parent_session_id = %job.parent_session_id,
                            child_session_id = %job.child_session_id,
                            panic = %message,
                            "child execution panicked; publishing terminal error completion"
                        );
                        Err(bamboo_agent_core::AgentError::LLM(format!(
                            "child execution panicked: {message}"
                        )))
                    }
                }
            } else {
                Err(bamboo_agent_core::AgentError::LLM(format!(
                "No child runner matched session runtime metadata: agent_id={:?}, protocol={:?}",
                session.metadata.get("external.agent_id"),
                session.metadata.get("external.protocol"),
            )))
            };

        // Actor execution owns provider I/O, but the host remains authoritative
        // for child/Bash wait ownership. Explicit SessionInbox steering may
        // interrupt one reasoning gate without clearing that durable wait.
        // Reconcile the newest host control-plane state before terminal mapping
        // so a still-armed wait re-suspends, while a concurrent completion that
        // cleared it wins over this task's stale snapshot.
        let durable_wait = match agent.storage().load_session(&session_id_clone).await {
            Ok(Some(persisted)) => Some(host_wait_snapshot(&persisted)),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    session_id = %session_id_clone,
                    %error,
                    "could not reconcile host-persisted wait ownership after actor execution; preserving in-memory wait"
                );
                None
            }
        };
        reconcile_actor_host_wait(&mut session, durable_wait, &activation_run_id);

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
        //
        // Issue #546: the same holds for EVERY suspend reason — a child-parent
        // suspending on its own grandchildren (waiting_for_children) or on a
        // human clarification is not done either. Any non-empty suspend reason
        // maps to the non-terminal "suspended" status (mirroring the top-level
        // mapping in `agent_spawn`), which the completion coordinator's
        // terminality guard leaves un-counted; the real terminal completion is
        // published when the child later resumes and finishes.
        let suspended_non_terminal = result.is_ok()
            && session
                .metadata
                .get("runtime.suspend_reason")
                .is_some_and(|reason| !reason.trim().is_empty());
        let (status, error) = if let Some(reason) = timeout_error {
            ("timeout".to_string(), Some(reason))
        } else if suspended_non_terminal {
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

        // Preserve the cursor reached by an actual execution safe boundary.
        // Terminal cleanup never claims/acks typed work; any newer durable
        // generation therefore remains pending for a successor reasoning turn.
        let executed_admitted_generation = session
            .session_inbox_admission()
            .map_or(0, |state| state.last_admitted_sequence);

        // Rolling-upgrade producers may have written the legacy queue after the
        // final provider boundary. Move those values into the durable typed
        // inbox, but deliberately do not claim/ack them in terminal cleanup:
        // they have not been presented to reasoning and require a successor.
        let legacy_migration = crate::runtime::runner::state_bridge::migrate_legacy_pending_only(
            &mut session,
            Some(agent.storage()),
            Some(agent.persistence()),
            agent.session_inbox(),
        )
        .await;
        let pending_boundary_generation = session
            .session_inbox_admission()
            .and_then(|state| state.pending_activation_generation());
        let pending_generation = match (
            pending_boundary_generation,
            legacy_migration.highest_generation,
        ) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        if let (Some(generation), Some(router)) = (pending_generation, agent.activation_router()) {
            let activation_ready = if let Some(inbox) = agent.session_inbox() {
                match inbox
                    .mark_activation_eligible(
                        &session_id_clone,
                        generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(
                            session_id = %session_id_clone,
                            %error,
                            "failed to persist unadmitted SessionInbox activation watermark"
                        );
                        false
                    }
                }
            } else {
                false
            };
            if activation_ready {
                if let Err(error) = bamboo_domain::SessionActivationPort::request_activation(
                    router.as_ref(),
                    &session_id_clone,
                    generation,
                )
                .await
                {
                    tracing::error!(
                        session_id = %session_id_clone,
                        %error,
                        "failed to hand unadmitted SessionInbox generation to activation router"
                    );
                }
            }
        }

        // Persist final session snapshot.
        session.set_last_run_status(status.clone());
        if let Some(err) = &error {
            session.set_last_run_error(err.clone());
        } else {
            session.clear_last_run_error();
        }
        if let Some(registration) = activation_registration.as_mut() {
            registration.begin_finalization().await;
        } else if let Some(router) = agent.activation_router() {
            router
                .begin_finalization(&session_id_clone, &activation_run_id)
                .await;
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
        let finalization = if let Some(registration) = activation_registration.take() {
            registration.finish(executed_admitted_generation).await
        } else if let Some(router) = agent.activation_router() {
            router
                .finish_finalization(
                    &session_id_clone,
                    &activation_run_id,
                    executed_admitted_generation,
                )
                .await
        } else {
            Ok(None)
        };
        if let Err(error) = finalization {
            tracing::error!(
                session_id = %session_id_clone,
                %error,
                "failed to activate child successor for finalization-racing SessionInbox delivery"
            );
        }
        sessions_cache.insert(
            session_id_clone.clone(),
            Arc::new(parking_lot::RwLock::new(session)),
        );

        // Stop forwarding/heartbeats and emit terminal child status through the
        // same durable completion path used by success/error/cancel/timeout.
        done.cancel();
        publish_child_completion_parts(
            &parent_event_publisher_for_done,
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

#[cfg(test)]
mod actor_host_wait_tests {
    use super::*;
    use bamboo_domain::ChildWaitPolicy;

    fn prepared_waiting_session() -> bamboo_agent_core::Session {
        let mut session =
            bamboo_agent_core::Session::new_child("child", "parent", "model", "Child");
        let mut runtime = AgentRuntimeState::new("prepared-run");
        runtime.status = AgentStatusState::Idle;
        runtime.waiting_for_children = Some(WaitingForChildrenState::for_children(
            vec!["grandchild".to_string()],
            ChildWaitPolicy::All,
            Utc::now(),
        ));
        session.agent_runtime_state = Some(runtime);
        session
    }

    #[test]
    fn actor_host_read_failure_preserves_wait_and_non_terminal_status() {
        let mut session = prepared_waiting_session();
        reconcile_actor_host_wait(&mut session, None, "actor-run");

        let runtime = session.agent_runtime_state.as_ref().unwrap();
        assert!(runtime.waiting_for_children.is_some());
        assert_eq!(runtime.status, AgentStatusState::Suspended);
        assert_eq!(
            session
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_children")
        );
        let legacy: AgentRuntimeState =
            serde_json::from_str(session.metadata.get("agent.runtime.state").unwrap()).unwrap();
        assert_eq!(legacy.status, AgentStatusState::Suspended);
        assert!(legacy.waiting_for_children.is_some());
        let suspended_non_terminal = session
            .metadata
            .get("runtime.suspend_reason")
            .is_some_and(|reason| !reason.trim().is_empty());
        assert!(suspended_non_terminal);
    }

    #[test]
    fn actor_host_successful_durable_clear_wins_over_prepared_wait() {
        let mut session = prepared_waiting_session();
        session.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Suspended;
        session.agent_runtime_state.as_mut().unwrap().suspension = Some(SuspensionState {
            reason: "waiting_for_children".to_string(),
            suspended_at: Utc::now(),
            resumable: true,
            hook_point: Some("ChildCompletion".to_string()),
        });

        reconcile_actor_host_wait(&mut session, Some((None, None)), "actor-run");

        let runtime = session.agent_runtime_state.as_ref().unwrap();
        assert!(runtime.waiting_for_children.is_none());
        assert!(runtime.waiting_for_bash.is_none());
        assert_eq!(runtime.status, AgentStatusState::Idle);
        assert!(runtime.suspension.is_none());
        assert!(!session.metadata.contains_key("runtime.suspend_reason"));
        let legacy: AgentRuntimeState =
            serde_json::from_str(session.metadata.get("agent.runtime.state").unwrap()).unwrap();
        assert_eq!(legacy, *runtime);
    }

    #[test]
    fn actor_host_latest_legacy_only_wait_is_authoritative() {
        let mut persisted =
            bamboo_agent_core::Session::new_child("child", "parent", "model", "Child");
        let mut legacy_runtime = AgentRuntimeState::new("legacy-run");
        legacy_runtime.waiting_for_bash = Some(WaitingForBashState::for_bash(
            vec!["shell-1".to_string()],
            Utc::now(),
        ));
        persisted.metadata.insert(
            "agent.runtime.state".to_string(),
            serde_json::to_string(&legacy_runtime).unwrap(),
        );
        assert!(persisted.agent_runtime_state.is_none());

        let mut live = bamboo_agent_core::Session::new_child("child", "parent", "model", "Child");
        reconcile_actor_host_wait(&mut live, Some(host_wait_snapshot(&persisted)), "actor-run");

        let runtime = live.agent_runtime_state.as_ref().unwrap();
        assert!(runtime.waiting_for_bash.is_some());
        assert_eq!(runtime.status, AgentStatusState::Suspended);
        assert_eq!(
            live.metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_bash")
        );
    }
}
