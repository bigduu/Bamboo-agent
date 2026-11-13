use std::{sync::Arc, time::Duration};

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{
    latest_response_occurrence, ResponseOccurrence, SessionCallerReference,
    ONCE_RESPONSE_HANDOFF_KEY,
};
use bamboo_engine::session_app::{
    execute::has_pending_clarification_resume,
    repository::SessionAccess,
    respond::{
        PlanModeTransition, PERMISSION_REEXECUTE_GENERATION_METADATA_KEY,
        PERMISSION_REEXECUTE_METADATA_KEY,
    },
    resume::{reserve_response_resume_handoff, ResponseResumeHandoff},
    types::ResumeOutcome,
};
use bamboo_tools::permission::{PermissionDecisionKind, PermissionDecisionReceipt};

use super::super::types::RespondRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseEndpoint {
    Legacy,
    TypedPermission,
}

fn response_expected_tool_call_id(
    session: &bamboo_agent_core::Session,
    pending: &bamboo_agent_core::PendingQuestion,
    requested: Option<&str>,
) -> Option<String> {
    requested.map(str::to_owned).or_else(|| {
        bamboo_engine::session_app::no_progress::is_no_progress_question(session, pending)
            .then(|| pending.tool_call_id.clone())
    })
}

async fn publish_progress_stop(
    state: web::Data<AppState>,
    session_id: String,
    message: bamboo_agent_core::Message,
    handoff: bamboo_engine::session_app::resume::ResponseResumeHandoff,
) -> bool {
    let (reservation, event_sender) = handoff.into_parts();
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(4);
    let mut history_commit =
        crate::handlers::agent::execute::runtime::spawn_event_forwarder_with_root_actor(
            state,
            session_id.clone(),
            reservation.run_id().to_string(),
            event_rx,
            event_sender,
            None,
            reservation.root_actor_writer(),
        );
    let published = event_tx
        .send(AgentEvent::message_appended(&session_id, &message))
        .await
        .is_ok()
        && event_tx
            .send(AgentEvent::Cancelled {
                message: Some("Stopped by the user.".into()),
            })
            .await
            .is_ok()
        && history_commit.send_and_wait(&event_tx, session_id).await;
    drop(event_tx);
    reservation.abandon().await;
    published
}

/// Submit a user response to a pending question from the `conclusion_with_options` tool.
///
/// When the agent calls the `conclusion_with_options` tool, it pauses execution and waits
/// for user input. This endpoint submits the user's response, allowing
/// the agent to resume execution.
///
/// # HTTP Method
///
/// `POST /api/v1/sessions/{session_id}/respond`
pub async fn submit_response(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    req: web::Json<RespondRequest>,
) -> Result<HttpResponse> {
    submit_response_inner(state, session_id, req, ResponseEndpoint::Legacy, None).await
}

struct TypedPermissionResponse<'a> {
    receipt: PermissionDecisionReceipt,
    guard: &'a bamboo_engine::session_app::respond::PendingResponseGuard,
    approval: Option<bamboo_domain::storage::SupervisorApprovalCommit>,
}

pub(super) async fn submit_typed_permission_response(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    req: web::Json<RespondRequest>,
    permission_receipt: PermissionDecisionReceipt,
    response_guard: &bamboo_engine::session_app::respond::PendingResponseGuard,
    approval: Option<bamboo_domain::storage::SupervisorApprovalCommit>,
) -> Result<HttpResponse> {
    submit_response_inner(
        state,
        session_id,
        req,
        ResponseEndpoint::TypedPermission,
        Some(TypedPermissionResponse {
            receipt: permission_receipt,
            guard: response_guard,
            approval,
        }),
    )
    .await
}

async fn submit_response_inner(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    req: web::Json<RespondRequest>,
    response_endpoint: ResponseEndpoint,
    mut permission: Option<TypedPermissionResponse<'_>>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let user_response = req.response.clone();
    let typed_permission_response = permission.is_some();

    tracing::info!("[{}] Received user response: {}", session_id, user_response);

    // One guard spans authoritative preflight, successor reservation, durable
    // compare-and-consume, and detached dispatch for every response source.
    // A duplicate waits here, then observes the consumed question without
    // allocating or replacing runner state.
    let acquired_response_guard = if permission.is_none() {
        Some(bamboo_engine::session_app::respond::acquire_pending_response_guard(&session_id).await)
    } else {
        None
    };
    let response_guard = permission
        .as_ref()
        .map(|permission| permission.guard)
        .or(acquired_response_guard.as_ref())
        .expect("a response guard is supplied or acquired");

    // A warm cache must not hide a future canonical contract or translate its
    // explicit refusal into a generic storage failure. This read grants no
    // reservation; the existing response CAS still owns consumption.
    match state.storage.load_session(&session_id).await {
        Ok(Some(session)) => {
            if let Some(response) = crate::handlers::agent::sessions::SessionSummary::execution_conflict(&session) {
                return Ok(response);
            }
        }
        Ok(None) => {},
        Err(error) => return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value(format!("Failed to inspect Session compatibility: {error}")),
        }))),
    }
    // Reload using the same durable-consumption rules as the response CAS
    // before creating a long-lived sender or reserving a successor runner.
    let preflight = match bamboo_engine::session_app::respond::inspect_pending_response_guarded(
        state.as_ref(),
        &session_id,
        response_guard,
    )
    .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found")
            })));
        }
        Err(error) => {
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!(
                    "Failed to load session before response: {error}"
                ))
            })));
        }
    };
    if let Some(response) =
        crate::handlers::agent::sessions::SessionSummary::execution_conflict(&preflight)
    {
        return Ok(response);
    }
    let Some(pending) = preflight.pending_question.as_ref() else {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("No pending question waiting for response")
        })));
    };
    let stop_progress_pause =
        bamboo_engine::session_app::no_progress::is_no_progress_question(&preflight, pending)
            && user_response == bamboo_engine::session_app::no_progress::STOP_OPTION;
    // Bind runtime control to the exact preflight question even when an older
    // client omits its expected ID. A replacement before CAS must stay pending.
    let expected_tool_call_id =
        response_expected_tool_call_id(&preflight, pending, req.expected_tool_call_id.as_deref());
    if let Some(expected) = req.expected_tool_call_id.as_deref() {
        if expected != pending.tool_call_id {
            return Ok(HttpResponse::Conflict().json(serde_json::json!({
                "error": crate::error::error_value("Pending question changed"),
                "expected_tool_call_id": expected,
                "actual_tool_call_id": pending.tool_call_id,
            })));
        }
    }
    if response_endpoint == ResponseEndpoint::Legacy {
        let in_memory_request = state
            .permission_checker
            .permission_config()
            .and_then(|config| config.pending_request(&session_id, &pending.tool_call_id));
        let interaction =
            super::pending::resolve_pending_interaction(&preflight, pending, in_memory_request);
        if interaction.kind == super::pending::PendingInteractionKind::Permission {
            return Ok(HttpResponse::Conflict().json(serde_json::json!({
                "error": crate::error::error_value(
                    "Permission interactions require the typed decision endpoint"
                ),
                "interaction_kind": "permission",
                "permission_decision_endpoint": format!(
                    "/api/v1/sessions/{session_id}/permission-decisions"
                ),
            })));
        }
    }
    if response_endpoint == ResponseEndpoint::Legacy {
        if let Err(message) =
            bamboo_engine::session_app::respond::validate_pending_response(pending, &user_response)
        {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value("Invalid response"),
                "message": message,
            })));
        }
    }

    let resume_port = crate::app_state::resume_adapter::AppStateResumeRef::new(state.clone());
    let mut handoff =
        match reserve_response_resume_handoff(&resume_port, &session_id, Duration::from_secs(15))
            .await
        {
            Ok(handoff) => handoff,
            Err(_) => {
                return Ok(HttpResponse::ServiceUnavailable().json(serde_json::json!({
                    "error": crate::error::error_value(
                        "The suspending run is still finalizing; the answer was not consumed"
                    )
                })));
            }
        };

    let input = bamboo_engine::session_app::types::RespondInput {
        session_id: session_id.clone(),
        user_response: user_response.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        provider: req.provider.clone(),
        reasoning_effort: req.reasoning_effort,
    };

    // Resolve the only async resume input before the durable response CAS. This
    // intentionally gives one response a stable config snapshot; concurrent
    // config changes apply to later requests/runs. Once that CAS succeeds, the
    // exact handoff must reach its detached owner without a cancellation point.
    let config_snapshot = state.config.read().await.clone();

    let owner = handoff.root_actor_writer();
    let approval = permission
        .as_mut()
        .and_then(|permission| permission.approval.take());
    let bound_access: Result<Option<Arc<dyn SessionAccess>>, _> = if let Some(approval) = approval {
        match owner {
            Some(owner) => state
                .session_repo
                .bind_supervisor_approval_response_writer(owner, approval)
                .map(|bound| Some(Arc::new(bound) as Arc<dyn SessionAccess>))
                .map_err(|error| error.to_string()),
            None => Err("delegated permission response requires its Root writer".into()),
        }
    } else {
        SessionAccess::bind_response_writer(state.as_ref(), owner)
            .map_err(|error| error.to_string())
    };
    let bound_access = match bound_access {
        Ok(access) => access,
        Err(error) => {
            handoff.abandon().await;
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!("Response writer unavailable: {error}"))
            })));
        }
    };
    let response_access: &dyn SessionAccess = bound_access.as_deref().unwrap_or(state.as_ref());

    let submission = if let Some(permission) = permission {
        bamboo_engine::session_app::respond::submit_resumable_permission_response_checked_guarded(
            response_access,
            input,
            expected_tool_call_id,
            permission.receipt,
            response_guard,
        )
        .await
    } else {
        bamboo_engine::session_app::respond::submit_pending_response_checked_guarded(
            response_access,
            input,
            expected_tool_call_id,
            response_guard,
        )
        .await
    };
    let (session, user_response, plan_mode_transition, permission_grants) = match submission {
        Ok(result) => result,
        Err(error) => {
            handoff.abandon().await;
            return match error {
                bamboo_engine::session_app::errors::RespondError::NotFound(_) => {
                    Ok(HttpResponse::NotFound().json(serde_json::json!({
                        "error": crate::error::error_value("Session not found")
                    })))
                }
                bamboo_engine::session_app::errors::RespondError::NoPendingQuestion => {
                    Ok(HttpResponse::BadRequest().json(serde_json::json!({
                        "error": crate::error::error_value(
                            "No pending question waiting for response"
                        )
                    })))
                }
                bamboo_engine::session_app::errors::RespondError::PendingQuestionMismatch {
                    expected,
                    actual,
                } => Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": crate::error::error_value("Pending question changed"),
                    "expected_tool_call_id": expected,
                    "actual_tool_call_id": actual,
                }))),
                bamboo_engine::session_app::errors::RespondError::InvalidResponse(msg) => {
                    Ok(HttpResponse::BadRequest().json(serde_json::json!({
                        "error": crate::error::error_value("Invalid response"),
                        "message": msg,
                    })))
                }
                _ => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(format!(
                        "Response submission failed: {error}"
                    ))
                }))),
            };
        }
    };

    if stop_progress_pause {
        // The same guarded CAS consumed the runtime question and saved the
        // stopped session. A detached owner publishes through the existing
        // fenced forwarder/barrier before releasing its unused successor.
        let message = session
            .messages
            .last()
            .expect("Stop acknowledgement")
            .clone();
        if !tokio::spawn(publish_progress_stop(
            state.clone(),
            session_id.clone(),
            message,
            handoff,
        ))
        .await
        .unwrap_or(false)
        {
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value("Stop was saved but its event publication failed")
            })));
        }
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Run stopped.",
            "response": user_response,
            "auto_resume_status": "completed",
            "stopped": true,
        })));
    }

    // Record session grants for any permission prompt the user approved, so the
    // resumed run's re-attempt of the gated operation passes the checker without
    // prompting again. `state.permission_checker` shares the same PermissionConfig
    // the tool executor checks against. Typed replay uses the committed receipt
    // to restore its exact authorization. The legacy endpoint creates the
    // compatibility `(session, request_id)` grant here.
    for (perm_type, resource) in &permission_grants {
        if !typed_permission_response {
            if let Some(request_id) = session
                .metadata
                .get(bamboo_engine::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY)
            {
                state.permission_checker.grant_once(
                    &session_id,
                    request_id,
                    *perm_type,
                    resource.clone(),
                );
            }
        }
        tracing::info!(
            "[{}] Granted session permission {:?} for: {}",
            session_id,
            perm_type,
            resource
        );
    }

    if let Some(event) = plan_mode_transition
        .as_ref()
        .map(|transition| match transition {
            PlanModeTransition::Entered {
                reason,
                pre_permission_mode,
                entered_at,
                status,
                plan_file_path,
            } => AgentEvent::PlanModeEntered {
                session_id: session_id.clone(),
                reason: reason.clone(),
                pre_permission_mode: pre_permission_mode.clone(),
                entered_at: *entered_at,
                status: *status,
                plan_file_path: plan_file_path.clone(),
            },
            PlanModeTransition::Exited {
                approved,
                restored_mode,
                plan,
            } => AgentEvent::PlanModeExited {
                session_id: session_id.clone(),
                approved: *approved,
                restored_mode: restored_mode.clone(),
                plan: plan.clone(),
            },
        })
    {
        handoff.publish_event(event);
    }

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    // Build resume config snapshot from server config via the single-source-of-
    // truth resolver (provider-name derivation + global auxiliary models + gold).
    let auto_resume_outcome =
        resume_committed_response(&resume_port, &config_snapshot, session, handoff).await;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response,
        "auto_resume_status": auto_resume_outcome.status_str(),
        "run_id": auto_resume_outcome.run_id()
    })))
}

async fn resume_committed_response(
    resume_port: &crate::app_state::resume_adapter::AppStateResumeRef,
    config: &bamboo_config::Config,
    session: bamboo_agent_core::Session,
    handoff: ResponseResumeHandoff,
) -> ResumeOutcome {
    let session_id = session.id.clone();
    let resume_config =
        bamboo_engine::session_app::resolution::resolve_resume_config_snapshot_for_execution(
            config,
            &resume_port.0.provider_registry,
            &session,
            None,
        );
    bamboo_engine::session_app::resume::resume_session_execution_with_handoff_and_configured_fallback(
        resume_port,
        &session_id,
        session,
        resume_config,
        handoff,
    )
    .await
}

fn needs_once_response_handoff(
    session: &bamboo_agent_core::Session,
    receipt: &PermissionDecisionReceipt,
) -> bool {
    let decision = &receipt.decision;
    let marker = session
        .metadata
        .get(ONCE_RESPONSE_HANDOFF_KEY)
        .and_then(|raw| serde_json::from_str::<ResponseOccurrence>(raw).ok());
    let Some(occurrence) = marker else {
        return false;
    };
    let replay_call = session.metadata.get(PERMISSION_REEXECUTE_METADATA_KEY);
    let replay_generation = session
        .metadata
        .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
    let replay_matches = match decision.decision {
        PermissionDecisionKind::AllowOnce => {
            replay_call == Some(&decision.request_id)
                && replay_generation == Some(&decision.request_generation)
        }
        PermissionDecisionKind::DenyOnce => replay_call.is_none() && replay_generation.is_none(),
        _ => false,
    };
    session.pending_question.is_none()
        && has_pending_clarification_resume(session)
        && replay_matches
        && occurrence.permission_generation.as_deref() == Some(decision.request_generation.as_str())
        && latest_response_occurrence(session, &decision.request_id).as_ref() == Some(&occurrence)
        && session
            .messages
            .last()
            .is_some_and(|message| message.id == occurrence.tool_result_message_id)
        && super::pending::persisted_permission_decision_receipt(session, &decision.request_id)
            .as_ref()
            == Some(receipt)
}

/// Finish an undispatched durable Once answer. The caller owns the response
/// guard; the exact runner reservation and a fresh canonical read fence resume.
pub(super) async fn recover_committed_once_response(
    state: &web::Data<AppState>,
    session: &bamboo_agent_core::Session,
    receipt: &PermissionDecisionReceipt,
    approval: Option<Arc<bamboo_domain::storage::SupervisorApprovalCommit>>,
) -> std::io::Result<Option<ResumeOutcome>> {
    if !needs_once_response_handoff(session, receipt) {
        return Ok(None);
    }
    let session_id = &session.id;
    let expected = SessionCallerReference::from(session);
    let resume_port = crate::app_state::resume_adapter::AppStateResumeRef::new(state.clone());
    let handoff =
        match reserve_response_resume_handoff(&resume_port, session_id, Duration::ZERO).await {
            Ok(handoff) => handoff,
            Err(outcome) => return Ok(Some(outcome)),
        };
    let current = match handoff.root_actor_writer() {
        Some(owner) => state
            .storage
            .confirm_root_response_checkpoint(&owner, approval.as_deref())
            .await
            .map(Some),
        None if approval.is_some() => Err(std::io::Error::other(
            "delegated permission recovery requires its Root writer",
        )),
        None => {
            state
                .storage
                .load_execution_admission_snapshot(session_id)
                .await
        }
    };
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            handoff.abandon().await;
            return Err(error);
        }
    };
    let Some(current) = current.filter(|current| {
        expected.matches_session(current)
            && current.check_execution_compatibility().is_ok()
            && needs_once_response_handoff(current, receipt)
    }) else {
        handoff.abandon().await;
        return Ok(None);
    };
    let config = state.config.read().await.clone();
    Ok(Some(
        resume_committed_response(
            &resume_port.with_approval(approval),
            &config,
            current,
            handoff,
        )
        .await,
    ))
}

#[cfg(test)]
mod no_progress_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{Message, PendingQuestionSource, Session};
    use bamboo_engine::execution::{AgentRunner, AgentStatus};
    use bamboo_tools::permission::PermissionType;

    #[actix_web::test]
    async fn nonexistent_response_ids_do_not_allocate_runner_or_sender_state() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let baseline_runners = state.agent_runners.read().await.len();
        let baseline_senders = state.session_event_senders.read().await.len();

        for index in 0..16 {
            let response = submit_response(
                state.clone(),
                web::Path::from(format!("missing-{index}")),
                web::Json(RespondRequest {
                    response: "Approve".to_string(),
                    expected_tool_call_id: Some("random-tool".to_string()),
                    model: None,
                    provider: None,
                    model_ref: None,
                    reasoning_effort: None,
                }),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), actix_web::http::StatusCode::NOT_FOUND);
        }

        assert_eq!(state.agent_runners.read().await.len(), baseline_runners);
        assert_eq!(
            state.session_event_senders.read().await.len(),
            baseline_senders
        );
    }

    #[actix_web::test]
    async fn queued_duplicate_cannot_replace_a_fast_completed_successor() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "duplicate-after-fast-successor";
        let tool_call_id = "call-fast-successor";
        let mut session = Session::new(session_id, "test-model");
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "conclusion_with_options".to_string(),
            "Continue?".to_string(),
            vec!["Yes".to_string(), "No".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;

        // Model the winning response while retaining the shared outer gate.
        // Its successor finishes before the queued duplicate is allowed to
        // revalidate, which is the race that previously installed a phantom
        // reservation and finalized it as Cancelled.
        let guard =
            bamboo_engine::session_app::respond::acquire_pending_response_guard(session_id).await;
        let baseline_senders = state.session_event_senders.read().await.len();
        let duplicate_state = state.clone();
        let duplicate = tokio::spawn(async move {
            submit_response(
                duplicate_state,
                web::Path::from(session_id.to_string()),
                web::Json(RespondRequest {
                    response: "Yes".to_string(),
                    expected_tool_call_id: Some(tool_call_id.to_string()),
                    model: None,
                    provider: None,
                    model_ref: None,
                    reasoning_effort: None,
                }),
            )
            .await
            .expect("duplicate response handler")
            .status()
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while bamboo_engine::session_app::respond::pending_response_waiter_count(session_id)
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("duplicate must reach the shared response gate");

        let input = bamboo_engine::session_app::types::RespondInput {
            session_id: session_id.to_string(),
            user_response: "Yes".to_string(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        let (mut completed, ..) =
            bamboo_engine::session_app::respond::submit_pending_response_checked_guarded(
                state.as_ref(),
                input,
                Some(tool_call_id.to_string()),
                &guard,
            )
            .await
            .expect("winning response");
        bamboo_engine::session_app::execute::consume_pending_clarification_resume(&mut completed);
        bamboo_engine::session_app::execute::clear_startup_handoff(&mut completed);
        completed.set_last_run_status("completed");
        completed.clear_last_run_error();
        state.save_and_cache_session(&mut completed).await;

        let mut winner = AgentRunner::new();
        winner.run_id = "winner-run".to_string();
        winner.status = AgentStatus::Completed;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), winner);
        assert!(
            !duplicate.is_finished(),
            "duplicate must wait behind the winner's response transaction"
        );

        drop(guard);
        let status = tokio::time::timeout(std::time::Duration::from_secs(1), duplicate)
            .await
            .expect("duplicate should finish after the gate opens")
            .expect("duplicate task");
        assert_eq!(status, actix_web::http::StatusCode::BAD_REQUEST);

        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("winner runner remains");
        assert_eq!(runner.run_id, "winner-run");
        assert!(matches!(runner.status, AgentStatus::Completed));
        drop(runners);
        assert_eq!(
            state.session_event_senders.read().await.len(),
            baseline_senders,
            "stale duplicate must not allocate a sender before rejecting"
        );
        let durable = state
            .storage
            .load_session(session_id)
            .await
            .expect("durable load")
            .expect("durable session");
        assert!(durable.pending_question.is_none());
        assert_eq!(durable.last_run_status().as_deref(), Some("completed"));
    }

    #[actix_web::test]
    async fn legacy_respond_rejects_persisted_permission_after_pending_map_loss() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "legacy-permission-after-map-loss";
        let tool_call_id = "permission-call-after-map-loss";
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(Message::tool_result(
            tool_call_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "question": "Allow cargo test?",
                "permission_type": "execute_command",
                "resource": "cargo test",
                "options": ["Approve", "Deny"],
                "allow_custom": false,
                "permission_request": {
                    "request_id": tool_call_id,
                    "session_id": session_id,
                    "allowed_decisions": "malformed"
                }
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "Bash".to_string(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;
        let permission_config = state
            .permission_checker
            .permission_config()
            .expect("permission config");
        assert!(permission_config
            .pending_request(session_id, tool_call_id)
            .is_none());
        let baseline_runners = state.agent_runners.read().await.len();
        let baseline_senders = state.session_event_senders.read().await.len();

        let response = submit_response(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(RespondRequest {
                response: "Approve".to_string(),
                expected_tool_call_id: Some(tool_call_id.to_string()),
                model: None,
                provider: None,
                model_ref: None,
                reasoning_effort: None,
            }),
        )
        .await
        .expect("legacy response");

        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("conflict body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("conflict JSON");
        assert_eq!(body["interaction_kind"], "permission");
        assert_eq!(
            body["permission_decision_endpoint"],
            format!("/api/v1/sessions/{session_id}/permission-decisions")
        );
        assert_eq!(
            body["error"]["message"],
            "Permission interactions require the typed decision endpoint"
        );
        assert!(
            !permission_config.consume_once(
                session_id,
                tool_call_id,
                PermissionType::ExecuteCommand,
                "cargo test"
            ),
            "legacy endpoint must not create a one-shot grant"
        );
        let durable = state
            .storage
            .load_session(session_id)
            .await
            .expect("durable load")
            .expect("durable session");
        assert_eq!(
            durable
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.as_str()),
            Some(tool_call_id)
        );
        assert_eq!(state.agent_runners.read().await.len(), baseline_runners);
        assert_eq!(
            state.session_event_senders.read().await.len(),
            baseline_senders
        );
    }
}
