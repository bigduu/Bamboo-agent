use actix_web::{web, HttpRequest, HttpResponse, ResponseError};

use super::image_fallback::resolve_image_fallback;
use super::{ExecuteRequest, ExecuteSyncInfo, ExecuteSyncReason};
use crate::app_state::AppState;
use bamboo_engine::execution::SessionExecutionReservation;
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{
    get_default_model_for_provider, get_reasoning_effort_for_provider, resolve_gold_config,
    resolve_provider_routing_key, resolve_provider_type,
};

use self::response::{
    already_running_response, bad_request_error_response, completed_response,
    internal_server_error_response,
};

mod ready;
mod receipt;
mod response;
#[cfg(test)]
mod tests;
mod validation;

/// Execute the AI agent on a chat session.
pub async fn handler(
    state: web::Data<AppState>,
    http_request: HttpRequest,
    path: web::Path<String>,
    req: web::Json<ExecuteRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if let Err(error) = crate::handlers::agent::tickets::require_supervisor_owner(
        &state,
        &http_request,
        &session_id,
    )
    .await
    {
        return crate::handlers::agent::tickets::TicketHttpError::from(error).error_response();
    }
    let selected = match receipt::selection(&http_request) {
        Ok(selected) => selected,
        Err(response) => return *response,
    };
    let continuation = match continuation_selector(&http_request, &req) {
        Ok(continuation) => continuation,
        Err(response) => return response,
    };
    if continuation.is_some() && selected.is_some() {
        return bad_request_error_response("A continuation cannot select a new input receipt");
    }
    let target = format!("POST /api/v1/execute/{session_id}");
    let prepared = match if let Some(receipt) = selected.as_ref() {
        crate::app_state::mutation_idempotency::prepare(
            &http_request,
            "execute",
            &target,
            &serde_json::json!({"request": &*req, "input": {"message_id": receipt.receipt.id.as_str(), "ingress_seq": receipt.receipt.generation, "session_created_at": receipt.created_at}}),
        )
    } else if let Some(run_id) = continuation.as_ref() {
        crate::app_state::mutation_idempotency::prepare(
            &http_request,
            "execute",
            &target,
            &serde_json::json!({"request": &*req, "continue_run_id": run_id}),
        )
    } else {
        crate::app_state::mutation_idempotency::prepare(&http_request, "execute", &target, &*req)
    } {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let store = state.mutation_idempotency.clone();
    let operation = || async move {
        if let Some(run_id) = continuation {
            continue_live_retry(&state, &session_id, &run_id).await
        } else {
            handle_execute_inner(state, session_id, req, selected).await
        }
    };
    match prepared {
        Some(prepared) => store.execute(prepared, operation).await,
        None => operation().await,
    }
}

// A continuation selects a still-live run; it cannot replace any of the
// model, provider, policy, or input parameters already captured by that run.
fn continuation_selector(
    http: &HttpRequest,
    req: &ExecuteRequest,
) -> Result<Option<String>, HttpResponse> {
    let values = http.headers().get_all("Bamboo-Run-Id");
    if values.count() == 0 {
        return Ok(None);
    }
    let run = http
        .headers()
        .get("Bamboo-Run-Id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_graphic())
        });
    if http.headers().get_all("Bamboo-Run-Id").count() != 1 || run.is_none() {
        return Err(bad_request_error_response("Invalid Bamboo-Run-Id"));
    }
    if !matches!(
        req.upstream_retry,
        Some(bamboo_llm::retry::UpstreamRetry::UntilCancelled)
    ) || req.model.is_some()
        || req.provider.is_some()
        || req.model_ref.is_some()
        || req.skill_mode.is_some()
        || req.reasoning_effort.is_some()
        || req.client_sync.is_some()
        || req.no_human_approver
        || req.run_budget.is_some()
        || req.live_bootstrap_resume
    {
        return Err(bad_request_error_response(
            "A continuation requires only upstream_retry.mode=until_cancelled",
        ));
    }
    Ok(run.map(str::to_owned))
}

async fn continue_live_retry(
    state: &web::Data<AppState>,
    session_id: &str,
    run_id: &str,
) -> HttpResponse {
    use crate::app_state::live_retry::ContinueResult;
    match state.live_retry.confirm(&state.agent_runners, session_id, run_id).await {
        ContinueResult::Continuing => HttpResponse::Ok().json(serde_json::json!({
            "session_id": session_id, "run_id": run_id, "status": "retry_continuing",
            "events_url": format!("/api/v1/events/{session_id}")
        })),
        ContinueResult::NotPrepared => continuous_retry_resume_not_prepared(session_id),
        ContinueResult::OutcomeUncertain => HttpResponse::Conflict().json(serde_json::json!({
            "session_id": session_id, "run_id": run_id,
            "error": {"type": "api_error", "code": "retry_resume_outcome_uncertain",
                "message": "The continuation outcome is uncertain. Follow the original run before retrying."}
        })),
    }
}

/// Nonterminal waiting observation for a live producer offer.
pub async fn retry_waiting_handler(
    state: web::Data<AppState>,
    http: HttpRequest,
    path: web::Path<String>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if let Err(error) =
        crate::handlers::agent::tickets::require_supervisor_owner(&state, &http, &session_id).await
    {
        return crate::handlers::agent::tickets::TicketHttpError::from(error).error_response();
    }
    match state
        .live_retry
        .waiting(&state.agent_runners, &session_id)
        .await
    {
        Some(waiting) => HttpResponse::Ok().json(serde_json::json!({
            "session_id": session_id, "run_id": waiting.run_id, "status": "retry_waiting",
            "expires_in_ms": waiting.expires_in_ms, "confirming": waiting.confirming,
        })),
        None => HttpResponse::Ok()
            .json(serde_json::json!({"session_id": session_id, "status": "unavailable"})),
    }
}

/// Execute a session for in-process callers that do not cross the HTTP
/// boundary. HTTP callers should use [`handler`] so an `Idempotency-Key` can
/// be honored before any durable work starts.
pub async fn handle_execute(
    state: web::Data<AppState>,
    session_id: String,
    req: web::Json<ExecuteRequest>,
) -> HttpResponse {
    handle_execute_inner(state, session_id, req, None).await
}

struct RestoredFreshAdmission {
    inputs: Option<bamboo_engine::config::UntrustedExecutionInputs>,
    witness: Option<crate::app_state::fresh_sync::FreshSyncWitness>,
    generate_title: bool,
    native_main: bool,
    live_bootstrap_resume: bool,
}

async fn handle_execute_inner(
    state: web::Data<AppState>,
    session_id: String,
    req: web::Json<ExecuteRequest>,
    selected: Option<receipt::Selection>,
) -> HttpResponse {
    match state.storage.load_session(&session_id).await {
        Ok(Some(session)) => {
            if let Some(response) = crate::handlers::agent::sessions::SessionSummary::execution_conflict(&session) {
                return response;
            }
        }
        Ok(None) => {},
        Err(error) => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value(format!("Failed to inspect session compatibility: {error}")),
        })),
    }
    // Linearize startup ownership against an abandoned-turn reconciliation.
    // Reconciliation holds the same persistence lock through its durable CAS
    // and broadcast; whichever acquires it first becomes the authoritative
    // transition for this turn.
    let startup_lock = state.persistence.acquire_lock(&session_id).await;
    // Protect the preparation window before `reserve_runner` can publish a
    // Pending runner. Reference-counted RAII releases on every return/panic.
    let mut startup_guard =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), &session_id);
    // Bind rejection rollback to the exact turn observed by this request. A
    // delayed failure from turn A must never poison a newer turn B.
    drop(startup_lock);
    // A resync handoff contains the original non-clonable reservation and
    // freshly admitted input. Never recreate it from history or a receipt.
    let handoff = if selected.is_none() {
        state.fresh_sync.take(state.get_ref(), &session_id).await
    } else {
        None
    };
    let mut restored = None;
    let selected_reservation = if let Some(handoff) = handoff {
        let crate::app_state::fresh_sync::FreshSyncHandoff {
            reservation,
            inputs,
            witness,
            generate_title,
            native_main,
            live_bootstrap_resume,
        } = handoff;
        restored = Some(RestoredFreshAdmission {
            inputs,
            witness,
            generate_title,
            native_main,
            live_bootstrap_resume,
        });
        Ok(Some(reservation))
    } else {
        match selected.as_ref() {
            Some(receipt) => receipt::reserve(&state, &session_id, receipt)
                .await
                .map(Some),
            None => state.reserve_root_chat_for_execute(&session_id).await,
        }
    };
    let mut admission_reservation = match selected_reservation {
        Ok(reservation) => reservation,
        Err(response) => return *response,
    };
    #[cfg(test)]
    super::tests::after_root_admission_selection(&session_id, admission_reservation.is_some())
        .await;
    let response = handle_reserved_execute(
        state,
        session_id,
        req,
        &mut startup_guard,
        &mut admission_reservation,
        restored,
    )
    .await;
    // Await exact cleanup on every non-spawn return. The reservation's own
    // cancellation-safe Drop remains the fallback if this HTTP future is lost.
    abandon_admission_reservation(&mut admission_reservation).await;
    receipt::annotate(response, selected)
}

async fn handle_reserved_execute(
    state: web::Data<AppState>,
    session_id: String,
    req: web::Json<ExecuteRequest>,
    startup_guard: &mut crate::handlers::agent::events::ExecuteStartupGuard,
    admission_reservation: &mut Option<SessionExecutionReservation>,
    restored: Option<RestoredFreshAdmission>,
) -> HttpResponse {
    let restored_owner = restored.is_some();
    // A sync correction reuses this owner; preserve the opt-in selected on
    // its first request so the later provider future can publish its offer.
    let live_bootstrap_resume = req.live_bootstrap_resume
        || restored
            .as_ref()
            .is_some_and(|handoff| handoff.live_bootstrap_resume);
    let admission = if restored_owner {
        None
    } else {
        Some(
            match state
                .admit_chat_for_execute_with_http_reservation(
                    &session_id,
                    admission_reservation,
                    matches!(
                        req.upstream_retry,
                        Some(bamboo_llm::retry::UpstreamRetry::UntilCancelled)
                    ),
                )
                .await
            {
                Ok(admission) => admission,
                Err(response) => return *response,
            },
        )
    };
    if admission
        .as_ref()
        .is_some_and(|admission| admission.retry_unprepared)
    {
        // No execution can start here. Preserve the existing sync contract
        // before reporting the absent continuation: a cached view must not
        // mask a newer canonical session, and a stale client must be told to
        // reload instead of being given a misleading recovery response.
        let canonical = match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => session,
            Ok(None) => {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "session_id": session_id,
                    "error": crate::error::error_value("Session not found"),
                }));
            }
            Err(_) => {
                return internal_server_error_response(
                    "Could not verify the session before continuous retry",
                );
            }
        };
        let snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&canonical);
        let cached = state.load_session(&session_id).await;
        if !cached.as_ref().is_some_and(|session| {
            session.created_at == canonical.created_at
                && bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(session)
                    == snapshot
        }) {
            return continuous_retry_state_changed(&session_id);
        }
        let client_sync = req.client_sync.as_ref().map(|sync| {
            bamboo_engine::session_app::types::ExecuteClientSync {
                client_message_count: sync.client_message_count,
                client_last_message_id: sync.client_last_message_id.clone(),
                client_has_pending_question: sync.client_has_pending_question,
                client_pending_question_tool_call_id: sync
                    .client_pending_question_tool_call_id
                    .clone(),
            }
        });
        if let Some(reason) = bamboo_engine::session_app::execute::evaluate_client_sync(
            client_sync.as_ref(),
            &snapshot,
        ) {
            state
                .metrics_service
                .collector()
                .execute_sync_mismatch(reason.as_str(), chrono::Utc::now());
            return completed_response(
                &session_id,
                server_snapshot_to_sync_info(&snapshot, Some(reason)),
            );
        }
        return continuous_retry_resume_not_prepared(&session_id);
    }
    let (mut untrusted_inputs, mut fresh_sync, generate_title, native_main) =
        if let Some(restored) = restored {
            (
                restored.inputs,
                restored.witness,
                restored.generate_title,
                restored.native_main,
            )
        } else {
            let admission = admission.expect("new admission exists");
            (
                admission.inputs,
                admission.fresh_sync,
                admission.generate_title,
                admission.native_main,
            )
        };
    let startup_turn_id = state
        .storage
        .load_session(&session_id)
        .await
        .ok()
        .flatten()
        .and_then(|session| crate::handlers::agent::events::startup_work_id(&session));
    tracing::debug!(
        "[{}] Execute requested: model={:?}, model_ref={:?}, reasoning_effort={:?}, has_client_sync={}",
        session_id,
        req.model,
        req.model_ref
            .as_ref()
            .map(|m| format!("{}/{}", m.provider, m.model)),
        req.reasoning_effort,
        req.client_sync.is_some(),
    );

    // ---- Build execution config snapshot from server config ----
    let config_snapshot = state.config.read().await.clone();
    let image_fallback = match resolve_image_fallback(&config_snapshot) {
        Ok(value) => value,
        Err(error) => {
            fail_reserved_pending_startup(
                &state,
                &session_id,
                startup_turn_id.as_deref(),
                &error,
                admission_reservation,
                startup_guard,
            )
            .await;
            return internal_server_error_response(error);
        }
    };

    let disabled_tools_vec: Vec<String> = config_snapshot
        .disabled_tool_references()
        .into_iter()
        .collect();
    let disabled_skill_ids_vec: Vec<String> =
        config_snapshot.disabled_skill_ids().into_iter().collect();
    let explicit_provider = req
        .model_ref
        .as_ref()
        .map(|model_ref| model_ref.provider.as_str())
        .or(req.provider.as_deref());
    let requested_provider_alias = resolve_requested_provider(&config_snapshot, explicit_provider);
    let requested_provider = match resolve_provider_routing_key(
        &config_snapshot,
        requested_provider_alias,
        &state.provider_registry,
    ) {
        Ok(provider) => provider,
        Err(error) => {
            let detail = format!("provider routing failed: {error}");
            fail_reserved_pending_startup(
                &state,
                &session_id,
                startup_turn_id.as_deref(),
                &detail,
                admission_reservation,
                startup_guard,
            )
            .await;
            return if explicit_provider.is_some() {
                bad_request_error_response(detail)
            } else {
                internal_server_error_response(detail)
            };
        }
    };

    let requested_provider_type = resolve_provider_type(
        &config_snapshot,
        &requested_provider,
        &state.provider_registry,
    );
    // Auxiliary (non-chat) models are global: resolved from config for the
    // requested provider, never from the session. One call replaces the
    // fast/background/summarization trio.
    let areas = resolve_global_area_models(
        &config_snapshot,
        &requested_provider,
        &state.provider_registry,
    );

    let config = bamboo_engine::session_app::types::ExecutionConfigSnapshot {
        default_model: get_default_model_for_provider(&config_snapshot, &requested_provider).ok(),
        default_model_ref: config_snapshot.defaults.as_ref().map(|d| d.chat.clone()),
        default_reasoning_effort: get_reasoning_effort_for_provider(
            &config_snapshot,
            &requested_provider,
        ),
        disabled_tools: disabled_tools_vec.clone(),
        disabled_skill_ids: disabled_skill_ids_vec.clone(),
        provider_name: requested_provider.clone(),
        provider_type: requested_provider_type.clone(),
        fast_model: areas.fast.as_ref().map(|m| m.model_name.clone()),
        fast_model_ref: areas.fast_ref.clone(),
        background_model: areas.background.as_ref().map(|m| m.model_name.clone()),
        background_model_ref: areas.background_ref.clone(),
        summarization_model: areas.summarization.as_ref().map(|m| m.model_name.clone()),
        summarization_model_ref: areas.summarization_ref.clone(),
        image_fallback: image_fallback.clone(),
        gold_config: resolve_gold_config(&config_snapshot, None),
        provider_model_ref_enabled: config_snapshot.features.provider_model_ref,
    };

    let mut request_model_ref = req.model_ref.clone();
    if let Some(model_ref) = &mut request_model_ref {
        model_ref.provider = requested_provider.clone();
    }
    let request_provider = req.provider.as_ref().map(|_| requested_provider.clone());
    let input = bamboo_engine::session_app::types::ExecuteInput {
        session_id: session_id.clone(),
        request_model: req.model.clone(),
        request_model_ref,
        request_provider,
        request_reasoning_effort: req.reasoning_effort,
        request_skill_mode: req.skill_mode.clone(),
        client_sync: req.client_sync.as_ref().map(|cs| {
            bamboo_engine::session_app::types::ExecuteClientSync {
                client_message_count: cs.client_message_count,
                client_last_message_id: cs.client_last_message_id.clone(),
                client_has_pending_question: cs.client_has_pending_question,
                client_pending_question_tool_call_id: cs
                    .client_pending_question_tool_call_id
                    .clone(),
            }
        }),
    };

    let outcome = match bamboo_engine::session_app::execute::prepare_execute(
        state.as_ref(),
        config.clone(),
        input,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            fail_reserved_pending_startup(
                &state,
                &session_id,
                startup_turn_id.as_deref(),
                &format!("execute preparation failed: {error}"),
                admission_reservation,
                startup_guard,
            )
            .await;
            return match error {
                bamboo_engine::session_app::errors::ExecutePreparationError::NotFound(_) => {
                    tracing::warn!("[{session_id}] Execute session not found");
                    HttpResponse::NotFound().json(serde_json::json!({
                        "error": crate::error::error_value("Session not found"),
                        "session_id": session_id
                    }))
                }
                bamboo_engine::session_app::errors::ExecutePreparationError::LoadFailed(
                    load_err,
                ) => {
                    let err_msg = load_err.to_string();
                    tracing::error!("[{session_id}] Execute session load error: {err_msg}");
                    HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": crate::error::error_value(format!(
                            "Failed to load session: {err_msg}"
                        ))
                    }))
                }
                _ => internal_server_error_response(format!("Execute preparation failed: {error}")),
            };
        }
    };

    match outcome {
        bamboo_engine::session_app::types::ExecutePreparationOutcome::Ready {
            session,
            effective_model,
            effective_reasoning_effort,
            model_source,
            reasoning_source,
            is_child_session,
        } => {
            let session = *session;
            ready::handle_execute_ready(ready::ExecuteReadyContext {
                state: &state,
                session_id: &session_id,
                ready: ready::ReadyExecution {
                    session,
                    startup_guard,
                    admission_reservation,
                    startup_turn_id: startup_turn_id.clone(),
                    untrusted_inputs,
                    native_main,
                    generate_title,
                    effective_model,
                    effective_reasoning_effort,
                    model_source,
                    reasoning_source,
                    is_child_session,
                    no_human_approver: req.no_human_approver,
                    run_budget: req.run_budget,
                    upstream_retry: req.upstream_retry.unwrap_or_default(),
                    live_bootstrap_resume,
                },
                config: &config,
                config_snapshot: &config_snapshot,
                image_fallback,
                disabled_tools: disabled_tools_vec,
                disabled_skill_ids: disabled_skill_ids_vec,
            })
            .await
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::AlreadyRunning {
            server_snapshot,
        } => {
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, None);
            already_running_response(&session_id, sync_info, None)
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::NoPendingMessage {
            server_snapshot,
        } => {
            // The per-run HTTP retry policy is not a recovery authorization.
            // Recheck the durable conversation and exact live runner before
            // describing a no-pending outcome to an opt-in retry caller.
            if matches!(
                req.upstream_retry,
                Some(bamboo_llm::retry::UpstreamRetry::UntilCancelled)
            ) {
                // Inbox is read before Main, matching the existing terminal
                // protocol: a consumer checkpoints its User before its ACK.
                let backlog = match state.session_inbox.inspect(&session_id).await {
                    Ok(backlog) => backlog,
                    Err(_) => {
                        // Inbox inspection can fail because the canonical
                        // session was deleted after an older snapshot was cached.
                        // Preserve the inbox-before-main ordering on success.
                        if matches!(state.storage.load_session(&session_id).await, Ok(None)) {
                            return HttpResponse::NotFound().json(serde_json::json!({
                                "session_id": session_id,
                                "error": crate::error::error_value("Session not found"),
                            }));
                        }
                        return internal_server_error_response(
                            "Could not verify pending work before continuous retry",
                        );
                    }
                };
                let latest = match state.storage.load_session(&session_id).await {
                    Ok(Some(latest)) => latest,
                    Ok(None) => {
                        return HttpResponse::NotFound().json(serde_json::json!({
                            "session_id": session_id,
                            "error": crate::error::error_value("Session not found"),
                        }));
                    }
                    Err(_) => {
                        return internal_server_error_response(
                            "Could not verify whether the previous run can be resumed",
                        );
                    }
                };
                let latest_snapshot =
                    bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&latest);
                // The earlier client sync check used the cached snapshot. A
                // changed durable snapshot cannot inherit its need_sync=false.
                if latest_snapshot != server_snapshot {
                    return continuous_retry_state_changed(&session_id);
                }
                let (active_runner, runner_busy, runner_failed) = {
                    let runners = state.agent_runners.read().await;
                    runners
                        .get(&session_id)
                        .map_or((None, false, false), |runner| {
                            let resetting = runner.root_reset_admission_reserved();
                            let running =
                                matches!(runner.status, crate::app_state::AgentStatus::Running);
                            let pending =
                                matches!(runner.status, crate::app_state::AgentStatus::Pending);
                            let failed = matches!(
                                runner.status,
                                crate::app_state::AgentStatus::Error(_)
                                    | crate::app_state::AgentStatus::Cancelled
                            );
                            (
                                (!resetting && running).then(|| runner.run_id.clone()),
                                resetting || running || pending,
                                failed,
                            )
                        })
                };
                if let Some(run_id) = active_runner.as_ref() {
                    let owned_by_this_request = admission_reservation
                        .as_ref()
                        .is_some_and(|reservation| reservation.run_id() == run_id.as_str());
                    if !owned_by_this_request
                        && state
                            .session_activation_router
                            .current_run_id(&session_id)
                            .await
                            .as_deref()
                            == Some(run_id.as_str())
                    {
                        return already_running_response(
                            &session_id,
                            build_sync_info_from_session(&latest),
                            Some(run_id.clone()),
                        );
                    }
                }
                let last_run_status = latest.last_run_status();
                let runtime_wait = latest.agent_runtime_state.as_ref().is_some_and(|runtime| {
                    runtime.waiting_for_children.is_some()
                        || runtime.waiting_for_bash.is_some()
                        || matches!(
                            runtime.status,
                            bamboo_domain::AgentStatusState::Initializing
                                | bamboo_domain::AgentStatusState::Running
                                | bamboo_domain::AgentStatusState::Suspended
                                | bamboo_domain::AgentStatusState::Finalizing
                        )
                        || (last_run_status.as_deref() == Some("completed")
                            && matches!(
                                runtime.status,
                                bamboo_domain::AgentStatusState::Failed
                                    | bamboo_domain::AgentStatusState::Cancelled
                            ))
                });
                let changed_or_busy = runner_busy
                    || (last_run_status.as_deref() == Some("completed") && runner_failed)
                    || latest_snapshot.has_pending_user_message
                    || latest_snapshot.has_pending_question
                    || backlog.activation_pending()
                    || latest.metadata.contains_key("chat.queued_ingress.v1")
                    || latest.has_pending_injected_messages()
                    || runtime_wait
                    || latest.metadata.contains_key("runtime.suspend_reason")
                    || matches!(
                        last_run_status.as_deref(),
                        Some("pending" | "running" | "suspended")
                    );
                if changed_or_busy {
                    return continuous_retry_state_changed(&session_id);
                }
                if last_run_status.as_deref() != Some("completed") {
                    return continuous_retry_resume_not_prepared(&session_id);
                }
                return completed_response(
                    &session_id,
                    server_snapshot_to_sync_info(&latest_snapshot, None),
                );
            }
            tracing::debug!(
                "[{}] No pending user message, returning completed status",
                session_id
            );
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, None);
            completed_response(&session_id, sync_info)
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::SyncMismatch {
            reason,
            server_snapshot,
        } => {
            // Retain the same owner only when this request actually admitted
            // fresh work, or recovered that very owner from an earlier sync.
            let needs_handoff = fresh_sync.is_some()
                && (restored_owner
                    || live_bootstrap_resume
                    || matches!(
                        req.upstream_retry,
                        Some(bamboo_llm::retry::UpstreamRetry::UntilCancelled)
                    ));
            if needs_handoff
                && !state
                    .fresh_sync
                    .park(
                        state.get_ref(),
                        admission_reservation,
                        &mut untrusted_inputs,
                        &mut fresh_sync,
                        generate_title,
                        native_main,
                        live_bootstrap_resume,
                    )
                    .await
            {
                return HttpResponse::ServiceUnavailable().json(serde_json::json!({
                    "session_id": session_id,
                    "error": {"type": "api_error", "code": "retry_input_handoff_unavailable",
                        "message": "The new message is preserved, but its execution handoff could not be retained."}
                }));
            }
            tracing::debug!(
                "[{}] Execute SyncMismatch (reason={:?}); returning completed/need_sync without starting a runner",
                session_id,
                reason
            );
            state
                .metrics_service
                .collector()
                .execute_sync_mismatch(reason.as_str(), chrono::Utc::now());
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, Some(reason));
            completed_response(&session_id, sync_info)
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::ModelRequired => {
            fail_reserved_pending_startup(
                &state,
                &session_id,
                startup_turn_id.as_deref(),
                "no model configured",
                admission_reservation,
                startup_guard,
            )
            .await;
            bad_request_error_response("no model configured for session or provider")
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::ImageFallbackError(error) => {
            fail_reserved_pending_startup(
                &state,
                &session_id,
                startup_turn_id.as_deref(),
                &error,
                admission_reservation,
                startup_guard,
            )
            .await;
            bad_request_error_response(error)
        }
    }
}

fn continuous_retry_resume_not_prepared(session_id: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "session_id": session_id,
        "error": {
            "message": "Continuous retry did not start because no safe resume was prepared for this conversation. The conversation was preserved.",
            "type": "api_error",
            "code": "retry_resume_not_prepared"
        }
    }))
}

fn continuous_retry_state_changed(session_id: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "session_id": session_id,
        "error": {
            "message": "Continuous retry did not start because the session has pending or changed work. Refresh the session before retrying.",
            "type": "api_error",
            "code": "retry_state_changed"
        }
    }))
}

fn resolve_requested_provider<'a>(
    config: &'a bamboo_config::Config,
    explicit_provider: Option<&'a str>,
) -> &'a str {
    explicit_provider.unwrap_or_else(|| config.effective_default_provider())
}

async fn abandon_admission_reservation(reservation: &mut Option<SessionExecutionReservation>) {
    if let Some(reservation) = reservation.take() {
        reservation.abandon().await;
    }
}

async fn fail_reserved_pending_startup(
    state: &web::Data<AppState>,
    session_id: &str,
    expected_turn_id: Option<&str>,
    detail: &str,
    admission_reservation: &mut Option<SessionExecutionReservation>,
    startup_guard: &mut crate::handlers::agent::events::ExecuteStartupGuard,
) {
    abandon_admission_reservation(admission_reservation).await;
    fail_pending_startup(state, session_id, expected_turn_id, detail, startup_guard).await;
}

async fn fail_pending_startup(
    state: &web::Data<AppState>,
    session_id: &str,
    expected_turn_id: Option<&str>,
    detail: &str,
    startup_guard: &mut crate::handlers::agent::events::ExecuteStartupGuard,
) {
    let Some(expected_turn_id) = expected_turn_id else {
        return;
    };
    crate::handlers::agent::events::transition_startup_failure_if_owned(
        state,
        session_id,
        crate::handlers::agent::events::StartupFailureTarget::WorkId {
            work_id: expected_turn_id,
            startup_guard,
        },
        detail,
    )
    .await;
}

/// Convert a crate's `ExecuteSyncReason` to the handler's `ExecuteSyncReason`.
fn crate_sync_reason_to_handler(
    reason: bamboo_engine::session_app::types::ExecuteSyncReason,
) -> ExecuteSyncReason {
    match reason {
        bamboo_engine::session_app::types::ExecuteSyncReason::PendingQuestionMismatch => {
            ExecuteSyncReason::PendingQuestionMismatch
        }
        bamboo_engine::session_app::types::ExecuteSyncReason::MessageCountMismatch => {
            ExecuteSyncReason::MessageCountMismatch
        }
        bamboo_engine::session_app::types::ExecuteSyncReason::LastMessageIdMismatch => {
            ExecuteSyncReason::LastMessageIdMismatch
        }
    }
}

fn server_snapshot_to_sync_info(
    server_snapshot: &bamboo_engine::session_app::types::ServerExecuteSnapshot,
    reason: Option<bamboo_engine::session_app::types::ExecuteSyncReason>,
) -> ExecuteSyncInfo {
    ExecuteSyncInfo {
        need_sync: reason.is_some(),
        reason: reason.map(crate_sync_reason_to_handler),
        server_message_count: server_snapshot.message_count,
        server_last_message_id: server_snapshot.last_message_id.clone(),
        has_pending_question: server_snapshot.has_pending_question,
        pending_question_tool_call_id: server_snapshot.pending_question_tool_call_id.clone(),
        has_pending_user_message: server_snapshot.has_pending_user_message,
    }
}

fn build_sync_info_from_session(session: &bamboo_agent_core::Session) -> ExecuteSyncInfo {
    // Reuse `server_snapshot_to_sync_info` so the visible-message filter
    // (matching GET /history) lives in exactly one place. Override
    // `has_pending_user_message` to false: this builder is used when the
    // runner is already started (Ready) or already running for the session,
    // i.e. the client should not retry execute.
    let snapshot = bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(session);
    let mut info = server_snapshot_to_sync_info(&snapshot, None);
    info.has_pending_user_message = false;
    info
}
