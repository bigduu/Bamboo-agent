use actix_web::{web, HttpResponse};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::image_fallback::resolve_image_fallback;
use super::runtime::{
    reserve_runner, spawn_agent_execution, spawn_event_forwarder, RunnerReservation,
    SpawnAgentExecution,
};
use super::{ExecuteRequest, ExecuteSyncInfo, ExecuteSyncReason};
use crate::app_state::AppState;
use crate::model_config_helper::{
    get_default_model_for_provider, get_memory_background_model_for_provider,
    get_reasoning_effort_for_provider,
};
use crate::session_app::provider_model::session_effective_model_ref;

use self::response::{
    already_running_response, bad_request_error_response, completed_response,
    internal_server_error_response, started_response,
};

mod response;
#[cfg(test)]
mod tests;
mod validation;

/// Execute the AI agent on a chat session.
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ExecuteRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();

    // ---- Build execution config snapshot from server config ----
    let config_snapshot = state.config.read().await.clone();
    let image_fallback = match resolve_image_fallback(&config_snapshot) {
        Ok(value) => value,
        Err(error) => return internal_server_error_response(error),
    };

    let disabled_tools_vec: Vec<String> =
        config_snapshot.disabled_tool_names().into_iter().collect();
    let disabled_skill_ids_vec: Vec<String> =
        config_snapshot.disabled_skill_ids().into_iter().collect();
    let requested_provider = req
        .model_ref
        .as_ref()
        .map(|model_ref| model_ref.provider.as_str())
        .or(req.provider.as_deref())
        .unwrap_or(config_snapshot.provider.as_str());

    let config = crate::session_app::types::ExecutionConfigSnapshot {
        default_model: get_default_model_for_provider(&config_snapshot, requested_provider).ok(),
        default_model_ref: config_snapshot.defaults.as_ref().map(|d| d.chat.clone()),
        default_reasoning_effort: get_reasoning_effort_for_provider(
            &config_snapshot,
            requested_provider,
        ),
        disabled_tools: disabled_tools_vec.clone(),
        disabled_skill_ids: disabled_skill_ids_vec.clone(),
        provider_name: requested_provider.to_string(),
        fast_model: get_memory_background_model_for_provider(&config_snapshot, requested_provider),
        fast_model_ref: config_snapshot
            .defaults
            .as_ref()
            .and_then(|d| d.fast.clone()),
        image_fallback: image_fallback.clone(),
        provider_model_ref_enabled: config_snapshot.features.provider_model_ref,
    };

    let input = crate::session_app::types::ExecuteInput {
        session_id: session_id.clone(),
        request_model: req.model.clone(),
        request_model_ref: req.model_ref.clone(),
        request_provider: req.provider.clone(),
        request_reasoning_effort: req.reasoning_effort,
        request_skill_mode: req.skill_mode.clone(),
        client_sync: req.client_sync.as_ref().map(|cs| {
            crate::session_app::types::ExecuteClientSync {
                client_message_count: cs.client_message_count,
                client_last_message_id: cs.client_last_message_id.clone(),
                client_has_pending_question: cs.client_has_pending_question,
                client_pending_question_tool_call_id: cs
                    .client_pending_question_tool_call_id
                    .clone(),
            }
        }),
    };

    let outcome =
        match crate::session_app::execute::prepare_execute(state.as_ref(), config.clone(), input)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return match error {
                    crate::session_app::errors::ExecutePreparationError::NotFound(_) => {
                        tracing::warn!("[{session_id}] Execute session not found");
                        HttpResponse::NotFound().json(serde_json::json!({
                            "error": "Session not found",
                            "session_id": session_id
                        }))
                    }
                    crate::session_app::errors::ExecutePreparationError::LoadFailed(load_err) => {
                        let err_msg = load_err.to_string();
                        tracing::error!("[{session_id}] Execute session load error: {err_msg}");
                        HttpResponse::InternalServerError().json(serde_json::json!({
                            "error": format!("Failed to load session: {err_msg}")
                        }))
                    }
                    _ => internal_server_error_response(format!(
                        "Execute preparation failed: {error}"
                    )),
                };
            }
        };

    match outcome {
        crate::session_app::types::ExecutePreparationOutcome::Ready {
            session,
            effective_model,
            effective_reasoning_effort,
            model_source,
            reasoning_source,
            is_child_session,
        } => {
            let session = *session;
            // ---- Reserve runner ----
            let session_tx = state.get_session_event_sender(&session_id).await;
            let cancel_token = match reserve_runner(state.get_ref(), &session_id, &session_tx).await
            {
                RunnerReservation::Started(token) => token,
                RunnerReservation::AlreadyRunning => {
                    let sync_info = build_sync_info_from_session(&session, None);
                    return already_running_response(&session_id, sync_info);
                }
            };

            // ---- Save session before spawn ----
            if let Err(error) = state.storage.save_session(&session).await {
                return internal_server_error_response(format!(
                    "Failed to persist session config before execute: {}",
                    error
                ));
            }
            {
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
            }

            let disabled_tools: BTreeSet<String> = disabled_tools_vec.into_iter().collect();
            let disabled_skill_ids: BTreeSet<String> = disabled_skill_ids_vec.into_iter().collect();
            let resolved_provider_name = session_effective_model_ref(&session)
                .map(|model_ref| model_ref.provider)
                .unwrap_or_else(|| config.provider_name.clone());
            let resolved_bg = crate::model_config_helper::resolve_background_model(
                &config_snapshot,
                &resolved_provider_name,
                &state.provider_registry,
            );
            let resolved_background_model = resolved_bg.as_ref().map(|m| m.model_name.clone());
            let resolved_bg_provider = resolved_bg.map(|m| m.provider);

            // Build sync info before moving session into SpawnAgentExecution.
            let sync_info = build_sync_info_from_session(&session, None);

            tracing::info!(
                "[{}] Starting agent execution with provider={}, model={}, model_source={}, reasoning_effort={}, reasoning_source={}",
                session_id,
                resolved_provider_name,
                effective_model,
                model_source,
                effective_reasoning_effort
                    .map(bamboo_domain::reasoning::ReasoningEffort::as_str)
                    .unwrap_or("none"),
                reasoning_source
            );

            // Create mpsc channel for agent loop.
            let (mpsc_tx, mpsc_rx) = mpsc::channel::<bamboo_agent_core::AgentEvent>(100);

            spawn_event_forwarder(
                state.clone(),
                session_id.clone(),
                mpsc_rx,
                session_tx.clone(),
            );
            spawn_agent_execution(SpawnAgentExecution {
                state: state.clone(),
                session_id: session_id.clone(),
                session,
                is_child_session,
                provider_name: resolved_provider_name,
                provider_override: None,
                model: effective_model,
                fast_model: resolved_background_model,
                background_model_provider: resolved_bg_provider,
                reasoning_effort: effective_reasoning_effort,
                reasoning_effort_source: reasoning_source.to_string(),
                disabled_tools,
                disabled_skill_ids,
                cancel_token,
                mpsc_tx,
                image_fallback,
            });

            started_response(&session_id, sync_info)
        }

        crate::session_app::types::ExecutePreparationOutcome::AlreadyRunning {
            server_snapshot,
        } => {
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, None);
            already_running_response(&session_id, sync_info)
        }

        crate::session_app::types::ExecutePreparationOutcome::NoPendingMessage {
            server_snapshot,
        } => {
            tracing::debug!(
                "[{}] No pending user message, returning completed status",
                session_id
            );
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, None);
            completed_response(&session_id, sync_info)
        }

        crate::session_app::types::ExecutePreparationOutcome::SyncMismatch {
            reason,
            server_snapshot,
        } => {
            state
                .metrics_service
                .collector()
                .execute_sync_mismatch(reason.as_str(), chrono::Utc::now());
            let sync_info = server_snapshot_to_sync_info(&server_snapshot, Some(reason));
            completed_response(&session_id, sync_info)
        }

        crate::session_app::types::ExecutePreparationOutcome::ModelRequired => {
            bad_request_error_response("no model configured for session or provider")
        }

        crate::session_app::types::ExecutePreparationOutcome::ImageFallbackError(error) => {
            bad_request_error_response(error)
        }
    }
}

/// Convert a crate's `ExecuteSyncReason` to the handler's `ExecuteSyncReason`.
fn crate_sync_reason_to_handler(
    reason: crate::session_app::types::ExecuteSyncReason,
) -> ExecuteSyncReason {
    match reason {
        crate::session_app::types::ExecuteSyncReason::PendingQuestionMismatch => {
            ExecuteSyncReason::PendingQuestionMismatch
        }
        crate::session_app::types::ExecuteSyncReason::MessageCountMismatch => {
            ExecuteSyncReason::MessageCountMismatch
        }
        crate::session_app::types::ExecuteSyncReason::LastMessageIdMismatch => {
            ExecuteSyncReason::LastMessageIdMismatch
        }
    }
}

fn server_snapshot_to_sync_info(
    server_snapshot: &crate::session_app::types::ServerExecuteSnapshot,
    reason: Option<crate::session_app::types::ExecuteSyncReason>,
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

fn build_sync_info_from_session(
    session: &bamboo_agent_core::Session,
    reason: Option<ExecuteSyncReason>,
) -> ExecuteSyncInfo {
    ExecuteSyncInfo {
        need_sync: reason.is_some(),
        reason,
        server_message_count: session.messages.len(),
        server_last_message_id: session.messages.last().map(|m| m.id.clone()),
        has_pending_question: session.pending_question.is_some(),
        pending_question_tool_call_id: session
            .pending_question
            .as_ref()
            .map(|p| p.tool_call_id.clone()),
        has_pending_user_message: false,
    }
}
