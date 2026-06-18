use actix_web::{web, HttpResponse};

use super::image_fallback::resolve_image_fallback;
use super::{ExecuteRequest, ExecuteSyncInfo, ExecuteSyncReason};
use crate::app_state::AppState;
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{
    get_default_model_for_provider, get_reasoning_effort_for_provider, resolve_gold_config,
    resolve_provider_type,
};

use self::response::{
    already_running_response, bad_request_error_response, completed_response,
    internal_server_error_response,
};

mod ready;
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

    let requested_provider_type = resolve_provider_type(
        &config_snapshot,
        requested_provider,
        &state.provider_registry,
    );
    // Auxiliary (non-chat) models are global: resolved from config for the
    // requested provider, never from the session. One call replaces the
    // fast/background/summarization trio.
    let areas = resolve_global_area_models(
        &config_snapshot,
        requested_provider,
        &state.provider_registry,
    );

    let config = bamboo_engine::session_app::types::ExecutionConfigSnapshot {
        default_model: get_default_model_for_provider(&config_snapshot, requested_provider).ok(),
        default_model_ref: config_snapshot.defaults.as_ref().map(|d| d.chat.clone()),
        default_reasoning_effort: get_reasoning_effort_for_provider(
            &config_snapshot,
            requested_provider,
        ),
        disabled_tools: disabled_tools_vec.clone(),
        disabled_skill_ids: disabled_skill_ids_vec.clone(),
        provider_name: requested_provider.to_string(),
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

    let input = bamboo_engine::session_app::types::ExecuteInput {
        session_id: session_id.clone(),
        request_model: req.model.clone(),
        request_model_ref: req.model_ref.clone(),
        request_provider: req.provider.clone(),
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
            return match error {
                bamboo_engine::session_app::errors::ExecutePreparationError::NotFound(_) => {
                    tracing::warn!("[{session_id}] Execute session not found");
                    HttpResponse::NotFound().json(serde_json::json!({
                        "error": "Session not found",
                        "session_id": session_id
                    }))
                }
                bamboo_engine::session_app::errors::ExecutePreparationError::LoadFailed(
                    load_err,
                ) => {
                    let err_msg = load_err.to_string();
                    tracing::error!("[{session_id}] Execute session load error: {err_msg}");
                    HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Failed to load session: {err_msg}")
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
            ready::handle_execute_ready(
                &state,
                &session_id,
                ready::ReadyExecution {
                    session: *session,
                    effective_model,
                    effective_reasoning_effort,
                    model_source,
                    reasoning_source,
                    is_child_session,
                    no_human_approver: req.no_human_approver,
                },
                &config,
                &config_snapshot,
                image_fallback,
                disabled_tools_vec,
                disabled_skill_ids_vec,
            )
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
            bad_request_error_response("no model configured for session or provider")
        }

        bamboo_engine::session_app::types::ExecutePreparationOutcome::ImageFallbackError(error) => {
            bad_request_error_response(error)
        }
    }
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
