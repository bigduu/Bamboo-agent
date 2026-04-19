use actix_web::{web, HttpResponse};
use anyhow::anyhow;

use bamboo_engine::metrics::types::ForwardStatus;
use crate::services::gemini_model_mapping_service::resolve_model;
use crate::{app_state::AppState, error::AppError};

use super::conversion::{convert_gemini_to_messages, convert_gemini_tools};
use super::usage::{build_estimated_usage, estimate_completion_tokens, estimate_prompt_tokens};

mod collect;
mod response;

#[cfg(test)]
mod tests;

use collect::collect_response_chunks;
use response::build_gemini_response;

/// Generate content (non-streaming).
pub async fn generate_content(
    path: web::Path<String>,
    request: web::Json<bamboo_infrastructure::protocol::gemini::GeminiRequest>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    let gemini_model = path.into_inner();

    let resolution = {
        let config = state.config.read().await;
        resolve_model(&config.gemini_model_mapping, &gemini_model)
    };

    tracing::info!(
        "Gemini generateContent: requested='{}', mapped='{}'",
        gemini_model,
        if resolution.mapped_model.is_empty() {
            "(default)"
        } else {
            &resolution.mapped_model
        }
    );

    let mut internal_messages = convert_gemini_to_messages(&request.contents)?;
    let internal_tools = convert_gemini_tools(&request.tools)?;

    let model_to_use = resolution.mapped_model.trim().to_string();
    if model_to_use.is_empty() {
        return Err(AppError::BadRequest(
            "No Gemini model mapping configured for requested model. Please configure gemini_model_mapping.".to_string(),
        ));
    }

    let config_snapshot = state.config.read().await.clone();
    crate::message_hooks::apply_message_preflight_hooks(
        Some(state.as_ref()),
        &config_snapshot,
        model_to_use.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "gemini.generate_content",
        model_to_use.clone(),
        false,
        chrono::Utc::now(),
    );

    let provider = state.get_provider().await;
    let mut stream = match provider
        .chat_stream(
            &internal_messages,
            &internal_tools,
            None,
            model_to_use.as_str(),
        )
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Provider error: {error}")),
            );
            return Err(AppError::InternalError(anyhow!("Provider error: {error}")));
        }
    };

    let collected = match collect_response_chunks(&mut stream).await {
        Ok(collected) => collected,
        Err(error) => {
            state.metrics_service.collector().forward_completed(
                forward_id.clone(),
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Stream error: {error}")),
            );
            return Err(AppError::InternalError(anyhow!("Stream error: {error}")));
        }
    };

    let completion_tokens = estimate_completion_tokens(&collected.full_content);
    let gemini_response = build_gemini_response(collected.full_content, collected.tool_calls);
    state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        Some(build_estimated_usage(
            estimated_prompt_tokens,
            completion_tokens,
        )),
        None,
    );

    Ok(HttpResponse::Ok().json(gemini_response))
}
