use actix_web::{web, HttpResponse};
use anyhow::anyhow;

use crate::services::gemini_model_mapping_service::resolve_model;
use crate::{app_state::AppState, error::AppError};
use bamboo_metrics::types::ForwardStatus;

use super::super::conversion::{convert_gemini_to_messages, convert_gemini_tools};
use super::runtime::{build_gemini_event_stream, StreamRuntimeContext};
use crate::handlers::llm_compat::usage::estimate_prompt_tokens;

/// Stream generate content.
pub async fn stream_generate_content(
    path: web::Path<String>,
    request: web::Json<bamboo_llm::protocol::gemini::GeminiRequest>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    let gemini_model = path.into_inner();

    let resolution = {
        let config = state.config.read().await;
        resolve_model(&config.gemini_model_mapping, &gemini_model)
    };

    tracing::info!(
        "Gemini streamGenerateContent: requested='{}', mapped='{}'",
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
    bamboo_engine::message_hooks::apply_message_preflight_hooks(
        Some(state.session_store.as_ref() as &dyn bamboo_agent_core::storage::AttachmentReader),
        &config_snapshot,
        model_to_use.as_str(),
        &mut internal_messages,
    )
    .await
    .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let estimated_prompt_tokens = estimate_prompt_tokens(&internal_messages);

    let metrics = state.metrics_service.collector();
    metrics.forward_started(
        forward_id.clone(),
        "gemini.stream_generate_content",
        model_to_use.clone(),
        true,
        chrono::Utc::now(),
    );

    let stream = match state
        .get_provider()
        .await
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
            metrics.forward_completed(
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

    let runtime = StreamRuntimeContext {
        metrics,
        forward_id,
        estimated_prompt_tokens,
    };
    let gemini_stream = build_gemini_event_stream(stream, runtime);

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(gemini_stream))
}
