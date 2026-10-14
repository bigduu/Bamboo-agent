use actix_web::{http::StatusCode, web, FromRequest, HttpRequest, HttpResponse};
use anyhow::anyhow;
use bamboo_llm::protocol::gemini::{GeminiContent, GeminiRequest, GeminiTool};

use crate::services::gemini_model_mapping_service::resolve_model;
use crate::{app_state::AppState, error::AppError};
use bamboo_metrics::types::ForwardStatus;

use super::conversion::{convert_gemini_to_messages, convert_gemini_tools};
use crate::handlers::llm_compat::usage::{
    build_estimated_usage, estimate_completion_tokens, estimate_prompt_tokens,
};

mod collect;
mod response;

#[cfg(test)]
mod tests;

use collect::collect_response_chunks;
use response::build_gemini_response;

/// Explicit receipt selectors choose the strict Gemini extension schema.
/// Ordinary requests retain the original native DTO and framework JSON policy.
pub enum GeminiHttpRequest {
    Activation(GeminiReceiptActivationBody),
    Forward(GeminiRequest),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeminiReceiptActivationBody {
    contents: Vec<GeminiContent>,
    #[serde(default)]
    tools: Vec<GeminiTool>,
}

impl FromRequest for GeminiHttpRequest {
    type Error = actix_web::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>>>>;
    fn from_request(http: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        if crate::handlers::llm_compat::activation::requested(http) {
            let parsed = web::Json::<GeminiReceiptActivationBody>::from_request(http, payload);
            Box::pin(async move {
                let body = parsed.await.map_err(|error| {
                    if error.as_response_error().status_code() != StatusCode::BAD_REQUEST {
                        return error;
                    }
                    actix_web::error::InternalError::from_response(error,
                        crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                            "unsupported_bamboo_activation_body",
                            "Receipt activation allows only present empty contents and optional empty tools; null and other fields are rejected"))
                        .into()
                })?;
                Ok(Self::Activation(body.into_inner()))
            })
        } else {
            let parsed = web::Json::<GeminiRequest>::from_request(http, payload);
            Box::pin(async move { Ok(Self::Forward(parsed.await?.into_inner())) })
        }
    }
}

/// Generate content (non-streaming).
pub async fn generate_content(
    path: web::Path<String>,
    request: GeminiHttpRequest,
    state: web::Data<AppState>,
    http: HttpRequest,
) -> Result<HttpResponse, AppError> {
    let gemini_model = path.into_inner();
    let request = match request {
        GeminiHttpRequest::Activation(body) => {
            if gemini_model != "bamboo-agent" || !body.contents.is_empty() || !body.tools.is_empty()
            {
                return Ok(crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                    "unsupported_bamboo_activation_body",
                    "Receipt activation requires URL model=bamboo-agent, empty contents and optional empty tools"));
            }
            // Preserve the original HTTP identity and use the existing Native
            // User gate, selected-input claim and actual Root lifecycle.
            return Ok(crate::handlers::llm_compat::activation::activate(state, http).await);
        }
        GeminiHttpRequest::Forward(request) => request,
    };
    let forward_id = uuid::Uuid::new_v4().to_string();

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
    bamboo_engine::message_hooks::apply_message_preflight_hooks(
        Some(state.session_store.as_ref() as &dyn bamboo_agent_core::storage::AttachmentReader),
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

    let provider = state.get_provider_for_endpoint("gemini").await?;
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
