use actix_web::{web, HttpResponse};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{app_state::AppState, error::AppError};
use bamboo_infrastructure::LLMRequestOptions;

use super::super::helpers::now_unix_ts;
use super::PreparedResponsesRequest;
use worker::{spawn_stream_worker, StreamWorkerArgs};

mod errors;
mod events;
#[cfg(test)]
mod tests;
mod worker;

pub(super) async fn handle_streaming_response(
    app_state: web::Data<AppState>,
    prepared: PreparedResponsesRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let display_model = prepared
        .provider_name
        .as_ref()
        .map(|p| format!("{}/{}", p, prepared.resolved_model))
        .unwrap_or_else(|| prepared.resolved_model.clone());

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.responses",
        display_model.clone(),
        true,
        chrono::Utc::now(),
    );

    let provider = match &prepared.provider_name {
        Some(name) => {
            let model_ref = bamboo_domain::ProviderModelRef::new(name, &prepared.resolved_model);
            app_state.get_provider_for_model_ref(&model_ref)?
        }
        None => app_state.get_provider().await,
    };
    let request_options = LLMRequestOptions {
        session_id: None,
        reasoning_effort: prepared.reasoning_effort,
        parallel_tool_calls: prepared.parallel_tool_calls,
        responses: Some(prepared.responses_options.clone()),
        request_purpose: Some("openai_compat".to_string()),
        cache: None,
    };
    let stream_result = provider
        .chat_stream_with_options(
            &prepared.internal_messages,
            &prepared.internal_tools,
            prepared.max_tokens,
            prepared.resolved_model.as_str(),
            Some(&request_options),
        )
        .await
        .map_err(errors::map_provider_error)?;

    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, anyhow::Error>>(10);

    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let created_at = now_unix_ts();

    spawn_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        metrics: app_state.metrics_service.collector(),
        forward_id,
        fallback_response_id: format!("resp_{}", uuid::Uuid::new_v4()),
        message_id,
        created_at,
        resolved_model: display_model,
        estimated_prompt_tokens: prepared.estimated_prompt_tokens,
    });

    let stream = ReceiverStream::new(rx).map(|result| result.map_err(AppError::InternalError));

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}
