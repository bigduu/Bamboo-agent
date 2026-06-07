use actix_web::{web, HttpResponse};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{app_state::AppState, error::AppError};

use super::PreparedChatRequest;
use bamboo_llm::LLMRequestOptions;
use sse::wrap_sse_data;
use worker::{spawn_stream_worker, StreamWorkerArgs};

mod sse;
#[cfg(test)]
mod tests;
mod worker;

pub(super) async fn handle_streaming_chat(
    app_state: web::Data<AppState>,
    prepared: PreparedChatRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let PreparedChatRequest {
        resolved_model,
        provider_name,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        estimated_prompt_tokens,
        ..
    } = prepared;

    let display_model = provider_name
        .as_ref()
        .map(|p| format!("{}/{}", p, resolved_model))
        .unwrap_or_else(|| resolved_model.clone());

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.chat_completions",
        display_model.clone(),
        true,
        chrono::Utc::now(),
    );

    let provider = match &provider_name {
        Some(name) => {
            let model_ref = bamboo_domain::ProviderModelRef::new(name, &resolved_model);
            app_state.get_provider_for_model_ref(&model_ref)?
        }
        None => app_state.get_provider_for_endpoint("openai").await?,
    };

    let stream_result = provider
        .chat_stream_with_options(
            &internal_messages,
            &internal_tools,
            max_tokens,
            resolved_model.as_str(),
            Some(&LLMRequestOptions {
                session_id: None,
                reasoning_effort,
                parallel_tool_calls,
                responses: None,
                request_purpose: Some("openai_compat".to_string()),
                cache: None,
            }),
        )
        .await
        .map_err(super::map_provider_error)?;

    let (tx, rx) = mpsc::channel(10);

    spawn_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        model: display_model,
        metrics: app_state.metrics_service.collector(),
        forward_id,
        estimated_prompt_tokens,
    });

    let stream = ReceiverStream::new(rx)
        .map(|result| result.map(wrap_sse_data).map_err(AppError::InternalError));

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}
