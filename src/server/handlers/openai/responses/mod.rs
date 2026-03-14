mod non_stream;
mod output;
mod prepare;
mod stream;

use actix_web::{web, HttpResponse};

use crate::server::{app_state::AppState, error::AppError};

use super::types::ResponsesCreateRequest;

pub(super) struct PreparedResponsesRequest {
    pub(super) resolved_model: String,
    pub(super) internal_messages: Vec<crate::agent::core::Message>,
    pub(super) internal_tools: Vec<crate::agent::core::tools::ToolSchema>,
    pub(super) max_tokens: Option<u32>,
    pub(super) estimated_prompt_tokens: u64,
}

pub async fn responses_create(
    app_state: web::Data<AppState>,
    req: web::Json<ResponsesCreateRequest>,
) -> Result<HttpResponse, AppError> {
    let request = req.into_inner();
    let stream = request.stream.unwrap_or(false);
    let forward_id = uuid::Uuid::new_v4().to_string();

    let prepared = prepare::prepare_request(&app_state, request).await?;

    if stream {
        stream::handle_streaming_response(app_state, prepared, forward_id).await
    } else {
        non_stream::handle_non_streaming_response(app_state, prepared, forward_id).await
    }
}
