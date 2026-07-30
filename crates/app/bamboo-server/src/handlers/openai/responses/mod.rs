mod non_stream;
mod output;
mod prepare;
mod stream;
mod usage;

use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::types::ResponsesCreateRequest;

pub(super) struct PreparedResponsesRequest {
    pub(super) resolved_model: String,
    pub(super) provider_name: Option<String>,
    pub(super) internal_messages: Vec<bamboo_agent_core::Message>,
    pub(super) internal_tools: Vec<bamboo_agent_core::tools::ToolSchema>,
    pub(super) max_tokens: Option<u32>,
    pub(super) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(super) parallel_tool_calls: Option<bool>,
    pub(super) responses_options: bamboo_llm::provider::ResponsesRequestOptions,
    pub(super) estimated_prompt_tokens: u64,
    pub(super) request_session_id: Option<String>,
}

pub async fn responses_create(
    app_state: web::Data<AppState>,
    http_request: HttpRequest,
    req: web::Json<ResponsesCreateRequest>,
) -> Result<HttpResponse, AppError> {
    let request = req.into_inner();
    let stream = request.stream.unwrap_or(false);
    let forward_id = uuid::Uuid::new_v4().to_string();

    let mut prepared = prepare::prepare_request(&app_state, request).await?;
    prepared.request_session_id = http_request
        .extensions()
        .get::<crate::codex_run_tokens::CodexRunAuthContext>()
        .map(|context| context.session_id.clone());

    if stream {
        stream::handle_streaming_response(app_state, prepared, forward_id).await
    } else {
        non_stream::handle_non_streaming_response(app_state, prepared, forward_id).await
    }
}
