mod non_stream;
mod output;
mod prepare;
mod stream;
mod usage;

use actix_web::{http::StatusCode, web, FromRequest, HttpMessage, HttpRequest, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::types::{ResponsesCreateRequest, ResponsesToolParam};

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

/// Only explicit receipt selectors choose the strict extension schema.
/// Ordinary Responses retain the original DTO, JSON limits and auth context.
pub enum ResponsesHttpRequest {
    Activation(ResponsesReceiptActivationBody),
    Forward(ResponsesCreateRequest),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponsesReceiptActivationBody {
    model: String,
    input: Vec<serde_json::Value>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    tools: Vec<ResponsesToolParam>,
}

impl FromRequest for ResponsesHttpRequest {
    type Error = actix_web::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>>>>;
    fn from_request(http: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        if crate::handlers::llm_compat::activation::requested(http) {
            let parsed = web::Json::<ResponsesReceiptActivationBody>::from_request(http, payload);
            Box::pin(async move {
                let body = parsed.await.map_err(|error| {
                    if error.as_response_error().status_code() != StatusCode::BAD_REQUEST {
                        return error;
                    }
                    actix_web::error::InternalError::from_response(error,
                        crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                            "unsupported_bamboo_activation_body",
                            "Receipt activation allows only model=bamboo-agent, empty input/tools and optional stream:false; null and other fields are rejected"))
                        .into()
                })?;
                Ok(Self::Activation(body.into_inner()))
            })
        } else {
            let parsed = web::Json::<ResponsesCreateRequest>::from_request(http, payload);
            Box::pin(async move { Ok(Self::Forward(parsed.await?.into_inner())) })
        }
    }
}

pub async fn responses_create(
    app_state: web::Data<AppState>,
    http_request: HttpRequest,
    req: ResponsesHttpRequest,
) -> Result<HttpResponse, AppError> {
    let request = match req {
        ResponsesHttpRequest::Activation(body) => {
            if body.model != "bamboo-agent"
                || !body.input.is_empty()
                || body.stream
                || !body.tools.is_empty()
            {
                return Ok(crate::handlers::llm_compat::activation::error(StatusCode::BAD_REQUEST,
                    "unsupported_bamboo_activation_body",
                    "Receipt activation requires model=bamboo-agent, empty input/tools and optional stream:false"));
            }
            // Keep the original HTTP extensions: a Responses model-proxy
            // credential must still fail the neutral verified User gate.
            return Ok(
                crate::handlers::llm_compat::activation::activate(app_state, http_request).await,
            );
        }
        ResponsesHttpRequest::Forward(request) => request,
    };
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
