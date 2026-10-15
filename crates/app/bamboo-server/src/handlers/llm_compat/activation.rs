//! Protocol-neutral receipt activation glue over the existing Native HTTP port.
//! No vendor transcript, independent claim, cache or lifecycle controller.
use crate::{
    app_state::AppState,
    handlers::agent::{execute, tickets},
};
use actix_web::{
    body::to_bytes,
    http::{header, StatusCode},
    web, HttpRequest, HttpResponse, ResponseError,
};

pub(crate) const MODE: &str = "Bamboo-Activation";
pub(crate) const SESSION: &str = "Bamboo-Session-Id";
const SELECTORS: [&str; 4] = [
    "Bamboo-Input-Id",
    "Bamboo-Ingress-Seq",
    "Bamboo-Session-Created-At",
    "Idempotency-Key",
];

pub(crate) fn requested(http: &HttpRequest) -> bool {
    [MODE, SESSION, SELECTORS[0], SELECTORS[1], SELECTORS[2]]
        .iter()
        .any(|name| http.headers().contains_key(*name))
}
pub(crate) fn error(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    HttpResponse::build(status).insert_header((header::CACHE_CONTROL,"no-store"))
        .json(serde_json::json!({"error":{"type":"bamboo_activation_error","code":code,"message":message}}))
}
pub(crate) fn unsupported() -> HttpResponse {
    error(
        StatusCode::NOT_IMPLEMENTED,
        "bamboo_activation_not_supported",
        "Receipt activation is not supported by this endpoint",
    )
}
fn no_store(mut response: HttpResponse) -> HttpResponse {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}
fn session_id(http: &HttpRequest) -> Option<String> {
    if http.headers().get_all(MODE).count() != 1
        || http.headers().get(MODE)?.to_str().ok()? != "receipt"
        || http.headers().get_all(SESSION).count() != 1
        || SELECTORS
            .iter()
            .any(|name| http.headers().get_all(*name).count() != 1)
    {
        return None;
    }
    let id = http.headers().get(SESSION)?.to_str().ok()?;
    // A deliberately bounded URL-safe subset of existing session identities.
    (!id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then(|| id.to_owned())
}

pub(crate) async fn activate(state: web::Data<AppState>, http: HttpRequest) -> HttpResponse {
    let Some(id) = session_id(&http) else {
        return error(StatusCode::BAD_REQUEST,"invalid_bamboo_activation","Receipt activation requires single mode=receipt, URL-safe session id, input id, positive ingress seq, expected Root birth and idempotency key headers");
    };
    // A provider/Worker-scoped credential must never become a User execution
    // capability merely by reaching its allowed model-proxy endpoint.
    if let Err(reason) = tickets::user(&state, &http).await {
        return no_store(tickets::TicketHttpError::from(reason).error_response());
    }
    let request = execute::ExecuteRequest {
        model: None,
        provider: None,
        model_ref: None,
        skill_mode: None,
        reasoning_effort: None,
        client_sync: None,
        no_human_approver: false,
        run_budget: None,
    };
    // Keep Native validation, the single execute idempotency namespace and
    // actual selected Root reservation/claim/Ready/spawn exactly authoritative.
    let response =
        execute::handler(state, http, web::Path::from(id.clone()), web::Json(request)).await;
    if response.status() != StatusCode::ACCEPTED {
        return no_store(response);
    }
    let (head, body) = response.into_parts();
    let Ok(bytes) = to_bytes(body).await else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_native_activation_response",
            "Native activation response could not be read",
        );
    };
    let Ok(native) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_native_activation_response",
            "Native activation response could not be decoded",
        );
    };
    let Some(run) = native["run_id"].as_str().filter(|run| {
        !run.is_empty()
            && run.len() <= 128
            && run
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_native_activation_response",
            "Native activation response has no exact run identity",
        );
    };
    let input = head
        .headers()
        .get(SELECTORS[0])
        .and_then(|v| v.to_str().ok());
    let generation = head
        .headers()
        .get(SELECTORS[1])
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0);
    let birth = head
        .headers()
        .get(SELECTORS[2])
        .and_then(|v| v.to_str().ok());
    let (Some(input), Some(generation), Some(birth)) = (input, generation, birth) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_native_activation_response",
            "Native activation response has no bound input receipt",
        );
    };
    if native["session_id"] != id || native["status"] != "started" {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_native_activation_response",
            "Native activation response target or disposition differs",
        );
    }
    let mut response = HttpResponse::Accepted();
    for (name, value) in head.headers() {
        if name != header::CONTENT_LENGTH
            && name != header::CONTENT_TYPE
            && name != header::TRANSFER_ENCODING
        {
            response.insert_header((name.clone(), value.clone()));
        }
    }
    response.insert_header((header::CACHE_CONTROL,"no-store")).json(serde_json::json!({
        "object":"bamboo.activation","status":"started","session_id":id,"run_id":run,
        "session_created_at":birth,"input":{"message_id":input,"ingress_seq":generation},
        "output_scope":"visible_messages","output_url":format!("/api/v1/sessions/{id}/runs/{run}/output"),
        "cancel_url":format!("/api/v1/sessions/{id}/stop"),"cancel_header":{"Bamboo-Run-Id":run}
    }))
}
