//! Authenticated workbench API over the same page used by the agent tool.

use actix_web::{web, HttpResponse};
use base64::Engine as _;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::app_state::AppState;
use crate::browser::BrowserError;

fn error_response(error: BrowserError) -> HttpResponse {
    let (status, code) = match &error {
        BrowserError::Unavailable(_) => (
            actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
            "browser_unavailable",
        ),
        BrowserError::NotOpen => (actix_web::http::StatusCode::NOT_FOUND, "browser_not_open"),
        BrowserError::StaleEpoch => (actix_web::http::StatusCode::CONFLICT, "stale_epoch"),
        BrowserError::StaleDialog => (actix_web::http::StatusCode::CONFLICT, "stale_dialog"),
        BrowserError::DialogPending => (actix_web::http::StatusCode::CONFLICT, "dialog_pending"),
        BrowserError::Invalid(_) => (
            actix_web::http::StatusCode::BAD_REQUEST,
            "invalid_browser_request",
        ),
        BrowserError::Failed(_) => (
            actix_web::http::StatusCode::BAD_GATEWAY,
            "browser_action_failed",
        ),
    };
    HttpResponse::build(status)
        .json(json!({"error":{"message":error.to_string(),"type":"browser_error","code":code}}))
}

async fn known_session(state: &AppState, session_id: &str) -> bool {
    state
        .session_store
        .get_index_entry(session_id)
        .await
        .is_some()
}

fn missing_session() -> HttpResponse {
    HttpResponse::NotFound().json(json!({"error":{"message":"Session not found","type":"not_found","code":"session_not_found"}}))
}

fn valid_tab_id(tab_id: &str) -> bool {
    tab_id.len() == 24
        && tab_id
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

pub async fn open(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state.browser.open(&session_id).await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

pub async fn state(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state.browser.state(&session_id).await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

pub async fn close(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state.browser.close(&session_id).await {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TabCreateRequest {
    expected_epoch: u64,
    url: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TabTargetRequest {
    tab_id: String,
    expected_epoch: u64,
}

pub async fn tab_create(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<TabCreateRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    let mut args = json!({"expected_epoch":body.expected_epoch});
    if let Some(raw_url) = &body.url {
        match http_browser_url(raw_url) {
            Ok(url) => args["url"] = json!(url.as_str()),
            Err(error) => return error_response(error),
        }
    }
    match state.browser.command(&session_id, "tab_create", args).await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

pub async fn tab_activate(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<TabTargetRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    if !valid_tab_id(&body.tab_id) {
        return error_response(BrowserError::Invalid(
            "tab_id must be a 24-character lowercase hex ID".into(),
        ));
    }
    match state
        .browser
        .command(
            &session_id,
            "tab_activate",
            json!({"tab_id":body.tab_id,"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

pub async fn tab_close(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<TabTargetRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    if !valid_tab_id(&body.tab_id) {
        return error_response(BrowserError::Invalid(
            "tab_id must be a 24-character lowercase hex ID".into(),
        ));
    }
    match state
        .browser
        .command(
            &session_id,
            "tab_close",
            json!({"tab_id":body.tab_id,"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NavigateRequest {
    url: String,
    expected_epoch: u64,
}

fn http_browser_url(raw: &str) -> Result<url::Url, BrowserError> {
    match url::Url::parse(raw) {
        Ok(url)
            if matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none() =>
        {
            Ok(url)
        }
        _ => Err(BrowserError::Invalid(
            "navigation requires an http(s) URL without credentials".into(),
        )),
    }
}

pub async fn navigate(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<NavigateRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    let url = match http_browser_url(&body.url) {
        Ok(url) => url,
        Err(error) => return error_response(error),
    };
    match state
        .browser
        .command(
            &session_id,
            "navigate",
            json!({"url":url.as_str(),"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    direction: String,
    expected_epoch: u64,
}

pub async fn history(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<HistoryRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    if !matches!(body.direction.as_str(), "back" | "forward" | "reload") {
        return error_response(BrowserError::Invalid(
            "direction must be back, forward, or reload".into(),
        ));
    }
    match state
        .browser
        .command(
            &session_id,
            "history",
            json!({"direction":body.direction,"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewportRequest {
    width: u32,
    height: u32,
    expected_epoch: u64,
}

pub async fn viewport(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<ViewportRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    if !(320..=1200).contains(&body.width) || !(240..=1000).contains(&body.height) {
        return error_response(BrowserError::Invalid(
            "viewport outside 320..1200 by 240..1000".into(),
        ));
    }
    match state
        .browser
        .command(
            &session_id,
            "viewport",
            json!({"width":body.width,"height":body.height,"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputRequest {
    Click {
        x: f64,
        y: f64,
        button: Option<String>,
        expected_epoch: u64,
    },
    Scroll {
        x: f64,
        y: f64,
        delta_x: f64,
        delta_y: f64,
        expected_epoch: u64,
    },
    Type {
        text: String,
        expected_epoch: u64,
    },
    Key {
        key: String,
        expected_epoch: u64,
    },
}

pub async fn input(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<InputRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    let args = match body.into_inner() {
        InputRequest::Click {
            x,
            y,
            button,
            expected_epoch,
        } => {
            if !button
                .as_deref()
                .is_none_or(|button| matches!(button, "left" | "right" | "middle"))
            {
                return error_response(BrowserError::Invalid("invalid mouse button".into()));
            }
            json!({"kind":"click","x":x,"y":y,"button":button,"expected_epoch":expected_epoch})
        }
        InputRequest::Scroll {
            x,
            y,
            delta_x,
            delta_y,
            expected_epoch,
        } => {
            json!({"kind":"scroll","x":x,"y":y,"delta_x":delta_x,"delta_y":delta_y,"expected_epoch":expected_epoch})
        }
        InputRequest::Type {
            text,
            expected_epoch,
        } => json!({"kind":"type","text":text,"expected_epoch":expected_epoch}),
        InputRequest::Key {
            key,
            expected_epoch,
        } => json!({"kind":"key","key":key,"expected_epoch":expected_epoch}),
    };
    match state.browser.command(&session_id, "input", args).await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogResponseRequest {
    dialog_id: String,
    accept: bool,
    text: Option<String>,
    expected_epoch: u64,
}

pub async fn dialog_respond(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<DialogResponseRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    if !valid_tab_id(&body.dialog_id)
        || body
            .text
            .as_ref()
            .is_some_and(|text| text.encode_utf16().count() > 4096)
        || (!body.accept && body.text.is_some())
    {
        return error_response(BrowserError::Invalid("invalid dialog response".into()));
    }
    match state
        .browser
        .command(
            &session_id,
            "dialog_respond",
            json!({"dialog_id":body.dialog_id,"accept":body.accept,"text":body.text,"expected_epoch":body.expected_epoch}),
        )
        .await
    {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => error_response(error),
    }
}

pub async fn dom(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state.browser.command(&session_id, "dom", json!({})).await {
        Ok(value) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(value),
        Err(error) => error_response(error),
    }
}

#[derive(Deserialize)]
pub struct FrameQuery {
    after: Option<u64>,
    wait_ms: Option<u64>,
}

pub async fn frame(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<FrameQuery>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state
        .browser
        .frame(
            &session_id,
            query.after.unwrap_or(0),
            query.wait_ms.unwrap_or(0),
        )
        .await
    {
        Ok(Some(frame)) => HttpResponse::Ok()
            .insert_header(("Content-Type", "image/jpeg"))
            .insert_header(("Cache-Control", "no-store"))
            .insert_header(("X-Frame-Seq", frame.frame_seq.to_string()))
            .insert_header(("X-Tab-Id", frame.tab_id.clone()))
            .insert_header(("X-Page-Epoch", frame.page_epoch.to_string()))
            .insert_header(("X-Viewport-Width", frame.viewport_width.to_string()))
            .insert_header(("X-Viewport-Height", frame.viewport_height.to_string()))
            .body(frame.jpeg.to_vec()),
        Ok(None) => HttpResponse::NoContent()
            .insert_header(("Cache-Control", "no-store"))
            .finish(),
        Err(error) => error_response(error),
    }
}

pub async fn screenshot(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    let session_id = path.into_inner();
    if !known_session(&state, &session_id).await {
        return missing_session();
    }
    match state
        .browser
        .command(&session_id, "screenshot", json!({}))
        .await
    {
        Ok(value) => {
            let Some(data) = value.get("data").and_then(Value::as_str) else {
                return error_response(BrowserError::Failed("screenshot data missing".into()));
            };
            let Some(tab_id) = value.get("active_tab_id").and_then(Value::as_str) else {
                return error_response(BrowserError::Failed("screenshot tab missing".into()));
            };
            let Ok(jpeg) = base64::engine::general_purpose::STANDARD.decode(data) else {
                return error_response(BrowserError::Failed("screenshot data invalid".into()));
            };
            HttpResponse::Ok()
                .insert_header(("Content-Type", "image/jpeg"))
                .insert_header(("Cache-Control", "no-store"))
                .insert_header(("X-Tab-Id", tab_id))
                .insert_header(("X-Page-Epoch", value["page_epoch"].to_string()))
                .insert_header(("X-Viewport-Width", value["viewport"]["width"].to_string()))
                .insert_header(("X-Viewport-Height", value["viewport"]["height"].to_string()))
                .body(jpeg)
        }
        Err(error) => error_response(error),
    }
}
