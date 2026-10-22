//! Explicit sensitive sharing. Bounded bodies and static diagnostics prevent
//! request payloads or passphrases entering URLs, logs, or public errors.

use std::sync::{Arc, LazyLock};

use actix_web::{
    dev::HttpServiceFactory, http::StatusCode, middleware::DefaultHeaders, web, HttpMessage,
    HttpRequest, HttpResponse,
};
use bamboo_config::config_share::{
    ShareDecisions, ShareError, ShareSelection, MAX_SHARE_STRING_BYTES,
};
use bamboo_config::config_share_api::{self, ShareApiError};
use bamboo_config::credential_migration::share_transaction::{
    read_share_operation, ShareRevisions,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::app_state::AppState;

const MAX_BODY_BYTES: usize = MAX_SHARE_STRING_BYTES + 64 * 1024;
// One KDF uses ~128 MiB. Reject concurrent requests without retaining a queue.
static SHARE_CRYPTO: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportRequest {
    selection: ShareSelection,
    #[serde(default)]
    include_secrets: bool,
    passphrase: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewRequest {
    share_string: String,
    passphrase: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyRequest {
    share_string: String,
    passphrase: Option<String>,
    choices: ShareDecisions,
    expected_revisions: ShareRevisions,
    preview_token: String,
    operation_id: Uuid,
}

#[derive(Serialize)]
struct ExportResponse {
    share_string: String,
}

pub fn config_share_routes() -> impl HttpServiceFactory {
    web::scope("/bamboo/config/share")
        .app_data(web::PayloadConfig::new(MAX_BODY_BYTES))
        .wrap(
            DefaultHeaders::new()
                .add(("Cache-Control", "no-store"))
                .add(("Pragma", "no-cache")),
        )
        .route("/export", web::post().to(export))
        .route("/preview", web::post().to(preview))
        .route("/apply", web::post().to(apply))
        .route("/operations/{operation_id}", web::get().to(operation))
}

fn failure(status: StatusCode, code: &'static str) -> HttpResponse {
    HttpResponse::build(status).insert_header(("Cache-Control", "no-store"))
        .json(serde_json::json!({"error":{"type":"configuration_share_error","code":code,"message":"Configuration sharing could not complete"}}))
}

fn parse<T: DeserializeOwned>(request: &HttpRequest, bytes: &[u8]) -> Result<T, Box<HttpResponse>> {
    // Match the existing JSON endpoints' browser preflight boundary. A
    // text/plain/form cross-origin simple request must not perform a write.
    if !request
        .content_type()
        .eq_ignore_ascii_case("application/json")
    {
        return Err(Box::new(failure(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "share_json_required",
        )));
    }
    if bytes.len() > MAX_BODY_BYTES {
        return Err(Box::new(failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            "share_size_limit",
        )));
    }
    serde_json::from_slice(bytes)
        .map_err(|_| Box::new(failure(StatusCode::BAD_REQUEST, "invalid_share_request")))
}

fn checked_input(share_string: &str, passphrase: Option<&str>) -> Result<(), Box<HttpResponse>> {
    if share_string.len() > MAX_SHARE_STRING_BYTES
        || passphrase.is_some_and(|value| value.len() > 1024)
    {
        return Err(Box::new(failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            "share_size_limit",
        )));
    }
    Ok(())
}

fn error_response(error: ShareApiError) -> HttpResponse {
    let (status, code) = match error {
        ShareApiError::Conflict => (StatusCode::CONFLICT, "share_preview_changed"),
        ShareApiError::RuntimeBoundary => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "automatic_provider_authentication_blocked",
        ),
        ShareApiError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "share_unavailable"),
        ShareApiError::Share(ShareError::Limit) => {
            (StatusCode::PAYLOAD_TOO_LARGE, "share_size_limit")
        }
        ShareApiError::Share(ShareError::Authentication) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "share_authentication_failed",
        ),
        ShareApiError::Share(_) | ShareApiError::Invalid => {
            (StatusCode::BAD_REQUEST, "invalid_share_request")
        }
    };
    failure(status, code)
}

async fn run<T, F>(operation: F) -> HttpResponse
where
    T: Serialize + Send + 'static,
    F: FnOnce() -> Result<T, ShareApiError> + Send + 'static,
{
    let Ok(permit) = SHARE_CRYPTO.clone().try_acquire_owned() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "share_busy");
    };
    // The blocking task owns the permit: aborting HTTP cannot free it while
    // the KDF or a committed transaction is still running.
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    {
        Ok(Ok(response)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(response),
        Ok(Err(error)) => error_response(error),
        Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "share_unavailable"),
    }
}

async fn export(
    state: web::Data<AppState>,
    request: HttpRequest,
    bytes: web::Bytes,
) -> HttpResponse {
    let request: ExportRequest = match parse(&request, &bytes) {
        Ok(value) => value,
        Err(error) => return *error,
    };
    if let Err(error) = checked_input("", request.passphrase.as_deref()) {
        return *error;
    }
    let data_dir = state.app_data_dir.clone();
    run(move || {
        config_share_api::export_config_share(
            &data_dir,
            &request.selection,
            request.include_secrets,
            request.passphrase.as_deref(),
        )
        .map(|share_string| ExportResponse { share_string })
    })
    .await
}

async fn preview(
    state: web::Data<AppState>,
    request: HttpRequest,
    bytes: web::Bytes,
) -> HttpResponse {
    let request: PreviewRequest = match parse(&request, &bytes) {
        Ok(value) => value,
        Err(error) => return *error,
    };
    if let Err(error) = checked_input(&request.share_string, request.passphrase.as_deref()) {
        return *error;
    }
    let data_dir = state.app_data_dir.clone();
    run(move || {
        config_share_api::preview_config_share(
            &data_dir,
            &request.share_string,
            request.passphrase.as_deref(),
        )
    })
    .await
}

async fn apply(
    state: web::Data<AppState>,
    request: HttpRequest,
    bytes: web::Bytes,
) -> HttpResponse {
    let request: ApplyRequest = match parse(&request, &bytes) {
        Ok(value) => value,
        Err(error) => return *error,
    };
    if let Err(error) = checked_input(&request.share_string, request.passphrase.as_deref()) {
        return *error;
    }
    let Ok(io_guard) = state.config_io_lock.clone().try_lock_owned() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "share_busy");
    };
    let data_dir = state.app_data_dir.clone();
    run(move || {
        let _io_guard = io_guard;
        config_share_api::apply_config_share(
            &data_dir,
            &request.share_string,
            request.passphrase.as_deref(),
            &request.choices,
            request.expected_revisions,
            &request.preview_token,
            request.operation_id,
        )
    })
    .await
}

async fn operation(state: web::Data<AppState>, operation_id: web::Path<String>) -> HttpResponse {
    let Ok(id) = Uuid::parse_str(&operation_id) else {
        return failure(StatusCode::BAD_REQUEST, "invalid_share_request");
    };
    let data_dir = state.app_data_dir.clone();
    // No KDF, recovery or writes. Query stays available while apply is running.
    match tokio::task::spawn_blocking(move || read_share_operation(&data_dir, id)).await {
        Ok(Ok(status)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(status),
        _ => failure(StatusCode::SERVICE_UNAVAILABLE, "share_unavailable"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_never_returns_sensitive_diagnostics() {
        let request = actix_web::test::TestRequest::post()
            .insert_header(("Content-Type", "application/json"))
            .to_http_request();
        assert!(parse::<PreviewRequest>(
            &request,
            br#"{"share_string":"synthetic-private-canary","unexpected":1}"#
        )
        .is_err());
        assert!(checked_input("safe", Some(&"x".repeat(1025))).is_err());
        assert!(parse::<PreviewRequest>(&request, &vec![b'x'; MAX_BODY_BYTES + 1]).is_err());
    }

    #[test]
    fn simple_cross_origin_mime_is_rejected_before_json_or_crypto() {
        let plain = actix_web::test::TestRequest::post()
            .insert_header(("Content-Type", "text/plain"))
            .to_http_request();
        let rejected = parse::<PreviewRequest>(&plain, br#"{"share_string":"synthetic-string"}"#)
            .err()
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let form = actix_web::test::TestRequest::post()
            .insert_header(("Content-Type", "application/x-www-form-urlencoded"))
            .to_http_request();
        assert_eq!(
            parse::<PreviewRequest>(&form, b"ignored")
                .err()
                .unwrap()
                .status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        let json = actix_web::test::TestRequest::post()
            .insert_header(("Content-Type", "Application/JSON; charset=utf-8"))
            .to_http_request();
        assert!(parse::<PreviewRequest>(&json, br#"{"share_string":"synthetic-string"}"#).is_ok());
    }

    #[actix_web::test]
    async fn concurrent_crypto_request_is_rejected_without_running_closure() {
        let permit = SHARE_CRYPTO.clone().try_acquire_owned().unwrap();
        let response = run::<(), _>(|| panic!("busy request must not run")).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("Cache-Control").unwrap(), "no-store");
        drop(permit);
    }
}
