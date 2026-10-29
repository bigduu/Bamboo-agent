//! Authenticated, session-lifetime-scoped access to bounded local OTel spans.
use actix_web::{
    http::{header, StatusCode},
    web, HttpRequest, HttpResponse,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::app_state::AppState;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    limit: Option<usize>,
    cursor: Option<String>,
}

fn error(status: StatusCode, code: &'static str) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header((header::CACHE_CONTROL, "private, no-store"))
        .json(json!({"schema_version": 1, "error": crate::error::error_value(code)}))
}

async fn lifetime(
    state: &AppState,
    session_id: &str,
) -> Result<DateTime<Utc>, (StatusCode, &'static str)> {
    match state.storage.load_session(session_id).await {
        Ok(Some(session)) if session.id == session_id => Ok(session.created_at),
        Ok(None) => Err((StatusCode::NOT_FOUND, "session_not_found")),
        Ok(Some(_)) => Err((StatusCode::CONFLICT, "session_identity_conflict")),
        Err(_) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "session_storage_unavailable",
        )),
    }
}

pub async fn list(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    if !super::actor_snapshot::host_owner_authorized(&state, &req).await {
        return error(StatusCode::UNAUTHORIZED, "host_authentication_required");
    }
    let query = match web::Query::<Query>::from_query(req.query_string()) {
        Ok(value) => value.into_inner(),
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_trace_query"),
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return error(StatusCode::BAD_REQUEST, "invalid_trace_limit");
    }
    let cursor = match query.cursor {
        Some(value)
            if value.len() <= 20
                && !value.is_empty()
                && value.bytes().all(|b| b.is_ascii_digit()) =>
        {
            match value.parse::<u64>() {
                Ok(value) if value > 0 => Some(value),
                _ => return error(StatusCode::BAD_REQUEST, "invalid_trace_cursor"),
            }
        }
        Some(_) => return error(StatusCode::BAD_REQUEST, "invalid_trace_cursor"),
        None => None,
    };
    let session_id = path.into_inner();
    let birth = match lifetime(&state, &session_id).await {
        Ok(value) => value,
        Err((status, code)) => return error(status, code),
    };
    HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "private, no-store"))
        .json(
            bamboo_infrastructure::telemetry::local::store().page_scoped(
                Some(&bamboo_infrastructure::telemetry::local::scope_for_path(
                    &state.app_data_dir,
                )),
                &session_id,
                birth,
                limit,
                cursor,
            ),
        )
}

pub async fn detail(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    req: HttpRequest,
) -> HttpResponse {
    if !super::actor_snapshot::host_owner_authorized(&state, &req).await {
        return error(StatusCode::UNAUTHORIZED, "host_authentication_required");
    }
    if !req.query_string().is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_trace_query");
    }
    let (session_id, trace_id) = path.into_inner();
    if trace_id.len() != 32
        || !trace_id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return error(StatusCode::BAD_REQUEST, "invalid_trace_id");
    }
    let birth = match lifetime(&state, &session_id).await {
        Ok(value) => value,
        Err((status, code)) => return error(status, code),
    };
    let Some(trace) = bamboo_infrastructure::telemetry::local::store().detail_scoped(
        Some(&bamboo_infrastructure::telemetry::local::scope_for_path(
            &state.app_data_dir,
        )),
        &session_id,
        birth,
        &trace_id,
    ) else {
        return error(StatusCode::NOT_FOUND, "trace_not_found");
    };
    HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "private, no-store"))
        .json(json!({"schema_version": 1, "session_id": session_id, "trace": trace}))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_domain::{ModelTokenUsage, Session, Storage};
    use bamboo_infrastructure::telemetry::{test_support::LocalTraceTest, Observation, Outcome};

    #[actix_web::test]
    async fn authenticates_and_isolates_real_local_spans_with_pagination() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5a; 32]);
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().canonicalize().unwrap())
                .await
                .unwrap(),
        );
        let one = Session::new("trace-api-one", "synthetic-model");
        let two = Session::new("trace-api-two", "synthetic-model");
        let unicode = Session::new("用户 会话", "synthetic-model");
        state.session_store.save_session(&one).await.unwrap();
        state.session_store.save_session(&two).await.unwrap();
        state.session_store.save_session(&unicode).await.unwrap();
        let other_temp = tempfile::tempdir().unwrap();
        let other_state = web::Data::new(
            AppState::new(other_temp.path().canonicalize().unwrap())
                .await
                .unwrap(),
        );
        // Even an identical session id and birth under a different data
        // authority must never expose this server's observations.
        other_state.session_store.save_session(&one).await.unwrap();
        let tracer = LocalTraceTest::global();
        tracer
            .scope(async {
                for _ in 0..2 {
                    let root = Observation::agent(&one.id, &one.id)
                        .bind_session_lifetime(&one.id, one.created_at)
                        .bind_local_scope(&state.app_data_dir);
                    root.scope(async {
                        let llm = Observation::llm(Some("openai"), "synthetic-model", &one.id);
                        llm.model_usage(&ModelTokenUsage {
                            input_tokens: Some(100),
                            cache_read_input_tokens: Some(0),
                            ..Default::default()
                        });
                        llm.finish(Outcome::Success);
                    })
                    .await;
                    root.finish(Outcome::Success);
                }
                Observation::agent(&two.id, &two.id)
                    .bind_session_lifetime(&two.id, two.created_at)
                    .bind_local_scope(&state.app_data_dir)
                    .finish(Outcome::Success);
                Observation::agent(&unicode.id, &unicode.id)
                    .bind_session_lifetime(&unicode.id, unicode.created_at)
                    .bind_local_scope(&state.app_data_dir)
                    .finish(Outcome::Success);
                Observation::agent(&one.id, &one.id)
                    .bind_session_lifetime(&one.id, one.created_at)
                    .bind_local_scope(&other_state.app_data_dir)
                    .finish(Outcome::Success);
            })
            .await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        let uri = "/api/v1/sessions/trace-api-one/traces?limit=1";
        let request = |uri: &str| {
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request()
        };
        let remote = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED);
        let response = test::call_service(&app, request(uri)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "private, no-store"
        );
        let page: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(page["schema_version"], 1);
        assert_eq!(page["collection_state"], "enabled");
        assert_eq!(page["traces"].as_array().unwrap().len(), 1);
        let trace_id = page["traces"][0]["trace_id"].as_str().unwrap();
        let cursor = page["next_cursor"].as_str().unwrap();
        let second: serde_json::Value = test::read_body_json(
            test::call_service(
                &app,
                request(&format!(
                    "/api/v1/sessions/trace-api-one/traces?limit=1&cursor={cursor}"
                )),
            )
            .await,
        )
        .await;
        assert_ne!(second["traces"][0]["trace_id"], trace_id);
        let other_app = test::init_service(
            App::new()
                .app_data(other_state)
                .configure(crate::agent_routes),
        )
        .await;
        let other_page: serde_json::Value = test::read_body_json(
            test::call_service(&other_app, request("/api/v1/sessions/trace-api-one/traces")).await,
        )
        .await;
        assert_eq!(other_page["traces"].as_array().unwrap().len(), 1);
        assert_ne!(other_page["traces"][0]["trace_id"], trace_id);
        assert_eq!(
            test::call_service(
                &other_app,
                request(&format!("/api/v1/sessions/trace-api-one/traces/{trace_id}"))
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        let detail_uri = format!("/api/v1/sessions/trace-api-one/traces/{trace_id}");
        let detail_response = test::call_service(&app, request(&detail_uri)).await;
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail: serde_json::Value = test::read_body_json(detail_response).await;
        let spans = detail["trace"]["spans"].as_array().unwrap();
        let llm = spans.iter().find(|span| span["kind"] == "llm").unwrap();
        assert_eq!(llm["usage"]["cache_read_input_tokens"], 0);
        assert!(llm["usage"]["cache_creation_input_tokens"].is_null());
        let other_uri = format!("/api/v1/sessions/trace-api-two/traces/{trace_id}");
        assert_eq!(
            test::call_service(&app, request(&other_uri)).await.status(),
            StatusCode::NOT_FOUND
        );
        let unicode_path = "/api/v1/sessions/%E7%94%A8%E6%88%B7%20%E4%BC%9A%E8%AF%9D/traces";
        let unicode_page: serde_json::Value =
            test::read_body_json(test::call_service(&app, request(unicode_path)).await).await;
        assert_eq!(unicode_page["session_id"], unicode.id);
        assert_eq!(unicode_page["traces"].as_array().unwrap().len(), 1);
        let unicode_trace_id = unicode_page["traces"][0]["trace_id"].as_str().unwrap();
        assert_eq!(
            test::call_service(&app, request(&format!("{unicode_path}/{unicode_trace_id}")))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            test::call_service(&app, request(&format!("{unicode_path}/{trace_id}")))
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            test::call_service(
                &app,
                request("/api/v1/sessions/trace-api-one/traces?limit=101")
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
        let missing = test::call_service(&app, request("/api/v1/sessions/missing/traces")).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            missing.headers().get(header::CACHE_CONTROL).unwrap(),
            "private, no-store"
        );
        let missing: serde_json::Value = test::read_body_json(missing).await;
        assert_eq!(missing["schema_version"], 1);
        assert_eq!(missing["error"]["message"], "session_not_found");
    }
}
