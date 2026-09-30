//! Authenticated host-owner view. Root/subtree IDs are data selectors only.

use actix_web::{http::header, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ActorSnapshotError, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    ACTOR_SNAPSHOT_SCHEMA_VERSION,
};
use serde::Deserialize;

use crate::app_state::AppState;
use crate::handlers::settings::{bootstrap_access_snapshot, BootstrapRequestState};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    subtree_id: Option<String>,
}

fn failure(error: ActorSnapshotError) -> HttpResponse {
    use actix_web::http::StatusCode;
    let status = match error {
        ActorSnapshotError::InvalidSelector => StatusCode::BAD_REQUEST,
        ActorSnapshotError::NotFound => StatusCode::NOT_FOUND,
        ActorSnapshotError::UnauthorizedScope => StatusCode::FORBIDDEN,
        ActorSnapshotError::BudgetExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ActorSnapshotError::UnsupportedAuthority => StatusCode::NOT_IMPLEMENTED,
        ActorSnapshotError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::CONFLICT,
    };
    HttpResponse::build(status).json(serde_json::json!({
        "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
        "error": crate::error::error_value(error.to_string())
    }))
}

/// The same HostOwner gate is used by snapshot REST and lazy Actor WS channels.
/// Open-policy access and Codex run tokens do not confer subtree visibility.
pub(crate) async fn host_owner_authorized(state: &web::Data<AppState>, req: &HttpRequest) -> bool {
    let codex_token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .map(str::trim)
        .is_some_and(|token| token.starts_with("bcx1_"));
    if codex_token {
        return false;
    }
    let config = state.config.read().await;
    matches!(
        bootstrap_access_snapshot(&config, req).request_state,
        BootstrapRequestState::LocalBypass | BootstrapRequestState::Authenticated
    )
}

pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    // The existing middleware limits bcx credentials to Responses/models.
    // Retain that restriction even when this handler is mounted independently.
    if !host_owner_authorized(&state, &req).await {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
            "error": crate::error::error_value("host_authentication_required")
        }));
    }
    let query = match web::Query::<Query>::from_query(req.query_string()) {
        Ok(query) => query.into_inner(),
        Err(_) => return failure(ActorSnapshotError::InvalidSelector),
    };
    let root = path.into_inner();
    let subtree = query.subtree_id.as_deref().unwrap_or(&root);
    match state
        .session_store
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root,
            subtree,
            ActorSnapshotLimits::default(),
        )
        .await
    {
        Ok(snapshot) => {
            let etag = format!("\"{}\"", snapshot.snapshot_id);
            if req
                .headers()
                .get(header::IF_NONE_MATCH)
                .and_then(|h| h.to_str().ok())
                == Some(etag.as_str())
            {
                return HttpResponse::NotModified()
                    .insert_header((header::ETAG, etag))
                    .insert_header((header::CACHE_CONTROL, "private, no-cache"))
                    .finish();
            }
            HttpResponse::Ok()
                .insert_header((header::ETAG, etag))
                .insert_header((header::CACHE_CONTROL, "private, no-cache"))
                .json(snapshot)
        }
        Err(error) => failure(error),
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos", windows)))]
mod tests {
    use super::*;
    use crate::handlers::settings::issue_device_token;
    use actix_web::{http::StatusCode, test, App};
    use bamboo_config::AccessControlConfig;
    use bamboo_domain::{Session, Storage};
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority;

    #[actix_web::test]
    async fn canonical_gateway_requires_trusted_host_owner_and_keeps_codex_response_scope() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x37; 32]);
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
        let root = Session::new("public-root", "model");
        let child = Session::new_child_of("public-child", &root, "model", "Child");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.save_session(&child).await.unwrap();
        // A live WS replay window is not a durable coordinate for this
        // independent REST snapshot, including when both are in one process.
        let _live_actor = state.actor_event_hub.subscribe(&child.id, None).unwrap();
        let source_path = home.join("sessions/public-root/session.json");
        let source_before = tokio::fs::read(&source_path).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        let uri = "/api/v1/actors/public-root/snapshot?subtree_id=public-child";
        let local = test::TestRequest::get()
            .uri(uri)
            .peer_addr("127.0.0.1:1234".parse().unwrap())
            .insert_header((header::HOST, "localhost"));
        let response = test::call_service(&app, local.to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["nodes"].as_array().unwrap().len(), 1);
        assert!(body["stream_cursor"]
            .as_str()
            .is_some_and(|cursor| cursor.starts_with("at1-")));
        assert_eq!(body["nodes"][0]["actor_id"], child.id);
        assert_eq!(body["nodes"][0]["logical_state"], serde_json::Value::Null);
        assert_eq!(tokio::fs::read(source_path).await.unwrap(), source_before);
        assert!(!home
            .join("sessions/public-root/actor-authority.json")
            .exists());
        assert!(!home
            .join("sessions/public-root/children/public-child/actor-authority.json")
            .exists());

        let refresh = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header((header::IF_NONE_MATCH, etag))
                .to_request(),
        )
        .await;
        assert_eq!(refresh.status(), StatusCode::NOT_MODIFIED);
        let remote = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header(("x-forwarded-for", "127.0.0.1"))
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED); // Open policy is not HostOwner.

        let (device, token) = issue_device_token("snapshot-device");
        let device_id = device.device_id.clone();
        state.config.write().await.access_control = Some(AccessControlConfig {
            devices: vec![device],
            ..Default::default()
        });
        let authorized = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "bamboo.example.com"))
                .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
                .insert_header(("x-device-id", device_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(authorized.status(), StatusCode::OK);
        let invalid = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "bamboo.example.com"))
                .insert_header((header::AUTHORIZATION, "Bearer invalid"))
                .insert_header(("x-device-id", device_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
        let codex = state.codex_run_tokens.issue(&child.id).unwrap();
        // Independently mounted handler must preserve the same restriction as
        // the canonical middleware, including lowercase and token whitespace.
        let direct = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/api/v1/actors/{root_id}/snapshot", web::get().to(handler)),
        )
        .await;
        for prefix in ["Bearer ", "bearer ", "Bearer   "] {
            let request = || {
                test::TestRequest::get()
                    .uri(uri)
                    .peer_addr("127.0.0.1:1234".parse().unwrap())
                    .insert_header((header::HOST, "localhost"))
                    .insert_header((header::AUTHORIZATION, format!("{prefix}{}", codex.token)))
                    .to_request()
            };
            let denied = test::call_service(&direct, request()).await;
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
            let denied_body: serde_json::Value = test::read_body_json(denied).await;
            assert_eq!(
                denied_body,
                serde_json::json!({
                    "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
                    "error": crate::error::error_value("host_authentication_required")
                })
            );
            let canonical = test::call_service(&app, request()).await;
            assert_eq!(canonical.status(), StatusCode::UNAUTHORIZED);
        }
        let forged = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/actors/public-root/snapshot?requesterId=public-root")
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(forged.status(), StatusCode::BAD_REQUEST);
        let forged_body: serde_json::Value = test::read_body_json(forged).await;
        assert_eq!(
            forged_body,
            serde_json::json!({
                "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
                "error": crate::error::error_value("invalid_selector")
            })
        );
    }
}
