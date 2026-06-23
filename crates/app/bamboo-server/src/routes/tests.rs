use actix_web::http::{header, StatusCode};
use actix_web::{test, web, App};
use tempfile::tempdir;

use super::{configure_routes, configure_routes_with_rate_limiting};
use crate::AppState;
use bamboo_config::AccessControlConfig;

#[actix_web::test]
async fn configure_routes_registers_expected_api_prefixes() {
    let app = test::init_service(App::new().configure(configure_routes)).await;

    let requests = vec![
        ("GET", "/api/v1/health"),
        ("POST", "/api/v1/sessions/example/project-dream/run"),
        ("GET", "/api/v1/sessions/example/discoverable-tools"),
        ("POST", "/api/v1/sessions/example/discoverable-tools"),
        ("DELETE", "/api/v1/sessions/example/discoverable-tools"),
        ("GET", "/v1/bamboo/workflows"),
        ("GET", "/v1/bamboo/access/status"),
        ("GET", "/openai/v1/models"),
        ("GET", "/anthropic/v1/models"),
        ("GET", "/gemini/v1beta/models"),
    ];

    for (method, uri) in requests {
        let req = match method {
            "POST" => test::TestRequest::post().uri(uri).to_request(),
            "DELETE" => test::TestRequest::delete().uri(uri).to_request(),
            _ => test::TestRequest::get().uri(uri).to_request(),
        };
        let resp = test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected route to be registered: {method} {uri}"
        );
    }
}

#[actix_web::test]
async fn configure_routes_with_rate_limiting_registers_expected_api_prefixes() {
    let app = test::init_service(App::new().configure(configure_routes_with_rate_limiting)).await;

    let requests = vec![
        ("GET", "/api/v1/health"),
        ("POST", "/api/v1/sessions/example/project-dream/run"),
        ("GET", "/api/v1/sessions/example/discoverable-tools"),
        ("POST", "/api/v1/sessions/example/discoverable-tools"),
        ("DELETE", "/api/v1/sessions/example/discoverable-tools"),
        ("GET", "/v1/bamboo/workflows"),
        ("GET", "/v1/bamboo/access/status"),
        ("GET", "/openai/v1/models"),
        ("GET", "/anthropic/v1/models"),
        ("GET", "/gemini/v1beta/models"),
    ];

    for (method, uri) in requests {
        let req = match method {
            "POST" => test::TestRequest::post().uri(uri).to_request(),
            "DELETE" => test::TestRequest::delete().uri(uri).to_request(),
            _ => test::TestRequest::get().uri(uri).to_request(),
        };
        let resp = test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected route to be registered: {method} {uri}"
        );
    }
}

#[actix_web::test]
async fn remote_unverified_request_is_blocked_by_access_middleware() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(AccessControlConfig {
            password_enabled: true,
            password_hash: Some(
                "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
            ),
            password_salt: Some("01010101010101010101010101010101".to_string()),
            updated_at: None,
            devices: Vec::new(),
        });
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn access_bootstrap_endpoints_remain_public() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(AccessControlConfig {
            password_enabled: true,
            password_hash: Some(
                "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
            ),
            password_salt: Some("01010101010101010101010101010101".to_string()),
            updated_at: None,
            devices: Vec::new(),
        });
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    for req in [
        test::TestRequest::get()
            .uri("/v1/bamboo/access/status")
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_request(),
        test::TestRequest::get()
            .uri("/api/v1/health")
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_request(),
    ] {
        let resp = test::call_service(&app, req).await;
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}

#[actix_web::test]
async fn verified_cookie_allows_remote_request_through_middleware() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(AccessControlConfig {
            password_enabled: true,
            password_hash: Some(
                "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
            ),
            password_salt: Some("01010101010101010101010101010101".to_string()),
            updated_at: None,
            devices: Vec::new(),
        });
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let verify_req = test::TestRequest::post()
        .uri("/v1/bamboo/access/verify")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "password": "secret" }))
        .to_request();
    let verify_resp = test::call_service(&app, verify_req).await;
    assert_eq!(verify_resp.status(), StatusCode::OK);

    let set_cookie = verify_resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("verify response should set cookie")
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let protected_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::COOKIE, set_cookie))
        .to_request();
    let protected_resp = test::call_service(&app, protected_req).await;
    assert_eq!(protected_resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn system_prompt_snapshot_route_returns_project_dream_over_http() {
    let data_dir = tempdir().unwrap();
    bamboo_config::paths::init_bamboo_dir(data_dir.path().to_path_buf());
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());

    let mut session = bamboo_agent_core::Session::new("session-http-project-dream", "gpt-5");
    session.add_message(bamboo_agent_core::Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Project Dream Summary\n````md\nHTTP project dream content\n````\n\n### Session Memory Note (markdown)\n````md\nHTTP session note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
    ));
    app_state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::get()
        .uri("/api/v1/sessions/session-http-project-dream/system-prompt")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = actix_web::body::to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("parse response payload");

    assert_eq!(
        payload["dream_notebook"],
        serde_json::json!("HTTP project dream content")
    );
    assert_eq!(
        payload["project_dream"],
        serde_json::json!("HTTP project dream content")
    );
    assert_eq!(
        payload["session_memory_note"],
        serde_json::json!("HTTP session note content")
    );
    assert!(payload.get("global_dream_fallback").is_none());
    assert!(payload["external_memory"]
        .as_str()
        .unwrap_or_default()
        .contains("### Project Dream Summary"));
}

#[actix_web::test]
async fn local_request_bypasses_access_middleware() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(AccessControlConfig {
            password_enabled: true,
            password_hash: Some(
                "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
            ),
            password_salt: Some("01010101010101010101010101010101".to_string()),
            updated_at: None,
            devices: Vec::new(),
        });
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn dev_reset_route_registered_when_dev_endpoints_enabled() {
    // Under `cargo test` (a debug build) dev endpoints are enabled, so the route is
    // wired and matches (non-404). In a release build with no
    // BAMBOO_ENABLE_DEV_ENDPOINTS it is absent — the production gate is covered by
    // `routes::agent`'s `dev_endpoints_env_gate_*` unit test.
    let app = test::init_service(App::new().configure(configure_routes)).await;
    let req = test::TestRequest::post()
        .uri("/api/v1/dev/reset")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_ne!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "dev/reset must be registered when dev endpoints are enabled"
    );
}

// --- v2-P1 (#181): `GET /v2/stream` unified WS multiplex ------------------

/// The `/v2/stream` route is registered and reaches its handler. A plain GET
/// (no WebSocket upgrade headers) is NOT a websocket request, so `actix_ws::handle`
/// rejects it with `400 Bad Request` — which proves the route is wired through
/// the `/v2` scope (a missing route would be `404`, a blocked one `401/426`).
#[actix_web::test]
async fn v2_stream_route_is_registered_and_reaches_handler() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    // Loopback peer (default test peer is 127.0.0.1) → `local_bypass`, so the
    // access middleware lets the request through to the handler.
    let req = test::TestRequest::get().uri("/v2/stream").to_request();
    let resp = test::call_service(&app, req).await;

    assert_ne!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "/v2/stream must be registered"
    );
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a non-WebSocket GET reaches the handler and is rejected by actix_ws::handle"
    );
}

/// `/v2/stream` is behind the SAME access-password middleware as `/api/v1`: a
/// remote (non-loopback) request with a password configured and no verified
/// cookie is blocked with `401` BEFORE reaching the WS handler.
#[actix_web::test]
async fn v2_stream_is_behind_access_middleware() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(AccessControlConfig {
            password_enabled: true,
            password_hash: Some(
                "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
            ),
            password_salt: Some("01010101010101010101010101010101".to_string()),
            updated_at: None,
            devices: Vec::new(),
        });
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::get()
        .uri("/v2/stream")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/v2/stream must be guarded by the access middleware for remote requests"
    );
}

// --- v2-P2 (#181): per-device token pairing + enforcement -----------------

/// The hash of password "secret" with salt `01..01` (matches the literals above).
const SECRET_HASH: &str = "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d";
const SECRET_SALT: &str = "01010101010101010101010101010101";

fn password_access_control() -> AccessControlConfig {
    AccessControlConfig {
        password_enabled: true,
        password_hash: Some(SECRET_HASH.to_string()),
        password_salt: Some(SECRET_SALT.to_string()),
        updated_at: None,
        devices: Vec::new(),
    }
}

/// `POST /v2/pair` with the correct root password issues a device token whose
/// hash (not the plaintext) is persisted; the token then authenticates a remote
/// request through the access middleware.
#[actix_web::test]
async fn v2_pair_issues_token_that_authenticates_remote_request() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Pair (public route; self-gates on root password).
    let pair_req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "root_password": "secret", "label": "iPhone 15" }))
        .to_request();
    let pair_resp = test::call_service(&app, pair_req).await;
    assert_eq!(pair_resp.status(), StatusCode::OK);
    let body = actix_web::body::to_bytes(pair_resp.into_body())
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = payload["device_id"].as_str().unwrap().to_string();
    let device_token = payload["device_token"].as_str().unwrap().to_string();
    assert!(device_token.starts_with("bd1_"));
    assert!(device_id.starts_with("bamboo_"));

    // The plaintext token is NEVER persisted — only the hash.
    {
        let config = app_state.config.read().await;
        let devices = &config.access_control.as_ref().unwrap().devices;
        assert_eq!(devices.len(), 1);
        assert_ne!(devices[0].token_hash, device_token);
    }

    // The token authenticates a remote request through the middleware.
    let ok_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {device_token}")))
        .insert_header(("X-Device-Id", device_id))
        .to_request();
    let ok_resp = test::call_service(&app, ok_req).await;
    assert_eq!(ok_resp.status(), StatusCode::OK);

    // A bogus token is rejected.
    let bad_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, "Bearer bd1_deadbeef"))
        .insert_header(("X-Device-Id", "bamboo_000000000000"))
        .to_request();
    let bad_resp = test::call_service(&app, bad_req).await;
    assert_eq!(bad_resp.status(), StatusCode::UNAUTHORIZED);
}

/// `POST /v2/pair` with a wrong root password is rejected with 401.
#[actix_web::test]
async fn v2_pair_rejects_wrong_root_password() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "root_password": "wrong", "label": "x" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// `POST /v2/pair` when no root password is set returns 400 with guidance.
#[actix_web::test]
async fn v2_pair_requires_root_password_to_be_set() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "root_password": "", "label": "x" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// Changing the root password MUST preserve already-paired devices — replacing
/// the whole `AccessControlConfig` would silently wipe every device token.
#[actix_web::test]
async fn password_change_preserves_paired_devices() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Pair a device.
    let pair_req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "root_password": "secret", "label": "iPad" }))
        .to_request();
    let pair_resp = test::call_service(&app, pair_req).await;
    assert_eq!(pair_resp.status(), StatusCode::OK);
    let device_count_before = app_state
        .config
        .read()
        .await
        .access_control
        .as_ref()
        .unwrap()
        .devices
        .len();
    assert_eq!(device_count_before, 1);

    // Change the root password (local bypass → current_password not required).
    let change_req = test::TestRequest::post()
        .uri("/v1/bamboo/access/password")
        .insert_header((header::HOST, "localhost:9562"))
        .set_json(serde_json::json!({ "new_password": "newsecret" }))
        .to_request();
    let change_resp = test::call_service(&app, change_req).await;
    assert_eq!(change_resp.status(), StatusCode::OK);

    // The device survives the password change.
    let config = app_state.config.read().await;
    let access = config.access_control.as_ref().unwrap();
    assert_eq!(
        access.devices.len(),
        1,
        "password change must NOT wipe paired devices"
    );
    assert_eq!(access.devices[0].label, "iPad");
}
