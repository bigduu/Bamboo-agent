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
        // Unversioned liveness/readiness probes (#251 finding 6).
        ("GET", "/healthz"),
        ("GET", "/readyz"),
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
        // Unversioned liveness/readiness probes (#251 finding 6).
        ("GET", "/healthz"),
        ("GET", "/readyz"),
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

/// `/v2/stream` upgrade is NOT middleware-gated (#189): browsers cannot set
/// auth headers on a WS upgrade, so the upgrade is whitelisted and the ws_v2
/// handler enforces auth via `hello` instead. A remote, password-protected,
/// credential-less request therefore REACHES the handler (it is NOT the
/// middleware's `401`); a non-WebSocket GET then gets the handler's own `400`
/// (no upgrade headers). The handler still serves NO channel without a verified
/// hello — that contract is covered by the ws_v2 unit tests (`apply_auth_gate`).
///
/// The sibling gated routes (`/v2/pair/code`, `/v2/devices`) STAY behind the
/// middleware: a remote credential-less request is still `401`.
#[actix_web::test]
async fn v2_stream_upgrade_is_open_but_siblings_stay_gated() {
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

    // Remote, no cookie/header credential. The upgrade is whitelisted, so the
    // middleware does NOT reject it: the request reaches the WS handler, and a
    // non-WebSocket GET surfaces the handler's 400 (not the middleware's 401).
    let req = test::TestRequest::get()
        .uri("/v2/stream")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_ne!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/v2/stream upgrade must NOT be middleware-rejected (#189: hello carries auth)"
    );
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a non-WebSocket GET reaches the handler and is rejected by actix_ws::handle"
    );

    // The sibling management routes are still gated: a remote credential-less
    // request is rejected by the middleware with 401.
    let gated = test::TestRequest::post()
        .uri("/v2/pair/code")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let resp = test::call_service(&app, gated).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/v2/pair/code must stay middleware-gated"
    );

    let gated = test::TestRequest::get()
        .uri("/v2/devices")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let resp = test::call_service(&app, gated).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/v2/devices must stay middleware-gated"
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

// --- v2-P2 (#181, slice 2): pairing codes + /v2/devices management ---------

use crate::handlers::settings::PairingCodeEntry;
use std::time::Duration;

/// Inject a code directly into the ephemeral store with the given TTL.
fn inject_code(app_state: &AppState, code: &str, ttl: Duration) {
    app_state
        .pairing_codes
        .insert(code.to_string(), PairingCodeEntry::new(ttl));
}

/// Inject an already-expired code (expiry in the past).
fn inject_expired_code(app_state: &AppState, code: &str) {
    // Reuse a 0-TTL entry: `expires_at == now` ⇒ already expired by the
    // `>=` predicate, with no sleep needed.
    app_state.pairing_codes.insert(
        code.to_string(),
        PairingCodeEntry::new(Duration::from_secs(0)),
    );
}

/// `POST /v2/pair { code }` → redeem a valid code → token authenticates a remote
/// request; the code is single-use (a second redeem fails).
#[actix_web::test]
async fn v2_pair_code_redeems_once_and_token_authenticates() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    inject_code(&app_state, "842913", Duration::from_secs(120));
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Redeem the code.
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "842913", "label": "iPad" }))
        .to_request();
    let resp = test::call_service(&app, redeem).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = payload["device_id"].as_str().unwrap().to_string();
    let device_token = payload["device_token"].as_str().unwrap().to_string();
    assert!(device_token.starts_with("bd1_"));

    // The token authenticates a remote request through the middleware.
    let ok_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {device_token}")))
        .insert_header(("X-Device-Id", device_id))
        .to_request();
    assert_eq!(
        test::call_service(&app, ok_req).await.status(),
        StatusCode::OK
    );

    // Second redeem of the SAME code fails (one-time consume).
    let again = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "842913", "label": "iPad2" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, again).await.status(),
        StatusCode::UNAUTHORIZED
    );
}

/// An expired code is rejected.
#[actix_web::test]
async fn v2_pair_code_expired_is_rejected() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    inject_expired_code(&app_state, "111111");
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "111111", "label": "x" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, req).await.status(),
        StatusCode::UNAUTHORIZED
    );
}

/// An unknown code is rejected.
#[actix_web::test]
async fn v2_pair_code_unknown_is_rejected() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "999999", "label": "x" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, req).await.status(),
        StatusCode::UNAUTHORIZED
    );
}

/// Brute-force guard: after the failure threshold, further code redemptions are
/// rejected for the cooldown; a correct code outside the cooldown still works.
#[actix_web::test]
async fn v2_pair_code_brute_force_guard_trips_then_recovers() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // 10 wrong codes trip the cooldown.
    for _ in 0..10 {
        let req = test::TestRequest::post()
            .uri("/v2/pair")
            .insert_header((header::HOST, "bamboo.example.com"))
            .set_json(serde_json::json!({ "code": "000000", "label": "x" }))
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    // Even a freshly-injected VALID code is now rejected while in cooldown.
    inject_code(&app_state, "123456", Duration::from_secs(120));
    let blocked = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "123456", "label": "x" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, blocked).await.status(),
        StatusCode::UNAUTHORIZED,
        "valid code must be blocked during cooldown"
    );

    // Simulate the cooldown elapsing by clearing the guard, then a valid code
    // (re-injected — the trip cleared outstanding codes) works again.
    app_state.pairing_code_guard.record_success();
    inject_code(&app_state, "654321", Duration::from_secs(120));
    let ok = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "654321", "label": "after-cooldown" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, ok).await.status(),
        StatusCode::OK,
        "a correct code outside the cooldown must work"
    );
}

/// `GET /v2/devices` lists devices as a summary DTO that EXCLUDES token_hash and
/// token_salt (assert the serialized JSON has no such keys/values).
#[actix_web::test]
async fn v2_devices_list_excludes_secret_material() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    inject_code(&app_state, "424242", Duration::from_secs(120));
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Pair a device so the list is non-empty.
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "424242", "label": "Pixel" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, redeem).await.status(),
        StatusCode::OK
    );

    // Grab the persisted secrets so we can assert they don't appear in the list.
    let (hash, salt) = {
        let config = app_state.config.read().await;
        let d = &config.access_control.as_ref().unwrap().devices[0];
        (d.token_hash.clone(), d.token_salt.clone())
    };

    // GET /v2/devices (local bypass → reaches the gated handler).
    let list = test::TestRequest::get()
        .uri("/v2/devices")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    let resp = test::call_service(&app, list).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
    let raw = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        !raw.contains("token_hash"),
        "DTO must not expose token_hash key"
    );
    assert!(
        !raw.contains("token_salt"),
        "DTO must not expose token_salt key"
    );
    assert!(!raw.contains(&hash), "DTO must not leak the hash value");
    assert!(!raw.contains(&salt), "DTO must not leak the salt value");

    let arr: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(arr.as_array().unwrap().len(), 1);
    assert_eq!(arr[0]["label"], "Pixel");
    assert_eq!(arr[0]["revoked"], false);
}

/// `DELETE /v2/devices/{id}` revokes (token stops verifying, has_active_devices
/// flips when it was the last device); an unknown id → 404.
#[actix_web::test]
async fn v2_devices_delete_revokes_and_404s_unknown() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    inject_code(&app_state, "333333", Duration::from_secs(120));
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Pair.
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "333333", "label": "Watch" }))
        .to_request();
    let resp = test::call_service(&app, redeem).await;
    let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = payload["device_id"].as_str().unwrap().to_string();
    let device_token = payload["device_token"].as_str().unwrap().to_string();

    // Token works before revoke.
    let before = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {device_token}")))
        .insert_header(("X-Device-Id", device_id.clone()))
        .to_request();
    assert_eq!(
        test::call_service(&app, before).await.status(),
        StatusCode::OK
    );

    // Revoke (local bypass → reaches the gated handler).
    let del = test::TestRequest::delete()
        .uri(&format!("/v2/devices/{device_id}"))
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    assert_eq!(test::call_service(&app, del).await.status(), StatusCode::OK);

    // The row is kept but marked revoked.
    {
        let config = app_state.config.read().await;
        let access = config.access_control.as_ref().unwrap();
        assert_eq!(access.devices.len(), 1, "revoke keeps the audit row");
        assert!(access.devices[0].revoked);
    }

    // Token no longer authenticates (instant revocation). Since this was the last
    // active device but a root password is still set, remote access still gates.
    let after = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {device_token}")))
        .insert_header(("X-Device-Id", device_id.clone()))
        .to_request();
    assert_eq!(
        test::call_service(&app, after).await.status(),
        StatusCode::UNAUTHORIZED,
        "revoked token must stop authenticating immediately"
    );

    // Unknown id → 404.
    let unknown = test::TestRequest::delete()
        .uri("/v2/devices/bamboo_doesnotexist")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    assert_eq!(
        test::call_service(&app, unknown).await.status(),
        StatusCode::NOT_FOUND
    );
}

/// `POST /v2/devices/{id}/rotate` issues a NEW working token; the OLD token stops
/// verifying; device_id is unchanged; unknown id → 404.
#[actix_web::test]
async fn v2_devices_rotate_swaps_token_and_404s_unknown() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    inject_code(&app_state, "555555", Duration::from_secs(120));
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes),
    )
    .await;

    // Pair.
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "555555", "label": "Laptop" }))
        .to_request();
    let resp = test::call_service(&app, redeem).await;
    let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = payload["device_id"].as_str().unwrap().to_string();
    let old_token = payload["device_token"].as_str().unwrap().to_string();

    // Rotate (local bypass → reaches the gated handler).
    let rot = test::TestRequest::post()
        .uri(&format!("/v2/devices/{device_id}/rotate"))
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    let rot_resp = test::call_service(&app, rot).await;
    assert_eq!(rot_resp.status(), StatusCode::OK);
    let rbody = actix_web::body::to_bytes(rot_resp.into_body())
        .await
        .unwrap();
    let rpayload: serde_json::Value = serde_json::from_slice(&rbody).unwrap();
    let new_id = rpayload["device_id"].as_str().unwrap().to_string();
    let new_token = rpayload["device_token"].as_str().unwrap().to_string();
    assert_eq!(new_id, device_id, "device_id is unchanged across rotation");
    assert_ne!(new_token, old_token, "rotation issues a different token");

    // OLD token no longer verifies.
    let old_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {old_token}")))
        .insert_header(("X-Device-Id", device_id.clone()))
        .to_request();
    assert_eq!(
        test::call_service(&app, old_req).await.status(),
        StatusCode::UNAUTHORIZED,
        "old token must stop verifying after rotation"
    );

    // NEW token works, and the label/created_at are preserved.
    let new_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .insert_header((header::HOST, "bamboo.example.com"))
        .insert_header((header::AUTHORIZATION, format!("Bearer {new_token}")))
        .insert_header(("X-Device-Id", device_id.clone()))
        .to_request();
    assert_eq!(
        test::call_service(&app, new_req).await.status(),
        StatusCode::OK
    );
    {
        let config = app_state.config.read().await;
        let d = &config.access_control.as_ref().unwrap().devices[0];
        assert_eq!(d.label, "Laptop", "label preserved across rotation");
        assert!(!d.revoked);
    }

    // Unknown id → 404.
    let unknown = test::TestRequest::post()
        .uri("/v2/devices/bamboo_nope/rotate")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    assert_eq!(
        test::call_service(&app, unknown).await.status(),
        StatusCode::NOT_FOUND
    );
}

/// `POST /v2/pair/code` is GATED: a remote unauthenticated caller is 401 by the
/// middleware (unlike `/v2/stream`, which is open-upgrade + handler-enforced).
#[actix_web::test]
async fn v2_pair_code_requires_auth() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v2/pair/code")
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    assert_eq!(
        test::call_service(&app, req).await.status(),
        StatusCode::UNAUTHORIZED,
        "/v2/pair/code must be behind the access middleware"
    );
}

/// `POST /v2/pair/code` from a local (bypass) caller returns a 6-digit code +
/// ttl, and that code then redeems at `/v2/pair`.
#[actix_web::test]
async fn v2_pair_code_local_issue_then_redeem() {
    let data_dir = tempdir().unwrap();
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(password_access_control());
    }
    let app = test::init_service(App::new().app_data(app_state).configure(configure_routes)).await;

    // Local request → bypass → reaches the gated handler.
    let code_req = test::TestRequest::post()
        .uri("/v2/pair/code")
        .insert_header((header::HOST, "localhost:9562"))
        .to_request();
    let code_resp = test::call_service(&app, code_req).await;
    assert_eq!(code_resp.status(), StatusCode::OK);
    let body = actix_web::body::to_bytes(code_resp.into_body())
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let code = payload["code"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 6);
    assert!(code.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(payload["ttl"].as_u64().unwrap(), 120);

    // Redeem it.
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": code, "label": "redeemed" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, redeem).await.status(),
        StatusCode::OK
    );
}

// --- #190: per-IP root-password brute-force throttle -------------------------

/// `POST /v1/bamboo/access/verify`: a few wrong passwords still 401; after the
/// 5th consecutive wrong attempt from one IP the next request gets 429 with a
/// Retry-After header, and the throttle is keyed PER IP (a different IP is not
/// blocked). A correct password before the threshold succeeds and resets.
#[actix_web::test]
async fn access_verify_throttles_after_threshold_per_ip() {
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

    let attacker = "203.0.113.50:5555".parse().unwrap();
    let wrong = |peer| {
        test::TestRequest::post()
            .uri("/v1/bamboo/access/verify")
            .peer_addr(peer)
            .insert_header((header::HOST, "bamboo.example.com"))
            .set_json(serde_json::json!({ "password": "wrong" }))
            .to_request()
    };

    // First 5 wrong attempts are plain 401 (Unauthorized), not throttled.
    for _ in 0..5 {
        let resp = test::call_service(&app, wrong(attacker)).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // The 6th request from the SAME IP is now 429 with a Retry-After header, and
    // the password is not even compared.
    let blocked = test::call_service(&app, wrong(attacker)).await;
    assert_eq!(blocked.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        blocked.headers().get(header::RETRY_AFTER).is_some(),
        "429 must carry a Retry-After header"
    );

    // A DIFFERENT IP is unaffected (per-IP isolation): a correct password from a
    // fresh IP succeeds.
    let other = "198.51.100.77:6666".parse().unwrap();
    let ok = test::TestRequest::post()
        .uri("/v1/bamboo/access/verify")
        .peer_addr(other)
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "password": "secret" }))
        .to_request();
    assert_eq!(test::call_service(&app, ok).await.status(), StatusCode::OK);
}

/// A correct password BEFORE the threshold succeeds and resets the counter, so a
/// later wrong attempt starts a fresh window (no premature lockout for a user who
/// fat-fingers a couple of times then gets it right).
#[actix_web::test]
async fn access_verify_success_resets_counter() {
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

    let peer = "203.0.113.51:5555".parse().unwrap();
    // 3 wrong attempts (under the threshold).
    for _ in 0..3 {
        let resp = test::TestRequest::post()
            .uri("/v1/bamboo/access/verify")
            .peer_addr(peer)
            .insert_header((header::HOST, "bamboo.example.com"))
            .set_json(serde_json::json!({ "password": "wrong" }))
            .to_request();
        assert_eq!(
            test::call_service(&app, resp).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    // A correct password succeeds AND resets the counter.
    let ok = test::TestRequest::post()
        .uri("/v1/bamboo/access/verify")
        .peer_addr(peer)
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "password": "secret" }))
        .to_request();
    assert_eq!(test::call_service(&app, ok).await.status(), StatusCode::OK);

    // Counter is reset: a single wrong attempt is still just 401, not 429.
    let after = test::TestRequest::post()
        .uri("/v1/bamboo/access/verify")
        .peer_addr(peer)
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "password": "wrong" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, after).await.status(),
        StatusCode::UNAUTHORIZED
    );
}

/// A local/loopback request is NEVER throttled, even past the threshold, so the
/// desktop can never lock itself out.
#[actix_web::test]
async fn access_verify_loopback_is_never_throttled() {
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

    // 10 wrong attempts from a loopback host — well over the threshold.
    for _ in 0..10 {
        let resp = test::TestRequest::post()
            .uri("/v1/bamboo/access/verify")
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .insert_header((header::HOST, "localhost:9562"))
            .set_json(serde_json::json!({ "password": "wrong" }))
            .to_request();
        // Local is never 429; a wrong local password is still a plain 401.
        assert_eq!(
            test::call_service(&app, resp).await.status(),
            StatusCode::UNAUTHORIZED,
            "loopback must never be throttled"
        );
    }
}

/// `POST /v2/pair` root-password path: after 5 wrong root passwords from one IP,
/// the next request is 429 with Retry-After (the password is not compared).
#[actix_web::test]
async fn v2_pair_root_password_throttles_after_threshold() {
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

    let attacker = "203.0.113.60:7777".parse().unwrap();
    let wrong = || {
        test::TestRequest::post()
            .uri("/v2/pair")
            .peer_addr(attacker)
            .insert_header((header::HOST, "bamboo.example.com"))
            .set_json(serde_json::json!({ "root_password": "wrong", "label": "x" }))
            .to_request()
    };

    // 5 wrong root passwords → plain 401.
    for _ in 0..5 {
        assert_eq!(
            test::call_service(&app, wrong()).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    // 6th is throttled with Retry-After.
    let blocked = test::call_service(&app, wrong()).await;
    assert_eq!(blocked.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(blocked.headers().get(header::RETRY_AFTER).is_some());
}

/// The root-password throttle is INDEPENDENT of the existing code-path guard:
/// burning failures on the root-password path must NOT lock out the code path
/// (and vice versa). Confirms we did not accidentally double-guard or share state.
#[actix_web::test]
async fn root_password_throttle_does_not_block_code_path() {
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

    // Trip the root-password guard for this IP (>= threshold wrong root passwords).
    let attacker = "203.0.113.61:8888".parse().unwrap();
    for _ in 0..6 {
        let _ = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/v2/pair")
                .peer_addr(attacker)
                .insert_header((header::HOST, "bamboo.example.com"))
                .set_json(serde_json::json!({ "root_password": "wrong", "label": "x" }))
                .to_request(),
        )
        .await;
    }

    // The code path (separate guard) still works: inject + redeem a valid code
    // from the SAME IP succeeds, proving the guards are not shared.
    inject_code(&app_state, "424242", Duration::from_secs(120));
    let redeem = test::TestRequest::post()
        .uri("/v2/pair")
        .peer_addr(attacker)
        .insert_header((header::HOST, "bamboo.example.com"))
        .set_json(serde_json::json!({ "code": "424242", "label": "code-device" }))
        .to_request();
    assert_eq!(
        test::call_service(&app, redeem).await.status(),
        StatusCode::OK,
        "code path must be unaffected by the root-password throttle"
    );
}
