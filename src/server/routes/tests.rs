use actix_web::http::{header, StatusCode};
use actix_web::{test, web, App};
use tempfile::tempdir;

use super::{configure_routes, configure_routes_with_rate_limiting};
use crate::core::AccessControlConfig;
use crate::server::AppState;

#[actix_web::test]
async fn configure_routes_registers_expected_api_prefixes() {
    let app = test::init_service(App::new().configure(configure_routes)).await;

    let requests = vec![
        ("GET", "/api/v1/health"),
        ("POST", "/api/v1/sessions/example/project-dream/run"),
        ("GET", "/v1/bamboo/workflows"),
        ("GET", "/v1/bamboo/access/status"),
        ("GET", "/openai/v1/models"),
        ("GET", "/anthropic/v1/models"),
        ("GET", "/gemini/v1beta/models"),
    ];

    for (method, uri) in requests {
        let req = match method {
            "POST" => test::TestRequest::post().uri(uri).to_request(),
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
        ("GET", "/v1/bamboo/workflows"),
        ("GET", "/v1/bamboo/access/status"),
        ("GET", "/openai/v1/models"),
        ("GET", "/anthropic/v1/models"),
        ("GET", "/gemini/v1beta/models"),
    ];

    for (method, uri) in requests {
        let req = match method {
            "POST" => test::TestRequest::post().uri(uri).to_request(),
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
    crate::core::paths::init_bamboo_dir(data_dir.path().to_path_buf());
    let app_state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());

    let mut session = crate::agent::core::Session::new("session-http-project-dream", "gpt-5");
    session.add_message(crate::agent::core::Message::system(
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
