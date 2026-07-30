use super::*;

#[actix_web::test]
async fn test_validate_config_patch_reports_domain_errors() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    // Root proxy edits fail closed in favor of the revisioned Core API.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(json!({
            "http_proxy": "http://"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    let body = test::read_body(resp).await;
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("revisioned Core"), "{body}");

    // Invalid setup shape should be reported under setup domain.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(json!({
            "setup": "nope"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["valid"], false);
    assert!(!result["errors"]["setup"].as_array().unwrap().is_empty());
}

#[actix_web::test]
async fn test_validate_lifecycle_hooks_reports_structured_field_errors() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/config/validate")
            .set_json(json!({
                "lifecycle_hooks": {
                    "enabled": true,
                    "PreToolUse": [{
                        "matcher": "[",
                        "hooks": [{"type": "command", "command": "   ", "timeout_ms": 0}]
                    }],
                    "SessionEnd": [{
                        "hooks": [{
                            "type": "command",
                            "command": "echo done",
                            "timeout_ms": bamboo_config::MAX_LIFECYCLE_HOOK_TIMEOUT_MS + 1
                        }]
                    }],
                    "SessionStart": [{
                        "hooks": [{
                            "type": "javascript",
                            "source": "   ",
                            "memory_limit_bytes":
                                bamboo_config::MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES - 1
                        }]
                    }],
                    "Stop": [{
                        "hooks": [{
                            "type": "javascript",
                            "source": "function hook() {}",
                            "memory_limit_bytes":
                                bamboo_config::MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES + 1
                        }]
                    }]
                }
            }))
            .to_request(),
    )
    .await;

    assert!(response.status().is_success());
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["valid"], false);
    let paths = body["errors"]["lifecycle_hooks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|issue| issue["path"].as_str())
        .collect::<Vec<_>>();
    assert!(paths.contains(&"lifecycle_hooks.PreToolUse[0].matcher"));
    assert!(paths.contains(&"lifecycle_hooks.PreToolUse[0].hooks[0].command"));
    assert!(paths.contains(&"lifecycle_hooks.PreToolUse[0].hooks[0].timeout_ms"));
    assert!(paths.contains(&"lifecycle_hooks.SessionEnd[0].hooks[0].timeout_ms"));
    assert!(paths.contains(&"lifecycle_hooks.SessionStart[0].hooks[0].source"));
    assert!(paths.contains(&"lifecycle_hooks.SessionStart[0].hooks[0].memory_limit_bytes"));
    assert!(paths.contains(&"lifecycle_hooks.Stop[0].hooks[0].memory_limit_bytes"));

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/config/validate")
            .set_json(json!({
                "lifecycle_hooks": {"BeforeEverything": []}
            }))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["valid"], false);
    assert_eq!(
        body["errors"]["lifecycle_hooks"][0]["path"],
        "lifecycle_hooks.BeforeEverything"
    );

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/config/validate")
            .set_json(json!({
                "lifecycle_hooks": {
                    "PreToolUse": [{
                        "hooks": [{"type": "command", "command": "echo ok", "timeout_ms": -1}]
                    }]
                }
            }))
            .to_request(),
    )
    .await;
    assert!(
        response.status().is_success(),
        "shape errors use the structured response"
    );
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["valid"], false);
    assert_eq!(
        body["errors"]["lifecycle_hooks"][0]["path"],
        "lifecycle_hooks.PreToolUse[0].hooks[0].timeout_ms"
    );
}
