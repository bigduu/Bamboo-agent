use actix_web::http::StatusCode;
use bamboo_agent::server::configure_routes;

use super::*;

#[actix_web::test]
async fn legacy_provider_settings_routes_are_retired_under_both_api_prefixes() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    let submitted_secret = "retired-provider-route-secret";
    for prefix in ["/v1", "/api/v1"] {
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("{prefix}/bamboo/settings/provider"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("{prefix}/bamboo/settings/provider"))
                .set_json(json!({
                    "provider": "openai",
                    "providers": {
                        "openai": {"api_key": submitted_secret}
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(!body.contains(submitted_secret));

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("{prefix}/bamboo/settings/provider/models"))
                .set_json(json!({"provider": "openai"}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[actix_web::test]
async fn canonical_provider_settings_catalog_instances_and_reload_routes_remain_registered() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    for uri in [
        "/v1/bamboo/config/provider-settings",
        "/v1/bamboo/settings/provider-instances",
    ] {
        let response =
            test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
        assert!(
            response.status().is_success(),
            "canonical route failed: {uri}"
        );
    }

    let catalog = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/provider-catalog/fetch-models")
            .set_json(json!({"provider": "retired-route-check"}))
            .to_request(),
    )
    .await;
    assert_eq!(catalog.status(), StatusCode::BAD_REQUEST);

    let reload = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/settings/reload")
            .to_request(),
    )
    .await;
    assert_ne!(reload.status(), StatusCode::NOT_FOUND);
}
