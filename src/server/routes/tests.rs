use actix_web::http::StatusCode;
use actix_web::{test, App};

use super::{configure_routes, configure_routes_with_rate_limiting};

#[actix_web::test]
async fn configure_routes_registers_expected_api_prefixes() {
    let app = test::init_service(App::new().configure(configure_routes)).await;

    for uri in [
        "/api/v1/health",
        "/v1/bamboo/workflows",
        "/openai/v1/models",
        "/anthropic/v1/models",
        "/gemini/v1beta/models",
    ] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected route to be registered: {uri}"
        );
    }
}

#[actix_web::test]
async fn configure_routes_with_rate_limiting_registers_expected_api_prefixes() {
    let app = test::init_service(App::new().configure(configure_routes_with_rate_limiting)).await;

    for uri in [
        "/api/v1/health",
        "/v1/bamboo/workflows",
        "/openai/v1/models",
        "/anthropic/v1/models",
        "/gemini/v1beta/models",
    ] {
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected route to be registered: {uri}"
        );
    }
}
