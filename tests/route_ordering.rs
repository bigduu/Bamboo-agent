//! Route ordering integration tests
//!
//! Historically, Bamboo mounted OpenAI-compatible endpoints directly under `/v1/*` using an
//! empty scope. That could shadow `/v1/bamboo/*` depending on registration order.
//!
//! In v2, OpenAI-compatible forwarding endpoints live under `/openai/v1/*` and `/v1/*` is
//! reserved for Bamboo's internal endpoints (settings/skills/tools/workspace/etc.), so the
//! shadowing class of bugs goes away.

use actix_web::http::StatusCode;
use actix_web::{test, App};

use bamboo_agent::server::configure_routes;

#[actix_web::test]
async fn openai_routes_are_prefixed_and_do_not_shadow_v1_bamboo_routes() {
    let app = test::init_service(App::new().configure(configure_routes)).await;

    let legacy_openai_req = test::TestRequest::get().uri("/v1/models").to_request();
    let legacy_openai_resp = test::call_service(&app, legacy_openai_req).await;
    assert_eq!(
        legacy_openai_resp.status(),
        StatusCode::NOT_FOUND,
        "legacy /v1/models route should not be registered"
    );

    let prefixed_openai_req = test::TestRequest::get()
        .uri("/openai/v1/models")
        .to_request();
    let prefixed_openai_resp = test::call_service(&app, prefixed_openai_req).await;
    assert_ne!(
        prefixed_openai_resp.status(),
        StatusCode::NOT_FOUND,
        "prefixed /openai/v1/models route should be registered"
    );

    let bamboo_internal_req = test::TestRequest::get()
        .uri("/v1/bamboo/setup/status")
        .to_request();
    let bamboo_internal_resp = test::call_service(&app, bamboo_internal_req).await;
    assert_ne!(
        bamboo_internal_resp.status(),
        StatusCode::NOT_FOUND,
        "internal /v1/bamboo/* route should be registered"
    );
}
