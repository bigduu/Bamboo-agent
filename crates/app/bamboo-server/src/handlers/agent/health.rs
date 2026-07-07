//! Health check API handlers.
//!
//! Two flavors:
//! - `GET /api/v1/health` — the original plain-text `"OK"` probe (kept for
//!   back-compat).
//! - `GET /healthz` (liveness) and `GET /readyz` (readiness) — **unversioned**,
//!   public JSON `{status, version}` endpoints for load balancers / Kubernetes
//!   probes, which conventionally expect a stable unversioned path and a
//!   machine-parseable body. #251 (finding 6).

use actix_web::{HttpResponse, Responder};
use serde::Serialize;

/// The server version, surfaced by the JSON health endpoints so probes and
/// dashboards can read the running build. `0.0.0` in dev; the release train
/// stamps the real date-version at publish.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// JSON body for the `/healthz` and `/readyz` endpoints.
#[derive(Serialize)]
struct HealthStatus {
    /// Always `"ok"` when the endpoint responds (a non-200 or no response is the
    /// unhealthy signal).
    status: &'static str,
    /// The running server version.
    version: &'static str,
}

impl HealthStatus {
    fn ok() -> Self {
        Self {
            status: "ok",
            version: SERVER_VERSION,
        }
    }
}

/// Legacy health check endpoint — `GET /api/v1/health`.
///
/// Returns plain-text `"OK"`. Kept unchanged for back-compat; new probes should
/// prefer the unversioned JSON [`healthz`] / [`readyz`].
pub async fn handler() -> impl Responder {
    "OK"
}

/// Liveness probe — `GET /healthz` (unversioned, public).
///
/// Returns `200 {"status":"ok","version":"…"}` whenever the process is running
/// and able to serve HTTP. Liveness asks only "is the process up?" — it does not
/// check downstream dependencies, so a load balancer won't cycle the process for
/// a transient dependency blip.
pub async fn healthz() -> impl Responder {
    HttpResponse::Ok().json(HealthStatus::ok())
}

/// Readiness probe — `GET /readyz` (unversioned, public).
///
/// Returns `200 {"status":"ok","version":"…"}` once the server is accepting
/// requests. The route only exists after `AppState` is initialized and the
/// listener is bound, so a successful response means the server is ready to serve
/// traffic. Kept distinct from [`healthz`] so future readiness checks (e.g.
/// storage reachability) can gate `/readyz` without affecting liveness.
pub async fn readyz() -> impl Responder {
    HttpResponse::Ok().json(HealthStatus::ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_handler_returns_ok() {
        // This test verifies the handler can be called and returns OK
        // We'll test the actual HTTP response in other tests
        let _response = handler().await;
        // If we get here without panicking, the handler works
    }

    #[tokio::test]
    async fn healthz_returns_json_status_and_version() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/healthz", web::get().to(healthz))).await;
        let req = test::TestRequest::get().uri("/healthz").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), 200);
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.starts_with("application/json"),
            "expected JSON, got {content_type}"
        );

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["version"], SERVER_VERSION);
    }

    #[tokio::test]
    async fn readyz_returns_json_status_and_version() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/readyz", web::get().to(readyz))).await;
        let req = test::TestRequest::get().uri("/readyz").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["version"], SERVER_VERSION);
    }

    #[tokio::test]
    async fn test_health_handler_responds_to_request() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/health", web::get().to(handler))).await;

        let req = test::TestRequest::get().uri("/health").to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body = test::read_body(resp).await;
        assert_eq!(body.as_ref(), b"OK");
    }

    #[tokio::test]
    async fn test_health_handler_returns_200_status() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/health", web::get().to(handler))).await;

        let req = test::TestRequest::get().uri("/health").to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_health_handler_content_type() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/health", web::get().to(handler))).await;

        let req = test::TestRequest::get().uri("/health").to_request();

        let resp = test::call_service(&app, req).await;

        // Check that content-type is text/plain
        let content_type = resp.headers().get("content-type");
        assert!(content_type.is_some());

        let content_type_str = content_type.unwrap().to_str().unwrap();
        assert!(content_type_str.starts_with("text/plain"));
    }

    #[tokio::test]
    async fn test_health_handler_multiple_requests() {
        use actix_web::{test, web, App};

        let app = test::init_service(App::new().route("/health", web::get().to(handler))).await;

        // Make multiple requests to ensure handler works repeatedly
        for _ in 0..10 {
            let req = test::TestRequest::get().uri("/health").to_request();

            let resp = test::call_service(&app, req).await;
            assert!(resp.status().is_success());

            let body = test::read_body(resp).await;
            assert_eq!(body.as_ref(), b"OK");
        }
    }
}
