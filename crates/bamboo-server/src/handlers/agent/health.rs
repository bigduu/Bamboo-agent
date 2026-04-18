//! Health check API handler.
//!
//! This module provides a simple health check endpoint for monitoring
//! and load balancer health probes.

use actix_web::Responder;

/// Health check endpoint.
///
/// Returns a simple "OK" response to indicate the server is running.
///
/// # HTTP Method
///
/// `GET /health`
///
/// # Response
///
/// - `200 OK` - Server is healthy (returns plain text "OK")
///
/// # Usage
///
/// This endpoint is commonly used by:
/// - Load balancers for health probes
/// - Monitoring systems for uptime checks
/// - Kubernetes liveness/readiness probes
///
/// # Example
///
/// ```bash
/// curl http://localhost:9562/health
/// # Returns: OK
/// ```
pub async fn handler() -> impl Responder {
    "OK"
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
