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
