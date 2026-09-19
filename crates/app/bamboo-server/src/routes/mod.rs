//! Unified route configuration for all API endpoints
//!
//! This module provides explicit route registration for all API endpoints,
//! with no macro-based routing for consistency and clarity.

use actix_web::web;

mod agent;
mod bamboo_v1;
mod provider;

pub use agent::agent_routes;
#[cfg(test)]
pub(crate) use agent::plugin_scope;
pub use bamboo_v1::bamboo_v1_routes;
pub use provider::{anthropic_routes, gemini_routes, openai_prefixed_routes};

/// Configure all routes for desktop mode (no rate limiting)
///
/// Desktop mode binds to localhost only, so rate limiting is not needed
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(bamboo_v1_routes)
        .configure(openai_prefixed_routes)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

/// Configure all routes for production mode.
///
/// This registers the same route set as [`configure_routes`]; the actual per-IP
/// rate limiting is applied as a [`crate::rate_limit`] middleware (`.wrap(RateLimit)`)
/// on the production App in `server::entrypoints` / `server::web_service`, since
/// rate limiting is App-level middleware, not route configuration. See
/// [`crate::config::build_rate_limiter`]. (Name kept for back-compat.) #13.
pub fn configure_routes_with_rate_limiting(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(bamboo_v1_routes)
        .configure(openai_prefixed_routes)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

#[cfg(test)]
mod tests;
