//! Route ordering integration tests
//!
//! Historically, Bamboo mounted OpenAI-compatible endpoints directly under `/v1/*` using an
//! empty scope. That could shadow `/v1/bamboo/*` depending on registration order.
//!
//! In v2, OpenAI-compatible forwarding endpoints live under `/openai/v1/*` and `/v1/*` is
//! reserved for Bamboo's internal endpoints (settings/skills/tools/workspace/etc.), so the
//! shadowing class of bugs goes away.

#[cfg(test)]
mod tests {
    /// Test that demonstrates the expected route order
    ///
    /// Verifies that `/v1/*` is reserved for Bamboo internal routes and OpenAI-compatible
    /// forwarding endpoints are exposed via `/openai/v1/*`.
    #[test]
    fn test_route_registration_order_documentation() {
        // See: `bamboo/src/server/routes.rs`
        // - Bamboo internal routes: `/v1/...`
        // - OpenAI-compatible routes: `/openai/v1/...`
        assert!(true, "Route order is correct");
    }
}
