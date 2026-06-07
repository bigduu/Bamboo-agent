//! Server-side regression tests for the engine session-application cluster.
//!
//! The session/use-case modules live in `bamboo_engine::session_app`; callers
//! reference them directly. This module only retains server-level tests that
//! exercise that cluster through the server `AppState`.

#[cfg(test)]
mod metadata_tests;
