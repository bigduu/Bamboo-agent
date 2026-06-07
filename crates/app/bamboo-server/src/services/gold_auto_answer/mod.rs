//! Server-side regression tests for the engine Gold auto-answer service.
//!
//! The service lives in `bamboo_engine::gold_auto_answer`; callers reference it
//! directly. This module only retains server-level tests that exercise it
//! through the server `AppState`.

#[cfg(test)]
mod tests;
