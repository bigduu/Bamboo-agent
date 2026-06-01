//! Server-side re-export shim for the engine session-application cluster.
//!
//! The session/use-case modules now live in `bamboo_engine::session_app`.
//! These re-exports preserve the historical `crate::session_app::…` paths used
//! throughout the server crate.

pub use bamboo_engine::session_app::{
    chat, child_session, errors, execute, metadata, provider_model, repository, respond, resume,
    session_create, system_prompt, truncation, types,
};

#[cfg(test)]
mod metadata_tests;
