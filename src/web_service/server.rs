//! Deprecated web_service server module.
//!
//! The web_service module has been consolidated into `crate::server`.
//! This file remains as a compatibility layer.

pub use crate::server::{run, run_with_bind, run_with_bind_and_static, WebService};
