//! Back-compat re-export shim for the subagent profile system.
//!
//! The canonical implementation now lives in
//! [`bamboo_engine::profiles`]. This shim keeps the existing server paths
//! (`crate::subagent_profiles::{builtin, load_registry, LoaderError}`) alive
//! without duplicating the profile definitions or loader logic.

pub use bamboo_engine::profiles::{builtin_profiles, load_registry, LoaderError};

pub mod builtin {
    pub use bamboo_engine::profiles::builtin::*;
}

pub mod loader {
    pub use bamboo_engine::profiles::loader::*;
}
