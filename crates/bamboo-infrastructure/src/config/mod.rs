//! Runtime/platform configuration for Bamboo agent.
//!
//! **Not a business domain model.** This crate carries:
//! - Provider / model mapping configuration
//! - Proxy authentication
//! - Environment variable hydration and encryption
//! - Keyword masking
//! - XDG-compliant path utilities
//!
//! Name: kept as `domain-config` for backward compatibility, but semantically
//! this is infrastructure/runtime config rather than a stable business domain.

#[allow(clippy::module_inception)]
pub mod config;
pub mod config_crypto;
pub mod encryption;
pub mod keyword_masking;
pub mod model_mapping;
pub mod patch;
pub mod paths;
pub mod provider_instance;
pub mod settings;
pub mod settings_loader;

pub use config::*;
pub use encryption::*;
pub use keyword_masking::*;
pub use model_mapping::*;
pub use paths::*;
pub use provider_instance::synthesize_legacy_instances;
pub use settings::PermissionMode;
