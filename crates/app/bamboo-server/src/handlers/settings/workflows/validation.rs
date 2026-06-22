//! Workflow-name validation.
//!
//! The strict validator now lives in [`bamboo_config::paths::is_safe_workflow_name`]
//! so the HTTP workflow handlers here and the root binary's Tauri-IPC commands
//! share ONE implementation that can't drift (#34 / #97). Re-exported so existing
//! `use super::validation::is_safe_workflow_name` call sites are unchanged.
pub(crate) use bamboo_config::paths::is_safe_workflow_name;
