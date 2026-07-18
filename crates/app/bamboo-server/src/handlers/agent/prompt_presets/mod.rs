//! Prompt preset CRUD endpoints for `/api/v1/prompt-presets`.

mod handlers;
mod storage;
mod types;

pub use handlers::{
    create_prompt_preset, delete_prompt_preset, list_prompt_presets, patch_prompt_preset,
};
// Re-exported (rather than making the `storage`/`types` submodules themselves
// `pub(crate)`) so `crate::plugin_installer` can reuse the exact same
// load/save/rename-on-collision logic the HTTP handlers use for prompt
// presets, without a second implementation that could drift (see
// PLUGIN_PLAN.md § Installer-core agent).
pub(crate) use storage::{
    ensure_unique_preset_id, load_store, load_stored_presets, save_store, store_file_path,
};
pub(crate) use types::StoredPromptPreset;

#[cfg(test)]
mod tests;
