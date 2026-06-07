//! Prompt preset CRUD endpoints for `/api/v1/prompt-presets`.

mod handlers;
mod storage;
mod types;

pub use handlers::{
    create_prompt_preset, delete_prompt_preset, list_prompt_presets, patch_prompt_preset,
};

#[cfg(test)]
mod tests;
