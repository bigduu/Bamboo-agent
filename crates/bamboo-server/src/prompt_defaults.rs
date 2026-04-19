//! Global prompt template defaults used by new sessions.
//!
//! Phase 3 semantics:
//! - Global system prompt settings are stored under Bamboo data.
//! - New sessions use this template when no explicit base prompt is provided.
//! - Existing sessions stay stable via persisted `metadata.base_system_prompt`.

use std::path::{Path, PathBuf};

const SYSTEM_PROMPT_FILE_NAME: &str = "system-prompt.md";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalSystemPromptSource {
    BambooFile,
    BuiltinDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalSystemPromptTemplate {
    pub content: String,
    pub source: GlobalSystemPromptSource,
    pub path: Option<PathBuf>,
}

fn bamboo_system_prompt_file_path() -> PathBuf {
    bamboo_infrastructure::paths::bamboo_dir().join(SYSTEM_PROMPT_FILE_NAME)
}

fn read_non_empty_prompt_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok().and_then(|content| {
        let trimmed = content.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn builtin_default_prompt() -> String {
    crate::app_state::DEFAULT_BASE_PROMPT.to_string()
}

fn resolve_with_paths(bamboo_path: &Path) -> GlobalSystemPromptTemplate {
    if let Some(content) = read_non_empty_prompt_file(bamboo_path) {
        return GlobalSystemPromptTemplate {
            content,
            source: GlobalSystemPromptSource::BambooFile,
            path: Some(bamboo_path.to_path_buf()),
        };
    }

    GlobalSystemPromptTemplate {
        content: builtin_default_prompt(),
        source: GlobalSystemPromptSource::BuiltinDefault,
        path: None,
    }
}

/// Read the global default prompt template used for new session initialization.
///
/// Fallback order:
/// 1. `${BAMBOO_DATA_DIR}/system-prompt.md` (typically `~/.bamboo/system-prompt.md`) if present and non-empty
/// 2. Builtin [`crate::app_state::DEFAULT_BASE_PROMPT`]
pub fn read_global_default_system_prompt_template() -> String {
    resolve_global_default_system_prompt_template().content
}

/// Resolve the global default prompt template with source metadata.
pub fn resolve_global_default_system_prompt_template() -> GlobalSystemPromptTemplate {
    let bamboo_path = bamboo_system_prompt_file_path();
    resolve_with_paths(&bamboo_path)
}

#[cfg(test)]
mod tests {
    use super::{
        read_global_default_system_prompt_template, resolve_with_paths, GlobalSystemPromptSource,
    };
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn global_default_template_is_never_empty() {
        assert!(!read_global_default_system_prompt_template()
            .trim()
            .is_empty());
    }

    #[test]
    fn resolution_prefers_bamboo_file_when_present() {
        let temp = tempdir().expect("temp dir");
        let bamboo_prompt = temp.path().join("bamboo-system-prompt.md");

        fs::write(&bamboo_prompt, "from bamboo").expect("write bamboo prompt");

        let resolved = resolve_with_paths(&bamboo_prompt);
        assert_eq!(resolved.content, "from bamboo");
        assert_eq!(resolved.source, GlobalSystemPromptSource::BambooFile);
        assert_eq!(resolved.path.as_deref(), Some(bamboo_prompt.as_path()));
    }

    #[test]
    fn resolution_falls_back_to_builtin_when_bamboo_missing() {
        let temp = tempdir().expect("temp dir");
        let bamboo_prompt = temp.path().join("missing-bamboo-system-prompt.md");

        let resolved = resolve_with_paths(&bamboo_prompt);
        assert_eq!(resolved.source, GlobalSystemPromptSource::BuiltinDefault);
        assert!(resolved.path.is_none());
        assert!(!resolved.content.trim().is_empty());
    }
}
