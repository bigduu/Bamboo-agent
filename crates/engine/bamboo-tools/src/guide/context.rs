//! Context for building tool usage guides.
//!
//! This module provides language detection and context building for generating
//! localized tool usage guidelines that match the language of the system prompt.

/// Language options for guide generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuideLanguage {
    Chinese,
    English,
}

impl GuideLanguage {
    /// Detects the language from source text.
    ///
    /// Returns `Chinese` if CJK characters are detected, otherwise `English`.
    pub fn detect(source: &str) -> Self {
        if source.chars().any(is_cjk) {
            Self::Chinese
        } else {
            Self::English
        }
    }
}

/// Build context for generating tool guides.
///
/// Contains language settings and configuration options for guide generation.
#[derive(Debug, Clone)]
pub struct GuideBuildContext {
    /// Detected or configured language for guide content
    pub language: GuideLanguage,
    /// Whether to include best practices section
    pub include_best_practices: bool,
    /// Maximum number of examples to include per tool
    pub max_examples_per_tool: usize,
    /// Discoverable tools that are currently activated for the session.
    /// Activated discoverable tools render with full guides; inactive ones
    /// render with short summaries only.
    pub activated_discoverable_tools: std::collections::BTreeSet<String>,
}

impl Default for GuideBuildContext {
    fn default() -> Self {
        Self {
            language: GuideLanguage::English,
            include_best_practices: true,
            max_examples_per_tool: 2,
            activated_discoverable_tools: std::collections::BTreeSet::new(),
        }
    }
}

impl GuideBuildContext {
    /// Creates a build context by detecting language from a system prompt.
    ///
    /// # Arguments
    ///
    /// * `prompt` - The system prompt to analyze for language detection
    pub fn from_system_prompt(prompt: &str) -> Self {
        Self {
            language: GuideLanguage::detect(prompt),
            ..Self::default()
        }
    }

    /// Shared rules that apply across tool schemas.
    pub fn best_practices(&self) -> &'static [&'static str] {
        &[
            "Assist only with defensive security. Refuse offensive activity, credential harvesting, and malware-oriented changes.",
            "Batch independent read-only calls. Narrow searches by path or pattern before requesting content.",
            "Prefer dedicated file and Git tools when available. For Git, start with status and relevant diffs. Inspect the merge base and unmerged index for conflicts, preserve user changes and unique commits, and verify the resolved diff. Commit or publish only with user authorization.",
        ]
    }
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch,
        '\u{3400}'..='\u{4DBF}' | '\u{4E00}'..='\u{9FFF}' | '\u{F900}'..='\u{FAFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::{GuideBuildContext, GuideLanguage};

    #[test]
    fn detect_language_prefers_chinese_when_cjk_present() {
        assert_eq!(
            GuideLanguage::detect("Please help me modify this file"),
            GuideLanguage::English
        );
    }

    #[test]
    fn shared_rules_keep_security_and_git_boundaries_without_tool_names() {
        let guidance = GuideBuildContext::default().best_practices().join("\n");
        for required in [
            "defensive security",
            "Batch independent read-only calls",
            "merge base and unmerged index",
            "preserve user changes and unique commits",
            "only with user authorization",
        ] {
            assert!(guidance.contains(required), "missing {required}");
        }
        for name in ["FileExists", "GetCurrentDir", "BashOutput", "exec_command"] {
            assert!(!guidance.contains(name), "unexpected {name}");
        }
    }

    #[test]
    fn detect_language_defaults_to_english_without_cjk() {
        assert_eq!(
            GuideLanguage::detect("Please inspect the codebase"),
            GuideLanguage::English
        );
    }

    #[test]
    fn from_system_prompt_carries_detected_language() {
        let context = GuideBuildContext::from_system_prompt("You are a coding assistant.");
        assert_eq!(context.language, GuideLanguage::English);
    }
}
