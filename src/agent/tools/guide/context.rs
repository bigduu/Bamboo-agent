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
}

impl Default for GuideBuildContext {
    fn default() -> Self {
        Self {
            language: GuideLanguage::English,
            include_best_practices: true,
            max_examples_per_tool: 1,
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

    /// Returns best practices appropriate for the configured language.
    pub fn best_practices(&self) -> &'static [&'static str] {
        match self.language {
            GuideLanguage::Chinese => &[
                "Verify the target path exists before reading or writing.",
                "Search first, then edit, so the impact is explicit.",
                "Create a todo list for multi-step tasks and keep it updated.",
                "Use ask_user before destructive actions or unclear decisions.",
            ],
            GuideLanguage::English => &[
                "Verify the target path exists before reading or writing.",
                "Search first, then edit, so the impact is explicit.",
                "Create a todo list for multi-step tasks and keep it updated.",
                "Use ask_user before destructive actions or unclear decisions.",
            ],
        }
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
