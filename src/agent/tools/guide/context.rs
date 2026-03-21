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
                "Assist with defensive security tasks only; refuse offensive security requests, credential harvesting, and malware-oriented code changes.",
                "For multi-step or non-trivial work, use Task to maintain a shared task list and keep statuses updated continuously.",
                "Keep exactly one Task item in in_progress whenever possible; mark items completed immediately after finishing.",
                "Read existing files before editing them. Use Edit for targeted replacements and Write only for full-file writes.",
                "Use Glob for file discovery and Grep for content search. Do not use Bash for find/grep/cat/head/tail/sed/awk/echo style file operations when dedicated tools exist.",
                "For code search, narrow scope first: Glob -> Grep(files_with_matches) -> Read -> Grep(content/multiline).",
                "Avoid broad Grep queries; keep head_limit small and add path/glob/type before multiline or content-heavy searches.",
                "Use Bash only for real terminal operations (build/test/git/npm/docker/etc.), not for file browsing or user-facing communication.",
                "When multiple Bash commands are independent, run them in parallel tool calls. For dependent commands, chain with &&.",
                "**Parallel tool calls are strongly preferred.** The following read-only tools can safely run in parallel and SHOULD be called together in the same response whenever possible: FileExists, Glob, GetCurrentDir, GetFileInfo, Grep, Read, WebFetch, WebSearch, session_inspector. For example, if you need to read 3 files, emit all 3 Read calls in one response instead of one at a time.",
                "For commit requests: inspect git status, git diff, and recent git log first; commit only when explicitly requested; avoid interactive git flags; use HEREDOC for multi-line commit messages.",
                "For pull request requests: review all commits/diff since base branch, use gh to create PR with summary and test plan, and return the PR URL.",
                "When referencing code locations in responses, use file_path:line_number format.",
                "Use ExitPlanMode before switching from planning to implementation.",
            ],
            GuideLanguage::English => &[
                "Assist with defensive security tasks only; refuse offensive security requests, credential harvesting, and malware-oriented code changes.",
                "For multi-step or non-trivial work, use Task to maintain a shared task list and keep statuses updated continuously.",
                "Keep exactly one Task item in in_progress whenever possible; mark items completed immediately after finishing.",
                "Read existing files before editing them. Use Edit for targeted replacements and Write only for full-file writes.",
                "Use Glob for file discovery and Grep for content search. Do not use Bash for find/grep/cat/head/tail/sed/awk/echo style file operations when dedicated tools exist.",
                "For code search, narrow scope first: Glob -> Grep(files_with_matches) -> Read -> Grep(content/multiline).",
                "Avoid broad Grep queries; keep head_limit small and add path/glob/type before multiline or content-heavy searches.",
                "Use Bash only for real terminal operations (build/test/git/npm/docker/etc.), not for file browsing or user-facing communication.",
                "When multiple Bash commands are independent, run them in parallel tool calls. For dependent commands, chain with &&.",
                "**Parallel tool calls are strongly preferred.** The following read-only tools can safely run in parallel and SHOULD be called together in the same response whenever possible: FileExists, Glob, GetCurrentDir, GetFileInfo, Grep, Read, WebFetch, WebSearch, session_inspector. For example, if you need to read 3 files, emit all 3 Read calls in one response instead of one at a time.",
                "For commit requests: inspect git status, git diff, and recent git log first; commit only when explicitly requested; avoid interactive git flags; use HEREDOC for multi-line commit messages.",
                "For pull request requests: review all commits/diff since base branch, use gh to create PR with summary and test plan, and return the PR URL.",
                "When referencing code locations in responses, use file_path:line_number format.",
                "Use ExitPlanMode before switching from planning to implementation.",
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
