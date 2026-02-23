#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuideLanguage {
    Chinese,
    English,
}

impl GuideLanguage {
    pub fn detect(source: &str) -> Self {
        if source.chars().any(is_cjk) {
            Self::Chinese
        } else {
            Self::English
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuideBuildContext {
    pub language: GuideLanguage,
    pub include_best_practices: bool,
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
    pub fn from_system_prompt(prompt: &str) -> Self {
        Self {
            language: GuideLanguage::detect(prompt),
            ..Self::default()
        }
    }

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
