use crate::agent::core::{Role, Session};
use sha2::{Digest, Sha256};

pub(super) const PROMPT_COMPOSER_VERSION: &str = "bamboo.prompt-composer.v1";

pub(super) fn upsert_system_prompt_message(session: &mut Session, system_prompt: String) {
    session
        .messages
        .retain(|message| !matches!(message.role, Role::System));
    session
        .messages
        .insert(0, crate::agent::core::Message::system(system_prompt));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PromptCompositionProfile {
    pub version: &'static str,
    pub fingerprint: String,
    pub has_enhancement: bool,
    pub has_workspace_context: bool,
    pub base_len: usize,
    pub enhancement_len: usize,
    pub workspace_context_len: usize,
    pub final_len: usize,
}

impl PromptCompositionProfile {
    pub(super) fn component_flags_value(&self) -> String {
        format!(
            "enhance={};workspace={}",
            self.has_enhancement as u8, self.has_workspace_context as u8
        )
    }

    pub(super) fn component_lengths_value(&self) -> String {
        format!(
            "base={};enhance={};workspace={};final={}",
            self.base_len, self.enhancement_len, self.workspace_context_len, self.final_len
        )
    }
}

fn build_prompt_fingerprint(
    base_prompt: &str,
    enhancement: Option<&str>,
    workspace: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PROMPT_COMPOSER_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(base_prompt.as_bytes());
    hasher.update([0u8]);
    hasher.update(enhancement.unwrap_or_default().as_bytes());
    hasher.update([0u8]);
    hasher.update(workspace.unwrap_or_default().as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn build_enhanced_system_prompt(
    base_prompt: &str,
    enhance_prompt: Option<&str>,
    workspace_path: Option<&str>,
) -> String {
    build_enhanced_system_prompt_with_profile(base_prompt, enhance_prompt, workspace_path).0
}

pub(super) fn build_enhanced_system_prompt_with_profile(
    base_prompt: &str,
    enhance_prompt: Option<&str>,
    workspace_path: Option<&str>,
) -> (String, PromptCompositionProfile) {
    let mut merged_prompt = base_prompt.to_string();

    let enhancement = enhance_prompt
        .map(str::trim)
        .filter(|enhancement| !enhancement.is_empty())
        .map(ToString::to_string);
    if let Some(enhancement) = enhancement.as_ref() {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(enhancement.as_str());
    }

    let workspace_context = workspace_path
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty())
        .and_then(crate::server::app_state::build_workspace_prompt_context);
    if let Some(workspace_context) = workspace_context.as_ref() {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(workspace_context.as_str());
    }

    let profile = PromptCompositionProfile {
        version: PROMPT_COMPOSER_VERSION,
        fingerprint: build_prompt_fingerprint(
            base_prompt,
            enhancement.as_deref(),
            workspace_context.as_deref(),
        ),
        has_enhancement: enhancement.is_some(),
        has_workspace_context: workspace_context.is_some(),
        base_len: base_prompt.len(),
        enhancement_len: enhancement.as_ref().map(|s| s.len()).unwrap_or(0),
        workspace_context_len: workspace_context.as_ref().map(|s| s.len()).unwrap_or(0),
        final_len: merged_prompt.len(),
    };

    (merged_prompt, profile)
}
