use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{Message, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};

use super::super::prompt_context::merge_system_prompt_with_contexts;

const RUNTIME_PROMPT_COMPOSER_VERSION: &str = "bamboo.runtime-system-prompt.v3";
const RUNTIME_PROMPT_VERSION_KEY: &str = "runtime_prompt_composer_version";
const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";

const BASE_PROMPT_SECTION_ID: &str = "base_prompt";
const WORKSPACE_CONTEXT_SECTION_ID: &str = "workspace_context";
const ENV_CONTEXT_SECTION_ID: &str = "env_context";
const SKILL_CONTEXT_SECTION_ID: &str = "skill_context";
const TOOL_GUIDE_SECTION_ID: &str = "tool_guide_context";
const WORKSPACE_CONTEXT_START_MARKER: &str =
    crate::server::app_state::WORKSPACE_CONTEXT_START_MARKER;
const WORKSPACE_CONTEXT_END_MARKER: &str = crate::server::app_state::WORKSPACE_CONTEXT_END_MARKER;
const WORKSPACE_CONTEXT_PREFIX: &str = crate::server::app_state::WORKSPACE_CONTEXT_PREFIX;
const ENV_CONTEXT_START_MARKER: &str = crate::server::app_state::ENV_CONTEXT_START_MARKER;
const ENV_CONTEXT_END_MARKER: &str = crate::server::app_state::ENV_CONTEXT_END_MARKER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptLayer {
    CoreStatic,
    EnvironmentWorkspace,
    EnvironmentConfiguration,
    SkillMetadata,
    CapabilityTool,
}

impl PromptLayer {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::CoreStatic => "core_static",
            Self::EnvironmentWorkspace => "environment_workspace",
            Self::EnvironmentConfiguration => "environment_configuration",
            Self::SkillMetadata => "skill_metadata",
            Self::CapabilityTool => "capability_tool",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptSection {
    pub id: &'static str,
    pub layer: PromptLayer,
    pub is_dynamic: bool,
    pub enabled: bool,
    pub content: String,
}

impl PromptSection {
    pub(crate) fn new(
        id: &'static str,
        layer: PromptLayer,
        is_dynamic: bool,
        content: impl Into<String>,
    ) -> Self {
        let content = content.into();
        let enabled = !content.trim().is_empty();
        Self {
            id,
            layer,
            is_dynamic,
            enabled,
            content,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.content.len()
    }

    pub(super) fn scope_label(&self) -> &'static str {
        if self.is_dynamic {
            "dynamic"
        } else {
            "static"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptAssemblyReport {
    pub version: &'static str,
    pub sections: Vec<PromptSection>,
    pub final_len: usize,
}

impl PromptAssemblyReport {
    pub(crate) fn from_sections(sections: Vec<PromptSection>, final_prompt: &str) -> Self {
        Self {
            version: RUNTIME_PROMPT_COMPOSER_VERSION,
            sections,
            final_len: final_prompt.len(),
        }
    }

    pub(super) fn section(&self, id: &str) -> Option<&PromptSection> {
        self.sections.iter().find(|section| section.id == id)
    }

    fn section_enabled(&self, id: &str) -> bool {
        self.section(id)
            .map(|section| section.enabled)
            .unwrap_or(false)
    }

    fn section_len(&self, id: &str) -> usize {
        self.section(id).map(PromptSection::len).unwrap_or(0)
    }

    fn section_enabled_by_name(&self, id: &str) -> bool {
        self.sections
            .iter()
            .any(|section| section.id == id && section.enabled)
    }

    fn dynamic_len_by_name(&self, id: &str) -> usize {
        self.sections
            .iter()
            .filter(|section| section.id == id && section.enabled && section.is_dynamic)
            .map(PromptSection::len)
            .sum()
    }

    pub(crate) fn component_flags_value(&self) -> String {
        format!(
            "workspace={};env={};skill={};tool_guide={};external_memory={};task_list={}",
            self.section_enabled_by_name(WORKSPACE_CONTEXT_SECTION_ID) as u8,
            self.section_enabled_by_name(ENV_CONTEXT_SECTION_ID) as u8,
            self.section_enabled_by_name(SKILL_CONTEXT_SECTION_ID) as u8,
            self.section_enabled_by_name(TOOL_GUIDE_SECTION_ID) as u8,
            self.section_enabled_by_name("external_memory") as u8,
            self.section_enabled_by_name("task_list") as u8,
        )
    }

    pub(crate) fn component_lengths_value(&self) -> String {
        let base_len: usize = self
            .sections
            .iter()
            .filter(|section| section.enabled && !section.is_dynamic)
            .map(PromptSection::len)
            .sum();
        format!(
            "base={};workspace={};env={};skill={};tool_guide={};external_memory={};task_list={};final={}",
            base_len,
            self.dynamic_len_by_name(WORKSPACE_CONTEXT_SECTION_ID),
            self.dynamic_len_by_name(ENV_CONTEXT_SECTION_ID),
            self.dynamic_len_by_name(SKILL_CONTEXT_SECTION_ID),
            self.dynamic_len_by_name(TOOL_GUIDE_SECTION_ID),
            self.dynamic_len_by_name("external_memory"),
            self.dynamic_len_by_name("task_list"),
            self.final_len,
        )
    }

    pub(crate) fn section_layout_value(&self) -> String {
        self.sections
            .iter()
            .map(|section| {
                format!(
                    "{}:{}:{}:{}:{}",
                    section.id,
                    section.layer.as_str(),
                    section.scope_label(),
                    section.enabled as u8,
                    section.len(),
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

pub(super) fn resolve_base_prompt_for_language<'a>(
    config: &'a AgentLoopConfig,
    session: &'a Session,
) -> &'a str {
    config
        .system_prompt
        .as_deref()
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| matches!(message.role, crate::agent::core::Role::System))
                .map(|message| message.content.as_str())
        })
        .unwrap_or_default()
}

pub(super) fn build_tool_guide_context(
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    base_prompt_for_language: &str,
    session_id: &str,
) -> String {
    let normalized_base_prompt = normalize_base_prompt(base_prompt_for_language);
    let guide_context = GuideBuildContext::from_system_prompt(&normalized_base_prompt);
    let tool_guide_context = EnhancedPromptBuilder::build(
        Some(config.tool_registry.as_ref()),
        tool_schemas,
        &guide_context,
    );
    tracing::info!(
        "[{}] Tool guide context built, length: {} chars",
        session_id,
        tool_guide_context.len()
    );
    tool_guide_context
}

pub(super) fn apply_system_prompt_contexts(
    session: &mut Session,
    config: &AgentLoopConfig,
    skill_context: &str,
    tool_guide_context: &str,
) -> PromptAssemblyReport {
    let (base_prompt, workspace_context, env_context, merged_prompt) = if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let raw_base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or(&system_message.content)
            .to_string();
        let workspace_context = extract_workspace_context(&raw_base_prompt);
        let env_context = extract_env_context(&raw_base_prompt)
            .or_else(crate::server::app_state::build_env_prompt_context);
        let base_prompt = normalize_base_prompt(&raw_base_prompt);
        let merged_prompt = merge_with_optional_contexts(
            &base_prompt,
            workspace_context.as_deref(),
            env_context.as_deref(),
            skill_context,
            tool_guide_context,
        );
        system_message.content = merged_prompt.clone();
        (base_prompt, workspace_context, env_context, merged_prompt)
    } else {
        let raw_base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or_default()
            .to_string();
        let workspace_context = extract_workspace_context(&raw_base_prompt);
        let env_context = extract_env_context(&raw_base_prompt)
            .or_else(crate::server::app_state::build_env_prompt_context);
        let base_prompt = normalize_base_prompt(&raw_base_prompt);
        let merged_prompt = merge_with_optional_contexts(
            &base_prompt,
            workspace_context.as_deref(),
            env_context.as_deref(),
            skill_context,
            tool_guide_context,
        );
        if !merged_prompt.is_empty() {
            session
                .messages
                .insert(0, Message::system(merged_prompt.clone()));
        }
        (base_prompt, workspace_context, env_context, merged_prompt)
    };

    let sections = build_prompt_sections(
        base_prompt.as_str(),
        workspace_context.as_deref(),
        env_context.as_deref(),
        skill_context,
        tool_guide_context,
    );
    let report = PromptAssemblyReport::from_sections(sections, merged_prompt.as_str());

    persist_runtime_prompt_metadata(session, &report);
    log_runtime_prompt_assembly_summary(session, &report, merged_prompt.as_str());

    report
}

fn build_prompt_sections(
    base_prompt: &str,
    workspace_context: Option<&str>,
    env_context: Option<&str>,
    skill_context: &str,
    tool_guide_context: &str,
) -> Vec<PromptSection> {
    vec![
        PromptSection::new(
            BASE_PROMPT_SECTION_ID,
            PromptLayer::CoreStatic,
            false,
            base_prompt,
        ),
        PromptSection::new(
            WORKSPACE_CONTEXT_SECTION_ID,
            PromptLayer::EnvironmentWorkspace,
            true,
            workspace_context.unwrap_or_default(),
        ),
        PromptSection::new(
            ENV_CONTEXT_SECTION_ID,
            PromptLayer::EnvironmentConfiguration,
            true,
            env_context.unwrap_or_default(),
        ),
        PromptSection::new(
            SKILL_CONTEXT_SECTION_ID,
            PromptLayer::SkillMetadata,
            true,
            skill_context,
        ),
        PromptSection::new(
            TOOL_GUIDE_SECTION_ID,
            PromptLayer::CapabilityTool,
            true,
            tool_guide_context,
        ),
    ]
}

fn normalize_base_prompt(prompt: &str) -> String {
    let without_workspace = strip_workspace_context(prompt);
    let without_env = strip_env_context(&without_workspace);
    merge_system_prompt_with_contexts(&without_env, "", "")
}

fn merge_with_optional_contexts(
    base_prompt: &str,
    workspace_context: Option<&str>,
    env_context: Option<&str>,
    skill_context: &str,
    tool_guide_context: &str,
) -> String {
    let merged = merge_system_prompt_with_contexts(base_prompt, skill_context, tool_guide_context);
    let mut sections = Vec::new();
    if let Some(workspace_context) = workspace_context
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        sections.push(workspace_context.to_string());
    }
    if let Some(env_context) = env_context.map(str::trim).filter(|value| !value.is_empty()) {
        sections.push(env_context.to_string());
    }

    if sections.is_empty() {
        merged
    } else {
        format!("{}\n\n{}", merged.trim_end(), sections.join("\n\n"))
    }
}

fn extract_workspace_context(prompt: &str) -> Option<String> {
    extract_wrapped_section(
        prompt,
        WORKSPACE_CONTEXT_START_MARKER,
        WORKSPACE_CONTEXT_END_MARKER,
    )
    .or_else(|| extract_legacy_workspace_context(prompt))
}

fn extract_env_context(prompt: &str) -> Option<String> {
    extract_wrapped_section(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER)
}

fn strip_workspace_context(prompt: &str) -> String {
    strip_wrapped_section(
        prompt,
        WORKSPACE_CONTEXT_START_MARKER,
        WORKSPACE_CONTEXT_END_MARKER,
    )
    .map(|stripped| stripped.trim().to_string())
    .unwrap_or_else(|| strip_legacy_workspace_context(prompt).trim().to_string())
}

fn strip_env_context(prompt: &str) -> String {
    strip_wrapped_section(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER)
        .map(|stripped| stripped.trim().to_string())
        .unwrap_or_else(|| prompt.trim().to_string())
}

fn extract_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let section_end = section_start + end_rel_idx;
    let section = prompt[start_idx..section_end + end_marker.len()].trim();
    (!section.is_empty()).then(|| section.to_string())
}

fn strip_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let end_idx = section_start + end_rel_idx + end_marker.len();

    let before = prompt[..start_idx].trim_end();
    let after = prompt[end_idx..].trim_start();

    Some(match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_string(),
        (false, true) => before.to_string(),
        (false, false) => format!("{before}\n\n{after}"),
    })
}

fn extract_legacy_workspace_context(prompt: &str) -> Option<String> {
    let start_idx = prompt.find(WORKSPACE_CONTEXT_PREFIX)?;
    let guidance = crate::server::app_state::workspace_prompt_guidance();
    let end_idx = prompt[start_idx..]
        .find(&guidance)
        .map(|guidance_rel_idx| start_idx + guidance_rel_idx + guidance.len())
        .unwrap_or(prompt.len());
    let section = prompt[start_idx..end_idx].trim();
    (!section.is_empty()).then(|| section.to_string())
}

fn strip_legacy_workspace_context(prompt: &str) -> String {
    let Some(start_idx) = prompt.find(WORKSPACE_CONTEXT_PREFIX) else {
        return prompt.to_string();
    };
    let guidance = crate::server::app_state::workspace_prompt_guidance();
    let end_idx = prompt[start_idx..]
        .find(&guidance)
        .map(|guidance_rel_idx| start_idx + guidance_rel_idx + guidance.len())
        .unwrap_or(prompt.len());

    let before = prompt[..start_idx].trim_end();
    let after = prompt[end_idx..].trim_start();
    match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_string(),
        (false, true) => before.to_string(),
        (false, false) => format!("{before}\n\n{after}"),
    }
}

fn log_runtime_prompt_assembly_summary(
    session: &Session,
    report: &PromptAssemblyReport,
    final_prompt: &str,
) {
    let session_id = session.id.as_str();

    tracing::info!(
        "[{}] Runtime prompt assembly summary: composer_version={}, {}, {}, sections={}",
        session_id,
        report.version,
        report.component_flags_value(),
        report.component_lengths_value(),
        report.section_layout_value(),
    );

    tracing::debug!(
        "[{}] Runtime prompt assembly details: final_len={}, section_count={}",
        session_id,
        report.final_len,
        report.sections.len(),
    );
    tracing::debug!(
        "[{}] ========== EFFECTIVE SYSTEM PROMPT AFTER SESSION SETUP ==========",
        session_id
    );
    tracing::debug!("[{}] {}", session_id, final_prompt);
    tracing::debug!(
        "[{}] ========== END EFFECTIVE SYSTEM PROMPT AFTER SESSION SETUP ==========",
        session_id
    );
}

fn persist_runtime_prompt_metadata(session: &mut Session, report: &PromptAssemblyReport) {
    session.metadata.insert(
        RUNTIME_PROMPT_VERSION_KEY.to_string(),
        report.version.to_string(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_FLAGS_KEY.to_string(),
        report.component_flags_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_LENGTHS_KEY.to_string(),
        report.component_lengths_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_SECTION_LAYOUT_KEY.to_string(),
        report.section_layout_value(),
    );
}
