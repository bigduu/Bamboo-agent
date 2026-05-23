use std::collections::BTreeSet;

use crate::runtime::config::AgentLoopConfig;
use super::prompt_envelope::StablePromptFrame;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    parse_prompt_external_memory_sections, Message, PromptMemoryObservability, PromptSnapshot,
    Session,
};
use bamboo_tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};

use super::super::prompt_context::{
    merge_system_prompt_with_contexts, strip_existing_external_memory,
    strip_existing_plan_mode_instructions, strip_existing_plan_runtime_context,
    strip_existing_task_list,
};

const RUNTIME_PROMPT_COMPOSER_VERSION: &str = "bamboo.runtime-system-prompt.v3";
const RUNTIME_PROMPT_VERSION_KEY: &str = "runtime_prompt_composer_version";
const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";
pub(crate) const RUNTIME_PROMPT_SNAPSHOT_KEY: &str = "runtime_prompt_snapshot";
const RUNTIME_PROMPT_MEMORY_OBSERVABILITY_KEY: &str =
    crate::runtime::runner::prompt_context::PROMPT_MEMORY_OBSERVABILITY_KEY;

const BASE_PROMPT_SECTION_ID: &str = "base_prompt";
const WORKSPACE_CONTEXT_SECTION_ID: &str = "workspace_context";
const INSTRUCTION_CONTEXT_SECTION_ID: &str = "instruction_context";
const ENV_CONTEXT_SECTION_ID: &str = "env_context";
const SKILL_CONTEXT_SECTION_ID: &str = "skill_context";
const TOOL_GUIDE_SECTION_ID: &str = "tool_guide_context";
const WORKSPACE_CONTEXT_START_MARKER: &str =
    crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER;
const WORKSPACE_CONTEXT_END_MARKER: &str = crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER;
const WORKSPACE_CONTEXT_PREFIX: &str = crate::runtime::context::WORKSPACE_CONTEXT_PREFIX;
const ENV_CONTEXT_START_MARKER: &str = crate::runtime::context::ENV_CONTEXT_START_MARKER;
const ENV_CONTEXT_END_MARKER: &str = crate::runtime::context::ENV_CONTEXT_END_MARKER;
const INSTRUCTION_CONTEXT_START_MARKER: &str =
    crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER;
const INSTRUCTION_CONTEXT_END_MARKER: &str =
    crate::runtime::context::instruction::INSTRUCTION_CONTEXT_END_MARKER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptLayer {
    CoreStatic,
    EnvironmentWorkspace,
    EnvironmentInstruction,
    EnvironmentConfiguration,
    SkillMetadata,
    CapabilityTool,
}

impl PromptLayer {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::CoreStatic => "core_static",
            Self::EnvironmentWorkspace => "environment_workspace",
            Self::EnvironmentInstruction => "environment_instruction",
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

    #[cfg(test)]
    pub(crate) fn section(&self, id: &str) -> Option<&PromptSection> {
        self.sections.iter().find(|section| section.id == id)
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
            "workspace={};instruction={};env={};skill={};tool_guide={};external_memory={};task_list={}",
            self.section_enabled_by_name(WORKSPACE_CONTEXT_SECTION_ID) as u8,
            self.section_enabled_by_name(INSTRUCTION_CONTEXT_SECTION_ID) as u8,
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
            "base={};workspace={};instruction={};env={};skill={};tool_guide={};external_memory={};task_list={};final={}",
            base_len,
            self.dynamic_len_by_name(WORKSPACE_CONTEXT_SECTION_ID),
            self.dynamic_len_by_name(INSTRUCTION_CONTEXT_SECTION_ID),
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

    pub(crate) fn to_prompt_snapshot(&self, effective_system_prompt: &str) -> PromptSnapshot {
        let section_content = |id: &str| {
            self.sections
                .iter()
                .find(|section| section.id == id && section.enabled)
                .map(|section| section.content.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let base_system_prompt = section_content(BASE_PROMPT_SECTION_ID).unwrap_or_default();
        let workspace_context = section_content(WORKSPACE_CONTEXT_SECTION_ID);
        let instruction_context = section_content(INSTRUCTION_CONTEXT_SECTION_ID);
        let env_context = section_content(ENV_CONTEXT_SECTION_ID);
        let skill_context = section_content(SKILL_CONTEXT_SECTION_ID);
        let tool_guide_context = section_content(TOOL_GUIDE_SECTION_ID);
        let enhancement_prompt = derive_enhancement_prompt(
            &base_system_prompt,
            effective_system_prompt,
            workspace_context.as_deref(),
            instruction_context.as_deref(),
            env_context.as_deref(),
            skill_context.as_deref(),
            tool_guide_context.as_deref(),
            None,
            None,
        );

        let external_memory_parts = parse_prompt_external_memory_sections(None);

        PromptSnapshot {
            base_system_prompt,
            enhancement_prompt,
            workspace_context,
            instruction_context,
            env_context,
            skill_context,
            tool_guide_context,
            dream_notebook: external_memory_parts.dream_notebook,
            session_memory_note: external_memory_parts.session_memory_note,
            project_memory_index: external_memory_parts.project_memory_index,
            relevant_durable_memories: external_memory_parts.relevant_durable_memories,
            project_dream: external_memory_parts.project_dream,
            global_dream_fallback: external_memory_parts.global_dream_fallback,
            prompt_memory_observability: None,
            external_memory: None,
            task_list: None,
            effective_system_prompt: effective_system_prompt.trim().to_string(),
        }
    }
}

pub(crate) fn resolve_base_prompt_for_language<'a>(
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
                .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
                .map(|message| message.content.as_str())
        })
        .unwrap_or_default()
}

pub(crate) fn build_stable_prompt_frame(
    session: &Session,
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    activated_discoverable_tools: &BTreeSet<String>,
) -> StablePromptFrame {
    let raw_base_prompt = resolve_base_prompt_for_language(config, session).to_string();
    let workspace_context = extract_workspace_context(&raw_base_prompt);
    let instruction_context = workspace_context
        .as_deref()
        .and_then(workspace_path_from_context)
        .and_then(crate::runtime::context::instruction::build_instruction_prompt_context);
    let env_context = extract_env_context(&raw_base_prompt)
        .or_else(crate::runtime::context::build_env_prompt_context);
    let base_prompt = normalize_base_prompt(&raw_base_prompt);

    let skill_context = session
        .metadata
        .get("skill.context")
        .map(String::as_str)
        .unwrap_or_default();

    let tool_guide_context = build_tool_guide_context(
        config,
        tool_schemas,
        &raw_base_prompt,
        session.id.as_str(),
        activated_discoverable_tools,
    );

    let stable_instructions = merge_with_optional_contexts(
        &base_prompt,
        workspace_context.as_deref(),
        instruction_context.as_deref(),
        env_context.as_deref(),
        skill_context,
        &tool_guide_context,
    );

    StablePromptFrame::new(stable_instructions, Vec::new())
}

pub(crate) fn build_tool_guide_context(
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    base_prompt_for_language: &str,
    session_id: &str,
    activated_discoverable_tools: &BTreeSet<String>,
) -> String {
    let normalized_base_prompt = normalize_base_prompt(base_prompt_for_language);
    let mut guide_context = GuideBuildContext::from_system_prompt(&normalized_base_prompt);
    guide_context.activated_discoverable_tools = activated_discoverable_tools.clone();
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

pub(crate) fn apply_system_prompt_contexts(
    session: &mut Session,
    config: &AgentLoopConfig,
    skill_context: &str,
    tool_guide_context: &str,
) -> PromptAssemblyReport {
    let (base_prompt, workspace_context, instruction_context, env_context, merged_prompt) =
        if let Some(system_message) = session
            .messages
            .iter_mut()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        {
            let raw_base_prompt = config
                .system_prompt
                .as_deref()
                .unwrap_or(&system_message.content)
                .to_string();
            let workspace_context = extract_workspace_context(&raw_base_prompt);
            let instruction_context = workspace_context
                .as_deref()
                .and_then(workspace_path_from_context)
                .and_then(crate::runtime::context::instruction::build_instruction_prompt_context);
            let env_context = extract_env_context(&raw_base_prompt)
                .or_else(crate::runtime::context::build_env_prompt_context);
            let base_prompt = normalize_base_prompt(&raw_base_prompt);
            let merged_prompt = merge_with_optional_contexts(
                &base_prompt,
                workspace_context.as_deref(),
                instruction_context.as_deref(),
                env_context.as_deref(),
                skill_context,
                tool_guide_context,
            );
            system_message.content = merged_prompt.clone();
            (
                base_prompt,
                workspace_context,
                instruction_context,
                env_context,
                merged_prompt,
            )
        } else {
            let raw_base_prompt = config
                .system_prompt
                .as_deref()
                .unwrap_or_default()
                .to_string();
            let workspace_context = extract_workspace_context(&raw_base_prompt);
            let instruction_context = workspace_context
                .as_deref()
                .and_then(workspace_path_from_context)
                .and_then(crate::runtime::context::instruction::build_instruction_prompt_context);
            let env_context = extract_env_context(&raw_base_prompt)
                .or_else(crate::runtime::context::build_env_prompt_context);
            let base_prompt = normalize_base_prompt(&raw_base_prompt);
            let merged_prompt = merge_with_optional_contexts(
                &base_prompt,
                workspace_context.as_deref(),
                instruction_context.as_deref(),
                env_context.as_deref(),
                skill_context,
                tool_guide_context,
            );
            if !merged_prompt.is_empty() {
                session
                    .messages
                    .insert(0, Message::system(merged_prompt.clone()));
            }
            (
                base_prompt,
                workspace_context,
                instruction_context,
                env_context,
                merged_prompt,
            )
        };

    let sections = build_prompt_sections(
        base_prompt.as_str(),
        workspace_context.as_deref(),
        instruction_context.as_deref(),
        env_context.as_deref(),
        skill_context,
        tool_guide_context,
    );
    let report = PromptAssemblyReport::from_sections(sections, merged_prompt.as_str());

    persist_runtime_prompt_metadata(session, &report);
    persist_prompt_snapshot_metadata(session, report.to_prompt_snapshot(merged_prompt.as_str()));
    log_runtime_prompt_assembly_summary(session, &report, merged_prompt.as_str());

    report
}

pub(crate) fn persist_prompt_snapshot_metadata(session: &mut Session, snapshot: PromptSnapshot) {
    session.prompt_snapshot = Some(snapshot);
}

pub fn read_prompt_snapshot_metadata(session: &Session) -> Option<PromptSnapshot> {
    session.prompt_snapshot.clone().or_else(|| {
        session
            .metadata
            .get(RUNTIME_PROMPT_SNAPSHOT_KEY)
            .and_then(|raw| serde_json::from_str::<PromptSnapshot>(raw).ok())
    })
}

fn read_prompt_memory_observability(session: &Session) -> Option<PromptMemoryObservability> {
    session
        .prompt_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.prompt_memory_observability.clone())
        .or_else(|| {
            session
                .metadata
                .get(RUNTIME_PROMPT_MEMORY_OBSERVABILITY_KEY)
                .and_then(|raw| serde_json::from_str::<PromptMemoryObservability>(raw).ok())
        })
}

pub fn refresh_prompt_snapshot_from_session(session: &mut Session) {
    let effective_system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        .map(|message| message.content.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_default();

    let workspace_context = session
        .metadata
        .get("workspace_path")
        .and_then(|workspace_path| {
            crate::runtime::context::build_workspace_prompt_context(workspace_path)
        })
        .or_else(|| extract_workspace_context(&effective_system_prompt));
    let instruction_context = session
        .metadata
        .get("workspace_path")
        .and_then(|workspace_path| {
            crate::runtime::context::instruction::build_instruction_prompt_context(workspace_path)
        })
        .or_else(|| {
            workspace_context
                .as_deref()
                .and_then(workspace_path_from_context)
                .and_then(crate::runtime::context::instruction::build_instruction_prompt_context)
        });
    let env_context = extract_env_context(&effective_system_prompt)
        .or_else(crate::runtime::context::build_env_prompt_context);
    let skill_context = extract_wrapped_section(
        &effective_system_prompt,
        "<!-- BAMBOO_SKILL_CONTEXT_START -->",
        "<!-- BAMBOO_SKILL_CONTEXT_END -->",
    );
    let tool_guide_context = extract_wrapped_section(
        &effective_system_prompt,
        "<!-- BAMBOO_TOOL_GUIDE_START -->",
        "<!-- BAMBOO_TOOL_GUIDE_END -->",
    );
    let external_memory = extract_wrapped_section(
        &effective_system_prompt,
        "<!-- BAMBOO_EXTERNAL_MEMORY_START -->",
        "<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
    );
    let task_list = extract_wrapped_section(
        &effective_system_prompt,
        "<!-- BAMBOO_TASK_LIST_START -->",
        "<!-- BAMBOO_TASK_LIST_END -->",
    );

    let base_system_prompt = session
        .metadata
        .get("base_system_prompt")
        .cloned()
        .unwrap_or_else(|| normalize_base_prompt(&effective_system_prompt));
    let enhancement_prompt = session.metadata.get("enhance_prompt").cloned().or_else(|| {
        derive_enhancement_prompt(
            &base_system_prompt,
            &effective_system_prompt,
            workspace_context.as_deref(),
            instruction_context.as_deref(),
            env_context.as_deref(),
            skill_context.as_deref(),
            tool_guide_context.as_deref(),
            external_memory.as_deref(),
            task_list.as_deref(),
        )
    });

    let external_memory_parts = parse_prompt_external_memory_sections(external_memory.as_deref());

    persist_prompt_snapshot_metadata(
        session,
        PromptSnapshot {
            base_system_prompt,
            enhancement_prompt,
            workspace_context,
            instruction_context,
            env_context,
            skill_context,
            tool_guide_context,
            dream_notebook: external_memory_parts.dream_notebook,
            session_memory_note: external_memory_parts.session_memory_note,
            project_memory_index: external_memory_parts.project_memory_index,
            relevant_durable_memories: external_memory_parts.relevant_durable_memories,
            project_dream: external_memory_parts.project_dream,
            global_dream_fallback: external_memory_parts.global_dream_fallback,
            prompt_memory_observability: read_prompt_memory_observability(session),
            external_memory,
            task_list,
            effective_system_prompt,
        },
    );
}

fn build_prompt_sections(
    base_prompt: &str,
    workspace_context: Option<&str>,
    instruction_context: Option<&str>,
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
            INSTRUCTION_CONTEXT_SECTION_ID,
            PromptLayer::EnvironmentInstruction,
            true,
            instruction_context.unwrap_or_default(),
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
    let without_instruction = strip_instruction_context(&without_workspace);
    let without_env = strip_env_context(&without_instruction);
    let without_external_memory = strip_existing_external_memory(&without_env);
    let without_task_list = strip_existing_task_list(&without_external_memory);
    let without_plan_mode = strip_existing_plan_mode_instructions(&without_task_list);
    let without_plan_runtime = strip_existing_plan_runtime_context(&without_plan_mode);
    merge_system_prompt_with_contexts(&without_plan_runtime, "", "")
}

fn merge_with_optional_contexts(
    base_prompt: &str,
    workspace_context: Option<&str>,
    instruction_context: Option<&str>,
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
    if let Some(instruction_context) = instruction_context
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        sections.push(instruction_context.to_string());
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

fn derive_enhancement_prompt(
    base_system_prompt: &str,
    effective_system_prompt: &str,
    workspace_context: Option<&str>,
    instruction_context: Option<&str>,
    env_context: Option<&str>,
    skill_context: Option<&str>,
    tool_guide_context: Option<&str>,
    external_memory: Option<&str>,
    task_list: Option<&str>,
) -> Option<String> {
    let mut stripped = effective_system_prompt.trim().to_string();
    for section in [
        workspace_context,
        instruction_context,
        env_context,
        skill_context,
        tool_guide_context,
        external_memory,
        task_list,
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .filter(|value| !value.is_empty())
    {
        stripped = stripped.replace(section, "");
    }
    let normalized = stripped.trim().to_string();
    let base = base_system_prompt.trim();
    if base.is_empty() || normalized.is_empty() || normalized == base {
        None
    } else if let Some(remainder) = normalized.strip_prefix(base) {
        let remainder = remainder.trim();
        (!remainder.is_empty()).then(|| remainder.to_string())
    } else {
        None
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

fn strip_instruction_context(prompt: &str) -> String {
    strip_wrapped_section(
        prompt,
        INSTRUCTION_CONTEXT_START_MARKER,
        INSTRUCTION_CONTEXT_END_MARKER,
    )
    .map(|stripped| stripped.trim().to_string())
    .unwrap_or_else(|| prompt.trim().to_string())
}

fn strip_env_context(prompt: &str) -> String {
    strip_wrapped_section(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER)
        .map(|stripped| stripped.trim().to_string())
        .unwrap_or_else(|| prompt.trim().to_string())
}

fn workspace_path_from_context(context: &str) -> Option<&str> {
    let context = context.trim();
    let start = context.find(WORKSPACE_CONTEXT_PREFIX)?;
    let rest = &context[start + WORKSPACE_CONTEXT_PREFIX.len()..];
    let first_line = rest.lines().next()?.trim();
    (!first_line.is_empty()).then_some(first_line)
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
    let guidance = crate::runtime::context::workspace_prompt_guidance();
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
    let guidance = crate::runtime::context::workspace_prompt_guidance();
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
