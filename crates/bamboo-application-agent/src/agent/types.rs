//! Core agent types re-exported from bamboo-domain-session.
//!
//! The actual domain types live in `crates/bamboo-domain-session/src/types.rs`.
//! This module re-exports them at the original module path for backward
//! compatibility and keeps the `pub(crate)` helpers that depend on facade internals.

// Re-export domain types from crate
pub use bamboo_domain_session::types::*;
pub use bamboo_domain_session::message_part::*;
pub use bamboo_domain_session::tool_types::*;
pub use bamboo_domain_session::budget_types::*;
pub use bamboo_domain_session::task::*;

// ─── pub(crate) helpers that stay in the facade ────────────────────────
// These reference facade internals and cannot live in the domain crate.

/// Structured snapshot of parsed external-memory subsections used for prompt observability.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromptSnapshotExternalMemoryParts {
    pub dream_notebook: Option<String>,
    pub session_memory_note: Option<String>,
    pub project_memory_index: Option<String>,
    pub relevant_durable_memories: Option<String>,
    pub project_dream: Option<String>,
    pub global_dream_fallback: Option<String>,
}

pub fn parse_prompt_external_memory_sections(
    external_memory: Option<&str>,
) -> PromptSnapshotExternalMemoryParts {
    let Some(external_memory) = external_memory
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return PromptSnapshotExternalMemoryParts::default();
    };

    let legacy_dream_notebook = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Cross-session Dream Notebook (read-only)",
    );
    let project_memory_index = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Project Durable Memory Index",
    );
    let relevant_durable_memories =
        extract_prompt_plain_section_by_heading(external_memory, "### Relevant Durable Memories");
    let project_dream =
        extract_prompt_markdown_block_by_heading(external_memory, "### Project Dream Summary");
    let global_dream_fallback = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Global Dream Summary (fallback)",
    );
    let session_memory_note = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Session Memory Note (markdown)",
    )
    .or_else(|| collect_prompt_session_memory_topics(external_memory));
    let dream_notebook = legacy_dream_notebook
        .clone()
        .or_else(|| project_dream.clone())
        .or_else(|| global_dream_fallback.clone());

    PromptSnapshotExternalMemoryParts {
        dream_notebook,
        session_memory_note,
        project_memory_index,
        relevant_durable_memories,
        project_dream,
        global_dream_fallback,
    }
}

pub fn extract_prompt_markdown_block_by_heading(content: &str, heading: &str) -> Option<String> {
    let start_idx = content.find(heading)?;
    let after_heading = &content[start_idx + heading.len()..];
    let fence_start_rel = after_heading.find("````md")?;
    let after_fence = &after_heading[fence_start_rel + "````md".len()..];
    let fence_end_rel = after_fence.find("````")?;
    let block = after_fence[..fence_end_rel].trim();
    (!block.is_empty()).then(|| block.to_string())
}

pub fn extract_prompt_plain_section_by_heading(content: &str, heading: &str) -> Option<String> {
    let mut collecting = false;
    let mut collected = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if !collecting {
            if trimmed == heading {
                collecting = true;
            }
            continue;
        }

        if trimmed.starts_with("### ") {
            break;
        }
        collected.push(line);
    }

    let section = collected.join("\n").trim().to_string();
    (!section.is_empty()).then_some(section)
}

pub fn collect_prompt_session_memory_topics(content: &str) -> Option<String> {
    let mut collected = Vec::new();
    let mut remaining = content;
    let heading = "### Session Memory Topic: `";
    while let Some(start_idx) = remaining.find(heading) {
        let after_start = &remaining[start_idx..];
        let Some(line_end) = after_start.find('\n') else {
            break;
        };
        let title_line = after_start[..line_end].trim();
        let rest = &after_start[line_end + 1..];
        let Some(fence_start_rel) = rest.find("````md") else {
            remaining = rest;
            continue;
        };
        let after_fence = &rest[fence_start_rel + "````md".len()..];
        let Some(fence_end_rel) = after_fence.find("````") else {
            break;
        };
        let block = after_fence[..fence_end_rel].trim();
        if !block.is_empty() {
            collected.push(format!("{}\n\n{}", title_line, block));
        }
        remaining = &after_fence[fence_end_rel + "````".len()..];
    }

    (!collected.is_empty()).then(|| collected.join("\n\n---\n\n"))
}
