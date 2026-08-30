use std::collections::{BTreeSet, HashMap};

use bamboo_agent_core::Session;

pub const ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY: &str = "activated_discoverable_tools";
const MAX_ACTIVATED_DISCOVERABLE_TOOLS: usize = 12;

/// Legacy tool-guide detail policy. This controls expandable guidance and
/// session metadata only; it is not callable Core/Deferred loading policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolGuideExposure {
    Full,
    Expandable,
}

/// Deprecated names for the legacy guide-detail policy.
///
/// `Core` here never meant the callable loading class introduced by #986.
#[deprecated(
    note = "use ToolGuideExposure::{Full, Expandable}; callable loading policy lives in bamboo_domain::CapabilityLoadingClass"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolExposure {
    Core,
    Discoverable,
}

pub fn canonical_tool_name(name: &str) -> String {
    bamboo_domain::canonical_tool_name(name)
}

pub fn guide_exposure_for_tool_name(name: &str) -> ToolGuideExposure {
    match canonical_tool_name(name).as_str() {
        // Lower-frequency or specialized tools stay discoverable by default.
        "Sleep" | "NotebookEdit" | "js_repl" | "WebFetch" | "WebSearch" | "memory"
        | "scheduler" | "SubAgent" | "session_history" | "ExitPlanMode" => {
            ToolGuideExposure::Expandable
        }
        _ => ToolGuideExposure::Full,
    }
}

#[allow(deprecated)]
#[deprecated(note = "use guide_exposure_for_tool_name for legacy guide detail policy")]
pub fn exposure_for_tool_name(name: &str) -> ToolExposure {
    match guide_exposure_for_tool_name(name) {
        ToolGuideExposure::Full => ToolExposure::Core,
        ToolGuideExposure::Expandable => ToolExposure::Discoverable,
    }
}

pub fn has_full_tool_guide(name: &str) -> bool {
    matches!(guide_exposure_for_tool_name(name), ToolGuideExposure::Full)
}

#[deprecated(note = "use has_full_tool_guide; this predicate is not callable Core policy")]
pub fn is_core_tool(name: &str) -> bool {
    has_full_tool_guide(name)
}

pub fn has_expandable_tool_guide(name: &str) -> bool {
    matches!(
        guide_exposure_for_tool_name(name),
        ToolGuideExposure::Expandable
    )
}

#[deprecated(note = "use has_expandable_tool_guide for legacy guide detail policy")]
pub fn is_discoverable_tool(name: &str) -> bool {
    has_expandable_tool_guide(name)
}

pub fn activated_discoverable_tools_from_metadata(
    metadata: &HashMap<String, String>,
) -> BTreeSet<String> {
    metadata
        .get(ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|name| canonical_tool_name(&name))
        .filter(|name| has_expandable_tool_guide(name))
        .collect()
}

pub fn activated_discoverable_tools(session: &Session) -> BTreeSet<String> {
    activated_discoverable_tools_from_metadata(&session.metadata)
}

pub fn activate_discoverable_tools<I, S>(session: &mut Session, tool_names: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut activated = activated_discoverable_tools(session);
    for tool_name in tool_names {
        let canonical = canonical_tool_name(tool_name.as_ref());
        if has_expandable_tool_guide(&canonical) {
            activated.insert(canonical);
        }
    }

    if activated.is_empty() {
        session
            .metadata
            .remove(ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY);
        return;
    }

    let mut names: Vec<String> = activated.into_iter().collect();
    names.truncate(MAX_ACTIVATED_DISCOVERABLE_TOOLS);
    if let Ok(raw) = serde_json::to_string(&names) {
        session
            .metadata
            .insert(ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY.to_string(), raw);
    }
}

/// Deactivate discoverable tools by canonical name.
///
/// Non-discoverable or non-activated names are silently ignored.
pub fn deactivate_discoverable_tools<I, S>(session: &mut Session, tool_names: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut activated = activated_discoverable_tools(session);
    for tool_name in tool_names {
        let canonical = canonical_tool_name(tool_name.as_ref());
        activated.remove(&canonical);
    }

    if activated.is_empty() {
        session
            .metadata
            .remove(ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY);
        return;
    }

    let mut names: Vec<String> = activated.into_iter().collect();
    names.truncate(MAX_ACTIVATED_DISCOVERABLE_TOOLS);
    if let Ok(raw) = serde_json::to_string(&names) {
        session
            .metadata
            .insert(ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY.to_string(), raw);
    }
}

/// Short summary for a legacy expandable guide shown before activation.
pub fn expandable_tool_short_description(name: &str) -> Option<&'static str> {
    match canonical_tool_name(name).as_str() {
        "Sleep" => Some("Pause briefly when waiting for an external state change before polling again."),
        "NotebookEdit" => Some("Edit notebook cells by replace/insert/delete."),
        "js_repl" => Some("Execute JavaScript code using Node.js with top-level await support."),
        "WebFetch" => Some("Fetch a webpage by URL when you need cleaned page text from a known target."),
        "WebSearch" => Some("Search the web with optional domain allow/block filters."),
        "memory" => Some("Manage Bamboo's unified memory system for session notes and durable project/global memories."),
        "scheduler" => Some("Manage Bamboo scheduled automation jobs for recurring or delayed work."),
        "SubAgent" => Some("Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work."),
        "session_history" => Some("Read-only viewer over local Bamboo session history (list/inspect/search prior conversations). Distinct from `memory` (durable knowledge)."),
        "ExitPlanMode" => Some("Ask for confirmation before leaving plan mode."),
        _ => None,
    }
}

#[deprecated(note = "use expandable_tool_short_description for legacy guide summaries")]
pub fn discoverable_tool_short_description(name: &str) -> Option<&'static str> {
    expandable_tool_short_description(name)
}

/// Return the legacy expandable-guide names used by the compatibility API.
/// This is not the complete typed capability discovery catalog.
pub fn list_discoverable_tools() -> Vec<&'static str> {
    vec![
        "Sleep",
        "NotebookEdit",
        "js_repl",
        "WebFetch",
        "WebSearch",
        "memory",
        "scheduler",
        "SubAgent",
        "session_history",
        "ExitPlanMode",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_name_resolves_builtin_aliases() {
        assert_eq!(canonical_tool_name("FileExists"), "GetFileInfo");
        assert_eq!(canonical_tool_name("default::set_workspace"), "Workspace");
        assert_eq!(canonical_tool_name("default::applyPatch"), "Edit");
        assert_eq!(
            canonical_tool_name("mcp__filesystem__read_file"),
            "mcp__filesystem__read_file"
        );
    }

    #[test]
    fn expandable_guides_include_memory_and_subagent() {
        assert!(has_full_tool_guide("Bash"));
        assert!(has_expandable_tool_guide("memory"));
        assert!(has_expandable_tool_guide("SubAgent"));
        assert!(has_expandable_tool_guide("sub_session_manager"));
        assert!(!has_expandable_tool_guide("future_dynamic_tool"));
        assert!(list_discoverable_tools().contains(&"memory"));
        assert!(list_discoverable_tools().contains(&"SubAgent"));
        assert!(!list_discoverable_tools().contains(&"Workspace"));
        assert!(expandable_tool_short_description("memory").is_some());
        assert!(expandable_tool_short_description("SubAgent").is_some());
        assert!(expandable_tool_short_description("sub_session_manager").is_some());
    }

    #[test]
    fn discoverable_tools_roundtrip_via_session_metadata() {
        let mut session = Session::new("session-1", "model");
        activate_discoverable_tools(&mut session, ["Sleep", "scheduler", "Read"]);

        let activated = activated_discoverable_tools(&session);
        assert!(activated.contains("Sleep"));
        assert!(activated.contains("scheduler"));
        assert!(!activated.contains("Read"));
    }
}
