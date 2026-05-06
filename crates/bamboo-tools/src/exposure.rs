use std::collections::{BTreeSet, HashMap};

use bamboo_agent_core::normalize_tool_name;
use bamboo_agent_core::Session;
use bamboo_domain::tool_names::resolve_alias;

pub const ACTIVATED_DISCOVERABLE_TOOLS_METADATA_KEY: &str = "activated_discoverable_tools";
const MAX_ACTIVATED_DISCOVERABLE_TOOLS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolExposure {
    Core,
    Discoverable,
}

pub fn canonical_tool_name(name: &str) -> String {
    let normalized = normalize_tool_name(name.trim());
    resolve_alias(normalized).unwrap_or(normalized).to_string()
}

pub fn exposure_for_tool_name(name: &str) -> ToolExposure {
    match canonical_tool_name(name).as_str() {
        // Lower-frequency or specialized tools stay discoverable by default.
        "Sleep" | "NotebookEdit" | "js_repl" | "WebFetch" | "WebSearch" | "memory"
        | "scheduler" | "SubAgent" | "session_history" | "ExitPlanMode" => {
            ToolExposure::Discoverable
        }
        _ => ToolExposure::Core,
    }
}

pub fn is_core_tool(name: &str) -> bool {
    matches!(exposure_for_tool_name(name), ToolExposure::Core)
}

pub fn is_discoverable_tool(name: &str) -> bool {
    matches!(exposure_for_tool_name(name), ToolExposure::Discoverable)
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
        .filter(|name| is_discoverable_tool(name))
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
        if is_discoverable_tool(&canonical) {
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

/// Short description for discoverable tools shown when not activated.
pub fn discoverable_tool_short_description(name: &str) -> Option<&'static str> {
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

/// Return the canonical names of all discoverable tools.
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
        assert_eq!(
            canonical_tool_name("default::set_workspace"),
            "set_workspace"
        );
    }

    #[test]
    fn discoverable_exposure_includes_memory_and_subagent() {
        assert!(is_discoverable_tool("memory"));
        assert!(is_discoverable_tool("SubAgent"));
        assert!(is_discoverable_tool("sub_session_manager"));
        assert!(list_discoverable_tools().contains(&"memory"));
        assert!(list_discoverable_tools().contains(&"SubAgent"));
        assert!(discoverable_tool_short_description("memory").is_some());
        assert!(discoverable_tool_short_description("SubAgent").is_some());
        assert!(discoverable_tool_short_description("sub_session_manager").is_some());
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
