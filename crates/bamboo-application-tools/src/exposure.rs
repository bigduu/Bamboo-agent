use std::collections::{BTreeSet, HashMap};

use bamboo_application_agent::normalize_tool_name;
use bamboo_application_agent::Session;
use bamboo_application_tool_types::tool_names::resolve_alias;

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
        "Sleep"
        | "NotebookEdit"
        | "js_repl"
        | "WebFetch"
        | "WebSearch"
        | "memory"
        | "scheduler"
        | "SubSession"
        | "sub_session_manager"
        | "recall" => ToolExposure::Discoverable,
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
    fn discoverable_tools_roundtrip_via_session_metadata() {
        let mut session = Session::new("session-1", "model");
        activate_discoverable_tools(&mut session, ["Sleep", "scheduler", "Read"]);

        let activated = activated_discoverable_tools(&session);
        assert!(activated.contains("Sleep"));
        assert!(activated.contains("scheduler"));
        assert!(!activated.contains("Read"));
    }
}
