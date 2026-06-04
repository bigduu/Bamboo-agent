//! Tool descriptors and the built-in tool catalog for the root SDK facade.
//!
//! The builder's [`tools`](super::AgentBuilder::tools) method accepts actual
//! tools — anything implementing the [`Tool`](bamboo_agent_core::tools::Tool)
//! trait, as `Arc<dyn Tool>`. The [`BuiltinTool`] enum is a *catalog* of the
//! tools that already ship with the runtime: it lets callers discover what is
//! available and obtain the real built-in implementation via
//! [`BuiltinTool::tool`]. Custom tools (your own `impl Tool`) can be passed the
//! same way.
//!
//! The canonical source of truth for built-in tool *names* is
//! [`bamboo_domain::tool_names::BUILTIN_TOOL_NAMES`]; [`BuiltinTool`] is kept in
//! lock-step with it by a drift-guard test.

use std::sync::OnceLock;

use bamboo_agent_core::tools::SharedTool;
use bamboo_domain::tool_names::BUILTIN_TOOL_NAMES;
use bamboo_tools::BuiltinToolExecutor;

/// A consumer-facing descriptor for a built-in tool (name + optional metadata).
///
/// `name` matches the canonical tool name used by the runtime
/// (see [`BUILTIN_TOOL_NAMES`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    /// Canonical tool name (matches `BUILTIN_TOOL_NAMES`).
    pub name: String,
    /// Short human description (best-effort; empty when none is known).
    pub description: String,
    /// When `true`, this tool is hidden from the agent's tool schema.
    pub disabled: bool,
}

impl ToolSpec {
    /// Construct an enabled `ToolSpec` for the given canonical tool name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            disabled: false,
        }
    }

    /// Set the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Mark this tool as disabled (hidden from the schema).
    pub fn disabled(mut self) -> Self {
        self.disabled = true;
        self
    }
}

/// Return the canonical list of built-in tool names.
pub fn builtin_tool_names() -> Vec<String> {
    BUILTIN_TOOL_NAMES.iter().map(|s| (*s).to_string()).collect()
}

/// Return a [`ToolSpec`] (enabled, no description) for every canonical built-in
/// tool, in the stable order defined by [`BUILTIN_TOOL_NAMES`].
pub fn builtin_tool_specs() -> Vec<ToolSpec> {
    BUILTIN_TOOL_NAMES.iter().map(|s| ToolSpec::new(*s)).collect()
}

/// Re-export of the canonical const array for callers that want the static
/// `&'static str` form.
pub use bamboo_domain::tool_names::BUILTIN_TOOL_NAMES as CANONICAL_TOOL_NAMES;

/// Shared default executor used solely to vend built-in tool *instances* by
/// name. Built once; cheap to clone the `Arc`s out of its registry.
fn default_builtin_executor() -> &'static BuiltinToolExecutor {
    static EXEC: OnceLock<BuiltinToolExecutor> = OnceLock::new();
    EXEC.get_or_init(BuiltinToolExecutor::new)
}

/// Type-safe catalog of the built-in tools.
///
/// This enum does **not** itself implement [`Tool`](bamboo_agent_core::tools::Tool);
/// it is a discovery aid. Call [`BuiltinTool::tool`] to get the real `Arc<dyn Tool>`
/// implementation to hand to [`AgentBuilder::tools`](super::AgentBuilder::tools):
///
/// ```rust,ignore
/// Agent::builder().tools([BuiltinTool::WebSearch.tool(), BuiltinTool::Read.tool()]);
/// ```
///
/// The variant set is kept in lock-step with [`BUILTIN_TOOL_NAMES`] by a
/// drift-guard test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinTool {
    ConclusionWithOptions,
    Bash,
    BashOutput,
    Edit,
    EnterPlanMode,
    ExitPlanMode,
    GetFileInfo,
    Glob,
    Grep,
    JsRepl,
    KillShell,
    SessionNote,
    NotebookEdit,
    Read,
    RequestPermissions,
    Sleep,
    Task,
    WebFetch,
    WebSearch,
    Workspace,
    Write,
}

impl BuiltinTool {
    /// Every built-in tool, in the canonical order of [`BUILTIN_TOOL_NAMES`].
    pub const ALL: [BuiltinTool; 21] = [
        BuiltinTool::ConclusionWithOptions,
        BuiltinTool::Bash,
        BuiltinTool::BashOutput,
        BuiltinTool::Edit,
        BuiltinTool::EnterPlanMode,
        BuiltinTool::ExitPlanMode,
        BuiltinTool::GetFileInfo,
        BuiltinTool::Glob,
        BuiltinTool::Grep,
        BuiltinTool::JsRepl,
        BuiltinTool::KillShell,
        BuiltinTool::SessionNote,
        BuiltinTool::NotebookEdit,
        BuiltinTool::Read,
        BuiltinTool::RequestPermissions,
        BuiltinTool::Sleep,
        BuiltinTool::Task,
        BuiltinTool::WebFetch,
        BuiltinTool::WebSearch,
        BuiltinTool::Workspace,
        BuiltinTool::Write,
    ];

    /// The canonical runtime name for this tool (matches [`BUILTIN_TOOL_NAMES`]).
    pub const fn name(self) -> &'static str {
        match self {
            BuiltinTool::ConclusionWithOptions => "conclusion_with_options",
            BuiltinTool::Bash => "Bash",
            BuiltinTool::BashOutput => "BashOutput",
            BuiltinTool::Edit => "Edit",
            BuiltinTool::EnterPlanMode => "EnterPlanMode",
            BuiltinTool::ExitPlanMode => "ExitPlanMode",
            BuiltinTool::GetFileInfo => "GetFileInfo",
            BuiltinTool::Glob => "Glob",
            BuiltinTool::Grep => "Grep",
            BuiltinTool::JsRepl => "js_repl",
            BuiltinTool::KillShell => "KillShell",
            BuiltinTool::SessionNote => "session_note",
            BuiltinTool::NotebookEdit => "NotebookEdit",
            BuiltinTool::Read => "Read",
            BuiltinTool::RequestPermissions => "request_permissions",
            BuiltinTool::Sleep => "Sleep",
            BuiltinTool::Task => "Task",
            BuiltinTool::WebFetch => "WebFetch",
            BuiltinTool::WebSearch => "WebSearch",
            BuiltinTool::Workspace => "Workspace",
            BuiltinTool::Write => "Write",
        }
    }

    /// The real built-in [`Tool`](ToolTrait) implementation as an
    /// `Arc<dyn Tool>`, ready to pass to
    /// [`AgentBuilder::tools`](super::AgentBuilder::tools).
    pub fn tool(self) -> SharedTool {
        default_builtin_executor()
            .registry()
            .get(self.name())
            .expect("built-in tool must be registered in the default executor")
    }

    /// Resolve a [`BuiltinTool`] from its canonical name, if it is a built-in.
    pub fn from_name(name: &str) -> Option<BuiltinTool> {
        BuiltinTool::ALL.into_iter().find(|t| t.name() == name)
    }
}

impl From<BuiltinTool> for SharedTool {
    fn from(tool: BuiltinTool) -> Self {
        tool.tool()
    }
}

impl std::fmt::Display for BuiltinTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_tool_names_match_canonical_const() {
        let names = builtin_tool_names();
        assert_eq!(names.len(), BUILTIN_TOOL_NAMES.len());
        for (got, want) in names.iter().zip(BUILTIN_TOOL_NAMES.iter()) {
            assert_eq!(got, want);
        }
    }

    #[test]
    fn builtin_tool_specs_are_enabled_by_default() {
        let specs = builtin_tool_specs();
        assert_eq!(specs.len(), BUILTIN_TOOL_NAMES.len());
        assert!(specs.iter().all(|s| !s.disabled));
    }

    #[test]
    fn tool_spec_builder_sets_fields() {
        let spec = ToolSpec::new("Read")
            .with_description("read a file")
            .disabled();
        assert_eq!(spec.name, "Read");
        assert_eq!(spec.description, "read a file");
        assert!(spec.disabled);
    }

    #[test]
    fn builtin_tool_enum_matches_canonical_names_exactly() {
        // Drift guard: the `BuiltinTool` enum must stay in lock-step with the
        // runtime's canonical built-in tool list (count, names, order).
        assert_eq!(BuiltinTool::ALL.len(), BUILTIN_TOOL_NAMES.len());
        for (tool, canonical) in BuiltinTool::ALL.iter().zip(BUILTIN_TOOL_NAMES.iter()) {
            assert_eq!(tool.name(), *canonical);
        }
    }

    #[test]
    fn builtin_tool_from_name_round_trips() {
        for tool in BuiltinTool::ALL {
            assert_eq!(BuiltinTool::from_name(tool.name()), Some(tool));
        }
        assert_eq!(BuiltinTool::from_name("NotARealTool"), None);
    }

    #[test]
    fn builtin_tool_vends_real_instance_with_matching_name() {
        // Every catalog variant resolves to a real Tool impl whose advertised
        // name matches the canonical name.
        for variant in BuiltinTool::ALL {
            let tool: SharedTool = variant.tool();
            assert_eq!(tool.name(), variant.name());
        }
    }
}
