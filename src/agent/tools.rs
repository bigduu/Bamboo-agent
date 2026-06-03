//! Ergonomic tool descriptors for the root SDK facade.
//!
//! [`ToolSpec`] is a lightweight, consumer-facing description of a built-in
//! tool. The canonical source of truth for tool *names* is
//! [`bamboo_domain::tool_names::BUILTIN_TOOL_NAMES`] — this module derives its
//! list from that const array rather than re-listing names, so the SDK can never
//! drift out of sync with the runtime's actual tool surface.

use bamboo_domain::tool_names::BUILTIN_TOOL_NAMES;

/// A consumer-facing descriptor for a built-in tool.
///
/// `name` matches the canonical tool name used by the runtime
/// (see [`BUILTIN_TOOL_NAMES`]). `disabled` lets SDK consumers mark a tool as
/// hidden from a given agent's schema without removing it from the catalog.
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
///
/// This is a thin re-export of [`BUILTIN_TOOL_NAMES`] as owned `String`s so SDK
/// consumers can enumerate the tool surface without importing `bamboo_domain`.
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
}
