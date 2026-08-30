//! Provider-neutral capability loading policy and classified tool catalogs.
//!
//! Provider adapters lower these logical classes to their own wire protocols.
//! The schema itself deliberately stays free of Anthropic/OpenAI fields.

use std::collections::{BTreeMap, BTreeSet};

use crate::{canonical_tool_name, ToolSchema, DISCOVER_CAPABILITY_NAME};

/// The loading class of a callable function schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CapabilityLoadingClass {
    /// Always present in the initial callable function set.
    Core,
    /// Eligible for discovery/loading; legacy providers may still receive it.
    Deferred,
    /// Retained for host compatibility but never advertised to a model.
    HostOnly,
}

/// How the current provider surface interprets eligible function schemas.
///
/// The default deliberately preserves Bamboo's existing full-catalog behavior.
/// Provider adapters opt into `Progressive` explicitly; an empty loaded-name
/// set cannot distinguish a progressive first round from a legacy request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CapabilityLoadingMode {
    #[default]
    LegacyFullCatalog,
    Progressive,
}

/// The complete and intentionally small always-resident function surface.
pub const CORE_TOOL_NAMES: [&str; 5] = ["Bash", "Read", "Grep", "Edit", "Write"];

/// Host protocol helpers that must not enter model catalogs or discovery.
pub const HOST_ONLY_TOOL_NAMES: [&str; 3] = [
    "Workspace",
    "conclusion_with_options",
    "request_permissions",
];

/// Examples whose deferred status is part of the public migration contract.
pub const EXPLICIT_DEFERRED_TOOL_NAMES: [&str; 5] = [
    "Glob",
    "GetFileInfo",
    "load_skill",
    "workflow_run",
    "update_goal",
];

/// Reserved provider-safe function name used only by Bamboo's compatibility
/// fallback adapter. Native provider search items map to the logical gateway
/// directly and do not use this function identity.
pub const DISCOVERY_CONTROL_FALLBACK_TOOL_NAME: &str = "discover_capabilities";

/// Trusted marker for Bamboo's separate, always-resident discovery gateway.
///
/// It is intentionally not constructed from a function schema or inferred by
/// name: a custom function called `discover` must remain ordinary Deferred
/// input and must not gain control-plane privileges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiscoveryControlGateway {
    _private: (),
}

impl DiscoveryControlGateway {
    pub const fn logical_name(self) -> &'static str {
        DISCOVER_CAPABILITY_NAME
    }

    pub const fn fallback_tool_name(self) -> &'static str {
        DISCOVERY_CONTROL_FALLBACK_TOOL_NAME
    }

    pub const fn is_initially_visible(self) -> bool {
        true
    }
}

pub const DISCOVERY_CONTROL_GATEWAY: DiscoveryControlGateway =
    DiscoveryControlGateway { _private: () };

/// Classify a host/model reference after canonical alias resolution.
///
/// This function is intentionally reference-oriented: callers that classify a
/// registered schema, build a logical catalog, or make admission decisions must
/// use [`ClassifiedToolIdentity`] / [`ClassifiedToolSchema`] instead. Their
/// exact-first registration semantics prevent a custom exact `bash` or
/// `apply_patch` registration from inheriting Core policy from a builtin alias.
/// Unknown references deliberately remain Deferred so a new builtin, server
/// overlay, plugin, desktop, MCP, or custom tool cannot silently expand the
/// always-resident surface.
pub fn capability_loading_class_for_reference(name: &str) -> CapabilityLoadingClass {
    let canonical = canonical_tool_name(name);
    if CORE_TOOL_NAMES
        .iter()
        .any(|core| core.eq_ignore_ascii_case(&canonical))
    {
        CapabilityLoadingClass::Core
    } else if HOST_ONLY_TOOL_NAMES
        .iter()
        .any(|host_only| host_only.eq_ignore_ascii_case(&canonical))
    {
        CapabilityLoadingClass::HostOnly
    } else {
        CapabilityLoadingClass::Deferred
    }
}

/// Immutable policy identity derived from a registered schema name.
///
/// Logical catalog identity, and the intended executor key, follow the
/// registered schema name exactly. Concrete executor routing is migrated in a
/// follow-up slice before loaded-state admission is enabled.
/// Alias fallback is retained separately because the shared resolution contract
/// is exact-first. Thus a custom exact `apply_patch` is a Deferred custom tool,
/// while an unshadowed invocation reference `apply_patch` may still resolve to
/// builtin `Edit`.
///
/// Classification is lexical policy, not implementation provenance: exact
/// reserved spellings select Bamboo's reserved loading class, but this wrapper
/// does not attest that the underlying implementation is a framework builtin.
/// Current registrars are host-trusted; any future untrusted registrar needs an
/// authoritative origin boundary before it may claim reserved names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedToolIdentity {
    execution_name: String,
    alias_fallback_name: String,
    loading_class: CapabilityLoadingClass,
}

impl ClassifiedToolIdentity {
    pub fn from_schema_name(schema_name: &str) -> Option<Self> {
        let registered_name = schema_name.trim();
        if registered_name.is_empty() || registered_name != schema_name {
            return None;
        }
        let alias_fallback_name = canonical_tool_name(registered_name);
        if alias_fallback_name == DISCOVERY_CONTROL_FALLBACK_TOOL_NAME {
            return None;
        }
        let exact_framework_identity = crate::BUILTIN_TOOL_NAMES
            .iter()
            .chain(crate::SERVER_CAPABILITY_NAMES.iter())
            .any(|known| *known == registered_name);
        let loading_class = if exact_framework_identity {
            capability_loading_class_for_reference(registered_name)
        } else if capability_loading_class_for_reference(&alias_fallback_name)
            == CapabilityLoadingClass::HostOnly
        {
            // Host protocol identities and their legacy aliases stay outside
            // every model catalog even when an exact custom registration would
            // otherwise shadow alias fallback.
            CapabilityLoadingClass::HostOnly
        } else {
            CapabilityLoadingClass::Deferred
        };
        Some(Self {
            execution_name: registered_name.to_string(),
            alias_fallback_name,
            loading_class,
        })
    }

    /// Exact registered name used as the logical catalog and executor key.
    /// This is deliberately not alias-canonicalized.
    pub fn execution_name(&self) -> &str {
        &self.execution_name
    }

    pub fn loading_class(&self) -> CapabilityLoadingClass {
        self.loading_class
    }

    pub fn alias_fallback_name(&self) -> &str {
        &self.alias_fallback_name
    }
}

/// Resolve a reference against the exact registered execution identities in a
/// catalog. Exact registrations win; only a missing exact name may fall back to
/// the shared legacy/builtin alias resolver.
///
/// The caller supplies the catalog membership predicate so config, discovery,
/// provider projection, and later admission can share these semantics without
/// coupling the domain policy to one collection type.
pub fn resolve_tool_reference_name(
    reference: &str,
    mut contains_registered_name: impl FnMut(&str) -> bool,
) -> Option<String> {
    let exact = reference.trim();
    if exact.is_empty() {
        return None;
    }
    if contains_registered_name(exact) {
        return Some(exact.to_string());
    }

    let unqualified = exact.rsplit("::").next().unwrap_or(exact).trim();
    if unqualified != exact && contains_registered_name(unqualified) {
        return Some(unqualified.to_string());
    }

    let legacy_normalized = crate::normalize_builtin_alias(unqualified);
    if legacy_normalized != unqualified && contains_registered_name(legacy_normalized) {
        return Some(legacy_normalized.to_string());
    }

    let fallback = canonical_tool_name(exact);
    (!fallback.is_empty() && contains_registered_name(&fallback)).then_some(fallback)
}

/// A logical catalog entry carrying provider-neutral loading policy alongside
/// an otherwise unchanged function schema.
#[derive(Clone)]
pub struct ClassifiedToolSchema {
    identity: ClassifiedToolIdentity,
    schema: ToolSchema,
}

impl std::fmt::Debug for ClassifiedToolSchema {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClassifiedToolSchema")
            .field("execution_name", &self.execution_name())
            .field("loading_class", &self.loading_class())
            .finish_non_exhaustive()
    }
}

impl ClassifiedToolSchema {
    /// Classify one non-empty function schema. Blank names fail closed rather
    /// than entering a provider or discovery catalog.
    pub fn new(schema: ToolSchema) -> Option<Self> {
        let identity = ClassifiedToolIdentity::from_schema_name(&schema.function.name)?;
        Some(Self { identity, schema })
    }

    pub fn execution_name(&self) -> &str {
        self.identity.execution_name()
    }

    pub fn loading_class(&self) -> CapabilityLoadingClass {
        self.identity.loading_class()
    }

    pub fn alias_fallback_name(&self) -> &str {
        self.identity.alias_fallback_name()
    }

    pub fn schema(&self) -> &ToolSchema {
        &self.schema
    }

    pub fn into_schema(self) -> ToolSchema {
        self.schema
    }

    pub fn is_model_visible(&self) -> bool {
        self.loading_class() != CapabilityLoadingClass::HostOnly
    }

    pub fn is_initially_visible(&self) -> bool {
        self.loading_class() == CapabilityLoadingClass::Core
    }
}

/// Provider-neutral callable membership at one conversation position.
///
/// Function identities use exact registered execution names. The complete
/// catalog name set is retained separately from the callable subset so an
/// unloaded exact registration still shadows legacy alias fallback. Bamboo's
/// trusted discovery gateway is intentionally exposed through its typed marker
/// rather than inserted as a string: an ordinary custom function named
/// `discover` remains Deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveCallableSet {
    catalog_execution_names: BTreeSet<String>,
    callable_execution_names: BTreeSet<String>,
}

impl EffectiveCallableSet {
    /// Resolve callable membership from an already eligible classified catalog.
    ///
    /// `loaded_execution_names` is the provider-adapter boundary: adapters
    /// extract names from their validated native/fallback history and this pure
    /// resolver canonicalizes them against the exact catalog, deduplicates them,
    /// and ignores unknown or HostOnly references. It neither parses provider
    /// payloads nor persists a second loaded-state representation.
    pub fn from_catalog<'a>(
        catalog: &[ClassifiedToolSchema],
        mode: CapabilityLoadingMode,
        loaded_execution_names: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let catalog_by_execution_name = catalog
            .iter()
            .map(|entry| (entry.execution_name(), entry))
            .collect::<BTreeMap<_, _>>();
        let catalog_execution_names = catalog_by_execution_name
            .keys()
            .map(|name| (*name).to_string())
            .collect::<BTreeSet<_>>();

        let mut callable_execution_names = catalog
            .iter()
            .filter(|entry| match mode {
                CapabilityLoadingMode::LegacyFullCatalog => entry.is_model_visible(),
                CapabilityLoadingMode::Progressive => entry.is_initially_visible(),
            })
            .map(|entry| entry.execution_name().to_string())
            .collect::<BTreeSet<_>>();

        if mode == CapabilityLoadingMode::Progressive {
            for loaded_reference in loaded_execution_names {
                let Some(execution_name) = resolve_tool_reference_name(loaded_reference, |name| {
                    catalog_by_execution_name.contains_key(name)
                }) else {
                    continue;
                };
                let Some(entry) = catalog_by_execution_name.get(execution_name.as_str()) else {
                    continue;
                };
                if entry.loading_class() == CapabilityLoadingClass::Deferred {
                    callable_execution_names.insert(execution_name);
                }
            }
        }

        Self {
            catalog_execution_names,
            callable_execution_names,
        }
    }

    /// Exact execution-name membership for provider projection and admission.
    pub fn contains_execution_name(&self, execution_name: &str) -> bool {
        self.callable_execution_names.contains(execution_name)
    }

    /// Resolve a model reference exact-first against the complete catalog, then
    /// admit it only when that exact identity belongs to the callable subset.
    pub fn resolve_callable_reference(&self, reference: &str) -> Option<String> {
        let execution_name = resolve_tool_reference_name(reference, |name| {
            self.catalog_execution_names.contains(name)
        })?;
        self.callable_execution_names
            .contains(&execution_name)
            .then_some(execution_name)
    }

    /// Callable function execution names in deterministic order.
    pub fn execution_names(&self) -> impl Iterator<Item = &str> {
        self.callable_execution_names.iter().map(String::as_str)
    }

    /// Bamboo's separate trusted discovery control surface.
    pub const fn discovery_gateway(&self) -> DiscoveryControlGateway {
        DISCOVERY_CONTROL_GATEWAY
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionSchema, BUILTIN_TOOL_ALIASES, BUILTIN_TOOL_NAMES, LEGACY_TOOL_NAME_ALIASES,
        SERVER_CAPABILITY_NAMES,
    };
    use serde_json::json;

    fn schema(name: &str) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: name.to_string(),
                description: format!("{name} description"),
                parameters: json!({"type": "object"}),
            },
        }
    }

    #[test]
    fn locks_exact_core_and_separate_discovery_gateway() {
        assert_eq!(CORE_TOOL_NAMES, ["Bash", "Read", "Grep", "Edit", "Write"]);
        for name in CORE_TOOL_NAMES {
            assert_eq!(
                capability_loading_class_for_reference(name),
                CapabilityLoadingClass::Core
            );
        }
        assert_eq!(DISCOVERY_CONTROL_GATEWAY.logical_name(), "discover");
        assert_eq!(
            DISCOVERY_CONTROL_GATEWAY.fallback_tool_name(),
            "discover_capabilities"
        );
        assert_ne!(
            DISCOVERY_CONTROL_GATEWAY.logical_name(),
            DISCOVERY_CONTROL_GATEWAY.fallback_tool_name()
        );
        assert!(DISCOVERY_CONTROL_GATEWAY.is_initially_visible());
        assert_eq!(
            capability_loading_class_for_reference(DISCOVER_CAPABILITY_NAME),
            CapabilityLoadingClass::Deferred,
            "the control gateway is outside the function Core set"
        );
        let ordinary_function = ClassifiedToolSchema::new(schema(DISCOVER_CAPABILITY_NAME))
            .expect("ordinary function schema");
        assert_eq!(
            ordinary_function.loading_class(),
            CapabilityLoadingClass::Deferred
        );
        assert!(!ordinary_function.is_initially_visible());
        assert!(ClassifiedToolSchema::new(schema(DISCOVERY_CONTROL_FALLBACK_TOOL_NAME)).is_none());
        assert!(ClassifiedToolSchema::new(schema("default::discover_capabilities")).is_none());
    }

    #[test]
    fn every_known_function_has_fail_closed_loading_policy() {
        for name in BUILTIN_TOOL_NAMES {
            let expected = if CORE_TOOL_NAMES.contains(&name) {
                CapabilityLoadingClass::Core
            } else if HOST_ONLY_TOOL_NAMES.contains(&name) {
                CapabilityLoadingClass::HostOnly
            } else {
                CapabilityLoadingClass::Deferred
            };
            assert_eq!(
                capability_loading_class_for_reference(name),
                expected,
                "{name}"
            );
        }
        for name in SERVER_CAPABILITY_NAMES {
            assert_eq!(
                capability_loading_class_for_reference(name),
                CapabilityLoadingClass::Deferred,
                "server overlay {name} must not become Core"
            );
        }
    }

    #[test]
    fn locks_required_deferred_and_host_only_names() {
        for name in EXPLICIT_DEFERRED_TOOL_NAMES {
            assert_eq!(
                capability_loading_class_for_reference(name),
                CapabilityLoadingClass::Deferred
            );
        }
        for name in HOST_ONLY_TOOL_NAMES {
            assert_eq!(
                capability_loading_class_for_reference(name),
                CapabilityLoadingClass::HostOnly
            );
        }
    }

    #[test]
    fn all_declared_aliases_share_their_target_classification() {
        for (alias, target) in BUILTIN_TOOL_ALIASES {
            assert_eq!(
                canonical_tool_name(alias),
                canonical_tool_name(target),
                "{alias}"
            );
            assert_eq!(
                capability_loading_class_for_reference(alias),
                capability_loading_class_for_reference(target),
                "{alias} -> {target}"
            );
        }
        assert_eq!(
            capability_loading_class_for_reference("applyPatch"),
            CapabilityLoadingClass::Core
        );
        assert_eq!(
            capability_loading_class_for_reference("default::getCurrentDir"),
            CapabilityLoadingClass::HostOnly
        );
        assert_eq!(
            capability_loading_class_for_reference("default::sub_task"),
            CapabilityLoadingClass::Deferred
        );
        for (alias, normalized) in LEGACY_TOOL_NAME_ALIASES {
            assert_eq!(
                canonical_tool_name(alias),
                canonical_tool_name(normalized),
                "legacy spelling {alias}"
            );
            assert_eq!(
                capability_loading_class_for_reference(alias),
                capability_loading_class_for_reference(normalized),
                "legacy spelling {alias}"
            );
        }
    }

    #[test]
    fn unknown_dynamic_and_namespaced_mcp_tools_default_deferred() {
        for name in [
            "future_builtin",
            "default::desktop_capture",
            "plugin_custom_tool",
            "mcp__alpha__read_file",
            "mcp__beta__read_file",
        ] {
            assert_eq!(
                capability_loading_class_for_reference(name),
                CapabilityLoadingClass::Deferred
            );
        }
    }

    #[test]
    fn exact_custom_alias_registrations_remain_deferred_execution_identities() {
        for name in [
            "apply_patch",
            "applyPatch",
            "read_file",
            "execute_command",
            "bash",
            "default::Bash",
        ] {
            let identity = ClassifiedToolIdentity::from_schema_name(name)
                .expect("exact custom registration remains catalogued");
            assert_eq!(identity.execution_name(), name);
            assert_eq!(
                identity.loading_class(),
                CapabilityLoadingClass::Deferred,
                "custom exact registration {name} must not gain reserved policy"
            );
        }
        let host_alias =
            ClassifiedToolIdentity::from_schema_name("GetCurrentDir").expect("host alias identity");
        assert_eq!(host_alias.execution_name(), "GetCurrentDir");
        assert_eq!(host_alias.loading_class(), CapabilityLoadingClass::HostOnly);
        assert_eq!(
            ClassifiedToolIdentity::from_schema_name("Bash")
                .expect("canonical builtin")
                .loading_class(),
            CapabilityLoadingClass::Core
        );
    }

    #[test]
    fn namespaced_custom_registration_stays_exact_and_reference_fallback_is_catalog_aware() {
        let classified = ClassifiedToolSchema::new(schema("default::custom_tool"))
            .expect("generic executors may expose exact namespaced schemas");
        assert_eq!(classified.execution_name(), "default::custom_tool");
        assert_eq!(classified.schema().function.name, "default::custom_tool");
        assert_eq!(classified.alias_fallback_name(), "custom_tool");
        assert_eq!(classified.loading_class(), CapabilityLoadingClass::Deferred);

        let registered = ["Bash", "custom_tool"];
        assert_eq!(
            resolve_tool_reference_name("default::Bash", |name| registered.contains(&name)),
            Some("Bash".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("default::custom_tool", |name| registered.contains(&name)),
            Some("custom_tool".to_string())
        );
    }

    #[test]
    fn catalog_reference_resolution_is_exact_first_then_alias_fallback() {
        let shadowed = ["Edit", "apply_patch", "default::applyPatch"];
        assert_eq!(
            resolve_tool_reference_name("Edit", |name| shadowed.contains(&name)),
            Some("Edit".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("apply_patch", |name| shadowed.contains(&name)),
            Some("apply_patch".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("applyPatch", |name| shadowed.contains(&name)),
            Some("apply_patch".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("default::applyPatch", |name| shadowed.contains(&name)),
            Some("default::applyPatch".to_string())
        );

        let shadowed_without_namespaced_exact = ["Edit", "apply_patch"];
        assert_eq!(
            resolve_tool_reference_name("default::applyPatch", |name| {
                shadowed_without_namespaced_exact.contains(&name)
            }),
            Some("apply_patch".to_string())
        );

        let unshadowed = ["Edit"];
        assert_eq!(
            resolve_tool_reference_name("apply_patch", |name| unshadowed.contains(&name)),
            Some("Edit".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("default::applyPatch", |name| unshadowed.contains(&name)),
            Some("Edit".to_string())
        );
        assert_eq!(
            resolve_tool_reference_name("unknown", |name| unshadowed.contains(&name)),
            None
        );
    }

    #[test]
    fn classified_wrapper_keeps_schema_provider_neutral() {
        let classified = ClassifiedToolSchema::new(schema("Edit")).expect("classified schema");
        assert_eq!(classified.execution_name(), "Edit");
        assert_eq!(classified.loading_class(), CapabilityLoadingClass::Core);
        assert!(classified.is_model_visible());
        assert!(classified.is_initially_visible());

        let serialized = serde_json::to_value(classified.schema()).expect("serialize schema");
        assert_eq!(serialized["function"]["name"], "Edit");
        assert!(serialized.get("loading_class").is_none());
        assert!(serialized.get("defer_loading").is_none());
    }

    #[test]
    fn host_only_wrapper_is_not_model_visible() {
        let classified = ClassifiedToolSchema::new(schema("Workspace")).expect("classified schema");
        assert_eq!(classified.execution_name(), "Workspace");
        assert_eq!(classified.loading_class(), CapabilityLoadingClass::HostOnly);
        assert!(!classified.is_model_visible());
        assert!(!classified.is_initially_visible());
    }

    #[test]
    fn blank_schema_names_fail_closed() {
        assert!(ClassifiedToolSchema::new(schema("  ")).is_none());
    }

    #[test]
    fn classified_debug_never_emits_schema_content() {
        const SECRET: &str = "private-classified-schema-sentinel";
        let mut sensitive = schema("custom_tool");
        sensitive.schema_type = SECRET.to_string();
        sensitive.function.description = SECRET.to_string();
        sensitive.function.parameters = json!({"secret": SECRET});
        let classified = ClassifiedToolSchema::new(sensitive).expect("classified schema");
        let debug = format!("{classified:?}");

        assert!(!debug.contains(SECRET));
        assert!(debug.contains("custom_tool"));
        assert!(debug.contains("Deferred"));
    }

    fn classified_catalog(names: &[&str]) -> Vec<ClassifiedToolSchema> {
        names
            .iter()
            .map(|name| schema(name))
            .filter_map(ClassifiedToolSchema::new)
            .collect()
    }

    #[test]
    fn legacy_effective_set_keeps_core_and_deferred_but_excludes_host_only() {
        let catalog = classified_catalog(&["Read", "Glob", "custom_tool", "Workspace"]);
        let effective = EffectiveCallableSet::from_catalog(
            &catalog,
            CapabilityLoadingMode::LegacyFullCatalog,
            std::iter::empty::<&str>(),
        );

        assert_eq!(
            effective.execution_names().collect::<Vec<_>>(),
            vec!["Glob", "Read", "custom_tool"]
        );
        assert_eq!(effective.discovery_gateway(), DISCOVERY_CONTROL_GATEWAY);
        assert!(effective.discovery_gateway().is_initially_visible());
    }

    #[test]
    fn progressive_effective_set_adds_only_loaded_eligible_deferred() {
        let catalog = classified_catalog(&["Read", "Glob", "custom_tool", "Workspace"]);
        let effective = EffectiveCallableSet::from_catalog(
            &catalog,
            CapabilityLoadingMode::Progressive,
            ["Glob", "Workspace", "unknown_tool", "Glob"],
        );

        assert_eq!(
            effective.execution_names().collect::<Vec<_>>(),
            vec!["Glob", "Read"]
        );
        assert!(!effective.contains_execution_name("custom_tool"));
        assert!(!effective.contains_execution_name("Workspace"));
        assert!(!effective.contains_execution_name("unknown_tool"));
    }

    #[test]
    fn callable_reference_resolution_preserves_exact_shadow_before_alias_fallback() {
        let catalog = classified_catalog(&["Edit", "apply_patch"]);
        let core_only = EffectiveCallableSet::from_catalog(
            &catalog,
            CapabilityLoadingMode::Progressive,
            std::iter::empty::<&str>(),
        );

        assert_eq!(
            core_only.resolve_callable_reference("Edit"),
            Some("Edit".to_string())
        );
        assert_eq!(core_only.resolve_callable_reference("apply_patch"), None);
        assert_eq!(core_only.resolve_callable_reference("applyPatch"), None);

        let with_custom = EffectiveCallableSet::from_catalog(
            &catalog,
            CapabilityLoadingMode::Progressive,
            ["apply_patch"],
        );
        assert_eq!(
            with_custom.resolve_callable_reference("apply_patch"),
            Some("apply_patch".to_string())
        );
        assert_eq!(
            with_custom.resolve_callable_reference("applyPatch"),
            Some("apply_patch".to_string())
        );
    }

    #[test]
    fn trusted_discovery_gateway_is_separate_from_deferred_discover_function() {
        let catalog = classified_catalog(&["discover"]);
        let effective = EffectiveCallableSet::from_catalog(
            &catalog,
            CapabilityLoadingMode::Progressive,
            std::iter::empty::<&str>(),
        );

        assert_eq!(effective.resolve_callable_reference("discover"), None);
        assert_eq!(effective.discovery_gateway().logical_name(), "discover");
        assert!(effective.discovery_gateway().is_initially_visible());
    }
}
