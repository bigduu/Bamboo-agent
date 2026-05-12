//! SubagentProfile data model.

use serde::{Deserialize, Serialize};

/// A subagent role definition.
///
/// `id` is the canonical lookup key and matches the `subagent_type` string
/// passed to the `SubAgent` tool (e.g. `"researcher"`, `"coder"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubagentProfile {
    /// Canonical role id (matches `subagent_type` string).
    pub id: String,

    /// Human-readable display name (used in UI tags / lists).
    pub display_name: String,

    /// Short description shown to users and to the LLM when listing roles.
    #[serde(default)]
    pub description: String,

    /// Role-specific system prompt injected as the first message of the
    /// child session.
    pub system_prompt: String,

    /// Tool policy applied to the child session's tool surface.
    #[serde(default)]
    pub tools: ToolPolicy,

    /// Optional model preference (lower priority than explicit
    /// `Config.subagent_models[id]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_hint: Option<ModelHint>,

    /// Optional default `responsibility` text used when the caller does
    /// not provide one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_responsibility: Option<String>,

    /// Optional UI hint (icon / colour) for frontend rendering.
    #[serde(default)]
    pub ui: UiHint,
}

/// Tool surface policy for a subagent profile.
///
/// - [`ToolPolicy::Inherit`] — use the default child tool set unchanged
///   (current behaviour).
/// - [`ToolPolicy::Allowlist`] — restrict the surface to the listed tools.
/// - [`ToolPolicy::Denylist`] — start from the default surface and remove
///   the listed tools.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ToolPolicy {
    /// Inherit the default child tool set.
    #[default]
    Inherit,
    /// Only allow the listed tool names.
    Allowlist {
        #[serde(default)]
        allow: Vec<String>,
    },
    /// Remove the listed tool names from the default child tool set.
    Denylist {
        #[serde(default)]
        deny: Vec<String>,
    },
}

/// Optional model preference for a subagent profile.
///
/// `tier` selects from configured tiers (`fast`, `chat`, `sub_agent`, ...).
/// `model_ref` overrides `tier` and pins an explicit `provider:model` ref.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelHint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,

    /// Explicit `provider:model` reference (overrides `tier`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<String>,
}

/// Optional UI presentation hint.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UiHint {
    /// Emoji or icon name displayed next to the role tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,

    /// Tag colour (free-form string, e.g. `"purple"` / `"#8b5cf6"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

/// Compute the set of tool names that should be hidden from the LLM schema
/// for a given [`ToolPolicy`].
///
/// - [`ToolPolicy::Inherit`] → returns an empty set.
/// - [`ToolPolicy::Allowlist`] → returns every tool in `all_tool_names` that
///   is **not** on the allow list (complement).
/// - [`ToolPolicy::Denylist`] → returns the tools on the deny list.
pub fn disabled_tools_for_profile(policy: &ToolPolicy, all_tool_names: &[String]) -> Vec<String> {
    match policy {
        ToolPolicy::Inherit => Vec::new(),
        ToolPolicy::Allowlist { allow } => {
            let allowed: std::collections::HashSet<&str> =
                allow.iter().map(|s| s.as_str()).collect();
            all_tool_names
                .iter()
                .filter(|name| !allowed.contains(name.as_str()))
                .cloned()
                .collect()
        }
        ToolPolicy::Denylist { deny } => deny.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_policy_default_is_inherit() {
        assert_eq!(ToolPolicy::default(), ToolPolicy::Inherit);
    }

    #[test]
    fn deserializes_inherit_policy() {
        let json = r#"{"mode":"inherit"}"#;
        let policy: ToolPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(policy, ToolPolicy::Inherit);
    }

    #[test]
    fn deserializes_allowlist_policy() {
        let json = r#"{"mode":"allowlist","allow":["Read","Grep"]}"#;
        let policy: ToolPolicy = serde_json::from_str(json).unwrap();
        match policy {
            ToolPolicy::Allowlist { allow } => assert_eq!(allow, vec!["Read", "Grep"]),
            other => panic!("expected allowlist, got {other:?}"),
        }
    }

    #[test]
    fn deserializes_denylist_policy() {
        let json = r#"{"mode":"denylist","deny":["Edit","Write"]}"#;
        let policy: ToolPolicy = serde_json::from_str(json).unwrap();
        match policy {
            ToolPolicy::Denylist { deny } => assert_eq!(deny, vec!["Edit", "Write"]),
            other => panic!("expected denylist, got {other:?}"),
        }
    }

    #[test]
    fn deserializes_full_profile() {
        let json = r#"{
            "id": "researcher",
            "display_name": "Researcher",
            "description": "Read-only investigation specialist",
            "system_prompt": "You are a researcher.",
            "tools": { "mode": "allowlist", "allow": ["Read", "Grep", "Glob"] },
            "model_hint": { "tier": "chat" },
            "default_responsibility": "Investigate the assigned topic",
            "ui": { "icon": "🔬", "color": "blue" }
        }"#;
        let profile: SubagentProfile = serde_json::from_str(json).unwrap();
        assert_eq!(profile.id, "researcher");
        assert_eq!(profile.display_name, "Researcher");
        assert_eq!(profile.ui.icon.as_deref(), Some("🔬"));
        assert_eq!(profile.model_hint.unwrap().tier.as_deref(), Some("chat"));
    }

    #[test]
    fn omits_optional_fields_when_serializing() {
        let profile = SubagentProfile {
            id: "minimal".into(),
            display_name: "Minimal".into(),
            description: String::new(),
            system_prompt: "hi".into(),
            tools: ToolPolicy::Inherit,
            model_hint: None,
            default_responsibility: None,
            ui: UiHint::default(),
        };
        let v = serde_json::to_value(&profile).unwrap();
        assert!(v.get("model_hint").is_none());
        assert!(v.get("default_responsibility").is_none());
    }

    #[test]
    fn disabled_tools_for_inherit_is_empty() {
        let policy = ToolPolicy::Inherit;
        let all = vec!["Read".into(), "Edit".into(), "Write".into()];
        assert!(disabled_tools_for_profile(&policy, &all).is_empty());
    }

    #[test]
    fn disabled_tools_for_allowlist_returns_complement() {
        let policy = ToolPolicy::Allowlist {
            allow: vec!["Read".into()],
        };
        let all = vec!["Read".into(), "Edit".into(), "Write".into()];
        let mut disabled = disabled_tools_for_profile(&policy, &all);
        disabled.sort();
        assert_eq!(disabled, vec!["Edit", "Write"]);
    }

    #[test]
    fn disabled_tools_for_allowlist_with_unknown_allowed_tool() {
        let policy = ToolPolicy::Allowlist {
            allow: vec!["Read".into(), "UnknownTool".into()],
        };
        let all = vec!["Read".into(), "Edit".into(), "Write".into()];
        let mut disabled = disabled_tools_for_profile(&policy, &all);
        disabled.sort();
        assert_eq!(disabled, vec!["Edit", "Write"]);
    }

    #[test]
    fn disabled_tools_for_denylist_returns_denied() {
        let policy = ToolPolicy::Denylist {
            deny: vec!["Edit".into(), "Write".into()],
        };
        let all = vec!["Read".into(), "Edit".into(), "Write".into()];
        assert_eq!(
            disabled_tools_for_profile(&policy, &all),
            vec!["Edit", "Write"]
        );
    }

    #[test]
    fn disabled_tools_for_denylist_ignores_all_tools() {
        let policy = ToolPolicy::Denylist {
            deny: vec!["SubAgent".into()],
        };
        assert_eq!(disabled_tools_for_profile(&policy, &[]), vec!["SubAgent"]);
    }
}
