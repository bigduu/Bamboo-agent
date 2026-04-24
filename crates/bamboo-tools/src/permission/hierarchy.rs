use serde_json::Value;

use crate::permission::config::{PermissionConfig, PermissionRule, PermissionType};
use crate::permission::rule_parser::ParsedRule;

/// A set of allow and deny rules extracted from a single config source.
#[derive(Debug, Clone, Default)]
pub struct PermissionRuleSet {
    pub allow: Vec<PermissionRule>,
    pub deny: Vec<PermissionRule>,
    /// Parsed tool-pattern rules (from allowed_tools/denied_tools lists).
    parsed_allow: Vec<ParsedRule>,
    parsed_deny: Vec<ParsedRule>,
}

impl PermissionRuleSet {
    pub fn from_config(config: &PermissionConfig) -> Self {
        let mut set = Self::default();
        for rule in config.get_rules() {
            if rule.allowed {
                set.allow.push(rule);
            } else {
                set.deny.push(rule);
            }
        }
        set
    }

    /// Merge `other` into this set, with `other` taking higher priority.
    /// Deny rules are always accumulated (never overridden). Allow rules
    /// from `other` replace matching rules in `self`.
    pub fn merge(&mut self, other: &PermissionRuleSet) {
        // Always accumulate deny rules from all levels
        for rule in &other.deny {
            if !self.deny.iter().any(|r| {
                r.resource_pattern == rule.resource_pattern && r.tool_type == rule.tool_type
            }) {
                self.deny.push(rule.clone());
            }
        }
        // Allow rules from other override self's matching allow rules
        for rule in &other.allow {
            if !self.allow.iter().any(|r| {
                r.resource_pattern == rule.resource_pattern && r.tool_type == rule.tool_type
            }) {
                self.allow.push(rule.clone());
            }
        }
    }

    /// Check if the given permission is allowed by this rule set.
    /// Returns `Some(true)` if explicitly allowed, `Some(false)` if denied, `None` if no rule.
    pub fn check(&self, perm_type: PermissionType, resource: &str) -> Option<bool> {
        // Deny always takes precedence
        for rule in &self.deny {
            if rule.matches(perm_type, resource) {
                return Some(false);
            }
        }
        for rule in &self.allow {
            if rule.matches(perm_type, resource) {
                return Some(true);
            }
        }
        None
    }

    /// Create a rule set from raw tool-pattern strings (e.g., `"Bash(npm run *)"`).
    pub fn from_rules(allowed: &[String], denied: &[String]) -> Self {
        let parsed_allow = allowed.iter().map(|s| ParsedRule::parse(s)).collect();
        let parsed_deny = denied.iter().map(|s| ParsedRule::parse(s)).collect();
        Self {
            allow: Vec::new(),
            deny: Vec::new(),
            parsed_allow,
            parsed_deny,
        }
    }

    /// Match a tool call against parsed tool-pattern rules.
    /// Returns `Some(false)` if denied, `Some(true)` if allowed, `None` if no rule matches.
    pub fn match_tool_call(&self, tool_name: &str, args: &Value) -> Option<bool> {
        for rule in &self.parsed_deny {
            if rule.matches_tool_call(tool_name, args) {
                return Some(false);
            }
        }
        for rule in &self.parsed_allow {
            if rule.matches_tool_call(tool_name, args) {
                return Some(true);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::PermissionRule;

    #[test]
    fn test_rule_set_from_config() {
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/safe/*",
            true,
        ));
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/safe/secret",
            false,
        ));

        let set = PermissionRuleSet::from_config(&config);
        assert_eq!(set.allow.len(), 1);
        assert_eq!(set.deny.len(), 1);
    }

    #[test]
    fn test_rule_set_check() {
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/safe/*",
            true,
        ));
        config.add_rule(PermissionRule::new(
            PermissionType::WriteFile,
            "/safe/secret",
            false,
        ));

        let set = PermissionRuleSet::from_config(&config);
        assert_eq!(
            set.check(PermissionType::WriteFile, "/safe/code.rs"),
            Some(true)
        );
        assert_eq!(
            set.check(PermissionType::WriteFile, "/safe/secret"),
            Some(false)
        );
        assert_eq!(set.check(PermissionType::WriteFile, "/other/file.rs"), None);
    }

    #[test]
    fn test_rule_set_merge_accumulates_denies() {
        let mut user = PermissionRuleSet::default();
        user.deny.push(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "curl *",
            false,
        ));

        let mut project = PermissionRuleSet::default();
        project.deny.push(PermissionRule::new(
            PermissionType::WriteFile,
            "./.env",
            false,
        ));
        project.allow.push(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "npm run *",
            true,
        ));

        user.merge(&project);
        // Deny from both sources present
        assert_eq!(user.deny.len(), 2);
        assert_eq!(user.allow.len(), 1);
    }

    #[test]
    fn test_rule_set_deny_overrides_allow() {
        let mut set = PermissionRuleSet::default();
        set.allow.push(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/*",
            true,
        ));
        set.deny.push(PermissionRule::new(
            PermissionType::WriteFile,
            "/tmp/secret",
            false,
        ));

        // Deny takes precedence even if allow also matches
        assert_eq!(
            set.check(PermissionType::WriteFile, "/tmp/secret"),
            Some(false)
        );
    }
}
