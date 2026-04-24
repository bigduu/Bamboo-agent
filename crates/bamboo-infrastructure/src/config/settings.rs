use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Permission mode controlling how the system handles permission requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionMode {
    /// Default interactive mode: prompt for all dangerous operations.
    #[default]
    Default,
    /// Plan (read-only) mode: deny all mutating tool calls, allow read-only tools.
    Plan,
    /// Accept edits mode: auto-approve file writes, prompt for command execution.
    AcceptEdits,
    /// Don't ask mode: auto-deny unless pre-approved by whitelist.
    DontAsk,
    /// Bypass all permission checks (dangerous, intended for CI/testing only).
    BypassPermissions,
}

impl PermissionMode {
    pub fn description(&self) -> &'static str {
        match self {
            PermissionMode::Default => "Interactive mode: prompt for dangerous operations",
            PermissionMode::Plan => "Plan mode: read-only, no mutations allowed",
            PermissionMode::AcceptEdits => "Accept edits: auto-approve file writes",
            PermissionMode::DontAsk => "Don't ask: auto-deny unless whitelisted",
            PermissionMode::BypassPermissions => "Bypass: skip all permission checks",
        }
    }
}

/// Bamboo settings loaded from settings.json files.
///
/// Supports three levels: user (`~/.bamboo/settings.json`),
/// project (`<project>/.bamboo/settings.json`), and local project
/// (`<project>/.bamboo/settings.local.json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BambooSettings {
    /// Active permission mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
    /// Default model name override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    /// Additional allowed tools (glob patterns).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    /// Explicitly denied tools (overrides allow at any level).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tools: Vec<String>,
    /// Extension fields for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl BambooSettings {
    /// Load settings from a single file, returning default if not found.
    pub fn load_from(path: &Path) -> Self {
        if !path.exists() {
            return Self::default();
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Failed to read settings from {}: {e}", path.display());
                return Self::default();
            }
        };
        match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to parse settings from {}: {e}", path.display());
                Self::default()
            }
        }
    }

    /// Merge `lower_priority` into `self`, with `self` taking precedence.
    ///
    /// Scalar values (mode, model) use `self` if set, otherwise fall back to `lower_priority`.
    /// Arrays (allowed_tools, denied_tools) are concatenated and deduplicated.
    /// Extra keys are deep-merged with `self` winning on conflicts.
    pub fn merge(&mut self, lower_priority: &Self) {
        if self.permission_mode.is_none() && lower_priority.permission_mode.is_some() {
            self.permission_mode = lower_priority.permission_mode;
        }
        if self.default_model.is_none() && lower_priority.default_model.is_some() {
            self.default_model = lower_priority.default_model.clone();
        }
        // Concatenate and deduplicate arrays
        for tool in &lower_priority.allowed_tools {
            if !self.allowed_tools.contains(tool) {
                self.allowed_tools.push(tool.clone());
            }
        }
        for tool in &lower_priority.denied_tools {
            if !self.denied_tools.contains(tool) {
                self.denied_tools.push(tool.clone());
            }
        }
        // Merge extra: lower_priority keys only if not in self
        for (key, value) in &lower_priority.extra {
            self.extra.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn test_bamboo_settings_default() {
        let settings = BambooSettings::default();
        assert!(settings.permission_mode.is_none());
        assert!(settings.default_model.is_none());
        assert!(settings.allowed_tools.is_empty());
        assert!(settings.denied_tools.is_empty());
    }

    #[test]
    fn test_bamboo_settings_merge_scalar_precedence() {
        let mut higher = BambooSettings {
            permission_mode: Some(PermissionMode::AcceptEdits),
            default_model: Some("gpt-4".to_string()),
            ..Default::default()
        };
        let lower = BambooSettings {
            permission_mode: Some(PermissionMode::Plan),
            default_model: Some("claude-3".to_string()),
            ..Default::default()
        };

        higher.merge(&lower);
        // Higher priority wins
        assert_eq!(higher.permission_mode, Some(PermissionMode::AcceptEdits));
        assert_eq!(higher.default_model, Some("gpt-4".to_string()));
    }

    #[test]
    fn test_bamboo_settings_merge_falls_back_to_lower() {
        let mut higher = BambooSettings::default();
        let lower = BambooSettings {
            permission_mode: Some(PermissionMode::Plan),
            default_model: Some("claude-3".to_string()),
            ..Default::default()
        };

        higher.merge(&lower);
        assert_eq!(higher.permission_mode, Some(PermissionMode::Plan));
        assert_eq!(higher.default_model, Some("claude-3".to_string()));
    }

    #[test]
    fn test_bamboo_settings_merge_arrays_concatenated() {
        let mut higher = BambooSettings {
            allowed_tools: vec!["Bash(npm run *)".to_string()],
            denied_tools: vec!["Bash(curl *)".to_string()],
            ..Default::default()
        };
        let lower = BambooSettings {
            allowed_tools: vec!["Write(/src/**)".to_string()],
            denied_tools: vec!["Read(./.env)".to_string()],
            ..Default::default()
        };

        higher.merge(&lower);
        assert_eq!(higher.allowed_tools.len(), 2);
        assert_eq!(higher.denied_tools.len(), 2);
        assert!(higher.allowed_tools.contains(&"Bash(npm run *)".to_string()));
        assert!(higher.allowed_tools.contains(&"Write(/src/**)".to_string()));
    }

    #[test]
    fn test_bamboo_settings_merge_arrays_deduplicated() {
        let mut higher = BambooSettings {
            allowed_tools: vec!["Bash(npm run *)".to_string()],
            ..Default::default()
        };
        let lower = BambooSettings {
            allowed_tools: vec!["Bash(npm run *)".to_string()],
            ..Default::default()
        };

        higher.merge(&lower);
        assert_eq!(higher.allowed_tools.len(), 1);
    }

    #[test]
    fn test_bamboo_settings_load_from_missing_file() {
        let settings = BambooSettings::load_from(Path::new("/nonexistent/settings.json"));
        assert!(settings.permission_mode.is_none());
    }

    #[test]
    fn test_bamboo_settings_load_from_valid_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("settings.json");
        std::fs::write(
            &path,
            r#"{"permissionMode": "plan", "defaultModel": "claude-4"}"#,
        )
        .unwrap();

        let settings = BambooSettings::load_from(&path);
        assert_eq!(settings.permission_mode, Some(PermissionMode::Plan));
        assert_eq!(settings.default_model, Some("claude-4".to_string()));
    }

    #[test]
    fn test_bamboo_settings_load_from_invalid_json() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("settings.json");
        std::fs::write(&path, "not json").unwrap();

        let settings = BambooSettings::load_from(&path);
        assert!(settings.permission_mode.is_none());
    }

    #[test]
    fn test_permission_mode_serialize_roundtrip() {
        let modes = [
            PermissionMode::Default,
            PermissionMode::Plan,
            PermissionMode::AcceptEdits,
            PermissionMode::DontAsk,
            PermissionMode::BypassPermissions,
        ];
        for mode in modes {
            let json = serde_json::to_string(&mode).unwrap();
            let back: PermissionMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }
}
