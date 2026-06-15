//! User preferences gating which agent events become notifications.
//!
//! Each flag is an opt-out (defaulting to `true`) so that a freshly created
//! preferences file — or one written by an older client that omits a key —
//! still notifies on everything. Preferences are persisted as pretty JSON.

use std::path::Path;

use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

/// Per-user notification opt-in flags.
///
/// `enabled` is the master switch; when `false`, no notifications are produced
/// regardless of the per-category flags. Each per-category flag gates a single
/// notification [`category`](crate::NotificationCategory).
///
/// Every field defaults to `true` (via `serde(default = ...)`), so missing keys
/// in a partial JSON document are treated as opted-in.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NotificationPreferences {
    /// Master switch. When `false`, nothing is ever notified.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Notify when the agent needs free-form clarification from the user.
    #[serde(default = "default_true")]
    pub on_clarification: bool,
    /// Notify when a tool requires explicit user approval.
    #[serde(default = "default_true")]
    pub on_tool_approval: bool,
    /// Notify when context usage reaches a critical level.
    #[serde(default = "default_true")]
    pub on_context_pressure: bool,
    /// Notify when a background sub-agent task completes.
    #[serde(default = "default_true")]
    pub on_subagent_complete: bool,
}

impl Default for NotificationPreferences {
    fn default() -> Self {
        Self {
            enabled: true,
            on_clarification: true,
            on_tool_approval: true,
            on_context_pressure: true,
            on_subagent_complete: true,
        }
    }
}

impl NotificationPreferences {
    /// Loads preferences from `path`.
    ///
    /// Returns [`Default`] when the file is missing (a fresh install) so the
    /// caller never has to special-case first run. A file that exists but fails
    /// to parse is logged via [`tracing::warn`] and also falls back to
    /// [`Default`], so a corrupt file never disables notifications silently.
    pub fn load(path: &Path) -> Self {
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(_) => return Self::default(),
        };
        match serde_json::from_str(&contents) {
            Ok(prefs) => prefs,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to parse notification preferences; using defaults"
                );
                Self::default()
            }
        }
    }

    /// Persists preferences to `path` as pretty JSON.
    ///
    /// Creates the parent directory if it does not yet exist.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        std::fs::write(path, json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_all_true() {
        let prefs = NotificationPreferences::default();
        assert!(prefs.enabled);
        assert!(prefs.on_clarification);
        assert!(prefs.on_tool_approval);
        assert!(prefs.on_context_pressure);
        assert!(prefs.on_subagent_complete);
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("prefs.json");
        let prefs = NotificationPreferences {
            enabled: true,
            on_clarification: false,
            on_tool_approval: true,
            on_context_pressure: false,
            on_subagent_complete: true,
        };
        prefs.save(&path).unwrap();
        let loaded = NotificationPreferences::load(&path);
        assert_eq!(prefs, loaded);
    }

    #[test]
    fn missing_file_yields_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        assert_eq!(
            NotificationPreferences::load(&path),
            NotificationPreferences::default()
        );
    }

    #[test]
    fn partial_json_fills_missing_with_true() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.json");
        std::fs::write(&path, r#"{"on_clarification": false}"#).unwrap();
        let loaded = NotificationPreferences::load(&path);
        assert!(!loaded.on_clarification);
        // Every other field defaults to true.
        assert!(loaded.enabled);
        assert!(loaded.on_tool_approval);
        assert!(loaded.on_context_pressure);
        assert!(loaded.on_subagent_complete);
    }
}
