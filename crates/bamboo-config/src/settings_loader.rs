use std::path::{Path, PathBuf};

use super::paths;
use super::settings::BambooSettings;

/// Effective settings resolved from all sources with proper priority.
///
/// Priority (highest → lowest):
/// 1. Managed (enterprise) settings (platform-specific, read-only)
/// 2. Local project settings (`<project>/.bamboo/settings.local.json`)
/// 3. Project settings (`<project>/.bamboo/settings.json`)
/// 4. User settings (`~/.bamboo/settings.json`)
#[derive(Debug, Clone)]
pub struct ResolvedSettings {
    pub settings: BambooSettings,
    pub source_paths: Vec<PathBuf>,
}

/// Load settings from all available sources and merge them.
///
/// If `project_dir` is `None`, only user-level settings are loaded.
pub fn load_settings(project_dir: Option<&Path>) -> ResolvedSettings {
    load_settings_from_dirs(&paths::user_settings_path(), project_dir)
}

/// Load settings with explicit directories (testable without global state).
fn load_settings_from_dirs(
    user_settings_path: &Path,
    project_dir: Option<&Path>,
) -> ResolvedSettings {
    let user = BambooSettings::load_from(user_settings_path);
    let mut source_paths = Vec::new();
    if user_settings_path.exists() {
        source_paths.push(user_settings_path.to_path_buf());
    }
    let mut settings = user;

    if let Some(proj_dir) = project_dir {
        let project_bamboo = proj_dir.join(".bamboo");

        // 3. Project settings (shared, committed to git)
        let project_path = project_bamboo.join("settings.json");
        let project_settings = BambooSettings::load_from(&project_path);
        if project_path.exists() {
            source_paths.push(project_path);
        }
        let mut merged = project_settings;
        merged.merge(&settings);
        settings = merged;

        // 2. Local project settings (gitignored, higher priority)
        let local_path = project_bamboo.join("settings.local.json");
        let local = BambooSettings::load_from(&local_path);
        if local_path.exists() {
            source_paths.push(local_path);
            let mut merged = local;
            merged.merge(&settings);
            settings = merged;
        }
    }

    // 1. Managed settings (enterprise, highest priority, read-only)
    let managed_path = paths::managed_settings_path();
    let managed = BambooSettings::load_from(&managed_path);
    if managed_path.exists() {
        source_paths.push(managed_path);
        let mut merged = managed;
        merged.merge(&settings);
        settings = merged;
    }

    ResolvedSettings {
        settings,
        source_paths,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_settings(dir: &Path, filename: &str, content: &str) {
        let path = dir.join(filename);
        std::fs::write(&path, content).unwrap();
    }

    #[test]
    fn test_load_settings_no_files() {
        let temp = TempDir::new().unwrap();
        let user_path = temp.path().join("settings.json"); // doesn't exist
        let resolved = load_settings_from_dirs(&user_path, None);
        assert!(resolved.settings.permission_mode.is_none());
        assert!(resolved.source_paths.is_empty());
    }

    #[test]
    fn test_load_settings_user_only() {
        let temp = TempDir::new().unwrap();
        let user_path = temp.path().join("settings.json");
        write_settings(
            temp.path(),
            "settings.json",
            r#"{"permissionMode": "plan"}"#,
        );

        let resolved = load_settings_from_dirs(&user_path, None);
        assert_eq!(
            resolved.settings.permission_mode,
            Some(crate::settings::PermissionMode::Plan)
        );
        assert_eq!(resolved.source_paths.len(), 1);
    }

    #[test]
    fn test_load_settings_project_overrides_user() {
        let temp = TempDir::new().unwrap();

        // User settings
        let user_dir = temp.path().join("user_bamboo");
        std::fs::create_dir_all(&user_dir).unwrap();
        write_settings(
            &user_dir,
            "settings.json",
            r#"{"permissionMode": "bypassPermissions", "defaultModel": "gpt-4"}"#,
        );

        // Project settings
        let project_dir = temp.path().join("my_project");
        let project_bamboo = project_dir.join(".bamboo");
        std::fs::create_dir_all(&project_bamboo).unwrap();
        write_settings(
            &project_bamboo,
            "settings.json",
            r#"{"permissionMode": "plan"}"#,
        );

        let resolved = load_settings_from_dirs(&user_dir.join("settings.json"), Some(&project_dir));
        // Project mode overrides user mode
        assert_eq!(
            resolved.settings.permission_mode,
            Some(crate::settings::PermissionMode::Plan)
        );
        // User model falls through (project doesn't set it)
        assert_eq!(resolved.settings.default_model, Some("gpt-4".to_string()));
    }

    #[test]
    fn test_load_settings_local_overrides_project() {
        let temp = TempDir::new().unwrap();

        // User settings
        let user_dir = temp.path().join("user_bamboo");
        std::fs::create_dir_all(&user_dir).unwrap();
        write_settings(&user_dir, "settings.json", r#"{}"#);

        // Project settings
        let project_dir = temp.path().join("my_project");
        let project_bamboo = project_dir.join(".bamboo");
        std::fs::create_dir_all(&project_bamboo).unwrap();
        write_settings(
            &project_bamboo,
            "settings.json",
            r#"{"permissionMode": "plan"}"#,
        );
        write_settings(
            &project_bamboo,
            "settings.local.json",
            r#"{"permissionMode": "acceptEdits"}"#,
        );

        let resolved = load_settings_from_dirs(&user_dir.join("settings.json"), Some(&project_dir));
        assert_eq!(
            resolved.settings.permission_mode,
            Some(crate::settings::PermissionMode::AcceptEdits)
        );
    }

    /// Test managed settings override via explicit managed path injection.
    /// The real `managed_settings_path()` returns a system path, so we test
    /// the merge logic by simulating a managed settings file in a temp dir.
    #[test]
    fn test_managed_settings_override_all() {
        let temp = TempDir::new().unwrap();

        // Simulated user settings
        let user_dir = temp.path().join("user_bamboo");
        std::fs::create_dir_all(&user_dir).unwrap();
        write_settings(
            &user_dir,
            "settings.json",
            r#"{"permissionMode": "acceptEdits", "defaultModel": "user-model"}"#,
        );

        // Simulated project settings
        let project_dir = temp.path().join("my_project");
        let project_bamboo = project_dir.join(".bamboo");
        std::fs::create_dir_all(&project_bamboo).unwrap();
        write_settings(
            &project_bamboo,
            "settings.json",
            r#"{"defaultModel": "project-model"}"#,
        );

        // Simulated managed settings (highest priority)
        let managed_dir = temp.path().join("managed");
        std::fs::create_dir_all(&managed_dir).unwrap();
        write_settings(
            &managed_dir,
            "settings.json",
            r#"{"permissionMode": "plan"}"#,
        );

        // Load user + project normally
        let mut resolved =
            load_settings_from_dirs(&user_dir.join("settings.json"), Some(&project_dir));

        // Now apply managed on top (simulating what load_settings_from_dirs does)
        let managed_path = managed_dir.join("settings.json");
        let managed = BambooSettings::load_from(&managed_path);
        let mut merged = managed;
        merged.merge(&resolved.settings);
        resolved.settings = merged;

        // Managed overrides permission mode
        assert_eq!(
            resolved.settings.permission_mode,
            Some(crate::settings::PermissionMode::Plan)
        );
        // User and project models merge through
        assert_eq!(
            resolved.settings.default_model,
            Some("project-model".to_string())
        );
    }
}
