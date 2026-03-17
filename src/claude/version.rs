use log::{debug, warn};
use regex::Regex;
use std::cmp::Ordering;

use super::ClaudeInstallation;

pub(super) fn source_preference(installation: &ClaudeInstallation) -> u8 {
    match installation.source.as_str() {
        "which" => 1,
        "homebrew" => 2,
        "system" => 3,
        "nvm-active" => 4,
        source if source.starts_with("nvm") => 5,
        "local-bin" => 6,
        "claude-local" => 7,
        "npm-global" => 8,
        "yarn" | "yarn-global" => 9,
        "bun" => 10,
        "node-modules" => 11,
        "home-bin" => 12,
        "PATH" => 13,
        _ => 14,
    }
}

pub(super) fn get_claude_version(path: &str) -> Result<Option<String>, String> {
    match super::command::create_command_with_env(path)
        .arg("--version")
        .output()
    {
        Ok(output) => {
            if output.status.success() {
                Ok(extract_version_from_output(&output.stdout))
            } else {
                Ok(None)
            }
        }
        Err(e) => {
            warn!("Failed to get version for {}: {}", path, e);
            Ok(None)
        }
    }
}

pub(super) fn extract_version_from_output(stdout: &[u8]) -> Option<String> {
    let output_str = String::from_utf8_lossy(stdout);

    debug!("Raw version output: {:?}", output_str);

    let version_regex =
        Regex::new(r"(\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?(?:\+[a-zA-Z0-9.-]+)?)").ok()?;

    if let Some(captures) = version_regex.captures(&output_str) {
        if let Some(version_match) = captures.get(1) {
            let version = version_match.as_str().to_string();
            debug!("Extracted version: {:?}", version);
            return Some(version);
        }
    }

    debug!("No version found in output");
    None
}

pub(super) fn compare_versions(a: &str, b: &str) -> Ordering {
    let a_parts: Vec<u32> = a
        .split('.')
        .filter_map(|s| {
            s.chars()
                .take_while(|c| c.is_numeric())
                .collect::<String>()
                .parse()
                .ok()
        })
        .collect();

    let b_parts: Vec<u32> = b
        .split('.')
        .filter_map(|s| {
            s.chars()
                .take_while(|c| c.is_numeric())
                .collect::<String>()
                .parse()
                .ok()
        })
        .collect();

    for i in 0..std::cmp::max(a_parts.len(), b_parts.len()) {
        let a_val = a_parts.get(i).unwrap_or(&0);
        let b_val = b_parts.get(i).unwrap_or(&0);
        match a_val.cmp(b_val) {
            Ordering::Equal => continue,
            other => return other,
        }
    }

    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claude::{ClaudeInstallation, InstallationType};
    use std::cmp::Ordering;

    #[test]
    fn test_source_preference_which() {
        let installation = ClaudeInstallation {
            path: "/usr/bin/claude".to_string(),
            source: "which".to_string(),
            version: None,
            installation_type: InstallationType::System,
        };
        assert_eq!(source_preference(&installation), 1);
    }

    #[test]
    fn test_source_preference_homebrew() {
        let installation = ClaudeInstallation {
            path: "/opt/homebrew/bin/claude".to_string(),
            source: "homebrew".to_string(),
            version: None,
            installation_type: InstallationType::System,
        };
        assert_eq!(source_preference(&installation), 2);
    }

    #[test]
    fn test_source_preference_system() {
        let installation = ClaudeInstallation {
            path: "/usr/local/bin/claude".to_string(),
            source: "system".to_string(),
            version: None,
            installation_type: InstallationType::System,
        };
        assert_eq!(source_preference(&installation), 3);
    }

    #[test]
    fn test_source_preference_nvm_active() {
        let installation = ClaudeInstallation {
            path: "/home/user/.nvm/versions/node/v20/bin/claude".to_string(),
            source: "nvm-active".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 4);
    }

    #[test]
    fn test_source_preference_nvm() {
        let installation = ClaudeInstallation {
            path: "/home/user/.nvm/versions/node/v18/bin/claude".to_string(),
            source: "nvm-v18.0.0".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 5);
    }

    #[test]
    fn test_source_preference_local_bin() {
        let installation = ClaudeInstallation {
            path: "/home/user/.local/bin/claude".to_string(),
            source: "local-bin".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 6);
    }

    #[test]
    fn test_source_preference_claude_local() {
        let installation = ClaudeInstallation {
            path: "/home/user/.claude/bin/claude".to_string(),
            source: "claude-local".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 7);
    }

    #[test]
    fn test_source_preference_npm_global() {
        let installation = ClaudeInstallation {
            path: "/usr/lib/node_modules/claude-cli/bin/claude".to_string(),
            source: "npm-global".to_string(),
            version: None,
            installation_type: InstallationType::System,
        };
        assert_eq!(source_preference(&installation), 8);
    }

    #[test]
    fn test_source_preference_yarn() {
        let installation = ClaudeInstallation {
            path: "/home/user/.yarn/bin/claude".to_string(),
            source: "yarn".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 9);
    }

    #[test]
    fn test_source_preference_yarn_global() {
        let installation = ClaudeInstallation {
            path: "/home/user/.yarn/global/bin/claude".to_string(),
            source: "yarn-global".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 9);
    }

    #[test]
    fn test_source_preference_bun() {
        let installation = ClaudeInstallation {
            path: "/home/user/.bun/bin/claude".to_string(),
            source: "bun".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 10);
    }

    #[test]
    fn test_source_preference_node_modules() {
        let installation = ClaudeInstallation {
            path: "/home/user/project/node_modules/.bin/claude".to_string(),
            source: "node-modules".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 11);
    }

    #[test]
    fn test_source_preference_home_bin() {
        let installation = ClaudeInstallation {
            path: "/home/user/bin/claude".to_string(),
            source: "home-bin".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 12);
    }

    #[test]
    fn test_source_preference_path() {
        let installation = ClaudeInstallation {
            path: "/some/path/claude".to_string(),
            source: "PATH".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 13);
    }

    #[test]
    fn test_source_preference_unknown() {
        let installation = ClaudeInstallation {
            path: "/unknown/claude".to_string(),
            source: "unknown".to_string(),
            version: None,
            installation_type: InstallationType::Custom,
        };
        assert_eq!(source_preference(&installation), 14);
    }

    #[test]
    fn test_extract_version_simple() {
        let output = b"claude version 1.2.3\n";
        assert_eq!(
            extract_version_from_output(output),
            Some("1.2.3".to_string())
        );
    }

    #[test]
    fn test_extract_version_with_prefix() {
        let output = b"Claude CLI v2.0.0\n";
        assert_eq!(
            extract_version_from_output(output),
            Some("2.0.0".to_string())
        );
    }

    #[test]
    fn test_extract_version_with_prerelease() {
        let output = b"version 3.0.0-beta.1\n";
        assert_eq!(
            extract_version_from_output(output),
            Some("3.0.0-beta.1".to_string())
        );
    }

    #[test]
    fn test_extract_version_with_build_metadata() {
        let output = b"1.0.0+build.123\n";
        assert_eq!(
            extract_version_from_output(output),
            Some("1.0.0+build.123".to_string())
        );
    }

    #[test]
    fn test_extract_version_no_match() {
        let output = b"no version here\n";
        assert_eq!(extract_version_from_output(output), None);
    }

    #[test]
    fn test_extract_version_empty() {
        let output = b"";
        assert_eq!(extract_version_from_output(output), None);
    }

    #[test]
    fn test_extract_version_multiple_numbers() {
        let output = b"claude version 1.2.3, built on 2024-01-01\n";
        assert_eq!(
            extract_version_from_output(output),
            Some("1.2.3".to_string())
        );
    }

    #[test]
    fn test_compare_versions_equal() {
        assert_eq!(compare_versions("1.0.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("2.3.4", "2.3.4"), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_greater_major() {
        assert_eq!(compare_versions("2.0.0", "1.0.0"), Ordering::Greater);
    }

    #[test]
    fn test_compare_versions_greater_minor() {
        assert_eq!(compare_versions("1.2.0", "1.1.0"), Ordering::Greater);
    }

    #[test]
    fn test_compare_versions_greater_patch() {
        assert_eq!(compare_versions("1.0.2", "1.0.1"), Ordering::Greater);
    }

    #[test]
    fn test_compare_versions_less_major() {
        assert_eq!(compare_versions("1.0.0", "2.0.0"), Ordering::Less);
    }

    #[test]
    fn test_compare_versions_less_minor() {
        assert_eq!(compare_versions("1.1.0", "1.2.0"), Ordering::Less);
    }

    #[test]
    fn test_compare_versions_less_patch() {
        assert_eq!(compare_versions("1.0.1", "1.0.2"), Ordering::Less);
    }

    #[test]
    fn test_compare_versions_different_lengths() {
        assert_eq!(compare_versions("1.2.3.4", "1.2.3"), Ordering::Greater);
        assert_eq!(compare_versions("1.2", "1.2.0"), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_with_prerelease() {
        // Prerelease suffixes are stripped by take_while(|c| c.is_numeric())
        assert_eq!(compare_versions("1.0.0-beta", "1.0.0"), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_with_non_numeric() {
        // Non-numeric parts are filtered out
        assert_eq!(compare_versions("1.a.0", "1.0.0"), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_empty() {
        assert_eq!(compare_versions("", ""), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_one_empty() {
        assert_eq!(compare_versions("1.0.0", ""), Ordering::Greater);
        assert_eq!(compare_versions("", "1.0.0"), Ordering::Less);
    }
}
