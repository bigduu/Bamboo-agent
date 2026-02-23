use log::{debug, warn};
use std::path::PathBuf;
use std::process::Command;

use super::version::{extract_version_from_output, get_claude_version};
use super::{ClaudeInstallation, InstallationType};

pub(super) fn discover_system_installations() -> Vec<ClaudeInstallation> {
    let mut installations = Vec::new();

    if let Some(installation) = try_which_command() {
        installations.push(installation);
    }

    installations.extend(find_nvm_installations());
    installations.extend(find_standard_installations());

    let mut unique_paths = std::collections::HashSet::new();
    installations.retain(|install| unique_paths.insert(install.path.clone()));

    installations
}

#[cfg(unix)]
fn try_which_command() -> Option<ClaudeInstallation> {
    debug!("Trying 'which claude' to find binary...");

    match Command::new("which").arg("claude").output() {
        Ok(output) if output.status.success() => {
            let output_str = String::from_utf8_lossy(&output.stdout).trim().to_string();

            if output_str.is_empty() {
                return None;
            }

            let path = if output_str.starts_with("claude:") && output_str.contains("aliased to") {
                output_str
                    .split("aliased to")
                    .nth(1)
                    .map(|s| s.trim().to_string())
            } else {
                Some(output_str)
            }?;

            debug!("'which' found claude at: {}", path);

            if !PathBuf::from(&path).exists() {
                warn!("Path from 'which' does not exist: {}", path);
                return None;
            }

            let version = get_claude_version(&path).ok().flatten();

            Some(ClaudeInstallation {
                path,
                version,
                source: "which".to_string(),
                installation_type: InstallationType::System,
            })
        }
        _ => None,
    }
}

#[cfg(windows)]
fn try_which_command() -> Option<ClaudeInstallation> {
    debug!("Trying 'where claude' to find binary...");

    match Command::new("where").arg("claude").output() {
        Ok(output) if output.status.success() => {
            let output_str = String::from_utf8_lossy(&output.stdout).trim().to_string();

            if output_str.is_empty() {
                return None;
            }

            let path = output_str.lines().next().unwrap_or("").trim().to_string();

            if path.is_empty() {
                return None;
            }

            debug!("'where' found claude at: {}", path);

            if !PathBuf::from(&path).exists() {
                warn!("Path from 'where' does not exist: {}", path);
                return None;
            }

            let version = get_claude_version(&path).ok().flatten();

            Some(ClaudeInstallation {
                path,
                version,
                source: "where".to_string(),
                installation_type: InstallationType::System,
            })
        }
        _ => None,
    }
}

#[cfg(unix)]
fn find_nvm_installations() -> Vec<ClaudeInstallation> {
    let mut installations = Vec::new();

    if let Ok(nvm_bin) = std::env::var("NVM_BIN") {
        let claude_path = PathBuf::from(&nvm_bin).join("claude");
        if claude_path.exists() && claude_path.is_file() {
            debug!("Found Claude via NVM_BIN: {:?}", claude_path);
            let version = get_claude_version(&claude_path.to_string_lossy())
                .ok()
                .flatten();
            installations.push(ClaudeInstallation {
                path: claude_path.to_string_lossy().to_string(),
                version,
                source: "nvm-active".to_string(),
                installation_type: InstallationType::System,
            });
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let nvm_dir = PathBuf::from(&home)
            .join(".nvm")
            .join("versions")
            .join("node");

        debug!("Checking NVM directory: {:?}", nvm_dir);

        if let Ok(entries) = std::fs::read_dir(&nvm_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let claude_path = entry.path().join("bin").join("claude");

                    if claude_path.exists() && claude_path.is_file() {
                        let path_str = claude_path.to_string_lossy().to_string();
                        let node_version = entry.file_name().to_string_lossy().to_string();

                        debug!("Found Claude in NVM node {}: {}", node_version, path_str);

                        let version = get_claude_version(&path_str).ok().flatten();

                        installations.push(ClaudeInstallation {
                            path: path_str,
                            version,
                            source: format!("nvm ({})", node_version),
                            installation_type: InstallationType::System,
                        });
                    }
                }
            }
        }
    }

    installations
}

#[cfg(windows)]
fn find_nvm_installations() -> Vec<ClaudeInstallation> {
    let mut installations = Vec::new();

    if let Ok(nvm_home) = std::env::var("NVM_HOME") {
        debug!("Checking NVM_HOME directory: {:?}", nvm_home);

        if let Ok(entries) = std::fs::read_dir(&nvm_home) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let claude_path = entry.path().join("claude.exe");

                    if claude_path.exists() && claude_path.is_file() {
                        let path_str = claude_path.to_string_lossy().to_string();
                        let node_version = entry.file_name().to_string_lossy().to_string();

                        debug!("Found Claude in NVM node {}: {}", node_version, path_str);

                        let version = get_claude_version(&path_str).ok().flatten();

                        installations.push(ClaudeInstallation {
                            path: path_str,
                            version,
                            source: format!("nvm ({})", node_version),
                            installation_type: InstallationType::System,
                        });
                    }
                }
            }
        }
    }

    installations
}

#[cfg(unix)]
fn find_standard_installations() -> Vec<ClaudeInstallation> {
    let mut installations = Vec::new();

    let mut paths_to_check: Vec<(String, String)> = vec![
        ("/usr/local/bin/claude".to_string(), "system".to_string()),
        (
            "/opt/homebrew/bin/claude".to_string(),
            "homebrew".to_string(),
        ),
        ("/usr/bin/claude".to_string(), "system".to_string()),
        ("/bin/claude".to_string(), "system".to_string()),
    ];

    if let Ok(home) = std::env::var("HOME") {
        paths_to_check.extend(vec![
            (
                format!("{}/.claude/local/claude", home),
                "claude-local".to_string(),
            ),
            (
                format!("{}/.local/bin/claude", home),
                "local-bin".to_string(),
            ),
            (
                format!("{}/.npm-global/bin/claude", home),
                "npm-global".to_string(),
            ),
            (format!("{}/.yarn/bin/claude", home), "yarn".to_string()),
            (format!("{}/.bun/bin/claude", home), "bun".to_string()),
            (format!("{}/bin/claude", home), "home-bin".to_string()),
            (
                format!("{}/node_modules/.bin/claude", home),
                "node-modules".to_string(),
            ),
            (
                format!("{}/.config/yarn/global/node_modules/.bin/claude", home),
                "yarn-global".to_string(),
            ),
        ]);
    }

    for (path, source) in paths_to_check {
        let path_buf = PathBuf::from(&path);
        if path_buf.exists() && path_buf.is_file() {
            debug!("Found claude at standard path: {} ({})", path, source);

            let version = get_claude_version(&path).ok().flatten();

            installations.push(ClaudeInstallation {
                path,
                version,
                source,
                installation_type: InstallationType::System,
            });
        }
    }

    if let Ok(output) = Command::new("claude").arg("--version").output() {
        if output.status.success() {
            debug!("claude is available in PATH");
            let version = extract_version_from_output(&output.stdout);

            installations.push(ClaudeInstallation {
                path: "claude".to_string(),
                version,
                source: "PATH".to_string(),
                installation_type: InstallationType::System,
            });
        }
    }

    installations
}

#[cfg(windows)]
fn find_standard_installations() -> Vec<ClaudeInstallation> {
    let mut installations = Vec::new();

    let mut paths_to_check: Vec<(String, String)> = vec![];

    if let Ok(user_profile) = std::env::var("USERPROFILE") {
        paths_to_check.extend(vec![
            (
                format!("{}\\.claude\\local\\claude.exe", user_profile),
                "claude-local".to_string(),
            ),
            (
                format!("{}\\.local\\bin\\claude.exe", user_profile),
                "local-bin".to_string(),
            ),
            (
                format!("{}\\AppData\\Roaming\\npm\\claude.cmd", user_profile),
                "npm-global".to_string(),
            ),
            (
                format!("{}\\.yarn\\bin\\claude.cmd", user_profile),
                "yarn".to_string(),
            ),
            (
                format!("{}\\.bun\\bin\\claude.exe", user_profile),
                "bun".to_string(),
            ),
        ]);
    }

    for (path, source) in paths_to_check {
        let path_buf = PathBuf::from(&path);
        if path_buf.exists() && path_buf.is_file() {
            debug!("Found claude at standard path: {} ({})", path, source);

            let version = get_claude_version(&path).ok().flatten();

            installations.push(ClaudeInstallation {
                path,
                version,
                source,
                installation_type: InstallationType::System,
            });
        }
    }

    if let Ok(output) = Command::new("claude.exe").arg("--version").output() {
        if output.status.success() {
            debug!("claude.exe is available in PATH");
            let version = extract_version_from_output(&output.stdout);

            installations.push(ClaudeInstallation {
                path: "claude.exe".to_string(),
                version,
                source: "PATH".to_string(),
                installation_type: InstallationType::System,
            });
        }
    }

    installations
}
