use log::{debug, warn};
use regex::Regex;
use std::cmp::Ordering;
use std::process::Command;

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
    match Command::new(path).arg("--version").output() {
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
