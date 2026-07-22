//! Shared Codex CLI discovery and capability preflight.
//!
//! Both the worker executor and the settings HTTP endpoint call this module so
//! a successful UI "Detect" result is exactly the binary the worker can use.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::process::Command;

/// The oldest Codex CLI schema Bamboo intentionally supports.
pub const MIN_CODEX_VERSION: (u64, u64, u64) = (0, 144, 0);

/// A discovery request must not leave a settings request or worker spawn
/// waiting forever on a broken wrapper executable.
const PREFLIGHT_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// A resolved Codex executable that passed version and capability checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexCliDiscovery {
    pub path: String,
    pub version: String,
}

/// Resolve `binary` (or `codex` from `PATH`) and verify the exact CLI surface
/// required by the executor.
pub async fn discover_codex_cli(binary: Option<&str>) -> Result<CodexCliDiscovery, String> {
    let requested = binary
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("codex");
    let resolved = resolve_binary(requested).ok_or_else(|| missing_binary_error(requested))?;
    ensure_executable(&resolved)?;

    let version_output = command_output(&resolved, &["--version"]).await?;
    if !version_output.status.success() {
        return Err(format!(
            "'{} --version' failed with status {}; reinstall or upgrade Codex CLI",
            resolved.display(),
            version_output.status
        ));
    }
    let version = String::from_utf8_lossy(&version_output.stdout)
        .trim()
        .to_string();
    let parsed_version = parse_codex_version(&version).ok_or_else(|| {
        format!("could not parse Codex CLI version from {version:?}; expected `codex-cli X.Y.Z`")
    })?;
    if parsed_version < MIN_CODEX_VERSION {
        return Err(format!(
            "Codex CLI {version} is too old; Bamboo requires >= {}.{}.{} with `exec --json` and `exec resume`",
            MIN_CODEX_VERSION.0, MIN_CODEX_VERSION.1, MIN_CODEX_VERSION.2
        ));
    }

    verify_help_surface(
        &resolved,
        &["exec", "--help"],
        &[
            "--json",
            "--output-last-message",
            "--config",
            "--sandbox",
            "--dangerously-bypass-approvals-and-sandbox",
            "stdin",
        ],
    )
    .await?;
    verify_help_surface(&resolved, &["exec", "resume", "--help"], &["--json"]).await?;

    Ok(CodexCliDiscovery {
        path: resolved.to_string_lossy().into_owned(),
        version,
    })
}

/// Parse `codex-cli X.Y.Z` while tolerating vendor prefixes and a missing
/// patch component.
pub fn parse_codex_version(text: &str) -> Option<(u64, u64, u64)> {
    let token = text
        .split_whitespace()
        .map(|token| token.trim_start_matches('v'))
        .find(|token| token.chars().next().is_some_and(|ch| ch.is_ascii_digit()))?;
    let clean = token.split(['-', '+']).next()?;
    let mut parts = clean.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

fn missing_binary_error(requested: &str) -> String {
    format!(
        "Codex CLI binary {requested:?} was not found or is not executable; install it with `npm i -g @openai/codex`, `brew install codex`, or an official GitHub release, or set codex_binary"
    )
}

fn resolve_binary(requested: &str) -> Option<PathBuf> {
    let requested_path = Path::new(requested);
    if requested_path.components().count() > 1 || requested_path.is_absolute() {
        return requested_path
            .exists()
            .then(|| requested_path.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(requested);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        for extension in ["exe", "cmd", "bat"] {
            let candidate = directory.join(format!("{requested}.{extension}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn ensure_executable(path: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("inspect Codex binary '{}': {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(missing_binary_error(&path.display().to_string()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(missing_binary_error(&path.display().to_string()));
        }
    }
    Ok(())
}

async fn verify_help_surface(
    binary: &Path,
    args: &[&str],
    required: &[&str],
) -> Result<(), String> {
    let output = command_output(binary, args).await?;
    if !output.status.success() {
        return Err(format!(
            "'{} {}' failed with status {}; upgrade Codex CLI",
            binary.display(),
            args.join(" "),
            output.status
        ));
    }
    let help = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let missing: Vec<_> = required
        .iter()
        .copied()
        .filter(|flag| !help.contains(flag))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Codex CLI '{}' lacks required `{}` capability flag(s): {}; upgrade to >= {}.{}.{}",
            binary.display(),
            args.join(" "),
            missing.join(", "),
            MIN_CODEX_VERSION.0,
            MIN_CODEX_VERSION.1,
            MIN_CODEX_VERSION.2
        ))
    }
}

async fn command_output(binary: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    let command_label = format!("'{} {}'", binary.display(), args.join(" "));
    let mut command = Command::new(binary);
    command.args(args).kill_on_drop(true);
    tokio::time::timeout(PREFLIGHT_COMMAND_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            format!(
                "{command_label} timed out after {} seconds; verify the Codex CLI installation or wrapper",
                PREFLIGHT_COMMAND_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| format!("run {command_label}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::parse_codex_version;

    #[test]
    fn version_parser_accepts_current_and_rejects_noise() {
        assert_eq!(parse_codex_version("codex-cli 0.144.5"), Some((0, 144, 5)));
        assert_eq!(parse_codex_version("vendor v0.144.5"), Some((0, 144, 5)));
        assert_eq!(parse_codex_version("codex 1.2"), Some((1, 2, 0)));
        assert_eq!(parse_codex_version("not-a-version"), None);
    }
}
