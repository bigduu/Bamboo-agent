use log::{debug, info};

#[cfg(windows)]
fn resolve_windows_program(program: &str) -> (String, Vec<String>) {
    use std::path::Path;

    // NPM on Windows commonly creates BOTH:
    // - claude (a non-executable shim script for Unix)
    // - claude.cmd (the actual Windows launcher)
    //
    // If the configured path points at the no-extension shim, CreateProcess fails with:
    // "%1 is not a valid Win32 application" (os error 193).
    let p = Path::new(program);

    let mut resolved = program.to_string();
    let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("").to_ascii_lowercase();

    if ext.is_empty() {
        // If the path exists as a file, prefer a sibling .exe/.cmd/.bat.
        if p.exists() && p.is_file() {
            let exe = p.with_extension("exe");
            let cmd = p.with_extension("cmd");
            let bat = p.with_extension("bat");
            if exe.exists() && exe.is_file() {
                resolved = exe.to_string_lossy().to_string();
            } else if cmd.exists() && cmd.is_file() {
                resolved = cmd.to_string_lossy().to_string();
            } else if bat.exists() && bat.is_file() {
                resolved = bat.to_string_lossy().to_string();
            }
        } else if program.eq_ignore_ascii_case("claude") {
            // For PATH lookups, prefer the Windows shim.
            resolved = "claude.cmd".to_string();
        }
    }

    let resolved_ext = Path::new(&resolved)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if resolved_ext == "cmd" || resolved_ext == "bat" {
        ("cmd".to_string(), vec!["/C".to_string(), resolved])
    } else {
        (resolved, Vec::new())
    }
}

#[cfg(not(windows))]
fn resolve_windows_program(program: &str) -> (String, Vec<String>) {
    (program.to_string(), Vec::new())
}

fn collect_inherited_env(program: &str) -> Vec<(String, String)> {
    let mut envs: Vec<(String, String)> = Vec::new();

    for (key, value) in std::env::vars() {
        if key == "PATH"
            || key == "HOME"
            || key == "USER"
            || key == "SHELL"
            || key == "LANG"
            || key == "LC_ALL"
            || key.starts_with("LC_")
            || key == "NODE_PATH"
            || key == "NVM_DIR"
            || key == "NVM_BIN"
            || key == "HOMEBREW_PREFIX"
            || key == "HOMEBREW_CELLAR"
            || key == "HTTP_PROXY"
            || key == "HTTPS_PROXY"
            || key == "NO_PROXY"
            || key == "ALL_PROXY"
        {
            debug!("Inheriting env var: {}={}", key, value);
            envs.push((key, value));
        }
    }

    // Ensure PATH contains the directory of the selected binary for common install layouts.
    let mut path_value = envs
        .iter()
        .find(|(k, _)| k == "PATH")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    if program.contains("/.nvm/versions/node/") {
        if let Some(node_bin_dir) = std::path::Path::new(program).parent() {
            let node_bin_str = node_bin_dir.to_string_lossy();
            if !path_value.contains(node_bin_str.as_ref()) {
                let joined = std::env::join_paths(
                    std::iter::once(node_bin_dir.to_path_buf())
                        .chain(std::env::split_paths(&path_value)),
                )
                .map(|os| os.to_string_lossy().to_string())
                .unwrap_or_else(|_| format!("{}:{}", node_bin_str, path_value));
                debug!("Adding NVM bin directory to PATH: {}", node_bin_str);
                path_value = joined;
            }
        }
    }

    if program.contains("/homebrew/") || program.contains("/opt/homebrew/") {
        if let Some(program_dir) = std::path::Path::new(program).parent() {
            let homebrew_bin_str = program_dir.to_string_lossy();
            if !path_value.contains(homebrew_bin_str.as_ref()) {
                let joined = std::env::join_paths(
                    std::iter::once(program_dir.to_path_buf())
                        .chain(std::env::split_paths(&path_value)),
                )
                .map(|os| os.to_string_lossy().to_string())
                .unwrap_or_else(|_| format!("{}:{}", homebrew_bin_str, path_value));
                debug!(
                    "Adding Homebrew bin directory to PATH: {}",
                    homebrew_bin_str
                );
                path_value = joined;
            }
        }
    }

    if let Some((_, v)) = envs.iter_mut().find(|(k, _)| k == "PATH") {
        *v = path_value;
    } else if !path_value.is_empty() {
        envs.push(("PATH".to_string(), path_value));
    }

    envs
}

fn log_proxy_settings() {
    info!("Command will use proxy settings:");
    if let Ok(http_proxy) = std::env::var("HTTP_PROXY") {
        info!("  HTTP_PROXY={}", http_proxy);
    }
    if let Ok(https_proxy) = std::env::var("HTTPS_PROXY") {
        info!("  HTTPS_PROXY={}", https_proxy);
    }
}

pub fn create_command_with_env(program: &str) -> std::process::Command {
    let (exe, prefix_args) = resolve_windows_program(program);
    let mut cmd = std::process::Command::new(&exe);
    cmd.args(prefix_args);
    info!("Creating command for: {} (exec: {})", program, exe);
    for (key, value) in collect_inherited_env(program) {
        cmd.env(&key, &value);
    }
    log_proxy_settings();
    cmd
}

pub fn create_tokio_command_with_env(program: &str) -> tokio::process::Command {
    let (exe, prefix_args) = resolve_windows_program(program);
    let mut cmd = tokio::process::Command::new(&exe);
    cmd.args(prefix_args);
    info!("Creating tokio command for: {} (exec: {})", program, exe);
    for (key, value) in collect_inherited_env(program) {
        cmd.env(&key, &value);
    }
    log_proxy_settings();

    cmd
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_join_paths_with_env_functions() {
        // Test that std::env::join_paths and split_paths work correctly
        // This is the cross-platform way to handle PATH environment variable
        let paths = vec![
            std::path::PathBuf::from("/usr/local/bin"),
            std::path::PathBuf::from("/usr/bin"),
            std::path::PathBuf::from("/bin"),
        ];

        let joined = std::env::join_paths(&paths).expect("Failed to join paths");
        let joined_str = joined.to_str().expect("Invalid UTF-8");

        // Verify we can split it back
        let split: Vec<_> = std::env::split_paths(joined_str).collect();
        assert_eq!(split.len(), 3);
        assert!(split.contains(&std::path::PathBuf::from("/usr/local/bin")));
        assert!(split.contains(&std::path::PathBuf::from("/usr/bin")));
        assert!(split.contains(&std::path::PathBuf::from("/bin")));
    }

    #[test]
    fn test_join_paths_prepends_to_existing_path() {
        // Simulate prepending a new directory to PATH
        let new_dir = std::path::PathBuf::from("/new/bin");
        let existing_path = "/usr/local/bin:/usr/bin:/bin";

        // Use the same pattern as in the actual code
        let new_path = std::env::join_paths(
            std::iter::once(new_dir.clone()).chain(std::env::split_paths(existing_path)),
        );

        assert!(new_path.is_ok());
        let new_path_str = new_path.unwrap().to_str().unwrap().to_string();

        // The new directory should be first
        assert!(new_path_str.starts_with("/new/bin"));
        // Should contain all original paths
        assert!(new_path_str.contains("/usr/local/bin"));
        assert!(new_path_str.contains("/usr/bin"));
        assert!(new_path_str.contains("/bin"));
    }

    #[test]
    fn test_path_separator_is_platform_specific() {
        // This test documents that we're using std::env::join_paths
        // which handles the platform-specific separator automatically
        // On Unix: ':'
        // On Windows: ';'
        let paths = vec![
            std::path::PathBuf::from("/first"),
            std::path::PathBuf::from("/second"),
        ];

        let joined = std::env::join_paths(&paths).unwrap();
        let joined_str = joined.to_str().unwrap();

        #[cfg(unix)]
        assert!(joined_str.contains(':'), "Unix uses ':' as PATH separator");

        #[cfg(windows)]
        assert!(
            joined_str.contains(';'),
            "Windows uses ';' as PATH separator"
        );
    }
}
