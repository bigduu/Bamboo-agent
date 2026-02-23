use log::{debug, info};
use std::process::Command;

pub fn create_command_with_env(program: &str) -> Command {
    let mut cmd = Command::new(program);

    info!("Creating command for: {}", program);

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
            cmd.env(&key, &value);
        }
    }

    info!("Command will use proxy settings:");
    if let Ok(http_proxy) = std::env::var("HTTP_PROXY") {
        info!("  HTTP_PROXY={}", http_proxy);
    }
    if let Ok(https_proxy) = std::env::var("HTTPS_PROXY") {
        info!("  HTTPS_PROXY={}", https_proxy);
    }

    if program.contains("/.nvm/versions/node/") {
        if let Some(node_bin_dir) = std::path::Path::new(program).parent() {
            let current_path = std::env::var("PATH").unwrap_or_default();
            let node_bin_str = node_bin_dir.to_string_lossy();
            if !current_path.contains(node_bin_str.as_ref()) {
                let new_path = std::env::join_paths(
                    std::iter::once(node_bin_dir.to_path_buf())
                        .chain(std::env::split_paths(&current_path)),
                )
                .unwrap_or_else(|_| format!("{}:{}", node_bin_str, current_path).into());
                debug!("Adding NVM bin directory to PATH: {}", node_bin_str);
                cmd.env("PATH", new_path);
            }
        }
    }

    if program.contains("/homebrew/") || program.contains("/opt/homebrew/") {
        if let Some(program_dir) = std::path::Path::new(program).parent() {
            let current_path = std::env::var("PATH").unwrap_or_default();
            let homebrew_bin_str = program_dir.to_string_lossy();
            if !current_path.contains(homebrew_bin_str.as_ref()) {
                let new_path = std::env::join_paths(
                    std::iter::once(program_dir.to_path_buf())
                        .chain(std::env::split_paths(&current_path)),
                )
                .unwrap_or_else(|_| format!("{}:{}", homebrew_bin_str, current_path).into());
                debug!(
                    "Adding Homebrew bin directory to PATH: {}",
                    homebrew_bin_str
                );
                cmd.env("PATH", new_path);
            }
        }
    }

    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

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
