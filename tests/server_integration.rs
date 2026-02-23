//! Server integration tests
//!
//! Tests for server startup, configuration, and basic functionality.

use bamboo_agent::{BambooConfig, BambooServer};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bamboo_config_default() {
        let config = BambooConfig::default();
        assert!(config.server.port > 0);
        assert!(!config.server.bind.is_empty());
        assert!(config.data_dir.exists() || config.data_dir.to_string_lossy().len() > 0);
    }

    #[test]
    fn test_bamboo_config_custom_port() {
        let config = BambooConfig {
            server: bamboo_agent::ServerConfig {
                port: 9090,
                bind: "127.0.0.1".to_string(),
                static_dir: None,
                workers: 4,
            },
            ..Default::default()
        };

        assert_eq!(config.server.port, 9090);
        assert_eq!(config.server.bind, "127.0.0.1");
    }

    #[test]
    fn test_bamboo_server_creation() {
        let config = BambooConfig::default();
        let server = BambooServer::new(config);
        assert!(!server.server_addr().is_empty());
    }

    #[test]
    fn test_bamboo_server_addr() {
        let config = BambooConfig {
            server: bamboo_agent::ServerConfig {
                port: 8080,
                bind: "127.0.0.1".to_string(),
                static_dir: None,
                workers: 4,
            },
            ..Default::default()
        };

        let server = BambooServer::new(config);
        assert!(server.server_addr().contains("127.0.0.1"));
        assert!(server.server_addr().contains("8080"));
    }

    #[test]
    fn test_xdg_paths() {
        use bamboo_agent::config::xdg_paths::*;

        let config_home = xdg_config_home();
        assert!(config_home.to_string_lossy().contains(".config"));

        let data_home = xdg_data_home();
        assert!(data_home.to_string_lossy().contains(".local/share"));

        let bamboo_config = bamboo_config_dir();
        assert!(bamboo_config.to_string_lossy().ends_with("bamboo"));

        let bamboo_data = bamboo_data_dir();
        assert!(bamboo_data.to_string_lossy().ends_with("bamboo"));
    }

    #[test]
    fn test_bamboo_builder() {
        use bamboo_agent::BambooBuilder;
        use std::path::PathBuf;

        let server = BambooBuilder::new()
            .port(3000)
            .bind("0.0.0.0")
            .data_dir(PathBuf::from("/tmp/test"))
            .build()
            .unwrap();

        assert!(server.server_addr().contains("0.0.0.0"));
        assert!(server.server_addr().contains("3000"));
    }
}
