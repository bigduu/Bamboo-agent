//! Server integration tests
//!
//! Tests for server startup, configuration, and basic functionality.

use bamboo_agent::{BambooServer, Config};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bamboo_config_default() {
        let config = Config::default();
        assert!(config.server.port > 0);
        assert!(!config.server.bind.is_empty());
        assert!(!bamboo_infrastructure::paths::bamboo_dir()
            .to_string_lossy()
            .is_empty());
    }

    #[test]
    fn test_bamboo_config_custom_port() {
        let mut config = Config::default();
        config.server.port = 9090;
        config.server.bind = "127.0.0.1".to_string();

        assert_eq!(config.server.port, 9090);
        assert_eq!(config.server.bind, "127.0.0.1");
    }

    #[test]
    fn test_bamboo_server_creation() {
        let config = Config::default();
        let server = BambooServer::new(config);
        assert!(!server.server_addr().is_empty());
    }

    #[test]
    fn test_bamboo_server_addr() {
        let mut config = Config::default();
        config.server.port = 8080;
        config.server.bind = "127.0.0.1".to_string();

        let server = BambooServer::new(config);
        assert!(server.server_addr().contains("127.0.0.1"));
        assert!(server.server_addr().contains("8080"));
    }

    #[test]
    fn test_bamboo_paths() {
        use bamboo_infrastructure::paths::*;

        let bamboo_home = bamboo_dir();
        assert!(bamboo_home.to_string_lossy().ends_with(".bamboo"));
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
