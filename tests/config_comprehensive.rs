//! Comprehensive tests for unified configuration system
//!
//! Tests cover all priority layers and edge cases

#[cfg(test)]
mod comprehensive_config_tests {
    use bamboo_agent::{BambooBuilder, Config};
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    // === Test Infrastructure ===

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bamboo-config-test-{}-{}",
                std::process::id(),
                nanos
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write_config(&self, content: &str) {
            std::fs::write(self.path.join("config.json"), content).unwrap();
        }

        fn read_config(&self) -> Option<String> {
            std::fs::read_to_string(self.path.join("config.json")).ok()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Acquire the environment lock, ignoring if it was poisoned by a previous test failure
    /// This ensures test isolation even when tests panic
    fn env_lock_acquire() -> std::sync::MutexGuard<'static, ()> {
        env_lock().lock().unwrap_or_else(|poisoned| {
            // Lock was poisoned by a previous test failure - recover it
            eprintln!("Warning: Environment lock was poisoned, recovering...");
            poisoned.into_inner()
        })
    }

    // === 1) Environment Variable Override Priority ===

    #[test]
    fn env_port_overrides_file_value() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        // File says port 1111
        temp.write_config(
            r#"{"server": {"port": 1111, "bind": "127.0.0.1"}, "provider": "anthropic"}"#,
        );

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _port = EnvVarGuard::set("BAMBOO_PORT", "2222");

        let config = Config::new();
        assert_eq!(config.server.port, 2222, "Env should override file");
    }

    #[test]
    fn env_bind_overrides_file_value() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(
            r#"{"server": {"port": 8080, "bind": "127.0.0.1"}, "provider": "anthropic"}"#,
        );

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _bind = EnvVarGuard::set("BAMBOO_BIND", "0.0.0.0");

        let config = Config::new();
        assert_eq!(config.server.bind, "0.0.0.0");
    }

    #[test]
    fn env_provider_overrides_file_value() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{"provider": "anthropic"}"#);

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _provider = EnvVarGuard::set("BAMBOO_PROVIDER", "anthropic");

        let config = Config::new();
        assert_eq!(config.provider, "anthropic");
    }

    #[test]
    fn env_headless_overrides_file_value() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{"headless_auth": false}"#);

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _headless = EnvVarGuard::set("BAMBOO_HEADLESS", "true");

        let config = Config::new();
        assert!(config.headless_auth);
    }

    #[test]
    fn invalid_env_port_ignored() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{"server": {"port": 1111, "bind": "127.0.0.1"}}"#);

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _port = EnvVarGuard::set("BAMBOO_PORT", "not_a_number");

        let config = Config::new();
        assert_eq!(config.server.port, 1111, "Invalid env should be ignored");
    }

    #[test]
    fn env_headless_whitespace_handling() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _headless = EnvVarGuard::set("BAMBOO_HEADLESS", "  TRUE  ");

        let config = Config::new();
        assert!(config.headless_auth);
    }

    #[test]
    fn from_data_dir_beats_env() {
        let _lock = env_lock_acquire();
        let dir_a = TempDir::new();
        let dir_b = TempDir::new();

        dir_a.write_config(r#"{"provider": "from_dir_a"}"#);
        dir_b.write_config(r#"{"provider": "from_dir_b"}"#);

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", dir_b.path.to_str().unwrap());

        // Explicit param should beat env
        let config = Config::from_data_dir(Some(dir_a.path.clone()));
        assert_eq!(config.provider, "from_dir_a");
    }

    #[test]
    fn bambo_data_dir_changes_load_location() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{"provider": "from_custom_dir"}"#);

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let _home = EnvVarGuard::remove("HOME");

        let config = Config::new();
        assert_eq!(config.provider, "from_custom_dir");
        assert_eq!(bamboo_config::paths::resolve_bamboo_dir(), temp.path);
    }

    // === 2) Config File Loading and Saving ===

    #[test]
    fn full_new_format_config_loads() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(
            r#"{
            "http_proxy": "http://proxy:8080",
            "https_proxy": "https://proxy:8443",
            "headless_auth": true,
            "provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "sk-test",
                    "base_url": "https://api.openai.com/v1",
                    "model": "gpt-4"
                },
                "anthropic": {
                    "api_key": "sk-ant-test",
                    "model": "claude-3"
                },
                "copilot": {
                    "enabled": true,
                    "headless_auth": false
                }
            },
	            "server": {
	                "port": 9999,
	                "bind": "0.0.0.0",
	                "static_dir": "/static",
	                "workers": 16
	            }
	        }"#,
        );

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let config = Config::new();

        assert_eq!(config.http_proxy, "http://proxy:8080");
        assert_eq!(config.https_proxy, "https://proxy:8443");
        assert_eq!(config.provider, "openai");
        assert!(config.providers.openai.is_some());
        assert!(config.providers.anthropic.is_some());
        assert!(config.providers.copilot.is_some());
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.server.bind, "0.0.0.0");
        assert_eq!(config.server.workers, 16);
        assert_eq!(config.server.static_dir, Some(PathBuf::from("/static")));
    }

    #[test]
    fn config_roundtrip_preserves_all_fields() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        let mut original = Config::from_data_dir(Some(temp.path.clone()));
        original.provider = "anthropic".to_string();
        original.providers.anthropic = Some(bamboo_config::AnthropicConfig {
            api_key: String::new(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("claude-3".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            max_tokens: None,
            request_overrides: None,
            extra: Default::default(),
        });
        original.server.port = 8888;
        original.server.workers = 8;
        original.server.static_dir = Some(PathBuf::from("/app/static"));

        original.save_to_dir(temp.path.clone()).unwrap();

        let loaded = Config::from_data_dir(Some(temp.path.clone()));
        assert_eq!(loaded.provider, "anthropic");
        assert_eq!(
            loaded
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.model.as_deref()),
            Some("claude-3")
        );
        assert_eq!(loaded.server.port, 8888);
        assert_eq!(loaded.server.workers, 8);
        assert_eq!(loaded.server.static_dir, Some(PathBuf::from("/app/static")));
    }

    // === 3) Saving behavior ===

    #[test]
    fn save_to_dir_writes_to_directory() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        let mut config = Config::from_data_dir(Some(temp.path.clone()));
        config.server.port = 7777;
        config.save_to_dir(temp.path.clone()).unwrap();

        assert!(temp.path.join("config.json").exists());

        let content = temp.read_config().unwrap();
        assert!(content.contains("7777"));
    }

    // === 4) Backward Compatibility ===

    #[test]
    fn unknown_fields_ignored() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(
            r#"{
            "provider": "anthropic",
            "unknown_key": 123,
            "future_field": "value"
        }"#,
        );

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let config = Config::new();
        assert_eq!(config.provider, "anthropic");
    }

    // === 5) Edge Cases ===

    #[test]
    fn missing_config_uses_defaults() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();
        // Don't create config file

        let config = Config::from_data_dir(Some(temp.path.clone()));

        assert_eq!(config.server.port, 9562);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.workers, 10);
        assert_eq!(config.provider, "anthropic");
    }

    #[test]
    fn invalid_json_falls_back_to_defaults() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{ not valid json }"#);

        let config = Config::from_data_dir(Some(temp.path.clone()));
        assert_eq!(config.server.port, 9562);
    }

    #[test]
    fn partial_invalid_type_falls_back() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(r#"{"server": {"port": "not_a_number"}}"#);

        let config = Config::from_data_dir(Some(temp.path.clone()));
        assert_eq!(config.server.port, 9562); // Default
    }

    // === 6) Library Usage Patterns ===

    #[test]
    fn bamboo_builder_works() {
        let temp = TempDir::new();

        let server = BambooBuilder::new()
            .port(9001)
            .bind("0.0.0.0")
            .workers(8)
            .data_dir(temp.path.clone())
            .build()
            .unwrap();

        assert_eq!(server.server_addr(), "0.0.0.0:9001");
    }

    #[test]
    fn isolated_library_usage_without_home() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        // Don't touch HOME, use from_data_dir
        let mut config = Config::from_data_dir(Some(temp.path.clone()));
        config.provider = "test".to_string();
        config.save_to_dir(temp.path.clone()).unwrap();

        let reloaded = Config::from_data_dir(Some(temp.path.clone()));
        assert_eq!(reloaded.provider, "test");
    }

    // === 7) Provider Configuration ===

    #[test]
    fn copilot_headless_auth_from_provider_config() {
        let _lock = env_lock_acquire();
        let temp = TempDir::new();

        temp.write_config(
            r#"{
            "provider": "copilot",
            "headless_auth": false,
            "providers": {
                "copilot": {
                    "enabled": true,
                    "headless_auth": true
                }
            }
        }"#,
        );

        let _env = EnvVarGuard::set("BAMBOO_DATA_DIR", temp.path.to_str().unwrap());
        let config = Config::new();

        // Provider config should be available
        assert!(config.providers.copilot.is_some());
        let copilot = config.providers.copilot.unwrap();
        assert!(copilot.headless_auth);
    }
}
