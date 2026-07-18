//! Independently persisted configuration modules and their registrar.
//!
//! A module owns one sidecar file in Bamboo's data directory.  Modules are
//! deliberately object-safe so new configuration domains can be registered
//! without adding another field to the root [`crate::Config`] DTO.

use std::{any::Any, collections::HashMap, path::Path};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// A named, independently loadable and flushable configuration domain.
#[async_trait]
pub trait ConfigModule: Send + Sync {
    fn name(&self) -> &'static str;
    async fn load(&mut self, data_dir: &Path) -> Result<()>;
    async fn save(&self, data_dir: &Path) -> Result<()>;
    async fn reload(&mut self, data_dir: &Path) -> Result<()> {
        self.load(data_dir).await
    }
    fn validate(&self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Registrar for configuration domains. Each module can be loaded or saved
/// without serializing unrelated configuration.
#[derive(Default)]
pub struct ConfigRegistry {
    modules: HashMap<String, Box<dyn ConfigModule>>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<M: ConfigModule + 'static>(
        &mut self,
        module: M,
    ) -> Option<Box<dyn ConfigModule>> {
        self.modules
            .insert(module.name().to_owned(), Box::new(module))
    }

    pub fn module<T: 'static>(&self, name: &str) -> Option<&T> {
        self.modules.get(name)?.as_any().downcast_ref()
    }

    pub fn module_mut<T: 'static>(&mut self, name: &str) -> Option<&mut T> {
        self.modules.get_mut(name)?.as_any_mut().downcast_mut()
    }

    pub async fn load_all(&mut self, data_dir: &Path) -> Result<()> {
        for module in self.modules.values_mut() {
            module.load(data_dir).await?;
            module.validate()?;
        }
        Ok(())
    }

    pub async fn reload_module(&mut self, name: &str, data_dir: &Path) -> Result<()> {
        let module = self
            .modules
            .get_mut(name)
            .ok_or_else(|| anyhow!("unknown config module: {name}"))?;
        module.reload(data_dir).await?;
        module.validate()
    }

    pub async fn save_module(&self, name: &str, data_dir: &Path) -> Result<()> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| anyhow!("unknown config module: {name}"))?;
        module.validate()?;
        module.save(data_dir).await
    }

    pub async fn save_all(&self, data_dir: &Path) -> Result<()> {
        for module in self.modules.values() {
            module.validate()?;
            module.save(data_dir).await?;
        }
        Ok(())
    }
}

pub(crate) fn load_sidecar<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok(Some(value)),
        Err(primary_error) => {
            let backup = path.with_extension("json.bak");
            if backup.exists() {
                let backup_bytes = std::fs::read(&backup)?;
                return serde_json::from_slice(&backup_bytes)
                    .map(Some)
                    .map_err(Into::into);
            }
            Err(primary_error.into())
        }
    }
}

pub(crate) fn save_sidecar<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    // Retain one last-known-good generation. A malformed current sidecar is
    // deliberately not promoted over the usable backup.
    if path.exists() {
        let current = std::fs::read(path)?;
        if serde_json::from_slice::<serde_json::Value>(&current).is_ok() {
            std::fs::copy(path, path.with_extension("json.bak"))?;
        }
    }
    crate::config::write_atomic(path, &bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::json;
    use tempfile::TempDir;

    use crate::{Config, MemoryConfig, OpenAIConfig};

    fn write_json(path: &Path, value: serde_json::Value) {
        std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    use std::path::Path;

    #[test]
    fn legacy_inline_sections_load_and_migrate_to_sidecars() {
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            json!({
                "memory": { "background_model": "legacy-memory" },
                "subagents": { "max_concurrent": 3 },
                "providers": { "openai": { "model": "legacy-model" } }
            }),
        );

        let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            config.memory.as_ref().unwrap().background_model.as_deref(),
            Some("legacy-memory")
        );
        assert_eq!(config.subagents.max_concurrent, Some(3));
        assert_eq!(
            config.providers.openai.as_ref().unwrap().model.as_deref(),
            Some("legacy-model")
        );

        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                .unwrap();
        assert!(root.get("memory").is_none());
        assert!(root.get("subagents").is_none());
        assert!(root.get("providers").is_none());
        assert!(dir.path().join("memory.json").exists());
        assert!(dir.path().join("subagents.json").exists());
        assert!(dir.path().join("providers.json").exists());

        let reloaded = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            reloaded
                .memory
                .as_ref()
                .unwrap()
                .background_model
                .as_deref(),
            Some("legacy-memory")
        );
        assert_eq!(reloaded.subagents.max_concurrent, Some(3));
        assert_eq!(
            reloaded.providers.openai.as_ref().unwrap().model.as_deref(),
            Some("legacy-model")
        );
    }

    #[test]
    fn sidecars_override_legacy_inline_sections() {
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            json!({
                "memory": { "background_model": "inline" },
                "subagents": { "max_concurrent": 1 },
                "providers": { "openai": { "model": "inline" } }
            }),
        );
        write_json(
            &dir.path().join("memory.json"),
            json!({"background_model": "sidecar"}),
        );
        write_json(
            &dir.path().join("subagents.json"),
            json!({"max_concurrent": 9}),
        );
        write_json(
            &dir.path().join("providers.json"),
            json!({"openai": {"model": "sidecar"}}),
        );

        let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            config.memory.unwrap().background_model.as_deref(),
            Some("sidecar")
        );
        assert_eq!(config.subagents.max_concurrent, Some(9));
        assert_eq!(
            config.providers.openai.unwrap().model.as_deref(),
            Some("sidecar")
        );
    }

    #[test]
    fn independent_memory_save_does_not_touch_other_documents() {
        let dir = TempDir::new().unwrap();
        let config = Config::default();
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        let paths = ["config.json", "providers.json", "subagents.json"];
        let before: Vec<_> = paths
            .iter()
            .map(|name| {
                let path = dir.path().join(name);
                (
                    std::fs::read(&path).unwrap(),
                    std::fs::metadata(path)
                        .unwrap()
                        .modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH),
                )
            })
            .collect();

        let mut changed = config;
        changed.memory = Some(MemoryConfig {
            background_model: Some("new-memory".into()),
            ..MemoryConfig::default()
        });
        changed.save_memory_to_dir(dir.path()).unwrap();

        for (index, name) in paths.iter().enumerate() {
            let path = dir.path().join(name);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before[index].0,
                "{name} bytes changed"
            );
            assert_eq!(
                std::fs::metadata(path)
                    .unwrap()
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH),
                before[index].1,
                "{name} mtime changed"
            );
        }
    }

    #[test]
    fn providers_sidecar_never_contains_plaintext_api_key() {
        let _key = crate::encryption::set_test_encryption_key([23; 32]);
        let dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-plaintext-must-not-leak".into(),
            ..OpenAIConfig::default()
        });
        config.save_providers_to_dir(dir.path()).unwrap();
        let raw = std::fs::read_to_string(dir.path().join("providers.json")).unwrap();
        assert!(!raw.contains("sk-plaintext-must-not-leak"));
        assert!(raw.contains("api_key_encrypted"));
    }

    #[test]
    fn malformed_sidecar_recovers_from_last_known_good_backup() {
        let dir = TempDir::new().unwrap();
        write_json(&dir.path().join("config.json"), json!({}));
        write_json(
            &dir.path().join("memory.json.bak"),
            json!({"background_model": "recovered"}),
        );
        std::fs::write(dir.path().join("memory.json"), b"{broken").unwrap();

        let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            config.memory.unwrap().background_model.as_deref(),
            Some("recovered")
        );
        assert_eq!(
            std::fs::read(dir.path().join("memory.json")).unwrap(),
            b"{broken"
        );
    }
}
