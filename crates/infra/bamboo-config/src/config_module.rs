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

pub(crate) fn save_sidecar<T: Serialize + DeserializeOwned>(path: &Path, value: &T) -> Result<()> {
    with_legacy_module_write(path.parent().unwrap_or_else(|| Path::new(".")), || {
        crate::config_store::AtomicFileStore::new(path)
            .backup_generations(1)
            .write_json(value)?;
        Ok(())
    })
}

/// Serialize the authority check and the physical legacy write with layout
/// activation. Checking only before acquiring the lock would let a waiting
/// writer replace a newly activated revisioned section with a raw sidecar.
pub(crate) fn with_legacy_module_write<T>(
    data_dir: &Path,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    crate::credential_migration::with_provider_mcp_migration_lock(data_dir, || {
        ensure_legacy_module_layout(data_dir)?;
        // Preserve the operation's original error without turning an I/O or
        // serialization failure into an unrelated validation diagnostic.
        Ok(operation())
    })?
}

/// Read-only early rejection, before validation/encryption/migration can create
/// files. Physical writes must additionally call `with_legacy_module_write`.
pub(crate) fn ensure_legacy_module_layout(data_dir: &Path) -> crate::ConfigStoreResult<()> {
    // A committed initial split may not have installed the marker or quorum
    // yet. The existing transaction guard also fences that pending interval.
    crate::ensure_provider_mcp_migration_ready(data_dir)?;
    if crate::modular_authority_boundary_present(data_dir)? {
        return Err(crate::ConfigStoreError::Validation(
            "legacy module writes are unavailable after modular configuration activation; use the revisioned section API".to_string(),
        ));
    }
    Ok(())
}

/// Save a sidecar whose deserializer intentionally drops in-memory-only
/// fields (for example provider plaintext API keys). The previous generation
/// is parsed and reserialized before becoming the backup so a hand-written
/// plaintext secret is never copied verbatim into `*.json.bak`.
/// The provider caller holds `with_legacy_module_write` across its complete
/// read/merge/write operation; this helper must not reacquire that lock.
pub(crate) fn save_sidecar_with_sanitized_backup<T: Serialize + DeserializeOwned>(
    path: &Path,
    value: &T,
) -> Result<()> {
    crate::config_store::AtomicFileStore::new(path)
        .backup_generations(1)
        .write_json(value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::json;
    use tempfile::TempDir;

    use crate::{
        Config, ConfigFacade, ConfigRegistry, MemoryConfig, MemoryConfigModule, OpenAIConfig,
        ProviderConfigs, ProviderConfigsModule, SubagentsConfigModule,
    };

    fn write_json(path: &Path, value: serde_json::Value) {
        std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    use std::path::Path;

    #[test]
    fn modular_layout_rejects_legacy_module_writes_without_touching_sections() {
        let _key = crate::encryption::set_test_encryption_key([203; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            json!({
                "provider": "openai",
                "providers": {"openai": {"api_key": "module-guard-test-secret", "model": "gpt-test"}},
                "memory": {"background_model": "memory-before"},
                "subagents": {"max_concurrent": 3}
            }),
        );
        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let mut candidate = facade.effective_config();
        let provider = facade
            .registry()
            .envelope_value(crate::SectionId::Providers)
            .unwrap();
        assert_eq!(provider.data["default_provider_instance"], "openai");
        assert_eq!(
            provider.data["provider_instances"]["openai"]["model"],
            "gpt-test"
        );
        let paths = [
            "memory.json",
            "subagents.json",
            "providers.json",
            "credentials.json",
            crate::SECTION_LAYOUT_FILE,
        ];
        let before: Vec<_> = paths
            .iter()
            .map(|name| std::fs::read(dir.path().join(name)).unwrap())
            .collect();
        candidate.memory.0 = Some(MemoryConfig {
            background_model: Some("must-not-publish".into()),
            ..MemoryConfig::default()
        });
        candidate.subagents.max_concurrent = Some(99);
        for result in [
            candidate.save_memory_to_dir(dir.path()),
            candidate.save_subagents_to_dir(dir.path()),
            candidate.save_providers_to_dir(dir.path()),
        ] {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("revisioned section API"));
        }

        // ConfigRegistry/ConfigModule callers are public SDK entry points too.
        let mut registry = ConfigRegistry::new();
        registry.register(MemoryConfigModule(candidate.memory.0.clone()));
        registry.register(SubagentsConfigModule(candidate.subagents.0.clone()));
        registry.register(ProviderConfigsModule(candidate.providers.0.clone()));
        for name in ["memory", "subagents", "providers"] {
            let error =
                futures::executor::block_on(registry.save_module(name, dir.path())).unwrap_err();
            assert!(error.to_string().contains("revisioned section API"));
        }
        assert!(futures::executor::block_on(registry.save_all(dir.path())).is_err());
        for (name, bytes) in paths.iter().zip(before) {
            assert_eq!(
                std::fs::read(dir.path().join(name)).unwrap(),
                bytes,
                "{name}"
            );
        }
        assert_eq!(
            ConfigFacade::open(dir.path())
                .unwrap()
                .registry()
                .envelope_value(crate::SectionId::Providers)
                .unwrap()
                .revision,
            provider.revision
        );
    }

    #[test]
    fn incomplete_modular_authority_never_reopens_legacy_module_writes() {
        for evidence in ["corrupt-marker", "completion-ledger", "section-quorum"] {
            let dir = TempDir::new().unwrap();
            let config = Config::default();
            config.save_to_dir(dir.path().to_path_buf()).unwrap();
            let paths = ["memory.json", "subagents.json", "providers.json"];
            let before: Vec<_> = paths
                .iter()
                .map(|name| std::fs::read(dir.path().join(name)).unwrap())
                .collect();
            match evidence {
                "corrupt-marker" => {
                    std::fs::write(dir.path().join(crate::SECTION_LAYOUT_FILE), b"{broken")
                        .unwrap();
                }
                "completion-ledger" => {
                    std::fs::write(
                        dir.path()
                            .join(crate::credential_migration::SECTION_LAYOUT_COMPLETION_FILE),
                        b"{broken",
                    )
                    .unwrap();
                }
                _ => {
                    write_json(&dir.path().join("core.json"), json!({}));
                    write_json(&dir.path().join("hooks.json"), json!({}));
                }
            }
            assert!(crate::modular_authority_boundary_present(dir.path()).unwrap());
            assert!(config.save_memory_to_dir(dir.path()).is_err(), "{evidence}");
            assert!(
                config.save_subagents_to_dir(dir.path()).is_err(),
                "{evidence}"
            );
            assert!(
                config.save_providers_to_dir(dir.path()).is_err(),
                "{evidence}"
            );
            for (name, bytes) in paths.iter().zip(before) {
                assert_eq!(
                    std::fs::read(dir.path().join(name)).unwrap(),
                    bytes,
                    "{evidence}: {name}"
                );
            }
        }
    }

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
    fn invalid_provider_credential_refs_from_external_edits_are_rejected() {
        for invalid_ref in ["../credentials".to_string(), "x".repeat(161)] {
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                json!({
                    "providers": {"openai": {"model": "root-lkg"}}
                }),
            );
            write_json(
                &dir.path().join("providers.json"),
                json!({
                    "openai": {
                        "model": "must-not-load",
                        "credential_ref": invalid_ref
                    }
                }),
            );

            let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
            assert_eq!(
                config.providers.openai.as_ref().unwrap().model.as_deref(),
                Some("root-lkg")
            );
        }
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
            config.memory.as_ref().unwrap().background_model.as_deref(),
            Some("sidecar")
        );
        assert_eq!(config.subagents.max_concurrent, Some(9));
        assert_eq!(
            config.providers.openai.as_ref().unwrap().model.as_deref(),
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
        changed.memory.0 = Some(MemoryConfig {
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
        let reference = crate::credential_ref("provider", "openai", "api_key").unwrap();
        crate::CredentialStore::open(dir.path())
            .replace(
                reference.clone(),
                "sk-plaintext-must-not-leak",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut config = Config::default();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-plaintext-must-not-leak".into(),
            credential_ref: Some(reference),
            ..OpenAIConfig::default()
        });
        config.save_providers_to_dir(dir.path()).unwrap();
        let raw = std::fs::read_to_string(dir.path().join("providers.json")).unwrap();
        assert!(!raw.contains("sk-plaintext-must-not-leak"));
        assert!(!raw.contains("api_key_encrypted"));
        assert!(raw.contains("credential_ref"));
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
            config.memory.as_ref().unwrap().background_model.as_deref(),
            Some("recovered")
        );
        assert_eq!(
            std::fs::read(dir.path().join("memory.json")).unwrap(),
            b"{broken"
        );
    }

    #[test]
    fn schema_invalid_sidecar_is_not_promoted_over_last_known_good_backup() {
        let dir = TempDir::new().unwrap();
        write_json(&dir.path().join("config.json"), json!({}));
        write_json(
            &dir.path().join("memory.json.bak"),
            json!({"background_model": "recovered"}),
        );
        write_json(
            &dir.path().join("memory.json"),
            json!({"auto_dream_interval_secs": "not-a-number"}),
        );

        let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            config.memory.as_ref().unwrap().background_model.as_deref(),
            Some("recovered")
        );
        config.save_memory_to_dir(dir.path()).unwrap();

        let backup: MemoryConfig =
            serde_json::from_slice(&std::fs::read(dir.path().join("memory.json.bak")).unwrap())
                .unwrap();
        assert_eq!(backup.background_model.as_deref(), Some("recovered"));
    }

    #[test]
    fn provider_registry_save_encrypts_keys_and_sanitizes_backup() {
        let _key = crate::encryption::set_test_encryption_key([29; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("providers.json"),
            json!({"openai": {"api_key": "sk-old-plaintext", "model": "old"}}),
        );
        crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
        let reference = crate::credential_ref("provider", "openai", "api_key").unwrap();
        let store = crate::CredentialStore::open(dir.path());
        store
            .replace(
                reference.clone(),
                "sk-new-plaintext",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();

        let providers = ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: "sk-new-plaintext".into(),
                credential_ref: Some(reference),
                model: Some("new".into()),
                ..OpenAIConfig::default()
            }),
            ..ProviderConfigs::default()
        };
        let mut registry = ConfigRegistry::new();
        registry.register(ProviderConfigsModule(providers));
        futures::executor::block_on(registry.save_module("providers", dir.path())).unwrap();

        let current = std::fs::read_to_string(dir.path().join("providers.json")).unwrap();
        assert!(!current.contains("sk-new-plaintext"));
        assert!(!current.contains("api_key_encrypted"));
        assert!(current.contains("credential_ref"));
        let backup = std::fs::read_to_string(dir.path().join("providers.json.bak")).unwrap();
        assert!(!backup.contains("sk-old-plaintext"));

        let mut loaded = ConfigRegistry::new();
        loaded.register(ProviderConfigsModule::default());
        futures::executor::block_on(loaded.load_all(dir.path())).unwrap();
        let module = loaded.module::<ProviderConfigsModule>("providers").unwrap();
        assert_eq!(
            module.0.openai.as_ref().unwrap().api_key,
            "sk-new-plaintext"
        );

        // Config's compatibility path performs its historical hydration pass
        // after module load. The module-level hydration above must remain
        // idempotent when reached through that path.
        let config = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            config.providers.openai.as_ref().unwrap().api_key,
            "sk-new-plaintext"
        );
    }

    #[test]
    fn sidecars_are_durable_before_root_migration_rewrite() {
        let dir = TempDir::new().unwrap();
        // A directory at config.json forces the final atomic rename to fail.
        // The independently persisted modules must already be durable so a
        // failed root rewrite cannot discard the migration source of truth.
        std::fs::create_dir(dir.path().join("config.json")).unwrap();
        let mut config = Config::default();
        config.memory.0 = Some(MemoryConfig {
            background_model: Some("durable-before-root".into()),
            ..MemoryConfig::default()
        });

        assert!(config.save_to_dir(dir.path().to_path_buf()).is_err());
        let memory: MemoryConfig =
            serde_json::from_slice(&std::fs::read(dir.path().join("memory.json")).unwrap())
                .unwrap();
        assert_eq!(
            memory.background_model.as_deref(),
            Some("durable-before-root")
        );
        assert!(dir.path().join("subagents.json").exists());
        let providers: ProviderConfigs =
            serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap())
                .unwrap();
        assert!(providers.openai.is_none());
        assert!(providers.anthropic.is_none());
        assert!(providers.gemini.is_none());
        assert!(providers.bodhi.is_none());
    }
}
