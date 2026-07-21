//! Independently persisted legacy provider configuration (`providers.json`).

use crate::{
    config_module::save_sidecar_with_sanitized_backup, AtomicJsonStore, Config, ConfigModule,
    ProviderConfigs,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{any::Any, path::Path};

pub const FILE_NAME: &str = "providers.json";

#[derive(Debug, Clone, Default)]
pub struct ProviderConfigsModule(pub ProviderConfigs);

impl std::ops::Deref for ProviderConfigsModule {
    type Target = ProviderConfigs;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ProviderConfigsModule {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ProviderConfigsModule {
    pub(crate) fn load_sync(&mut self, data_dir: &Path) -> Result<bool> {
        crate::migrate_provider_mcp_credentials(data_dir)?;
        crate::ensure_provider_mcp_migration_ready(data_dir)?;
        let store = AtomicJsonStore::new(data_dir.join(FILE_NAME), 1);
        if let Some(stored) = store.load_validated_allowing_unversioned(|_| Ok(()))? {
            // A provider module loaded through ConfigRegistry must be just as
            // usable as one loaded through Config::from_data_dir. Hydrate its
            // at-rest ciphertext here; the later Config-wide hydration pass is
            // intentionally idempotent for the compatibility path.
            let mut config = Config::default();
            config.providers.0 = stored.data;
            config.hydrate_provider_api_keys_from_encrypted();
            config.hydrate_provider_credentials_from_store(data_dir)?;
            self.0 = config.providers.0.clone();
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn save_sync(&self, data_dir: &Path) -> Result<()> {
        // The sidecar is metadata-only after credential-ref migration. Runtime
        // plaintext is skipped by serde and legacy ciphertext is explicitly
        // cleared. A new non-environment secret must be written through the
        // credential API so the credential + section transaction is explicit.
        let mut providers = self.0.clone();
        let mut candidate_has_legacy_secret = false;
        macro_rules! sanitize {
            ($field:ident) => {
                if let Some(provider) = providers.$field.as_mut() {
                    if !provider.api_key.trim().is_empty()
                        && !provider.api_key_from_env
                        && provider.credential_ref.is_none()
                    {
                        anyhow::bail!(
                            "provider secret requires credential API before section persistence"
                        );
                    }
                    candidate_has_legacy_secret |= provider
                        .api_key_encrypted
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty());
                    provider.api_key_encrypted = None;
                }
            };
        }
        sanitize!(openai);
        sanitize!(anthropic);
        sanitize!(gemini);
        if let Some(provider) = providers.bodhi.as_mut() {
            if !provider.api_key.trim().is_empty() && provider.credential_ref.is_none() {
                anyhow::bail!("provider secret requires credential API before section persistence");
            }
            candidate_has_legacy_secret |= provider
                .api_key_encrypted
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
            provider.api_key_encrypted = None;
        }
        let path = data_dir.join(FILE_NAME);

        // A legacy secret must remain on disk until the credential migration
        // has durably committed it. Metadata-only candidates have no such
        // dependency, so publish them first: if an unrelated root migration
        // then fails, the independently owned sidecar remains durable.
        let migration_first =
            candidate_has_legacy_secret || existing_sidecar_has_legacy_provider_secret(&path)?;
        if migration_first {
            crate::migrate_provider_mcp_credentials(data_dir)?;
            crate::ensure_provider_mcp_migration_ready(data_dir)?;
            persist_provider_sidecar(&path, providers)?;
        } else {
            persist_provider_sidecar(&path, providers)?;
            crate::migrate_provider_mcp_credentials(data_dir)?;
            crate::ensure_provider_mcp_migration_ready(data_dir)?;
        }
        Ok(())
    }
}

fn persist_provider_sidecar(path: &Path, mut providers: ProviderConfigs) -> Result<()> {
    let existing = std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
    let is_complete_envelope = existing
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .is_some_and(is_complete_envelope_object);
    if is_complete_envelope {
        let store = AtomicJsonStore::new(path, 1);
        let revision = store
            .load_validated_allowing_unversioned(|_| Ok(()))?
            .map_or(0, |stored| stored.revision);
        store.commit_allowing_unversioned(revision, providers, |_| Ok(()))?;
    } else {
        // Partial envelope markers are ordinary forward-compatible provider
        // keys. Preserve them instead of treating the object as an envelope
        // or silently deleting fields this runtime does not own.
        if let Some(existing) =
            existing.and_then(|value| serde_json::from_value::<ProviderConfigs>(value).ok())
        {
            for (key, value) in existing.extra {
                providers.extra.entry(key).or_insert(value);
            }
        }
        save_sidecar_with_sanitized_backup(path, &providers)?;
    }
    Ok(())
}

fn existing_sidecar_has_legacy_provider_secret(path: &Path) -> Result<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        // Preserve malformed/unsupported documents by letting migration fail
        // closed before the save path can replace them.
        Err(_) => return Ok(true),
    };
    let Some(mut data) = value.as_object() else {
        return Ok(true);
    };
    if is_complete_envelope_object(data) {
        let Some(enveloped) = data.get("data").and_then(serde_json::Value::as_object) else {
            return Ok(true);
        };
        data = enveloped;
    }
    if serde_json::from_value::<ProviderConfigs>(serde_json::Value::Object(data.clone())).is_err() {
        return Ok(true);
    }
    Ok(["openai", "anthropic", "gemini", "bodhi"]
        .into_iter()
        .filter_map(|provider| data.get(provider).and_then(serde_json::Value::as_object))
        .any(|provider| {
            ["api_key", "api_key_encrypted"]
                .into_iter()
                .any(|field| match provider.get(field) {
                    None | Some(serde_json::Value::Null) => false,
                    Some(serde_json::Value::String(value)) => !value.trim().is_empty(),
                    Some(_) => true,
                })
        }))
}

fn is_complete_envelope_object(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    object.contains_key("schema_version")
        && object.contains_key("revision")
        && object.contains_key("data")
}

#[async_trait]
impl ConfigModule for ProviderConfigsModule {
    fn name(&self) -> &'static str {
        "providers"
    }
    async fn load(&mut self, data_dir: &Path) -> Result<()> {
        self.load_sync(data_dir).map(|_| ())
    }
    async fn save(&self, data_dir: &Path) -> Result<()> {
        self.save_sync(data_dir)
    }
    fn validate(&self) -> Result<()> {
        serde_json::to_value(&self.0)
            .map(|_| ())
            .map_err(Into::into)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_runs_before_migration_or_sidecar_writes() {
        let dir = tempfile::tempdir().unwrap();
        let module = ProviderConfigsModule(ProviderConfigs {
            openai: Some(crate::OpenAIConfig {
                api_key: "must-use-credential-api".to_string(),
                ..crate::OpenAIConfig::default()
            }),
            ..ProviderConfigs::default()
        });

        assert!(module.save_sync(dir.path()).is_err());
        assert!(!dir.path().join(FILE_NAME).exists());
        assert!(!dir
            .path()
            .join(".config-credential-migration.lock")
            .exists());
    }

    #[test]
    fn migration_failure_preserves_existing_legacy_ciphertext_sidecar() {
        let _key = crate::encryption::set_test_encryption_key([0x51; 32]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "openai": {
                "api_key_encrypted": crate::encryption::encrypt("legacy-secret").unwrap(),
                "model": "legacy-model"
            }
        }))
        .unwrap();
        std::fs::write(&path, &original).unwrap();
        // Force migration's root source read to fail before it can commit the
        // provider credential transaction.
        std::fs::create_dir(dir.path().join("config.json")).unwrap();
        let module = ProviderConfigsModule(ProviderConfigs {
            openai: Some(crate::OpenAIConfig {
                model: Some("new-metadata".to_string()),
                ..crate::OpenAIConfig::default()
            }),
            ..ProviderConfigs::default()
        });

        assert!(module.save_sync(dir.path()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!dir.path().join("providers.json.bak").exists());
    }

    #[test]
    fn save_never_overwrites_future_revision_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let module = ProviderConfigsModule::default();
        let original = br#"{"schema_version":99,"revision":7,"data":{}}"#;
        std::fs::write(&path, original).unwrap();
        assert!(module.save_sync(dir.path()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }

    #[test]
    fn partial_envelope_markers_remain_ordinary_preserved_provider_data() {
        for original in [
            serde_json::json!({"schema_version": 7, "future": "schema-only"}),
            serde_json::json!({"revision": 7, "future": "revision-only"}),
            serde_json::json!({"data": {"nested": true}, "future": "data-only"}),
            serde_json::json!({"schema_version": 1, "revision": 7, "future": "no-data"}),
            serde_json::json!({"schema_version": 1, "data": {}, "future": "no-revision"}),
            serde_json::json!({"revision": 7, "data": {}, "future": "no-schema"}),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(FILE_NAME);
            std::fs::write(&path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();

            // The metadata-only save is durable before migration readiness;
            // regardless of whether a compatibility migration is needed, it
            // must preserve every ordinary unknown key.
            let _ = ProviderConfigsModule::default().save_sync(dir.path());
            let persisted: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(persisted, original);
        }
    }
}
