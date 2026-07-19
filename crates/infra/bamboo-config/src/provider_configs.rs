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
        crate::migrate_provider_mcp_credentials(data_dir)?;
        // The sidecar is metadata-only after credential-ref migration. Runtime
        // plaintext is skipped by serde and legacy ciphertext is explicitly
        // cleared. A new non-environment secret must be written through the
        // credential API so the credential + section transaction is explicit.
        let mut providers = self.0.clone();
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
            provider.api_key_encrypted = None;
        }
        let path = data_dir.join(FILE_NAME);
        let has_envelope_marker = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|value| value.as_object().cloned())
            .is_some_and(|object| {
                object.contains_key("schema_version")
                    || object.contains_key("revision")
                    || object.contains_key("data")
            });
        if has_envelope_marker {
            let store = AtomicJsonStore::new(path, 1);
            let revision = store
                .load_validated_allowing_unversioned(|_| Ok(()))?
                .map_or(0, |stored| stored.revision);
            store.commit_allowing_unversioned(revision, providers, |_| Ok(()))?;
        } else {
            save_sidecar_with_sanitized_backup(&path, &providers)?;
        }
        Ok(())
    }
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
    fn save_never_overwrites_partial_or_future_revision_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let module = ProviderConfigsModule::default();
        for original in [
            br#"{"schema_version":1,"data":{}}"#.as_slice(),
            br#"{"schema_version":99,"revision":7,"data":{}}"#.as_slice(),
        ] {
            std::fs::write(&path, original).unwrap();
            assert!(module.save_sync(dir.path()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), original);
        }
    }
}
