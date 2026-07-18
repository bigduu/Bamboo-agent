//! Independently persisted legacy provider configuration (`providers.json`).

use crate::{
    config_module::{load_sidecar, save_sidecar_with_sanitized_backup},
    Config, ConfigModule, ProviderConfigs,
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
        if let Some(value) = load_sidecar(&data_dir.join(FILE_NAME))? {
            // A provider module loaded through ConfigRegistry must be just as
            // usable as one loaded through Config::from_data_dir. Hydrate its
            // at-rest ciphertext here; the later Config-wide hydration pass is
            // intentionally idempotent for the compatibility path.
            let mut config = Config::default();
            config.providers.0 = value;
            config.hydrate_provider_api_keys_from_encrypted();
            self.0 = config.providers.0.clone();
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn save_sync(&self, data_dir: &Path) -> Result<()> {
        // Provider fields keep plaintext only in memory. Make the module safe
        // to persist through ConfigRegistry directly, not only through
        // Config::save_to_dir's broader secret-refresh pass.
        let mut config = Config::default();
        config.providers.0 = self.0.clone();
        config.refresh_provider_api_keys_encrypted()?;
        save_sidecar_with_sanitized_backup(&data_dir.join(FILE_NAME), &config.providers.0)
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
