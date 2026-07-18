//! Independently persisted memory configuration (`memory.json`).

use crate::{
    config_module::{load_sidecar, save_sidecar},
    ConfigModule, MemoryConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{any::Any, path::Path};

pub const FILE_NAME: &str = "memory.json";

#[derive(Debug, Clone, Default)]
pub struct MemoryConfigModule(pub Option<MemoryConfig>);

impl std::ops::Deref for MemoryConfigModule {
    type Target = Option<MemoryConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MemoryConfigModule {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MemoryConfigModule {
    pub(crate) fn load_sync(&mut self, data_dir: &Path) -> Result<bool> {
        if let Some(value) = load_sidecar(&data_dir.join(FILE_NAME))? {
            self.0 = value;
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn save_sync(&self, data_dir: &Path) -> Result<()> {
        save_sidecar(&data_dir.join(FILE_NAME), &self.0)
    }
}

#[async_trait]
impl ConfigModule for MemoryConfigModule {
    fn name(&self) -> &'static str {
        "memory"
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
