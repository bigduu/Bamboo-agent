//! Workflow system for defining and executing agent workflows
//!
//! This module provides a workflow engine that allows users to define
//! complex agent behaviors using a declarative composition syntax.
//!
//! The domain types (`WorkflowDefinition`, validation) live in
//! `bamboo-domain-workflow`. This module keeps the filesystem loader
//! and cache.

mod loader;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;

use bamboo_domain::{WorkflowDefinition, WorkflowLoadError};

#[derive(Debug, Clone)]
pub(crate) struct CachedWorkflow {
    pub(crate) modified: Option<SystemTime>,
    pub(crate) definition: WorkflowDefinition,
}

pub struct WorkflowLoader {
    workflows_dir: PathBuf,
    cache: RwLock<HashMap<PathBuf, CachedWorkflow>>,
}

impl WorkflowLoader {
    pub fn new() -> Self {
        Self {
            workflows_dir: bamboo_infrastructure::paths::workflows_dir(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_dir(path: PathBuf) -> Self {
        Self {
            workflows_dir: path,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_from_file<P>(&self, path: P) -> Result<WorkflowDefinition, WorkflowLoadError>
    where
        P: AsRef<Path>,
    {
        loader::load_from_file(self, path.as_ref())
    }

    pub fn load_all_from_directory<P>(
        &self,
        dir: P,
    ) -> Result<Vec<WorkflowDefinition>, WorkflowLoadError>
    where
        P: AsRef<Path>,
    {
        loader::load_all_from_directory(self, dir.as_ref())
    }

    pub fn load_all(&self) -> Result<Vec<WorkflowDefinition>, WorkflowLoadError> {
        self.load_all_from_directory(&self.workflows_dir)
    }

    pub fn validate_definition(&self, definition: &WorkflowDefinition) -> Result<(), String> {
        definition.validate()
    }

    pub(crate) fn validate_with_path(
        &self,
        path: &Path,
        definition: &WorkflowDefinition,
    ) -> Result<(), WorkflowLoadError> {
        definition
            .validate()
            .map_err(|message| WorkflowLoadError::InvalidWorkflow {
                path: path.to_path_buf(),
                message,
            })
    }
}

impl Default for WorkflowLoader {
    fn default() -> Self {
        Self::new()
    }
}
