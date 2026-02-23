mod loader;

#[cfg(test)]
mod tests;

use agent_core::composition::ToolExpr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: String,
    pub composition: ToolExpr,
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        validate_required("id", &self.id)?;
        validate_required("name", &self.name)?;
        validate_required("description", &self.description)?;
        validate_required("version", &self.version)?;
        validate_expr(&self.composition)
    }
}

fn validate_required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} cannot be empty"));
    }

    Ok(())
}

fn validate_expr(expr: &ToolExpr) -> Result<(), String> {
    match expr {
        ToolExpr::Call { tool, .. } => {
            if tool.trim().is_empty() {
                return Err("composition call tool cannot be empty".to_string());
            }
        }
        ToolExpr::Sequence { steps, .. } => {
            if steps.is_empty() {
                return Err("composition sequence requires at least one step".to_string());
            }

            for step in steps {
                validate_expr(step)?;
            }
        }
        ToolExpr::Parallel { branches, .. } => {
            if branches.is_empty() {
                return Err("composition parallel requires at least one branch".to_string());
            }

            for branch in branches {
                validate_expr(branch)?;
            }
        }
        ToolExpr::Choice {
            then_branch,
            else_branch,
            ..
        } => {
            validate_expr(then_branch)?;

            if let Some(else_expr) = else_branch {
                validate_expr(else_expr)?;
            }
        }
        ToolExpr::Retry {
            expr, max_attempts, ..
        } => {
            if *max_attempts == 0 {
                return Err("composition retry max_attempts must be greater than zero".to_string());
            }

            validate_expr(expr)?;
        }
        ToolExpr::Let { var, expr, body } => {
            if var.trim().is_empty() {
                return Err("composition let variable cannot be empty".to_string());
            }

            validate_expr(expr)?;
            validate_expr(body)?;
        }
        ToolExpr::Var(name) => {
            if name.trim().is_empty() {
                return Err("composition var cannot be empty".to_string());
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum WorkflowLoadError {
    FileNotFound(PathBuf),
    NotAFile(PathBuf),
    NotADirectory(PathBuf),
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Parse {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    InvalidWorkflow {
        path: PathBuf,
        message: String,
    },
}

impl fmt::Display for WorkflowLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowLoadError::FileNotFound(path) => {
                write!(f, "workflow file not found: {}", path.display())
            }
            WorkflowLoadError::NotAFile(path) => {
                write!(f, "path is not a file: {}", path.display())
            }
            WorkflowLoadError::NotADirectory(path) => {
                write!(f, "path is not a directory: {}", path.display())
            }
            WorkflowLoadError::Io { path, source } => {
                write!(f, "I/O error for {}: {}", path.display(), source)
            }
            WorkflowLoadError::Parse { path, source } => {
                write!(f, "failed to parse workflow {}: {}", path.display(), source)
            }
            WorkflowLoadError::InvalidWorkflow { path, message } => {
                write!(f, "invalid workflow in {}: {}", path.display(), message)
            }
        }
    }
}

impl std::error::Error for WorkflowLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WorkflowLoadError::Io { source, .. } => Some(source),
            WorkflowLoadError::Parse { source, .. } => Some(source),
            _ => None,
        }
    }
}

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
        let home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
        Self {
            workflows_dir: home.join(".bamboo").join("workflows"),
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
