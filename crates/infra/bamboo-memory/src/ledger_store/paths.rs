use std::path::PathBuf;

use bamboo_config::paths::bamboo_dir;
use bamboo_domain::ledger::LedgerScope;

pub const LEDGER_ROOT_DIR: &str = "ledger";
pub const LEDGER_VERSION_DIR: &str = "v1";
pub const SCOPES_DIR: &str = "scopes";
pub const GLOBAL_DIR: &str = "global";
pub const PROJECTS_DIR: &str = "projects";
pub const RECORDS_DIR: &str = "records";
pub const INDEXES_DIR: &str = "indexes";
pub const VIEWS_DIR: &str = "views";
pub const LOGS_DIR: &str = "logs";

/// Path resolver for the ledger's dedicated on-disk area, parallel to the
/// memory store's `memory/v1`:
///
/// ```text
/// {data_dir}/ledger/v1/scopes/{global|projects/<key>}/
///     records/<record_id>.md
///     indexes/{by_time.json, by_status.json}
///     views/{AGENDA.md, TODO.md}
///     logs/audit.jsonl
/// ```
#[derive(Debug, Clone)]
pub struct LedgerPathResolver {
    root: PathBuf,
}

impl Default for LedgerPathResolver {
    fn default() -> Self {
        Self::from_data_dir(bamboo_dir())
    }
}

impl LedgerPathResolver {
    pub fn from_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        let root = data_dir
            .into()
            .join(LEDGER_ROOT_DIR)
            .join(LEDGER_VERSION_DIR);
        Self { root }
    }

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn scope_root(&self, scope: LedgerScope, project_key: Option<&str>) -> PathBuf {
        let scopes = self.root.join(SCOPES_DIR);
        match scope {
            LedgerScope::Global => scopes.join(GLOBAL_DIR),
            LedgerScope::Project => scopes
                .join(PROJECTS_DIR)
                .join(project_key.unwrap_or("unknown")),
        }
    }

    pub fn records_dir(&self, scope: LedgerScope, project_key: Option<&str>) -> PathBuf {
        self.scope_root(scope, project_key).join(RECORDS_DIR)
    }

    pub fn record_path(
        &self,
        scope: LedgerScope,
        project_key: Option<&str>,
        record_id: &str,
    ) -> PathBuf {
        self.records_dir(scope, project_key)
            .join(format!("{}.md", record_id))
    }

    pub fn indexes_dir(&self, scope: LedgerScope, project_key: Option<&str>) -> PathBuf {
        self.scope_root(scope, project_key).join(INDEXES_DIR)
    }

    pub fn views_dir(&self, scope: LedgerScope, project_key: Option<&str>) -> PathBuf {
        self.scope_root(scope, project_key).join(VIEWS_DIR)
    }

    pub fn logs_dir(&self, scope: LedgerScope, project_key: Option<&str>) -> PathBuf {
        self.scope_root(scope, project_key).join(LOGS_DIR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_follow_v1_layout() {
        let resolver = LedgerPathResolver::from_data_dir("/tmp/.bamboo");
        assert_eq!(
            resolver.record_path(LedgerScope::Global, None, "rec_1"),
            PathBuf::from("/tmp/.bamboo/ledger/v1/scopes/global/records/rec_1.md")
        );
        assert_eq!(
            resolver.record_path(LedgerScope::Project, Some("proj-1"), "rec_2"),
            PathBuf::from("/tmp/.bamboo/ledger/v1/scopes/projects/proj-1/records/rec_2.md")
        );
    }
}
