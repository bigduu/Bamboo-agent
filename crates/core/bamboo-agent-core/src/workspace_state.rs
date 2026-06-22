use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Instant;

const MAX_TRACKED_WORKSPACES: usize = 2_000;

#[derive(Clone, Debug)]
struct WorkspaceEntry {
    workspace: PathBuf,
    last_touched: Instant,
}

fn workspaces() -> &'static DashMap<String, WorkspaceEntry> {
    static WORKSPACES: OnceLock<DashMap<String, WorkspaceEntry>> = OnceLock::new();
    WORKSPACES.get_or_init(DashMap::new)
}

pub fn set_workspace(session_id: &str, workspace: PathBuf) {
    let store = workspaces();
    store.insert(
        session_id.to_string(),
        WorkspaceEntry {
            workspace,
            last_touched: Instant::now(),
        },
    );
    evict_oldest_if_needed(store, MAX_TRACKED_WORKSPACES);
}

pub fn get_workspace(session_id: &str) -> Option<PathBuf> {
    let mut entry = workspaces().get_mut(session_id)?;
    entry.last_touched = Instant::now();
    Some(entry.workspace.clone())
}

/// Resolver for the configured default workspace.
///
/// `agent-core` is a core layer and must not depend on the infrastructure
/// config crate. Instead, the composition root (the server bootstrap) registers
/// a provider here via [`set_default_workspace_provider`]; until then this
/// resolves to `None` and callers fall back to the process working directory.
/// A boxed closure (not a bare `fn`) so the composition root can capture state —
/// e.g. a handle to the server's live in-memory config — instead of being forced
/// to re-read config from disk on every call. #38.
type DefaultWorkspaceProvider = Box<dyn Fn() -> Option<PathBuf> + Send + Sync>;
static DEFAULT_WORKSPACE_PROVIDER: OnceLock<DefaultWorkspaceProvider> = OnceLock::new();

/// Register the provider that resolves the configured default workspace.
///
/// Called once at startup by the layer that owns the infrastructure config, so
/// that this crate keeps a dependency only on `bamboo-domain`. Subsequent calls
/// are ignored (first registration wins).
pub fn set_default_workspace_provider(provider: DefaultWorkspaceProvider) {
    let _ = DEFAULT_WORKSPACE_PROVIDER.set(provider);
}

pub fn get_configured_default_workspace() -> Option<PathBuf> {
    DEFAULT_WORKSPACE_PROVIDER
        .get()
        .and_then(|provider| provider())
}

/// Whether a default-workspace provider has been registered — i.e. we're running
/// in a context (the server) that owns the live config and wired the provider at
/// startup. When true the provider is authoritative: callers must NOT fall back
/// to a disk read even if it resolves to `None`, since that would re-introduce a
/// divergent disk read of config (#38 / #131). When false (SDK / CLI / unit
/// tests) callers may use their own config source.
pub fn has_default_workspace_provider() -> bool {
    DEFAULT_WORKSPACE_PROVIDER.get().is_some()
}

pub fn ensure_session_workspace(session_id: &str, preferred: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(workspace) = preferred {
        set_workspace(session_id, workspace.clone());
        return Some(workspace);
    }

    if let Some(existing) = get_workspace(session_id) {
        return Some(existing);
    }

    if let Some(configured) = get_configured_default_workspace() {
        set_workspace(session_id, configured.clone());
        return Some(configured);
    }

    None
}

pub fn workspace_or_process_cwd(session_id: Option<&str>) -> PathBuf {
    if let Some(session_id) = session_id {
        if let Some(workspace) = ensure_session_workspace(session_id, None) {
            return workspace;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn evict_oldest_if_needed(store: &DashMap<String, WorkspaceEntry>, max_tracked_workspaces: usize) {
    if store.len() <= max_tracked_workspaces {
        return;
    }

    let oldest = store
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().last_touched))
        .min_by_key(|(_, touched)| *touched);
    if let Some((session_id, _)) = oldest {
        let _ = store.remove(&session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn evict_oldest_if_needed_removes_least_recent_entry() {
        let store: DashMap<String, WorkspaceEntry> = DashMap::new();
        let now = Instant::now();
        store.insert(
            "s1".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s1"),
                last_touched: now - Duration::from_secs(3),
            },
        );
        store.insert(
            "s2".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s2"),
                last_touched: now - Duration::from_secs(2),
            },
        );
        store.insert(
            "s3".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s3"),
                last_touched: now - Duration::from_secs(1),
            },
        );

        evict_oldest_if_needed(&store, 2);

        assert_eq!(store.len(), 2);
        assert!(!store.contains_key("s1"));
        assert!(store.contains_key("s2"));
        assert!(store.contains_key("s3"));
    }
}
