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

pub fn workspace_or_process_cwd(session_id: Option<&str>) -> PathBuf {
    if let Some(session_id) = session_id {
        if let Some(workspace) = get_workspace(session_id) {
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
