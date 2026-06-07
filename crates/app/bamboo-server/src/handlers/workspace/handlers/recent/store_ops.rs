use std::time::{SystemTime, UNIX_EPOCH};

use crate::handlers::workspace::types::{
    AddRecentWorkspaceRequest, RecentWorkspaceEntry, RecentWorkspaceStore,
};

pub(super) fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn upsert_recent_workspace(
    store: &mut RecentWorkspaceStore,
    payload: &AddRecentWorkspaceRequest,
    now: u64,
) {
    if let Some(existing) = store
        .items
        .iter_mut()
        .find(|entry| entry.path == payload.path)
    {
        existing.metadata = payload.metadata.clone();
        existing.last_opened = now;
    } else {
        store.items.insert(
            0,
            RecentWorkspaceEntry {
                path: payload.path.clone(),
                metadata: payload.metadata.clone(),
                last_opened: now,
            },
        );
    }

    store
        .items
        .sort_by_key(|item| std::cmp::Reverse(item.last_opened));
    store.items.truncate(50);
}
