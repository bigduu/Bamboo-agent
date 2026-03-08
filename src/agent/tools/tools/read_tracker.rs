use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

fn tracker() -> &'static DashMap<String, Arc<Mutex<HashSet<String>>>> {
    static TRACKER: OnceLock<DashMap<String, Arc<Mutex<HashSet<String>>>>> = OnceLock::new();
    TRACKER.get_or_init(DashMap::new)
}

fn normalize_path(path: &str) -> String {
    std::fs::canonicalize(path)
        .ok()
        .and_then(|value| value.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| path.to_string())
}

pub async fn mark_read(session_id: &str, path: &str) {
    let normalized = normalize_path(path);
    let entry = tracker()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(HashSet::new())))
        .clone();

    entry.lock().await.insert(normalized);
}

pub async fn has_read(session_id: &str, path: &str) -> bool {
    let normalized = normalize_path(path);
    let Some(entry) = tracker().get(session_id).map(|value| value.clone()) else {
        return false;
    };

    let contains = entry.lock().await.contains(&normalized);
    contains
}
