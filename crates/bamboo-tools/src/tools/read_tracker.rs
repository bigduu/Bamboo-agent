use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::Mutex;

const MAX_TRACKED_SESSIONS: usize = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadState {
    Unread,
    Stale,
    Fresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileSnapshot {
    size_bytes: u64,
    modified_ns: Option<u128>,
}

#[derive(Debug, Default)]
struct SessionReads {
    files: HashMap<String, FileSnapshot>,
    last_touched: Option<Instant>,
}

fn tracker() -> &'static DashMap<String, Arc<Mutex<SessionReads>>> {
    static TRACKER: OnceLock<DashMap<String, Arc<Mutex<SessionReads>>>> = OnceLock::new();
    TRACKER.get_or_init(DashMap::new)
}

fn normalize_path(path: &str) -> String {
    std::fs::canonicalize(path)
        .ok()
        .and_then(|value| value.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| path.to_string())
}

fn snapshot_for_path(path: &str) -> Option<FileSnapshot> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());

    Some(FileSnapshot {
        size_bytes: metadata.len(),
        modified_ns,
    })
}

async fn cleanup_if_needed() {
    let map = tracker();
    if map.len() <= MAX_TRACKED_SESSIONS {
        return;
    }

    let mut oldest: Option<(String, Instant)> = None;
    for entry in map.iter() {
        let key = entry.key().clone();
        let session = entry.value().clone();
        let touched = session.lock().await.last_touched.unwrap_or(Instant::now());
        match oldest {
            Some((_, ts)) if touched >= ts => {}
            _ => oldest = Some((key, touched)),
        }
    }

    if let Some((key, _)) = oldest {
        map.remove(&key);
    }
}

pub async fn mark_read(session_id: &str, path: &str) {
    let normalized = normalize_path(path);
    let snapshot = snapshot_for_path(path);
    let entry = tracker()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(SessionReads::default())))
        .clone();

    {
        let mut guard = entry.lock().await;
        guard.last_touched = Some(Instant::now());
        if let Some(snapshot) = snapshot {
            guard.files.insert(normalized, snapshot);
        } else {
            // Keep a sentinel entry so we still know a read happened.
            guard.files.insert(
                normalized,
                FileSnapshot {
                    size_bytes: 0,
                    modified_ns: None,
                },
            );
        }
    }

    cleanup_if_needed().await;
}

pub async fn has_read(session_id: &str, path: &str) -> bool {
    let normalized = normalize_path(path);
    let Some(entry) = tracker().get(session_id).map(|value| value.clone()) else {
        return false;
    };

    let mut guard = entry.lock().await;
    guard.last_touched = Some(Instant::now());
    guard.files.contains_key(&normalized)
}

pub async fn read_state(session_id: &str, path: &str) -> ReadState {
    let normalized = normalize_path(path);
    let Some(entry) = tracker().get(session_id).map(|value| value.clone()) else {
        return ReadState::Unread;
    };

    let mut guard = entry.lock().await;
    guard.last_touched = Some(Instant::now());
    let Some(snapshot) = guard.files.get(&normalized).copied() else {
        return ReadState::Unread;
    };

    let Some(current) = snapshot_for_path(path) else {
        return ReadState::Stale;
    };

    if snapshot == current {
        ReadState::Fresh
    } else {
        ReadState::Stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_id(label: &str) -> String {
        format!("read-tracker-{label}-{}", uuid::Uuid::new_v4())
    }

    #[tokio::test]
    async fn read_state_transitions_from_fresh_to_stale_after_external_change() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("fresh-stale");

        mark_read(&session, &path).await;
        assert!(has_read(&session, &path).await);
        assert_eq!(read_state(&session, &path).await, ReadState::Fresh);

        tokio::fs::write(file.path(), "v2 changed").await.unwrap();
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn missing_file_marked_read_is_treated_as_stale_for_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.txt");
        let path_str = path.to_string_lossy().to_string();
        let session = session_id("missing");

        mark_read(&session, &path_str).await;
        assert!(has_read(&session, &path_str).await);
        assert_eq!(read_state(&session, &path_str).await, ReadState::Stale);
    }
}
