//! Durable state machine for human-loop child approvals.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    DecisionRecorded,
    Delivered,
    DeliveryFailed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableApproval {
    pub parent_session_id: String,
    pub child_session_id: String,
    #[serde(default)]
    pub child_attempt: u32,
    pub request_id: String,
    pub tool_name: String,
    pub permission: String,
    pub resource: String,
    pub created_at: String,
    pub updated_at: String,
    pub version: u64,
    pub state: ApprovalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile {
    schema_version: u32,
    revision: u64,
    approvals: Vec<DurableApproval>,
}

#[derive(Debug)]
pub struct ApprovalRegistry {
    path: PathBuf,
    revision: u64,
    records: HashMap<(String, String, u32, String), DurableApproval>,
}

pub type SharedApprovalRegistry = std::sync::Arc<std::sync::Mutex<ApprovalRegistry>>;

impl ApprovalRegistry {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let loaded = match load_file(&path) {
            Ok(file) => Ok(file),
            Err(primary) => match load_file(&backup_path(&path)) {
                Ok(file) => {
                    let bytes = serde_json::to_vec_pretty(&file).map_err(io::Error::other)?;
                    replace_primary(&path, &bytes)?;
                    Ok(file)
                }
                Err(backup)
                    if primary.kind() == io::ErrorKind::NotFound
                        && backup.kind() == io::ErrorKind::NotFound =>
                {
                    Err(primary)
                }
                Err(backup) if primary.kind() == io::ErrorKind::NotFound => Err(backup),
                Err(_) => Err(primary),
            },
        };
        match loaded {
            Ok(file) => {
                if file.schema_version != SCHEMA_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unsupported approval registry schema {}",
                            file.schema_version
                        ),
                    ));
                }
                Ok(Self {
                    path,
                    revision: file.revision,
                    records: file
                        .approvals
                        .into_iter()
                        .map(|record| {
                            (
                                (
                                    record.parent_session_id.clone(),
                                    record.child_session_id.clone(),
                                    record.child_attempt,
                                    record.request_id.clone(),
                                ),
                                record,
                            )
                        })
                        .collect(),
                })
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self {
                path,
                revision: 0,
                records: HashMap::new(),
            }),
            Err(error) => Err(error),
        }
    }

    pub fn register(&mut self, mut record: DurableApproval) -> io::Result<()> {
        let key = (
            record.parent_session_id.clone(),
            record.child_session_id.clone(),
            record.child_attempt,
            record.request_id.clone(),
        );
        if self.records.contains_key(&key) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "approval request already registered",
            ));
        }
        record.state = ApprovalState::Pending;
        record.approved = None;
        self.records.insert(key.clone(), record);
        if let Err(error) = self.persist() {
            self.records.remove(&key);
            return Err(error);
        }
        Ok(())
    }

    pub fn record_decision(
        &mut self,
        parent_id: &str,
        child_id: &str,
        child_attempt: u32,
        request_id: &str,
        approved: bool,
    ) -> io::Result<Option<DurableApproval>> {
        let key = approval_key(parent_id, child_id, child_attempt, request_id);
        let Some(record) = self.records.get_mut(&key) else {
            return Ok(None);
        };
        if record.state != ApprovalState::Pending {
            return Ok(None);
        }
        let previous = record.clone();
        record.state = ApprovalState::DecisionRecorded;
        record.approved = Some(approved);
        bump(record);
        let updated = record.clone();
        if let Err(error) = self.persist() {
            self.records.insert(key, previous);
            return Err(error);
        }
        Ok(Some(updated))
    }

    pub fn finish(
        &mut self,
        parent_id: &str,
        child_id: &str,
        child_attempt: u32,
        request_id: &str,
        delivered: bool,
        reason: Option<&str>,
    ) -> io::Result<Option<DurableApproval>> {
        let key = approval_key(parent_id, child_id, child_attempt, request_id);
        let Some(record) = self.records.get_mut(&key) else {
            return Ok(None);
        };
        if delivered && record.state != ApprovalState::DecisionRecorded {
            return Ok(None);
        }
        if !delivered
            && !matches!(
                record.state,
                ApprovalState::Pending | ApprovalState::DecisionRecorded
            )
        {
            return Ok(None);
        }
        let previous = record.clone();
        record.state = if reason == Some("approval_timeout") {
            ApprovalState::Expired
        } else if delivered {
            ApprovalState::Delivered
        } else {
            ApprovalState::DeliveryFailed
        };
        record.reason = reason.map(str::to_string);
        bump(record);
        let updated = record.clone();
        if let Err(error) = self.persist() {
            self.records.insert(key, previous);
            return Err(error);
        }
        Ok(Some(updated))
    }

    pub fn reconcile_restart(&mut self) -> io::Result<Vec<DurableApproval>> {
        let previous = self.records.clone();
        let mut changed = Vec::new();
        for record in self.records.values_mut() {
            if matches!(
                record.state,
                ApprovalState::Pending | ApprovalState::DecisionRecorded
            ) {
                record.state = ApprovalState::DeliveryFailed;
                record.reason = Some("server_restart".to_string());
                bump(record);
                changed.push(record.clone());
            }
        }
        if !changed.is_empty() {
            if let Err(error) = self.persist() {
                self.records = previous;
                return Err(error);
            }
        }
        Ok(changed)
    }

    #[cfg(test)]
    fn get(&self, child: &str, request: &str) -> Option<&DurableApproval> {
        self.records
            .values()
            .find(|record| record.child_session_id == child && record.request_id == request)
    }

    fn persist(&mut self) -> io::Result<()> {
        let next_revision = self.revision.saturating_add(1);
        let file = RegistryFile {
            schema_version: SCHEMA_VERSION,
            revision: next_revision,
            approvals: self.records.values().cloned().collect(),
        };
        atomic_write(
            &self.path,
            &serde_json::to_vec_pretty(&file).map_err(io::Error::other)?,
        )?;
        self.revision = next_revision;
        Ok(())
    }
}

fn approval_key(
    parent_id: &str,
    child_id: &str,
    child_attempt: u32,
    request_id: &str,
) -> (String, String, u32, String) {
    (
        parent_id.to_string(),
        child_id.to_string(),
        child_attempt,
        request_id.to_string(),
    )
}

fn bump(record: &mut DurableApproval) {
    record.version = record.version.saturating_add(1);
    record.updated_at = chrono::Utc::now().to_rfc3339();
}

fn load_file(path: &Path) -> io::Result<RegistryFile> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn backup_path(path: &Path) -> PathBuf {
    path.with_extension("json.bak")
}

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("registry path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let backup = backup_path(path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    if path.exists() {
        std::fs::copy(path, &backup)?;
        File::open(&backup)?.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn replace_primary(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("registry path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.recovery.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> DurableApproval {
        DurableApproval {
            parent_session_id: "parent".into(),
            child_session_id: "child".into(),
            child_attempt: 1,
            request_id: "request".into(),
            tool_name: "Bash".into(),
            permission: "execute".into(),
            resource: "git status".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            version: 1,
            state: ApprovalState::Pending,
            approved: None,
            reason: None,
        }
    }

    #[test]
    fn decision_is_one_shot_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("child-approvals-v1.json");
        let mut registry = ApprovalRegistry::open(path.clone()).unwrap();
        registry.register(pending()).unwrap();
        assert!(registry
            .record_decision("parent", "child", 1, "request", true)
            .unwrap()
            .is_some());
        assert!(registry
            .record_decision("parent", "child", 1, "request", false)
            .unwrap()
            .is_none());
        let mut restored = ApprovalRegistry::open(path).unwrap();
        assert_eq!(
            restored.get("child", "request").unwrap().state,
            ApprovalState::DecisionRecorded
        );
        let reconciled = restored.reconcile_restart().unwrap();
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].state, ApprovalState::DeliveryFailed);
    }

    #[test]
    fn corrupt_primary_falls_back_to_last_known_good() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("child-approvals-v1.json");
        let mut registry = ApprovalRegistry::open(path.clone()).unwrap();
        registry.register(pending()).unwrap();
        registry
            .record_decision("parent", "child", 1, "request", true)
            .unwrap();
        std::fs::write(&path, b"{").unwrap();
        let restored = ApprovalRegistry::open(path).unwrap();
        assert!(restored.get("child", "request").is_some());
    }

    #[test]
    fn concurrent_decisions_record_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let registry = std::sync::Arc::new(std::sync::Mutex::new(
            ApprovalRegistry::open(dir.path().join("child-approvals-v1.json")).unwrap(),
        ));
        registry.lock().unwrap().register(pending()).unwrap();
        let successes = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|index| {
                    let registry = registry.clone();
                    scope.spawn(move || {
                        registry
                            .lock()
                            .unwrap()
                            .record_decision("parent", "child", 1, "request", index % 2 == 0)
                            .unwrap()
                            .is_some()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|accepted| *accepted)
                .count()
        });
        assert_eq!(successes, 1);
    }

    #[test]
    fn corrupt_primary_and_backup_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("child-approvals-v1.json");
        std::fs::write(&path, b"{").unwrap();
        std::fs::write(backup_path(&path), b"{").unwrap();
        assert_eq!(
            ApprovalRegistry::open(path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn missing_primary_and_corrupt_backup_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("child-approvals-v1.json");
        std::fs::write(backup_path(&path), b"{").unwrap();
        assert_eq!(
            ApprovalRegistry::open(path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn attempts_are_isolated_and_can_be_finished_independently() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry =
            ApprovalRegistry::open(dir.path().join("child-approvals-v1.json")).unwrap();
        let first = pending();
        let mut retry = first.clone();
        retry.child_attempt = 2;
        registry.register(first).unwrap();
        registry.register(retry).unwrap();
        assert!(registry
            .record_decision("parent", "child", 1, "request", true)
            .unwrap()
            .is_some());
        assert!(registry
            .finish("parent", "child", 1, "request", true, None)
            .unwrap()
            .is_some());
        assert!(registry
            .record_decision("parent", "child", 2, "request", false)
            .unwrap()
            .is_some());
    }

    #[test]
    fn last_known_good_pending_is_failed_closed_after_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("child-approvals-v1.json");
        let mut registry = ApprovalRegistry::open(path.clone()).unwrap();
        registry.register(pending()).unwrap();
        registry
            .record_decision("parent", "child", 1, "request", true)
            .unwrap();

        // Simulate a torn latest write. The backup contains the preceding
        // Pending state, which must never become actionable after restart.
        std::fs::write(&path, b"{").unwrap();
        let mut restored = ApprovalRegistry::open(path).unwrap();
        let reconciled = restored.reconcile_restart().unwrap();
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].state, ApprovalState::DeliveryFailed);
        assert_eq!(reconciled[0].reason.as_deref(), Some("server_restart"));
        assert!(restored
            .record_decision("parent", "child", 1, "request", false)
            .unwrap()
            .is_none());
    }

    #[test]
    fn failed_persist_rolls_back_in_memory_transition() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        std::fs::create_dir(&store).unwrap();
        let path = store.join("child-approvals-v1.json");
        let mut registry = ApprovalRegistry::open(path).unwrap();
        std::fs::remove_dir(&store).unwrap();
        std::fs::write(&store, b"not a directory").unwrap();

        assert!(registry.register(pending()).is_err());
        assert!(registry.get("child", "request").is_none());
    }

    #[test]
    fn registries_are_isolated_for_separate_app_states() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let mut first = ApprovalRegistry::open(first_dir.path().join("registry.json")).unwrap();
        let mut second = ApprovalRegistry::open(second_dir.path().join("registry.json")).unwrap();
        first.register(pending()).unwrap();
        second.register(pending()).unwrap();

        assert!(first
            .record_decision("parent", "child", 1, "request", true)
            .unwrap()
            .is_some());
        assert_eq!(
            second.get("child", "request").unwrap().state,
            ApprovalState::Pending
        );
    }
}
