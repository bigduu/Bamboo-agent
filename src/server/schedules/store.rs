use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::core::ReasoningEffort;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

fn other_io_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message.into())
}

async fn atomic_write_json(path: &Path, bytes: Vec<u8>) -> io::Result<()> {
    let tmp = path.with_extension(format!("json.tmp.{}", Uuid::new_v4()));

    // Write + fsync to ensure data is on disk before rename.
    {
        let mut file = fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
        file.sync_all().await?;
    }

    fs::rename(&tmp, path).await?;

    // fsync parent directory to persist the rename metadata.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }

    Ok(())
}

/// Remove leftover `.tmp.*` files from a previous interrupted atomic write.
async fn cleanup_stale_tmp_files(dir: &Path, prefix: &str) {
    let mut entries = match fs::read_dir(dir).await {
        Ok(e) => e,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(prefix) {
                tracing::info!("Removing stale temp file: {}", entry.path().display());
                let _ = fs::remove_file(entry.path()).await;
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScheduleRunConfig {
    /// Optional system prompt override for new sessions created by this schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Optional task message to add to the new session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_message: Option<String>,
    /// Model used when auto-executing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional reasoning effort override used when auto-executing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional workspace path context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Optional enhancement prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enhance_prompt: Option<String>,
    /// If true, immediately execute the new session (only meaningful if `task_message` exists).
    #[serde(default)]
    pub auto_execute: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    /// Simple interval schedule (seconds). (Cron can be added later.)
    pub interval_seconds: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: DateTime<Utc>,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchedulesIndex {
    version: u32,
    updated_at: DateTime<Utc>,
    schedules: HashMap<String, ScheduleEntry>,
}

impl SchedulesIndex {
    fn empty() -> Self {
        Self {
            version: 1,
            updated_at: Utc::now(),
            schedules: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedScheduleRun {
    pub schedule_id: String,
    pub schedule_name: String,
    pub run_config: ScheduleRunConfig,
}

#[derive(Debug)]
pub struct ScheduleStore {
    index_path: PathBuf,
    index: RwLock<SchedulesIndex>,
    write_lock: Mutex<()>,
}

impl ScheduleStore {
    pub async fn new(bamboo_home_dir: PathBuf) -> io::Result<Self> {
        let index_path = bamboo_home_dir.join("schedules.json");

        let index = if index_path.exists() {
            let raw = fs::read_to_string(&index_path).await?;
            match serde_json::from_str::<SchedulesIndex>(&raw) {
                Ok(parsed) => parsed,
                Err(e) => {
                    // Corrupted file (e.g. partial write before crash).
                    // Back up the broken file and start fresh so the app can boot.
                    let backup_path =
                        index_path.with_extension(format!("json.corrupted.{}", Uuid::new_v4()));
                    tracing::error!(
                        "schedules.json is corrupted ({}). Backing up to {} and resetting.",
                        e,
                        backup_path.display()
                    );
                    if let Err(rename_err) = fs::rename(&index_path, &backup_path).await {
                        tracing::warn!(
                            "Failed to back up corrupted schedules.json: {}",
                            rename_err
                        );
                    }
                    let fresh = SchedulesIndex::empty();
                    atomic_write_json(
                        &index_path,
                        serde_json::to_vec_pretty(&fresh)
                            .map_err(|e| other_io_error(e.to_string()))?,
                    )
                    .await?;
                    fresh
                }
            }
        } else {
            let index = SchedulesIndex::empty();
            atomic_write_json(
                &index_path,
                serde_json::to_vec_pretty(&index).map_err(|e| other_io_error(e.to_string()))?,
            )
            .await?;
            index
        };

        // Clean up stale temp files left behind by interrupted atomic writes.
        cleanup_stale_tmp_files(&bamboo_home_dir, "schedules.json.tmp.").await;

        Ok(Self {
            index_path,
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
        })
    }

    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    async fn update_index<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&mut SchedulesIndex) -> io::Result<T>,
    {
        let _guard = self.write_lock.lock().await;
        let mut index = self.index.write().await;
        let out = f(&mut index)?;
        index.updated_at = Utc::now();
        atomic_write_json(
            &self.index_path,
            serde_json::to_vec_pretty(&*index).map_err(|e| other_io_error(e.to_string()))?,
        )
        .await?;
        Ok(out)
    }

    pub async fn list_schedules(&self) -> Vec<ScheduleEntry> {
        let index = self.index.read().await;
        let mut items: Vec<_> = index.schedules.values().cloned().collect();
        items.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        items
    }

    pub async fn get_schedule(&self, id: &str) -> Option<ScheduleEntry> {
        let index = self.index.read().await;
        index.schedules.get(id).cloned()
    }

    pub async fn create_schedule(
        &self,
        name: String,
        interval_seconds: u64,
        enabled: bool,
        run_config: ScheduleRunConfig,
    ) -> io::Result<ScheduleEntry> {
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let next_run_at = now + Duration::seconds(interval_seconds as i64);
        let entry = ScheduleEntry {
            id: id.clone(),
            name,
            enabled,
            interval_seconds,
            created_at: now,
            updated_at: now,
            last_run_at: None,
            next_run_at,
            run_config,
        };

        self.update_index(|index| {
            index.schedules.insert(id.clone(), entry.clone());
            Ok(entry.clone())
        })
        .await
    }

    pub async fn patch_schedule(
        &self,
        id: &str,
        name: Option<String>,
        enabled: Option<bool>,
        interval_seconds: Option<u64>,
        run_config: Option<ScheduleRunConfig>,
    ) -> io::Result<Option<ScheduleEntry>> {
        self.update_index(|index| {
            let Some(existing) = index.schedules.get_mut(id) else {
                return Ok(None);
            };
            if let Some(name) = name {
                existing.name = name;
            }
            if let Some(enabled) = enabled {
                existing.enabled = enabled;
            }
            if let Some(interval_seconds) = interval_seconds {
                existing.interval_seconds = interval_seconds;
                // Reschedule from "now" so changes take effect quickly.
                existing.next_run_at = Utc::now() + Duration::seconds(interval_seconds as i64);
            }
            if let Some(run_config) = run_config {
                existing.run_config = run_config;
            }
            existing.updated_at = Utc::now();
            Ok(Some(existing.clone()))
        })
        .await
    }

    pub async fn delete_schedule(&self, id: &str) -> io::Result<bool> {
        self.update_index(|index| Ok(index.schedules.remove(id).is_some()))
            .await
    }

    /// Claim all due schedules and advance their `next_run_at`.
    ///
    /// Returns a list of run descriptors to execute out-of-band.
    ///
    /// **Important**: only writes to disk when at least one schedule is actually
    /// due.  The ticker calls this every few seconds, so avoiding unnecessary
    /// writes is critical for disk health and crash-safety.
    pub async fn claim_due_runs(&self, now: DateTime<Utc>) -> io::Result<Vec<ClaimedScheduleRun>> {
        // --- Phase 1: read-only check (no write lock, no disk I/O) ---
        {
            let index = self.index.read().await;
            let any_due = index
                .schedules
                .values()
                .any(|e| e.enabled && e.next_run_at <= now);
            if !any_due {
                return Ok(Vec::new());
            }
        }

        // --- Phase 2: write path (only reached when ≥1 schedule is due) ---
        self.update_index(|index| {
            let mut out = Vec::new();
            for entry in index.schedules.values_mut() {
                if !entry.enabled {
                    continue;
                }
                if entry.next_run_at > now {
                    continue;
                }
                entry.last_run_at = Some(now);
                entry.next_run_at = now + Duration::seconds(entry.interval_seconds as i64);
                entry.updated_at = now;
                out.push(ClaimedScheduleRun {
                    schedule_id: entry.id.clone(),
                    schedule_name: entry.name.clone(),
                    run_config: entry.run_config.clone(),
                });
            }
            Ok(out)
        })
        .await
    }

    /// Create a run descriptor immediately (does not change the schedule cadence).
    pub async fn create_run_now(&self, id: &str) -> io::Result<Option<ClaimedScheduleRun>> {
        let entry = self.get_schedule(id).await;
        Ok(entry.map(|e| ClaimedScheduleRun {
            schedule_id: e.id,
            schedule_name: e.name,
            run_config: e.run_config,
        }))
    }
}
