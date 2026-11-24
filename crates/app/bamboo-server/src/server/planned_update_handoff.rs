//! Durable handoff between two desktop-owned Bamboo processes.

use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort, Storage};
use bamboo_engine::session_activation::ParkedUpdateRoot;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const SCHEMA_VERSION: u8 = 1;
const MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateHandoff {
    schema_version: u8,
    request: Uuid,
    pub(super) roots: Vec<ParkedUpdateRoot>,
}

#[derive(Default, Serialize)]
pub(super) struct UpdateResumeReport {
    resumed: usize,
    advanced: usize,
    pending_sessions: Vec<String>,
}

impl UpdateHandoff {
    fn validate(&self, request: Uuid) -> io::Result<()> {
        let mut targets = BTreeSet::new();
        let prepared_batch = self.roots.first().map(|root| root.update_id);
        if request.is_nil()
            || self.schema_version != SCHEMA_VERSION
            || self.request != request
            || self.roots.iter().any(|root| {
                root.update_id.is_nil()
                    || Some(root.update_id) != prepared_batch
                    || root.lease_epoch == 0
                    || !targets.insert(&root.session_id)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid planned update handoff",
            ));
        }
        Ok(())
    }
}

/// The profile is already selected and owned by the desktop process. The
/// command supplies only an opaque request id, never an arbitrary file path.
fn directory(profile: &Path, create: bool) -> io::Result<PathBuf> {
    let path = profile.join("managed-update");
    if create {
        match fs::create_dir(&path) {
            Ok(()) => sync_directory(profile)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error),
        }
    }
    if !fs::symlink_metadata(&path)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid planned update directory",
        ));
    }
    Ok(path)
}

fn record_path(directory: &Path, request: Uuid) -> PathBuf {
    directory.join(format!("{request}.json"))
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> io::Result<()> {
    Ok(())
}

pub(super) fn publish(profile: &Path, request: Uuid, roots: &[ParkedUpdateRoot]) -> io::Result<()> {
    let handoff = UpdateHandoff {
        schema_version: SCHEMA_VERSION,
        request,
        roots: roots.to_vec(),
    };
    handoff.validate(request)?;
    let bytes = serde_json::to_vec(&handoff)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Planned update handoff is too large",
        ));
    }
    let directory = directory(profile, true)?;
    let path = record_path(&directory, request);
    let mut file = tempfile::NamedTempFile::new_in(&directory)?;
    file.write_all(&bytes)?;
    file.as_file().sync_all()?;
    match file.persist_noclobber(&path) {
        Ok(_) => (),
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            if read(profile, request)? != handoff {
                return Err(error.error);
            }
        }
        Err(error) => return Err(error.error),
    }
    sync_directory(&directory)
}

pub(super) fn read(profile: &Path, request: Uuid) -> io::Result<UpdateHandoff> {
    let path = record_path(&directory(profile, false)?, request);
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid planned update record",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Planned update handoff is too large",
        ));
    }
    let handoff: UpdateHandoff = serde_json::from_slice(&bytes)?;
    handoff.validate(request)?;
    Ok(handoff)
}

/// Called after the desktop has verified its replacement runtime. Reopen the
/// recorded profile and prove that each exact old Actor finished before
/// asking the ordinary coordinator to claim the continuation.
pub(super) async fn resume(
    state: &crate::app_state::AppState,
    request: Uuid,
) -> io::Result<UpdateResumeReport> {
    let profile = state.app_data_dir.clone();
    let handoff = tokio::task::spawn_blocking(move || read(&profile, request))
        .await
        .map_err(io::Error::other)??;
    let mut report = UpdateResumeReport::default();
    for root in &handoff.roots {
        let activation = state.session_store.inspect_actor(&root.session_id).await;
        if let Ok(actor) = activation {
            if let Some(owner) = actor
                .activation
                .filter(|_| actor.actor.session_created_at == root.created_at)
            {
                if owner.lease_epoch > root.lease_epoch
                    && state
                        .session_store
                        .load_session(&root.session_id)
                        .await
                        .is_ok_and(|saved| {
                            saved.is_some_and(|session| {
                                session.host_run_id() == Some(owner.run_id.as_str())
                                    && session
                                        .metadata
                                        .get("runtime.suspend_reason")
                                        .map(String::as_str)
                                        != Some("planned_update")
                            })
                        })
                {
                    report.advanced += 1;
                    continue;
                }
                if owner.run_id == root.run_id
                    && owner.lease_epoch == root.lease_epoch
                    && owner.status == ActorActivationStatus::Succeeded
                {
                    match state
                        .child_completion_coordinator
                        .resume_planned_update_root(root)
                        .await
                    {
                        Ok(true) => {
                            report.resumed += 1;
                            continue;
                        }
                        Ok(false) => (),
                        Err(error) => {
                            tracing::warn!(session_id = %root.session_id, %error, "Planned update session recovery remains pending")
                        }
                    }
                }
            }
        }
        report.pending_sessions.push(root.session_id.clone());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reopens_exact_request_and_rejects_a_record_from_another_update() {
        let profile = tempfile::TempDir::new().unwrap();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        publish(profile.path(), first, &[]).unwrap();
        publish(profile.path(), first, &[]).unwrap();
        assert!(read(profile.path(), first).unwrap().roots.is_empty());
        assert!(read(profile.path(), second).is_err());
        let folder = directory(profile.path(), false).unwrap();
        fs::copy(record_path(&folder, first), record_path(&folder, second)).unwrap();
        assert_eq!(
            read(profile.path(), second).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
