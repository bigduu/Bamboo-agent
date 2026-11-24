//! Durable handoff between two desktop-owned Bamboo processes.

use bamboo_domain::session::model_context::sha256_hex;
use bamboo_domain::{
    is_root_planned_update_attempt, ActorActivationStatus, ActorDirectoryPort, AgentStatusState,
    Role, Session, Storage,
};
use bamboo_engine::session_activation::ParkedUpdateRoot;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const SCHEMA_VERSION: u8 = 1;
const MAX_BYTES: u64 = 4 * 1024 * 1024;
const AUTHORIZATION_BYTES: u64 = 64;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateHandoff {
    schema_version: u8,
    request: Uuid,
    pub(super) roots: Vec<ParkedUpdateRoot>,
}

/// Proof of the exact intent written before parked executions are retired.
/// Only the original Host keeps this until it has verified final closure.
pub(super) struct PublishedHandoff {
    directory: PathBuf,
    request: Uuid,
    digest: String,
}

impl PublishedHandoff {
    pub(super) fn authorize(&self) -> io::Result<()> {
        let bytes = read_bounded(&record_path(&self.directory, self.request), MAX_BYTES)?;
        if sha256_hex(&bytes) != self.digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Planned update intent changed before authorization",
            ));
        }
        persist_immutable(
            &self.directory,
            &authorization_path(&self.directory, self.request),
            self.digest.as_bytes(),
        )
    }
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
                    || root.fence.actor_id.as_str() != root.session_id
                    || root.fence.lease_epoch == 0
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

fn authorization_path(directory: &Path, request: Uuid) -> PathBuf {
    directory.join(format!("{request}.ready"))
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> io::Result<()> {
    Ok(())
}

pub(super) fn publish(
    profile: &Path,
    request: Uuid,
    roots: &[ParkedUpdateRoot],
) -> io::Result<PublishedHandoff> {
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
    persist_immutable(&directory, &record_path(&directory, request), &bytes)?;
    Ok(PublishedHandoff {
        directory,
        request,
        digest: sha256_hex(&bytes),
    })
}

fn persist_immutable(directory: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    match file.persist_noclobber(path) {
        Ok(_) => (),
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            if read_bounded(path, bytes.len() as u64)?.as_slice() != bytes {
                return Err(error.error);
            }
        }
        Err(error) => return Err(error.error),
    }
    sync_directory(directory)
}

fn read_bounded(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid planned update record",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Planned update handoff is too large",
        ));
    }
    Ok(bytes)
}

fn read(profile: &Path, request: Uuid) -> io::Result<UpdateHandoff> {
    let directory = directory(profile, false)?;
    let bytes = read_bounded(&record_path(&directory, request), MAX_BYTES)?;
    let handoff: UpdateHandoff = serde_json::from_slice(&bytes)?;
    handoff.validate(request)?;
    let authorization = read_bounded(
        &authorization_path(&directory, request),
        AUTHORIZATION_BYTES,
    )?;
    if sha256_hex(&bytes).as_bytes() != authorization.as_slice() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Planned update authorization does not match its intent",
        ));
    }
    Ok(handoff)
}

enum RootReplay {
    Retry,
    Advanced,
    Pending,
}

fn continued_checkpoint(root: &ParkedUpdateRoot, session: &Session) -> bool {
    if session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|runtime| runtime.status == AgentStatusState::Initializing)
    {
        return false;
    }
    let Some((before, after)) = session.messages.split_at_checked(root.message_count) else {
        return false;
    };
    after
        .iter()
        .any(|message| matches!(message.role, Role::Assistant | Role::Tool))
        && before
            .last()
            .is_some_and(|message| message.id == root.message_id)
        && ParkedUpdateRoot::hash_transcript(before)
            .is_ok_and(|hash| hash == root.transcript_sha256)
}

async fn observe_root(state: &crate::app_state::AppState, root: &ParkedUpdateRoot) -> RootReplay {
    let Ok(actor) = state.session_store.inspect_actor(&root.session_id).await else {
        return RootReplay::Pending;
    };
    let Some(owner) = actor
        .activation
        .filter(|_| actor.actor.session_created_at == root.created_at)
    else {
        return RootReplay::Pending;
    };
    let Ok(Some(session)) = state.session_store.load_session(&root.session_id).await else {
        return RootReplay::Pending;
    };
    if session.created_at != root.created_at
        || session.project_id_meta() != root.project_id
        || !session
            .conversation_reset_sha256()
            .is_ok_and(|hash| hash == root.conversation_reset_sha256)
    {
        return RootReplay::Pending;
    }
    if bamboo_storage::SessionStoreV2::root_completed_planned_update(&session, &root.fence) {
        return RootReplay::Advanced;
    }
    let original =
        owner.matches_fence(&root.fence) && owner.status == ActorActivationStatus::Succeeded;
    let same_origin = owner.lease_epoch > root.fence.lease_epoch
        && is_root_planned_update_attempt(&owner.lease_owner, &root.fence);
    if same_origin
        && owner.checkpoint_revision != 0
        && !session.is_parked_for_planned_update()
        && bamboo_storage::SessionStoreV2::root_runtime_matches_activation(&session, &owner.fence())
            .unwrap_or(false)
        && ((owner.status == ActorActivationStatus::Succeeded
            && session.last_run_status().as_deref() == Some("completed"))
            || continued_checkpoint(root, &session))
    {
        return RootReplay::Advanced;
    }

    let abandoned_attempt = same_origin
        && owner.checkpoint_revision == 0
        && (matches!(
            owner.status,
            ActorActivationStatus::Cancelled | ActorActivationStatus::Failed
        ) || (owner.status.is_live() && owner.lease_expires_at <= chrono::Utc::now()));
    if !original && !abandoned_attempt {
        return RootReplay::Pending;
    }
    let original_pause = session.is_parked_for_planned_update()
        && bamboo_storage::SessionStoreV2::root_runtime_matches_activation(&session, &root.fence)
            .unwrap_or(false);
    let unstarted_startup = abandoned_attempt
        && bamboo_storage::SessionStoreV2::root_has_unstarted_planned_update_projection(
            &session, &owner,
        )
        .unwrap_or(false);
    if (original_pause || unstarted_startup)
        && root.matches_checkpoint(&session)
        && !state
            .session_activation_router
            .owns_run(&root.session_id, &owner.run_id)
            .await
    {
        return RootReplay::Retry;
    }
    RootReplay::Pending
}

/// Called after the desktop has verified its replacement runtime. Only durable
/// progress by the authorized continuation completes a recorded member.
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
        match observe_root(state, root).await {
            RootReplay::Advanced => {
                report.advanced += 1;
                continue;
            }
            RootReplay::Retry => {
                if let Err(error) = state
                    .child_completion_coordinator
                    .resume_planned_update_root(root)
                    .await
                {
                    tracing::warn!(session_id = %root.session_id, %error, "Planned update session recovery remains pending");
                }
                if matches!(observe_root(state, root).await, RootReplay::Advanced) {
                    report.resumed += 1;
                    continue;
                }
            }
            RootReplay::Pending => (),
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
        let intent = publish(profile.path(), first, &[]).unwrap();
        assert_eq!(
            read(profile.path(), first).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        intent.authorize().unwrap();
        let repeated = publish(profile.path(), first, &[]).unwrap();
        repeated.authorize().unwrap();
        assert!(read(profile.path(), first).unwrap().roots.is_empty());
        assert!(read(profile.path(), second).is_err());
        let folder = directory(profile.path(), false).unwrap();
        fs::copy(record_path(&folder, first), record_path(&folder, second)).unwrap();
        assert_eq!(
            read(profile.path(), second).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let mut changed = File::options()
            .append(true)
            .open(record_path(&folder, first))
            .unwrap();
        changed.write_all(b"\n").unwrap();
        assert_eq!(
            read(profile.path(), first).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            intent.authorize().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
