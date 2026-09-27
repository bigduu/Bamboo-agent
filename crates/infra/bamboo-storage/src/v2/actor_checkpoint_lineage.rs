//! Private current-ancestor observation shared by the three checkpoint ports.
//! Bytes/existence do not grant authority; the existing pure lineage validator
//! supplies identity/Project checks. Existing full-read and cooperative-writer
//! boundaries remain unchanged.
use super::actor_transcript::ActorTranscriptAppendError;
use super::*;
use bamboo_domain::{ActorAncestorObservation, ActorDirectoryError, ActorSession};

type Result<T> = std::result::Result<T, ActorTranscriptAppendError>;

pub(super) struct Witness(Vec<Option<Vec<u8>>>);
impl Witness {
    pub(super) fn matches_current(&self, home: &Path, actor: &ActorSession) -> Result<bool> {
        Ok(self.0 == capture(home, actor)?.0)
    }
}

pub(super) fn capture(home: &Path, actor: &ActorSession) -> Result<Witness> {
    let root = home.join(&actor.root_actor_id);
    let mut bytes = Vec::new();
    for ancestor in &actor.ancestor_observations {
        validate_session_id(&ancestor.actor_id)?;
        let directory = if ancestor.actor_id == actor.root_actor_id {
            root.clone()
        } else {
            root.join("children").join(&ancestor.actor_id)
        };
        for directory in directory.ancestors().take_while(|p| p.starts_with(home)) {
            if !std::fs::symlink_metadata(directory)?.file_type().is_dir() {
                return Err(ActorTranscriptAppendError::InvalidSource);
            }
        }
        for name in [
            "session.json",
            RUNTIME_SIDECAR_FILE,
            root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
            supervisor_proof::SUPERVISOR_PROOF_FILE,
        ] {
            let path = directory.join(name);
            bytes.push(match std::fs::symlink_metadata(&path) {
                Ok(m) if m.file_type().is_file() => Some(std::fs::read(path)?),
                Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                _ => return Err(ActorTranscriptAppendError::InvalidSource),
            });
        }
    }
    Ok(Witness(bytes))
}

pub(super) fn validate_recorded_observations(
    recorded: &[ActorAncestorObservation],
    current: &[ActorAncestorObservation],
) -> std::result::Result<(), ActorDirectoryError> {
    if recorded.len() != current.len() {
        return Err(ActorDirectoryError::InvalidIdentity);
    }
    for (old, new) in recorded.iter().zip(current) {
        if old.actor_id != new.actor_id
            || old.session_created_at != new.session_created_at
            || new.metadata_version < old.metadata_version
        {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        if new.metadata_version - old.metadata_version >= 2 {
            return Err(ActorDirectoryError::ProjectTransitionBlocked);
        }
    }
    Ok(())
}
