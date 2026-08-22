//! Shared, metadata-only boundary for fresh builtin Workflow clone publication.
//!
//! The server owns the write protocol. Discovery only consumes this sealed
//! receipt to decide whether a target directory may be catalog-visible. Crash
//! recovery, retirement and delete/reclone epochs deliberately live in the
//! follow-up recovery slice rather than this fresh-publication contract.

use serde::{Deserialize, Serialize};

pub const CLONE_MARKER_SCHEMA: u8 = 1;
pub const MAX_CLONE_MARKER_BYTES: usize = 64 * 1024;
pub const MAX_CLONE_BUNDLE_FILES: usize = 1024;
pub const MAX_CLONE_BUNDLE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_CLONE_RELATIVE_PATH_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CloneNodeIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClonePublicationPhase {
    Prepared,
    Staged,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClonePublicationMarker {
    pub schema: u8,
    pub workflow_id: String,
    pub source_revision: u64,
    pub source_content_digest: String,
    pub bundle_digest: String,
    pub staging_name: String,
    pub phase: ClonePublicationPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_identity: Option<CloneNodeIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_identity: Option<CloneNodeIdentity>,
}

impl ClonePublicationMarker {
    pub fn validate_for(&self, workflow_id: &str) -> bool {
        self.schema == CLONE_MARKER_SCHEMA
            && self.workflow_id == workflow_id
            && self.source_revision > 0
            && valid_sha256(&self.source_content_digest)
            && valid_sha256(&self.bundle_digest)
            && valid_staging_name(&self.staging_name)
            && match self.phase {
                ClonePublicationPhase::Prepared => {
                    self.stage_identity.is_none() && self.target_identity.is_none()
                }
                ClonePublicationPhase::Staged => {
                    self.stage_identity.is_some() && self.target_identity.is_none()
                }
                ClonePublicationPhase::Complete => {
                    self.stage_identity.is_some()
                        && self.target_identity.is_some()
                        && self.stage_identity == self.target_identity
                }
            }
    }

    pub fn complete_target_identity(&self, workflow_id: &str) -> Option<CloneNodeIdentity> {
        (self.validate_for(workflow_id) && self.phase == ClonePublicationPhase::Complete)
            .then_some(self.target_identity)
            .flatten()
    }
}

pub fn clone_marker_name(workflow_id: &str) -> String {
    format!(".{workflow_id}.clone-v1.json")
}

pub fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_staging_name(value: &str) -> bool {
    let Some(suffix) = value.strip_prefix("txn-") else {
        return false;
    };
    suffix.len() == 36
        && suffix.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

#[cfg(unix)]
pub fn std_file_identity(file: &std::fs::File) -> Option<CloneNodeIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().ok()?;
    Some(CloneNodeIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(windows)]
pub fn std_file_identity(file: &std::fs::File) -> Option<CloneNodeIdentity> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    let mut information = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    let succeeded =
        unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, information.as_mut_ptr()) };
    if succeeded == 0 {
        return None;
    }
    let information = unsafe { information.assume_init() };
    Some(CloneNodeIdentity {
        device: u64::from(information.dwVolumeSerialNumber),
        inode: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    })
}

#[cfg(not(any(unix, windows)))]
pub fn std_file_identity(_file: &std::fs::File) -> Option<CloneNodeIdentity> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(phase: ClonePublicationPhase) -> ClonePublicationMarker {
        let identity = CloneNodeIdentity {
            device: 7,
            inode: 11,
        };
        ClonePublicationMarker {
            schema: CLONE_MARKER_SCHEMA,
            workflow_id: "review".to_string(),
            source_revision: 4,
            source_content_digest: "a".repeat(64),
            bundle_digest: "b".repeat(64),
            staging_name: "txn-12345678-1234-1234-1234-123456789abc".to_string(),
            phase,
            stage_identity: (phase != ClonePublicationPhase::Prepared).then_some(identity),
            target_identity: (phase == ClonePublicationPhase::Complete).then_some(identity),
        }
    }

    #[test]
    fn marker_phase_contract_is_strict_and_generation_bound() {
        for phase in [
            ClonePublicationPhase::Prepared,
            ClonePublicationPhase::Staged,
            ClonePublicationPhase::Complete,
        ] {
            assert!(marker(phase).validate_for("review"));
        }

        let mut malformed = marker(ClonePublicationPhase::Complete);
        malformed.target_identity = None;
        assert!(!malformed.validate_for("review"));
        assert!(!marker(ClonePublicationPhase::Complete).validate_for("other"));
    }
}
