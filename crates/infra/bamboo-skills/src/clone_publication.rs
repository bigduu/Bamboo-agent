//! Shared, metadata-only boundary for builtin Workflow clone publication.
//!
//! The server owns the write protocol. Discovery only consumes this sealed
//! journal to decide whether a target directory may be catalog-visible. Each
//! bounded epoch is recoverable until completion, abort, or retirement.

use serde::{Deserialize, Serialize};

pub const CLONE_MARKER_SCHEMA: u8 = 1;
pub const MAX_CLONE_MARKER_BYTES: usize = 64 * 1024;
pub const MAX_CLONE_MARKER_RECORDS: usize = 8;
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
    StageBound,
    Staged,
    Complete,
    Aborted,
    Retired,
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
                ClonePublicationPhase::StageBound => {
                    self.stage_identity.is_some() && self.target_identity.is_none()
                }
                ClonePublicationPhase::Staged => {
                    self.stage_identity.is_some() && self.target_identity.is_none()
                }
                ClonePublicationPhase::Complete => {
                    self.stage_identity.is_some()
                        && self.target_identity.is_some()
                        && self.stage_identity == self.target_identity
                }
                ClonePublicationPhase::Aborted => self.target_identity.is_none(),
                ClonePublicationPhase::Retired => {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloneMarkerJournal {
    pub records: Vec<ClonePublicationMarker>,
    pub partial: Vec<u8>,
}

impl CloneMarkerJournal {
    pub fn current(&self) -> Option<&ClonePublicationMarker> {
        self.records.last()
    }
}

fn same_epoch(left: &ClonePublicationMarker, right: &ClonePublicationMarker) -> bool {
    left.schema == right.schema
        && left.workflow_id == right.workflow_id
        && left.source_revision == right.source_revision
        && left.source_content_digest == right.source_content_digest
        && left.bundle_digest == right.bundle_digest
        && left.staging_name == right.staging_name
}

fn valid_transition(previous: &ClonePublicationMarker, next: &ClonePublicationMarker) -> bool {
    if previous == next {
        return true;
    }
    if !same_epoch(previous, next) {
        return false;
    }
    match (previous.phase, next.phase) {
        (ClonePublicationPhase::Prepared, ClonePublicationPhase::StageBound) => {
            next.stage_identity.is_some() && next.target_identity.is_none()
        }
        (ClonePublicationPhase::StageBound, ClonePublicationPhase::Staged) => {
            previous.stage_identity == next.stage_identity && next.target_identity.is_none()
        }
        (ClonePublicationPhase::Staged, ClonePublicationPhase::Complete) => {
            previous.stage_identity == next.stage_identity
                && next.target_identity == next.stage_identity
        }
        (
            ClonePublicationPhase::Prepared
            | ClonePublicationPhase::StageBound
            | ClonePublicationPhase::Staged,
            ClonePublicationPhase::Aborted,
        ) => previous.stage_identity == next.stage_identity && next.target_identity.is_none(),
        (ClonePublicationPhase::Complete, ClonePublicationPhase::Retired) => {
            previous.stage_identity == next.stage_identity
                && previous.target_identity == next.target_identity
        }
        (ClonePublicationPhase::Aborted, ClonePublicationPhase::Prepared) => {
            next.stage_identity.is_none() && next.target_identity.is_none()
        }
        (
            ClonePublicationPhase::Aborted,
            ClonePublicationPhase::StageBound | ClonePublicationPhase::Staged,
        ) => previous.stage_identity == next.stage_identity && next.target_identity.is_none(),
        _ => false,
    }
}

/// Parse one bounded publication epoch. Records are newline-delimited so a
/// crash can leave only a recoverable final prefix; the last complete record
/// remains authoritative until that suffix is completed exactly.
pub fn parse_clone_marker_journal(bytes: &[u8], workflow_id: &str) -> Option<CloneMarkerJournal> {
    if bytes.len() > MAX_CLONE_MARKER_BYTES {
        return None;
    }

    if !bytes.contains(&b'\n') {
        if let Ok(record) = serde_json::from_slice::<ClonePublicationMarker>(bytes) {
            return (record.validate_for(workflow_id)).then_some(CloneMarkerJournal {
                records: vec![record],
                partial: Vec::new(),
            });
        }
        return Some(CloneMarkerJournal {
            records: Vec::new(),
            partial: bytes.to_vec(),
        });
    }

    let complete_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let mut records = Vec::new();
    for line in bytes[..complete_len].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let record = serde_json::from_slice::<ClonePublicationMarker>(line).ok()?;
        if !record.validate_for(workflow_id)
            || records
                .last()
                .is_some_and(|previous| !valid_transition(previous, &record))
        {
            return None;
        }
        records.push(record);
    }
    if records.len() > MAX_CLONE_MARKER_RECORDS {
        return None;
    }
    Some(CloneMarkerJournal {
        records,
        partial: bytes[complete_len..].to_vec(),
    })
}

pub fn clone_marker_record_bytes(marker: &ClonePublicationMarker) -> Option<Vec<u8>> {
    let mut bytes = serde_json::to_vec(marker).ok()?;
    bytes.push(b'\n');
    (bytes.len() <= MAX_CLONE_MARKER_BYTES).then_some(bytes)
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
            target_identity: matches!(
                phase,
                ClonePublicationPhase::Complete | ClonePublicationPhase::Retired
            )
            .then_some(identity),
        }
    }

    #[test]
    fn marker_phase_contract_is_strict_and_generation_bound() {
        for phase in [
            ClonePublicationPhase::Prepared,
            ClonePublicationPhase::StageBound,
            ClonePublicationPhase::Staged,
            ClonePublicationPhase::Complete,
            ClonePublicationPhase::Aborted,
            ClonePublicationPhase::Retired,
        ] {
            assert!(marker(phase).validate_for("review"));
        }

        let mut malformed = marker(ClonePublicationPhase::Complete);
        malformed.target_identity = None;
        assert!(!malformed.validate_for("review"));
        assert!(!marker(ClonePublicationPhase::Complete).validate_for("other"));
    }

    #[test]
    fn journal_keeps_the_last_complete_transition_and_a_partial_suffix() {
        let prepared = marker(ClonePublicationPhase::Prepared);
        let stage_bound = marker(ClonePublicationPhase::StageBound);
        let mut bytes = clone_marker_record_bytes(&prepared).unwrap();
        bytes.extend(clone_marker_record_bytes(&stage_bound).unwrap());
        let complete = clone_marker_record_bytes(&marker(ClonePublicationPhase::Staged)).unwrap();
        bytes.extend(&complete[..complete.len() / 2]);

        let journal = parse_clone_marker_journal(&bytes, "review").expect("journal");
        assert_eq!(journal.records, vec![prepared, stage_bound]);
        assert!(!journal.partial.is_empty());
    }
}
