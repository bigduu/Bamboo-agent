//! Bounded Host-only Assignment file port. No ambient Worker file executor.
use crate::{
    canonical_bytes, content_hash,
    service::{active_permit, validate_authority, validate_snapshot},
    *,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

pub const FILE_BYTES_LIMIT: usize = 16384;
// Includes the HostBridge's result envelope and JSON escaping. A raw file
// within FILE_BYTES_LIMIT can still exceed the existing SubAgent reply budget.
const FILE_REPLY_BYTES_LIMIT: usize = 16384;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "tool", deny_unknown_fields)]
pub enum FileOperation {
    Read {
        file_path: String,
    },
    Write {
        file_path: String,
        content: String,
        expected_sha256: Option<String>,
    },
}
impl FileOperation {
    pub fn tool(&self) -> &'static str {
        match self {
            Self::Read { .. } => "Read",
            Self::Write { .. } => "Write",
        }
    }
    fn path(&self) -> &str {
        match self {
            Self::Read { file_path } | Self::Write { file_path, .. } => file_path,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileReply {
    pub content: Option<String>,
    pub sha256: String,
    pub artifact: Option<Artifact>,
}

impl TicketService {
    /// Identity and tools come from a creation-fenced Host Run. Callers must
    /// preserve the native permission gate before this Rust-only port.
    pub fn workspace_file(
        &self,
        authority: &Authority,
        call_id: &str,
        op: &FileOperation,
    ) -> Result<FileReply> {
        let mut store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        let Principal::Worker { assignment_id, .. } = &authority.principal else {
            return Err(Error::ScopeDenied("Worker file capability required".into()));
        };
        let assignment = &snapshot.assignments[assignment_id];
        authority.worker(assignment)?;
        if call_id.is_empty() || call_id.len() > 128 || canonical_bytes(op)?.len() > 32768 {
            return Err(Error::InvalidTransition(
                "file callback budget exceeded".into(),
            ));
        }
        let operation_id = format!(
            "worker-file/{}",
            content_hash(format!("{}:{call_id}", authority.identity()).as_bytes())
        );
        let request_hash = content_hash(&canonical_bytes(op)?);
        if let Some(receipt) = snapshot.receipts.get(&operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("file receipt subject".into()));
            }
            if receipt.request_hash != request_hash {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(FileReply {
                content: None,
                sha256: receipt.ids["sha256"].clone(),
                artifact: Some(Artifact {
                    uri: receipt.ids["artifact"].clone(),
                    sha256: receipt.ids["sha256"].clone(),
                }),
            });
        }
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(format!("{:?}", store.health)));
        }
        active_permit(snapshot, assignment)?;
        if !assignment.allowed_tools.contains(op.tool()) {
            return Err(Error::ScopeDenied("file tool outside contract".into()));
        }
        if assignment.effects.contains_key(&operation_id) {
            return Err(Error::ResourceBlocked(
                "file effect outcome requires reconciliation; no automatic retry".into(),
            ));
        }
        let workspace = assignment.workspace.as_ref().ok_or_else(|| {
            Error::ScopeDenied("file capability has no isolated workspace".into())
        })?;
        let path = Path::new(op.path());
        if !path.is_absolute()
            || path
                .components()
                .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
            || path
                .components()
                .any(|c| matches!(c, Component::Normal(n) if n == ".git" || n == ".bamboo"))
        {
            return Err(Error::ScopeDenied(
                "file path must be absolute without traversal, .git or .bamboo control directories"
                    .into(),
            ));
        }
        let root = workspace
            .write_roots
            .iter()
            .map(Path::new)
            .find(|root| path.starts_with(root))
            .ok_or_else(|| Error::ScopeDenied("file outside Assignment write roots".into()))?;
        if store.root().starts_with(root) || root.starts_with(store.root()) {
            return Err(Error::ScopeDenied(
                "TicketStore cannot be a Worker file root".into(),
            ));
        }
        let (dir, name) = physical::parent(root, path)?;
        let prior = physical::read(&dir, &name)?;
        if let FileOperation::Read { .. } = op {
            let bytes = prior.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
            let sha256 = content_hash(&bytes);
            let content = String::from_utf8(bytes)
                .map_err(|_| Error::InvalidTransition("file is not UTF-8".into()))?;
            let reply = FileReply {
                content: Some(content),
                sha256,
                artifact: None,
            };
            if canonical_bytes(&serde_json::json!({"result": &reply}))?.len()
                > FILE_REPLY_BYTES_LIMIT
            {
                return Err(Error::ContextBudgetExceeded);
            }
            return Ok(reply);
        }
        let FileOperation::Write {
            content,
            expected_sha256,
            ..
        } = op
        else {
            unreachable!()
        };
        if content.len() > FILE_BYTES_LIMIT {
            return Err(Error::ContextBudgetExceeded);
        }
        if prior.as_ref().map(|bytes| content_hash(bytes)) != *expected_sha256 {
            return Err(Error::RevisionConflict);
        }
        let artifact = store.store_artifact(content.as_bytes())?;
        let mut started = snapshot.clone();
        started.seq += 1;
        started.schema = 3;
        let a = started
            .assignments
            .get_mut(assignment_id)
            .expect("verified Assignment");
        a.record_revision += 1;
        a.updated_seq = started.seq;
        a.effects.insert(
            operation_id.clone(),
            Effect {
                action_fingerprint: request_hash.clone(),
                state: EffectState::Started,
                provider_receipt: None,
                artifact: Some(artifact.clone()),
            },
        );
        // Commit intent and complete immutable content before any physical write.
        validate_snapshot(&started)?;
        store.publish(started)?;
        // IO can fail after rename. Retain Started and the resource claim until
        // an actual stopped Run and explicit reconciliation are confirmed.
        physical::replace(&dir, &name, content.as_bytes())?;
        let mut next = store.published.as_ref().expect("published start").1.clone();
        next.seq += 1;
        let a = next
            .assignments
            .get_mut(assignment_id)
            .expect("verified Assignment");
        a.record_revision += 1;
        a.updated_seq = next.seq;
        let effect = a.effects.get_mut(&operation_id).expect("durable intent");
        effect.state = EffectState::Succeeded;
        effect.provider_receipt = Some(format!("local-file:{}", artifact.sha256));
        let receipt = OperationReceipt {
            operation_id: operation_id.clone(),
            principal: authority.identity(),
            request_hash,
            canonical_request: String::from_utf8(canonical_bytes(op)?).expect("JSON UTF-8"),
            committed_seq: next.seq,
            ids: BTreeMap::from([
                ("sha256".into(), artifact.sha256.clone()),
                ("artifact".into(), artifact.uri.clone()),
            ]),
        };
        next.receipts.insert(operation_id.clone(), receipt);
        validate_snapshot(&next)?;
        #[cfg(feature = "test-utils")]
        store.publish_operation(next, &operation_id)?;
        #[cfg(not(feature = "test-utils"))]
        store.publish(next)?;
        Ok(FileReply {
            content: None,
            sha256: artifact.sha256.clone(),
            artifact: Some(artifact),
        })
    }
}

#[cfg(unix)]
mod physical {
    use super::*;
    use std::{
        ffi::CString,
        fs::File,
        io::{Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{ffi::OsStrExt, fs::MetadataExt},
        },
    };
    fn name(value: &std::ffi::OsStr) -> Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| Error::ScopeDenied("NUL in file path".into()))
    }
    fn open(parent: &File, name: &CString, flags: i32) -> Result<File> {
        // The parent owns a live directory FD; the returned FD is exclusively
        // owned by File. No symlink is followed at any path component.
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    pub fn parent(root: &Path, path: &Path) -> Result<(File, CString)> {
        let mut dir = File::open("/")?;
        for component in root.components() {
            match component {
                Component::RootDir => {}
                Component::Normal(part) => {
                    dir = open(&dir, &name(part)?, libc::O_RDONLY | libc::O_DIRECTORY)?;
                }
                _ => return Err(Error::ScopeDenied("invalid write root".into())),
            }
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| Error::ScopeDenied("outside root".into()))?;
        let mut parts = relative.components().peekable();
        while let Some(part) = parts.next() {
            let Component::Normal(part) = part else {
                return Err(Error::ScopeDenied("invalid file path".into()));
            };
            let part = name(part)?;
            if parts.peek().is_none() {
                return Ok((dir, part));
            }
            dir = open(&dir, &part, libc::O_RDONLY | libc::O_DIRECTORY)?;
        }
        Err(Error::ScopeDenied("file path is a write root".into()))
    }
    pub fn read(dir: &File, name: &CString) -> Result<Option<Vec<u8>>> {
        let file = match open(dir, name, libc::O_RDONLY | libc::O_NONBLOCK) {
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            other => other?,
        };
        let meta = file.metadata()?;
        if !meta.is_file() || meta.nlink() != 1 {
            return Err(Error::ScopeDenied(
                "only regular unlinked files are supported".into(),
            ));
        }
        if meta.len() > FILE_BYTES_LIMIT as u64 {
            return Err(Error::ContextBudgetExceeded);
        }
        let mut bytes = Vec::new();
        file.take((FILE_BYTES_LIMIT + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > FILE_BYTES_LIMIT {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(Some(bytes))
    }
    pub fn replace(dir: &File, name: &CString, bytes: &[u8]) -> Result<()> {
        let staging = CString::new(format!(".ticket-file-{}", uuid::Uuid::new_v4())).expect("UUID");
        let result = (|| {
            let mut file = open(dir, &staging, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            if unsafe {
                libc::renameat(
                    dir.as_raw_fd(),
                    staging.as_ptr(),
                    dir.as_raw_fd(),
                    name.as_ptr(),
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            dir.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            unsafe {
                libc::unlinkat(dir.as_raw_fd(), staging.as_ptr(), 0);
            }
        }
        result
    }
}

#[cfg(not(unix))]
mod physical {
    use super::*;
    pub fn parent(_: &Path, _: &Path) -> Result<((), ())> {
        Err(Error::AuthorityUnavailable(
            "Ticket file tools require the tested Unix directory capability".into(),
        ))
    }
    pub fn read(_: &(), _: &()) -> Result<Option<Vec<u8>>> {
        unreachable!()
    }
    pub fn replace(_: &(), _: &(), _: &[u8]) -> Result<()> {
        unreachable!()
    }
}
