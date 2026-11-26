//! Test-only Windows replacement of a literal Ticket HEAD on one retained scope.
use std::{
    ffi::OsStr,
    fs::File,
    io::{self, ErrorKind, Seek, SeekFrom},
};

use windows_sys::{
    Wdk::Storage::FileSystem::FILE_OPEN,
    Win32::{
        Foundation::STATUS_SUCCESS,
        Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE},
    },
};

use super::immutable::{
    observe_named, observe_regular, read_bounded, rename_opened, require_name_absent, stage_synced,
    ObjectObservation, RenameForm, RenameReceipt, StagedObject, PUBLICATION_SHARING,
};
use super::{
    entry_identity, handle_identity, invalid_entry, open_relative, open_relative_shared,
    FileIdentity, WriterGuard, DIRECTORY_ACCESS, SHARE, WRITER_NAME,
};

const HEAD: &str = "HEAD";
const HEAD_LENGTH: usize = 64;

pub(super) struct HeadScope<'writer, 'scope> {
    writer: &'writer WriterGuard<'scope>,
    directory: File,
    identity: FileIdentity,
}

pub(super) struct HeadReader {
    file: File,
    scope: FileIdentity,
    identity: FileIdentity,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct HeldHeadObservation {
    pub(super) identity: FileIdentity,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct HeadReplaceReceipt {
    pub(super) rename: RenameReceipt,
    pub(super) installation: Option<io::Result<InstalledHead>>,
}

#[derive(Debug)]
pub(super) struct InstalledHead {
    pub(super) old: HeldHeadObservation,
    pub(super) named: ObjectObservation,
    pub(super) stage_absence_win32_error: i32,
    pub(super) scope_flush_win32_error: i32,
}

fn require_head_bytes(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() != HEAD_LENGTH
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "scratch HEAD must contain exactly 64 lowercase hexadecimal bytes",
        ));
    }
    Ok(())
}

impl<'scope> WriterGuard<'scope> {
    pub(super) fn open_head_scope(&self) -> io::Result<HeadScope<'_, 'scope>> {
        self.scope.check_named_scope()?;
        let name = self
            .scope
            .path
            .file_name()
            .ok_or_else(|| invalid_entry("missing HEAD scope name"))?;
        // WindowsScope admits at least one component below the drive root.
        let parent = &self.scope.ancestry[self.scope.ancestry.len() - 2].0;
        let directory = open_relative(
            parent,
            name,
            true,
            FILE_GENERIC_WRITE | DIRECTORY_ACCESS,
            FILE_OPEN,
        )?;
        let identity = entry_identity(&directory, true)?;
        if identity != self.scope.identity() {
            return Err(invalid_entry(
                "writable HEAD scope does not match its retained identity",
            ));
        }
        let head = HeadScope {
            writer: self,
            directory,
            identity,
        };
        head.check_binding()?;
        Ok(head)
    }
}

impl HeadReader {
    pub(super) fn observe(&self, limit: usize) -> io::Result<HeldHeadObservation> {
        // A POSIX replacement can leave the old reader without a directory
        // name. Use its retained handle ID, skipping the named-open link gate.
        let identity = handle_identity(&self.file)?;
        if identity != self.identity {
            return Err(invalid_entry("held HEAD reader changed its file identity"));
        }
        (&self.file).seek(SeekFrom::Start(0))?;
        Ok(HeldHeadObservation {
            identity,
            bytes: read_bounded(&self.file, limit)?,
        })
    }
}

impl HeadScope<'_, '_> {
    pub(super) fn identity(&self) -> FileIdentity {
        self.identity
    }

    fn check_binding(&self) -> io::Result<()> {
        let lock = self.writer.scope.inspect_file(WRITER_NAME)?;
        if lock.identity != self.writer.identity() || lock.link_count != 1 {
            return Err(invalid_entry("HEAD publisher lost its named writer lock"));
        }
        if entry_identity(&self.directory, true)? != self.writer.scope.identity() {
            return Err(invalid_entry("writable HEAD scope changed identity"));
        }
        Ok(())
    }

    pub(super) fn seed_head(&self, bytes: &[u8]) -> io::Result<FileIdentity> {
        require_head_bytes(bytes)?;
        self.check_binding()?;
        let seed = stage_synced(
            self.writer.scope,
            &self.directory,
            self.identity,
            HEAD,
            bytes,
            false,
        )?;
        let named = self.observe(HEAD, bytes.len())?;
        if named.identity != seed.identity() || named.bytes != bytes {
            return Err(invalid_entry(
                "initial HEAD did not retain its created identity and bytes",
            ));
        }
        self.directory.sync_all()?;
        Ok(seed.identity())
    }

    pub(super) fn stage(
        &self,
        name: &str,
        bytes: &[u8],
        delete_access: bool,
    ) -> io::Result<StagedObject> {
        require_head_bytes(bytes)?;
        if name.eq_ignore_ascii_case(HEAD) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "HEAD staging name must differ from HEAD",
            ));
        }
        self.check_binding()?;
        stage_synced(
            self.writer.scope,
            &self.directory,
            self.identity,
            name,
            bytes,
            delete_access,
        )
    }

    pub(super) fn open_reader(&self, share_delete: bool) -> io::Result<HeadReader> {
        self.check_binding()?;
        let sharing = if share_delete {
            PUBLICATION_SHARING
        } else {
            SHARE
        };
        let file = open_relative_shared(
            &self.directory,
            OsStr::new(HEAD),
            false,
            FILE_GENERIC_READ,
            FILE_OPEN,
            sharing,
        )?;
        let identity = observe_regular(self.writer.scope, &file)?.identity;
        Ok(HeadReader {
            file,
            scope: self.identity,
            identity,
        })
    }

    pub(super) fn observe(&self, name: &str, limit: usize) -> io::Result<ObjectObservation> {
        self.check_binding()?;
        observe_named(self.writer.scope, &self.directory, name, limit)
    }

    pub(super) fn replace(
        &self,
        stage: &StagedObject,
        reader: &HeadReader,
    ) -> io::Result<HeadReplaceReceipt> {
        self.check_binding()?;
        require_head_bytes(&stage.bytes)?;
        if reader.scope != self.identity
            || stage.category != self.identity
            || stage.name.eq_ignore_ascii_case(HEAD)
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "both HEAD handles must name the retained scope",
            ));
        }
        let old = reader.observe(HEAD_LENGTH)?;
        require_head_bytes(&old.bytes)?;
        let named = self.observe(HEAD, HEAD_LENGTH)?;
        let source = self.observe(&stage.name, HEAD_LENGTH)?;
        if named.identity != old.identity
            || named.bytes != old.bytes
            || source.identity != stage.identity()
            || source.bytes != stage.bytes
            || source.identity == old.identity
            || source.bytes == old.bytes
        {
            return Err(invalid_entry(
                "HEAD replacement requires the current old file and a distinct verified stage",
            ));
        }
        // Missing DELETE on the source, or missing delete sharing on the
        // reader, must be decided by this same native operation.
        let rename = rename_opened(&stage.file, &self.directory, HEAD, RenameForm::PosixReplace);
        let installation = if rename.ntstatus == STATUS_SUCCESS {
            Some(self.verify_replacement(stage, reader, &old))
        } else {
            None
        };
        Ok(HeadReplaceReceipt {
            rename,
            installation,
        })
    }

    fn verify_replacement(
        &self,
        stage: &StagedObject,
        reader: &HeadReader,
        old_before: &HeldHeadObservation,
    ) -> io::Result<InstalledHead> {
        self.check_binding()?;
        self.directory.sync_all()?;
        let old = reader.observe(HEAD_LENGTH)?;
        let named = self.observe(HEAD, HEAD_LENGTH)?;
        let stage_absence_win32_error = require_name_absent(&self.directory, &stage.name)?;
        if old != *old_before || named.identity != stage.identity() || named.bytes != stage.bytes {
            return Err(invalid_entry(
                "replacement did not retain old-reader and new-name identities and bytes",
            ));
        }
        Ok(InstalledHead {
            old,
            named,
            stage_absence_win32_error,
            scope_flush_win32_error: 0,
        })
    }
}
