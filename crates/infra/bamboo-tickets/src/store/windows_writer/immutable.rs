//! One retained category for Windows-only Ticket immutable publication tests.
use std::{
    ffi::OsStr,
    fs::File,
    io::{self, ErrorKind, Read, Write},
    mem::{align_of, offset_of, size_of},
    os::windows::io::AsRawHandle,
    ptr,
};

use windows_sys::{
    Wdk::Storage::FileSystem::{
        FileRenameInformation, FileRenameInformationEx, NtSetInformationFile, FILE_CREATE,
        FILE_OPEN, FILE_RENAME_INFORMATION, FILE_RENAME_POSIX_SEMANTICS,
        FILE_RENAME_REPLACE_IF_EXISTS,
    },
    Win32::{
        Foundation::{RtlNtStatusToDosError, ERROR_FILE_NOT_FOUND, STATUS_SUCCESS},
        Storage::FileSystem::{
            DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
            FILE_SHARE_READ, SYNCHRONIZE,
        },
        System::IO::IO_STATUS_BLOCK,
    },
};

use super::{
    entry_identity, invalid_entry, open_relative, open_relative_shared, FileIdentity,
    FileObservation, WindowsScope, WriterGuard, DIRECTORY_ACCESS, SHARE, WRITER_NAME,
};

pub(super) const PUBLICATION_SHARING: u32 = SHARE | FILE_SHARE_DELETE;
pub(super) const HEAD_RENAME_INFORMATION_CLASS: i32 = FileRenameInformationEx;
pub(super) const HEAD_RENAME_FLAGS: u32 =
    FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS;

pub(super) struct ImmutableCategory<'writer, 'scope> {
    writer: &'writer WriterGuard<'scope>,
    directory: File,
    name: String,
    identity: FileIdentity,
}

pub(super) struct StagedObject {
    pub(super) file: File,
    pub(super) category: FileIdentity,
    pub(super) name: String,
    pub(super) identity: FileIdentity,
    pub(super) bytes: Vec<u8>,
}

impl StagedObject {
    pub(super) fn identity(&self) -> FileIdentity {
        self.identity
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ObjectObservation {
    pub(super) identity: FileIdentity,
    pub(super) link_count: u32,
    pub(super) bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RenameReceipt {
    pub(super) ntstatus: i32,
    pub(super) win32_error: i32,
    pub(super) information_class: i32,
    pub(super) flags: u32,
}

pub(super) enum RenameForm {
    CreateOnly,
    PosixReplace,
}

#[derive(Debug)]
pub(super) struct PublishReceipt {
    pub(super) rename: RenameReceipt,
    pub(super) installation: Option<io::Result<InstalledObject>>,
}

#[derive(Debug)]
pub(super) struct InstalledObject {
    pub(super) named: ObjectObservation,
    pub(super) stage_absence_win32_error: i32,
    pub(super) category_flush_win32_error: i32,
}

#[derive(Debug)]
pub(super) struct ReuseReceipt {
    pub(super) observed: ObjectObservation,
    pub(super) matched: bool,
    pub(super) category_flush_win32_error: Option<i32>,
}

impl<'scope> WriterGuard<'scope> {
    pub(super) fn open_category(&self, name: &str) -> io::Result<ImmutableCategory<'_, 'scope>> {
        self.scope.check_named_scope()?;
        let directory = open_relative(
            self.scope.directory(),
            OsStr::new(name),
            true,
            FILE_GENERIC_WRITE | DIRECTORY_ACCESS,
            FILE_OPEN,
        )?;
        let identity = entry_identity(&directory, true)?;
        if identity.volume_serial != self.scope.identity().volume_serial {
            return Err(invalid_entry("immutable category is on another volume"));
        }
        let category = ImmutableCategory {
            writer: self,
            directory,
            name: name.to_owned(),
            identity,
        };
        category.check_binding()?;
        Ok(category)
    }
}

impl ImmutableCategory<'_, '_> {
    pub(super) fn identity(&self) -> FileIdentity {
        self.identity
    }

    // The retained handles exclude delete sharing. Reopen the names to also attest
    // that the operation still applies to the current scope, lock and category.
    fn check_binding(&self) -> io::Result<()> {
        let locked = self.writer.scope.inspect_file(WRITER_NAME)?;
        if locked.identity != self.writer.identity() || locked.link_count != 1 {
            return Err(invalid_entry(
                "immutable publisher lost its named writer lock",
            ));
        }
        let named = open_relative(
            self.writer.scope.directory(),
            OsStr::new(&self.name),
            true,
            DIRECTORY_ACCESS,
            FILE_OPEN,
        )?;
        if entry_identity(&named, true)? != self.identity {
            return Err(invalid_entry("immutable category name changed identity"));
        }
        Ok(())
    }

    pub(super) fn stage(&self, name: &str, bytes: &[u8]) -> io::Result<StagedObject> {
        if name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "immutable staging name must not be a digest",
            ));
        }
        self.create_synced(name, bytes)
    }

    pub(super) fn seed_conflicting_fixture(
        &self,
        digest: &str,
        bytes: &[u8],
    ) -> io::Result<FileIdentity> {
        require_digest(digest)?;
        if crate::content_hash(bytes) == digest {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "conflict fixture bytes must differ from the named digest",
            ));
        }
        Ok(self.create_synced(digest, bytes)?.identity())
    }

    fn create_synced(&self, name: &str, bytes: &[u8]) -> io::Result<StagedObject> {
        self.check_binding()?;
        stage_synced(
            self.writer.scope,
            &self.directory,
            self.identity,
            name,
            bytes,
            true,
        )
    }

    pub(super) fn observe(&self, name: &str, limit: usize) -> io::Result<ObjectObservation> {
        self.check_binding()?;
        observe_named(self.writer.scope, &self.directory, name, limit)
    }

    pub(super) fn publish_no_replace(
        &self,
        stage: &StagedObject,
        digest: &str,
    ) -> io::Result<PublishReceipt> {
        self.check_binding()?;
        require_digest(digest)?;
        if stage.category != self.identity || stage.name.eq_ignore_ascii_case(digest) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "immutable stage must be a distinct name in the retained category",
            ));
        }
        if crate::content_hash(&stage.bytes) != digest {
            return Err(invalid_entry(
                "immutable digest does not match staged bytes",
            ));
        }
        let source = self.observe(&stage.name, stage.bytes.len())?;
        if source.identity != stage.identity || source.bytes != stage.bytes {
            return Err(invalid_entry(
                "immutable staging name changed before publication",
            ));
        }
        let rename = rename_opened(&stage.file, &self.directory, digest, RenameForm::CreateOnly);
        let installation = if rename.ntstatus == STATUS_SUCCESS {
            Some(self.verify_installation(stage, digest))
        } else {
            None
        };
        Ok(PublishReceipt {
            rename,
            installation,
        })
    }

    fn verify_installation(
        &self,
        stage: &StagedObject,
        digest: &str,
    ) -> io::Result<InstalledObject> {
        let named = self.observe(digest, stage.bytes.len())?;
        if named.identity != stage.identity || named.bytes != stage.bytes {
            return Err(invalid_entry(
                "installed object did not retain staging identity and bytes",
            ));
        }
        let stage_absence_win32_error = require_name_absent(&self.directory, &stage.name)?;
        self.check_binding()?;
        self.directory.sync_all()?;
        Ok(InstalledObject {
            named,
            stage_absence_win32_error,
            category_flush_win32_error: 0,
        })
    }

    pub(super) fn reuse(&self, digest: &str, expected: &[u8]) -> io::Result<ReuseReceipt> {
        require_digest(digest)?;
        if crate::content_hash(expected) != digest {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "immutable digest does not match expected bytes",
            ));
        }
        let observed = self.observe(digest, expected.len())?;
        let matched = observed.bytes == expected;
        let category_flush_win32_error = if matched {
            self.check_binding()?;
            self.directory.sync_all()?;
            Some(0)
        } else {
            None
        };
        Ok(ReuseReceipt {
            observed,
            matched,
            category_flush_win32_error,
        })
    }
}

pub(super) fn observe_regular(scope: &WindowsScope, file: &File) -> io::Result<FileObservation> {
    let observation = scope.observe_file(file)?;
    if observation.link_count != 1 {
        return Err(invalid_entry("immutable object requires exactly one link"));
    }
    Ok(observation)
}

pub(super) fn stage_synced(
    scope: &WindowsScope,
    directory: &File,
    identity: FileIdentity,
    name: &str,
    bytes: &[u8],
    delete_access: bool,
) -> io::Result<StagedObject> {
    let access = FILE_GENERIC_READ | FILE_GENERIC_WRITE | if delete_access { DELETE } else { 0 };
    let mut file = open_relative_shared(
        directory,
        OsStr::new(name),
        false,
        access,
        FILE_CREATE,
        FILE_SHARE_READ,
    )?;
    let observed = observe_regular(scope, &file)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(StagedObject {
        file,
        category: identity,
        name: name.to_owned(),
        identity: observed.identity,
        bytes: bytes.to_vec(),
    })
}

pub(super) fn read_bounded(file: &File, limit: usize) -> io::Result<Vec<u8>> {
    let limit = limit
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidInput, "immutable read limit is too large")
        })?;
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn observe_named(
    scope: &WindowsScope,
    directory: &File,
    name: &str,
    limit: usize,
) -> io::Result<ObjectObservation> {
    let file = open_relative_shared(
        directory,
        OsStr::new(name),
        false,
        FILE_GENERIC_READ,
        FILE_OPEN,
        PUBLICATION_SHARING,
    )?;
    let observed = observe_regular(scope, &file)?;
    Ok(ObjectObservation {
        identity: observed.identity,
        link_count: observed.link_count,
        bytes: read_bounded(&file, limit)?,
    })
}

pub(super) fn require_name_absent(directory: &File, name: &str) -> io::Result<i32> {
    match open_relative_shared(
        directory,
        OsStr::new(name),
        false,
        FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN,
        PUBLICATION_SHARING,
    ) {
        Ok(_) => Err(invalid_entry("published staging name still exists")),
        Err(error) if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) => {
            Ok(ERROR_FILE_NOT_FOUND as i32)
        }
        Err(error) => Err(error),
    }
}

fn require_digest(digest: &str) -> io::Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "immutable name must be a lowercase SHA-256 digest",
        ));
    }
    Ok(())
}

pub(super) fn rename_opened(
    source: &File,
    parent: &File,
    name: &str,
    form: RenameForm,
) -> RenameReceipt {
    let units: Vec<u16> = name.encode_utf16().collect();
    let name_bytes = units.len() * size_of::<u16>();
    let request_size = size_of::<FILE_RENAME_INFORMATION>() + name_bytes;
    // A machine word matches the native request's alignment. Initialize the
    // entire variable-length header, including padding and the UTF-16 tail.
    const _: () = assert!(align_of::<usize>() >= align_of::<FILE_RENAME_INFORMATION>());
    let mut buffer = vec![0_usize; request_size.div_ceil(size_of::<usize>())];
    let request = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: the aligned owned buffer holds the native header and literal name.
    // Both handles remain live, and the native operation checks their access.
    let (ntstatus, information_class, flags) = unsafe {
        let (class, flags) = match form {
            // The zeroed legacy flags prohibit replacing an existing name.
            RenameForm::CreateOnly => (FileRenameInformation, 0),
            RenameForm::PosixReplace => {
                (*request).Anonymous.Flags = HEAD_RENAME_FLAGS;
                (HEAD_RENAME_INFORMATION_CLASS, HEAD_RENAME_FLAGS)
            }
        };
        (*request).RootDirectory = parent.as_raw_handle();
        (*request).FileNameLength = name_bytes as u32;
        let name = buffer
            .as_mut_ptr()
            .cast::<u8>()
            .add(offset_of!(FILE_RENAME_INFORMATION, FileName))
            .cast::<u16>();
        ptr::copy_nonoverlapping(units.as_ptr(), name, units.len());
        let status = NtSetInformationFile(
            source.as_raw_handle(),
            &mut io_status,
            buffer.as_ptr().cast(),
            request_size as u32,
            class,
        );
        (status, class, flags)
    };
    // SAFETY: the status came from the native rename call above.
    let win32_error = unsafe { RtlNtStatusToDosError(ntstatus) } as i32;
    RenameReceipt {
        ntstatus,
        win32_error,
        information_class,
        flags,
    }
}
