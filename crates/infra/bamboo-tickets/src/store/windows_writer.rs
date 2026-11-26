//! Private, Windows-only qualification of the Ticket scope and writer lock.
use std::{
    ffi::OsStr,
    fs::File,
    io::{self, ErrorKind},
    mem::size_of,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Component, Path, PathBuf, Prefix},
    ptr,
};

use fs2::FileExt;
use windows_sys::{
    Wdk::{
        Foundation::OBJECT_ATTRIBUTES,
        Storage::FileSystem::{
            NtCreateFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_IF,
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, NTCREATEFILE_CREATE_DISPOSITION,
        },
    },
    Win32::{
        Foundation::{
            RtlNtStatusToDosError, HANDLE, INVALID_HANDLE_VALUE, OBJ_CASE_INSENSITIVE,
            OBJ_DONT_REPARSE, UNICODE_STRING,
        },
        Storage::FileSystem::{
            CreateFileW, FileAttributeTagInfo, FileIdInfo, FileStandardInfo, GetDriveTypeW,
            GetFileInformationByHandleEx, GetFileType, GetFinalPathNameByHandleW,
            GetVolumeInformationByHandleW, QueryDosDeviceW, FILE_ATTRIBUTE_DEVICE,
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO,
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ,
            FILE_GENERIC_WRITE, FILE_ID_INFO, FILE_INFO_BY_HANDLE_CLASS, FILE_NAME_NORMALIZED,
            FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_STANDARD_INFO,
            FILE_TRAVERSE, FILE_TYPE_DISK, OPEN_EXISTING, SYNCHRONIZE, VOLUME_NAME_DOS,
        },
        System::{WindowsProgramming::DRIVE_FIXED, IO::IO_STATUS_BLOCK},
    },
};

const DIRECTORY_ACCESS: u32 = FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
const SHARE: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE;
const WRITER_NAME: &str = "writer.lock";
const WRITER_LINK_ERROR: &str = "writer.lock requires exactly one link";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FileIdentity {
    pub(super) volume_serial: u64,
    pub(super) file_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FileObservation {
    pub(super) identity: FileIdentity,
    pub(super) link_count: u32,
}

#[derive(Debug)]
pub(super) struct WindowsScope {
    path: PathBuf,
    ancestry: Vec<(File, FileIdentity)>,
}

#[derive(Debug)]
pub(super) struct WriterGuard<'scope> {
    scope: &'scope WindowsScope,
    file: File,
    identity: FileIdentity,
}

impl WriterGuard<'_> {
    pub(super) fn identity(&self) -> FileIdentity {
        self.identity
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn unsupported_root() -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        "Windows Ticket scope requires a fixed local NTFS drive path",
    )
}

fn invalid_entry(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

// Each call supplies the output type corresponding to its information class.
fn file_info<T: Default>(file: &File, class: FILE_INFO_BY_HANDLE_CLASS) -> io::Result<T> {
    let mut output = T::default();
    // SAFETY: the file owns a live handle and the output buffer is valid for the selected class.
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            class,
            (&mut output as *mut T).cast(),
            size_of::<T>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(output)
}

fn entry_identity(file: &File, directory: bool) -> io::Result<FileIdentity> {
    // SAFETY: the file owns a live handle.
    if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
        return Err(invalid_entry("Windows Ticket scope requires a disk object"));
    }
    let attrs = file_info::<FILE_ATTRIBUTE_TAG_INFO>(file, FileAttributeTagInfo)?;
    let flags = attrs.FileAttributes;
    if flags & (FILE_ATTRIBUTE_DEVICE | FILE_ATTRIBUTE_REPARSE_POINT) != 0
        || (flags & FILE_ATTRIBUTE_DIRECTORY != 0) != directory
    {
        return Err(invalid_entry(
            "Windows Ticket scope requires a plain filesystem entry",
        ));
    }
    handle_identity(file)
}

fn handle_identity(file: &File) -> io::Result<FileIdentity> {
    let id = file_info::<FILE_ID_INFO>(file, FileIdInfo)?;
    Ok(FileIdentity {
        volume_serial: id.VolumeSerialNumber,
        file_id: id.FileId.Identifier,
    })
}

fn open_relative(
    parent: &File,
    component: &OsStr,
    directory: bool,
    access: u32,
    disposition: NTCREATEFILE_CREATE_DISPOSITION,
) -> io::Result<File> {
    open_relative_shared(parent, component, directory, access, disposition, SHARE)
}

fn open_relative_shared(
    parent: &File,
    component: &OsStr,
    directory: bool,
    access: u32,
    disposition: NTCREATEFILE_CREATE_DISPOSITION,
    sharing: u32,
) -> io::Result<File> {
    let name_units: Vec<u16> = component.encode_wide().collect();
    if name_units.is_empty()
        || name_units == [u16::from(b'.')]
        || name_units == [u16::from(b'.'), u16::from(b'.')]
        || name_units
            .iter()
            .any(|&unit| matches!(unit, 0 | 0x2f | 0x3a | 0x5c))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "expected a literal scope name",
        ));
    }
    let length = name_units
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|length| u16::try_from(length).ok())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "scope name is too long"))?;
    let name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: name_units.as_ptr().cast_mut(),
    };
    let attrs = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &name,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let kind = if directory {
        FILE_DIRECTORY_FILE
    } else {
        FILE_NON_DIRECTORY_FILE
    };
    let mut handle: HANDLE = ptr::null_mut();
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: the live parent handle and all buffers outlive this synchronous operation.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            access,
            &attrs,
            &mut io_status,
            ptr::null(),
            0,
            sharing,
            disposition,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT | kind,
            ptr::null(),
            0,
        )
    };
    if status < 0 {
        // SAFETY: the status is the value returned by NtCreateFile.
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(status) } as i32,
        ));
    }
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(invalid_entry("relative scope open did not return a handle"));
    }
    // SAFETY: a successful NtCreateFile returns a newly owned handle.
    Ok(unsafe { File::from_raw_handle(handle) })
}

fn open_drive(disk: u8) -> io::Result<File> {
    let drive = [u16::from(disk), u16::from(b':'), 0];
    let root = [u16::from(disk), u16::from(b':'), u16::from(b'\\'), 0];
    // SAFETY: root is a NUL-terminated DOS drive root.
    if unsafe { GetDriveTypeW(root.as_ptr()) } != DRIVE_FIXED {
        return Err(unsupported_root());
    }
    let mut device_mapping = [0_u16; 1024];
    // SAFETY: drive is NUL-terminated, and the target buffer can hold the specified count.
    let units = unsafe {
        QueryDosDeviceW(
            drive.as_ptr(),
            device_mapping.as_mut_ptr(),
            device_mapping.len() as u32,
        )
    } as usize;
    if units == 0 {
        return Err(io::Error::last_os_error());
    }
    // QueryDosDevice supplies a multi-string. The first mapping is the current one.
    let local_device = (*b"\\Device\\").map(u16::from);
    if !device_mapping
        .get(..units)
        .is_some_and(|mapping| mapping.starts_with(&local_device))
    {
        return Err(unsupported_root());
    }
    // SAFETY: root and the other arguments are valid for this directory open.
    let handle = unsafe {
        CreateFileW(
            root.as_ptr(),
            DIRECTORY_ACCESS,
            SHARE,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE || handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW succeeded and gives this file ownership of its new handle.
    let drive = unsafe { File::from_raw_handle(handle) };
    let mut final_name = [0_u16; 16];
    // SAFETY: the drive and writable output buffer remain live for this call.
    let len = unsafe {
        GetFinalPathNameByHandleW(
            drive.as_raw_handle(),
            final_name.as_mut_ptr(),
            final_name.len() as u32,
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
        )
    } as usize;
    if len == 0 {
        return Err(io::Error::last_os_error());
    }
    let final_name = final_name.get(..len).ok_or_else(unsupported_root)?;
    let final_name = final_name
        .strip_suffix(&[u16::from(b'\\')])
        .unwrap_or(final_name);
    let prefix = (*b"\\\\?\\").map(u16::from);
    if let [head @ .., actual, colon] = final_name {
        if head == prefix
            && u8::try_from(*actual).is_ok_and(|actual| actual.eq_ignore_ascii_case(&disk))
            && *colon == u16::from(b':')
        {
            return Ok(drive);
        }
    }
    Err(unsupported_root())
}

fn require_ntfs(directory: &File) -> io::Result<()> {
    let mut filesystem = [0_u16; 32];
    // SAFETY: the live directory handle and correctly sized output outlive the call.
    let succeeded = unsafe {
        GetVolumeInformationByHandleW(
            directory.as_raw_handle(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            filesystem.as_mut_ptr(),
            filesystem.len() as u32,
        )
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    if !filesystem.starts_with(&[b'N', b'T', b'F', b'S', 0].map(u16::from)) {
        return Err(unsupported_root());
    }
    Ok(())
}

impl WindowsScope {
    pub(super) fn open_existing(path: &Path) -> io::Result<Self> {
        let mut components = path.components();
        let Some(Component::Prefix(prefix)) = components.next() else {
            return Err(unsupported_root());
        };
        let disk = match prefix.kind() {
            Prefix::Disk(disk) | Prefix::VerbatimDisk(disk) => disk,
            _ => return Err(unsupported_root()),
        };
        if !matches!(components.next(), Some(Component::RootDir)) {
            return Err(unsupported_root());
        }
        let drive = open_drive(disk)?;
        let drive_id = entry_identity(&drive, true)?;
        let mut scope = Self {
            path: path.to_owned(),
            ancestry: vec![(drive, drive_id)],
        };
        for component in components {
            let Component::Normal(name) = component else {
                return Err(unsupported_root());
            };
            let directory =
                open_relative(scope.directory(), name, true, DIRECTORY_ACCESS, FILE_OPEN)?;
            let identity = entry_identity(&directory, true)?;
            if identity.volume_serial != drive_id.volume_serial {
                return Err(invalid_entry(
                    "Windows Ticket scope crosses a volume boundary",
                ));
            }
            scope.ancestry.push((directory, identity));
        }
        if scope.ancestry.len() == 1 {
            return Err(unsupported_root());
        }
        require_ntfs(scope.directory())?;
        Ok(scope)
    }

    fn directory(&self) -> &File {
        &self.ancestry.last().expect("drive root is retained").0
    }

    pub(super) fn identity(&self) -> FileIdentity {
        self.ancestry.last().expect("drive root is retained").1
    }

    fn check_named_scope(&self) -> io::Result<()> {
        let named = Self::open_existing(&self.path)?;
        if !self
            .ancestry
            .iter()
            .map(|(_, id)| id)
            .eq(named.ancestry.iter().map(|(_, id)| id))
        {
            return Err(invalid_entry("Windows Ticket scope name changed identity"));
        }
        Ok(())
    }

    fn observe_file(&self, file: &File) -> io::Result<FileObservation> {
        let identity = entry_identity(file, false)?;
        let info = file_info::<FILE_STANDARD_INFO>(file, FileStandardInfo)?;
        if info.Directory
            || info.DeletePending
            || identity.volume_serial != self.identity().volume_serial
        {
            return Err(invalid_entry(
                "Windows Ticket scope file is not an active regular file on the admitted volume",
            ));
        }
        Ok(FileObservation {
            identity,
            link_count: info.NumberOfLinks,
        })
    }

    pub(super) fn inspect_file(&self, name: &str) -> io::Result<FileObservation> {
        self.check_named_scope()?;
        let file = open_relative(
            self.directory(),
            OsStr::new(name),
            false,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN,
        )?;
        self.observe_file(&file)
    }

    pub(super) fn acquire_writer(&self, create: bool) -> io::Result<WriterGuard<'_>> {
        self.check_named_scope()?;
        let disposition = if create { FILE_OPEN_IF } else { FILE_OPEN };
        let file = open_relative(
            self.directory(),
            OsStr::new(WRITER_NAME),
            false,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            disposition,
        )?;
        let observed = self.observe_file(&file)?;
        if observed.link_count != 1 {
            return Err(invalid_entry(WRITER_LINK_ERROR));
        }
        FileExt::try_lock_exclusive(&file)?;
        let guard = WriterGuard {
            scope: self,
            file,
            identity: observed.identity,
        };
        let named = self.inspect_file(WRITER_NAME)?;
        if named.link_count != 1 {
            return Err(invalid_entry(WRITER_LINK_ERROR));
        }
        if named.identity != guard.identity() {
            return Err(invalid_entry(
                "Windows Ticket writer lock name changed identity",
            ));
        }
        Ok(guard)
    }
}

mod head;
mod immutable;
mod tests;
