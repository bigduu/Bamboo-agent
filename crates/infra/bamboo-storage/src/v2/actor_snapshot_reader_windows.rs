//! Windows snapshot reader. Every descendant is opened relative to a retained
//! directory handle; reparse points are opened as objects and then rejected.
//! Snapshot reads never resolve an ancestor again through a path string.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Component, Path, PathBuf, Prefix};

use bamboo_domain::{ActorSnapshotError as Error, ActorSnapshotLimits};
use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::{
    NtCreateFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN,
    FILE_OPEN_FOR_BACKUP_INTENT, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
};
use windows_sys::Win32::Foundation::{
    ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE, OBJ_DONT_REPARSE,
    STATUS_FILE_IS_A_DIRECTORY, STATUS_NOT_A_DIRECTORY, STATUS_NO_SUCH_FILE,
    STATUS_OBJECT_NAME_NOT_FOUND, STATUS_OBJECT_PATH_NOT_FOUND, STATUS_REPARSE_POINT_ENCOUNTERED,
    UNICODE_STRING,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileAttributeTagInfo, FileIdBothDirectoryInfo, FileIdBothDirectoryRestartInfo,
    GetFileInformationByHandleEx, GetFileType, FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY,
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_BOTH_DIR_INFO, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES,
    FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK,
    OPEN_EXISTING, SYNCHRONIZE,
};
use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

pub(super) struct ReadBudget {
    pub limits: ActorSnapshotLimits,
    entries: usize,
    reads: usize,
    bytes: usize,
}

impl ReadBudget {
    pub fn new(limits: ActorSnapshotLimits) -> Self {
        Self {
            limits,
            entries: 0,
            reads: 0,
            bytes: 0,
        }
    }
}

pub(super) struct Directory(File);

impl Directory {
    pub fn open_absolute(path: &Path) -> Result<Self, Error> {
        if !path.is_absolute() {
            return Err(Error::UnsupportedAuthority);
        }
        let mut components = path.components();
        let Some(Component::Prefix(prefix)) = components.next() else {
            return Err(Error::UnsupportedAuthority);
        };
        // A drive root is the only initial pathname. UNC, device and volume
        // namespaces need a separate trusted root acquisition contract.
        if !matches!(prefix.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_))
            || !matches!(components.next(), Some(Component::RootDir))
        {
            return Err(Error::UnsupportedAuthority);
        }
        let mut volume = PathBuf::from(prefix.as_os_str());
        volume.push(r"\");
        let mut directory = open_volume_root(&volume)?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(Error::UnsupportedAuthority);
            };
            directory = directory
                .child(name)
                .map_err(|error| match error {
                    Error::InconsistentAuthority => Error::UnsupportedAuthority,
                    other => other,
                })?
                .ok_or(Error::NotFound)?;
        }
        Ok(directory)
    }

    pub fn child(&self, name: &OsStr) -> Result<Option<Self>, Error> {
        open_relative(&self.0, name, true).map(|file| file.map(Self))
    }

    pub fn entries(&self, budget: &mut ReadBudget) -> Result<Vec<OsString>, Error> {
        // GetFileInformationByHandleEx advances the enumeration cursor on this
        // retained directory handle. A 64 KiB batch exceeds a maximal NTFS
        // filename, and the node/scan budget is checked for every returned row.
        const WORDS: usize = 8192;
        let mut buffer = [0u64; WORDS];
        let capacity = std::mem::size_of_val(&buffer);
        let name_offset = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
        let length_offset = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileNameLength);
        let next_offset = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, NextEntryOffset);
        let mut restart = true;
        let mut entries = Vec::new();
        loop {
            buffer.fill(0);
            let class = if restart {
                FileIdBothDirectoryRestartInfo
            } else {
                FileIdBothDirectoryInfo
            };
            let ok = unsafe {
                GetFileInformationByHandleEx(
                    self.0.as_raw_handle() as HANDLE,
                    class,
                    buffer.as_mut_ptr().cast(),
                    capacity as u32,
                )
            };
            if ok == 0 {
                return match io::Error::last_os_error().raw_os_error() {
                    Some(code) if code == ERROR_NO_MORE_FILES as i32 => Ok(sorted(entries)),
                    _ => Err(Error::StorageUnavailable),
                };
            }
            restart = false;
            let base = buffer.as_ptr().cast::<u8>();
            let mut offset = 0usize;
            loop {
                if offset
                    .checked_add(name_offset)
                    .is_none_or(|end| end > capacity)
                {
                    return Err(Error::InconsistentAuthority);
                }
                let next = unsafe {
                    base.add(offset + next_offset)
                        .cast::<u32>()
                        .read_unaligned()
                } as usize;
                let name_bytes = unsafe {
                    base.add(offset + length_offset)
                        .cast::<u32>()
                        .read_unaligned()
                } as usize;
                let end = offset
                    .checked_add(name_offset)
                    .and_then(|start| start.checked_add(name_bytes))
                    .ok_or(Error::InconsistentAuthority)?;
                if name_bytes == 0
                    || name_bytes % 2 != 0
                    || end > capacity
                    || (next != 0 && offset.checked_add(next).is_none_or(|bound| end > bound))
                {
                    return Err(Error::InconsistentAuthority);
                }
                let mut units = Vec::with_capacity(name_bytes / 2);
                for index in 0..name_bytes / 2 {
                    units.push(unsafe {
                        base.add(offset + name_offset + index * 2)
                            .cast::<u16>()
                            .read_unaligned()
                    });
                }
                if units != [b'.' as u16] && units != [b'.' as u16, b'.' as u16] {
                    budget.entries += 1;
                    if budget.entries > budget.limits.directory_entries {
                        return Err(Error::BudgetExceeded);
                    }
                    entries.push(OsString::from_wide(&units));
                }
                if next == 0 {
                    break;
                }
                if next < name_offset || offset.checked_add(next).is_none_or(|end| end >= capacity)
                {
                    return Err(Error::InconsistentAuthority);
                }
                offset += next;
            }
        }
    }

    pub fn read_main_section(&self, budget: &mut ReadBudget) -> Result<Option<Vec<u8>>, Error> {
        use super::compact_main::{HEADER_BYTES, PREFIX, SECTION_CAP};
        budget.reads += 1;
        if budget.reads > budget.limits.file_reads {
            return Err(Error::BudgetExceeded);
        }
        let Some(mut file) = open_relative(&self.0, OsStr::new("session.json"), false)? else {
            return Ok(None);
        };
        let max = SECTION_CAP.min(budget.limits.file_bytes).min(
            budget
                .limits
                .aggregate_read_bytes
                .saturating_sub(budget.bytes),
        );
        if max < HEADER_BYTES {
            return Err(Error::BudgetExceeded);
        }
        let mut bytes = vec![0; HEADER_BYTES];
        read_exact_counted(&mut file, &mut bytes[..PREFIX.len()], budget, Some(PREFIX))?;
        read_exact_counted(&mut file, &mut bytes[PREFIX.len()..], budget, None)?;
        let section_len = super::compact_main::section_length(&bytes)
            .map_err(|_| Error::InconsistentAuthority)?;
        if section_len > max {
            return Err(Error::BudgetExceeded);
        }
        bytes.resize(section_len, 0);
        read_exact_counted(&mut file, &mut bytes[HEADER_BYTES..], budget, None)?;
        Ok(Some(bytes))
    }

    pub fn read(
        &self,
        name: &str,
        file_limit: usize,
        budget: &mut ReadBudget,
    ) -> Result<Option<Vec<u8>>, Error> {
        budget.reads += 1;
        if budget.reads > budget.limits.file_reads {
            return Err(Error::BudgetExceeded);
        }
        let Some(file) = open_relative(&self.0, OsStr::new(name), false)? else {
            return Ok(None);
        };
        let max = file_limit.min(budget.limits.file_bytes).min(
            budget
                .limits
                .aggregate_read_bytes
                .saturating_sub(budget.bytes),
        );
        if file
            .metadata()
            .map_err(|_| Error::StorageUnavailable)?
            .len()
            > max as u64
        {
            return Err(Error::BudgetExceeded);
        }
        read_content(file, max, budget).map(Some)
    }
}

fn sorted(mut entries: Vec<OsString>) -> Vec<OsString> {
    entries.sort();
    entries
}

fn read_exact_counted(
    file: &mut File,
    mut destination: &mut [u8],
    budget: &mut ReadBudget,
    expected: Option<&[u8]>,
) -> Result<(), Error> {
    let mut offset = 0;
    while !destination.is_empty() {
        match file.read(destination) {
            Ok(0) => return Err(Error::InconsistentAuthority),
            Ok(count) => {
                budget.bytes += count;
                if expected
                    .is_some_and(|prefix| destination[..count] != prefix[offset..offset + count])
                {
                    return Err(Error::UnsupportedAuthority);
                }
                offset += count;
                destination = &mut destination[count..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(Error::StorageUnavailable),
        }
    }
    Ok(())
}

pub(super) fn read_content(
    mut file: File,
    max: usize,
    budget: &mut ReadBudget,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    let result = file.by_ref().take(max as u64 + 1).read_to_end(&mut bytes);
    budget.bytes += bytes.len();
    if bytes.len() > max || budget.bytes > budget.limits.aggregate_read_bytes {
        return Err(Error::BudgetExceeded);
    }
    result.map_err(|_| Error::StorageUnavailable)?;
    Ok(bytes)
}

fn open_volume_root(path: &Path) -> Result<Directory, Error> {
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(Error::StorageUnavailable);
    }
    let file = unsafe { File::from_raw_handle(handle.cast()) };
    validate_handle(&file, true)?;
    Ok(Directory(file))
}

fn open_relative(parent: &File, name: &OsStr, directory: bool) -> Result<Option<File>, Error> {
    let units: Vec<u16> = name.encode_wide().collect();
    if units.is_empty()
        || units == [b'.' as u16]
        || units == [b'.' as u16, b'.' as u16]
        || units.iter().any(|unit| {
            *unit == 0 || *unit == b'\\' as u16 || *unit == b'/' as u16 || *unit == b':' as u16
        })
    {
        return Err(Error::InconsistentAuthority);
    }
    let byte_len = units
        .len()
        .checked_mul(2)
        .and_then(|length| u16::try_from(length).ok())
        .ok_or(Error::InconsistentAuthority)?;
    let name = UNICODE_STRING {
        Length: byte_len,
        MaximumLength: byte_len,
        Buffer: units.as_ptr().cast_mut(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle() as HANDLE,
        ObjectName: &name,
        Attributes: OBJ_DONT_REPARSE,
        SecurityDescriptor: std::ptr::null(),
        SecurityQualityOfService: std::ptr::null(),
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut handle: HANDLE = std::ptr::null_mut();
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            if directory {
                FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE
            } else {
                FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE
            },
            &attributes,
            &mut status_block,
            std::ptr::null(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN,
            FILE_OPEN_REPARSE_POINT
                | FILE_SYNCHRONOUS_IO_NONALERT
                | FILE_OPEN_FOR_BACKUP_INTENT
                | if directory {
                    FILE_DIRECTORY_FILE
                } else {
                    FILE_NON_DIRECTORY_FILE
                },
            std::ptr::null(),
            0,
        )
    };
    if status < 0 {
        return match status {
            STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND | STATUS_NO_SUCH_FILE => {
                Ok(None)
            }
            STATUS_REPARSE_POINT_ENCOUNTERED
            | STATUS_NOT_A_DIRECTORY
            | STATUS_FILE_IS_A_DIRECTORY => Err(Error::InconsistentAuthority),
            _ => Err(Error::StorageUnavailable),
        };
    }
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(Error::StorageUnavailable);
    }
    let file = unsafe { File::from_raw_handle(handle.cast()) };
    validate_handle(&file, directory)?;
    Ok(Some(file))
}

fn validate_handle(file: &File, directory: bool) -> Result<(), Error> {
    let handle = file.as_raw_handle() as HANDLE;
    if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
        return Err(Error::InconsistentAuthority);
    }
    let mut info = FILE_ATTRIBUTE_TAG_INFO::default();
    let ok = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileAttributeTagInfo,
            (&mut info as *mut FILE_ATTRIBUTE_TAG_INFO).cast(),
            std::mem::size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    };
    if ok == 0 {
        return Err(Error::StorageUnavailable);
    }
    let attributes = info.FileAttributes;
    if attributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DEVICE) != 0
        || (attributes & FILE_ATTRIBUTE_DIRECTORY != 0) != directory
    {
        return Err(Error::InconsistentAuthority);
    }
    Ok(())
}
