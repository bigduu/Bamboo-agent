//! Anchored, link-free acquisition of the Inbox operation lock.
//!
//! The Inbox directory name and its advisory lock name are security-relevant:
//! when either is replaced by a symbolic link, a path-based writer both mints a
//! lock file outside the canonical Session Inbox and serializes on it. Every
//! component below the canonical Inbox parent is therefore opened relative to a
//! retained directory handle with `O_NOFOLLOW` (Unix) or through `NtCreateFile`
//! with `FILE_OPEN_REPARSE_POINT` plus reparse validation (Windows).
//!
//! Ancestors above that parent are deliberately still resolved through the
//! filesystem: platform roots such as macOS `/var` are symbolic links by design,
//! so a blanket no-follow walk from `/` would reject every real Inbox. The
//! retained parent is canonicalized once, and no name is resolved through a path
//! string again after that.

use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

/// A retained handle to the canonical Inbox directory.
pub(super) struct InboxAnchor {
    #[cfg_attr(
        not(any(target_os = "linux", target_os = "macos", windows)),
        allow(dead_code)
    )]
    dir: File,
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn unsupported() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::Unsupported,
        "Inbox anchoring requires retained no-follow directories",
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod platform {
    use super::*;
    use std::ffi::{CStr, CString, OsStr};
    use std::os::fd::{FromRawFd, RawFd};
    use std::os::unix::ffi::OsStrExt;

    fn invalid() -> std::io::Error {
        std::io::Error::new(ErrorKind::InvalidInput, "invalid Inbox anchor name")
    }

    pub(super) fn c_name(value: &OsStr) -> std::io::Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| invalid())
    }

    /// Reopened from a canonical path, so `O_NOFOLLOW` only rejects a
    /// replacement between canonicalization and this open.
    pub(super) fn open_canonical_dir(path: &Path) -> std::io::Result<File> {
        let name = c_name(path.as_os_str())?;
        let fd = unsafe {
            libc::open(
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    pub(super) fn open_child_dir(parent: RawFd, name: &CStr) -> std::io::Result<File> {
        let fd = unsafe {
            libc::openat(
                parent,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    pub(super) fn create_child_dir(parent: RawFd, name: &CStr) -> std::io::Result<()> {
        let created = unsafe { libc::mkdirat(parent, name.as_ptr(), 0o755 as libc::mode_t) };
        if created == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        // A concurrent creator is re-validated by the caller's retried open.
        match error.kind() {
            ErrorKind::AlreadyExists => Ok(()),
            _ => Err(error),
        }
    }

    pub(super) fn open_child_file(
        parent: RawFd,
        name: &CStr,
        flags: libc::c_int,
    ) -> std::io::Result<File> {
        let fd = unsafe {
            libc::openat(
                parent,
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o644 as libc::c_int,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl InboxAnchor {
    /// Open the Inbox directory itself, creating it once when it is absent.
    /// A link, non-directory or unreadable name fails closed.
    pub(super) fn acquire(dir: &Path) -> std::io::Result<Self> {
        use std::path::Path as StdPath;
        let name = dir
            .file_name()
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "Inbox has no name"))?;
        let name = platform::c_name(&name.to_owned())
            .map_err(|_| std::io::Error::new(ErrorKind::InvalidInput, "Inbox has no name"))?;
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| StdPath::new("."));
        // Ancestors may legitimately be links; the retained anchor starts at the
        // canonical parent so that only the Inbox name itself must be link-free.
        std::fs::create_dir_all(parent)?;
        let parent = platform::open_canonical_dir(&std::fs::canonicalize(parent)?)?;
        let dir = match platform::open_child_dir(parent.as_raw_fd(), &name) {
            Ok(dir) => dir,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Create exactly once, then retry the same no-follow open so a
                // replacement inside the window is rejected instead of trusted.
                platform::create_child_dir(parent.as_raw_fd(), &name)?;
                platform::open_child_dir(parent.as_raw_fd(), &name)?
            }
            Err(error) => return Err(error),
        };
        Ok(Self { dir })
    }

    /// Create or open `name` inside the retained Inbox and require a regular
    /// file. A link (`ELOOP`), directory or device at that name fails closed.
    pub(super) fn open_lock(&self, name: &str) -> std::io::Result<File> {
        let name = platform::c_name(std::ffi::OsStr::new(name))?;
        let file =
            platform::open_child_file(self.dir.as_raw_fd(), &name, libc::O_RDWR | libc::O_CREAT)?;
        if !file.metadata()?.is_file() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Inbox operation lock is not a regular file",
            ));
        }
        Ok(file)
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use std::path::Path as StdPath;
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        NtCreateFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN_FOR_BACKUP_INTENT,
        FILE_OPEN_IF, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        HANDLE, INVALID_HANDLE_VALUE, OBJ_DONT_REPARSE, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FileAttributeTagInfo, GetFileInformationByHandleEx, GetFileType,
        FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_ATTRIBUTE_TAG_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK, FILE_WRITE_DATA, OPEN_EXISTING,
        SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    fn rejected() -> std::io::Error {
        std::io::Error::new(
            ErrorKind::InvalidData,
            "Inbox anchor name is not a link-free regular object",
        )
    }

    /// Absolute parent acquisition. The canonical path is opened with
    /// `FILE_FLAG_OPEN_REPARSE_POINT` and then validated, so a reparse point is
    /// observed as an object instead of being followed.
    pub(super) fn open_canonical_dir(path: &StdPath) -> std::io::Result<File> {
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
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle.cast()) };
        validate(&file, true)?;
        Ok(file)
    }

    fn open_relative(
        parent: &File,
        name: &OsStr,
        directory: bool,
        access: u32,
    ) -> std::io::Result<File> {
        let units: Vec<u16> = name.encode_wide().collect();
        if units.is_empty()
            || units == [b'.' as u16]
            || units == [b'.' as u16, b'.' as u16]
            || units.iter().any(|unit| {
                *unit == 0 || *unit == b'\\' as u16 || *unit == b'/' as u16 || *unit == b':' as u16
            })
        {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "invalid Inbox anchor name",
            ));
        }
        let byte_len = units
            .len()
            .checked_mul(2)
            .and_then(|length| u16::try_from(length).ok())
            .ok_or_else(|| {
                std::io::Error::new(ErrorKind::InvalidInput, "invalid Inbox anchor name")
            })?;
        let name = UNICODE_STRING {
            Length: byte_len,
            MaximumLength: byte_len,
            Buffer: units.as_ptr().cast_mut(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_raw_handle() as HANDLE,
            ObjectName: &name,
            // Fail closed when any component of the relative name is a reparse
            // point instead of silently traversing it.
            Attributes: OBJ_DONT_REPARSE,
            SecurityDescriptor: std::ptr::null(),
            SecurityQualityOfService: std::ptr::null(),
        };
        let mut status_block = IO_STATUS_BLOCK::default();
        let mut handle: HANDLE = std::ptr::null_mut();
        let status = unsafe {
            NtCreateFile(
                &mut handle,
                access,
                &attributes,
                &mut status_block,
                std::ptr::null(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN_IF,
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
        // Any failure rejects: This anchor never retries through a path and
        // never accepts a substituted object.
        if status < 0 {
            return Err(rejected());
        }
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(rejected());
        }
        let file = unsafe { File::from_raw_handle(handle.cast()) };
        validate(&file, directory)?;
        Ok(file)
    }

    fn validate(file: &File, directory: bool) -> std::io::Result<()> {
        let handle = file.as_raw_handle() as HANDLE;
        if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
            return Err(rejected());
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
            return Err(std::io::Error::last_os_error());
        }
        let attributes = info.FileAttributes;
        if attributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DEVICE) != 0
            || (attributes & FILE_ATTRIBUTE_DIRECTORY != 0) != directory
        {
            return Err(rejected());
        }
        Ok(())
    }

    /// Reject a relative parent that cannot anchor a no-follow walk.
    pub(super) fn canonical_parent(path: &StdPath) -> std::io::Result<std::path::PathBuf> {
        // `canonicalize` returns a `\\?\`-verbatim path, which `CreateFileW`
        // accepts and which no longer contains a traversable reparse point.
        std::fs::canonicalize(path).map_err(|_| rejected())
    }
}

#[cfg(windows)]
impl InboxAnchor {
    pub(super) fn acquire(dir: &Path) -> std::io::Result<Self> {
        let name = dir
            .file_name()
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "Inbox has no name"))?
            .to_owned();
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let parent = platform::open_canonical_dir(&platform::canonical_parent(parent)?)?;
        let dir = platform::open_relative(
            &parent,
            &name,
            true,
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )?;
        Ok(Self { dir })
    }

    pub(super) fn open_lock(&self, name: &str) -> std::io::Result<File> {
        platform::open_relative(
            &self.dir,
            std::ffi::OsStr::new(name),
            false,
            FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
impl InboxAnchor {
    pub(super) fn acquire(_: &Path) -> std::io::Result<Self> {
        Err(unsupported())
    }

    pub(super) fn open_lock(&self, _: &str) -> std::io::Result<File> {
        Err(unsupported())
    }
}
