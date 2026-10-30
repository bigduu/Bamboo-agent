//! Anchored, link-free access to the canonical Inbox directory.
//!
//! The Inbox directory name and every name inside it are security-relevant:
//! when one of them is replaced by a symbolic link (or a Windows reparse
//! point), a path-based writer both mints a lock file outside the canonical
//! Session Inbox and publishes queue entries there. Names below the canonical
//! Inbox parent are therefore resolved relative to retained directory handles
//! only, with `O_NOFOLLOW` on Unix and `NtCreateFile`/`NtSetInformationFile`
//! with `FILE_OPEN_REPARSE_POINT` on Windows.
//!
//! Ancestors above that parent are deliberately still resolved through the
//! filesystem: platform roots such as macOS `/var` are symbolic links by design,
//! so a blanket no-follow walk from `/` would reject every real Inbox. The
//! parent is canonicalized once, and no name is resolved through a path string
//! again after that.

use std::ffi::OsString;
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

/// A retained handle to the canonical Inbox directory.
pub(super) struct InboxAnchor {
    /// The Inbox address exactly as the caller names it. Relative paths built
    /// from that address are resolved against `dir`, never through the string.
    root: PathBuf,
    dir: File,
}

/// One mutation address inside the Inbox: intermediate names plus the leaf.
struct Parts {
    parents: Vec<OsString>,
    name: OsString,
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidInput, message)
}

/// Bounded repetitions of the create-then-open cycle for the lock name. It only
/// spans the interleaving between a creator and the callers that lost the race.
const LOCK_ATTEMPTS: usize = 8;

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
impl InboxAnchor {
    /// Split a path that must name an entry inside this Inbox. Absolute,
    /// `CurDir` and parent components are rejected, so an anchored operation can
    /// never be redirected out of the retained directory.
    fn parts(&self, path: &Path) -> std::io::Result<Parts> {
        let relative = path
            .strip_prefix(&self.root)
            .map_err(|_| invalid("Inbox path is outside its retained anchor"))?;
        let mut parents = Vec::new();
        let mut name = None;
        for component in relative.components() {
            let Component::Normal(part) = component else {
                return Err(invalid("Inbox path has a non-literal component"));
            };
            if let Some(previous) = name.replace(part.to_owned()) {
                parents.push(previous);
            }
        }
        let name = name.ok_or_else(|| invalid("Inbox path names the anchor itself"))?;
        Ok(Parts { parents, name })
    }

    /// Walk the intermediate names from the retained Inbox handle. A link or
    /// non-directory anywhere in the chain fails closed.
    fn parent_handle(&self, parents: &[OsString], create: bool) -> std::io::Result<File> {
        let mut current = platform::duplicate(&self.dir)?;
        for parent in parents {
            current = match platform::open_dir(&current, parent) {
                Ok(next) => next,
                Err(error) if create && error.kind() == ErrorKind::NotFound => {
                    platform::create_dir(&current, parent)?;
                    platform::open_dir(&current, parent)?
                }
                Err(error) => return Err(error),
            };
        }
        Ok(current)
    }

    pub(super) fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        let parts = self.parts(path)?;
        let parent = self.parent_handle(&parts.parents, true)?;
        platform::create_dir(&parent, &parts.name)?;
        // Re-open so an existing non-directory at that name is rejected instead
        // of being reported as a successful create.
        platform::open_dir(&parent, &parts.name)?;
        Ok(())
    }

    /// Create a new entry that must not already exist. Callers use this for the
    /// hidden Maildir temp, so a pre-existing name is never truncated.
    pub(super) fn create_exclusive(&self, path: &Path) -> std::io::Result<File> {
        let parts = self.parts(path)?;
        let parent = self.parent_handle(&parts.parents, false)?;
        platform::create_exclusive(&parent, &parts.name)
    }

    /// Atomically replace the destination, matching the previous
    /// `rename`/`MoveFileEx` replacement contract.
    pub(super) fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let from = self.parts(from)?;
        let to = self.parts(to)?;
        let from_dir = self.parent_handle(&from.parents, false)?;
        let to_dir = self.parent_handle(&to.parents, false)?;
        platform::rename(&from_dir, &from.name, &to_dir, &to.name)
    }

    pub(super) fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        let parts = self.parts(path)?;
        let parent = self.parent_handle(&parts.parents, false)?;
        platform::remove(&parent, &parts.name)
    }

    /// `symlink_metadata` equivalent: reports whether the name exists without
    /// following it.
    pub(super) fn entry_exists(&self, path: &Path) -> std::io::Result<bool> {
        let parts = self.parts(path)?;
        let parent = self.parent_handle(&parts.parents, false)?;
        platform::exists(&parent, &parts.name)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod platform {
    use super::*;
    use std::ffi::{CStr, CString, OsStr};
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};
    use std::os::unix::ffi::OsStrExt;

    pub(super) fn c_name(value: &OsStr) -> std::io::Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| invalid("invalid Inbox anchor name"))
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

    pub(super) fn duplicate(dir: &File) -> std::io::Result<File> {
        let fd = unsafe { libc::dup(dir.as_raw_fd()) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    pub(super) fn open_dir(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_child_dir(parent.as_raw_fd(), &c_name(name)?)
    }

    pub(super) fn create_dir(parent: &File, name: &OsStr) -> std::io::Result<()> {
        create_child_dir(parent.as_raw_fd(), &c_name(name)?)
    }

    pub(super) fn create_exclusive(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_child_file(
            parent.as_raw_fd(),
            &c_name(name)?,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        )
    }

    pub(super) fn rename(
        from_dir: &File,
        from: &OsStr,
        to_dir: &File,
        to: &OsStr,
    ) -> std::io::Result<()> {
        let from = c_name(from)?;
        let to = c_name(to)?;
        let renamed = unsafe {
            libc::renameat(
                from_dir.as_raw_fd(),
                from.as_ptr(),
                to_dir.as_raw_fd(),
                to.as_ptr(),
            )
        };
        if renamed != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn remove(parent: &File, name: &OsStr) -> std::io::Result<()> {
        let name = c_name(name)?;
        let removed = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
        if removed != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn exists(parent: &File, name: &OsStr) -> std::io::Result<bool> {
        let name = c_name(name)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
        let found = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if found == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        match error.kind() {
            ErrorKind::NotFound => Ok(false),
            _ => Err(error),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl InboxAnchor {
    /// Open the Inbox directory itself, creating it once when it is absent.
    /// A link, non-directory or unreadable name fails closed.
    pub(super) fn acquire(dir: &Path) -> std::io::Result<Self> {
        let leaf = dir
            .file_name()
            .ok_or_else(|| invalid("Inbox has no name"))?;
        let leaf = platform::c_name(leaf).map_err(|_| invalid("Inbox has no name"))?;
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        // Ancestors may legitimately be links; the retained anchor starts at the
        // canonical parent so that only the Inbox name itself must be link-free.
        std::fs::create_dir_all(parent)?;
        let canonical = std::fs::canonicalize(parent)?;
        let parent = platform::open_canonical_dir(&canonical)?;
        let anchored = match platform::open_child_dir(parent.as_raw_fd(), &leaf) {
            Ok(anchored) => anchored,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Create exactly once, then retry the same no-follow open so a
                // replacement inside the window is rejected instead of trusted.
                platform::create_child_dir(parent.as_raw_fd(), &leaf)?;
                platform::open_child_dir(parent.as_raw_fd(), &leaf)?
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            root: dir.to_owned(),
            dir: anchored,
        })
    }

    /// Open the Inbox operation lock, creating it exactly once when absent, and
    /// require a regular file. A link (`ELOOP`), directory or device fails closed.
    ///
    /// Creation is separated from opening on purpose: a racing
    /// `openat(O_CREAT)` of a *new* name can spuriously fail with `ENOENT` on
    /// APFS, while a plain open of an existing name never does. The first writer
    /// therefore creates with `O_EXCL`, and every other caller only ever opens.
    pub(super) fn open_lock(&self, name: &str) -> std::io::Result<File> {
        let name = platform::c_name(std::ffi::OsStr::new(name))?;
        let mut last = None;
        for _ in 0..LOCK_ATTEMPTS {
            match platform::open_child_file(self.dir.as_raw_fd(), &name, libc::O_RDWR) {
                Ok(file) => return require_regular(file),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match platform::open_child_file(
                        self.dir.as_raw_fd(),
                        &name,
                        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                    ) {
                        // This caller won the race and owns the new lock file.
                        Ok(file) => return require_regular(file),
                        // Another caller created it first, or the window is still
                        // settling: the next round opens the published name.
                        Err(create)
                            if matches!(
                                create.kind(),
                                ErrorKind::AlreadyExists | ErrorKind::NotFound
                            ) =>
                        {
                            last = Some(create);
                        }
                        Err(create) => return Err(create),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            std::io::Error::new(
                ErrorKind::WouldBlock,
                "Inbox operation lock could not be opened",
            )
        }))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn require_regular(file: File) -> std::io::Result<File> {
    if !file.metadata()?.is_file() {
        return Err(invalid("Inbox operation lock is not a regular file"));
    }
    Ok(file)
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FileDispositionInformation, FileRenameInformation, NtCreateFile, NtSetInformationFile,
        FILE_CREATE, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN,
        FILE_OPEN_FOR_BACKUP_INTENT, FILE_OPEN_IF, FILE_OPEN_REPARSE_POINT,
        FILE_RENAME_INFORMATION, FILE_RENAME_INFORMATION_0, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        HANDLE, INVALID_HANDLE_VALUE, OBJ_DONT_REPARSE, STATUS_NO_SUCH_FILE,
        STATUS_OBJECT_NAME_NOT_FOUND, STATUS_OBJECT_PATH_NOT_FOUND, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FileAttributeTagInfo, GetFileInformationByHandleEx, GetFileType, DELETE,
        FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_ATTRIBUTE_TAG_INFO, FILE_DISPOSITION_INFO, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK, FILE_WRITE_DATA,
        OPEN_EXISTING, SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    fn rejected(message: &str) -> std::io::Error {
        std::io::Error::new(ErrorKind::InvalidData, message)
    }

    fn name_units(name: &OsStr) -> std::io::Result<Vec<u16>> {
        let units: Vec<u16> = name.encode_wide().collect();
        if units.is_empty()
            || units == [b'.' as u16]
            || units == [b'.' as u16, b'.' as u16]
            || units.iter().any(|unit| {
                *unit == 0 || *unit == b'\\' as u16 || *unit == b'/' as u16 || *unit == b':' as u16
            })
        {
            return Err(invalid("invalid Inbox anchor name"));
        }
        Ok(units)
    }

    /// Absolute parent acquisition. The canonical path is opened with
    /// `FILE_FLAG_OPEN_REPARSE_POINT` and then validated, so a reparse point is
    /// observed as an object instead of being followed.
    pub(super) fn open_canonical_dir(path: &Path) -> std::io::Result<File> {
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
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
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
        disposition: u32,
        access: u32,
    ) -> std::io::Result<File> {
        let units = name_units(name)?;
        let byte_len = units
            .len()
            .checked_mul(2)
            .and_then(|length| u16::try_from(length).ok())
            .ok_or_else(|| invalid("invalid Inbox anchor name"))?;
        let unicode = UNICODE_STRING {
            Length: byte_len,
            MaximumLength: byte_len,
            Buffer: units.as_ptr().cast_mut(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_raw_handle() as HANDLE,
            ObjectName: &unicode,
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
                disposition,
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
                STATUS_OBJECT_NAME_NOT_FOUND
                | STATUS_OBJECT_PATH_NOT_FOUND
                | STATUS_NO_SUCH_FILE => Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    "Inbox anchor name is absent",
                )),
                _ => Err(rejected("Inbox anchor name was rejected")),
            };
        }
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(rejected("Inbox anchor handle was rejected"));
        }
        let file = unsafe { File::from_raw_handle(handle.cast()) };
        validate(&file, directory)?;
        Ok(file)
    }

    fn validate(file: &File, directory: bool) -> std::io::Result<()> {
        let handle = file.as_raw_handle() as HANDLE;
        if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
            return Err(rejected("Inbox anchor object is not a disk object"));
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
            return Err(rejected(
                "Inbox anchor object is a reparse point or wrong type",
            ));
        }
        Ok(())
    }

    /// `canonicalize` returns a `\\?\`-verbatim path, which `CreateFileW`
    /// accepts and which no longer contains a traversable reparse point.
    pub(super) fn canonical_parent(path: &Path) -> std::io::Result<PathBuf> {
        std::fs::canonicalize(path).map_err(|_| rejected("Inbox parent is not canonical"))
    }

    pub(super) fn duplicate(dir: &File) -> std::io::Result<File> {
        dir.try_clone()
    }

    pub(super) fn open_dir(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_relative(
            parent,
            name,
            true,
            FILE_OPEN,
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )
    }

    pub(super) fn create_dir(parent: &File, name: &OsStr) -> std::io::Result<()> {
        // `FILE_OPEN_IF` is the mkdir-with-existing-tolerant step; `validate`
        // inside the open still rejects a reparse point or a plain file.
        open_relative(
            parent,
            name,
            true,
            FILE_OPEN_IF,
            FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )?;
        Ok(())
    }

    /// Open an existing regular file only, failing closed when it is absent.
    pub(super) fn open_file(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_relative(
            parent,
            name,
            false,
            FILE_OPEN,
            FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )
    }

    /// Create a new regular file, failing when the name already exists.
    pub(super) fn create_file(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_relative(
            parent,
            name,
            false,
            FILE_CREATE,
            FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )
    }

    pub(super) fn create_exclusive(parent: &File, name: &OsStr) -> std::io::Result<File> {
        open_relative(
            parent,
            name,
            false,
            FILE_CREATE,
            FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        )
    }

    pub(super) fn rename(
        from_dir: &File,
        from: &OsStr,
        to_dir: &File,
        to: &OsStr,
    ) -> std::io::Result<()> {
        // The rename target is the file itself, opened only for its DELETE
        // access so the directory entry can be replaced.
        let file = open_relative(from_dir, from, false, FILE_OPEN, DELETE | SYNCHRONIZE)?;
        let units = name_units(to)?;
        let header = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
        let byte_len = units.len() * 2;
        let mut info = FILE_RENAME_INFORMATION::default();
        info.Anonymous = FILE_RENAME_INFORMATION_0 {
            ReplaceIfExists: true,
        };
        info.RootDirectory = to_dir.as_raw_handle() as HANDLE;
        info.FileNameLength = byte_len as u32;
        let mut buffer = vec![0u8; header + byte_len];
        unsafe {
            std::ptr::copy_nonoverlapping(
                (&info as *const FILE_RENAME_INFORMATION).cast::<u8>(),
                buffer.as_mut_ptr(),
                header,
            );
            std::ptr::copy_nonoverlapping(
                units.as_ptr(),
                buffer.as_mut_ptr().add(header).cast::<u16>(),
                units.len(),
            );
        }
        set_information(
            &file,
            buffer.as_ptr().cast(),
            buffer.len(),
            FileRenameInformation,
        )
    }

    pub(super) fn remove(parent: &File, name: &OsStr) -> std::io::Result<()> {
        let file = open_relative(parent, name, false, FILE_OPEN, DELETE | SYNCHRONIZE)?;
        // Deletion is bound to the retained handle and completes when it closes.
        let info = FILE_DISPOSITION_INFO { DeleteFile: true };
        set_information(
            &file,
            (&info as *const FILE_DISPOSITION_INFO).cast(),
            std::mem::size_of::<FILE_DISPOSITION_INFO>(),
            FileDispositionInformation,
        )
    }

    fn set_information(
        file: &File,
        information: *const core::ffi::c_void,
        length: usize,
        class: i32,
    ) -> std::io::Result<()> {
        let mut status_block = IO_STATUS_BLOCK::default();
        let status = unsafe {
            NtSetInformationFile(
                file.as_raw_handle() as HANDLE,
                &mut status_block,
                information,
                length as u32,
                class,
            )
        };
        if status < 0 {
            return Err(rejected("Inbox anchored mutation was rejected"));
        }
        Ok(())
    }

    pub(super) fn exists(parent: &File, name: &OsStr) -> std::io::Result<bool> {
        match open_relative(
            parent,
            name,
            false,
            FILE_OPEN,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        ) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[cfg(windows)]
impl InboxAnchor {
    pub(super) fn acquire(dir: &Path) -> std::io::Result<Self> {
        let leaf = dir
            .file_name()
            .ok_or_else(|| invalid("Inbox has no name"))?
            .to_owned();
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let parent = platform::open_canonical_dir(&platform::canonical_parent(parent)?)?;
        let anchored = match platform::open_dir(&parent, &leaf) {
            Ok(anchored) => anchored,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                platform::create_dir(&parent, &leaf)?;
                platform::open_dir(&parent, &leaf)?
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            root: dir.to_owned(),
            dir: anchored,
        })
    }

    pub(super) fn open_lock(&self, name: &str) -> std::io::Result<File> {
        let name = std::ffi::OsStr::new(name);
        let mut last = None;
        for _ in 0..LOCK_ATTEMPTS {
            match platform::open_file(&self.dir, name) {
                Ok(file) => return Ok(file),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match platform::create_file(&self.dir, name) {
                        Ok(file) => return Ok(file),
                        Err(create)
                            if matches!(
                                create.kind(),
                                ErrorKind::AlreadyExists | ErrorKind::NotFound
                            ) =>
                        {
                            last = Some(create);
                        }
                        Err(create) => return Err(create),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            std::io::Error::new(
                ErrorKind::WouldBlock,
                "Inbox operation lock could not be opened",
            )
        }))
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

    pub(super) fn create_dir_all(&self, _: &Path) -> std::io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn create_exclusive(&self, _: &Path) -> std::io::Result<File> {
        Err(unsupported())
    }

    pub(super) fn rename(&self, _: &Path, _: &Path) -> std::io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn remove_file(&self, _: &Path) -> std::io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn entry_exists(&self, _: &Path) -> std::io::Result<bool> {
        Err(unsupported())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn unsupported() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::Unsupported,
        "Inbox anchoring requires retained no-follow directories",
    )
}
