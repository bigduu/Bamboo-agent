//! Private snapshot reader: actual reads remain bound to opened directory/file fds.
//! Compiled only on the two supported platforms, with no path-check/reopen step.

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path};

use bamboo_domain::{ActorSnapshotError as Error, ActorSnapshotLimits};

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
        if !path.is_absolute()
            || path
                .components()
                .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
        {
            return Err(Error::UnsupportedAuthority);
        }
        let fd = unsafe {
            libc::open(
                c"/".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(Error::StorageUnavailable);
        }
        // SAFETY: each successful open returns a fresh owned fd.
        let mut directory = Self(unsafe { File::from_raw_fd(fd) });
        for component in path.components() {
            if let Component::Normal(name) = component {
                directory = directory
                    .child(name)
                    .map_err(|error| match error {
                        Error::InconsistentAuthority => Error::UnsupportedAuthority,
                        other => other,
                    })?
                    .ok_or(Error::NotFound)?;
            }
        }
        Ok(directory)
    }

    pub fn child(&self, name: &OsStr) -> Result<Option<Self>, Error> {
        open_relative(&self.0, name, true).map(|file| file.map(Self))
    }

    pub fn entries(&self, budget: &mut ReadBudget) -> Result<Vec<OsString>, Error> {
        let fd = self
            .0
            .try_clone()
            .map_err(|_| Error::StorageUnavailable)?
            .into_raw_fd();
        // fdopendir owns the duplicate; the capability used for reads is retained.
        let pointer = unsafe { libc::fdopendir(fd) };
        if pointer.is_null() {
            unsafe { libc::close(fd) };
            return Err(Error::StorageUnavailable);
        }
        struct Stream(*mut libc::DIR);
        impl Drop for Stream {
            fn drop(&mut self) {
                unsafe { libc::closedir(self.0) };
            }
        }
        let stream = Stream(pointer);
        let mut entries = Vec::new();
        loop {
            unsafe { *errno_pointer() = 0 };
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                if unsafe { *errno_pointer() } != 0 {
                    return Err(Error::StorageUnavailable);
                }
                break;
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            budget.entries += 1;
            if budget.entries > budget.limits.directory_entries {
                return Err(Error::BudgetExceeded);
            }
            entries.push(OsString::from_vec(name.to_vec()));
        }
        entries.sort();
        Ok(entries)
    }

    /// Main history is deliberately unobserved. All reads use this one opened
    /// regular-file FD, without a buffered reader, suffix probe or reopen.
    pub fn read_main_section(&self, budget: &mut ReadBudget) -> Result<Option<Vec<u8>>, Error> {
        use super::compact_main::{HEADER_BYTES, PREFIX, SECTION_CAP};
        budget.reads += 1;
        if budget.reads > budget.limits.file_reads {
            return Err(Error::BudgetExceeded);
        }
        let Some(mut file) = open_relative(&self.0, OsStr::new("session.json"), false)? else {
            return Ok(None);
        };
        if !file
            .metadata()
            .map_err(|_| Error::StorageUnavailable)?
            .is_file()
        {
            return Err(Error::InconsistentAuthority);
        }
        #[cfg(test)]
        let hook = super::actor_snapshot_tests::main_read_hook(&file);
        #[cfg(test)]
        if let Some(hook) = &hook {
            hook.opened();
        }
        // Capture the remaining budget before any header debit. Comparing the
        // total section against the post-header remainder would bill it twice.
        let max = SECTION_CAP.min(budget.limits.file_bytes).min(
            budget
                .limits
                .aggregate_read_bytes
                .saturating_sub(budget.bytes),
        );
        #[cfg(test)]
        let before = budget.bytes;
        let result = (|| {
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
            // The caller decodes this exact section once, including its close.
            Ok(Some(bytes))
        })();
        #[cfg(test)]
        if let Some(hook) = &hook {
            hook.finished(&mut file, budget.bytes - before);
        }
        result
    }

    pub fn read(
        &self,
        name: &str,
        file_limit: usize,
        budget: &mut ReadBudget,
    ) -> Result<Option<Vec<u8>>, Error> {
        // Bound attempted opens too, including absent optional evidence.
        budget.reads += 1;
        if budget.reads > budget.limits.file_reads {
            return Err(Error::BudgetExceeded);
        }
        let Some(file) = open_relative(&self.0, OsStr::new(name), false)? else {
            return Ok(None);
        };
        let metadata = file.metadata().map_err(|_| Error::StorageUnavailable)?;
        if !metadata.is_file() {
            return Err(Error::InconsistentAuthority);
        }
        let max = file_limit.min(budget.limits.file_bytes).min(
            budget
                .limits
                .aggregate_read_bytes
                .saturating_sub(budget.bytes),
        );
        if metadata.len() > max as u64 {
            return Err(Error::BudgetExceeded);
        }
        read_content(file, max, budget).map(Some)
    }
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
    // One overflow probe catches post-stat growth. It is counted as actual I/O.
    let result = file.by_ref().take(max as u64 + 1).read_to_end(&mut bytes);
    budget.bytes += bytes.len();
    if bytes.len() > max || budget.bytes > budget.limits.aggregate_read_bytes {
        return Err(Error::BudgetExceeded);
    }
    result.map_err(|_| Error::StorageUnavailable)?;
    Ok(bytes)
}

fn open_relative(parent: &File, name: &OsStr, directory: bool) -> Result<Option<File>, Error> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(Error::InconsistentAuthority);
    }
    let name = CString::new(bytes).map_err(|_| Error::InconsistentAuthority)?;
    let mut flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
    if directory {
        flags |= libc::O_DIRECTORY;
    }
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(None),
            Some(libc::ELOOP | libc::ENOTDIR | libc::EINVAL) => Err(Error::InconsistentAuthority),
            _ => Err(Error::StorageUnavailable),
        };
    }
    Ok(Some(unsafe { File::from_raw_fd(fd) }))
}

#[cfg(target_os = "linux")]
unsafe fn errno_pointer() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
#[cfg(target_os = "macos")]
unsafe fn errno_pointer() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
