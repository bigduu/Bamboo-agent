use std::ffi::{OsStr, OsString};
use std::path::Path;

use super::{NamedAgentDiagnosticCode as Code, NamedAgentLimits, ScanBudget};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod supported {
    use std::ffi::{CStr, CString};
    use std::fs::File;
    use std::io::{self, Read};
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::path::Component;

    use super::*;

    pub(crate) struct AgentDirectory(File);

    impl AgentDirectory {
        pub(crate) fn open(data_root: &Path) -> Result<Option<Self>, Code> {
            if !data_root.is_absolute()
                || data_root
                    .components()
                    .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
            {
                return Err(Code::PathEscape);
            }
            // Begin at the filesystem root and retain each opened directory
            // capability. No checked path is ever reopened for a content read.
            let fd = unsafe {
                libc::open(
                    c"/".as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(Code::RootUnavailable);
            }
            // SAFETY: open returned a fresh owned descriptor.
            let mut directory = unsafe { File::from_raw_fd(fd) };
            for component in data_root.components() {
                if let Component::Normal(name) = component {
                    directory = open_relative(&directory, name, true)
                        .map_err(|error| io_code(&error, Code::RootUnavailable))?;
                }
            }
            match open_relative(&directory, OsStr::new("agents"), true) {
                Ok(directory) => Ok(Some(Self(directory))),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(io_code(&error, Code::RootUnavailable)),
            }
        }

        pub(crate) fn candidates(
            &self,
            limits: NamedAgentLimits,
            budget: &mut ScanBudget,
        ) -> Result<Vec<OsString>, Code> {
            // fdopendir takes ownership of its fd; enumerate a duplicate of the
            // retained directory, never the configured path or /proc fd paths.
            let fd = self
                .0
                .try_clone()
                .map_err(|_| Code::ReadFailed)?
                .into_raw_fd();
            let pointer = unsafe { libc::fdopendir(fd) };
            if pointer.is_null() {
                unsafe { libc::close(fd) };
                return Err(Code::ReadFailed);
            }
            struct DirectoryStream(*mut libc::DIR);
            impl Drop for DirectoryStream {
                fn drop(&mut self) {
                    unsafe { libc::closedir(self.0) };
                }
            }
            let stream = DirectoryStream(pointer);
            let mut candidates = Vec::new();
            loop {
                // readdir's null result is either end-of-stream or an error.
                // Clear errno for this thread immediately before the call.
                unsafe { *errno_pointer() = 0 };
                let entry = unsafe { libc::readdir(stream.0) };
                if entry.is_null() {
                    if unsafe { *errno_pointer() } != 0 {
                        return Err(Code::ReadFailed);
                    }
                    break;
                }
                // SAFETY: d_name is a NUL-terminated name valid until readdir.
                let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
                if name == b"." || name == b".." {
                    continue;
                }
                budget.entries += 1;
                if budget.entries > limits.max_scan_entries {
                    return Err(Code::ScanLimitExceeded);
                }
                if name.ends_with(b".md") {
                    candidates.push(OsString::from_vec(name.to_vec()));
                    budget.candidates += 1;
                    if budget.candidates > limits.max_candidates {
                        return Err(Code::CandidateLimitExceeded);
                    }
                }
            }
            candidates.sort();
            Ok(candidates)
        }

        pub(crate) fn read(
            &self,
            name: &OsStr,
            max_bytes: usize,
            remaining_bytes: usize,
        ) -> Result<Vec<u8>, (Code, usize)> {
            let file = open_relative(&self.0, name, false)
                .map_err(|error| (io_code(&error, Code::ReadFailed), 0))?;
            let metadata = file.metadata().map_err(|_| (Code::ReadFailed, 0))?;
            if !metadata.is_file() {
                return Err((Code::NotRegularFile, 0));
            }
            if metadata.len() > max_bytes as u64 {
                return Err((Code::FileTooLarge, 0));
            }
            if metadata.len() > remaining_bytes as u64 {
                return Err((Code::AggregateLimitExceeded, 0));
            }
            read_content(file, max_bytes.min(remaining_bytes))
        }
    }

    pub(crate) fn read_content(mut file: File, max_bytes: usize) -> Result<Vec<u8>, (Code, usize)> {
        let mut bytes = Vec::new();
        // Read a bounded probe beyond the budget to detect growth after stat.
        file.by_ref()
            .take(max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| (Code::ReadFailed, bytes.len()))?;
        Ok(bytes)
    }

    fn open_relative(directory: &File, name: &OsStr, is_directory: bool) -> io::Result<File> {
        // Names from enumeration are still validated before crossing the fd API.
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let name = CString::new(bytes).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
        let flags = if is_directory {
            flags | libc::O_DIRECTORY
        } else {
            flags
        };
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful openat returns a fresh owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn io_code(error: &io::Error, fallback: Code) -> Code {
        match error.raw_os_error() {
            Some(libc::ELOOP | libc::ENOTDIR) => Code::UnsafePath,
            Some(libc::EINVAL) => Code::PathEscape,
            _ => fallback,
        }
    }

    #[cfg(target_os = "linux")]
    unsafe fn errno_pointer() -> *mut libc::c_int {
        unsafe { libc::__errno_location() }
    }

    #[cfg(target_os = "macos")]
    unsafe fn errno_pointer() -> *mut libc::c_int {
        unsafe { libc::__error() }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) use supported::AgentDirectory;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) struct AgentDirectory;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl AgentDirectory {
    pub(super) fn open(_: &Path) -> Result<Option<Self>, Code> {
        Err(Code::UnsupportedPlatform)
    }

    pub(super) fn candidates(
        &self,
        _: NamedAgentLimits,
        _: &mut ScanBudget,
    ) -> Result<Vec<OsString>, Code> {
        Err(Code::UnsupportedPlatform)
    }

    pub(super) fn read(&self, _: &OsStr, _: usize, _: usize) -> Result<Vec<u8>, (Code, usize)> {
        Err((Code::UnsupportedPlatform, 0))
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(super) use supported::read_content;
