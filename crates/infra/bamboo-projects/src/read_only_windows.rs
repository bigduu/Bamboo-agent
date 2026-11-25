//! Read existing local-drive Project names relative to owned directory handles.
use super::{invalid, ProjectStoreResult};
use std::{
    ffi::OsStr,
    fs::File,
    io::{self, ErrorKind},
    mem::size_of,
    ops::Deref,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Component, Path, Prefix},
};
use windows_sys::Wdk::{
    Foundation::OBJECT_ATTRIBUTES,
    Storage::FileSystem::{
        NtCreateFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN,
        FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
    },
};
use windows_sys::Win32::{
    Foundation::{
        HANDLE, INVALID_HANDLE_VALUE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, STATUS_NO_SUCH_FILE,
        STATUS_OBJECT_NAME_NOT_FOUND, STATUS_OBJECT_PATH_NOT_FOUND, UNICODE_STRING,
    },
    Storage::FileSystem::{
        CreateFileW, FileAttributeTagInfo, FileIdInfo, FileStandardInfo, GetDriveTypeW,
        GetFileInformationByHandleEx, GetFileType, GetFinalPathNameByHandleW,
        FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_ATTRIBUTE_TAG_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_GENERIC_READ, FILE_ID_INFO, FILE_INFO_BY_HANDLE_CLASS, FILE_LIST_DIRECTORY,
        FILE_NAME_NORMALIZED, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, FILE_STANDARD_INFO, FILE_TYPE_DISK, OPEN_EXISTING, SYNCHRONIZE,
        VOLUME_NAME_DOS,
    },
    System::{
        WindowsProgramming::{DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOVABLE},
        IO::IO_STATUS_BLOCK,
    },
};

const DIRECTORY_READ: u32 = FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
const SHARE: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

#[derive(PartialEq, Eq)]
struct FileIdentity(u64, [u8; 16]);

pub(super) struct Root {
    ancestry: Vec<(File, FileIdentity)>,
}

impl Deref for Root {
    type Target = File;

    fn deref(&self) -> &File {
        &self.ancestry.last().expect("drive root is retained").0
    }
}

fn unsupported_root() -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        "read-only Project proof requires a local drive root",
    )
}

// Callers pair each information class with its corresponding output type.
fn file_info<T: Default>(file: &File, class: FILE_INFO_BY_HANDLE_CLASS) -> io::Result<T> {
    let mut value = T::default();
    // SAFETY: file owns a live handle and the output size matches the C struct
    // identified by class at each call site.
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            class,
            (&mut value as *mut T).cast(),
            size_of::<T>() as u32,
        )
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

fn validate_object(file: &File, directory: bool) -> ProjectStoreResult<()> {
    // SAFETY: file owns a live handle.
    if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
        return Err(invalid());
    }
    let attrs = file_info::<FILE_ATTRIBUTE_TAG_INFO>(file, FileAttributeTagInfo)?;
    let flags = attrs.FileAttributes;
    if flags & (FILE_ATTRIBUTE_DEVICE | FILE_ATTRIBUTE_REPARSE_POINT) != 0
        || (flags & FILE_ATTRIBUTE_DIRECTORY != 0) != directory
    {
        return Err(invalid());
    }
    if !directory {
        let standard = file_info::<FILE_STANDARD_INFO>(file, FileStandardInfo)?;
        if standard.Directory || standard.DeletePending || standard.NumberOfLinks != 1 {
            return Err(invalid());
        }
    }
    Ok(())
}

fn identity(file: &File, directory: bool) -> ProjectStoreResult<FileIdentity> {
    validate_object(file, directory)?;
    let result = file_info::<FILE_ID_INFO>(file, FileIdInfo)?;
    Ok(FileIdentity(
        result.VolumeSerialNumber,
        result.FileId.Identifier,
    ))
}

fn open_component(parent: &File, component: &OsStr, directory: bool) -> ProjectStoreResult<File> {
    let units: Vec<u16> = component.encode_wide().collect();
    if units.is_empty()
        || units == [u16::from(b'.')]
        || units == [u16::from(b'.'), u16::from(b'.')]
        || units
            .iter()
            .any(|&unit| matches!(unit, 0 | 0x2f | 0x3a | 0x5c))
    {
        return Err(invalid());
    }
    let length = units
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|length| u16::try_from(length).ok())
        .ok_or_else(invalid)?;
    let name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: units.as_ptr().cast_mut(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &name,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        SecurityDescriptor: std::ptr::null(),
        SecurityQualityOfService: std::ptr::null(),
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut handle: HANDLE = std::ptr::null_mut();
    let (access, kind) = if directory {
        (DIRECTORY_READ, FILE_DIRECTORY_FILE)
    } else {
        (FILE_GENERIC_READ, FILE_NON_DIRECTORY_FILE)
    };
    // SAFETY: the live parent, component and buffers outlive this synchronous
    // open. FILE_OPEN only opens an existing literal child.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            access,
            &attributes,
            &mut status_block,
            std::ptr::null(),
            0,
            SHARE,
            FILE_OPEN,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT | kind,
            std::ptr::null(),
            0,
        )
    };
    if matches!(
        status,
        STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND | STATUS_NO_SUCH_FILE
    ) {
        return Err(io::Error::new(ErrorKind::NotFound, "Project proof name is absent").into());
    }
    if status < 0 || handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(invalid());
    }
    // SAFETY: the successful call returned a new owned handle.
    let file = unsafe { File::from_raw_handle(handle) };
    validate_object(&file, directory)?;
    Ok(file)
}

pub(super) fn open_at(parent: &File, component: &str, directory: bool) -> ProjectStoreResult<File> {
    open_component(parent, OsStr::new(component), directory)
}

pub(super) fn check_at(
    parent: &File,
    component: &str,
    file: &File,
    directory: bool,
) -> ProjectStoreResult<()> {
    let named = open_at(parent, component, directory)?;
    if identity(&named, directory)? != identity(file, directory)? {
        return Err(invalid());
    }
    Ok(())
}

fn open_drive(disk: u8) -> ProjectStoreResult<File> {
    let dos = [u16::from(disk), u16::from(b':'), u16::from(b'\\'), 0];
    // SAFETY: dos is a NUL-terminated drive root.
    if !matches!(
        unsafe { GetDriveTypeW(dos.as_ptr()) },
        DRIVE_FIXED | DRIVE_REMOVABLE | DRIVE_RAMDISK
    ) {
        return Err(unsupported_root().into());
    }
    // SAFETY: dos and the other arguments are valid for this synchronous open.
    let handle = unsafe {
        CreateFileW(
            dos.as_ptr(),
            DIRECTORY_READ,
            SHARE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: the successful call returned a new owned handle.
    let file = unsafe { File::from_raw_handle(handle) };
    let mut final_name = [0_u16; 16];
    // SAFETY: the file and writable output buffer live throughout the call.
    let len = unsafe {
        GetFinalPathNameByHandleW(
            file.as_raw_handle(),
            final_name.as_mut_ptr(),
            final_name.len() as u32,
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
        )
    } as usize;
    // Reject drive aliases that open a directory below a volume root.
    let final_name = final_name.get(..len).unwrap_or_default();
    let final_name = final_name
        .strip_suffix(&[u16::from(b'\\')])
        .unwrap_or(final_name);
    let prefix = [b'\\', b'\\', b'?', b'\\'].map(u16::from);
    if let [head @ .., drive, colon] = final_name {
        if head == prefix
            && u8::try_from(*drive).is_ok_and(|actual| actual.eq_ignore_ascii_case(&disk))
            && *colon == u16::from(b':')
        {
            return Ok(file);
        }
    }
    Err(unsupported_root().into())
}

pub(super) fn root_at(path: &Path) -> ProjectStoreResult<Root> {
    let mut components = path.components();
    let Some(Component::Prefix(prefix)) = components.next() else {
        return Err(unsupported_root().into());
    };
    let disk = match prefix.kind() {
        Prefix::Disk(disk) | Prefix::VerbatimDisk(disk) => disk,
        _ => return Err(unsupported_root().into()),
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(unsupported_root().into());
    }
    let drive = open_drive(disk)?;
    let volume = identity(&drive, true)?;
    let mut root = Root {
        ancestry: vec![(drive, volume)],
    };
    for component in components {
        let Component::Normal(name) = component else {
            return Err(unsupported_root().into());
        };
        let data = open_component(&root, name, true)?;
        let entry = identity(&data, true)?;
        root.ancestry.push((data, entry));
    }
    Ok(root)
}

pub(super) fn check_root(root: &Root, path: &Path) -> ProjectStoreResult<()> {
    let current = root_at(path)?;
    if root.ancestry.len() != current.ancestry.len()
        || root
            .ancestry
            .iter()
            .zip(&current.ancestry)
            .any(|((_, opened), (_, named))| opened != named)
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lock_exclusive, ProjectManifest, ProjectStore, ProjectStoreError};
    use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};

    fn fixture() -> (tempfile::TempDir, ProjectStore, ProjectManifest) {
        let root = tempfile::tempdir().unwrap();
        let store = ProjectStore::open(root.path()).unwrap();
        let project = store.create("Read-only proof", None).unwrap();
        (root, store, project)
    }

    fn durable_files(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        fn walk(root: &Path, at: &Path, output: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
            for entry in fs::read_dir(at).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                let relative = path.strip_prefix(root).unwrap().to_owned();
                if entry.file_type().unwrap().is_dir() {
                    output.insert(relative, None);
                    walk(root, &path, output);
                } else {
                    output.insert(relative, Some(fs::read(path).unwrap()));
                }
            }
        }
        let mut output = BTreeMap::new();
        walk(root, root, &mut output);
        output
    }

    #[test]
    fn current_project_proof_preserves_all_bytes() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("MiXeD-Project")).unwrap();
        let store = ProjectStore::open(root.path().join("mixed-project")).unwrap();
        let project = store.create("Read-only proof", None).unwrap();
        let home = store.paths.project_home(&project.id);
        fs::write(home.join(".leftover.tmp.fixture"), b"retain").unwrap();
        let before = durable_files(root.path());
        for _ in 0..3 {
            assert_eq!(store.get_read_only(&project.id).unwrap(), project);
        }
        assert_eq!(durable_files(root.path()), before);
    }

    #[test]
    fn project_proof_respects_writer_lock_without_creating_it() {
        let (root, store, project) = fixture();
        let lock = store.paths.project_home(&project.id).join(".project.lock");
        let before = durable_files(root.path());
        let writer = lock_exclusive(lock.clone()).unwrap();
        assert!(matches!(
            store.get_read_only(&project.id),
            Err(ProjectStoreError::Io(error))
                if error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
        ));
        drop(writer);
        assert_eq!(durable_files(root.path()), before);
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);

        fs::remove_file(&lock).unwrap();
        let before = durable_files(root.path());
        assert!(store.get_read_only(&project.id).is_err());
        assert!(!lock.exists());
        assert_eq!(durable_files(root.path()), before);
    }

    #[test]
    fn project_proof_rejects_ancestor_junction() {
        let root = tempfile::tempdir().unwrap();
        let ancestor = root.path().join("ancestor");
        let retained = root.path().join("retained-ancestor");
        let store = ProjectStore::open(ancestor.join("data")).unwrap();
        let project = store.create("Ancestor proof", None).unwrap();
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);

        fs::rename(&ancestor, &retained).unwrap();
        let output = Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&ancestor)
            .arg(&retained)
            .output()
            .unwrap();
        assert!(output.status.success(), "junction creation: {output:?}");
        let outside = durable_files(&retained);
        assert!(store.get_read_only(&project.id).is_err());
        assert_eq!(durable_files(&retained), outside);
        fs::remove_dir(&ancestor).unwrap();
        fs::rename(&retained, &ancestor).unwrap();
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);
    }

    #[test]
    fn project_proof_rejects_replaced_or_hardlinked_primary() {
        let (_root, store, project) = fixture();
        let home = store.paths.project_home(&project.id);
        let primary = home.join("project.json");
        let retained = home.join("retained-project.json");
        let original = fs::read(&primary).unwrap();
        assert!(super::super::read(&store, &project.id, || {
            fs::rename(&primary, &retained).unwrap();
            fs::copy(&retained, &primary).unwrap();
        })
        .is_err());
        assert_eq!(fs::read(&primary).unwrap(), original);
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);

        fs::remove_file(&retained).unwrap();
        fs::hard_link(&primary, &retained).unwrap();
        assert!(store.get_read_only(&project.id).is_err());
        fs::remove_file(&retained).unwrap();
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);
    }

    #[test]
    fn project_proof_rejects_missing_or_stale_floor() {
        let (root, store, project) = fixture();
        let floor = store.paths.manifest_revision_path(&project.id);
        let original = fs::read(&floor).unwrap();
        fs::remove_file(&floor).unwrap();
        let before = durable_files(root.path());
        assert!(store.get_read_only(&project.id).is_err());
        assert_eq!(durable_files(root.path()), before);

        let stale = format!("{}\n", project.revision + 1);
        fs::write(&floor, &stale).unwrap();
        let before = durable_files(root.path());
        assert!(store.get_read_only(&project.id).is_err());
        assert_eq!(durable_files(root.path()), before);
        fs::write(&floor, original).unwrap();
        assert_eq!(store.get_read_only(&project.id).unwrap(), project);
    }
}
