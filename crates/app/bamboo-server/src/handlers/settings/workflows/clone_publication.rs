use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use bamboo_skills::clone_publication::{
    clone_marker_name, CloneNodeIdentity, ClonePublicationMarker, ClonePublicationPhase,
    CLONE_MARKER_SCHEMA, MAX_CLONE_BUNDLE_BYTES, MAX_CLONE_BUNDLE_FILES, MAX_CLONE_MARKER_BYTES,
    MAX_CLONE_RELATIVE_PATH_BYTES,
};
use bamboo_skills::store::builtin::{builtin_clone_bundle_digest, BuiltinSkillFile};
#[cfg(unix)]
use cap_std::ambient_authority;
use cap_std::fs::{Dir as CapDir, File as CapFile, OpenOptions as CapOpenOptions};

const CLONE_TRANSACTION_DIRECTORY: &str = ".workflow-clone-txn";

#[derive(Debug)]
pub(super) enum ClonePublicationError {
    Conflict(&'static str),
    Io(std::io::Error),
    Internal(&'static str),
}

impl From<std::io::Error> for ClonePublicationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClonePublicationReceipt {
    pub target_identity: CloneNodeIdentity,
}

struct ClonePublicationRoot {
    trusted_root: CapDir,
    skills_dir: CapDir,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum PublicationFault {
    #[default]
    None,
    #[cfg(test)]
    StopAfterPrepared,
    #[cfg(test)]
    StopAfterStaged,
    #[cfg(test)]
    StopAfterRename,
    #[cfg(test)]
    RenameReportsErrorAfterMove,
    #[cfg(test)]
    TargetDirectoryBeforeRename,
}

#[cfg(test)]
fn injected_failure() -> ClonePublicationError {
    ClonePublicationError::Io(std::io::Error::other(
        "injected Workflow clone publication failure",
    ))
}

#[cfg(unix)]
fn cap_node_identity(metadata: &cap_std::fs::Metadata) -> CloneNodeIdentity {
    use cap_std::fs::MetadataExt;

    CloneNodeIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(windows)]
fn cap_node_identity(metadata: &cap_std::fs::Metadata) -> CloneNodeIdentity {
    use cap_fs_ext::MetadataExt;

    CloneNodeIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(any(unix, windows)))]
fn cap_node_identity(_metadata: &cap_std::fs::Metadata) -> CloneNodeIdentity {
    CloneNodeIdentity {
        device: 0,
        inode: 0,
    }
}

fn cap_entries_match(left: &cap_std::fs::Metadata, right: &cap_std::fs::Metadata) -> bool {
    cap_node_identity(left) == cap_node_identity(right)
}

#[cfg(unix)]
fn cap_regular_file_has_single_link(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt;

    metadata.nlink() == 1
}

#[cfg(windows)]
fn cap_regular_file_has_single_link(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_fs_ext::MetadataExt;

    metadata.nlink() == 1
}

#[cfg(not(any(unix, windows)))]
fn cap_regular_file_has_single_link(_metadata: &cap_std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn open_io_cap_directory(directory: &CapDir) -> Result<CapFile, ClonePublicationError> {
    // cap-std may represent directories with O_PATH on Linux. Such a
    // descriptor is valid as an *at syscall authority, but fsync/fchmod
    // reject it with EBADF. Open `.` through the pinned capability to get an
    // I/O-capable handle while preserving the same directory identity.
    let opened_file = directory.open(".")?;
    let opened = opened_file.metadata()?;
    if !opened.is_dir()
        || cap_node_identity(&opened) != cap_node_identity(&directory.dir_metadata()?)
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone directory authority changed",
        ));
    }
    Ok(opened_file)
}

#[cfg(unix)]
fn set_cap_directory_mode(directory: &CapDir, mode: u32) -> Result<(), ClonePublicationError> {
    use cap_std::fs::PermissionsExt;

    open_io_cap_directory(directory)?.set_permissions(cap_std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn sync_cap_directory(directory: &CapDir) -> Result<(), ClonePublicationError> {
    #[cfg(unix)]
    open_io_cap_directory(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn open_expected_real_child_dir(
    parent: &CapDir,
    name: &Path,
    expected: &cap_std::fs::Metadata,
) -> Result<CapDir, ClonePublicationError> {
    if expected.file_type().is_symlink() || !expected.is_dir() {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone directory authority is invalid",
        ));
    }
    let child = parent.open_dir(name)?;
    let opened = child.dir_metadata()?;
    let after = parent.symlink_metadata(name)?;
    if after.file_type().is_symlink()
        || !after.is_dir()
        || !cap_entries_match(expected, &opened)
        || !cap_entries_match(&after, &opened)
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone directory changed during validation",
        ));
    }
    Ok(child)
}

fn open_real_child_dir(parent: &CapDir, name: &Path) -> Result<CapDir, ClonePublicationError> {
    let expected = parent.symlink_metadata(name)?;
    open_expected_real_child_dir(parent, name, &expected)
}

fn open_optional_real_child_dir(
    parent: &CapDir,
    name: &Path,
) -> Result<Option<CapDir>, ClonePublicationError> {
    match parent.symlink_metadata(name) {
        Ok(expected) => open_expected_real_child_dir(parent, name, &expected).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn ensure_real_child_dir(parent: &CapDir, name: &Path) -> Result<CapDir, ClonePublicationError> {
    let expected = match parent.symlink_metadata(name) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match parent.create_dir(name) {
                Ok(()) => sync_cap_directory(parent)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            parent.symlink_metadata(name)?
        }
        Err(error) => return Err(error.into()),
    };
    open_expected_real_child_dir(parent, name, &expected)
}

fn ensure_open_child_is_still_named(
    parent: &CapDir,
    name: &Path,
    child: &CapDir,
) -> Result<(), ClonePublicationError> {
    let named = parent.symlink_metadata(name)?;
    let opened = child.dir_metadata()?;
    if named.file_type().is_symlink() || !named.is_dir() || !cap_entries_match(&named, &opened) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone directory changed while it was in use",
        ));
    }
    Ok(())
}

fn ensure_cap_file_is_still_named(
    parent: &CapDir,
    name: &Path,
    opened: &cap_std::fs::Metadata,
) -> Result<(), ClonePublicationError> {
    let named = parent.symlink_metadata(name)?;
    if named.file_type().is_symlink() || !named.is_file() || !cap_entries_match(&named, opened) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone file changed while it was in use",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn open_windows_root_no_follow(root: &Path) -> std::io::Result<CapDir> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options.open(root)?;
    let metadata = file.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Workflow clone root must be a real directory",
        ));
    }
    Ok(CapDir::from_std_file(file))
}

fn open_publication_root(root: &Path) -> Result<ClonePublicationRoot, ClonePublicationError> {
    let initial = std::fs::symlink_metadata(root)?;
    if initial.file_type().is_symlink() || !initial.is_dir() {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone root must be a real directory",
        ));
    }
    #[cfg(windows)]
    let trusted_root = {
        let trusted_root = open_windows_root_no_follow(root)?;
        let expected_identity = cap_node_identity(&trusted_root.dir_metadata()?);
        let named_again = open_windows_root_no_follow(root)?;
        if cap_node_identity(&named_again.dir_metadata()?) != expected_identity {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone root changed during validation",
            ));
        }
        trusted_root
    };
    #[cfg(unix)]
    let trusted_root = {
        use std::os::unix::fs::MetadataExt;

        let expected_identity = CloneNodeIdentity {
            device: initial.dev(),
            inode: initial.ino(),
        };
        let canonical = std::fs::canonicalize(root)?;
        let trusted_root = CapDir::open_ambient_dir(&canonical, ambient_authority())?;
        let after = std::fs::symlink_metadata(root)?;
        if after.file_type().is_symlink()
            || !after.is_dir()
            || cap_node_identity(&trusted_root.dir_metadata()?) != expected_identity
            || after.dev() != expected_identity.device
            || after.ino() != expected_identity.inode
        {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone root changed during validation",
            ));
        }
        trusted_root
    };
    #[cfg(not(any(unix, windows)))]
    let trusted_root: CapDir = return Err(ClonePublicationError::Internal(
        "Workflow clone root identity is unavailable",
    ));
    let skills_dir = ensure_real_child_dir(&trusted_root, Path::new("skills"))?;
    Ok(ClonePublicationRoot {
        trusted_root,
        skills_dir,
    })
}

fn open_transaction_parent(root: &ClonePublicationRoot) -> Result<CapDir, ClonePublicationError> {
    let transaction =
        ensure_real_child_dir(&root.trusted_root, Path::new(CLONE_TRANSACTION_DIRECTORY))?;
    #[cfg(unix)]
    set_cap_directory_mode(&transaction, 0o700)?;
    ensure_open_child_is_still_named(
        &root.trusted_root,
        Path::new(CLONE_TRANSACTION_DIRECTORY),
        &transaction,
    )?;
    if cap_node_identity(&transaction.dir_metadata()?).device
        != cap_node_identity(&root.skills_dir.dir_metadata()?).device
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone atomic publication is unavailable across filesystems",
        ));
    }
    Ok(transaction)
}

fn checked_relative_path(path: &str) -> Result<PathBuf, ClonePublicationError> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.as_os_str().as_encoded_bytes().len() > MAX_CLONE_RELATIVE_PATH_BYTES
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ClonePublicationError::Internal(
            "Embedded Workflow bundle contains an unsafe resource path",
        ));
    }
    Ok(path.to_path_buf())
}

pub(super) fn validate_clone_bundle(
    files: &BTreeMap<String, BuiltinSkillFile>,
) -> Result<(), ClonePublicationError> {
    if files.is_empty() || files.len() > MAX_CLONE_BUNDLE_FILES || !files.contains_key("SKILL.md") {
        return Err(ClonePublicationError::Internal(
            "Embedded Workflow bundle exceeds its publication limits",
        ));
    }
    let mut total = 0usize;
    for (relative, file) in files {
        checked_relative_path(relative)?;
        total = total
            .checked_add(file.bytes.len())
            .ok_or(ClonePublicationError::Internal(
                "Embedded Workflow bundle size overflow",
            ))?;
        if total > MAX_CLONE_BUNDLE_BYTES {
            return Err(ClonePublicationError::Internal(
                "Embedded Workflow bundle exceeds its publication limits",
            ));
        }
    }
    Ok(())
}

fn fresh_file_parent(
    root: &CapDir,
    relative: &Path,
    created_directories: &mut BTreeMap<PathBuf, CloneNodeIdentity>,
) -> Result<(CapDir, OsString), ClonePublicationError> {
    let file_name = relative.file_name().ok_or(ClonePublicationError::Internal(
        "Embedded Workflow bundle contains an unsafe resource path",
    ))?;
    let mut current = root.try_clone()?;
    let mut cumulative = PathBuf::new();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Component::Normal(component) = component else {
                return Err(ClonePublicationError::Internal(
                    "Embedded Workflow bundle contains an unsafe resource path",
                ));
            };
            cumulative.push(component);
            if let Some(expected) = created_directories.get(&cumulative) {
                let child = open_real_child_dir(&current, Path::new(component))?;
                if cap_node_identity(&child.dir_metadata()?) != *expected {
                    return Err(ClonePublicationError::Conflict(
                        "Workflow clone staging directory changed",
                    ));
                }
                current = child;
                continue;
            }
            match current.create_dir(component) {
                Ok(()) => sync_cap_directory(&current)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(ClonePublicationError::Conflict(
                        "Workflow clone staging directory was not exclusively created",
                    ));
                }
                Err(error) => return Err(error.into()),
            }
            let child = open_real_child_dir(&current, Path::new(component))?;
            #[cfg(unix)]
            set_cap_directory_mode(&child, 0o755)?;
            created_directories.insert(
                cumulative.clone(),
                cap_node_identity(&child.dir_metadata()?),
            );
            current = child;
        }
    }
    Ok((current, file_name.to_os_string()))
}

fn populate_stage(
    stage: &CapDir,
    files: &BTreeMap<String, BuiltinSkillFile>,
) -> Result<(), ClonePublicationError> {
    let mut directories = BTreeMap::new();
    for (relative, embedded) in files {
        let path = checked_relative_path(relative)?;
        let (parent, file_name) = fresh_file_parent(stage, &path, &mut directories)?;
        let mut options = CapOpenOptions::new();
        options.read(true).write(true).create_new(true);
        let mut output = parent.open_with(&file_name, &options)?;
        let initial = output.metadata()?;
        if !initial.is_file() || !cap_regular_file_has_single_link(&initial) {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone staging resource is not exclusively owned",
            ));
        }
        output.write_all(&embedded.bytes)?;
        output.sync_all()?;
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt;
            output.set_permissions(cap_std::fs::Permissions::from_mode(
                if embedded.executable { 0o755 } else { 0o644 },
            ))?;
            output.sync_all()?;
        }
        let final_metadata = output.metadata()?;
        if !cap_entries_match(&initial, &final_metadata)
            || !cap_regular_file_has_single_link(&final_metadata)
        {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone staging resource changed during write",
            ));
        }
        ensure_cap_file_is_still_named(&parent, Path::new(&file_name), &final_metadata)?;
        sync_cap_directory(&parent)?;
    }
    #[cfg(unix)]
    set_cap_directory_mode(stage, 0o755)?;
    Ok(())
}

fn read_clone_file_bounded(
    parent: &CapDir,
    name: &Path,
    max_len: usize,
) -> Result<(Vec<u8>, cap_std::fs::Metadata), ClonePublicationError> {
    let before = parent.symlink_metadata(name)?;
    if before.file_type().is_symlink() || !before.is_file() || before.len() > max_len as u64 {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone target resource is not an expected regular file",
        ));
    }
    let mut input = parent.open(name)?;
    let opened = input.metadata()?;
    if !opened.is_file() || !cap_entries_match(&before, &opened) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone target resource changed during validation",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    Read::by_ref(&mut input)
        .take(max_len.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    let after = parent.symlink_metadata(name)?;
    if bytes.len() > max_len
        || bytes.len() as u64 != before.len()
        || after.file_type().is_symlink()
        || !after.is_file()
        || !cap_entries_match(&after, &opened)
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone target resource changed during validation",
        ));
    }
    Ok((bytes, opened))
}

fn collect_clone_tree(
    directory: &CapDir,
    prefix: &Path,
    files: &BTreeMap<String, BuiltinSkillFile>,
    actual: &mut BTreeSet<String>,
) -> Result<(), ClonePublicationError> {
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone target contains a symbolic link",
            ));
        }
        let relative_path = prefix.join(&name);
        if file_type.is_file() {
            let relative = relative_path
                .to_str()
                .ok_or(ClonePublicationError::Conflict(
                    "Workflow clone target contains a non-UTF-8 resource",
                ))?
                .replace('\\', "/");
            let expected = files.get(&relative).ok_or(ClonePublicationError::Conflict(
                "Workflow clone target contains an unexpected resource",
            ))?;
            let (bytes, metadata) =
                read_clone_file_bounded(directory, Path::new(&name), expected.bytes.len())?;
            if bytes != expected.bytes || !cap_regular_file_has_single_link(&metadata) {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone target contains divergent resources",
                ));
            }
            #[cfg(unix)]
            let mode_matches = {
                use cap_std::fs::PermissionsExt;
                let expected_mode = if expected.executable { 0o755 } else { 0o644 };
                metadata.permissions().mode() & 0o7777 == expected_mode
            };
            #[cfg(not(unix))]
            let mode_matches = true;
            if !mode_matches || !actual.insert(relative) {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone target contains divergent resources",
                ));
            }
        } else if file_type.is_dir() {
            if !files
                .keys()
                .any(|path| Path::new(path).starts_with(&relative_path))
            {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone target contains an unexpected directory",
                ));
            }
            let child = open_real_child_dir(directory, Path::new(&name))?;
            collect_clone_tree(&child, &relative_path, files, actual)?;
        } else {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone target contains a non-regular resource",
            ));
        }
    }
    Ok(())
}

fn verify_clone_tree(
    target: &CapDir,
    files: &BTreeMap<String, BuiltinSkillFile>,
) -> Result<(), ClonePublicationError> {
    let mut actual = BTreeSet::new();
    collect_clone_tree(target, Path::new(""), files, &mut actual)?;
    if actual.len() != files.len() || files.keys().any(|path| !actual.contains(path)) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone target contains divergent resources",
        ));
    }
    Ok(())
}

fn sync_clone_tree_directories(root: &CapDir) -> Result<(), ClonePublicationError> {
    #[cfg(unix)]
    {
        for entry in root.entries()? {
            let entry = entry?;
            let name = entry.file_name();
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone target contains a symbolic link",
                ));
            }
            if file_type.is_dir() {
                let child = open_real_child_dir(root, Path::new(&name))?;
                sync_clone_tree_directories(&child)?;
            } else if !file_type.is_file() {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone target contains a non-regular resource",
                ));
            }
        }
        sync_cap_directory(root)?;
    }
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}

fn ensure_marker_is_still_named(
    root: &ClonePublicationRoot,
    marker_name: &Path,
    expected: CloneNodeIdentity,
) -> Result<(), ClonePublicationError> {
    let metadata = root.skills_dir.symlink_metadata(marker_name)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || cap_node_identity(&metadata) != expected
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone marker changed during publication",
        ));
    }
    Ok(())
}

fn write_marker(
    root: &ClonePublicationRoot,
    marker_name: &Path,
    marker_file: &mut CapFile,
    marker_identity: CloneNodeIdentity,
    marker: &ClonePublicationMarker,
) -> Result<(), ClonePublicationError> {
    let bytes = serde_json::to_vec(marker).map_err(|_| {
        ClonePublicationError::Internal("Workflow clone marker serialization failed")
    })?;
    if bytes.len() > MAX_CLONE_MARKER_BYTES {
        return Err(ClonePublicationError::Internal(
            "Workflow clone marker exceeds its durable limit",
        ));
    }
    ensure_marker_is_still_named(root, marker_name, marker_identity)?;
    let metadata = marker_file.metadata()?;
    if cap_node_identity(&metadata) != marker_identity
        || !cap_regular_file_has_single_link(&metadata)
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone marker authority changed",
        ));
    }
    marker_file.set_len(0)?;
    marker_file.seek(SeekFrom::Start(0))?;
    marker_file.write_all(&bytes)?;
    marker_file.sync_all()?;
    ensure_marker_is_still_named(root, marker_name, marker_identity)?;
    sync_cap_directory(&root.skills_dir)?;
    Ok(())
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "redox"
))]
fn rename_stage_noreplace(
    source_parent: &CapDir,
    source_name: &Path,
    destination_parent: &CapDir,
    destination_name: &Path,
    expected_source: CloneNodeIdentity,
) -> std::io::Result<()> {
    use std::os::fd::AsFd;

    let named = source_parent.symlink_metadata(source_name)?;
    if named.file_type().is_symlink()
        || !named.is_dir()
        || cap_node_identity(&named) != expected_source
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Workflow clone source identity changed before no-replace rename",
        ));
    }
    rustix::fs::renameat_with(
        source_parent.as_fd(),
        source_name,
        destination_parent.as_fd(),
        destination_name,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)
}

#[cfg(windows)]
fn rename_stage_noreplace(
    source_parent: &CapDir,
    source_name: &Path,
    destination_parent: &CapDir,
    destination_name: &Path,
    expected_source: CloneNodeIdentity,
) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FileRenameInformationEx, NtCreateFile, NtSetInformationFile, FILE_OPEN,
        FILE_OPEN_REPARSE_POINT, FILE_RENAME_INFORMATION, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        RtlNtStatusToDosError, HANDLE, OBJ_CASE_INSENSITIVE, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_LIST_DIRECTORY,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    let mut source_name_wide = source_name.as_os_str().encode_wide().collect::<Vec<_>>();
    if source_name_wide.is_empty()
        || source_name_wide.iter().any(|character| *character == 0)
        || source_name_wide.len() > (u16::MAX as usize / std::mem::size_of::<u16>())
    {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }
    let source_len = (source_name_wide.len() * std::mem::size_of::<u16>()) as u16;
    let source_unicode = UNICODE_STRING {
        Length: source_len,
        MaximumLength: source_len,
        Buffer: source_name_wide.as_mut_ptr(),
    };
    let source_attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: source_parent.as_raw_handle() as HANDLE,
        ObjectName: &source_unicode,
        Attributes: OBJ_CASE_INSENSITIVE,
        SecurityDescriptor: std::ptr::null(),
        SecurityQualityOfService: std::ptr::null(),
    };
    let mut source_handle: HANDLE = std::ptr::null_mut();
    let mut source_status = IO_STATUS_BLOCK::default();
    let opened = unsafe {
        NtCreateFile(
            &mut source_handle,
            DELETE | FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            &source_attributes,
            &mut source_status,
            std::ptr::null(),
            FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            std::ptr::null(),
            0,
        )
    };
    if opened < 0 {
        return Err(std::io::Error::from_raw_os_error(unsafe {
            RtlNtStatusToDosError(opened) as i32
        }));
    }
    let source = unsafe { std::fs::File::from_raw_handle(source_handle) };
    let metadata = source.metadata()?;
    if !metadata.is_dir()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || bamboo_skills::clone_publication::std_file_identity(&source) != Some(expected_source)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Workflow clone source identity changed before no-replace rename",
        ));
    }

    let target = destination_name
        .as_os_str()
        .encode_wide()
        .collect::<Vec<_>>();
    if target.is_empty()
        || target.iter().any(|character| *character == 0)
        || target.len() > (u32::MAX as usize / std::mem::size_of::<u16>())
    {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }
    let header_len = std::mem::size_of::<FILE_RENAME_INFORMATION>() - std::mem::size_of::<u16>();
    let target_bytes = target.len() * std::mem::size_of::<u16>();
    let mut buffer = vec![0_u8; header_len + target_bytes];
    let rename = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    unsafe {
        (*rename).Anonymous.Flags = 0;
        (*rename).RootDirectory = destination_parent.as_raw_handle() as HANDLE;
        (*rename).FileNameLength = target_bytes as u32;
        std::ptr::copy_nonoverlapping(
            target.as_ptr(),
            (*rename).FileName.as_mut_ptr(),
            target.len(),
        );
    }
    let mut rename_status = IO_STATUS_BLOCK::default();
    let renamed = unsafe {
        NtSetInformationFile(
            source.as_raw_handle() as HANDLE,
            &mut rename_status,
            buffer.as_ptr().cast(),
            buffer.len() as u32,
            FileRenameInformationEx,
        )
    };
    if renamed < 0 {
        return Err(std::io::Error::from_raw_os_error(unsafe {
            RtlNtStatusToDosError(renamed) as i32
        }));
    }
    Ok(())
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "redox"
)))]
fn rename_stage_noreplace(
    _source_parent: &CapDir,
    _source_name: &Path,
    _destination_parent: &CapDir,
    _destination_name: &Path,
    _expected_source: CloneNodeIdentity,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unavailable on this platform",
    ))
}

pub(super) fn publish_builtin_clone(
    trusted_root: &Path,
    workflow_id: &str,
    source_revision: u64,
    source_content_digest: &str,
    files: &BTreeMap<String, BuiltinSkillFile>,
) -> Result<ClonePublicationReceipt, ClonePublicationError> {
    publish_builtin_clone_with_fault(
        trusted_root,
        workflow_id,
        source_revision,
        source_content_digest,
        files,
        PublicationFault::None,
    )
}

fn publish_builtin_clone_with_fault(
    trusted_root: &Path,
    workflow_id: &str,
    source_revision: u64,
    source_content_digest: &str,
    files: &BTreeMap<String, BuiltinSkillFile>,
    _fault: PublicationFault,
) -> Result<ClonePublicationReceipt, ClonePublicationError> {
    validate_clone_bundle(files)?;
    let root = open_publication_root(trusted_root)?;
    let transaction_parent = open_transaction_parent(&root)?;
    let target_name = Path::new(workflow_id);
    match root.skills_dir.symlink_metadata(target_name) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(ClonePublicationError::Conflict(
                "Workflow already exists in the target layer",
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let marker_name = PathBuf::from(clone_marker_name(workflow_id));
    let staging_name = format!("txn-{}", uuid::Uuid::new_v4());
    let bundle_digest = builtin_clone_bundle_digest(files);
    let mut marker = ClonePublicationMarker {
        schema: CLONE_MARKER_SCHEMA,
        workflow_id: workflow_id.to_string(),
        source_revision,
        source_content_digest: source_content_digest.to_string(),
        bundle_digest,
        staging_name: staging_name.clone(),
        phase: ClonePublicationPhase::Prepared,
        stage_identity: None,
        target_identity: None,
    };
    if !marker.validate_for(workflow_id) {
        return Err(ClonePublicationError::Internal(
            "Workflow clone marker validation failed",
        ));
    }
    let mut marker_options = CapOpenOptions::new();
    marker_options.read(true).write(true).create_new(true);
    let mut marker_file = match root.skills_dir.open_with(&marker_name, &marker_options) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone marker already exists",
            ));
        }
        Err(error) => return Err(error.into()),
    };
    let marker_metadata = marker_file.metadata()?;
    if !marker_metadata.is_file() || !cap_regular_file_has_single_link(&marker_metadata) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone marker is not exclusively owned",
        ));
    }
    let marker_identity = cap_node_identity(&marker_metadata);
    write_marker(
        &root,
        &marker_name,
        &mut marker_file,
        marker_identity,
        &marker,
    )?;
    #[cfg(test)]
    if _fault == PublicationFault::StopAfterPrepared {
        return Err(injected_failure());
    }

    match root.skills_dir.symlink_metadata(target_name) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(ClonePublicationError::Conflict(
                "Workflow appeared in the target layer while cloning",
            ));
        }
        Err(error) => return Err(error.into()),
    }
    transaction_parent.create_dir(&staging_name)?;
    sync_cap_directory(&transaction_parent)?;
    let stage = open_real_child_dir(&transaction_parent, Path::new(&staging_name))?;
    let stage_identity = cap_node_identity(&stage.dir_metadata()?);
    populate_stage(&stage, files)?;
    verify_clone_tree(&stage, files)?;
    sync_clone_tree_directories(&stage)?;
    ensure_open_child_is_still_named(&transaction_parent, Path::new(&staging_name), &stage)?;
    marker.phase = ClonePublicationPhase::Staged;
    marker.stage_identity = Some(stage_identity);
    write_marker(
        &root,
        &marker_name,
        &mut marker_file,
        marker_identity,
        &marker,
    )?;
    #[cfg(test)]
    if _fault == PublicationFault::StopAfterStaged {
        return Err(injected_failure());
    }
    #[cfg(test)]
    if _fault == PublicationFault::TargetDirectoryBeforeRename {
        root.skills_dir.create_dir(target_name)?;
        let competitor = open_real_child_dir(&root.skills_dir, target_name)?;
        let mut options = CapOpenOptions::new();
        options.write(true).create_new(true);
        let mut sentinel = competitor.open_with("sentinel.txt", &options)?;
        sentinel.write_all(b"competitor-owned")?;
        sentinel.sync_all()?;
        sync_cap_directory(&competitor)?;
        sync_cap_directory(&root.skills_dir)?;
    }
    drop(stage);

    let rename_result = rename_stage_noreplace(
        &transaction_parent,
        Path::new(&staging_name),
        &root.skills_dir,
        target_name,
        stage_identity,
    );
    #[cfg(test)]
    let rename_result =
        if rename_result.is_ok() && _fault == PublicationFault::RenameReportsErrorAfterMove {
            Err(std::io::Error::other(
                "injected ambiguous no-replace rename result",
            ))
        } else {
            rename_result
        };

    let target = match open_optional_real_child_dir(&root.skills_dir, target_name) {
        Ok(Some(target)) => target,
        Ok(None) => {
            return Err(rename_result
                .err()
                .unwrap_or_else(|| {
                    std::io::Error::other("Workflow clone publication target is missing")
                })
                .into());
        }
        Err(error) => return Err(error),
    };
    let target_identity = cap_node_identity(&target.dir_metadata()?);
    if target_identity != stage_identity {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone publication resolved to a different target",
        ));
    }
    verify_clone_tree(&target, files)?;
    sync_clone_tree_directories(&target)?;
    ensure_open_child_is_still_named(&root.skills_dir, target_name, &target)?;
    sync_cap_directory(&root.skills_dir)?;
    sync_cap_directory(&transaction_parent)?;
    #[cfg(test)]
    if _fault == PublicationFault::StopAfterRename {
        return Err(injected_failure());
    }

    marker.phase = ClonePublicationPhase::Complete;
    marker.target_identity = Some(target_identity);
    write_marker(
        &root,
        &marker_name,
        &mut marker_file,
        marker_identity,
        &marker,
    )?;
    Ok(ClonePublicationReceipt { target_identity })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_skills::store::builtin::{builtin_clone_files, load_builtin_skill_bundles};

    fn review_files() -> BTreeMap<String, BuiltinSkillFile> {
        let bundles = load_builtin_skill_bundles().expect("builtin bundles");
        let review = bundles
            .iter()
            .find(|bundle| bundle.skill.id == "review")
            .expect("review builtin");
        builtin_clone_files(review).expect("review clone files")
    }

    fn publish_with_fault(
        root: &Path,
        fault: PublicationFault,
    ) -> Result<ClonePublicationReceipt, ClonePublicationError> {
        publish_builtin_clone_with_fault(root, "review", 7, &"a".repeat(64), &review_files(), fault)
    }

    #[test]
    fn fresh_publication_is_exact_mode_safe_and_outside_discovery_staging() {
        let root = tempfile::tempdir().expect("root");
        publish_with_fault(root.path(), PublicationFault::None).expect("publish");

        let target = root.path().join("skills/review");
        assert!(target.join("SKILL.md").is_file());
        assert!(root.path().join(CLONE_TRANSACTION_DIRECTORY).is_dir());
        assert!(!root
            .path()
            .join("skills")
            .join(CLONE_TRANSACTION_DIRECTORY)
            .exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for (relative, file) in review_files() {
                let mode = std::fs::metadata(target.join(relative))
                    .expect("published file")
                    .permissions()
                    .mode()
                    & 0o7777;
                assert_eq!(mode, if file.executable { 0o755 } else { 0o644 });
            }
        }
    }

    #[test]
    fn existing_target_kinds_and_marker_are_never_overwritten() {
        for kind in ["directory", "file"] {
            let root = tempfile::tempdir().expect("root");
            std::fs::create_dir_all(root.path().join("skills")).expect("skills");
            let target = root.path().join("skills/review");
            if kind == "directory" {
                std::fs::create_dir(&target).expect("competitor directory");
                std::fs::write(target.join("sentinel"), b"keep").expect("sentinel");
            } else {
                std::fs::write(&target, b"keep").expect("competitor file");
            }
            assert!(matches!(
                publish_with_fault(root.path(), PublicationFault::None),
                Err(ClonePublicationError::Conflict(_))
            ));
            if kind == "directory" {
                assert_eq!(std::fs::read(target.join("sentinel")).unwrap(), b"keep");
            } else {
                assert_eq!(std::fs::read(target).unwrap(), b"keep");
            }
        }

        let root = tempfile::tempdir().expect("root");
        std::fs::create_dir_all(root.path().join("skills")).expect("skills");
        let marker = root.path().join("skills/.review.clone-v1.json");
        std::fs::write(&marker, b"competitor marker").expect("marker");
        assert!(matches!(
            publish_with_fault(root.path(), PublicationFault::None),
            Err(ClonePublicationError::Conflict(_))
        ));
        assert_eq!(std::fs::read(marker).unwrap(), b"competitor marker");
    }

    #[cfg(unix)]
    #[test]
    fn existing_target_symlink_is_never_followed_or_overwritten() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir_all(root.path().join("skills")).expect("skills");
        std::fs::write(outside.path().join("sentinel"), b"keep").expect("sentinel");
        symlink(outside.path(), root.path().join("skills/review")).expect("target symlink");
        assert!(matches!(
            publish_with_fault(root.path(), PublicationFault::None),
            Err(ClonePublicationError::Conflict(_))
        ));
        assert_eq!(
            std::fs::read(outside.path().join("sentinel")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn crash_points_leave_incomplete_marker_and_never_expose_partial_target() {
        for fault in [
            PublicationFault::StopAfterPrepared,
            PublicationFault::StopAfterStaged,
            PublicationFault::StopAfterRename,
        ] {
            let root = tempfile::tempdir().expect("root");
            assert!(publish_with_fault(root.path(), fault).is_err());
            let marker: ClonePublicationMarker = serde_json::from_slice(
                &std::fs::read(root.path().join("skills/.review.clone-v1.json"))
                    .expect("durable marker"),
            )
            .expect("marker JSON");
            assert_ne!(marker.phase, ClonePublicationPhase::Complete);
            if fault == PublicationFault::StopAfterRename {
                assert!(root.path().join("skills/review/SKILL.md").is_file());
            } else {
                assert!(!root.path().join("skills/review").exists());
            }
        }
    }

    #[test]
    fn target_race_preserves_competitor_and_ambiguous_success_reconciles_exact_identity() {
        let root = tempfile::tempdir().expect("root");
        assert!(matches!(
            publish_with_fault(root.path(), PublicationFault::TargetDirectoryBeforeRename),
            Err(ClonePublicationError::Conflict(_))
        ));
        assert_eq!(
            std::fs::read(root.path().join("skills/review/sentinel.txt")).unwrap(),
            b"competitor-owned"
        );

        let root = tempfile::tempdir().expect("root");
        publish_with_fault(root.path(), PublicationFault::RenameReportsErrorAfterMove)
            .expect("exact moved target reconciles");
        let marker: ClonePublicationMarker = serde_json::from_slice(
            &std::fs::read(root.path().join("skills/.review.clone-v1.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(marker.phase, ClonePublicationPhase::Complete);
        assert_eq!(marker.stage_identity, marker.target_identity);
    }
}
