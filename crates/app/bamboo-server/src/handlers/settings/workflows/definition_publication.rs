//! Publication of locally authored typed definitions. The catalog remains the
//! only registry: this module writes `skills/<id>/{SKILL.md,workflow.yaml}`.
//!
//! Clone and authoring writes share one descriptor-bound transaction lock.
//! Failed stages are deliberately retained, never adopted or recursively
//! removed: without a durable ownership journal, their ownership is uncertain.
//! Updates also retain the displaced file outside catalog discovery. An
//! exchange detects concurrent replacement without destroying its bytes; it
//! does not provide an expected-inode CAS against uncooperative editors.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use bamboo_domain::WorkflowRunDefinition;
use bamboo_skills::clone_publication::{clone_marker_name, std_file_identity};
use cap_std::fs::{Dir as CapDir, File as CapFile, Metadata, OpenOptions as CapOpenOptions};
use sha2::{Digest, Sha256};

use super::clone_publication::{
    cap_node_identity, cap_regular_file_has_single_link, ensure_cap_file_is_still_named,
    ensure_open_child_is_still_named, lock_clone_transactions, open_publication_root,
    open_real_child_dir, open_transaction_parent, rename_entry_noreplace, sync_cap_directory,
    ClonePublicationError, ClonePublicationRoot, CLONE_TRANSACTION_DIRECTORY,
    CLONE_TRANSACTION_LOCK,
};

pub(super) const MAX_DEFINITION_BYTES: usize = 256 * 1024;
pub(super) const MAX_SKILL_BYTES: usize = 64 * 1024;
const DEFINITION_FILE: &str = "workflow.yaml";
const SKILL_FILE: &str = "SKILL.md";

#[derive(Debug)]
pub(super) enum DefinitionPublicationError {
    Invalid(&'static str),
    Conflict(&'static str),
    Unsupported(&'static str),
    Io(std::io::Error),
    Internal(&'static str),
    /// Rename may have committed. The client must reload before retrying; an
    /// error here must never be presented as proof that old bytes survived.
    PublicationUncertain(&'static str),
}

impl From<std::io::Error> for DefinitionPublicationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ClonePublicationError> for DefinitionPublicationError {
    fn from(error: ClonePublicationError) -> Self {
        match error {
            ClonePublicationError::Conflict(message) => Self::Conflict(message),
            ClonePublicationError::Io(error) => Self::Io(error),
            ClonePublicationError::Internal(message) => Self::Internal(message),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DefinitionSnapshot {
    pub bytes: Vec<u8>,
    /// SHA-256 of the exact workflow.yaml bytes, not the catalog bundle digest.
    pub digest: String,
    pub revision: u64,
    pub skill_bytes: Vec<u8>,
    /// Separate immutable-header compare-and-swap token.
    pub skill_digest: String,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_id(id: &str) -> Result<(), DefinitionPublicationError> {
    if !super::validation::is_safe_workflow_name(id) || id.starts_with('.') || id.ends_with('.') {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow definition id is not a safe directory name",
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), DefinitionPublicationError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow definition digest must be a lowercase SHA-256 value",
        ));
    }
    Ok(())
}

fn definition_revision(id: &str, bytes: &[u8]) -> Result<u64, DefinitionPublicationError> {
    if bytes.is_empty() || bytes.len() > MAX_DEFINITION_BYTES {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow definition exceeds its byte limit",
        ));
    }
    let definition: WorkflowRunDefinition = serde_yaml::from_slice(bytes).map_err(|_| {
        DefinitionPublicationError::Invalid("Workflow definition is not valid typed YAML or JSON")
    })?;
    if definition.workflow_schema != 1 || definition.id != id || definition.revision == 0 {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow definition identity or schema does not match its directory",
        ));
    }
    Ok(definition.revision)
}

fn validate_authored_skill(id: &str, bytes: &[u8]) -> Result<(), DefinitionPublicationError> {
    if bytes.is_empty() || bytes.len() > MAX_SKILL_BYTES {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow Skill metadata exceeds its byte limit",
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| {
        DefinitionPublicationError::Invalid("Workflow Skill metadata must be UTF-8")
    })?;
    let frontmatter = text
        .strip_prefix("---\n")
        .and_then(|text| text.split_once("\n---\n"))
        .map(|(frontmatter, _)| frontmatter)
        .ok_or(DefinitionPublicationError::Conflict(
            "Workflow is not an authored definition bundle",
        ))?;
    let metadata: serde_yaml::Value = serde_yaml::from_str(frontmatter).map_err(|_| {
        DefinitionPublicationError::Conflict("Workflow authored Skill metadata is malformed")
    })?;
    if metadata.get("name").and_then(serde_yaml::Value::as_str) != Some(id)
        || metadata
            .get("metadata")
            .and_then(|metadata| metadata.get("workflow_definition_authoring"))
            .and_then(serde_yaml::Value::as_u64)
            != Some(1)
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow is not an authored definition bundle",
        ));
    }
    // The legacy workflow filename allowlist is intentionally broader than
    // canonical Skill discovery. Reuse its parser so an authored bundle can
    // never be committed under an ID/name the catalog would refuse to load.
    bamboo_skills::store::parser::parse_markdown_skill(&Path::new(id).join(SKILL_FILE), text)
        .map_err(|_| {
            DefinitionPublicationError::Invalid(
                "Workflow Skill metadata does not satisfy canonical discovery rules",
            )
        })?;
    Ok(())
}

struct Authority {
    path: PathBuf,
    root: ClonePublicationRoot,
    transaction: CapDir,
    lock: std::fs::File,
}

impl Authority {
    fn open(path: &Path) -> Result<Self, DefinitionPublicationError> {
        let root = open_publication_root(path)?;
        let transaction = open_transaction_parent(&root)?;
        let lock = lock_clone_transactions(&transaction)?;
        let authority = Self {
            path: path.to_path_buf(),
            root,
            transaction,
            lock,
        };
        authority.ensure_named()?;
        Ok(authority)
    }

    fn ensure_named(&self) -> Result<(), DefinitionPublicationError> {
        // Reopening is read-only; it cannot create a missing root/skills tree.
        // Checking the ambient name binds the trusted root's pathname as well
        // as every handle-relative descendant used for a mutation.
        let named = std::fs::symlink_metadata(&self.path)?;
        if named.file_type().is_symlink() || !named.is_dir() {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow definition root changed while in use",
            ));
        }
        #[cfg(unix)]
        let root_matches = {
            use std::os::unix::fs::MetadataExt;
            let opened = cap_node_identity(&self.root.trusted_root.dir_metadata()?);
            named.dev() == opened.device && named.ino() == opened.inode
        };
        #[cfg(windows)]
        let root_matches = {
            // The original root's Windows handle denies deletion. Reopen via
            // the shared no-follow root implementation and compare identity.
            let opened = super::clone_publication::open_windows_root_no_follow(&self.path)?;
            cap_node_identity(&opened.dir_metadata()?)
                == cap_node_identity(&self.root.trusted_root.dir_metadata()?)
        };
        #[cfg(not(any(unix, windows)))]
        let root_matches = false;
        if !root_matches {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow definition root authority was replaced",
            ));
        }
        ensure_open_child_is_still_named(
            &self.root.trusted_root,
            Path::new("skills"),
            &self.root.skills_dir,
        )?;
        ensure_open_child_is_still_named(
            &self.root.trusted_root,
            Path::new(CLONE_TRANSACTION_DIRECTORY),
            &self.transaction,
        )?;
        let lock = self.transaction.symlink_metadata(CLONE_TRANSACTION_LOCK)?;
        if lock.file_type().is_symlink()
            || !lock.is_file()
            || !cap_regular_file_has_single_link(&lock)
            || Some(cap_node_identity(&lock)) != std_file_identity(&self.lock)
        {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow publication lock authority was replaced",
            ));
        }
        Ok(())
    }

    fn reject_clone_marker(&self, id: &str) -> Result<(), DefinitionPublicationError> {
        match self.root.skills_dir.symlink_metadata(clone_marker_name(id)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
            Ok(_) => Err(DefinitionPublicationError::Conflict(
                "Workflow id belongs to a clone publication",
            )),
        }
    }

    fn ensure_bundle_named(
        &self,
        id: &str,
        bundle: &CapDir,
    ) -> Result<(), DefinitionPublicationError> {
        self.ensure_named()?;
        self.reject_clone_marker(id)?;
        ensure_open_child_is_still_named(&self.root.skills_dir, Path::new(id), bundle)?;
        Ok(())
    }
}

fn open_file_nofollow(parent: &CapDir, name: &Path) -> Result<CapFile, DefinitionPublicationError> {
    #[cfg(unix)]
    {
        use rustix::fs::{Mode, OFlags};
        use std::os::fd::AsFd;
        let file = rustix::fs::openat(
            parent.as_fd(),
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        Ok(CapFile::from_std(std::fs::File::from(file)))
    }
    #[cfg(windows)]
    {
        use cap_fs_ext::OpenOptionsFollowExt;
        let mut options = CapOpenOptions::new();
        options.read(true).follow(cap_fs_ext::FollowSymlinks::No);
        Ok(parent.open_with(name, &options)?)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (parent, name);
        Err(DefinitionPublicationError::Internal(
            "No-follow workflow definition reads are unavailable on this platform",
        ))
    }
}

fn read_file(
    parent: &CapDir,
    name: &str,
    limit: usize,
) -> Result<(Vec<u8>, Metadata), DefinitionPublicationError> {
    let before = parent.symlink_metadata(name)?;
    if before.file_type().is_symlink()
        || !before.is_file()
        || !cap_regular_file_has_single_link(&before)
        || before.len() > limit as u64
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition resource must be a bounded, unlinked regular file",
        ));
    }
    let mut file = open_file_nofollow(parent, Path::new(name))?;
    let opened = file.metadata()?;
    if !opened.is_file()
        || !cap_regular_file_has_single_link(&opened)
        || cap_node_identity(&before) != cap_node_identity(&opened)
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition resource changed while opening",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    Read::by_ref(&mut file)
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    ensure_cap_file_is_still_named(parent, Path::new(name), &after)?;
    if bytes.len() > limit
        || bytes.len() as u64 != before.len()
        || after.len() != before.len()
        || !cap_regular_file_has_single_link(&after)
        || after.modified()? != before.modified()?
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition resource changed while reading",
        ));
    }
    Ok((bytes, after))
}

fn read_bundle(
    id: &str,
    bundle: &CapDir,
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    read_bundle_with_metadata(id, bundle).map(|(snapshot, _, _)| snapshot)
}

fn read_bundle_with_metadata(
    id: &str,
    bundle: &CapDir,
) -> Result<(DefinitionSnapshot, Metadata, Metadata), DefinitionPublicationError> {
    let (skill_bytes, skill_metadata) = read_file(bundle, SKILL_FILE, MAX_SKILL_BYTES)?;
    validate_authored_skill(id, &skill_bytes)?;
    let (bytes, definition_metadata) = read_file(bundle, DEFINITION_FILE, MAX_DEFINITION_BYTES)?;
    let revision = definition_revision(id, &bytes)?;
    Ok((
        DefinitionSnapshot {
            digest: sha256(&bytes),
            skill_digest: sha256(&skill_bytes),
            bytes,
            skill_bytes,
            revision,
        },
        skill_metadata,
        definition_metadata,
    ))
}

fn ensure_file_unchanged(
    parent: &CapDir,
    name: &str,
    expected: &Metadata,
) -> Result<(), DefinitionPublicationError> {
    ensure_cap_file_is_still_named(parent, Path::new(name), expected)?;
    let named = parent.symlink_metadata(name)?;
    if named.file_type().is_symlink()
        || !named.is_file()
        || cap_node_identity(&named) != cap_node_identity(expected)
        || !cap_regular_file_has_single_link(&named)
        || named.len() != expected.len()
        || named.modified()? != expected.modified()?
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition file authority changed before publication",
        ));
    }
    Ok(())
}

fn create_file(
    parent: &CapDir,
    name: &str,
    bytes: &[u8],
) -> Result<Metadata, DefinitionPublicationError> {
    let mut options = CapOpenOptions::new();
    options.read(true).write(true).create_new(true);
    let mut file = parent.open_with(name, &options)?;
    let initial = file.metadata()?;
    if !initial.is_file() || !cap_regular_file_has_single_link(&initial) {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition temporary file is not exclusively owned",
        ));
    }
    file.write_all(bytes)?;
    file.sync_all()?;
    let after = file.metadata()?;
    if cap_node_identity(&initial) != cap_node_identity(&after)
        || !cap_regular_file_has_single_link(&after)
        || after.len() != bytes.len() as u64
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition temporary file changed while writing",
        ));
    }
    ensure_cap_file_is_still_named(parent, Path::new(name), &after)?;
    Ok(after)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
type PublicationHook = std::sync::Arc<dyn Fn(&CapDir, &CapDir) + Send + Sync>;

#[cfg_attr(not(test), derive(Copy))]
#[derive(Clone, Default)]
enum PublicationFault {
    #[default]
    None,
    #[cfg(test)]
    BeforeRename,
    #[cfg(test)]
    AfterRename,
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    BeforeRenameHook(PublicationHook),
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    AfterRenameHook(PublicationHook),
}

#[cfg(test)]
fn injected_failure() -> DefinitionPublicationError {
    DefinitionPublicationError::Io(std::io::Error::other(
        "injected definition publication failure",
    ))
}

pub(super) fn create_definition(
    trusted_root: &Path,
    id: &str,
    skill_bytes: &[u8],
    bytes: &[u8],
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    create_definition_with_fault(trusted_root, id, skill_bytes, bytes, PublicationFault::None)
}

fn create_definition_with_fault(
    trusted_root: &Path,
    id: &str,
    skill_bytes: &[u8],
    bytes: &[u8],
    _fault: PublicationFault,
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    validate_id(id)?;
    validate_authored_skill(id, skill_bytes)?;
    if definition_revision(id, bytes)? != 1 {
        return Err(DefinitionPublicationError::Invalid(
            "New workflow definition must start at revision 1",
        ));
    }
    let authority = Authority::open(trusted_root)?;
    authority.reject_clone_marker(id)?;
    match authority.root.skills_dir.symlink_metadata(id) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow already exists",
            ))
        }
    }
    // Stages live outside catalog discovery. Never reuse or remove an existing
    // stage, even after an earlier request returned an I/O error.
    let stage_name = format!("definition-{}", uuid::Uuid::new_v4());
    authority.ensure_named()?;
    authority.transaction.create_dir(&stage_name)?;
    let stage = open_real_child_dir(&authority.transaction, Path::new(&stage_name))?;
    let stage_identity = cap_node_identity(&stage.dir_metadata()?);
    let mut created_files = Vec::with_capacity(2);
    for (name, contents) in [(SKILL_FILE, skill_bytes), (DEFINITION_FILE, bytes)] {
        authority.ensure_named()?;
        ensure_open_child_is_still_named(&authority.transaction, Path::new(&stage_name), &stage)?;
        created_files.push((name, create_file(&stage, name, contents)?));
    }
    let snapshot = read_bundle(id, &stage)?;
    if snapshot.bytes != bytes || snapshot.skill_bytes != skill_bytes {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition stage differs from requested bytes",
        ));
    }
    sync_cap_directory(&stage)?;
    sync_cap_directory(&authority.transaction)?;
    authority.ensure_named()?;
    authority.reject_clone_marker(id)?;
    ensure_open_child_is_still_named(&authority.transaction, Path::new(&stage_name), &stage)?;
    #[cfg(test)]
    if matches!(_fault, PublicationFault::BeforeRename) {
        return Err(injected_failure());
    }
    for (name, metadata) in &created_files {
        ensure_file_unchanged(&stage, name, metadata)?;
    }
    // Drop handles before directory rename on Windows; the pinned parent and
    // shared no-replace helper still validate the exact source identity.
    drop(stage);
    if let Err(error) = rename_entry_noreplace(
        &authority.transaction,
        Path::new(&stage_name),
        &authority.root.skills_dir,
        Path::new(id),
        stage_identity,
        true,
    ) {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow already exists",
            ));
        }
        // Some filesystems may report an error after completing a rename.
        return Err(DefinitionPublicationError::PublicationUncertain(
            "Workflow creation could not confirm publication; reload before retrying",
        ));
    }
    #[cfg(test)]
    if matches!(_fault, PublicationFault::AfterRename) {
        return Err(DefinitionPublicationError::PublicationUncertain(
            "Workflow creation committed but durability could not be confirmed",
        ));
    }
    let published = open_real_child_dir(&authority.root.skills_dir, Path::new(id))
        .map_err(|_| DefinitionPublicationError::PublicationUncertain("Reload created workflow"))?;
    let published_metadata = published
        .dir_metadata()
        .map_err(|_| DefinitionPublicationError::PublicationUncertain("Reload created workflow"))?;
    if cap_node_identity(&published_metadata) != stage_identity {
        return Err(DefinitionPublicationError::PublicationUncertain(
            "Workflow creation target changed after publication",
        ));
    }
    authority.ensure_bundle_named(id, &published).map_err(|_| {
        DefinitionPublicationError::PublicationUncertain("Workflow creation authority changed")
    })?;
    sync_cap_directory(&authority.root.skills_dir)
        .and_then(|()| sync_cap_directory(&authority.transaction))
        .map_err(|_| {
            DefinitionPublicationError::PublicationUncertain(
                "Workflow creation committed but durability could not be confirmed",
            )
        })?;
    Ok(snapshot)
}

pub(super) fn read_definition(
    trusted_root: &Path,
    id: &str,
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    validate_id(id)?;
    let authority = Authority::open(trusted_root)?;
    authority.reject_clone_marker(id)?;
    let bundle = open_real_child_dir(&authority.root.skills_dir, Path::new(id))?;
    let snapshot = read_bundle(id, &bundle)?;
    authority.ensure_bundle_named(id, &bundle)?;
    Ok(snapshot)
}

pub(super) fn update_definition(
    trusted_root: &Path,
    id: &str,
    expected_digest: &str,
    expected_revision: u64,
    expected_skill_digest: &str,
    bytes: &[u8],
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    update_definition_with_fault(
        trusted_root,
        id,
        expected_digest,
        expected_revision,
        expected_skill_digest,
        bytes,
        PublicationFault::None,
    )
}

pub(super) fn require_update_support() -> Result<(), DefinitionPublicationError> {
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        Ok(())
    } else {
        Err(DefinitionPublicationError::Unsupported(
            "Workflow definition updates require Linux or macOS exchange support",
        ))
    }
}

fn exchange_definition(
    transaction: &CapDir,
    temporary_name: &str,
    bundle: &CapDir,
) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::fd::AsFd;

        rustix::fs::renameat_with(
            transaction.as_fd(),
            Path::new(temporary_name),
            bundle.as_fd(),
            Path::new(DEFINITION_FILE),
            rustix::fs::RenameFlags::EXCHANGE,
        )
        .map_err(std::io::Error::from)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (transaction, temporary_name, bundle);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Workflow definition exchange is unavailable on this platform",
        ))
    }
}

fn update_definition_with_fault(
    trusted_root: &Path,
    id: &str,
    expected_digest: &str,
    expected_revision: u64,
    expected_skill_digest: &str,
    bytes: &[u8],
    _fault: PublicationFault,
) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
    validate_id(id)?;
    validate_digest(expected_digest)?;
    validate_digest(expected_skill_digest)?;
    let revision = definition_revision(id, bytes)?;
    if expected_revision == 0 || expected_revision.checked_add(1) != Some(revision) {
        return Err(DefinitionPublicationError::Invalid(
            "Workflow definition revision must increase by exactly one",
        ));
    }
    // In particular, do not create a transaction directory, lock or stage on
    // platforms where preserving the displaced target is unavailable.
    require_update_support()?;
    let authority = Authority::open(trusted_root)?;
    authority.reject_clone_marker(id)?;
    let bundle = open_real_child_dir(&authority.root.skills_dir, Path::new(id))?;
    let (current, initial_skill, initial_definition) = read_bundle_with_metadata(id, &bundle)?;
    if current.digest != expected_digest
        || current.revision != expected_revision
        || current.skill_digest != expected_skill_digest
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition changed; reload before updating",
        ));
    }
    authority.ensure_bundle_named(id, &bundle)?;
    // Temporary bytes stay outside the bundle, so no failed/incomplete write
    // can enter a catalog resource snapshot.
    let temporary_name = format!("definition-{}.yaml", uuid::Uuid::new_v4());
    let temporary = create_file(&authority.transaction, &temporary_name, bytes)?;
    sync_cap_directory(&authority.transaction)?;
    let (staged_bytes, staged_metadata) = read_file(
        &authority.transaction,
        &temporary_name,
        MAX_DEFINITION_BYTES,
    )?;
    if staged_bytes != bytes || cap_node_identity(&temporary) != cap_node_identity(&staged_metadata)
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition replacement changed while staged",
        ));
    }
    // Re-read both files under the shared lock and reject outside edits too.
    // Matching bytes alone are insufficient for replacement file authority.
    let (old_bytes, old_metadata) = read_file(&bundle, DEFINITION_FILE, MAX_DEFINITION_BYTES)?;
    let (skill_bytes, skill_metadata) = read_file(&bundle, SKILL_FILE, MAX_SKILL_BYTES)?;
    if old_bytes != current.bytes
        || skill_bytes != current.skill_bytes
        || cap_node_identity(&old_metadata) != cap_node_identity(&initial_definition)
        || cap_node_identity(&skill_metadata) != cap_node_identity(&initial_skill)
    {
        return Err(DefinitionPublicationError::Conflict(
            "Workflow definition bundle changed during update",
        ));
    }
    authority.ensure_bundle_named(id, &bundle)?;
    ensure_file_unchanged(&bundle, DEFINITION_FILE, &old_metadata)?;
    ensure_file_unchanged(&bundle, SKILL_FILE, &skill_metadata)?;
    ensure_file_unchanged(&authority.transaction, &temporary_name, &temporary)?;
    #[cfg(test)]
    if matches!(_fault, PublicationFault::BeforeRename) {
        return Err(injected_failure());
    }
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    if let PublicationFault::BeforeRenameHook(hook) = &_fault {
        hook(&authority.transaction, &bundle);
    }
    // The outside editor need not take our lock. Preserve whichever entry is
    // actually displaced, then verify it was the checked original. Ordinary
    // rename would unlink an editor's replacement in this last-check window.
    // Never fall back to rename or swap back over a later visible version.
    let exchanged = exchange_definition(&authority.transaction, &temporary_name, &bundle);
    #[cfg(test)]
    if exchanged.is_ok() && matches!(_fault, PublicationFault::AfterRename) {
        return Err(DefinitionPublicationError::PublicationUncertain(
            "Workflow update committed but durability could not be confirmed",
        ));
    }
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    if exchanged.is_ok() {
        if let PublicationFault::AfterRenameHook(hook) = &_fault {
            hook(&authority.transaction, &bundle);
        }
    }
    // Attempt both barriers even if exchange reported an error or displaced
    // an unexpected entry. Retain every entry on all uncertainty paths.
    let published_sync = sync_cap_directory(&bundle);
    let retained_sync = sync_cap_directory(&authority.transaction);
    if exchanged.is_err() || published_sync.is_err() || retained_sync.is_err() {
        return Err(DefinitionPublicationError::PublicationUncertain(
            "Workflow update publication or durability could not be confirmed; reload before retrying",
        ));
    }
    let confirmed = (|| -> Result<(), DefinitionPublicationError> {
        authority.ensure_bundle_named(id, &bundle)?;
        let (retained_bytes, _) = read_file(
            &authority.transaction,
            &temporary_name,
            MAX_DEFINITION_BYTES,
        )?;
        if retained_bytes != old_bytes {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow update displaced different definition bytes",
            ));
        }
        ensure_file_unchanged(&authority.transaction, &temporary_name, &old_metadata)?;
        let (published_bytes, _) = read_file(&bundle, DEFINITION_FILE, MAX_DEFINITION_BYTES)?;
        let (published_skill, _) = read_file(&bundle, SKILL_FILE, MAX_SKILL_BYTES)?;
        if published_bytes != bytes || published_skill != current.skill_bytes {
            return Err(DefinitionPublicationError::Conflict(
                "Workflow update target or immutable metadata changed after exchange",
            ));
        }
        ensure_file_unchanged(&bundle, DEFINITION_FILE, &temporary)?;
        ensure_file_unchanged(&bundle, SKILL_FILE, &skill_metadata)?;
        authority.ensure_bundle_named(id, &bundle)
    })();
    confirmed.map_err(|_| {
        DefinitionPublicationError::PublicationUncertain(
            "Workflow update exchanged files but the original target or current publication changed; reload before retrying",
        )
    })?;
    Ok(DefinitionSnapshot {
        bytes: bytes.to_vec(),
        digest: sha256(bytes),
        revision,
        skill_bytes: current.skill_bytes,
        skill_digest: current.skill_digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    const ID: &str = "authored-workflow";

    fn skill(id: &str) -> Vec<u8> {
        format!(
            "---\nname: {id}\ndescription: An authored workflow.\nmetadata:\n  workflow_definition_authoring: 1\n---\n\nUse the typed workflow definition.\n"
        )
        .into_bytes()
    }

    fn definition(id: &str, revision: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "workflow_schema": 1,
            "id": id,
            "revision": revision,
            "steps": [{"id": "check", "type": "tool", "tool": "read_file", "args": {}}],
            "plan": {"type": "step", "step": "check"}
        }))
        .unwrap()
    }

    fn fixture() -> (tempfile::TempDir, DefinitionSnapshot) {
        let root = tempfile::tempdir().unwrap();
        let snapshot = create_definition(root.path(), ID, &skill(ID), &definition(ID, 1)).unwrap();
        (root, snapshot)
    }

    fn update(
        root: &Path,
        snapshot: &DefinitionSnapshot,
        bytes: &[u8],
    ) -> Result<DefinitionSnapshot, DefinitionPublicationError> {
        update_definition(
            root,
            ID,
            &snapshot.digest,
            snapshot.revision,
            &snapshot.skill_digest,
            bytes,
        )
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn create_read_update_preserves_skill_and_added_resources() {
        let (root, original) = fixture();
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
        assert_eq!(original.digest, sha256(&definition(ID, 1)));
        let bundle = root.path().join("skills").join(ID);
        std::fs::write(bundle.join("notes.txt"), "user resource").unwrap();
        std::fs::create_dir(bundle.join("agents")).unwrap();
        std::fs::write(bundle.join("agents/bamboo.yaml"), "automatic: false\n").unwrap();
        let revised = update(root.path(), &original, &definition(ID, 2)).unwrap();
        assert_eq!(revised.revision, 2);
        assert_eq!(revised.skill_bytes, original.skill_bytes);
        assert_eq!(revised.skill_digest, original.skill_digest);
        assert_eq!(
            std::fs::read(bundle.join(SKILL_FILE)).unwrap(),
            original.skill_bytes
        );
        assert_eq!(
            std::fs::read_to_string(bundle.join("notes.txt")).unwrap(),
            "user resource"
        );
        assert_eq!(
            std::fs::read_to_string(bundle.join("agents/bamboo.yaml")).unwrap(),
            "automatic: false\n"
        );
        assert_eq!(read_definition(root.path(), ID).unwrap(), revised);
    }

    #[test]
    fn create_conflicts_without_replacing_existing_bundle() {
        let (root, original) = fixture();
        assert!(matches!(
            create_definition(root.path(), ID, &skill(ID), &definition(ID, 1)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn stale_digest_revision_and_immutable_skill_fail_without_writes() {
        let (root, original) = fixture();
        let newer = update(root.path(), &original, &definition(ID, 2)).unwrap();
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert!(matches!(
            update_definition(
                root.path(),
                ID,
                &newer.digest,
                1,
                &newer.skill_digest,
                &definition(ID, 2)
            ),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert!(matches!(
            update_definition(
                root.path(),
                ID,
                &newer.digest,
                2,
                &"0".repeat(64),
                &definition(ID, 3)
            ),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(read_definition(root.path(), ID).unwrap(), newer);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn same_revision_external_bytes_and_skill_changes_conflict() {
        let (root, original) = fixture();
        let bundle = root.path().join("skills").join(ID);
        let mut edited = original.bytes.clone();
        edited.push(b'\n');
        std::fs::write(bundle.join(DEFINITION_FILE), &edited).unwrap();
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(std::fs::read(bundle.join(DEFINITION_FILE)).unwrap(), edited);
        std::fs::write(bundle.join(DEFINITION_FILE), &original.bytes).unwrap();
        let mut skill_edit = original.skill_bytes.clone();
        skill_edit.extend_from_slice(b"User edits must survive.\n");
        std::fs::write(bundle.join(SKILL_FILE), &skill_edit).unwrap();
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(std::fs::read(bundle.join(SKILL_FILE)).unwrap(), skill_edit);
        assert_eq!(
            std::fs::read(bundle.join(DEFINITION_FILE)).unwrap(),
            original.bytes
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn concurrent_updates_have_one_compare_and_swap_winner() {
        let (root, original) = fixture();
        let barrier = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let root = root.path().to_path_buf();
                let original = original.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    update(&root, &original, &definition(ID, 2))
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(DefinitionPublicationError::Conflict(_))))
                .count(),
            1
        );
        assert_eq!(read_definition(root.path(), ID).unwrap().revision, 2);
    }

    #[test]
    fn concurrent_creates_have_one_no_replace_winner() {
        let root = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let root = root.path().to_path_buf();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    create_definition(&root, ID, &skill(ID), &definition(ID, 1))
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(DefinitionPublicationError::Conflict(_))))
                .count(),
            1
        );
        assert_eq!(read_definition(root.path(), ID).unwrap().revision, 1);
    }

    #[test]
    fn malformed_oversized_and_mismatched_identity_are_rejected_before_staging() {
        let root = tempfile::tempdir().unwrap();
        for bytes in [
            b"not a definition".to_vec(),
            vec![b'x'; MAX_DEFINITION_BYTES + 1],
            definition("other", 1),
            definition(ID, 0),
            definition(ID, 2),
        ] {
            assert!(matches!(
                create_definition(root.path(), ID, &skill(ID), &bytes),
                Err(DefinitionPublicationError::Invalid(_))
            ));
        }
        assert!(!root.path().join("skills").exists());
        assert!(create_definition(
            root.path(),
            ID,
            &vec![b'x'; MAX_SKILL_BYTES + 1],
            &definition(ID, 1)
        )
        .is_err());
        assert!(create_definition(root.path(), ID, &skill("other"), &definition(ID, 1)).is_err());
        assert!(!root.path().join(CLONE_TRANSACTION_DIRECTORY).exists());
    }

    #[test]
    fn unsafe_ids_cannot_escape_or_alias_control_names() {
        let root = tempfile::tempdir().unwrap();
        for id in [
            "",
            ".",
            "..",
            "../escape",
            "a/b",
            "a\\b",
            "/tmp/escape",
            "C:\\escape",
            "NUL",
            "CON.txt",
            ".hidden",
            "trailing.",
            "  spaces",
            "x\0y",
            &"x".repeat(256),
        ] {
            assert!(
                matches!(
                    create_definition(root.path(), id, &skill(id), &definition(id, 1)),
                    Err(DefinitionPublicationError::Invalid(_))
                ),
                "{id:?}"
            );
            assert!(
                matches!(
                    read_definition(root.path(), id),
                    Err(DefinitionPublicationError::Invalid(_))
                ),
                "{id:?}"
            );
        }
        assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
    }

    #[test]
    fn canonical_skill_ids_and_name_limits_are_enforced_before_publication() {
        let root = tempfile::tempdir().unwrap();
        for id in [
            "Uppercase",
            "unsafe_id",
            "has space",
            "with.dot",
            "double--hyphen",
            &"x".repeat(65),
        ] {
            assert!(
                matches!(
                    create_definition(root.path(), id, &skill(id), &definition(id, 1)),
                    Err(DefinitionPublicationError::Invalid(_))
                ),
                "{id:?}"
            );
        }
        assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
        let maximum = "x".repeat(64);
        create_definition(
            root.path(),
            &maximum,
            &skill(&maximum),
            &definition(&maximum, 1),
        )
        .unwrap();
        assert_eq!(read_definition(root.path(), &maximum).unwrap().revision, 1);
    }

    #[test]
    fn invalid_revision_and_digest_do_not_change_current_bytes() {
        let (root, original) = fixture();
        for revision in [0, 1, 3, u64::MAX] {
            assert!(matches!(
                update(root.path(), &original, &definition(ID, revision)),
                Err(DefinitionPublicationError::Invalid(_))
            ));
        }
        assert!(matches!(
            update_definition(
                root.path(),
                ID,
                "bad",
                1,
                &original.skill_digest,
                &definition(ID, 2)
            ),
            Err(DefinitionPublicationError::Invalid(_))
        ));
        assert!(matches!(
            update_definition(
                root.path(),
                ID,
                &original.digest,
                1,
                &"A".repeat(64),
                &definition(ID, 2)
            ),
            Err(DefinitionPublicationError::Invalid(_))
        ));
        assert!(matches!(
            update_definition(
                root.path(),
                ID,
                &original.digest,
                u64::MAX,
                &original.skill_digest,
                &definition(ID, 1)
            ),
            Err(DefinitionPublicationError::Invalid(_))
        ));
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
    }

    #[test]
    fn ordinary_skill_and_clone_marker_are_not_authoring_targets() {
        let (root, original) = fixture();
        let bundle = root.path().join("skills").join(ID);
        std::fs::write(
            bundle.join(SKILL_FILE),
            b"---\nname: authored-workflow\n---\nAn ordinary Skill.\n",
        )
        .unwrap();
        assert!(matches!(
            read_definition(root.path(), ID),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        std::fs::write(bundle.join(SKILL_FILE), &original.skill_bytes).unwrap();
        std::fs::write(
            root.path().join("skills").join(clone_marker_name(ID)),
            b"unknown clone transaction",
        )
        .unwrap();
        assert!(matches!(
            read_definition(root.path(), ID),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(
            std::fs::read(bundle.join(DEFINITION_FILE)).unwrap(),
            original.bytes
        );
    }

    #[test]
    fn clone_marker_blocks_create_without_changing_unknown_content() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("skills")).unwrap();
        let marker = root.path().join("skills").join(clone_marker_name(ID));
        std::fs::write(&marker, b"unknown clone transaction").unwrap();
        assert!(matches!(
            create_definition(root.path(), ID, &skill(ID), &definition(ID, 1)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert_eq!(std::fs::read(marker).unwrap(), b"unknown clone transaction");
        assert!(!root.path().join("skills").join(ID).exists());
    }

    #[test]
    fn failures_before_rename_leave_old_bytes_and_unknown_stages_untouched() {
        let (root, original) = fixture();
        let sentinel = root
            .path()
            .join(CLONE_TRANSACTION_DIRECTORY)
            .join("definition-unknown");
        std::fs::create_dir(&sentinel).unwrap();
        std::fs::write(sentinel.join("sentinel"), b"other owner's content").unwrap();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(matches!(
            update_definition_with_fault(
                root.path(),
                ID,
                &original.digest,
                original.revision,
                &original.skill_digest,
                &definition(ID, 2),
                PublicationFault::BeforeRename
            ),
            Err(DefinitionPublicationError::Io(_))
        ));
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
        assert_eq!(
            std::fs::read(sentinel.join("sentinel")).unwrap(),
            b"other owner's content"
        );
        assert!(create_definition_with_fault(
            root.path(),
            "another",
            &skill("another"),
            &definition("another", 1),
            PublicationFault::BeforeRename
        )
        .is_err());
        assert!(!root.path().join("skills/another").exists());
        create_definition(
            root.path(),
            "another",
            &skill("another"),
            &definition("another", 1),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(sentinel.join("sentinel")).unwrap(),
            b"other owner's content"
        );
    }

    #[test]
    fn failures_after_rename_report_uncertainty_and_preserve_committed_bytes() {
        let (root, original) = fixture();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(matches!(
                update_definition_with_fault(
                    root.path(),
                    ID,
                    &original.digest,
                    original.revision,
                    &original.skill_digest,
                    &definition(ID, 2),
                    PublicationFault::AfterRename
                ),
                Err(DefinitionPublicationError::PublicationUncertain(_))
            ));
            assert_eq!(read_definition(root.path(), ID).unwrap().revision, 2);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
        assert!(matches!(
            create_definition_with_fault(
                root.path(),
                "another",
                &skill("another"),
                &definition("another", 1),
                PublicationFault::AfterRename
            ),
            Err(DefinitionPublicationError::PublicationUncertain(_))
        ));
        assert_eq!(read_definition(root.path(), "another").unwrap().revision, 1);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn replacement_after_final_check_is_retained_and_reported_uncertain() {
        let (root, original) = fixture();
        let mut external_bytes = original.bytes.clone();
        external_bytes.extend_from_slice(b"\n# Concurrent external editor version B\n");
        let external_identity = Arc::new(std::sync::Mutex::new(None));
        let captured_identity = external_identity.clone();
        let replacement = external_bytes.clone();
        let result = update_definition_with_fault(
            root.path(),
            ID,
            &original.digest,
            original.revision,
            &original.skill_digest,
            &definition(ID, 2),
            PublicationFault::BeforeRenameHook(Arc::new(move |transaction, bundle| {
                let name = format!("external-{}.yaml", uuid::Uuid::new_v4());
                let metadata = create_file(transaction, &name, &replacement).unwrap();
                *captured_identity.lock().unwrap() = Some(cap_node_identity(&metadata));
                transaction.rename(&name, bundle, DEFINITION_FILE).unwrap();
                sync_cap_directory(bundle).unwrap();
                sync_cap_directory(transaction).unwrap();
            })),
        );
        let external_identity = external_identity.lock().unwrap().unwrap();
        let bundle = root.path().join("skills").join(ID);
        let mut candidates = vec![bundle.join(DEFINITION_FILE)];
        candidates.extend(
            std::fs::read_dir(root.path().join(CLONE_TRANSACTION_DIRECTORY))
                .unwrap()
                .map(|entry| entry.unwrap().path()),
        );
        let preserved = candidates
            .iter()
            .filter(|path| {
                let Ok(file) = std::fs::File::open(path) else {
                    return false;
                };
                std_file_identity(&file) == Some(external_identity)
                    && std::fs::read(path).unwrap() == external_bytes
            })
            .count();
        assert_eq!(
            preserved, 1,
            "the concurrent editor's exact inode and bytes must survive; result: {result:?}"
        );
        assert!(matches!(
            result,
            Err(DefinitionPublicationError::PublicationUncertain(_))
        ));
        assert_eq!(
            std::fs::read(bundle.join(SKILL_FILE)).unwrap(),
            original.skill_bytes
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn retained_file_count(
        root: &Path,
        identity: bamboo_skills::clone_publication::CloneNodeIdentity,
        bytes: &[u8],
    ) -> usize {
        std::fs::read_dir(root.join(CLONE_TRANSACTION_DIRECTORY))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let Ok(file) = std::fs::File::open(path) else {
                    return false;
                };
                std_file_identity(&file) == Some(identity) && std::fs::read(path).unwrap() == bytes
            })
            .count()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn same_bytes_replacement_after_final_check_still_requires_original_identity() {
        let (root, original) = fixture();
        let replacement = original.bytes.clone();
        let identity = Arc::new(std::sync::Mutex::new(None));
        let captured = identity.clone();
        let result = update_definition_with_fault(
            root.path(),
            ID,
            &original.digest,
            original.revision,
            &original.skill_digest,
            &definition(ID, 2),
            PublicationFault::BeforeRenameHook(Arc::new(move |transaction, bundle| {
                let name = format!("external-{}.yaml", uuid::Uuid::new_v4());
                let metadata = create_file(transaction, &name, &replacement).unwrap();
                *captured.lock().unwrap() = Some(cap_node_identity(&metadata));
                transaction.rename(&name, bundle, DEFINITION_FILE).unwrap();
            })),
        );
        assert!(matches!(
            result,
            Err(DefinitionPublicationError::PublicationUncertain(_))
        ));
        let identity = identity.lock().unwrap().unwrap();
        assert_eq!(
            retained_file_count(root.path(), identity, &original.bytes),
            1
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn later_editor_version_is_never_replaced_by_an_uncertainty_rollback() {
        let (root, original) = fixture();
        let path = root.path().join("skills").join(ID).join(DEFINITION_FILE);
        let original_file = std::fs::File::open(&path).unwrap();
        let original_identity = std_file_identity(&original_file).unwrap();
        drop(original_file);
        let mut later_bytes = definition(ID, 2);
        later_bytes.extend_from_slice(b"\n# Later external editor version C\n");
        let replacement = later_bytes.clone();
        let later_identity = Arc::new(std::sync::Mutex::new(None));
        let captured = later_identity.clone();
        let result = update_definition_with_fault(
            root.path(),
            ID,
            &original.digest,
            original.revision,
            &original.skill_digest,
            &definition(ID, 2),
            PublicationFault::AfterRenameHook(Arc::new(move |transaction, bundle| {
                let name = format!("external-{}.yaml", uuid::Uuid::new_v4());
                let metadata = create_file(transaction, &name, &replacement).unwrap();
                *captured.lock().unwrap() = Some(cap_node_identity(&metadata));
                transaction.rename(&name, bundle, DEFINITION_FILE).unwrap();
            })),
        );
        assert!(matches!(
            result,
            Err(DefinitionPublicationError::PublicationUncertain(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), later_bytes);
        assert_eq!(
            std_file_identity(&std::fs::File::open(&path).unwrap()),
            *later_identity.lock().unwrap()
        );
        assert_eq!(
            retained_file_count(root.path(), original_identity, &original.bytes),
            1
        );
        assert_eq!(
            std::fs::read(path.with_file_name(SKILL_FILE)).unwrap(),
            original.skill_bytes
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn successful_updates_retain_each_displaced_version_outside_the_bundle() {
        let (root, original) = fixture();
        let path = root.path().join("skills").join(ID).join(DEFINITION_FILE);
        let original_identity = std_file_identity(&std::fs::File::open(&path).unwrap()).unwrap();
        let revised = update(root.path(), &original, &definition(ID, 2)).unwrap();
        let revised_identity = std_file_identity(&std::fs::File::open(&path).unwrap()).unwrap();
        let latest = update(root.path(), &revised, &definition(ID, 3)).unwrap();
        assert_eq!(read_definition(root.path(), ID).unwrap(), latest);
        assert_eq!(
            retained_file_count(root.path(), original_identity, &original.bytes),
            1
        );
        assert_eq!(
            retained_file_count(root.path(), revised_identity, &revised.bytes),
            1
        );
        assert_eq!(
            std::fs::read(path.with_file_name(SKILL_FILE)).unwrap(),
            original.skill_bytes
        );
    }

    #[test]
    fn oversized_and_nonregular_existing_files_fail_closed() {
        let (root, original) = fixture();
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
        let definition_file = root.path().join("skills").join(ID).join(DEFINITION_FILE);
        std::fs::write(&definition_file, vec![b' '; MAX_DEFINITION_BYTES + 1]).unwrap();
        assert!(matches!(
            read_definition(root.path(), ID),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        std::fs::remove_file(&definition_file).unwrap();
        std::fs::create_dir(&definition_file).unwrap();
        assert!(matches!(
            read_definition(root.path(), ID),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Conflict(_))
        ));
        assert!(definition_file.is_dir());
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    #[test]
    fn unsupported_updates_do_not_create_or_change_publication_files() {
        let (root, original) = fixture();
        let transaction = root.path().join(CLONE_TRANSACTION_DIRECTORY);
        let entries = || {
            let mut names = std::fs::read_dir(&transaction)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            names.sort();
            names
        };
        let before = entries();
        let missing = root.path().join("not-created");
        assert!(matches!(
            update(&missing, &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Unsupported(_))
        ));
        assert!(!missing.exists());
        let resource = root.path().join("skills").join(ID).join("notes.txt");
        std::fs::write(&resource, b"user resource").unwrap();
        assert!(matches!(
            update(root.path(), &original, &definition(ID, 2)),
            Err(DefinitionPublicationError::Unsupported(_))
        ));
        assert_eq!(entries(), before);
        assert_eq!(read_definition(root.path(), ID).unwrap(), original);
        assert_eq!(std::fs::read(resource).unwrap(), b"user resource");
    }

    #[cfg(unix)]
    #[test]
    fn root_skills_and_bundle_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;
        let (root, original) = fixture();
        let outside = tempfile::tempdir().unwrap();
        let linked_root = outside.path().join("root");
        symlink(root.path(), &linked_root).unwrap();
        assert!(read_definition(&linked_root, ID).is_err());
        assert!(create_definition(
            &linked_root,
            "another",
            &skill("another"),
            &definition("another", 1)
        )
        .is_err());
        let skills = root.path().join("skills");
        let real_skills = root.path().join("real-skills");
        std::fs::rename(&skills, &real_skills).unwrap();
        symlink(&real_skills, &skills).unwrap();
        assert!(read_definition(root.path(), ID).is_err());
        assert!(update(root.path(), &original, &definition(ID, 2)).is_err());
        std::fs::remove_file(&skills).unwrap();
        std::fs::rename(&real_skills, &skills).unwrap();
        let bundle = skills.join(ID);
        let moved = root.path().join("moved-bundle");
        std::fs::rename(&bundle, &moved).unwrap();
        symlink(&moved, &bundle).unwrap();
        assert!(read_definition(root.path(), ID).is_err());
        assert!(update(root.path(), &original, &definition(ID, 2)).is_err());
        assert_eq!(
            std::fs::read(moved.join(DEFINITION_FILE)).unwrap(),
            original.bytes
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_hardlink_resources_are_never_updated() {
        use std::os::unix::fs::symlink;
        for resource in [SKILL_FILE, DEFINITION_FILE] {
            for hardlink in [false, true] {
                let (root, original) = fixture();
                let file = root.path().join("skills").join(ID).join(resource);
                let outside = root.path().join("outside");
                let original_bytes = std::fs::read(&file).unwrap();
                std::fs::rename(&file, &outside).unwrap();
                if hardlink {
                    std::fs::hard_link(&outside, &file).unwrap();
                } else {
                    symlink(&outside, &file).unwrap();
                }
                assert!(read_definition(root.path(), ID).is_err());
                assert!(update(root.path(), &original, &definition(ID, 2)).is_err());
                assert_eq!(std::fs::read(&outside).unwrap(), original_bytes);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn detached_root_and_transaction_authorities_are_rejected() {
        for part in [
            "root",
            "skills",
            CLONE_TRANSACTION_DIRECTORY,
            CLONE_TRANSACTION_LOCK,
        ] {
            let outer = tempfile::tempdir().unwrap();
            let root = outer.path().join("root");
            std::fs::create_dir(&root).unwrap();
            let authority = Authority::open(&root).unwrap();
            let original = match part {
                "root" => root.clone(),
                CLONE_TRANSACTION_LOCK => root.join(CLONE_TRANSACTION_DIRECTORY).join(part),
                _ => root.join(part),
            };
            let moved = outer.path().join("detached");
            std::fs::rename(&original, &moved).unwrap();
            if part == CLONE_TRANSACTION_LOCK {
                std::fs::write(&original, b"").unwrap();
            } else {
                std::fs::create_dir(&original).unwrap();
            }
            assert!(
                matches!(
                    authority.ensure_named(),
                    Err(DefinitionPublicationError::Conflict(_))
                ),
                "{part}"
            );
        }
    }
}
