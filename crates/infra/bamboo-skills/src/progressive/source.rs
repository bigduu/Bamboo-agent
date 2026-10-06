//! Descriptor-bound input capture. These are private publication inputs, not
//! caller permissions or an alternative Skill parser.

#[cfg(not(windows))]
use cap_fs_ext::DirExt;
use cap_fs_ext::{MetadataExt, OpenOptionsFollowExt};
use cap_std::fs::{Dir, Metadata, OpenOptions};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use super::read::{SelectedBudget, SelectedBuffer};

#[cfg(not(windows))]
type SourceFile = cap_std::fs::File;
#[cfg(windows)]
type SourceFile = std::fs::File;

use crate::catalog::{bundle_metadata_from_bytes, BundleMetadata, WorkflowKind};
use crate::store::storage::{LoadedSkillRecord, SkillDirectorySource};

pub(crate) const POLICY_FILES: [&str; 3] =
    ["workflow.yaml", "agents/bamboo.yaml", "agents/openai.yaml"];

#[cfg(not(windows))]
type SourceDir = Dir;

// Windows cap Dir requires denying deletion for its generic path operations.
// This private owner supports only single-component, handle-relative reads.
#[cfg(windows)]
#[derive(Debug)]
struct SourceDir(std::fs::File);

#[cfg(windows)]
impl SourceDir {
    fn component(path: &Path) -> io::Result<&Path> {
        let mut components = path.components();
        match (components.next(), components.next()) {
            (Some(Component::Normal(name)), None) if path.as_os_str() == name => Ok(path),
            _ => Err(io::Error::other(
                "source entry must be one normal component",
            )),
        }
    }

    fn open_with(&self, path: &Path, options: &OpenOptions) -> io::Result<std::fs::File> {
        let path = Self::component(path)?;
        let mut options = options.clone();
        options.follow(cap_primitives::fs::FollowSymlinks::No);
        cap_primitives::fs::open(&self.0, path, &options)
    }

    fn dir_metadata(&self) -> io::Result<Metadata> {
        Metadata::from_file(&self.0)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        cap_primitives::fs::stat(
            &self.0,
            Self::component(path)?,
            cap_primitives::fs::FollowSymlinks::No,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PhysicalIdentity(u64, u64);

fn physical(metadata: &Metadata) -> io::Result<PhysicalIdentity> {
    #[cfg(windows)]
    {
        use cap_primitives::fs::_WindowsByHandle;
        if metadata.volume_serial_number().is_none() || metadata.file_index().is_none() {
            return Err(io::Error::other("opened handle has no physical identity"));
        }
    }
    Ok(PhysicalIdentity(metadata.dev(), metadata.ino()))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceLimits {
    pub roots: usize,
    pub temporary: usize,
    pub index: usize,
}

impl Default for SourceLimits {
    fn default() -> Self {
        Self {
            roots: 128,
            temporary: 16,
            index: 128,
        }
    }
}

#[derive(Debug, Default)]
struct PoolState {
    retained: usize,
    temporary: usize,
    next_anchor: u64,
    roots: HashMap<PhysicalIdentity, Weak<SourceRoot>>,
}

#[derive(Debug)]
pub(crate) struct SourcePool {
    state: Arc<Mutex<PoolState>>,
    limits: SourceLimits,
}

impl Default for SourcePool {
    fn default() -> Self {
        Self::new(SourceLimits::default())
    }
}

#[derive(Debug)]
struct Lease {
    state: Arc<Mutex<PoolState>>,
    retained: bool,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("source budget");
        if self.retained {
            state.retained -= 1;
        } else {
            state.temporary -= 1;
        }
    }
}

#[derive(Debug)]
pub(crate) struct SourceRoot {
    dir: SourceDir,
    identity: PhysicalIdentity,
    anchor: u64,
    _lease: Lease,
}

struct TemporaryDir {
    dir: SourceDir,
    _lease: Lease,
}

impl SourcePool {
    pub(crate) fn new(limits: SourceLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(PoolState::default())),
            limits,
        }
    }

    fn temporary(&self) -> io::Result<Lease> {
        let mut state = self.state.lock().expect("source budget");
        if state.temporary >= self.limits.temporary {
            return Err(io::Error::other("source temporary handle budget exhausted"));
        }
        state.temporary += 1;
        Ok(Lease {
            state: self.state.clone(),
            retained: false,
        })
    }

    fn open_directory(&self, parent: &SourceDir, name: &Path) -> io::Result<TemporaryDir> {
        // Charge the returned handle and bounded library-open scratch before IO.
        let lease = self.temporary()?;
        let _scratch = self.temporary()?;
        #[cfg(not(windows))]
        let dir = parent.open_dir_nofollow(name)?;
        #[cfg(windows)]
        let dir = {
            use cap_std::fs::{MetadataExt as _, OpenOptionsExt};
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE,
                FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let mut options = OpenOptions::new();
            options
                .read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
            let file = parent.open_with(name, &options)?;
            let metadata = Metadata::from_file(&file)?;
            if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(io::Error::other(
                    "source directory is not an ordinary directory",
                ));
            }
            SourceDir(file)
        };
        Ok(TemporaryDir { dir, _lease: lease })
    }

    pub(crate) fn admit(&self, path: &Path) -> io::Result<Arc<SourceRoot>> {
        // Resolve only the trusted host locator. The resulting pathname is
        // never an identity: every normal component is opened without following
        // links and the admitted root uses opened-handle metadata below. This
        // also avoids an unbounded library FD stack for a symlink's long target.
        let absolute = {
            let _locator_scratch = self.temporary()?;
            std::fs::canonicalize(path)?
        };
        let mut anchor = PathBuf::new();
        let mut names = Vec::new();
        for component in absolute.components() {
            match component {
                Component::Prefix(_) | Component::RootDir => anchor.push(component.as_os_str()),
                Component::Normal(name) => names.push(name),
                Component::CurDir => {}
                Component::ParentDir => return Err(io::Error::other("non-normal source root")),
            }
        }
        let mut opened = {
            let lease = self.temporary()?;
            let _scratch = self.temporary()?;
            let dir = Dir::open_ambient_dir(anchor, cap_std::ambient_authority())?;
            #[cfg(windows)]
            let dir = SourceDir(dir.into_std_file());
            TemporaryDir { dir, _lease: lease }
        };
        for name in names {
            opened = self.open_directory(&opened.dir, Path::new(name))?;
        }
        let identity = physical(&opened.dir.dir_metadata()?)?;
        let mut state = self.state.lock().expect("source budget");
        state.roots.retain(|_, root| root.strong_count() != 0);
        if let Some(root) = state.roots.get(&identity).and_then(Weak::upgrade) {
            return Ok(root);
        }
        if state.retained >= self.limits.roots || state.roots.len() >= self.limits.index {
            return Err(io::Error::other(
                "source retained root/index budget exhausted",
            ));
        }
        state.next_anchor = state
            .next_anchor
            .checked_add(1)
            .ok_or_else(|| io::Error::other("source anchor capacity exhausted"))?;
        state.retained += 1;
        let root = Arc::new(SourceRoot {
            dir: opened.dir,
            identity,
            anchor: state.next_anchor,
            _lease: Lease {
                state: self.state.clone(),
                retained: true,
            },
        });
        state.roots.insert(identity, Arc::downgrade(&root));
        Ok(root)
    }

    fn walk(&self, root: &SourceDir, relative: &Path) -> io::Result<Option<TemporaryDir>> {
        let mut opened: Option<TemporaryDir> = None;
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(io::Error::other("non-normal bundle component"));
            };
            let parent = opened.as_ref().map(|value| &value.dir).unwrap_or(root);
            let next = self.open_directory(parent, Path::new(name))?;
            opened = Some(next);
        }
        Ok(opened)
    }

    fn visit<T>(
        &self,
        base: &SourceDir,
        relative: &Path,
        limit: usize,
        read: impl FnOnce(&mut SourceFile, PhysicalIdentity, usize) -> io::Result<T>,
    ) -> io::Result<(PhysicalIdentity, Option<T>)> {
        let mut components = relative.components().peekable();
        let mut opened: Option<TemporaryDir> = None;
        while let Some(component) = components.next() {
            let Component::Normal(name) = component else {
                return Err(io::Error::other("non-normal resource component"));
            };
            let parent = opened.as_ref().map(|value| &value.dir).unwrap_or(base);
            let parent_identity = physical(&parent.dir_metadata()?)?;
            if components.peek().is_some() {
                match self.open_directory(parent, Path::new(name)) {
                    Ok(next) => opened = Some(next),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        let ObservedFile::Absent(identity) =
                            self.absent(parent, Path::new(name), parent_identity)?
                        else {
                            unreachable!()
                        };
                        return Ok((identity, None));
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }
            let _leaf = self.temporary()?;
            let _scratch = self.temporary()?;
            let mut options = OpenOptions::new();
            options.read(true).follow(cap_fs_ext::FollowSymlinks::No);
            #[cfg(unix)]
            {
                use cap_std::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NONBLOCK);
            }
            let mut file = match parent.open_with(Path::new(name), &options) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let ObservedFile::Absent(identity) =
                        self.absent(parent, Path::new(name), parent_identity)?
                    else {
                        unreachable!()
                    };
                    return Ok((identity, None));
                }
                Err(error) => return Err(error),
            };
            #[cfg(not(windows))]
            let metadata = file.metadata()?;
            #[cfg(windows)]
            let metadata = Metadata::from_file(&file)?;
            #[cfg(windows)]
            {
                use cap_std::fs::MetadataExt as _;
                if metadata.file_attributes()
                    & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
                    != 0
                {
                    return Err(io::Error::other("source leaf is a reparse point"));
                }
            }
            if !metadata.is_file() || metadata.len() > limit as u64 {
                return Err(io::Error::other(
                    "source input is not a bounded regular file",
                ));
            }
            let identity = physical(&metadata)?;
            let value = read(&mut file, identity, metadata.len() as usize)?;
            return Ok((identity, Some(value)));
        }
        Err(io::Error::other("empty resource path"))
    }

    fn observe(&self, base: &SourceDir, relative: &Path, limit: usize) -> io::Result<ObservedFile> {
        let (identity, bytes) = self.visit(base, relative, limit, |file, _, _| {
            let mut bytes = Vec::new();
            file.take(limit.saturating_add(1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > limit {
                return Err(io::Error::other("source input exceeds byte limit"));
            }
            Ok(Arc::new(bytes))
        })?;
        Ok(match bytes {
            Some(bytes) => ObservedFile::Present(identity, bytes),
            None => ObservedFile::Absent(identity),
        })
    }

    fn probe(
        &self,
        base: &SourceDir,
        relative: &Path,
        limit: usize,
        budget: &SelectedBudget,
    ) -> io::Result<FileSignature> {
        let (identity, hash) = self.visit(base, relative, limit, |file, _, _| {
            let mut scratch = budget.buffer(4096, false).map_err(io::Error::other)?;
            let mut hash = Sha256::new();
            let mut size = 0;
            loop {
                let count = file.read(&mut scratch.contents)?;
                if count == 0 {
                    break;
                }
                size += count;
                if size > limit {
                    return Err(io::Error::other("source input exceeds byte limit"));
                }
                hash.update(&scratch.contents[..count]);
            }
            Ok(hash.finalize().into())
        })?;
        Ok(match hash {
            Some(hash) => FileSignature::Present(identity, hash),
            None => FileSignature::Absent(identity),
        })
    }

    fn absent(
        &self,
        parent: &SourceDir,
        name: &Path,
        identity: PhysicalIdentity,
    ) -> io::Result<ObservedFile> {
        let _scratch = self.temporary()?;
        match parent.symlink_metadata(name) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(ObservedFile::Absent(identity))
            }
            Err(error) => Err(error),
            Ok(_) => Err(io::Error::other("source input exists but cannot be opened")),
        }
    }

    fn inputs(&self, source: &Path, bundle: &Path, limit: usize) -> io::Result<RawInputs> {
        let root = self.admit(source)?;
        let opened = self.walk(&root.dir, bundle)?;
        let dir = opened.as_ref().map(|value| &value.dir).unwrap_or(&root.dir);
        let bundle_identity = physical(&dir.dir_metadata()?)?;
        let main = self.observe(dir, Path::new("SKILL.md"), limit)?;
        let ObservedFile::Present(_, main_bytes) = &main else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "SKILL.md absent"));
        };
        // Validate main UTF-8 without normalizing any raw bytes.
        std::str::from_utf8(main_bytes).map_err(|_| io::Error::other("SKILL.md is not UTF-8"))?;
        let mut policies = Vec::with_capacity(POLICY_FILES.len());
        let mut openai_error = None;
        for path in POLICY_FILES {
            match self.observe(dir, Path::new(path), limit) {
                Ok(observed) => policies.push(observed),
                Err(error) if path == "agents/openai.yaml" => {
                    openai_error = Some(error);
                    policies.push(ObservedFile::Error);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(RawInputs {
            root,
            bundle_identity,
            main,
            policies,
            openai_error,
        })
    }

    pub(crate) fn capture(
        &self,
        source: &Path,
        bundle: &Path,
        scope: SkillDirectorySource,
        mode: Option<String>,
        limit: usize,
    ) -> io::Result<CapturedSkill> {
        let mut inputs = self.inputs(source, bundle, limit)?;
        let metadata =
            bundle_metadata_from_bytes(inputs.policies[0].bytes(), inputs.policies[1].bytes())
                .map_err(io::Error::other)?;
        if let Some(error) = inputs.openai_error.take() {
            if metadata.kind == WorkflowKind::Instruction {
                return Err(error);
            }
            // Existing Workflow input tolerance is outside the new Instruction
            // authority. The Error signature is never an admitted reader binding.
        }
        let mut metadata = metadata;
        if let Some(raw) = inputs.policies[2].bytes() {
            metadata.apply_openai_metadata(raw);
        }
        let signature = inputs.signature();
        let main = std::str::from_utf8(inputs.main.bytes().expect("captured main"))
            .expect("validated UTF-8")
            .to_owned();
        let policies = POLICY_FILES
            .into_iter()
            .zip(&inputs.policies)
            .filter_map(|(name, observed)| match observed {
                ObservedFile::Present(_, bytes) => Some((name.to_owned(), bytes.clone())),
                ObservedFile::Absent(_) | ObservedFile::Error => None,
            })
            .collect();
        Ok(CapturedSkill {
            main,
            metadata,
            policies,
            binding: SourceBinding {
                root: inputs.root,
                source: source.to_path_buf(),
                bundle: bundle.to_path_buf(),
                scope,
                mode,
                signature,
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (state.retained, state.temporary, state.roots.len())
    }

    #[cfg(all(windows, test))]
    pub(crate) fn probe_directory_component(&self, source: &Path, name: &Path) -> io::Result<()> {
        let root = self.admit(source)?;
        self.open_directory(&root.dir, name)?;
        Ok(())
    }

    pub(crate) fn prune(&self) {
        self.state
            .lock()
            .expect("source budget")
            .roots
            .retain(|_, root| root.strong_count() != 0);
    }
}

enum ObservedFile {
    Error,
    Absent(PhysicalIdentity),
    Present(PhysicalIdentity, Arc<Vec<u8>>),
}

impl ObservedFile {
    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Present(_, bytes) => Some(bytes.as_slice()),
            Self::Absent(_) | Self::Error => None,
        }
    }

    fn signature(&self) -> FileSignature {
        match self {
            Self::Error => FileSignature::Error,
            Self::Absent(parent) => FileSignature::Absent(*parent),
            Self::Present(identity, bytes) => FileSignature::Present(*identity, digest(bytes)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileSignature {
    Error,
    Absent(PhysicalIdentity),
    Present(PhysicalIdentity, [u8; 32]),
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputSignature {
    source: PhysicalIdentity,
    anchor: u64,
    bundle: PhysicalIdentity,
    main: FileSignature,
    policies: Vec<FileSignature>,
}

struct RawInputs {
    root: Arc<SourceRoot>,
    bundle_identity: PhysicalIdentity,
    main: ObservedFile,
    policies: Vec<ObservedFile>,
    openai_error: Option<io::Error>,
}

impl RawInputs {
    fn signature(&self) -> InputSignature {
        InputSignature {
            source: self.root.identity,
            anchor: self.root.anchor,
            bundle: self.bundle_identity,
            main: self.main.signature(),
            policies: self.policies.iter().map(ObservedFile::signature).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceBinding {
    root: Arc<SourceRoot>,
    source: PathBuf,
    bundle: PathBuf,
    scope: SkillDirectorySource,
    mode: Option<String>,
    signature: InputSignature,
}

impl PartialEq for SourceBinding {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && self.source == other.source
            && self.bundle == other.bundle
            && self.scope == other.scope
            && self.mode == other.mode
    }
}
impl Eq for SourceBinding {}

impl SourceBinding {
    /// Tagged physical/raw identity from this existing canonical capture.
    pub(crate) fn metadata_identity(&self) -> String {
        fn physical(hash: &mut Sha256, value: PhysicalIdentity) {
            hash.update(value.0.to_le_bytes());
            hash.update(value.1.to_le_bytes());
        }
        fn file(hash: &mut Sha256, value: &FileSignature) {
            match value {
                FileSignature::Error => hash.update([0]),
                FileSignature::Absent(parent) => {
                    hash.update([1]);
                    physical(hash, *parent);
                }
                FileSignature::Present(identity, bytes) => {
                    hash.update([2]);
                    physical(hash, *identity);
                    hash.update(bytes);
                }
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"bamboo-skill-source-v1");
        physical(&mut hash, self.signature.source);
        hash.update(self.signature.anchor.to_le_bytes());
        physical(&mut hash, self.signature.bundle);
        file(&mut hash, &self.signature.main);
        for policy in &self.signature.policies {
            file(&mut hash, policy);
        }
        for value in [
            self.source.as_os_str().as_encoded_bytes(),
            self.bundle.as_os_str().as_encoded_bytes(),
            self.mode.as_deref().unwrap_or_default().as_bytes(),
            crate::WorkflowSource::from(self.scope).as_str().as_bytes(),
        ] {
            hash.update(value.len().to_le_bytes());
            hash.update(value);
        }
        hex::encode(hash.finalize())
    }

    pub(crate) fn main_locator(&self) -> String {
        self.source
            .join(&self.bundle)
            .join("SKILL.md")
            .to_string_lossy()
            .into_owned()
    }

    pub(crate) fn root_locator(&self) -> String {
        self.source.to_string_lossy().into_owned()
    }

    pub(crate) fn scope(&self) -> SkillDirectorySource {
        self.scope
    }

    pub(crate) fn validate(
        &self,
        pool: &SourcePool,
        auxiliary: &HashMap<String, Arc<Vec<u8>>>,
        limit: usize,
    ) -> io::Result<()> {
        // Reopen the trusted locator to catch source-root replacement, then walk
        // through that capability. Parsing is never repeated during validation.
        let mut current = pool.inputs(&self.source, &self.bundle, limit)?;
        if let Some(error) = current.openai_error.take() {
            return Err(error);
        }
        if self.root.identity != current.root.identity || self.signature != current.signature() {
            return Err(io::Error::other("Skill source changed during capture"));
        }
        for (path, observed) in POLICY_FILES.into_iter().zip(&self.signature.policies) {
            let agrees = match (observed, auxiliary.get(path)) {
                (FileSignature::Absent(_), None) => true,
                (FileSignature::Present(_, hash), Some(bytes)) => *hash == digest(bytes),
                _ => false,
            };
            if !agrees {
                return Err(io::Error::other(
                    "Skill policy and auxiliary snapshot disagree",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_charged(
        &self,
        pool: &SourcePool,
        auxiliary: &HashMap<String, Arc<Vec<u8>>>,
        limit: usize,
        budget: &SelectedBudget,
    ) -> io::Result<()> {
        let root = pool.admit(&self.source)?;
        let opened = pool.walk(&root.dir, &self.bundle)?;
        let dir = opened.as_ref().map(|value| &value.dir).unwrap_or(&root.dir);
        let signature = InputSignature {
            source: root.identity,
            anchor: root.anchor,
            bundle: physical(&dir.dir_metadata()?)?,
            main: pool.probe(dir, Path::new("SKILL.md"), limit, budget)?,
            policies: POLICY_FILES
                .iter()
                .map(|path| pool.probe(dir, Path::new(path), limit, budget))
                .collect::<io::Result<_>>()?,
        };
        if signature != self.signature {
            return Err(io::Error::other("Skill source changed during probe"));
        }
        for (path, observed) in POLICY_FILES.into_iter().zip(&self.signature.policies) {
            if !match (observed, auxiliary.get(path)) {
                (FileSignature::Absent(_), None) => true,
                (FileSignature::Present(_, hash), Some(bytes)) => *hash == digest(bytes),
                _ => false,
            } {
                return Err(io::Error::other(
                    "Skill policy and auxiliary snapshot disagree",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn selected(
        &self,
        pool: &SourcePool,
        resource: &Path,
        expected: Option<&[u8]>,
        limit: usize,
        budget: &SelectedBudget,
        materialize: bool,
    ) -> io::Result<(String, Option<SelectedBuffer>)> {
        let root = pool.admit(&self.source)?;
        let opened = pool.walk(&root.dir, &self.bundle)?;
        let dir = opened.as_ref().map(|value| &value.dir).unwrap_or(&root.dir);
        if root.identity != self.root.identity
            || physical(&dir.dir_metadata()?)? != self.signature.bundle
        {
            return Err(io::Error::other("selected Skill root/bundle changed"));
        }
        let mut contents = None;
        let signature = if materialize {
            let (identity, bytes) = pool.visit(dir, resource, limit, |file, _, size| {
                let mut buffer = budget
                    .buffer(size.saturating_add(1), true)
                    .map_err(io::Error::other)?;
                let mut length = 0;
                while length < buffer.contents.len() {
                    let count = file.read(&mut buffer.contents[length..])?;
                    if count == 0 {
                        break;
                    }
                    length += count;
                }
                if length > size {
                    return Err(io::Error::other("selected Skill grew during read"));
                }
                buffer.contents.truncate(length);
                Ok(buffer)
            })?;
            let bytes = bytes.ok_or_else(|| io::Error::other("selected Skill file absent"))?;
            let signature = FileSignature::Present(identity, digest(&bytes.contents));
            contents = Some(bytes);
            signature
        } else {
            pool.probe(dir, resource, limit, budget)?
        };
        let agrees = if let Some(expected) = expected {
            matches!(&signature, FileSignature::Present(_, hash) if *hash == digest(expected))
        } else {
            signature == self.signature.main
        };
        if !agrees || !matches!(signature, FileSignature::Present(_, _)) {
            return Err(io::Error::other("selected Skill differs from publication"));
        }
        let mut hash = Sha256::new();
        hash.update(self.metadata_identity());
        hash.update(resource.as_os_str().as_encoded_bytes());
        if let FileSignature::Present(identity, digest) = signature {
            hash.update(identity.0.to_le_bytes());
            hash.update(identity.1.to_le_bytes());
            hash.update(digest);
        }
        Ok((hex::encode(hash.finalize()), contents))
    }

    #[cfg(test)]
    pub(crate) fn same_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.root, &other.root)
    }

    #[cfg(test)]
    pub(crate) fn anchor(&self) -> u64 {
        self.root.anchor
    }
}

#[derive(Debug)]
pub(crate) struct CapturedSkill {
    pub main: String,
    pub metadata: BundleMetadata,
    pub policies: HashMap<String, Arc<Vec<u8>>>,
    pub binding: SourceBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CandidateKey {
    file: PathBuf,
    source: SkillDirectorySource,
    mode: Option<String>,
}

impl CandidateKey {
    pub(crate) fn new(file: PathBuf, source: SkillDirectorySource, mode: Option<String>) -> Self {
        Self { file, source, mode }
    }
    pub(crate) fn for_record(record: &LoadedSkillRecord) -> Self {
        Self::new(
            record.skill_file.clone(),
            record.source,
            record.mode.clone(),
        )
    }
}

pub(crate) type CapturedSources = HashMap<CandidateKey, Arc<CapturedSkill>>;
