use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::{canonical_bytes, content_hash, Error, Result, ScopeBinding, Snapshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Health {
    Writable,
    ReadOnly { reason: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeWrite,
    AfterWrite,
    BeforeFileSync,
    AfterFileSync,
    BeforeDirectorySync,
    AfterDirectorySync,
    BeforeObjectRename,
    AfterObjectRename,
    BeforeHeadRename,
    AfterHeadRename,
}

/// A host/test hook: invoked at every actual persistence boundary. It may return
/// an I/O failure or terminate a fixture process; never installed by a Worker.
pub type PublicationFault = Arc<dyn Fn(FaultPoint) -> std::io::Result<()> + Send + Sync>;

#[derive(Debug, Serialize, Deserialize)]
struct Commit {
    schema: u32,
    seq: u64,
    parent: Option<String>,
    manifest: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    schema: u32,
    objects: BTreeMap<String, String>,
}

/// Host-only scope authority. Client reads must use a service published snapshot.
pub struct FileStore {
    root: PathBuf,
    _writer_lock: File,
    pub health: Health,
    pub published: Option<(String, Snapshot)>,
    fault: Option<PublicationFault>,
}

fn checked_file(path: &Path) -> Result<File> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(Error::AuthorityUnavailable("symlink in authority".into()));
    }
    Ok(File::open(path)?)
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

impl FileStore {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
    pub fn open(root: &Path, binding: ScopeBinding) -> Result<Self> {
        if !cfg!(any(target_os = "macos", target_os = "linux")) {
            return Err(Error::AuthorityUnavailable(
                "filesystem platform not validated".into(),
            ));
        }
        fs::create_dir_all(root)?;
        if fs::symlink_metadata(root)?.file_type().is_symlink() {
            return Err(Error::AuthorityUnavailable(
                "authority root is a symlink".into(),
            ));
        }
        let root = root.canonicalize()?;
        let lock_path = root.join("writer.lock");
        if lock_path.exists() && fs::symlink_metadata(&lock_path)?.file_type().is_symlink() {
            return Err(Error::AuthorityUnavailable(
                "writer lock is a symlink".into(),
            ));
        }
        let writer_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        writer_lock.try_lock_exclusive().map_err(|_| {
            Error::AuthorityUnavailable("another local writer holds the process lock".into())
        })?;
        let mut store = Self {
            root,
            _writer_lock: writer_lock,
            health: Health::Writable,
            published: None,
            fault: None,
        };
        let head = store.root.join("HEAD");
        if head.exists() {
            match store.load_head() {
                Ok((hash, snapshot)) if snapshot.binding == binding => {
                    store.published = Some((hash, snapshot));
                }
                Ok(_) => {
                    store.health = Health::ReadOnly {
                        reason: "scope binding mismatch".into(),
                    }
                }
                Err(error) => {
                    store.health = Health::ReadOnly {
                        reason: error.to_string(),
                    }
                }
            }
        } else if store.root.join("commits").exists() || store.root.join("objects").exists() {
            store.health = Health::ReadOnly {
                reason: "HEAD missing; history is not a recovery oracle".into(),
            };
        } else {
            for dir in ["objects", "manifests", "commits"] {
                fs::create_dir(store.root.join(dir))?;
            }
            store.sync_dir(&store.root)?;
            store.publish(Snapshot::empty(binding))?;
        }
        if store.root.join("BACKUP_READ_ONLY").exists() {
            store.health = Health::ReadOnly {
                reason: "backup requires verified stopped-authority migration".into(),
            };
        }
        Ok(store)
    }

    pub fn set_fault(&mut self, fault: Option<PublicationFault>) {
        self.fault = fault;
    }

    fn hit(&self, point: FaultPoint) -> Result<()> {
        if let Some(fault) = &self.fault {
            fault(point)?;
        }
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.hit(FaultPoint::BeforeDirectorySync)?;
        checked_file(dir)?.sync_all()?;
        self.hit(FaultPoint::AfterDirectorySync)
    }

    fn write_synced(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        self.hit(FaultPoint::BeforeWrite)?;
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.write_all(bytes)?;
        self.hit(FaultPoint::AfterWrite)?;
        self.hit(FaultPoint::BeforeFileSync)?;
        file.sync_all()?;
        self.hit(FaultPoint::AfterFileSync)
    }

    fn write_object(&self, category: &str, bytes: &[u8]) -> Result<String> {
        let hash = content_hash(bytes);
        let dir = self.root.join(category);
        if fs::symlink_metadata(&dir)?.file_type().is_symlink() {
            return Err(Error::AuthorityUnavailable(
                "authority directory is a symlink".into(),
            ));
        }
        let final_path = dir.join(&hash);
        if final_path.exists() {
            if self.read_object(category, &hash)? != bytes {
                return Err(Error::AuthorityUnavailable(
                    "immutable object mismatch".into(),
                ));
            }
        } else {
            // Unpublished staging never carries authority. A failed partial write
            // cannot poison the content-addressed name for an identical retry.
            let staging = dir.join(format!(".staging-{}", uuid::Uuid::new_v4()));
            self.write_synced(&staging, bytes)?;
            self.hit(FaultPoint::BeforeObjectRename)?;
            fs::rename(&staging, &final_path)?;
            self.hit(FaultPoint::AfterObjectRename)?;
            self.sync_dir(&dir)?;
        }
        Ok(hash)
    }

    fn read_object(&self, category: &str, hash: &str) -> Result<Vec<u8>> {
        if !valid_hash(hash) {
            return Err(Error::AuthorityUnavailable("invalid object hash".into()));
        }
        let dir = self.root.join(category);
        if fs::symlink_metadata(&dir)?.file_type().is_symlink() {
            return Err(Error::AuthorityUnavailable(
                "authority directory is a symlink".into(),
            ));
        }
        let mut bytes = Vec::new();
        checked_file(&dir.join(hash))?.read_to_end(&mut bytes)?;
        if content_hash(&bytes) != hash {
            return Err(Error::AuthorityUnavailable("object hash mismatch".into()));
        }
        Ok(bytes)
    }

    pub fn publish(&mut self, snapshot: Snapshot) -> Result<String> {
        if self.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(format!("{:?}", self.health)));
        }
        let mut renamed = false;
        let result = (|| {
            let mut value = serde_json::to_value(&snapshot)?
                .as_object()
                .cloned()
                .ok_or_else(|| Error::AuthorityUnavailable("snapshot is not an object".into()))?;
            let mut objects = BTreeMap::new();
            for category in [
                "tickets",
                "assignments",
                "requests",
                "submissions",
                "receipts",
                "intents",
                "resolutions",
            ] {
                let items = value
                    .remove(category)
                    .and_then(|v| v.as_object().cloned())
                    .ok_or_else(|| {
                        Error::AuthorityUnavailable("missing snapshot section".into())
                    })?;
                for (id, object) in items {
                    objects.insert(
                        format!("{category}:{id}"),
                        self.write_object("objects", &canonical_bytes(&object)?)?,
                    );
                }
            }
            objects.insert(
                "header".into(),
                self.write_object("objects", &canonical_bytes(&value)?)?,
            );
            let manifest = self.write_object(
                "manifests",
                &canonical_bytes(&Manifest { schema: 1, objects })?,
            )?;
            let commit = Commit {
                schema: 1,
                seq: snapshot.seq,
                parent: self.published.as_ref().map(|(hash, _)| hash.clone()),
                manifest,
            };
            let hash = self.write_object("commits", &canonical_bytes(&commit)?)?;
            let staged_head = self.root.join(format!(".HEAD-{}", uuid::Uuid::new_v4()));
            self.write_synced(&staged_head, hash.as_bytes())?;
            self.hit(FaultPoint::BeforeHeadRename)?;
            fs::rename(&staged_head, self.root.join("HEAD"))?;
            renamed = true;
            self.hit(FaultPoint::AfterHeadRename)?;
            self.sync_dir(&self.root)?;
            Ok(hash)
        })();
        match result {
            Ok(hash) => {
                self.published = Some((hash.clone(), snapshot));
                Ok(hash)
            }
            Err(error) => {
                if renamed {
                    self.health = Health::ReadOnly {
                        reason: format!("HEAD publication needs reconciliation: {error}"),
                    };
                }
                Err(error)
            }
        }
    }

    fn load_head(&self) -> Result<(String, Snapshot)> {
        let mut hash = String::new();
        checked_file(&self.root.join("HEAD"))?.read_to_string(&mut hash)?;
        let snapshot = self.load_snapshot(&hash)?;
        let mut next = Some(hash.clone());
        let mut seen = std::collections::BTreeSet::new();
        let mut upper_seq = None;
        while let Some(commit_hash) = next {
            if !seen.insert(commit_hash.clone()) {
                return Err(Error::AuthorityUnavailable("commit ancestry cycle".into()));
            }
            let ancestor = self.load_snapshot(&commit_hash)?;
            if ancestor.binding != snapshot.binding
                || upper_seq.is_some_and(|seq| ancestor.seq >= seq)
            {
                return Err(Error::AuthorityUnavailable(
                    "invalid commit ancestry".into(),
                ));
            }
            upper_seq = Some(ancestor.seq);
            let commit: Commit =
                serde_json::from_slice(&self.read_object("commits", &commit_hash)?)?;
            next = commit.parent;
        }
        Ok((hash, snapshot))
    }

    pub fn load_snapshot(&self, hash: &str) -> Result<Snapshot> {
        let commit: Commit = serde_json::from_slice(&self.read_object("commits", hash)?)?;
        let manifest: Manifest =
            serde_json::from_slice(&self.read_object("manifests", &commit.manifest)?)?;
        if commit.schema != 1 || manifest.schema != 1 {
            return Err(Error::AuthorityUnavailable("unsupported schema".into()));
        }
        let header = manifest
            .objects
            .get("header")
            .ok_or_else(|| Error::AuthorityUnavailable("manifest has no header".into()))?;
        let mut value: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&self.read_object("objects", header)?)?;
        for category in [
            "tickets",
            "assignments",
            "requests",
            "submissions",
            "receipts",
            "intents",
            "resolutions",
        ] {
            value.insert(category.into(), serde_json::json!({}));
        }
        for (key, hash) in &manifest.objects {
            if key == "header" {
                continue;
            }
            let (category, id) = key
                .split_once(':')
                .ok_or_else(|| Error::AuthorityUnavailable("malformed manifest key".into()))?;
            let section = value
                .get_mut(category)
                .and_then(|v| v.as_object_mut())
                .ok_or_else(|| Error::AuthorityUnavailable("unknown manifest section".into()))?;
            section.insert(
                id.into(),
                serde_json::from_slice(&self.read_object("objects", hash)?)?,
            );
        }
        let snapshot: Snapshot = serde_json::from_value(serde_json::Value::Object(value))?;
        if snapshot.schema != 1 || snapshot.seq != commit.seq {
            return Err(Error::AuthorityUnavailable(
                "snapshot header mismatch".into(),
            ));
        }
        Ok(snapshot)
    }

    pub(crate) fn parent_commit(&self, hash: &str) -> Result<Option<String>> {
        let commit: Commit = serde_json::from_slice(&self.read_object("commits", hash)?)?;
        Ok(commit.parent)
    }

    /// Export one fixed commit and every immutable reachable ancestor/object.
    /// The resulting directory is a backup, not a new writable authority.
    pub fn export(&self, destination: &Path) -> Result<String> {
        if destination.exists() {
            return Err(Error::InvalidTransition("export destination exists".into()));
        }
        let head = self
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no published snapshot".into()))?
            .0
            .clone();
        fs::create_dir(destination)?;
        for category in ["objects", "manifests", "commits"] {
            fs::create_dir(destination.join(category))?;
        }
        let mut next = Some(head.clone());
        while let Some(hash) = next {
            self.load_snapshot(&hash)?;
            let bytes = self.read_object("commits", &hash)?;
            let commit: Commit = serde_json::from_slice(&bytes)?;
            copy_synced(destination, "commits", &hash, &bytes)?;
            let bytes = self.read_object("manifests", &commit.manifest)?;
            let manifest: Manifest = serde_json::from_slice(&bytes)?;
            copy_synced(destination, "manifests", &commit.manifest, &bytes)?;
            for hash in manifest.objects.values() {
                copy_synced(
                    destination,
                    "objects",
                    hash,
                    &self.read_object("objects", hash)?,
                )?;
            }
            next = commit.parent;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination.join("HEAD"))?;
        file.write_all(head.as_bytes())?;
        file.sync_all()?;
        fs::write(
            destination.join("BACKUP_READ_ONLY"),
            b"Original authority and runs must stop before migration.",
        )?;
        checked_file(&destination.join("BACKUP_READ_ONLY"))?.sync_all()?;
        for category in ["objects", "manifests", "commits"] {
            checked_file(&destination.join(category))?.sync_all()?;
        }
        checked_file(destination)?.sync_all()?;
        if let Some(parent) = destination.parent() {
            checked_file(parent)?.sync_all()?;
        }
        Ok(head)
    }
}

fn copy_synced(destination: &Path, category: &str, hash: &str, bytes: &[u8]) -> Result<()> {
    let path = destination.join(category).join(hash);
    if path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
