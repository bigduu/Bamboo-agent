//! Restricted configuration-share transactions on the existing credential
//! journal. This layer accepts only prepared encrypted credential bytes. It
//! never loads encryption keys, decrypts values, hydrates environment variables,
//! publishes process state, or starts a runtime.
//!
//! The durable manifest is the commit point. Before it, only private staging
//! files change. After it, existing configuration readers fail closed until
//! all four members have rolled forward and the manifest is complete.

use super::*;
use crate::config_store::LockedShareFileStore;
use crate::{EnvSection, ProvidersSection};

const OPERATIONS_FILE: &str = "config-share-operations.json";
const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;
/// Fail closed at capacity; evicting a receipt would break replay idempotency.
pub const MAX_SHARE_OPERATIONS: usize = 4096;
pub const SHARE_TRANSACTION_INVALID: &str = "configuration-share transaction is invalid";
pub const SHARE_OPERATION_ID_REUSED: &str = "configuration-share operation ID was already used";
pub const SHARE_OPERATION_CAPACITY_REACHED: &str =
    "configuration-share operation receipt capacity reached";
const MEMBERS: [&str; 4] = [CREDENTIALS_FILE, PROVIDERS_FILE, ENV_FILE, OPERATIONS_FILE];

/// Ordinary file operations call this after obtaining their member lock. It
/// never obtains another lock, creates files or runs recovery.
pub(crate) fn ensure_no_pending_share_member(path: &Path) -> ConfigStoreResult<()> {
    if !path
        .file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| MEMBERS.contains(&name))
    {
        return Ok(());
    }
    let data_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let Some(bytes) = bounded_read(&data_dir.join(MANIFEST_FILE))? else {
        return Ok(());
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    super::validate_manifest(&manifest)?;
    if manifest.migration_scope != Some(MigrationScope::ConfigShare) {
        return Ok(());
    }
    if manifest.state != MigrationState::Complete {
        return Err(ConfigStoreError::Validation(
            "configuration-share transaction is pending".into(),
        ));
    }
    Ok(())
}

/// The sole internal bypass of the pending guard. The caller already owns
/// the shared migration lock. Open relative to a pinned directory, rejecting
/// links, with the same primitive as the existing journal lock.
pub(crate) fn lock_member(path: &Path) -> ConfigStoreResult<File> {
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(invalid)?;
    if !MEMBERS.contains(&name) {
        return Err(invalid());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file = open_migration_lock(&parent.join(format!("{name}.lock")))?;
    file.lock()?;
    Ok(file)
}

fn lock_members(data_dir: &Path) -> ConfigStoreResult<Vec<LockedShareFileStore>> {
    MEMBERS
        .iter()
        .map(|name| {
            AtomicFileStore::new(data_dir.join(name))
                .sensitive(*name == CREDENTIALS_FILE)
                .lock_for_share_transaction()
        })
        .collect()
}

fn lock_existing_members_for_read(data_dir: &Path) -> ConfigStoreResult<Vec<MigrationLock>> {
    MEMBERS
        .iter()
        .take(3)
        .map(|name| {
            let file = open_existing_regular_file(&data_dir.join(format!("{name}.lock")))?;
            file.lock_shared()?;
            Ok(MigrationLock(file))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareRevisions {
    pub providers: u64,
    pub env: u64,
    pub credentials: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareOperationReceipt {
    pub operation_id: Uuid,
    pub revisions: ShareRevisions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShareOperationStatus {
    Committed {
        operation_id: Uuid,
        revisions: ShareRevisions,
    },
    Pending {
        operation_id: Uuid,
    },
    NotFound {
        operation_id: Uuid,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReceipt {
    request_sha256: String,
    receipt: ShareOperationReceipt,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationLedger {
    version: u32,
    operations: BTreeMap<Uuid, StoredReceipt>,
}

impl Default for OperationLedger {
    fn default() -> Self {
        Self {
            version: 1,
            operations: BTreeMap::new(),
        }
    }
}

/// Byte-presence is part of the CAS base, including revision-zero documents.
/// No Debug implementation: encrypted documents are intentionally opaque.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ShareTransactionSnapshot {
    pub providers: Option<Vec<u8>>,
    pub env: Option<Vec<u8>>,
    pub credentials: Option<Vec<u8>>,
    pub revisions: ShareRevisions,
}

/// Prepared by the typed import planner, never deserialized from an API.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ShareTransactionCandidate {
    pub providers: Vec<u8>,
    pub env: Vec<u8>,
    pub credentials: Vec<u8>,
}

fn invalid() -> ConfigStoreError {
    ConfigStoreError::Validation(SHARE_TRANSACTION_INVALID.into())
}

fn indeterminate() -> ConfigStoreError {
    ConfigStoreError::CommitIndeterminate(
        "configuration-share commit requires operation-status query and recovery".into(),
    )
}

fn bounded_read(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    let file = match open_existing_regular_file(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    bounded_file(file).map(Some)
}

fn bounded_file(file: File) -> ConfigStoreResult<Vec<u8>> {
    if file.metadata()?.len() > MAX_DOCUMENT_BYTES {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
        return Err(invalid());
    }
    Ok(bytes)
}

fn bounded_managed_read(
    data_dir: &Path,
    directory: &str,
    name: &str,
) -> ConfigStoreResult<Vec<u8>> {
    validate_relative_component(OsStr::new(directory))?;
    validate_relative_component(OsStr::new(name))?;
    #[cfg(any(unix, windows))]
    let file = {
        let directory = open_managed_directory(data_dir, directory)?;
        open_relative_no_follow(&directory, OsStr::new(name), RelativeKind::RegularFile)?
    };
    #[cfg(not(any(unix, windows)))]
    let file = open_existing_regular_file(&data_dir.join(directory).join(name))?;
    bounded_file(file)
}

fn ledger_from_bytes(bytes: Option<&[u8]>) -> ConfigStoreResult<OperationLedger> {
    let Some(bytes) = bytes else {
        return Ok(OperationLedger::default());
    };
    let ledger: OperationLedger = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if ledger.version != 1
        || ledger.operations.len() > MAX_SHARE_OPERATIONS
        || ledger.operations.iter().any(|(id, item)| {
            id.is_nil() || *id != item.receipt.operation_id || !is_sha256_hex(&item.request_sha256)
        })
    {
        return Err(invalid());
    }
    Ok(ledger)
}

fn load_snapshot(data_dir: &Path) -> ConfigStoreResult<ShareTransactionSnapshot> {
    if !crate::section_layout_is_active(data_dir)? {
        return Err(invalid());
    }
    let providers = bounded_read(&data_dir.join(PROVIDERS_FILE))?;
    let env = bounded_read(&data_dir.join(ENV_FILE))?;
    let credentials = bounded_read(&data_dir.join(CREDENTIALS_FILE))?;
    let revisions = ShareRevisions {
        providers: section_revision(PROVIDERS_FILE, providers.as_deref())?,
        env: section_revision(ENV_FILE, env.as_deref())?,
        credentials: credentials
            .as_deref()
            .map(CredentialStore::validate_section_document_revision)
            .transpose()
            .map_err(|_| invalid())?
            .unwrap_or(0),
    };
    Ok(ShareTransactionSnapshot {
        providers,
        env,
        credentials,
        revisions,
    })
}

fn section_revision(name: &str, bytes: Option<&[u8]>) -> ConfigStoreResult<u64> {
    bytes
        .map(|bytes| crate::section_facade::validate_section_envelope(name, bytes, 0))
        .transpose()
        .map_err(|_| invalid())
        .map(|value| value.unwrap_or(0))
}

/// Read a coherent encrypted snapshot without creating lock files, migrating,
/// repairing, or recovering anything. Uninitialized/pending authorities fail
/// closed. The process mutex is the same one as all credential writers use.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn read_share_snapshot(data_dir: &Path) -> ConfigStoreResult<ShareTransactionSnapshot> {
    let key = data_dir.canonicalize()?;
    let process_lock = {
        let mut locks = PROCESS_MIGRATION_LOCKS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(Mutex::new(()));
            locks.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    let _process_guard = process_lock
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let lock = open_existing_regular_file(&data_dir.join(LOCK_FILE))?;
    lock.lock_shared()?;
    let _lock = MigrationLock(lock);
    crate::ensure_provider_mcp_migration_ready(data_dir)?;
    let _member_locks = lock_existing_members_for_read(data_dir)?;
    load_snapshot(data_dir)
}

/// Operation status is deliberately read-only. A pending manifest wins even
/// when the last receipt file has already been installed after a crash.
pub fn read_share_operation(
    data_dir: &Path,
    operation_id: Uuid,
) -> ConfigStoreResult<ShareOperationStatus> {
    if operation_id.is_nil() {
        return Err(invalid());
    }
    for _ in 0..4 {
        let before = bounded_read(&data_dir.join(MANIFEST_FILE))?;
        let ledger_bytes = bounded_read(&data_dir.join(OPERATIONS_FILE))?;
        if before != bounded_read(&data_dir.join(MANIFEST_FILE))? {
            continue;
        }
        if let Some(bytes) = before {
            let manifest: MigrationManifest =
                serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            super::validate_manifest(&manifest)?;
            if manifest.migration_scope == Some(MigrationScope::ConfigShare)
                && manifest.transaction_id == operation_id.to_string()
                && manifest.state != MigrationState::Complete
            {
                return Ok(ShareOperationStatus::Pending { operation_id });
            }
        }
        return Ok(
            match ledger_from_bytes(ledger_bytes.as_deref())?
                .operations
                .get(&operation_id)
            {
                Some(item) => ShareOperationStatus::Committed {
                    operation_id,
                    revisions: item.receipt.revisions,
                },
                None => ShareOperationStatus::NotFound { operation_id },
            },
        );
    }
    Err(ConfigStoreError::Validation(
        "configuration-share status changed during read".into(),
    ))
}

/// The callback is internal and runs under the shared credential lock. It
/// receives current encrypted bytes only after recovery, replay lookup and
/// revision checks. The digest must cover the original encrypted envelope,
/// decisions and revision preconditions, never a password or plaintext.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn commit_share_transaction(
    data_dir: &Path,
    operation_id: Uuid,
    request_sha256: &str,
    expected: ShareRevisions,
    prepare: impl FnOnce(&ShareTransactionSnapshot) -> ConfigStoreResult<ShareTransactionCandidate>,
) -> ConfigStoreResult<ShareOperationReceipt> {
    commit_inner(
        data_dir,
        operation_id,
        request_sha256,
        expected,
        prepare,
        ShareFault::None,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ShareFault {
    None,
    AfterStage(usize),
    AfterStaging,
    AfterJournal,
    AfterManifest,
    AfterInstall(usize),
    AfterComplete,
    AfterJournalCleanup,
    AfterCleanup,
}

fn commit_inner(
    data_dir: &Path,
    operation_id: Uuid,
    request_sha256: &str,
    expected: ShareRevisions,
    prepare: impl FnOnce(&ShareTransactionSnapshot) -> ConfigStoreResult<ShareTransactionCandidate>,
    fault: ShareFault,
) -> ConfigStoreResult<ShareOperationReceipt> {
    if operation_id.is_nil() || !is_sha256_hex(request_sha256) {
        return Err(invalid());
    }
    with_provider_mcp_migration_lock(data_dir, || {
        recover_committed(
            data_dir,
            #[cfg(test)]
            None,
        )
        .map_err(|_| indeterminate())?;
        // Normal commit and recovery use the same fixed member lock order.
        // Keep all guards through snapshot, preparation and durable finish.
        let member_locks = lock_members(data_dir)?;
        let ledger_original = bounded_read(&data_dir.join(OPERATIONS_FILE))?;
        let mut ledger = ledger_from_bytes(ledger_original.as_deref())?;
        if let Some(item) = ledger.operations.get(&operation_id) {
            return if item.request_sha256 == request_sha256 {
                Ok(item.receipt.clone())
            } else {
                Err(ConfigStoreError::Validation(
                    SHARE_OPERATION_ID_REUSED.into(),
                ))
            };
        }
        if ledger.operations.len() == MAX_SHARE_OPERATIONS {
            return Err(ConfigStoreError::Validation(
                SHARE_OPERATION_CAPACITY_REACHED.into(),
            ));
        }
        let snapshot = load_snapshot(data_dir)?;
        for (expected, actual) in [
            (expected.providers, snapshot.revisions.providers),
            (expected.env, snapshot.revisions.env),
            (expected.credentials, snapshot.revisions.credentials),
        ] {
            if expected != actual {
                return Err(ConfigStoreError::Conflict { expected, actual });
            }
        }
        let candidate = prepare(&snapshot)?;
        let revisions = validate_candidate(&snapshot, &candidate)?;
        let receipt = ShareOperationReceipt {
            operation_id,
            revisions,
        };
        ledger.operations.insert(
            operation_id,
            StoredReceipt {
                request_sha256: request_sha256.into(),
                receipt: receipt.clone(),
            },
        );
        let ledger_candidate = serde_json::to_vec_pretty(&ledger)?;
        let candidates = [
            candidate.credentials,
            candidate.providers,
            candidate.env,
            ledger_candidate,
        ];
        let originals = [
            snapshot.credentials,
            snapshot.providers,
            snapshot.env,
            ledger_original,
        ];
        discard_uncommitted(data_dir)?;
        cleanup_orphan_transaction_dirs(data_dir)?;
        let stage_dir_name = format!("{STAGE_PREFIX}{operation_id}");
        let stage_dir = data_dir.join(&stage_dir_name);
        create_private_dir(&stage_dir)?;
        sync_dir(data_dir)?;
        let mut files = Vec::new();
        for (index, name) in MEMBERS.iter().enumerate() {
            if candidates[index].len() as u64 > MAX_DOCUMENT_BYTES {
                return Err(invalid());
            }
            // All staging files are private, including secret-free metadata.
            // No backup encryption or key initialization is needed: recovery
            // is strictly roll-forward from immutable staged candidates.
            AtomicFileStore::new(stage_dir.join(name))
                .sensitive(true)
                .write_bytes_without_backup(&candidates[index])?;
            files.push(StagedFile {
                name: (*name).into(),
                staged_name: (*name).into(),
                sha256: sha256(&candidates[index]),
                original_sha256: originals[index].as_deref().map(sha256),
                original_present: Some(originals[index].is_some()),
                migration_generation: None,
                sensitive: *name == CREDENTIALS_FILE,
                install_mode: InstallMode::Migration,
                expected_revision: match *name {
                    CREDENTIALS_FILE => Some(revisions.credentials),
                    PROVIDERS_FILE => Some(revisions.providers),
                    ENV_FILE => Some(revisions.env),
                    _ => None,
                },
                transaction_base_sha256: None,
                touched_credential_refs: Vec::new(),
                required_credential_refs: Vec::new(),
                touched_env_names: Vec::new(),
            });
            if fault == ShareFault::AfterStage(index) {
                return Err(invalid());
            }
        }
        sync_dir(&stage_dir)?;
        if fault == ShareFault::AfterStaging {
            return Err(invalid());
        }
        // A raw editor does not participate in the shared lock. Bind presence
        // and exact bytes again before crossing the commit point.
        for (index, name) in MEMBERS.iter().enumerate() {
            if bounded_read(&data_dir.join(name))? != originals[index] {
                return Err(invalid());
            }
        }
        let manifest = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: operation_id.to_string(),
            stage_dir: stage_dir_name,
            state: MigrationState::Pending,
            exact_scope: None,
            migration_scope: Some(MigrationScope::ConfigShare),
            source_attestation: BTreeMap::new(),
            files,
        };
        validate_manifest(&manifest)?;
        write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
        if fault == ShareFault::AfterJournal {
            return Err(invalid());
        }
        // A failed atomic rename/fsync may already have crossed the commit
        // point, so every error from this attempted write is indeterminate.
        write_manifest(data_dir.join(MANIFEST_FILE), &manifest).map_err(|_| indeterminate())?;
        #[cfg(test)]
        run_test_hook(data_dir, ShareHookPoint::AfterManifest);
        if fault == ShareFault::AfterManifest {
            return Err(indeterminate());
        }
        install_with_fault(data_dir, &manifest, &member_locks, fault)
            .map_err(|_| indeterminate())?;
        finish_with_fault(data_dir, manifest, fault).map_err(|_| indeterminate())?;
        Ok(receipt)
    })
}

fn candidate_section_revision(
    name: &str,
    original: Option<&[u8]>,
    candidate: &[u8],
) -> ConfigStoreResult<u64> {
    let revision = section_revision(name, Some(candidate))?;
    let (old_revision, old_data) = match original {
        Some(bytes) => {
            let value: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
            (section_revision(name, Some(bytes))?, value["data"].clone())
        }
        None => (
            0,
            if name == PROVIDERS_FILE {
                serde_json::to_value(ProvidersSection::default())?
            } else {
                serde_json::to_value(EnvSection::default())?
            },
        ),
    };
    let value: Value = serde_json::from_slice(candidate).map_err(|_| invalid())?;
    let changed = value["data"] != old_data;
    if revision
        != old_revision
            .checked_add(u64::from(changed))
            .ok_or_else(invalid)?
    {
        return Err(invalid());
    }
    Ok(revision)
}

fn referenced_credentials(
    providers: &[u8],
    env: &[u8],
) -> ConfigStoreResult<BTreeSet<CredentialRef>> {
    let providers_value: Value = serde_json::from_slice(providers).map_err(|_| invalid())?;
    let providers: ProvidersSection =
        serde_json::from_value(providers_value["data"].clone()).map_err(|_| invalid())?;
    let env_value: Value = serde_json::from_slice(env).map_err(|_| invalid())?;
    let env: EnvSection =
        serde_json::from_value(env_value["data"].clone()).map_err(|_| invalid())?;
    let mut refs: BTreeSet<_> = providers
        .provider_instances
        .values()
        .filter_map(|item| item.credential_ref.clone())
        .collect();
    refs.extend(env.0.iter().filter_map(|item| item.credential_ref.clone()));
    for reference in [
        providers
            .providers
            .openai
            .as_ref()
            .and_then(|item| item.credential_ref.clone()),
        providers
            .providers
            .anthropic
            .as_ref()
            .and_then(|item| item.credential_ref.clone()),
        providers
            .providers
            .gemini
            .as_ref()
            .and_then(|item| item.credential_ref.clone()),
        providers
            .providers
            .bodhi
            .as_ref()
            .and_then(|item| item.credential_ref.clone()),
    ]
    .into_iter()
    .flatten()
    {
        refs.insert(reference);
    }
    Ok(refs)
}

fn validate_candidate(
    snapshot: &ShareTransactionSnapshot,
    candidate: &ShareTransactionCandidate,
) -> ConfigStoreResult<ShareRevisions> {
    if [&candidate.providers, &candidate.env, &candidate.credentials]
        .iter()
        .any(|bytes| bytes.len() as u64 > MAX_DOCUMENT_BYTES)
    {
        return Err(invalid());
    }
    let providers = candidate_section_revision(
        PROVIDERS_FILE,
        snapshot.providers.as_deref(),
        &candidate.providers,
    )?;
    let env = candidate_section_revision(ENV_FILE, snapshot.env.as_deref(), &candidate.env)?;
    let refs = referenced_credentials(&candidate.providers, &candidate.env)?;
    let credentials = CredentialStore::validate_share_document_change(
        snapshot.credentials.as_deref().unwrap_or_default(),
        &candidate.credentials,
        &refs,
    )
    .map_err(|_| invalid())?;
    CredentialStore::ensure_share_refs_present(&candidate.credentials, &refs)
        .map_err(|_| invalid())?;
    Ok(ShareRevisions {
        providers,
        env,
        credentials,
    })
}

pub(super) fn validate_manifest(manifest: &MigrationManifest) -> ConfigStoreResult<()> {
    let id = Uuid::parse_str(&manifest.transaction_id).map_err(|_| invalid())?;
    if id.is_nil()
        || manifest.transaction_id != id.to_string()
        || manifest.version != MIGRATION_VERSION
        || manifest.stage_dir != format!("{STAGE_PREFIX}{id}")
        || manifest.exact_scope.is_some()
        || manifest.migration_scope != Some(MigrationScope::ConfigShare)
        || !manifest.source_attestation.is_empty()
        || manifest.state == MigrationState::Aborting
        || manifest.files.len() != MEMBERS.len()
        || manifest.files.iter().zip(MEMBERS).any(|(file, name)| {
            file.name != name
                || file.staged_name != name
                || !is_sha256_hex(&file.sha256)
                || file.original_present.is_none()
                || match file.original_present {
                    Some(true) => file
                        .original_sha256
                        .as_deref()
                        .is_none_or(|hash| !is_sha256_hex(hash)),
                    Some(false) => file.original_sha256.is_some(),
                    None => true,
                }
                || file.migration_generation.is_some()
                || file.install_mode != InstallMode::Migration
                || file.sensitive != (name == CREDENTIALS_FILE)
                || file.expected_revision.is_some() != (name != OPERATIONS_FILE)
                || file.transaction_base_sha256.is_some()
                || !file.touched_credential_refs.is_empty()
                || !file.required_credential_refs.is_empty()
                || !file.touched_env_names.is_empty()
        })
    {
        return Err(invalid());
    }
    Ok(())
}

fn receipt_for_manifest(
    manifest: &MigrationManifest,
    ledger: &OperationLedger,
) -> ConfigStoreResult<ShareOperationReceipt> {
    let id = Uuid::parse_str(&manifest.transaction_id).map_err(|_| invalid())?;
    let receipt = ledger
        .operations
        .get(&id)
        .ok_or_else(invalid)?
        .receipt
        .clone();
    let revisions = ShareRevisions {
        credentials: manifest.files[0].expected_revision.ok_or_else(invalid)?,
        providers: manifest.files[1].expected_revision.ok_or_else(invalid)?,
        env: manifest.files[2].expected_revision.ok_or_else(invalid)?,
    };
    if receipt.revisions != revisions {
        return Err(invalid());
    }
    Ok(receipt)
}

pub(super) fn install_pending(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let locks = lock_members(data_dir)?;
    install_with_fault(data_dir, manifest, &locks, ShareFault::None)
}

pub(super) fn recover_pending(
    data_dir: &Path,
    manifest: MigrationManifest,
) -> ConfigStoreResult<()> {
    let locks = lock_members(data_dir)?;
    install_with_fault(data_dir, &manifest, &locks, ShareFault::None)?;
    finish_with_fault(data_dir, manifest, ShareFault::None)
}

fn install_with_fault(
    data_dir: &Path,
    manifest: &MigrationManifest,
    member_locks: &[LockedShareFileStore],
    fault: ShareFault,
) -> ConfigStoreResult<()> {
    validate_manifest(manifest)?;
    if manifest.state != MigrationState::Pending {
        return Err(invalid());
    }
    validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let mut staged = Vec::new();
    // Verify the entire transaction before installing its first member.
    for file in &manifest.files {
        let bytes = bounded_managed_read(data_dir, &manifest.stage_dir, &file.staged_name)?;
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES || sha256(&bytes) != file.sha256 {
            return Err(invalid());
        }
        let revision = match file.name.as_str() {
            CREDENTIALS_FILE => Some(
                CredentialStore::validate_section_document_revision(&bytes)
                    .map_err(|_| invalid())?,
            ),
            PROVIDERS_FILE | ENV_FILE => Some(section_revision(&file.name, Some(&bytes))?),
            _ => {
                ledger_from_bytes(Some(&bytes))?;
                None
            }
        };
        if revision != file.expected_revision {
            return Err(invalid());
        }
        let current = bounded_read(&data_dir.join(&file.name))?;
        let installed = current
            .as_deref()
            .is_some_and(|bytes| sha256(bytes) == file.sha256);
        if !installed && !base_matches(current.as_deref(), file) {
            return Err(invalid());
        }
        staged.push(bytes);
    }
    let refs = referenced_credentials(&staged[1], &staged[2])?;
    CredentialStore::ensure_share_refs_present(&staged[0], &refs).map_err(|_| invalid())?;
    receipt_for_manifest(manifest, &ledger_from_bytes(Some(&staged[3]))?)?;
    for (index, file) in manifest.files.iter().enumerate() {
        let target = data_dir.join(&file.name);
        let current = bounded_read(&target)?;
        if !current
            .as_deref()
            .is_some_and(|bytes| sha256(bytes) == file.sha256)
            && !member_locks[index]
                .write_bytes_if_state(file.original_sha256.as_deref(), &staged[index])?
        {
            return Err(invalid());
        }
        if fault == ShareFault::AfterInstall(index) {
            return Err(invalid());
        }
        #[cfg(test)]
        if file.name == PROVIDERS_FILE {
            run_test_hook(data_dir, ShareHookPoint::AfterProviderInstall);
        }
    }
    Ok(())
}

fn base_matches(current: Option<&[u8]>, file: &StagedFile) -> bool {
    match (file.original_present, current) {
        (Some(false), None) => true,
        (Some(true), Some(bytes)) => {
            file.original_sha256.as_deref() == Some(sha256(bytes).as_str())
        }
        _ => false,
    }
}

pub(super) fn validate_completed(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    validate_manifest(manifest)?;
    let bytes = bounded_read(&data_dir.join(OPERATIONS_FILE))?.ok_or_else(invalid)?;
    receipt_for_manifest(manifest, &ledger_from_bytes(Some(&bytes))?)?;
    Ok(())
}

pub(super) fn finish(data_dir: &Path, manifest: MigrationManifest) -> ConfigStoreResult<()> {
    let _locks = lock_members(data_dir)?;
    finish_with_fault(data_dir, manifest, ShareFault::None)
}

fn finish_with_fault(
    data_dir: &Path,
    mut manifest: MigrationManifest,
    fault: ShareFault,
) -> ConfigStoreResult<()> {
    validate_completed(data_dir, &manifest)?;
    // This check belongs only to the pending-to-complete transition. A later
    // normal configuration writer may advance a historical completed member,
    // so completed-manifest recovery validates its durable receipt instead.
    for file in &manifest.files {
        let bytes = bounded_read(&data_dir.join(&file.name))?.ok_or_else(invalid)?;
        if sha256(&bytes) != file.sha256 {
            return Err(invalid());
        }
    }
    manifest.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    if fault == ShareFault::AfterComplete {
        return Err(invalid());
    }
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    if fault == ShareFault::AfterJournalCleanup {
        return Err(invalid());
    }
    cleanup_transaction_dirs(data_dir, &manifest)?;
    if fault == ShareFault::AfterCleanup {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShareHookPoint {
    AfterManifest,
    AfterProviderInstall,
}

#[cfg(test)]
type ShareTestHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
fn test_hooks() -> &'static Mutex<BTreeMap<(PathBuf, ShareHookPoint), ShareTestHook>> {
    static HOOKS: std::sync::OnceLock<Mutex<BTreeMap<(PathBuf, ShareHookPoint), ShareTestHook>>> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn run_test_hook(data_dir: &Path, point: ShareHookPoint) {
    let hook = test_hooks()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&(data_dir.to_path_buf(), point));
    if let Some(hook) = hook {
        hook();
    }
}
