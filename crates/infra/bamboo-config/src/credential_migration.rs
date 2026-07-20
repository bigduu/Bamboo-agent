//! Recoverable provider/MCP/external-broker credential extraction.
//!
//! Provider/MCP/root and external-broker planners are independent so a bad
//! optional broker document cannot disable the main configuration. Both use
//! one lock and manifest protocol: candidate bytes are staged and fsynced
//! before the commit point, and every reader recovers a committed transaction
//! before reading any credential transaction member.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    credential_ref, AtomicFileStore, ConfigStoreError, ConfigStoreResult, CredentialRef,
    CredentialStore, ProviderConfigs,
};

const MIGRATION_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "config-credential-migration.json";
const JOURNAL_FILE: &str = "config-credential-migration.journal.json";
const LOCK_FILE: &str = ".config-credential-migration.lock";
const STAGE_PREFIX: &str = ".config-credential-stage-v1-";
const BACKUP_PREFIX: &str = "config-credential-migration-backup-v1-";
const PROVIDERS_FILE: &str = "providers.json";
const MCP_FILE: &str = "mcp.json";
const BROKER_FILE: &str = "broker.json";
const CREDENTIALS_FILE: &str = "credentials.json";
const CONFIG_FILE: &str = "config.json";
const PROXY_AUTH_DOMAIN_KEYS: [&str; 5] = [
    "proxy_auth",
    "proxy_auth_encrypted",
    "http_proxy_auth_encrypted",
    "https_proxy_auth_encrypted",
    "proxy_auth_credential_ref",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialMigrationOutcome {
    pub migrated_credentials: usize,
    pub resumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationManifest {
    version: u32,
    transaction_id: String,
    stage_dir: String,
    state: MigrationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exact_scope: Option<ExactTransactionScope>,
    files: Vec<StagedFile>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MigrationState {
    Pending,
    Complete,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExactTransactionScope {
    ProxyAuth,
    EnvVars,
    Notifications,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedFile {
    name: String,
    staged_name: String,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    original_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    migration_generation: Option<u64>,
    sensitive: bool,
    #[serde(default)]
    install_mode: InstallMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_revision: Option<u64>,
    /// Immutable hash of the original credential document used for three-way
    /// merge. `original_sha256` advances after each rebase CAS attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_base_sha256: Option<String>,
    /// Exact credential transactions use these refs for a three-way rebase.
    /// Empty on migration members and on manifests produced before this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    touched_credential_refs: Vec<String>,
    /// Metadata publication is forbidden until each of these refs decrypts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_credential_refs: Vec<String>,
    /// Exact env transactions use names for per-entry three-way config merge.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    touched_env_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum InstallMode {
    #[default]
    Migration,
    Exact,
}

#[derive(Debug)]
struct PlannedSection {
    name: &'static str,
    bytes: Vec<u8>,
    original: Vec<u8>,
    migration_generation: u64,
}

#[derive(Debug)]
enum LegacySecret {
    Plaintext(String),
    Ciphertext(String),
}

#[derive(Debug)]
struct ExtractedSecret {
    credential_ref: CredentialRef,
    value: LegacySecret,
    migration_generation: u64,
    kind: ExtractedSecretKind,
    env_owner: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractedSecretKind {
    Other,
    ProxyAuth,
    EnvVar,
    NotificationNtfy,
    NotificationBark,
    ExternalBroker,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationFault {
    None,
    AfterStaging,
    AfterJournal,
    AfterManifest,
    AfterCredentials,
    AfterProviders,
    AfterMcp,
    AfterBroker,
    AfterConfig,
    AfterRebaseCredentialCommit,
    AfterRebaseStageWrite,
    AfterRebaseManifest,
    BeforeExactCommitCredentialRace,
    AfterExactCommitCredentialRace,
    AfterExactCommitUnrelatedCredentialRace,
    AfterExactCommitCredentialClearRace,
    AfterExactCredentialRebaseStage,
    AfterExactCredentialRebaseManifest,
    AfterExactProxyConfigRebaseManifestExternalWrite,
}

#[cfg(feature = "test-utils")]
type EnvTransactionTestHook = Box<dyn FnOnce(&Path) + Send + 'static>;

#[cfg(feature = "test-utils")]
static ENV_TRANSACTION_TEST_HOOK: std::sync::Mutex<Option<EnvTransactionTestHook>> =
    std::sync::Mutex::new(None);

/// Install a one-shot hook immediately after an env exact manifest commits.
/// Test-only: production builds have no hook or timing branch.
#[cfg(feature = "test-utils")]
pub fn set_env_transaction_test_hook(hook: impl FnOnce(&Path) + Send + 'static) {
    *ENV_TRANSACTION_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(hook));
}

/// Extract provider/MCP/root secrets into the isolated credential store.
/// Calling this on every load is intentional: it is idempotent and also
/// catches legacy ciphertext written later by an older binary.
pub fn migrate_provider_mcp_credentials(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    #[cfg(test)]
    return migrate_with_fault(data_dir.as_ref(), MigrationFault::None);
    #[cfg(not(test))]
    migrate_inner(data_dir.as_ref(), None)
}

/// Extract the external broker bearer token without coupling a malformed
/// `broker.json` to provider/MCP/root configuration loading. This uses the
/// same migration lock and manifest as every other credential transaction, so
/// an already committed partial transaction still blocks all readers until it
/// is recovered.
pub fn migrate_external_broker_credentials(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    #[cfg(test)]
    return migrate_broker_with_fault(data_dir.as_ref(), MigrationFault::None);
    #[cfg(not(test))]
    migrate_broker_inner(data_dir.as_ref(), None)
}

/// Fail-closed guard for every production reader of credential transaction
/// members. A malformed manifest is treated like a pending one: callers must
/// retain their existing runtime rather than guessing which files committed.
pub fn ensure_provider_mcp_migration_ready(data_dir: impl AsRef<Path>) -> ConfigStoreResult<()> {
    let path = data_dir.as_ref().join(MANIFEST_FILE);
    let Some(bytes) = read_optional_migration_file(&path)? else {
        return Ok(());
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| {
        ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        )
    })?;
    validate_manifest(&manifest).map_err(|_| {
        ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        )
    })?;
    if manifest.state == MigrationState::Pending {
        return Err(ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        ));
    }
    Ok(())
}

/// Serialize public credential access with provider/MCP/broker transactions.
/// Keeping `ensure -> load/decrypt` (and mutation CAS) under this lock closes
/// windows where a reader could observe a transaction member with stale
/// metadata or a writer could commit after the durable transaction point.
pub(crate) fn with_provider_mcp_migration_lock<T>(
    data_dir: &Path,
    operation: impl FnOnce() -> ConfigStoreResult<T>,
) -> ConfigStoreResult<T> {
    let data_dir = if data_dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        data_dir
    };
    std::fs::create_dir_all(data_dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data_dir.join(LOCK_FILE))?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);
    operation()
}

/// Persist a legacy provider-key update as one manifest-gated transaction.
/// The caller must pass a detached candidate config and publish it to live
/// memory only after this function succeeds.
pub fn persist_provider_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    intents: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    persist_provider_instance_credential_transaction(data_dir, config, intents, &BTreeSet::new())
}

/// Persist proxy authentication and its stable root metadata as one
/// recoverable credential/config transaction.
#[cfg(test)]
fn persist_proxy_auth_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
) -> ConfigStoreResult<u64> {
    let data_dir = data_dir.as_ref();
    let expected_revision = CredentialStore::open(data_dir).revision()?;
    persist_proxy_auth_credential_transaction_at_revision(data_dir, config, expected_revision)
}

/// Persist proxy authentication with an explicit credential-store revision
/// precondition. The returned revision is the durable credential revision
/// committed by the exact transaction.
pub fn persist_proxy_auth_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::from(["__proxy_auth".to_string()]),
        &BTreeSet::new(),
        Some(expected_revision),
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::from(["__proxy_auth".to_string()]),
        &BTreeSet::new(),
        Some(expected_revision),
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
    )
}

/// Persist secret env values and their root metadata as one recoverable exact
/// transaction guarded by the credential document revision.
pub fn persist_env_var_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    env_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        env_intents,
        Some(expected_revision),
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        env_intents,
        Some(expected_revision),
        false,
        &BTreeSet::new(),
        false,
        None,
    )
}

/// Persist the complete notification metadata domain and explicitly touched
/// ntfy/Bark credentials in one recoverable exact transaction.
pub fn persist_notification_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    secret_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_notification_credential_transaction_at_revision_with_reset(
        data_dir,
        config,
        secret_intents,
        false,
        expected_revision,
    )
}

/// Notification transaction variant used by the root compatibility API's
/// explicit `notifications: null` domain reset.
pub fn persist_notification_credential_transaction_at_revision_with_reset(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    secret_intents: &BTreeSet<String>,
    reset_domain: bool,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        true,
        secret_intents,
        reset_domain,
        Some(expected_revision),
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        true,
        secret_intents,
        reset_domain,
        Some(expected_revision),
    )
}

/// Persist built-in and provider-instance API-key updates as one recoverable
/// credential/config transaction. Instance deletes are represented by an
/// intent whose id is absent from `config`; the prior durable credential ref
/// is recovered from `config.json` so custom legacy refs are cleared too.
pub fn persist_provider_instance_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    if provider_intents.contains("__proxy_auth") {
        return Err(ConfigStoreError::Validation(
            "proxy auth requires the dedicated revisioned credential transaction".to_string(),
        ));
    }
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    )
    .map(|_| ());
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
    )
    .map(|_| ())
}

#[cfg(test)]
fn persist_provider_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    persist_provider_credential_transaction_with_instances_inner(
        data_dir,
        config,
        provider_intents,
        &BTreeSet::new(),
        provider_intents
            .contains("__proxy_auth")
            .then(|| CredentialStore::open(data_dir).revision_unchecked())
            .transpose()?,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        fault,
    )
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn persist_provider_credential_transaction_with_instances_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    proxy_expected_revision: Option<u64>,
    env_intents: &BTreeSet<String>,
    env_expected_revision: Option<u64>,
    notification_transaction: bool,
    notification_intents: &BTreeSet<String>,
    notification_reset: bool,
    notification_expected_revision: Option<u64>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    let proxy_only = provider_intents.len() == 1
        && provider_intents.contains("__proxy_auth")
        && provider_instance_intents.is_empty();
    let env_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && !env_intents.is_empty();
    let notification_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && notification_transaction;
    if !env_intents.is_empty() && !env_only {
        return Err(ConfigStoreError::Validation(
            "env credentials must be updated in their own transaction".to_string(),
        ));
    }
    if !notification_intents.is_empty() && !notification_only {
        return Err(ConfigStoreError::Validation(
            "notification credentials must be updated in their own transaction".to_string(),
        ));
    }
    std::fs::create_dir_all(data_dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data_dir.join(LOCK_FILE))?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);

    cleanup_orphan_transaction_dirs(data_dir)?;
    recover_committed(
        data_dir,
        #[cfg(test)]
        None,
    )?;
    discard_uncommitted(data_dir)?;

    let credentials_original = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    let providers_original = read_target_or_empty(&data_dir.join(PROVIDERS_FILE))?;
    let config_original = read_target_or_empty(&data_dir.join(CONFIG_FILE))?;
    let persisted_instance_refs = provider_instance_refs_from_document(&config_original)?;
    let persisted_env_refs = env_refs_from_document(&config_original)?;
    let persisted_notification_refs = notification_refs_from_document(&config_original)?;
    if env_only {
        for name in env_intents {
            if let Some(reference) = persisted_env_refs.get(name) {
                ensure_env_ref_exclusive(data_dir, reference.as_str(), name)?;
            }
        }
    }
    if notification_only {
        for channel in ["ntfy", "bark"] {
            let configured = if channel == "ntfy" {
                config.notifications.ntfy.configured
            } else {
                config.notifications.bark.configured
            };
            if !persisted_notification_refs.contains_key(channel)
                && !notification_intents.contains(channel)
                && !configured
            {
                continue;
            }
            let reference =
                persisted_notification_refs
                    .get(channel)
                    .cloned()
                    .unwrap_or(credential_ref(
                        "notification",
                        channel,
                        if channel == "ntfy" {
                            "token"
                        } else {
                            "device_key"
                        },
                    )?);
            ensure_notification_ref_exclusive(data_dir, reference.as_str(), channel)?;
        }
    }
    let store = CredentialStore::open(data_dir);
    let prepared = if env_only {
        store.prepare_env_var_intents(config, env_intents, &persisted_env_refs)?
    } else if notification_only {
        store.prepare_notification_intents(
            config,
            notification_intents,
            &persisted_notification_refs,
            notification_reset,
        )?
    } else {
        store.prepare_provider_api_key_intents(
            config,
            provider_intents,
            provider_instance_intents,
            &persisted_instance_refs,
        )?
    };
    let Some(mut prepared) = prepared else {
        return store.revision_unchecked();
    };
    if proxy_only {
        let expected = proxy_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "proxy auth credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if env_only {
        let expected = env_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "env credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if notification_only {
        let expected = notification_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "notification credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    let (config_bytes, provider_bytes) = if proxy_only {
        (
            prepare_proxy_auth_config_document(&config_original, config)?,
            providers_original.clone(),
        )
    } else if env_only {
        (
            prepare_env_var_config_document(&config_original, config)?,
            providers_original.clone(),
        )
    } else if notification_only {
        (
            prepare_notification_config_document(&config_original, config, notification_reset)?,
            providers_original.clone(),
        )
    } else {
        config
            .prepare_provider_transaction_documents(&providers_original)
            .map_err(|error| ConfigStoreError::Validation(error.to_string()))?
    };
    let env_domain_changed = env_only && env_var_domain_changed(&config_original, &config_bytes)?;
    let notification_domain_changed =
        notification_only && notification_domain_changed(&config_original, &config_bytes)?;
    if env_domain_changed || notification_domain_changed {
        prepared.advance_revision_for_domain_change()?;
    }
    if store.revision_unchecked()? != prepared.expected_revision {
        return Err(ConfigStoreError::Conflict {
            expected: prepared.expected_revision,
            actual: store.revision_unchecked()?,
        });
    }
    // A true env-domain no-op keeps the CAS revision and avoids publishing an
    // event or rewriting either durable member. Any credential or config
    // semantic change advances the one shared revision exactly once.
    if (env_only && !env_domain_changed || notification_only && !notification_domain_changed)
        && prepared.revision == prepared.expected_revision
    {
        return Ok(prepared.revision);
    }

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;
    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared.bytes,
        Some(&credentials_original),
        true,
        None,
        InstallMode::Exact,
        Some(prepared.expected_revision),
        &mut staged,
    )?;
    let credential_file = staged
        .last_mut()
        .expect("credential transaction stages credentials first");
    credential_file.touched_credential_refs = prepared
        .touched_refs
        .iter()
        .map(|reference| reference.as_str().to_string())
        .collect();
    credential_file.required_credential_refs = prepared
        .required_refs
        .iter()
        .map(|reference| reference.as_str().to_string())
        .collect();
    credential_file.transaction_base_sha256 = credential_file.original_sha256.clone();
    if !proxy_only && !env_only && !notification_only {
        stage_file(
            &stage_dir,
            &backup_dir,
            PROVIDERS_FILE,
            &provider_bytes,
            Some(&providers_original),
            false,
            None,
            InstallMode::Exact,
            None,
            &mut staged,
        )?;
    }
    stage_file(
        &stage_dir,
        &backup_dir,
        CONFIG_FILE,
        &config_bytes,
        Some(&config_original),
        false,
        None,
        InstallMode::Exact,
        None,
        &mut staged,
    )?;
    if env_only {
        staged
            .last_mut()
            .expect("env transaction stages config last")
            .touched_env_names = env_intents.iter().cloned().collect();
    }
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: if proxy_only {
            Some(ExactTransactionScope::ProxyAuth)
        } else if env_only {
            Some(ExactTransactionScope::EnvVars)
        } else if notification_only {
            Some(ExactTransactionScope::Notifications)
        } else {
            None
        },
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;

    #[cfg(test)]
    if fault == Some(MigrationFault::BeforeExactCommitCredentialRace) {
        let reference = prepared.touched_refs.first().cloned().ok_or_else(|| {
            ConfigStoreError::Validation("credential intent is empty".to_string())
        })?;
        store.replace_unchecked(
            reference,
            "concurrent-winner",
            crate::CredentialSource::User,
            prepared.expected_revision,
        )?;
    }

    // Recheck every CAS immediately before the durable commit point. A loser
    // leaves only an uncommitted journal, which the next run safely discards.
    for file in &manifest.files {
        let current = read_target_or_empty(&data_dir.join(&file.name))?;
        let current_sha256 = sha256(&current);
        if file.original_sha256.as_deref() != Some(current_sha256.as_str()) {
            if file.name == CREDENTIALS_FILE {
                return Err(ConfigStoreError::Conflict {
                    expected: file.expected_revision.unwrap_or(0),
                    actual: store.revision_unchecked()?,
                });
            }
            return Err(ConfigStoreError::Validation(format!(
                "{} changed during provider credential transaction",
                file.name
            )));
        }
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(feature = "test-utils")]
    if env_only {
        if let Some(hook) = ENV_TRANSACTION_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            hook(data_dir);
        }
    }
    #[cfg(test)]
    if matches!(
        fault,
        Some(
            MigrationFault::AfterExactCommitCredentialRace
                | MigrationFault::AfterExactCommitUnrelatedCredentialRace
                | MigrationFault::AfterExactCommitCredentialClearRace
                | MigrationFault::AfterExactCredentialRebaseStage
                | MigrationFault::AfterExactCredentialRebaseManifest
        )
    ) {
        let clear_same_ref = fault == Some(MigrationFault::AfterExactCommitCredentialClearRace);
        let same_ref =
            clear_same_ref || fault == Some(MigrationFault::AfterExactCommitCredentialRace);
        let reference = if same_ref {
            prepared.touched_refs.first().cloned().ok_or_else(|| {
                ConfigStoreError::Validation("credential intent is empty".to_string())
            })?
        } else {
            credential_ref("provider", "anthropic", "api_key")?
        };
        if clear_same_ref {
            store.clear_unchecked(&reference, prepared.expected_revision)?;
        } else {
            store.replace_unchecked(
                reference,
                if same_ref {
                    "concurrent-post-commit-winner"
                } else {
                    "concurrent-unrelated-winner"
                },
                crate::CredentialSource::User,
                prepared.expected_revision,
            )?;
        }
    }
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }
    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    store.revision_unchecked()
}

#[cfg(test)]
fn migrate_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_inner(data_dir, Some(fault))
}

#[cfg(test)]
fn migrate_broker_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_broker_inner(data_dir, Some(fault))
}

fn migrate_broker_inner(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    std::fs::create_dir_all(data_dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data_dir.join(LOCK_FILE))?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);

    cleanup_orphan_transaction_dirs(data_dir)?;
    if let Some(outcome) = recover_committed(
        data_dir,
        #[cfg(test)]
        fault,
    )? {
        return Ok(outcome);
    }
    discard_uncommitted(data_dir)?;

    let mut extracted = Vec::new();
    let broker = plan_broker_section(data_dir, &mut extracted, 1)?;
    if broker.is_none() {
        let migrated_credentials = scrub_broker_credentials_from_backups(data_dir)?;
        return Ok(CredentialMigrationOutcome {
            migrated_credentials,
            resumed: false,
        });
    }

    let store = CredentialStore::open(data_dir);
    let resolved = resolve_extracted_secrets(&store, extracted)?;
    let prepared_credentials = store.prepare_migration(resolved)?;
    let broker = broker.expect("checked above");

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;

    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared_credentials.bytes,
        std::fs::read(data_dir.join(CREDENTIALS_FILE))
            .ok()
            .as_deref(),
        true,
        None,
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    stage_file(
        &stage_dir,
        &backup_dir,
        broker.name,
        &broker.bytes,
        Some(&broker.original),
        false,
        Some(broker.migration_generation),
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: None,
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterJournal) {
        return Err(injected_fault());
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }

    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(CredentialMigrationOutcome {
        migrated_credentials: prepared_credentials.added,
        resumed: false,
    })
}

fn migrate_inner(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    std::fs::create_dir_all(data_dir)?;
    let lock_path = data_dir.join(LOCK_FILE);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);

    cleanup_orphan_transaction_dirs(data_dir)?;
    if let Some(outcome) = recover_committed(
        data_dir,
        #[cfg(test)]
        fault,
    )? {
        return Ok(outcome);
    }
    discard_uncommitted(data_dir)?;

    let mut extracted = Vec::new();
    let providers = plan_provider_section(data_dir, &mut extracted, 1)?;
    let mcp = plan_mcp_section(data_dir, &mut extracted, 1)?;
    let provider_instances = plan_provider_instance_section(data_dir, &mut extracted, 1, true)?;
    let credential_store = CredentialStore::open(data_dir);
    let prospective_documents = [&providers, &mcp, &provider_instances]
        .into_iter()
        .filter_map(Option::as_ref)
        .map(|section| (section.bytes.as_slice(), section.name == CONFIG_FILE))
        .collect::<Vec<_>>();
    ensure_legacy_env_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_notification_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_proxy_extractions_are_safe(
        data_dir,
        &credential_store,
        &extracted,
        &prospective_documents,
    )?;
    ensure_backup_legacy_proxy_extractions_are_safe(data_dir, &credential_store)?;
    if providers.is_none() && mcp.is_none() && provider_instances.is_none() {
        scrub_provider_instance_credentials_from_backups(
            data_dir,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )?;
        return Ok(CredentialMigrationOutcome {
            migrated_credentials: 0,
            resumed: false,
        });
    }

    let resolved = resolve_extracted_secrets(&credential_store, extracted)?;
    let prepared_credentials = credential_store.prepare_migration(resolved)?;

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;

    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared_credentials.bytes,
        std::fs::read(data_dir.join(CREDENTIALS_FILE))
            .ok()
            .as_deref(),
        true,
        None,
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    for section in providers.into_iter().chain(mcp).chain(provider_instances) {
        stage_file(
            &stage_dir,
            &backup_dir,
            section.name,
            &section.bytes,
            Some(&section.original),
            false,
            Some(section.migration_generation),
            InstallMode::Migration,
            None,
            &mut staged,
        )?;
    }
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: None,
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterJournal) {
        return Err(injected_fault());
    }

    // Commit point. Every candidate and pre-migration backup is durable before
    // this manifest becomes visible.
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }

    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(CredentialMigrationOutcome {
        migrated_credentials: prepared_credentials.added,
        resumed: false,
    })
}

fn resolve_extracted_secrets(
    store: &CredentialStore,
    extracted: Vec<ExtractedSecret>,
) -> ConfigStoreResult<Vec<(CredentialRef, String, u64)>> {
    let mut resolved = Vec::new();
    for secret in extracted {
        // A user replacement is authoritative over any stale legacy copy. This
        // also makes a fresh plan after an uncommitted crash idempotent.
        let status = store.status_unchecked(&secret.credential_ref)?;
        if status.configured && status.source != crate::CredentialSource::Migrated {
            if secret.kind == ExtractedSecretKind::EnvVar {
                return Err(ConfigStoreError::Validation(
                    "legacy env credential target is already user-managed".to_string(),
                ));
            }
            continue;
        }
        let value = match secret.value {
            LegacySecret::Plaintext(value) => value,
            LegacySecret::Ciphertext(value) => {
                crate::encryption::decrypt(&value).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy credential could not be decrypted for migration".to_string(),
                    )
                })?
            }
        };
        resolved.push((secret.credential_ref, value, secret.migration_generation));
    }
    Ok(resolved)
}

fn ensure_legacy_proxy_extractions_are_safe(
    data_dir: &Path,
    store: &CredentialStore,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted
        .iter()
        .filter(|secret| secret.kind == ExtractedSecretKind::ProxyAuth)
    {
        ensure_no_durable_non_proxy_consumers(data_dir, secret.credential_ref.as_str())?;
        for (bytes, config_root) in prospective_documents {
            ensure_no_non_proxy_consumers_in_document(
                bytes,
                secret.credential_ref.as_str(),
                *config_root,
                "prospective configuration",
            )?;
        }
        let status = store.status_unchecked(&secret.credential_ref)?;
        if status.configured && status.source != crate::CredentialSource::Migrated {
            return Err(ConfigStoreError::Validation(
                "legacy proxy auth target credential is already user-managed".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_legacy_env_extractions_are_safe(
    data_dir: &Path,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted
        .iter()
        .filter(|secret| secret.kind == ExtractedSecretKind::EnvVar)
    {
        let owner = secret.env_owner.as_deref().ok_or_else(|| {
            ConfigStoreError::Validation("legacy env credential owner is missing".to_string())
        })?;
        ensure_env_ref_exclusive(data_dir, secret.credential_ref.as_str(), owner)?;
        for (bytes, _) in prospective_documents {
            let value: Value = serde_json::from_slice(bytes)?;
            if contains_other_env_credential_consumer(&value, secret.credential_ref.as_str(), owner)
            {
                return Err(ConfigStoreError::Validation(
                    "env credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_legacy_notification_extractions_are_safe(
    data_dir: &Path,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted.iter().filter(|secret| {
        matches!(
            secret.kind,
            ExtractedSecretKind::NotificationNtfy | ExtractedSecretKind::NotificationBark
        )
    }) {
        let channel = match secret.kind {
            ExtractedSecretKind::NotificationNtfy => "ntfy",
            ExtractedSecretKind::NotificationBark => "bark",
            _ => unreachable!("filtered above"),
        };
        for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
            let bytes = read_target_or_empty(&data_dir.join(name))?;
            if notification_document_has_other_consumer(
                &bytes,
                secret.credential_ref.as_str(),
                channel,
                name == CONFIG_FILE,
            )? {
                return Err(ConfigStoreError::Validation(
                    "notification credential reference is shared by another consumer".to_string(),
                ));
            }
        }
        for (bytes, config_root) in prospective_documents {
            if notification_document_has_other_consumer(
                bytes,
                secret.credential_ref.as_str(),
                channel,
                *config_root,
            )? {
                return Err(ConfigStoreError::Validation(
                    "notification credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_broker_ref_exclusive(
    data_dir: &Path,
    reference: &CredentialRef,
) -> ConfigStoreResult<()> {
    for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = data_dir.join(format!("{name}{suffix}"));
            let bytes = read_target_or_empty(&path)?;
            if bytes.is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) if !suffix.is_empty() => continue,
                Err(error) => return Err(error.into()),
            };
            if contains_credential_reference(&value, reference.as_str()) {
                return Err(ConfigStoreError::Validation(
                    "broker credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    let broker_bytes = read_target_or_empty(&data_dir.join(BROKER_FILE))?;
    if !broker_bytes.is_empty() {
        let mut broker: Value = serde_json::from_slice(&broker_bytes)?;
        if let Some(object) = broker.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&broker, reference.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker configuration".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_broker_backup_ownership(
    data_dir: &Path,
    preferred_ref: &CredentialRef,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes = match std::fs::read(data_dir.join(format!("{BROKER_FILE}.{suffix}"))) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        broker_backup_candidate(&bytes, Some(preferred_ref))?;
        let mut value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(object) = value.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&value, preferred_ref.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker backup".to_string(),
            ));
        }
    }
    Ok(())
}

fn contains_credential_reference(value: &Value, expected: &str) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            ((key == "credential_ref" || key.ends_with("_credential_ref"))
                && child.as_str() == Some(expected))
                || (key.ends_with("_credential_refs") && contains_string_value(child, expected))
                || contains_credential_reference(child, expected)
        }),
        Value::Array(values) => values
            .iter()
            .any(|child| contains_credential_reference(child, expected)),
        _ => false,
    }
}

fn notification_document_has_other_consumer(
    bytes: &[u8],
    reference: &str,
    channel: &str,
    config_root: bool,
) -> ConfigStoreResult<bool> {
    if bytes.is_empty() {
        return Ok(false);
    }
    let mut value: Value = serde_json::from_slice(bytes)?;
    if config_root {
        if let Some(config) = value
            .get_mut("notifications")
            .and_then(Value::as_object_mut)
            .and_then(|notifications| notifications.get_mut(channel))
            .and_then(Value::as_object_mut)
        {
            if config.get("credential_ref").and_then(Value::as_str) == Some(reference) {
                config.remove("credential_ref");
            }
        }
    }
    Ok(contains_notification_credential_reference(
        &value, reference,
    ))
}

fn contains_notification_credential_reference(value: &Value, expected: &str) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| contains_notification_credential_reference(value, expected)),
        Value::Object(object) => object.iter().any(|(key, value)| {
            let credential_field = key == "credential_ref"
                || key.ends_with("_credential_ref")
                || key.ends_with("_credential_refs");
            (credential_field && contains_string_value(value, expected))
                || contains_notification_credential_reference(value, expected)
        }),
        _ => false,
    }
}

fn ensure_env_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    owner: &str,
) -> ConfigStoreResult<()> {
    for name in [
        CONFIG_FILE,
        PROVIDERS_FILE,
        MCP_FILE,
        "config.json.bak",
        "config.json.bak.1",
        "config.json.bak.2",
    ] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if bytes.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) if name.starts_with("config.json.bak") => continue,
            Err(error) => return Err(error.into()),
        };
        if contains_other_env_credential_consumer(&value, reference, owner) {
            return Err(ConfigStoreError::Validation(
                "env credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn contains_other_env_credential_consumer(value: &Value, reference: &str, owner: &str) -> bool {
    match value {
        Value::Object(object) => {
            let owned_env_entry = object.get("name").and_then(Value::as_str) == Some(owner)
                && object.contains_key("secret");
            object.iter().any(|(key, child)| {
                if owned_env_entry && key == "credential_ref" {
                    return false;
                }
                (key == "credential_ref" && child.as_str() == Some(reference))
                    || (key.ends_with("_credential_refs")
                        && contains_string_value(child, reference))
                    || contains_other_env_credential_consumer(child, reference, owner)
            })
        }
        Value::Array(values) => values
            .iter()
            .any(|child| contains_other_env_credential_consumer(child, reference, owner)),
        _ => false,
    }
}

fn ensure_backup_legacy_proxy_extractions_are_safe(
    data_dir: &Path,
    store: &CredentialStore,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes = match std::fs::read(data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        let Some(object) = root.as_object_mut() else {
            continue;
        };
        let mut extracted = Vec::new();
        if !scrub_authoritative_or_tombstoned_proxy_auth(object, store, &BTreeSet::new())? {
            migrate_proxy_auth(object, &mut extracted, 1)?;
        }
        ensure_legacy_proxy_extractions_are_safe(
            data_dir,
            store,
            &extracted,
            &[(bytes.as_slice(), true)],
        )?;
    }
    Ok(())
}

fn scrub_authoritative_or_tombstoned_proxy_auth(
    object: &mut Map<String, Value>,
    store: &CredentialStore,
    proxy_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<bool> {
    let had_legacy = [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    if !had_legacy {
        return Ok(false);
    }
    let tombstone_reference = proxy_clear_tombstones
        .iter()
        .next()
        .map(|reference| CredentialRef::parse(reference.clone()))
        .transpose()?;
    let reference = match tombstone_reference.as_ref() {
        Some(reference) => reference.clone(),
        None => object
            .get("proxy_auth_credential_ref")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| {
                        ConfigStoreError::Validation(
                            "proxy auth credential reference must be a string".to_string(),
                        )
                    })
                    .and_then(|value| CredentialRef::parse(value.to_string()))
            })
            .transpose()?
            .unwrap_or(credential_ref("proxy", "default", "auth")?),
    };
    let authoritative = if tombstone_reference.is_some() {
        true
    } else {
        let status = store.status_unchecked(&reference)?;
        status.configured && status.source != crate::CredentialSource::Migrated
    };
    if !authoritative {
        return Ok(false);
    }
    // The store value or exact clear is authoritative. Scrub the stale backup
    // by metadata only: do not decrypt, normalize, or compare its secret.
    for key in [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ] {
        object.remove(key);
    }
    object.insert(
        "proxy_auth_credential_ref".to_string(),
        Value::String(reference.as_str().to_string()),
    );
    Ok(true)
}

fn plan_provider_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(PROVIDERS_FILE);
    let original = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = next_revision(revision.unwrap_or(0))?.max(minimum_generation);
    let object = data.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("provider section must be an object".to_string())
    })?;
    let mut changed = false;
    for provider in ["openai", "anthropic", "gemini", "bodhi"] {
        let Some(config) = object.get_mut(provider).and_then(Value::as_object_mut) else {
            continue;
        };
        let plaintext = take_nonempty_string(config, "api_key")?;
        let ciphertext = take_nonempty_string(config, "api_key_encrypted")?;
        if plaintext.is_none() && ciphertext.is_none() {
            continue;
        }
        let reference = existing_or_generated_ref(
            config.get("credential_ref"),
            "provider",
            provider,
            "api_key",
        )?;
        config.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        extracted.push(ExtractedSecret {
            credential_ref: reference,
            value: plaintext
                .map(LegacySecret::Plaintext)
                .or_else(|| ciphertext.map(LegacySecret::Ciphertext))
                .expect("one provider credential exists"),
            migration_generation,
            kind: ExtractedSecretKind::Other,
            env_owner: None,
        });
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    validate_provider_data(data)?;
    advance_or_wrap(&mut root, revision.is_some(), migration_generation);
    Ok(Some(PlannedSection {
        name: PROVIDERS_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn provider_instance_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let Some(instances) = root.get("provider_instances") else {
        return Ok(BTreeMap::new());
    };
    let instances = instances.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("provider_instances must be an object".to_string())
    })?;
    instances
        .iter()
        .filter_map(|(instance_id, value)| {
            value
                .get("credential_ref")
                .map(|value| (instance_id, value))
        })
        .map(|(instance_id, value)| {
            let value = value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "provider instance credential reference must be a string".to_string(),
                )
            })?;
            Ok((
                instance_id.clone(),
                CredentialRef::parse(value.to_string())?,
            ))
        })
        .collect()
}

fn env_refs_from_document(bytes: &[u8]) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let Some(entries) = root.get("env_vars").and_then(Value::as_array) else {
        return Ok(BTreeMap::new());
    };
    let mut refs = BTreeMap::new();
    for entry in entries {
        let Some(name) = entry.get("name").and_then(Value::as_str) else {
            continue;
        };
        if let Some(raw) = entry.get("credential_ref") {
            let raw = raw.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "env credential reference must be a string".to_string(),
                )
            })?;
            refs.insert(name.to_string(), CredentialRef::parse(raw.to_string())?);
        }
    }
    Ok(refs)
}

fn notification_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let mut refs = BTreeMap::new();
    for channel in ["ntfy", "bark"] {
        let Some(raw) = root
            .get("notifications")
            .and_then(|value| value.get(channel))
            .and_then(|value| value.get("credential_ref"))
        else {
            continue;
        };
        let raw = raw.as_str().ok_or_else(|| {
            ConfigStoreError::Validation(
                "notification credential reference must be a string".to_string(),
            )
        })?;
        refs.insert(channel.to_string(), CredentialRef::parse(raw.to_string())?);
    }
    Ok(refs)
}

fn plan_provider_instance_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
    tolerate_corrupt_root: bool,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(CONFIG_FILE);
    let original = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        // A non-file at the optional root-config location cannot contain a
        // provider-instance migration source. Leave it for Config's normal
        // root write/recovery handling while still propagating real read and
        // permission failures.
        Err(error) if tolerate_corrupt_root && error.kind() == std::io::ErrorKind::IsADirectory => {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let mut root_extracted = Vec::new();
    let planned =
        plan_provider_instance_document(original, &mut root_extracted, minimum_generation);
    match planned {
        Ok(planned) => {
            extracted.extend(root_extracted);
            Ok(planned)
        }
        Err(ConfigStoreError::Validation(message))
            if message.starts_with("legacy proxy auth")
                || message.starts_with("legacy env credential")
                || message.starts_with("legacy notification credential") =>
        {
            Err(ConfigStoreError::Validation(message))
        }
        // Root-config corruption recovery runs after migration readiness. Do
        // not block provider/MCP/store hydration because this optional member
        // has invalid JSON or validation errors. Crucially, extraction is
        // isolated above so an error cannot publish a partial credential set.
        Err(_) if tolerate_corrupt_root => Ok(None),
        Err(error) => Err(error),
    }
}

fn plan_provider_instance_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("root configuration must be an object".to_string())
    })?;
    let migration_generation = minimum_generation;
    let mut changed = migrate_proxy_auth(object, extracted, migration_generation)?;
    changed |= migrate_env_vars(object, extracted, migration_generation)?;
    changed |= migrate_notification_credentials(object, extracted, migration_generation)?;
    if let Some(instances) = object.get_mut("provider_instances") {
        let instances = instances.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("provider_instances must be an object".to_string())
        })?;
        for (instance_id, value) in instances {
            let instance = value.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("provider instance must be an object".to_string())
            })?;
            let plaintext = take_nonempty_string(instance, "api_key")?;
            let ciphertext = take_nonempty_string(instance, "api_key_encrypted")?;
            if plaintext.is_none() && ciphertext.is_none() {
                continue;
            }
            let reference = existing_or_generated_ref(
                instance.get("credential_ref"),
                "provider_instance",
                instance_id,
                "api_key",
            )?;
            instance.insert(
                "credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            extracted.push(ExtractedSecret {
                credential_ref: reference.clone(),
                value: plaintext
                    .map(LegacySecret::Plaintext)
                    .or_else(|| ciphertext.map(LegacySecret::Ciphertext))
                    .expect("one provider-instance credential exists"),
                migration_generation,
                kind: ExtractedSecretKind::Other,
                env_owner: None,
            });
            changed = true;
        }
    }
    if !changed {
        return Ok(None);
    }
    serde_json::from_value::<crate::Config>(root.clone()).map_err(ConfigStoreError::Json)?;
    Ok(Some(PlannedSection {
        name: CONFIG_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn migrate_env_vars(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let Some(entries) = object.get_mut("env_vars").and_then(Value::as_array_mut) else {
        return Ok(false);
    };
    let mut changed = false;
    for entry in entries {
        let entry = entry.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("env var entry must be an object".to_string())
        })?;
        if !entry
            .get("secret")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            changed |= entry.remove("value_encrypted").is_some();
            changed |= entry.remove("credential_ref").is_some();
            changed |= entry.remove("configured").is_some();
            continue;
        }
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ConfigStoreError::Validation("env var name is missing".to_string()))?
            .to_string();
        let had_plaintext_field = entry.contains_key("value");
        let had_ciphertext_field = entry.contains_key("value_encrypted");
        let previous_ref = entry.get("credential_ref").cloned();
        let previous_configured = entry.get("configured").cloned();
        let plaintext = take_nonempty_string(entry, "value")?;
        let ciphertext = take_nonempty_string(entry, "value_encrypted")?;
        let value = match (plaintext, ciphertext) {
            (Some(plaintext), Some(ciphertext)) => {
                let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy env credential could not be decrypted".to_string(),
                    )
                })?;
                if plaintext != decrypted {
                    return Err(ConfigStoreError::Validation(
                        "legacy env credential fields contain conflicting values".to_string(),
                    ));
                }
                Some(LegacySecret::Plaintext(plaintext))
            }
            (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
            (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
            (None, None) => None,
        };
        let reference =
            existing_or_generated_ref(entry.get("credential_ref"), "env", &name, "value")?;
        entry.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        let configured = value.is_some()
            || entry
                .get("configured")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        entry.insert("configured".to_string(), Value::Bool(configured));
        if let Some(value) = value {
            extracted.push(ExtractedSecret {
                credential_ref: reference.clone(),
                value,
                migration_generation,
                kind: ExtractedSecretKind::EnvVar,
                env_owner: Some(name.clone()),
            });
        }
        changed |= had_plaintext_field
            || had_ciphertext_field
            || previous_ref.as_ref() != Some(&Value::String(reference.as_str().to_string()))
            || previous_configured.as_ref() != Some(&Value::Bool(configured));
    }
    Ok(changed)
}

fn migrate_notification_credentials(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let Some(notifications) = object
        .get_mut("notifications")
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };
    let mut changed = false;
    for (channel, plaintext_key, ciphertext_key, field) in [
        ("ntfy", "token", "token_encrypted", "token"),
        ("bark", "device_key", "device_key_encrypted", "device_key"),
    ] {
        let Some(config) = notifications
            .get_mut(channel)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        let had_plaintext = config.contains_key(plaintext_key);
        let had_ciphertext = config.contains_key(ciphertext_key);
        let previous_ref = config.get("credential_ref").cloned();
        let previous_configured = config.get("configured").cloned();
        let plaintext = take_nonempty_string(config, plaintext_key)?;
        let ciphertext = take_nonempty_string(config, ciphertext_key)?;
        let secret = match (plaintext, ciphertext) {
            (Some(plaintext), Some(ciphertext)) => {
                let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy notification credential could not be decrypted".to_string(),
                    )
                })?;
                if plaintext != decrypted {
                    return Err(ConfigStoreError::Validation(
                        "legacy notification credential fields contain conflicting values"
                            .to_string(),
                    ));
                }
                Some(LegacySecret::Plaintext(plaintext))
            }
            (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
            (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
            (None, None) => None,
        };
        let configured = secret.is_some()
            || config
                .get("configured")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        if secret.is_none() && previous_ref.is_none() && !configured {
            changed |= had_plaintext || had_ciphertext;
            continue;
        }
        let reference = existing_or_generated_ref(
            config.get("credential_ref"),
            "notification",
            channel,
            field,
        )?;
        config.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        config.insert("configured".to_string(), Value::Bool(configured));
        if let Some(secret) = secret {
            extracted.push(ExtractedSecret {
                credential_ref: reference.clone(),
                value: secret,
                migration_generation,
                kind: if channel == "ntfy" {
                    ExtractedSecretKind::NotificationNtfy
                } else {
                    ExtractedSecretKind::NotificationBark
                },
                env_owner: None,
            });
        }
        changed |= had_plaintext
            || had_ciphertext
            || previous_ref.as_ref() != Some(&Value::String(reference.as_str().to_string()))
            || previous_configured.as_ref() != Some(&Value::Bool(configured));
    }
    Ok(changed)
}

fn migrate_proxy_auth(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let had_legacy = [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    let plaintext = match object.remove("proxy_auth") {
        None | Some(Value::Null) => None,
        Some(value) => Some(normalize_proxy_auth_secret(&serde_json::to_string(
            &value,
        )?)?),
    };
    let mut secrets = plaintext.into_iter().collect::<BTreeSet<_>>();
    for key in [
        "proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
    ] {
        if let Some(ciphertext) = take_nonempty_string(object, key)? {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy proxy auth credential could not be decrypted".to_string(),
                )
            })?;
            secrets.insert(normalize_proxy_auth_secret(&decrypted)?);
        }
    }
    if secrets.len() > 1 {
        return Err(ConfigStoreError::Validation(
            "legacy proxy auth fields contain conflicting credentials".to_string(),
        ));
    }
    let existing = object
        .get("proxy_auth_credential_ref")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "proxy auth credential reference must be a string".to_string(),
                    )
                })
                .and_then(|value| CredentialRef::parse(value.to_string()))
        })
        .transpose()?;
    let Some(secret) = secrets.into_iter().next() else {
        return Ok(had_legacy);
    };
    let reference = existing.unwrap_or(credential_ref("proxy", "default", "auth")?);
    object.insert(
        "proxy_auth_credential_ref".to_string(),
        Value::String(reference.as_str().to_string()),
    );
    extracted.push(ExtractedSecret {
        credential_ref: reference,
        value: LegacySecret::Plaintext(secret),
        migration_generation,
        kind: ExtractedSecretKind::ProxyAuth,
        env_owner: None,
    });
    Ok(true)
}

fn normalize_proxy_auth_secret(value: &str) -> ConfigStoreResult<String> {
    let auth: crate::ProxyAuth = serde_json::from_str(value).map_err(|_| {
        ConfigStoreError::Validation("legacy proxy auth credential is invalid".to_string())
    })?;
    serde_json::to_string(&auth).map_err(ConfigStoreError::Json)
}

fn plan_mcp_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(MCP_FILE);
    let original = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = next_revision(revision.unwrap_or(0))?.max(minimum_generation);
    let mut changed = false;
    if let Some(servers) = data.get_mut("servers").and_then(Value::as_array_mut) {
        for server in servers {
            let object = server.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP server must be an object".to_string())
            })?;
            let id = object
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigStoreError::Validation("MCP server id is missing".to_string())
                })?
                .to_string();
            if let Some(transport) = object.get_mut("transport").and_then(Value::as_object_mut) {
                changed |= migrate_mcp_transport(&id, transport, extracted, migration_generation)?;
            }
        }
    } else {
        let servers = data.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("MCP section must be an object".to_string())
        })?;
        for (id, server) in servers {
            let object = server.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP server must be an object".to_string())
            })?;
            changed |= migrate_mcp_transport(id, object, extracted, migration_generation)?;
            if let Some(transport) = object.get_mut("transport").and_then(Value::as_object_mut) {
                changed |= migrate_mcp_transport(id, transport, extracted, migration_generation)?;
            }
        }
    }
    if !changed {
        return Ok(None);
    }
    validate_mcp_data(data)?;
    advance_or_wrap(&mut root, revision.is_some(), migration_generation);
    Ok(Some(PlannedSection {
        name: MCP_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn plan_broker_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(BROKER_FILE);
    let original = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut root: Value = serde_json::from_slice(&original)?;
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("broker configuration must be an object".to_string())
    })?;
    let had_plaintext = object.contains_key("token");
    let had_ciphertext = object.contains_key("token_encrypted");
    if !had_plaintext && !had_ciphertext {
        let config: crate::BrokerClientConfig = serde_json::from_value(root).map_err(|_| {
            ConfigStoreError::Validation("broker configuration is invalid".to_string())
        })?;
        if let Some(reference) = config.credential_ref.as_ref() {
            ensure_broker_ref_exclusive(data_dir, reference)?;
            ensure_broker_backup_ownership(data_dir, reference)?;
        }
        return Ok(None);
    }

    let plaintext = take_nonempty_string(object, "token")?;
    let ciphertext = take_nonempty_string(object, "token_encrypted")?;
    let secret = match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy broker credential could not be decrypted".to_string(),
                )
            })?;
            if plaintext != decrypted {
                return Err(ConfigStoreError::Validation(
                    "legacy broker credential fields contain conflicting values".to_string(),
                ));
            }
            Some(LegacySecret::Plaintext(plaintext))
        }
        (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
        (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
        (None, None) => None,
    };

    let previous_ref = object.get("credential_ref").cloned();
    let previous_configured = object.get("configured").cloned();
    let configured = secret.is_some()
        || object
            .get("configured")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    if secret.is_some() || previous_ref.is_some() || configured {
        let reference = existing_or_generated_ref(
            object.get("credential_ref"),
            "broker",
            "external",
            "bearer_token",
        )?;
        ensure_broker_ref_exclusive(data_dir, &reference)?;
        ensure_broker_backup_ownership(data_dir, &reference)?;
        object.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        object.insert("configured".to_string(), Value::Bool(configured));
        if let Some(secret) = secret {
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value: secret,
                migration_generation: minimum_generation,
                kind: ExtractedSecretKind::ExternalBroker,
                env_owner: None,
            });
        }
    } else {
        object.remove("credential_ref");
        object.remove("configured");
    }

    serde_json::from_value::<crate::BrokerClientConfig>(root.clone())
        .map_err(|_| ConfigStoreError::Validation("broker configuration is invalid".to_string()))?;
    let changed = had_plaintext
        || had_ciphertext
        || previous_ref != root.get("credential_ref").cloned()
        || previous_configured != root.get("configured").cloned();
    if !changed {
        return Ok(None);
    }
    Ok(Some(PlannedSection {
        name: BROKER_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation: minimum_generation,
    }))
}

fn migrate_mcp_transport(
    server_id: &str,
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let mut changed = migrate_named_secret_map(
        server_id,
        object,
        "env",
        "env_encrypted",
        "env_credential_refs",
        "env",
        extracted,
        migration_generation,
    )?;

    let array_headers = object.get("headers").is_some_and(Value::is_array);
    if array_headers {
        let headers = object
            .get_mut("headers")
            .and_then(Value::as_array_mut)
            .expect("array checked");
        for header in headers {
            let header = header.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP header must be an object".to_string())
            })?;
            let name = header
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigStoreError::Validation("MCP header name is missing".to_string())
                })?
                .to_string();
            let plaintext = take_nonempty_string(header, "value")?;
            let ciphertext = take_nonempty_string(header, "value_encrypted")?;
            if plaintext.is_none() && ciphertext.is_none() {
                continue;
            }
            let reference = existing_or_generated_ref(
                header.get("credential_ref"),
                "mcp",
                server_id,
                &format!("header_{name}"),
            )?;
            header.insert(
                "credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value: plaintext
                    .map(LegacySecret::Plaintext)
                    .or_else(|| ciphertext.map(LegacySecret::Ciphertext))
                    .expect("one header credential exists"),
                migration_generation,
                kind: ExtractedSecretKind::Other,
                env_owner: None,
            });
            changed = true;
        }
    } else {
        changed |= migrate_named_secret_map(
            server_id,
            object,
            "headers",
            "headers_encrypted",
            "header_credential_refs",
            "header",
            extracted,
            migration_generation,
        )?;
    }
    Ok(changed)
}

#[allow(clippy::too_many_arguments)]
fn migrate_named_secret_map(
    server_id: &str,
    object: &mut Map<String, Value>,
    plaintext_key: &str,
    ciphertext_key: &str,
    refs_key: &str,
    field_prefix: &str,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let plaintext = take_string_map(object, plaintext_key)?;
    let ciphertext = take_string_map(object, ciphertext_key)?;
    if plaintext.is_empty() && ciphertext.is_empty() {
        return Ok(false);
    }
    let mut refs = object
        .get(refs_key)
        .map(parse_string_map)
        .transpose()?
        .unwrap_or_default();
    let names = plaintext
        .keys()
        .chain(ciphertext.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for name in names {
        let reference = if let Some(existing) = refs.get(&name) {
            CredentialRef::parse(existing.clone())?
        } else {
            credential_ref("mcp", server_id, &format!("{field_prefix}_{name}"))?
        };
        refs.insert(name.clone(), reference.as_str().to_string());
        let value = plaintext
            .get(&name)
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .map(LegacySecret::Plaintext)
            .or_else(|| {
                ciphertext
                    .get(&name)
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .map(LegacySecret::Ciphertext)
            });
        if let Some(value) = value {
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value,
                migration_generation,
                kind: ExtractedSecretKind::Other,
                env_owner: None,
            });
        }
    }
    object.insert(
        refs_key.to_string(),
        serde_json::to_value(refs).expect("string map serializes"),
    );
    Ok(true)
}

fn section_data_mut(root: &mut Value) -> ConfigStoreResult<(&mut Value, Option<u64>)> {
    let revisioned = root.as_object().is_some_and(|object| {
        object.contains_key("schema_version")
            || object.contains_key("revision")
            || object.contains_key("data")
    });
    if !revisioned {
        return Ok((root, None));
    }
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("section envelope must be an object".to_string())
    })?;
    let schema = object.get("schema_version").and_then(Value::as_u64);
    let revision = object.get("revision").and_then(Value::as_u64);
    if schema != Some(1) || revision.is_none() || !object.contains_key("data") {
        return Err(ConfigStoreError::Validation(
            "section envelope is incomplete or unsupported".to_string(),
        ));
    }
    Ok((object.get_mut("data").expect("checked"), revision))
}

fn advance_or_wrap(root: &mut Value, revisioned: bool, migration_generation: u64) {
    if revisioned {
        let object = root.as_object_mut().expect("validated envelope");
        object.insert(
            "revision".to_string(),
            Value::Number(migration_generation.into()),
        );
    } else {
        let data = std::mem::take(root);
        *root = serde_json::json!({
            "schema_version": 1,
            "revision": migration_generation,
            "data": data,
        });
    }
}

fn validate_provider_data(value: &Value) -> ConfigStoreResult<()> {
    serde_json::from_value::<ProviderConfigs>(value.clone())?;
    Ok(())
}

fn validate_mcp_data(value: &Value) -> ConfigStoreResult<()> {
    serde_json::from_value::<bamboo_domain::McpConfig>(value.clone())?;
    Ok(())
}

fn existing_or_generated_ref(
    existing: Option<&Value>,
    domain: &str,
    owner: &str,
    field: &str,
) -> ConfigStoreResult<CredentialRef> {
    match existing {
        Some(Value::String(value)) => CredentialRef::parse(value.clone()),
        Some(_) => Err(ConfigStoreError::Validation(
            "credential reference must be a string".to_string(),
        )),
        None => credential_ref(domain, owner, field),
    }
}

fn take_nonempty_string(
    object: &mut Map<String, Value>,
    key: &str,
) -> ConfigStoreResult<Option<String>> {
    let Some(value) = object.remove(key) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => Ok(Some(value)),
        _ => Err(ConfigStoreError::Validation(
            "legacy credential must be a string".to_string(),
        )),
    }
}

fn take_string_map(
    object: &mut Map<String, Value>,
    key: &str,
) -> ConfigStoreResult<BTreeMap<String, String>> {
    object
        .remove(key)
        .map(|value| parse_string_map(&value))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_string_map(value: &Value) -> ConfigStoreResult<BTreeMap<String, String>> {
    let object = value.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("legacy credential map must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "legacy credential map values must be strings".to_string(),
                    )
                })
        })
        .collect()
}

fn stage_file(
    stage_dir: &Path,
    backup_dir: &Path,
    name: &str,
    candidate: &[u8],
    original: Option<&[u8]>,
    sensitive: bool,
    migration_generation: Option<u64>,
    install_mode: InstallMode,
    expected_revision: Option<u64>,
    staged: &mut Vec<StagedFile>,
) -> ConfigStoreResult<()> {
    AtomicFileStore::new(stage_dir.join(name))
        .sensitive(sensitive)
        .write_bytes_without_backup(candidate)?;
    if let Some(original) = original {
        if let Some(data_dir) = backup_dir.parent() {
            restrict_file_to_owner(&data_dir.join(name))?;
        }
        let plaintext = std::str::from_utf8(original).map_err(|_| {
            ConfigStoreError::Validation("migration backup source is not valid UTF-8".to_string())
        })?;
        let encrypted = crate::encryption::encrypt(plaintext).map_err(|_| {
            ConfigStoreError::Validation("migration backup encryption failed".to_string())
        })?;
        let protected_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "ciphertext": encrypted,
        }))?;
        AtomicFileStore::new(backup_dir.join(name))
            .sensitive(true)
            .write_bytes_without_backup(&protected_backup)?;
    }
    staged.push(StagedFile {
        name: name.to_string(),
        staged_name: name.to_string(),
        sha256: sha256(candidate),
        original_sha256: original.map(sha256),
        migration_generation,
        sensitive,
        install_mode,
        expected_revision,
        transaction_base_sha256: None,
        touched_credential_refs: Vec::new(),
        required_credential_refs: Vec::new(),
        touched_env_names: Vec::new(),
    });
    Ok(())
}

fn prepare_proxy_auth_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during proxy credential transaction",
    )?;
    for key in PROXY_AUTH_DOMAIN_KEYS {
        root.remove(key);
    }
    if let Some(reference) = candidate.proxy_auth_credential_ref.as_ref() {
        root.insert(
            "proxy_auth_credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
    }
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during proxy credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn prepare_env_var_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during env credential transaction",
    )?;
    let current_entries = root
        .get("env_vars")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let object = entry.as_object()?.clone();
            let name = object.get("name")?.as_str()?.to_string();
            Some((name, object))
        })
        .collect::<BTreeMap<_, _>>();
    let mut entries = Vec::with_capacity(candidate.env_vars.len());
    for mut entry in candidate.env_vars.clone() {
        if entry.secret {
            if entry.credential_ref.is_none() {
                return Err(ConfigStoreError::Validation(
                    "secret env credential metadata is incomplete".to_string(),
                ));
            }
            entry.value.clear();
            entry.value_encrypted = None;
        } else {
            entry.credential_ref = None;
            entry.configured = !entry.value.is_empty();
        }
        let mut durable = current_entries
            .get(&entry.name)
            .cloned()
            .unwrap_or_default();
        for key in [
            "name",
            "value",
            "secret",
            "value_encrypted",
            "credential_ref",
            "configured",
            "description",
        ] {
            durable.remove(key);
        }
        let typed = serde_json::to_value(entry)?;
        durable.extend(
            typed
                .as_object()
                .expect("env var serializes as object")
                .clone(),
        );
        entries.push(Value::Object(durable));
    }
    if entries.is_empty() {
        root.remove("env_vars");
    } else {
        root.insert("env_vars".to_string(), Value::Array(entries));
    }
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during env credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn prepare_notification_config_document(
    original: &[u8],
    candidate: &crate::Config,
    reset_domain: bool,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        original,
        "config.json is invalid during notification credential transaction",
    )?;
    let candidate_notifications = serde_json::to_value(&candidate.notifications)?;
    let candidate_notifications = candidate_notifications.as_object().ok_or_else(|| {
        ConfigStoreError::Validation(
            "notification config transaction document is invalid".to_string(),
        )
    })?;
    if reset_domain {
        root.insert(
            "notifications".to_string(),
            Value::Object(candidate_notifications.clone()),
        );
        let value = Value::Object(root);
        serde_json::from_value::<crate::Config>(value.clone()).map_err(|_| {
            ConfigStoreError::Validation(
                "config.json is invalid during notification credential transaction".to_string(),
            )
        })?;
        return Ok(serde_json::to_vec_pretty(&value)?);
    }
    let notifications = root
        .entry("notifications".to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "notifications must be an object during credential transaction".to_string(),
            )
        })?;
    for channel in ["desktop", "ntfy", "bark"] {
        let candidate_channel = candidate_notifications
            .get(channel)
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "notification channel config transaction document is invalid".to_string(),
                )
            })?;
        let channel_object = notifications
            .entry(channel.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .ok_or_else(|| {
                ConfigStoreError::Validation(format!(
                    "notification channel '{channel}' must be an object"
                ))
            })?;
        for (key, value) in candidate_channel {
            channel_object.insert(key.clone(), value.clone());
        }
        if channel == "ntfy" {
            channel_object.remove("token");
            channel_object.remove("token_encrypted");
        } else if channel == "bark" {
            channel_object.remove("device_key");
            channel_object.remove("device_key_encrypted");
        }
    }
    let value = Value::Object(root);
    serde_json::from_value::<crate::Config>(value.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during notification credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&value)?)
}

fn env_var_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    fn domain(bytes: &[u8]) -> ConfigStoreResult<Vec<Value>> {
        let root = parse_config_root_object(
            bytes,
            "config.json is invalid during env credential transaction",
        )?;
        Ok(root
            .get("env_vars")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    Ok(domain(current)? != domain(candidate)?)
}

fn notification_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    let current = parse_config_root_object(
        current,
        "config.json is invalid during notification credential transaction",
    )?;
    let candidate = parse_config_root_object(
        candidate,
        "config.json is invalid during notification credential transaction",
    )?;
    Ok(current.get("notifications") != candidate.get("notifications"))
}

fn parse_config_root_object(
    bytes: &[u8],
    invalid_message: &str,
) -> ConfigStoreResult<Map<String, Value>> {
    if bytes.is_empty() {
        return Ok(Map::new());
    }
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| ConfigStoreError::Validation(invalid_message.to_string()))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ConfigStoreError::Validation(invalid_message.to_string()))
}

fn proxy_auth_domain(root: &Map<String, Value>) -> Map<String, Value> {
    PROXY_AUTH_DOMAIN_KEYS
        .iter()
        .filter_map(|key| {
            root.get(*key)
                .cloned()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

fn replace_proxy_auth_domain(target: &mut Map<String, Value>, source: &Map<String, Value>) {
    for key in PROXY_AUTH_DOMAIN_KEYS {
        target.remove(key);
        if let Some(value) = source.get(key) {
            target.insert(key.to_string(), value.clone());
        }
    }
}

fn write_manifest(path: PathBuf, manifest: &MigrationManifest) -> ConfigStoreResult<()> {
    AtomicFileStore::new(path).write_bytes_without_backup(&serde_json::to_vec_pretty(manifest)?)
}

fn recover_committed(
    data_dir: &Path,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<Option<CredentialMigrationOutcome>> {
    let path = data_dir.join(MANIFEST_FILE);
    let Some(bytes) = read_optional_migration_file(&path)? else {
        return Ok(None);
    };
    let mut manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.state == MigrationState::Complete {
        cleanup_transaction_dirs(data_dir, &manifest)?;
        remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
        return Ok(None);
    }
    install_pending(
        data_dir,
        &mut manifest,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(Some(CredentialMigrationOutcome {
        migrated_credentials: 0,
        resumed: true,
    }))
}

fn initial_exact_transaction_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
    name: &str,
) -> ConfigStoreResult<(StagedFile, Vec<u8>)> {
    let journal_bytes =
        read_optional_migration_file(&data_dir.join(JOURNAL_FILE))?.ok_or_else(|| {
            ConfigStoreError::Validation("credential transaction journal is missing".to_string())
        })?;
    let journal: MigrationManifest = serde_json::from_slice(&journal_bytes)?;
    validate_manifest(&journal)?;
    if journal.transaction_id != manifest.transaction_id
        || journal.stage_dir != manifest.stage_dir
        || journal.exact_scope != manifest.exact_scope
    {
        return Err(ConfigStoreError::Validation(
            "credential transaction journal does not match the committed manifest".to_string(),
        ));
    }
    let file = journal
        .files
        .into_iter()
        .find(|file| file.name == name)
        .ok_or_else(|| {
            ConfigStoreError::Validation("credential transaction journal is incomplete".to_string())
        })?;
    let staged =
        std::fs::read(validated_stage_dir(data_dir, &journal.stage_dir)?.join(&file.staged_name))?;
    if sha256(&staged) != file.sha256 {
        return Err(ConfigStoreError::Validation(
            "initial transaction document failed integrity validation".to_string(),
        ));
    }
    Ok((file, staged))
}

fn rollback_proxy_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    if initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str()) {
        return Err(ConfigStoreError::Validation(
            "config transaction backup failed integrity validation".to_string(),
        ));
    }
    let original_root = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let staged_root = parse_config_root_object(
        &initial_staged,
        "initial proxy config transaction document is invalid",
    )?;
    let staged_domain = proxy_auth_domain(&staged_root);
    let target = data_dir.join(CONFIG_FILE);

    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == sha256(&original) {
            return Ok(());
        }
        let mut current_root = parse_config_root_object(
            &current,
            "config.json changed to an invalid document while aborting proxy transaction",
        )?;
        if proxy_auth_domain(&current_root) != staged_domain {
            // The transaction never installed its auth-domain change, or a
            // later same-domain writer won. Preserve that current value.
            return Ok(());
        }
        replace_proxy_auth_domain(&mut current_root, &original_root);
        let rolled_back = Value::Object(current_root);
        serde_json::from_value::<crate::Config>(rolled_back.clone()).map_err(|_| {
            ConfigStoreError::Validation(
                "config.json is invalid after proxy transaction rollback".to_string(),
            )
        })?;
        let rolled_back = serde_json::to_vec_pretty(&rolled_back)?;
        if sha256(&rolled_back) == current_hash {
            return Ok(());
        }
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "proxy config transaction rollback could not obtain a stable document".to_string(),
    ))
}

fn rollback_proxy_credential_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CREDENTIALS_FILE)?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CREDENTIALS_FILE))?;
    if initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str()) {
        return Err(ConfigStoreError::Validation(
            "credential transaction backup failed integrity validation".to_string(),
        ));
    }
    let target = data_dir.join(CREDENTIALS_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == sha256(&original) {
            return Ok(());
        }
        let (rolled_back, _revision, changed) =
            CredentialStore::rollback_exact_transaction_documents(
                &original,
                &initial_staged,
                &current,
                &credential_file.touched_credential_refs,
            )?;
        if !changed {
            return Ok(());
        }
        if AtomicFileStore::new(&target)
            .sensitive(true)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "proxy credential transaction rollback could not obtain a stable revision".to_string(),
    ))
}

fn abort_proxy_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(());
    }
    rollback_proxy_config_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_env_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (_initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let staged = parse_config_root_object(
        &initial_staged,
        "staged env config transaction document is invalid",
    )?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    let original = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let mut object = parse_config_root_object(
            &current,
            "config.json changed to an invalid document during env rollback",
        )?;
        if object.get("env_vars") != staged.get("env_vars") {
            return Ok(());
        }
        match original.get("env_vars").cloned() {
            Some(entries) => {
                object.insert("env_vars".to_string(), entries);
            }
            None => {
                object.remove("env_vars");
            }
        }
        let rolled_back = serde_json::to_vec_pretty(&Value::Object(object))?;
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "env config transaction rollback could not obtain a stable document".to_string(),
    ))
}

fn abort_env_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::EnvVars) {
        return Ok(());
    }
    rollback_env_config_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_notification_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (_initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let staged = parse_config_root_object(
        &initial_staged,
        "staged notification config transaction document is invalid",
    )?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    let original = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let mut object = parse_config_root_object(
            &current,
            "config.json changed to an invalid document during notification rollback",
        )?;
        if object.get("notifications") != staged.get("notifications") {
            return Ok(());
        }
        match original.get("notifications").cloned() {
            Some(value) => {
                object.insert("notifications".to_string(), value);
            }
            None => {
                object.remove("notifications");
            }
        }
        let rolled_back = serde_json::to_vec_pretty(&Value::Object(object))?;
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "notification config rollback could not obtain a stable document".to_string(),
    ))
}

fn abort_notification_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return Ok(());
    }
    rollback_notification_config_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn ensure_proxy_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if let Err(error) = ensure_proxy_ref_has_no_durable_non_proxy_consumers(data_dir, manifest) {
        abort_proxy_exact_transaction(data_dir, manifest)?;
        return Err(error);
    }
    Ok(())
}

fn ensure_notification_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    channel: &str,
) -> ConfigStoreResult<()> {
    for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if notification_document_has_other_consumer(
            &bytes,
            reference,
            channel,
            name == CONFIG_FILE,
        )? {
            return Err(ConfigStoreError::Validation(
                "notification credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_notification_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let (_, staged_config) = initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let mut refs = notification_refs_from_document(&staged_config)?;
    for (channel, reference) in
        notification_refs_from_document(&read_target_or_empty(&data_dir.join(CONFIG_FILE))?)?
    {
        refs.entry(channel).or_insert(reference);
    }
    for reference in &credential_file.touched_credential_refs {
        let channel = refs
            .iter()
            .find_map(|(channel, candidate)| {
                (candidate.as_str() == reference).then_some(channel.as_str())
            })
            .or_else(|| {
                (reference == "notification.ntfy.token")
                    .then_some("ntfy")
                    .or_else(|| (reference == "notification.bark.device_key").then_some("bark"))
            })
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "committed notification transaction reference is invalid".to_string(),
                )
            })?;
        if let Err(error) = ensure_notification_ref_exclusive(data_dir, reference, channel) {
            abort_notification_exact_transaction(data_dir, manifest)?;
            return Err(error);
        }
    }
    Ok(())
}

fn install_pending(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    validate_manifest(manifest)?;
    ensure_proxy_consumers_or_abort(data_dir, manifest)?;
    ensure_notification_consumers_or_abort(data_dir, manifest)?;
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let names = manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    for name in names {
        let Some(file_index) = manifest.files.iter().position(|file| file.name == name) else {
            continue;
        };
        let file = manifest.files[file_index].clone();
        let target = data_dir.join(&file.name);
        if file.install_mode == InstallMode::Exact && file.name == CREDENTIALS_FILE {
            install_exact_credential_member(
                data_dir,
                manifest,
                file_index,
                #[cfg(test)]
                fault,
            )?;
            #[cfg(test)]
            if fault == Some(MigrationFault::AfterCredentials) {
                return Err(injected_fault());
            }
            continue;
        }
        let staged = std::fs::read(stage_dir.join(&file.staged_name))?;
        if sha256(&staged) != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "staged migration document failed integrity validation".to_string(),
            ));
        }
        if file.install_mode == InstallMode::Exact {
            ensure_exact_transaction_credentials(data_dir, manifest)?;
            let current = read_target_or_empty(&target)?;
            if sha256(&current) != file.sha256 {
                let expected = file.original_sha256.as_deref().ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "transaction member base hash is missing".to_string(),
                    )
                })?;
                if !AtomicFileStore::new(&target)
                    .sensitive(file.sensitive)
                    .write_bytes_if_hash_with_backup(expected, &staged)?
                {
                    if file.name == CONFIG_FILE
                        && install_rebased_exact_config_member(
                            data_dir,
                            manifest,
                            file_index,
                            #[cfg(test)]
                            fault,
                        )?
                    {
                        continue;
                    }
                    return Err(ConfigStoreError::Validation(format!(
                        "{} changed during committed transaction",
                        file.name
                    )));
                }
            }
        } else if file.name == CREDENTIALS_FILE {
            CredentialStore::open(data_dir).commit_migration(&staged)?;
        } else {
            let current_hash = std::fs::read(&target).ok().map(|bytes| sha256(&bytes));
            if current_hash.as_deref() != Some(file.sha256.as_str()) {
                let expected = file.original_sha256.as_deref().ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "migration section base hash is missing".to_string(),
                    )
                })?;
                if !AtomicFileStore::new(&target)
                    .sensitive(file.sensitive)
                    .write_bytes_if_hash(expected, &staged)?
                {
                    if !rebase_changed_section(
                        data_dir,
                        manifest,
                        file_index,
                        #[cfg(test)]
                        fault,
                    )? {
                        continue;
                    }
                    let rebased = manifest.files[file_index].clone();
                    let rebased_bytes = std::fs::read(stage_dir.join(&rebased.staged_name))?;
                    let expected = rebased.original_sha256.as_deref().ok_or_else(|| {
                        ConfigStoreError::Validation(
                            "migration section base hash is missing".to_string(),
                        )
                    })?;
                    if !AtomicFileStore::new(&target)
                        .sensitive(rebased.sensitive)
                        .write_bytes_if_hash(expected, &rebased_bytes)?
                    {
                        return Err(ConfigStoreError::Validation(
                            "configuration section changed repeatedly during migration".to_string(),
                        ));
                    }
                }
            }
        }
        #[cfg(test)]
        if matches!(
            (fault, file.name.as_str()),
            (Some(MigrationFault::AfterCredentials), CREDENTIALS_FILE)
                | (Some(MigrationFault::AfterProviders), PROVIDERS_FILE)
                | (Some(MigrationFault::AfterMcp), MCP_FILE)
                | (Some(MigrationFault::AfterBroker), BROKER_FILE)
                | (Some(MigrationFault::AfterConfig), CONFIG_FILE)
        ) {
            return Err(injected_fault());
        }
    }
    Ok(())
}

fn install_rebased_exact_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    if manifest.exact_scope == Some(ExactTransactionScope::EnvVars) {
        return install_rebased_env_config_member(data_dir, manifest, file_index);
    }
    if manifest.exact_scope == Some(ExactTransactionScope::Notifications) {
        return install_rebased_notification_config_member(data_dir, manifest, file_index);
    }
    install_rebased_proxy_config_member(
        data_dir,
        manifest,
        file_index,
        #[cfg(test)]
        fault,
    )
}

fn install_rebased_notification_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
) -> ConfigStoreResult<bool> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged_bytes = std::fs::read(stage_dir.join(&file.staged_name))?;
    let staged = parse_config_root_object(
        &staged_bytes,
        "staged notification config transaction document is invalid",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut current_object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed notification transaction",
    )?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    if current_object.get("notifications") != original_object.get("notifications")
        && current_object.get("notifications") != staged.get("notifications")
    {
        abort_notification_exact_transaction(data_dir, manifest)?;
        return Err(ConfigStoreError::Validation(
            "notification metadata changed during committed transaction".to_string(),
        ));
    }
    match staged.get("notifications").cloned() {
        Some(value) => {
            current_object.insert("notifications".to_string(), value);
        }
        None => {
            current_object.remove("notifications");
        }
    }
    let rebased = Value::Object(current_object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed notification transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed notification transaction".to_string(),
        ));
    }
    Ok(true)
}

fn install_rebased_env_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
) -> ConfigStoreResult<bool> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged_bytes = std::fs::read(stage_dir.join(&file.staged_name))?;
    let staged = parse_config_root_object(
        &staged_bytes,
        "staged env config transaction document is invalid",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut current_object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed env transaction",
    )?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let current_entries = env_entry_map(&current_object)?;
    let original_entries = env_entry_map(&original_object)?;
    let staged_entries = env_entry_map(&staged)?;
    let touched_names = file
        .touched_env_names
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if touched_names.is_empty() {
        return Err(ConfigStoreError::Validation(
            "committed env transaction is missing touched names".to_string(),
        ));
    }
    let mut merged_entries = current_entries.clone();
    let required_refs = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .map(|file| {
            file.required_credential_refs
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for name in &touched_names {
        let current_entry = current_entries.get(name);
        let original_entry = original_entries.get(name);
        let staged_entry = staged_entries.get(name);
        if current_entry == staged_entry {
            continue;
        }
        match staged_entry {
            Some(staged_entry) => {
                match merge_env_entry_three_way(current_entry, original_entry, staged_entry)? {
                    Some(merged) => {
                        merged_entries.insert(name.clone(), merged);
                    }
                    None => {
                        let current_ref = current_entry
                            .and_then(|entry| entry.get("credential_ref"))
                            .and_then(Value::as_str);
                        if current_ref.is_some_and(|reference| required_refs.contains(reference)) {
                            // The external same-name winner still consumes the
                            // credential required by the transaction. Keep both;
                            // rolling the credential back could manufacture a
                            // dangling configured reference.
                            continue;
                        }
                        abort_env_exact_transaction(data_dir, manifest)?;
                        return Err(ConfigStoreError::Validation(
                            "env metadata changed during committed transaction".to_string(),
                        ));
                    }
                }
            }
            None => {
                if env_known_fields(current_entry) == env_known_fields(original_entry) {
                    merged_entries.remove(name);
                } else if current_entry.is_some() {
                    let current_ref = current_entry
                        .and_then(|entry| entry.get("credential_ref"))
                        .and_then(Value::as_str);
                    if !current_ref.is_some_and(|reference| required_refs.contains(reference)) {
                        abort_env_exact_transaction(data_dir, manifest)?;
                        return Err(ConfigStoreError::Validation(
                            "env metadata changed during committed transaction".to_string(),
                        ));
                    }
                }
            }
        }
    }
    let mut ordered = Vec::new();
    let mut emitted = BTreeSet::new();
    if let Some(current_array) = current_object.get("env_vars").and_then(Value::as_array) {
        for entry in current_array {
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            if let Some(merged) = merged_entries.get(name) {
                ordered.push(merged.clone());
                emitted.insert(name.to_string());
            }
        }
    }
    if let Some(staged_array) = staged.get("env_vars").and_then(Value::as_array) {
        for entry in staged_array {
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            if !emitted.contains(name) {
                if let Some(merged) = merged_entries.get(name) {
                    ordered.push(merged.clone());
                    emitted.insert(name.to_string());
                }
            }
        }
    }
    if ordered.is_empty() {
        current_object.remove("env_vars");
    } else {
        current_object.insert("env_vars".to_string(), Value::Array(ordered));
    }
    let rebased = Value::Object(current_object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed env transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed env transaction".to_string(),
        ));
    }
    Ok(true)
}

const ENV_ENTRY_KNOWN_KEYS: [&str; 7] = [
    "name",
    "value",
    "secret",
    "value_encrypted",
    "credential_ref",
    "configured",
    "description",
];

fn env_entry_map(object: &Map<String, Value>) -> ConfigStoreResult<BTreeMap<String, Value>> {
    let mut entries = BTreeMap::new();
    let Some(array) = object.get("env_vars").and_then(Value::as_array) else {
        return Ok(entries);
    };
    for entry in array {
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ConfigStoreError::Validation("env var name is missing".to_string()))?;
        if entries.insert(name.to_string(), entry.clone()).is_some() {
            return Err(ConfigStoreError::Validation(
                "environment variable names must be unique".to_string(),
            ));
        }
    }
    Ok(entries)
}

fn env_known_fields(entry: Option<&Value>) -> Option<Map<String, Value>> {
    let object = entry?.as_object()?;
    Some(
        ENV_ENTRY_KNOWN_KEYS
            .iter()
            .filter_map(|key| {
                object
                    .get(*key)
                    .cloned()
                    .map(|value| ((*key).to_string(), value))
            })
            .collect(),
    )
}

/// Returns `None` for a true same-field three-way conflict. Unknown fields are
/// always taken from the current document so forward-compatible metadata is
/// not normalized away by an env credential update.
fn merge_env_entry_three_way(
    current: Option<&Value>,
    original: Option<&Value>,
    staged: &Value,
) -> ConfigStoreResult<Option<Value>> {
    let staged = staged.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("staged env entry is not an object".to_string())
    })?;
    let current_object = current.and_then(Value::as_object);
    let original_object = original.and_then(Value::as_object);
    let mut merged = current_object.cloned().unwrap_or_default();
    for key in ENV_ENTRY_KNOWN_KEYS {
        let current_value = current_object.and_then(|object| object.get(key));
        let original_value = original_object.and_then(|object| object.get(key));
        let staged_value = staged.get(key);
        let chosen = if current_value == staged_value || current_value == original_value {
            staged_value
        } else if staged_value == original_value {
            current_value
        } else {
            return Ok(None);
        };
        match chosen {
            Some(value) => {
                merged.insert(key.to_string(), value.clone());
            }
            None => {
                merged.remove(key);
            }
        }
    }
    Ok(Some(Value::Object(merged)))
}

fn install_rebased_proxy_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(false);
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    if credential_file.touched_credential_refs.len() != 1 {
        return Ok(false);
    }
    let touched_ref = credential_file.touched_credential_refs[0].clone();
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged: Value = serde_json::from_slice(&std::fs::read(stage_dir.join(&file.staged_name))?)?;
    let staged_ref = staged
        .get("proxy_auth_credential_ref")
        .and_then(Value::as_str);
    if staged_ref != Some(touched_ref.as_str()) {
        return Ok(false);
    }

    ensure_proxy_consumers_or_abort(data_dir, manifest)?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed proxy transaction",
    )?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CONFIG_FILE))?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let staged_object = parse_config_root_object(
        &serde_json::to_vec(&staged)?,
        "staged proxy config transaction document is invalid",
    )?;
    let current_domain = proxy_auth_domain(&object);
    let original_domain = proxy_auth_domain(&original_object);
    let staged_domain = proxy_auth_domain(&staged_object);
    if current_domain != original_domain && current_domain != staged_domain {
        abort_proxy_exact_transaction(data_dir, manifest)?;
        return Err(ConfigStoreError::Validation(
            "proxy authentication metadata changed during committed transaction".to_string(),
        ));
    }
    replace_proxy_auth_domain(&mut object, &staged_object);
    let rebased = Value::Object(object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed proxy transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterExactProxyConfigRebaseManifestExternalWrite) {
        inject_external_proxy_root_write(&target)?;
        return Err(injected_fault());
    }
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed proxy transaction".to_string(),
        ));
    }
    Ok(true)
}

fn ensure_proxy_ref_has_no_durable_non_proxy_consumers(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let [touched_ref] = credential_file.touched_credential_refs.as_slice() else {
        return Err(ConfigStoreError::Validation(
            "committed proxy credential transaction is invalid".to_string(),
        ));
    };

    ensure_no_durable_non_proxy_consumers(data_dir, touched_ref)
}

fn ensure_no_durable_non_proxy_consumers(
    data_dir: &Path,
    touched_ref: &str,
) -> ConfigStoreResult<()> {
    for name in [CONFIG_FILE, PROVIDERS_FILE, MCP_FILE] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if bytes.is_empty() {
            continue;
        }
        ensure_no_non_proxy_consumers_in_document(&bytes, touched_ref, name == CONFIG_FILE, name)?;
    }
    Ok(())
}

fn ensure_no_non_proxy_consumers_in_document(
    bytes: &[u8],
    touched_ref: &str,
    config_root: bool,
    document_name: &str,
) -> ConfigStoreResult<()> {
    let document: Value = serde_json::from_slice(bytes).map_err(|_| {
        ConfigStoreError::Validation(format!(
            "{document_name} changed to an invalid document during proxy credential migration"
        ))
    })?;
    if contains_non_proxy_credential_reference(&document, touched_ref, config_root) {
        return Err(ConfigStoreError::Validation(
            "proxy auth credential reference has a durable non-proxy consumer".to_string(),
        ));
    }
    Ok(())
}

fn contains_non_proxy_credential_reference(
    value: &Value,
    touched_ref: &str,
    config_root: bool,
) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            if config_root && key == "proxy_auth_credential_ref" {
                return false;
            }
            (key == "credential_ref" && child.as_str() == Some(touched_ref))
                || (key.ends_with("_credential_refs") && contains_string_value(child, touched_ref))
                || contains_non_proxy_credential_reference(child, touched_ref, false)
        }),
        Value::Array(values) => values
            .iter()
            .any(|child| contains_non_proxy_credential_reference(child, touched_ref, false)),
        _ => false,
    }
}

fn contains_string_value(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_string_value(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_string_value(value, expected)),
        _ => false,
    }
}

#[cfg(test)]
fn inject_external_proxy_root_write(target: &Path) -> ConfigStoreResult<()> {
    let mut current: Value = serde_json::from_slice(&std::fs::read(target)?)?;
    let object = current.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed proxy transaction"
                .to_string(),
        )
    })?;
    object.insert(
        "external_rebase_generation".to_string(),
        Value::Number(2_u64.into()),
    );
    AtomicFileStore::new(target).write_bytes_without_backup(&serde_json::to_vec_pretty(&current)?)
}

fn install_exact_credential_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let backup_dir = validated_backup_dir(data_dir, &manifest.transaction_id)?;
    let original = read_encrypted_migration_backup(&backup_dir.join(CREDENTIALS_FILE))?;
    let target = data_dir.join(CREDENTIALS_FILE);

    for _ in 0..16 {
        let file = manifest.files[file_index].clone();
        if file
            .transaction_base_sha256
            .as_deref()
            .is_some_and(|expected| sha256(&original) != expected)
        {
            return Err(ConfigStoreError::Validation(
                "credential migration backup failed integrity validation".to_string(),
            ));
        }
        let staged = std::fs::read(stage_dir.join(&file.staged_name))?;
        if sha256(&staged) != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "staged migration document failed integrity validation".to_string(),
            ));
        }
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == file.sha256 {
            CredentialStore::ensure_required_refs_in_bytes(
                &current,
                &file.required_credential_refs,
            )?;
            return Ok(());
        }
        let expected_hash = file.original_sha256.as_deref().ok_or_else(|| {
            ConfigStoreError::Validation("transaction member base hash is missing".to_string())
        })?;
        if current_hash == expected_hash {
            CredentialStore::ensure_required_refs_in_bytes(
                &staged,
                &file.required_credential_refs,
            )?;
            if AtomicFileStore::new(&target)
                .sensitive(true)
                .write_bytes_if_hash_with_backup(expected_hash, &staged)?
            {
                CredentialStore::ensure_required_refs_in_bytes(
                    &staged,
                    &file.required_credential_refs,
                )?;
                return Ok(());
            }
            continue;
        }
        if file.touched_credential_refs.is_empty()
            && manifest.exact_scope != Some(ExactTransactionScope::Notifications)
        {
            return Err(ConfigStoreError::Validation(
                "committed credential transaction cannot be safely rebased".to_string(),
            ));
        }
        let preserve_notification_domain_revision =
            if manifest.exact_scope == Some(ExactTransactionScope::Notifications) {
                let (_, staged_config) =
                    initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
                let original_config = read_encrypted_migration_backup(
                    &validated_backup_dir(data_dir, &manifest.transaction_id)?.join(CONFIG_FILE),
                )?;
                notification_domain_changed(&original_config, &staged_config)?
            } else {
                false
            };
        let (rebased, current_revision, remaining_required_refs) =
            CredentialStore::merge_exact_transaction_documents(
                &original,
                &staged,
                &current,
                &file.touched_credential_refs,
                &file.required_credential_refs,
                preserve_notification_domain_revision,
            )?;
        let staged_name = format!("{CREDENTIALS_FILE}.rebase.{}", Uuid::new_v4());
        AtomicFileStore::new(stage_dir.join(&staged_name))
            .sensitive(true)
            .write_bytes_without_backup(&rebased)?;
        sync_dir(&stage_dir)?;
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterExactCredentialRebaseStage) {
            return Err(injected_fault());
        }
        let file = &mut manifest.files[file_index];
        file.staged_name = staged_name;
        file.sha256 = sha256(&rebased);
        file.original_sha256 = Some(current_hash);
        file.expected_revision = Some(current_revision);
        file.required_credential_refs = remaining_required_refs;
        write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterExactCredentialRebaseManifest) {
            return Err(injected_fault());
        }
    }
    Err(ConfigStoreError::Validation(
        "credential transaction could not obtain a stable revision".to_string(),
    ))
}

fn ensure_exact_transaction_credentials(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let current = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    CredentialStore::ensure_required_refs_in_bytes(&current, &file.required_credential_refs)
}

fn read_encrypted_migration_backup(path: &Path) -> ConfigStoreResult<Vec<u8>> {
    let protected: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    if protected.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(ConfigStoreError::Validation(
            "credential migration backup is invalid".to_string(),
        ));
    }
    let ciphertext = protected
        .get("ciphertext")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ConfigStoreError::Validation("credential migration backup is invalid".to_string())
        })?;
    crate::encryption::decrypt(ciphertext)
        .map(String::into_bytes)
        .map_err(|_| {
            ConfigStoreError::Validation("credential migration backup is unavailable".to_string())
        })
}

fn rebase_changed_section(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    let name = manifest.files[file_index].name.clone();
    let mut extracted = Vec::new();
    let minimum_generation =
        next_revision(manifest.files[file_index].migration_generation.unwrap_or(0))?;
    let section = match name.as_str() {
        PROVIDERS_FILE => plan_provider_section(data_dir, &mut extracted, minimum_generation)?,
        MCP_FILE => plan_mcp_section(data_dir, &mut extracted, minimum_generation)?,
        BROKER_FILE => plan_broker_section(data_dir, &mut extracted, minimum_generation)?,
        CONFIG_FILE => {
            plan_provider_instance_section(data_dir, &mut extracted, minimum_generation, false)?
        }
        _ => {
            return Err(ConfigStoreError::Validation(
                "migration section target is invalid".to_string(),
            ))
        }
    };
    let Some(section) = section else {
        // A concurrent writer already produced a secret-free document. Treat
        // that newer revision as authoritative and remove the stale candidate.
        manifest.files.remove(file_index);
        write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        return Ok(false);
    };
    let store = CredentialStore::open(data_dir);
    let resolved = resolve_extracted_secrets(&store, extracted)?;
    let prepared = store.prepare_migration(resolved)?;
    store.commit_migration(&prepared.bytes)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseCredentialCommit) {
        return Err(injected_fault());
    }

    let staged_name = format!("{}.rebase.{}", section.name, Uuid::new_v4());
    let stage_path = validated_stage_dir(data_dir, &manifest.stage_dir)?.join(&staged_name);
    AtomicFileStore::new(stage_path).write_bytes_without_backup(&section.bytes)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseStageWrite) {
        return Err(injected_fault());
    }
    manifest.files[file_index].staged_name = staged_name;
    manifest.files[file_index].sha256 = sha256(&section.bytes);
    manifest.files[file_index].original_sha256 = Some(sha256(&section.original));
    manifest.files[file_index].migration_generation = Some(section.migration_generation);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseManifest) {
        return Err(injected_fault());
    }
    Ok(true)
}

fn finish_transaction(data_dir: &Path, mut manifest: MigrationManifest) -> ConfigStoreResult<()> {
    let contains_broker = manifest.files.iter().any(|file| file.name == BROKER_FILE);
    let contains_non_broker_config = manifest
        .files
        .iter()
        .any(|file| matches!(file.name.as_str(), PROVIDERS_FILE | MCP_FILE | CONFIG_FILE));
    if contains_non_broker_config {
        let proxy_clear_tombstones = proxy_clear_tombstones_from_manifest(&manifest);
        let env_clear_tombstones = env_clear_tombstones_from_manifest(&manifest);
        let notification_clear_tombstones = notification_clear_tombstones_from_manifest(&manifest);
        scrub_provider_instance_credentials_from_backups(
            data_dir,
            &proxy_clear_tombstones,
            &env_clear_tombstones,
            &notification_clear_tombstones,
        )?;
    }
    if contains_broker {
        scrub_broker_credentials_from_backups(data_dir)?;
    }
    manifest.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &manifest)
}

fn scrub_broker_credentials_from_backups(data_dir: &Path) -> ConfigStoreResult<usize> {
    let mut reference = match std::fs::read(data_dir.join(BROKER_FILE)) {
        Ok(bytes) => serde_json::from_slice::<crate::BrokerClientConfig>(&bytes)
            .ok()
            .and_then(|primary| primary.credential_ref),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let store = CredentialStore::open(data_dir);
    let mut backup_secret = None;
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{BROKER_FILE}.{suffix}"));
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let (candidate_ref, candidate_secret) =
            broker_backup_candidate(&bytes, reference.as_ref())?;
        if reference.is_none() {
            reference = candidate_ref;
        }
        let Some(current_ref) = reference.as_ref() else {
            continue;
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        if let Some(object) = root.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&root, current_ref.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker backup".to_string(),
            ));
        }
        if backup_secret.is_none()
            && !store.status_unchecked(current_ref)?.configured
            && candidate_secret.is_some()
        {
            backup_secret = candidate_secret;
        }
    }
    let Some(reference) = reference else {
        return Ok(0);
    };
    ensure_broker_ref_exclusive(data_dir, &reference)?;
    let migrated_credentials = if let Some(secret) = backup_secret {
        let resolved = resolve_extracted_secrets(
            &store,
            vec![ExtractedSecret {
                credential_ref: reference.clone(),
                value: secret,
                migration_generation: 1,
                kind: ExtractedSecretKind::ExternalBroker,
                env_owner: None,
            }],
        )?;
        let prepared = store.prepare_migration(resolved)?;
        let added = prepared.added;
        store.commit_migration(&prepared.bytes)?;
        added
    } else {
        0
    };
    let configured = store.status_unchecked(&reference)?.configured;
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{BROKER_FILE}.{suffix}"));
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        let Some(object) = root.as_object_mut() else {
            continue;
        };
        let mut changed = object.remove("token").is_some();
        changed |= object.remove("token_encrypted").is_some();
        changed |= object.get("credential_ref").and_then(Value::as_str) != Some(reference.as_str());
        changed |= object.get("configured").and_then(Value::as_bool) != Some(configured);
        if !changed {
            continue;
        }
        object.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        object.insert("configured".to_string(), Value::Bool(configured));
        AtomicFileStore::new(path)
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&root)?)?;
    }
    Ok(migrated_credentials)
}

fn broker_backup_candidate(
    bytes: &[u8],
    preferred_ref: Option<&CredentialRef>,
) -> ConfigStoreResult<(Option<CredentialRef>, Option<LegacySecret>)> {
    let mut root: Value = match serde_json::from_slice(bytes) {
        Ok(root) => root,
        Err(_) => return Ok((None, None)),
    };
    let Some(object) = root.as_object_mut() else {
        return Ok((None, None));
    };
    let plaintext = take_nonempty_string(object, "token")?;
    let ciphertext = take_nonempty_string(object, "token_encrypted")?;
    let secret = match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy broker backup credential could not be decrypted".to_string(),
                )
            })?;
            if plaintext != decrypted {
                return Err(ConfigStoreError::Validation(
                    "legacy broker backup credential fields contain conflicting values".to_string(),
                ));
            }
            Some(LegacySecret::Plaintext(plaintext))
        }
        (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
        (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
        (None, None) => None,
    };
    let configured = object
        .get("configured")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let explicit_ref = object
        .get("credential_ref")
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "broker backup credential reference must be a string".to_string(),
                )
            })
        })
        .transpose()?
        .map(|value| CredentialRef::parse(value.to_string()))
        .transpose()?;
    if let (Some(preferred), Some(explicit)) = (preferred_ref, explicit_ref.as_ref()) {
        if preferred != explicit {
            return Err(ConfigStoreError::Validation(
                "broker backup credential reference conflicts with primary configuration"
                    .to_string(),
            ));
        }
    }
    let reference = match preferred_ref {
        Some(reference) => Some(reference.clone()),
        None if explicit_ref.is_some() => explicit_ref,
        None if secret.is_some() || configured => {
            Some(credential_ref("broker", "external", "bearer_token")?)
        }
        None => None,
    };
    Ok((reference, secret))
}

fn proxy_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return BTreeSet::new();
    }
    manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .filter(|file| file.required_credential_refs.is_empty())
        .into_iter()
        .flat_map(|file| file.touched_credential_refs.iter().cloned())
        .collect()
}

fn env_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::EnvVars) {
        return BTreeSet::new();
    }
    let Some(file) = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
    else {
        return BTreeSet::new();
    };
    file.touched_credential_refs
        .iter()
        .filter(|reference| !file.required_credential_refs.contains(reference))
        .cloned()
        .collect()
}

fn notification_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return BTreeSet::new();
    }
    let Some(file) = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
    else {
        return BTreeSet::new();
    };
    file.touched_credential_refs
        .iter()
        .filter(|reference| !file.required_credential_refs.contains(reference))
        .cloned()
        .collect()
}

fn scrub_provider_instance_credentials_from_backups(
    data_dir: &Path,
    proxy_clear_tombstones: &BTreeSet<String>,
    env_clear_tombstones: &BTreeSet<String>,
    notification_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    let store = CredentialStore::open(data_dir);
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{CONFIG_FILE}.{suffix}"));
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        if !scrub_provider_instance_credentials(
            data_dir,
            &store,
            &mut root,
            proxy_clear_tombstones,
            env_clear_tombstones,
            notification_clear_tombstones,
        )? {
            continue;
        }
        AtomicFileStore::new(path)
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&root)?)?;
    }
    Ok(())
}

fn scrub_provider_instance_credentials(
    data_dir: &Path,
    store: &CredentialStore,
    root: &mut Value,
    proxy_clear_tombstones: &BTreeSet<String>,
    env_clear_tombstones: &BTreeSet<String>,
    notification_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<bool> {
    let Some(object) = root.as_object_mut() else {
        return Ok(false);
    };
    let mut extracted = Vec::new();
    let mut changed =
        if scrub_authoritative_or_tombstoned_proxy_auth(object, store, proxy_clear_tombstones)? {
            true
        } else {
            migrate_proxy_auth(object, &mut extracted, 1)?
        };
    changed |= migrate_env_vars(object, &mut extracted, 1)?;
    changed |= migrate_notification_credentials(object, &mut extracted, 1)?;
    if let Some(entries) = object.get_mut("env_vars").and_then(Value::as_array_mut) {
        for entry in entries {
            let Some(entry) = entry.as_object_mut() else {
                continue;
            };
            if entry
                .get("credential_ref")
                .and_then(Value::as_str)
                .is_some_and(|reference| env_clear_tombstones.contains(reference))
            {
                entry.insert("configured".to_string(), Value::Bool(false));
            }
        }
    }
    for channel in ["ntfy", "bark"] {
        let Some(config) = object
            .get_mut("notifications")
            .and_then(Value::as_object_mut)
            .and_then(|notifications| notifications.get_mut(channel))
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if config
            .get("credential_ref")
            .and_then(Value::as_str)
            .is_some_and(|reference| notification_clear_tombstones.contains(reference))
        {
            config.insert("configured".to_string(), Value::Bool(false));
        }
    }
    let mut pending = Vec::with_capacity(extracted.len());
    for secret in extracted.drain(..) {
        if secret.kind == ExtractedSecretKind::EnvVar {
            if env_clear_tombstones.contains(secret.credential_ref.as_str()) {
                continue;
            }
            let status = store.status_unchecked(&secret.credential_ref)?;
            if status.configured && status.source != crate::CredentialSource::Migrated {
                // The live store is authoritative; the backup was already
                // rewritten to metadata above, so never replay its stale copy.
                continue;
            }
        }
        if matches!(
            secret.kind,
            ExtractedSecretKind::NotificationNtfy | ExtractedSecretKind::NotificationBark
        ) {
            if notification_clear_tombstones.contains(secret.credential_ref.as_str()) {
                continue;
            }
            let status = store.status_unchecked(&secret.credential_ref)?;
            if status.configured && status.source != crate::CredentialSource::Migrated {
                continue;
            }
        }
        pending.push(secret);
    }
    extracted = pending;
    if let Some(instances) = object
        .get_mut("provider_instances")
        .and_then(Value::as_object_mut)
    {
        for (instance_id, value) in instances {
            let Some(instance) = value.as_object_mut() else {
                continue;
            };
            let plaintext = instance
                .get("api_key")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string);
            let ciphertext = instance
                .get("api_key_encrypted")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string);
            let had_secret =
                instance.contains_key("api_key") || instance.contains_key("api_key_encrypted");
            if !had_secret {
                continue;
            }
            let reference = if let Some(existing) = instance.get("credential_ref") {
                let existing = existing.as_str().ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "provider instance credential reference must be a string".to_string(),
                    )
                })?;
                CredentialRef::parse(existing.to_string())?
            } else {
                credential_ref("provider_instance", instance_id, "api_key")?
            };
            let value = plaintext
                .map(LegacySecret::Plaintext)
                .or_else(|| ciphertext.map(LegacySecret::Ciphertext));
            let status = store.status_unchecked(&reference)?;
            if !status.configured {
                if let Some(value) = value {
                    extracted.push(ExtractedSecret {
                        credential_ref: reference.clone(),
                        value,
                        migration_generation: 1,
                        kind: ExtractedSecretKind::Other,
                        env_owner: None,
                    });
                } else {
                    instance.remove("api_key");
                    instance.remove("api_key_encrypted");
                    changed = true;
                    continue;
                }
            }
            instance.remove("api_key");
            instance.remove("api_key_encrypted");
            instance.insert(
                "credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            changed = true;
        }
    }
    let prospective = serde_json::to_vec(root)?;
    ensure_legacy_proxy_extractions_are_safe(
        data_dir,
        store,
        &extracted,
        &[(prospective.as_slice(), true)],
    )?;
    ensure_legacy_notification_extractions_are_safe(
        data_dir,
        &extracted,
        &[(prospective.as_slice(), true)],
    )?;
    if !extracted.is_empty() {
        let resolved = resolve_extracted_secrets(store, extracted)?;
        let prepared = store.prepare_migration(resolved)?;
        store.commit_migration(&prepared.bytes)?;
    }
    Ok(changed)
}

fn cleanup_transaction_dirs(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    remove_managed_directory_if_exists(&stage_dir)?;
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
    remove_managed_directory_if_exists(&backup_dir)?;
    sync_dir(data_dir)?;
    Ok(())
}

fn discard_uncommitted(data_dir: &Path) -> ConfigStoreResult<()> {
    let journal_path = data_dir.join(JOURNAL_FILE);
    let Some(bytes) = read_optional_migration_file(&journal_path)? else {
        return Ok(());
    };
    let journal: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&journal)?;
    let stage_dir = validated_stage_dir(data_dir, &journal.stage_dir)?;
    remove_managed_directory_if_exists(&stage_dir)?;
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{}", journal.transaction_id));
    remove_managed_directory_if_exists(&backup_dir)?;
    remove_file_if_exists(&journal_path)?;
    sync_dir(data_dir)
}

fn cleanup_orphan_transaction_dirs(data_dir: &Path) -> ConfigStoreResult<()> {
    let mut referenced = BTreeSet::new();
    for file in [MANIFEST_FILE, JOURNAL_FILE] {
        let Some(bytes) = read_optional_migration_file(&data_dir.join(file))? else {
            continue;
        };
        let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| {
            ConfigStoreError::Validation("credential migration metadata is unavailable".to_string())
        })?;
        validate_manifest(&manifest).map_err(|_| {
            ConfigStoreError::Validation("credential migration metadata is unavailable".to_string())
        })?;
        referenced.insert(manifest.stage_dir.clone());
        referenced.insert(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
    }

    let mut removed = false;
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if referenced.contains(&name) || !is_strict_transaction_dir_name(&name) {
            continue;
        }
        std::fs::remove_dir_all(entry.path())?;
        removed = true;
    }
    if removed {
        sync_dir(data_dir)?;
    }
    Ok(())
}

fn is_strict_transaction_dir_name(name: &str) -> bool {
    [STAGE_PREFIX, BACKUP_PREFIX].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|suffix| Uuid::parse_str(suffix).ok().map(|uuid| (suffix, uuid)))
            .is_some_and(|(suffix, uuid)| uuid.to_string() == suffix)
    })
}

fn read_optional_migration_file(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ConfigStoreError::Validation(
            "credential migration metadata is unavailable".to_string(),
        )),
    }
}

fn read_target_or_empty(path: &Path) -> ConfigStoreResult<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn validate_manifest(manifest: &MigrationManifest) -> ConfigStoreResult<()> {
    let expected_stage = format!("{STAGE_PREFIX}{}", manifest.transaction_id);
    let unique = manifest
        .files
        .iter()
        .map(|file| file.name.as_str())
        .collect::<BTreeSet<_>>();
    let credential_count = manifest
        .files
        .iter()
        .filter(|file| file.name == CREDENTIALS_FILE)
        .count();
    let exact_transaction = manifest
        .files
        .iter()
        .any(|file| file.install_mode == InstallMode::Exact);
    let exact_scope_valid = match (exact_transaction, manifest.exact_scope) {
        (false, None) => true,
        (true, Some(ExactTransactionScope::ProxyAuth)) => {
            manifest.files.len() == 2
                && unique.contains(CONFIG_FILE)
                && manifest
                    .files
                    .iter()
                    .find(|file| file.name == CREDENTIALS_FILE)
                    .is_some_and(|file| file.touched_credential_refs.len() == 1)
        }
        (true, Some(ExactTransactionScope::EnvVars)) => {
            manifest.files.len() == 2
                && unique.contains(CONFIG_FILE)
                && manifest
                    .files
                    .iter()
                    .find(|file| file.name == CREDENTIALS_FILE)
                    .is_some_and(|file| !file.touched_credential_refs.is_empty())
        }
        (true, Some(ExactTransactionScope::Notifications)) => {
            manifest.files.len() == 2 && unique.contains(CONFIG_FILE)
        }
        (true, None) => manifest.files.len() == 3 && unique.contains(PROVIDERS_FILE),
        (false, Some(_)) => false,
    };
    let exact_shape_valid = !exact_transaction
        || ((manifest.files.len() == 2 || manifest.files.len() == 3)
            && manifest
                .files
                .iter()
                .all(|file| file.install_mode == InstallMode::Exact)
            && unique.contains(CONFIG_FILE)
            && exact_scope_valid
            && manifest.files.iter().all(|file| {
                file.migration_generation.is_none()
                    && ((file.name == CREDENTIALS_FILE && file.expected_revision.is_some())
                        || (file.name != CREDENTIALS_FILE && file.expected_revision.is_none()))
            }));
    if manifest.version != MIGRATION_VERSION
        || Uuid::parse_str(&manifest.transaction_id).is_err()
        || manifest.stage_dir != expected_stage
        || manifest.files.is_empty()
        || unique.len() != manifest.files.len()
        || credential_count != 1
        || !exact_scope_valid
        || !exact_shape_valid
        || manifest.files.iter().any(|file| {
            !matches!(
                file.name.as_str(),
                PROVIDERS_FILE | MCP_FILE | BROKER_FILE | CREDENTIALS_FILE | CONFIG_FILE
            ) || file.sha256.len() != 64
                || !valid_staged_name(file)
                || (file.name != CREDENTIALS_FILE
                    && file
                        .original_sha256
                        .as_ref()
                        .is_none_or(|hash| hash.len() != 64))
                || (file.install_mode == InstallMode::Exact
                    && file
                        .original_sha256
                        .as_ref()
                        .is_none_or(|hash| hash.len() != 64))
                || (file.name == CREDENTIALS_FILE && !file.sensitive)
                || (file.name != CREDENTIALS_FILE && file.sensitive)
                || (file.expected_revision.is_some() && file.name != CREDENTIALS_FILE)
                || file
                    .transaction_base_sha256
                    .as_ref()
                    .is_some_and(|hash| hash.len() != 64)
                || !valid_credential_ref_metadata(file)
        })
    {
        return Err(ConfigStoreError::Validation(
            "credential migration manifest is invalid".to_string(),
        ));
    }
    Ok(())
}

fn valid_staged_name(file: &StagedFile) -> bool {
    if file.staged_name == file.name {
        return true;
    }
    let Some(suffix) = file
        .staged_name
        .strip_prefix(&format!("{}.rebase.", file.name))
    else {
        return false;
    };
    Uuid::parse_str(suffix).is_ok()
}

fn valid_credential_ref_metadata(file: &StagedFile) -> bool {
    if file.name != CREDENTIALS_FILE || file.install_mode != InstallMode::Exact {
        let env_names = file.touched_env_names.iter().collect::<BTreeSet<_>>();
        return file.transaction_base_sha256.is_none()
            && file.touched_credential_refs.is_empty()
            && file.required_credential_refs.is_empty()
            && (file.touched_env_names.is_empty()
                || (file.name == CONFIG_FILE
                    && file.install_mode == InstallMode::Exact
                    && env_names.len() == file.touched_env_names.len()
                    && file.touched_env_names.iter().all(|name| !name.is_empty())));
    }
    let touched = file.touched_credential_refs.iter().collect::<BTreeSet<_>>();
    let required = file
        .required_credential_refs
        .iter()
        .collect::<BTreeSet<_>>();
    (file.touched_credential_refs.is_empty() || file.transaction_base_sha256.is_some())
        && touched.len() == file.touched_credential_refs.len()
        && required.len() == file.required_credential_refs.len()
        && required.is_subset(&touched)
        && touched
            .iter()
            .all(|value| CredentialRef::parse((*value).clone()).is_ok())
        && file.touched_env_names.is_empty()
}

fn validated_stage_dir(data_dir: &Path, name: &str) -> ConfigStoreResult<PathBuf> {
    let path = data_dir.join(name);
    if !name.starts_with(STAGE_PREFIX) || path.parent() != Some(data_dir) {
        return Err(ConfigStoreError::Validation(
            "credential migration stage path is invalid".to_string(),
        ));
    }
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() => {
            return Err(ConfigStoreError::Validation(
                "credential migration stage path is invalid".to_string(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(path)
}

fn validated_backup_dir(data_dir: &Path, transaction_id: &str) -> ConfigStoreResult<PathBuf> {
    let name = format!("{BACKUP_PREFIX}{transaction_id}");
    let path = data_dir.join(&name);
    if Uuid::parse_str(transaction_id).is_err() || path.parent() != Some(data_dir) {
        return Err(ConfigStoreError::Validation(
            "credential migration backup path is invalid".to_string(),
        ));
    }
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            Ok(path)
        }
        Ok(_) => Err(ConfigStoreError::Validation(
            "credential migration backup path is invalid".to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            ConfigStoreError::Validation("credential migration backup is unavailable".to_string()),
        ),
        Err(error) => Err(error.into()),
    }
}

fn remove_managed_directory_if_exists(path: &Path) -> ConfigStoreResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            std::fs::remove_dir_all(path)?;
            Ok(())
        }
        Ok(_) => Err(ConfigStoreError::Validation(
            "credential migration directory is invalid".to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn next_revision(revision: u64) -> ConfigStoreResult<u64> {
    revision.checked_add(1).ok_or_else(|| {
        ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
    })
}

fn remove_file_if_exists(path: &Path) -> ConfigStoreResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn create_private_dir(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir(path)?;
    Ok(())
}

fn restrict_file_to_owner(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if path.exists() {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(())
}

fn restrict_directory_files_to_owner(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            restrict_file_to_owner(&entry.path())?;
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sync_dir(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
fn injected_fault() -> ConfigStoreError {
    ConfigStoreError::Io(std::io::Error::other("injected migration crash"))
}

struct MigrationLock(File);

impl Drop for MigrationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_no_migration_transaction_artifacts(data_dir: &Path) {
        assert!(!data_dir.join(MANIFEST_FILE).exists());
        assert!(!data_dir.join(JOURNAL_FILE).exists());
        for entry in std::fs::read_dir(data_dir).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            assert!(!name.starts_with(STAGE_PREFIX));
            assert!(!name.starts_with(BACKUP_PREFIX));
        }
    }

    #[test]
    fn legacy_proxy_auth_migrates_idempotently_and_hydrates_runtime() {
        let _key = crate::encryption::set_test_encryption_key([0x91; 32]);
        let dir = tempfile::tempdir().unwrap();
        let auth = serde_json::json!({"username": "alice", "password": "proxy-secret"});
        let ciphertext = crate::encryption::encrypt(&auth.to_string()).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "http_proxy": "http://proxy.example:8080",
                "proxy_auth_encrypted": ciphertext,
                "unknown_proxy_peer": {"kept": true}
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let first_root = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let first_credentials = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let root: Value = serde_json::from_slice(&first_root).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth").is_none());
        assert!(root.get("proxy_auth_encrypted").is_none());
        assert!(root.get("http_proxy_auth_encrypted").is_none());
        assert!(root.get("https_proxy_auth_encrypted").is_none());
        assert_eq!(root["unknown_proxy_peer"]["kept"], true);
        assert!(!String::from_utf8_lossy(&first_credentials).contains("proxy-secret"));

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let loaded_auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(loaded_auth.username, "alice");
        assert_eq!(loaded_auth.password, "proxy-secret");

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            first_root
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            first_credentials
        );
    }

    #[test]
    fn conflicting_legacy_proxy_auth_rolls_back_before_any_commit() {
        let _key = crate::encryption::set_test_encryption_key([0x92; 32]);
        let dir = tempfile::tempdir().unwrap();
        let first = crate::encryption::encrypt(
            &serde_json::json!({"username": "alice", "password": "one"}).to_string(),
        )
        .unwrap();
        let second = crate::encryption::encrypt(
            &serde_json::json!({"username": "alice", "password": "two"}).to_string(),
        )
        .unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "proxy_auth_encrypted": first,
            "https_proxy_auth_encrypted": second
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("conflicting"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_ref_shared_with_a_provider_instance_before_staging() {
        let _key = crate::encryption::set_test_encryption_key([0x9c; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = CredentialRef::parse("provider.shared").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared-consumer": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "credential_ref": shared.as_str()
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_generated_canonical_ref_used_by_provider_sidecar() {
        let _key = crate::encryption::set_test_encryption_key([0x9d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "model": "gpt-test",
                    "credential_ref": canonical.as_str()
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_an_invalid_user_managed_canonical_target() {
        let _key = crate::encryption::set_test_encryption_key([0x9e; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical,
                "not-a-proxy-auth-document",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("already user-managed"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_prospective_provider_instance_ref_collision() {
        let _key = crate::encryption::set_test_encryption_key([0xa0; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider_instance", "shared", "api_key").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "api_key": same_secret
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_prospective_provider_sidecar_ref_collision() {
        let _key = crate::encryption::set_test_encryption_key([0xa1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str()
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "model": "gpt-test",
                    "api_key": same_secret
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_auth_exact_transaction_preserves_shared_refs_on_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x93; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "shared-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared.clone()),
            ..crate::OpenAIConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = None;

        persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap();
        assert_eq!(
            store.resolve(&shared).unwrap().unwrap().expose(),
            "shared-secret"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth_encrypted").is_none());
        assert!(store
            .resolve(&credential_ref("proxy", "default", "auth").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn proxy_clear_tombstone_scrubs_a_late_backup_without_reimporting_secret() {
        let _key = crate::encryption::set_test_encryption_key([0xa7; 32]);
        let dir = tempfile::tempdir().unwrap();
        let cleared_ref = CredentialRef::parse("proxy.custom.auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                cleared_ref.clone(),
                &serde_json::to_string(&crate::ProxyAuth {
                    username: "current-user".to_string(),
                    password: "current-password".to_string(),
                })
                .unwrap(),
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": cleared_ref.as_str()
            }))
            .unwrap(),
        )
        .unwrap();
        let mut candidate = crate::Config::default();
        candidate.proxy_auth_credential_ref = Some(cleared_ref.clone());
        candidate.proxy_auth = None;
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterConfig),
        )
        .is_err());
        assert!(store.resolve_unchecked(&cleared_ref).unwrap().is_none());

        let late_ciphertext = crate::encryption::encrypt(
            &serde_json::to_string(&crate::ProxyAuth {
                username: "late-backup-user".to_string(),
                password: "late-backup-password".to_string(),
            })
            .unwrap(),
        )
        .unwrap();
        let late_backup = dir.path().join("config.json.bak.2");
        std::fs::write(
            &late_backup,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": late_ciphertext
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        assert!(store.resolve_unchecked(&cleared_ref).unwrap().is_none());
        let backup: Value = serde_json::from_slice(&std::fs::read(&late_backup).unwrap()).unwrap();
        assert!(backup.get("proxy_auth_encrypted").is_none());
        assert_eq!(backup["proxy_auth_credential_ref"], cleared_ref.as_str());
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert!(loaded.proxy_auth.is_none());
        assert!(store.resolve(&cleared_ref).unwrap().is_none());
        assert!(store
            .resolve(&credential_ref("proxy", "default", "auth").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn proxy_auth_replace_detaches_from_shared_provider_ref_without_overwrite() {
        let _key = crate::encryption::set_test_encryption_key([0x96; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared.clone()),
            ..crate::OpenAIConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = Some(crate::ProxyAuth {
            username: "proxy-user".to_string(),
            password: "proxy-password".to_string(),
        });

        persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap();
        assert_eq!(
            store.resolve(&shared).unwrap().unwrap().expose(),
            "provider-secret"
        );
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let proxy: crate::ProxyAuth =
            serde_json::from_str(store.resolve(&canonical).unwrap().unwrap().expose()).unwrap();
        assert_eq!(proxy.username, "proxy-user");
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], canonical.as_str());
    }

    #[test]
    fn occupied_canonical_proxy_ref_fails_closed_without_any_write() {
        let _key = crate::encryption::set_test_encryption_key([0x97; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "openai-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        store
            .replace(
                canonical.clone(),
                "anthropic-secret",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared),
            ..crate::OpenAIConfig::default()
        });
        config.providers.anthropic = Some(crate::AnthropicConfig {
            credential_ref: Some(canonical),
            ..crate::AnthropicConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = Some(crate::ProxyAuth {
            username: "proxy-user".to_string(),
            password: "proxy-password".to_string(),
        });
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap_err();
        assert!(error
            .to_string()
            .contains("canonical reference is occupied"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
    }

    #[test]
    fn custom_proxy_only_ref_replace_and_clear_resume_without_touching_other_credentials() {
        let _key = crate::encryption::set_test_encryption_key([0x9b; 32]);
        for replacement in [
            Some(crate::ProxyAuth {
                username: "replacement-user".to_string(),
                password: "replacement-password".to_string(),
            }),
            None,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let custom = CredentialRef::parse("proxy.historical.custom").unwrap();
            let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
            let store = CredentialStore::open(dir.path());
            store
                .replace(
                    custom.clone(),
                    &serde_json::to_string(&crate::ProxyAuth {
                        username: "original-user".to_string(),
                        password: "original-password".to_string(),
                    })
                    .unwrap(),
                    crate::CredentialSource::User,
                    0,
                )
                .unwrap();
            store
                .replace(
                    unrelated.clone(),
                    "unrelated-provider-secret",
                    crate::CredentialSource::User,
                    store.revision().unwrap(),
                )
                .unwrap();
            let mut initial = crate::Config::default();
            initial.proxy_auth_credential_ref = Some(custom.clone());
            initial.save_to_dir(dir.path().to_path_buf()).unwrap();
            let mut candidate =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            candidate.proxy_auth = replacement.clone();
            let intents = BTreeSet::from(["__proxy_auth".to_string()]);

            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(MigrationFault::AfterManifest),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["proxy_auth_credential_ref"], custom.as_str());
            match replacement {
                Some(expected) => {
                    let resolved: crate::ProxyAuth =
                        serde_json::from_str(store.resolve(&custom).unwrap().unwrap().expose())
                            .unwrap();
                    assert_eq!(resolved.username, expected.username);
                    assert_eq!(resolved.password, expected.password);
                }
                None => assert!(store.resolve(&custom).unwrap().is_none()),
            }
            assert_eq!(
                store.resolve(&unrelated).unwrap().unwrap().expose(),
                "unrelated-provider-secret"
            );
        }
    }

    #[test]
    fn committed_proxy_auth_transaction_recovers_without_partial_publication() {
        let _key = crate::encryption::set_test_encryption_key([0x94; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "recover-user".to_string(),
            password: "recover-secret".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let before_recovery: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(before_recovery.get("proxy_auth_credential_ref").is_none());

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded
            .proxy_auth
            .as_ref()
            .expect("committed proxy auth recovered");
        assert_eq!(auth.username, "recover-user");
        assert_eq!(auth.password, "recover-secret");
        let root = std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE)).unwrap();
        for secret in ["recover-user", "recover-secret"] {
            assert!(!root.contains(secret));
            assert!(!credentials.contains(secret));
        }
    }

    #[test]
    fn committed_proxy_transaction_rebases_valid_external_root_edit() {
        let _key = crate::encryption::set_test_encryption_key([0x98; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut initial = crate::Config::default();
        initial.http_proxy = "http://before.example:8080".to_string();
        initial.save_to_dir(dir.path().to_path_buf()).unwrap();
        let mut candidate = initial.clone();
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "race-user".to_string(),
            password: "race-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["http_proxy"] = Value::String("http://external.example:9090".to_string());
        external["external_edit"] = serde_json::json!({"preserved": true});
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["http_proxy"], "http://external.example:9090");
        assert_eq!(root["external_edit"]["preserved"], true);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth_encrypted").is_none());
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(auth.username, "race-user");
        assert_eq!(auth.password, "race-password");
    }

    #[test]
    fn proxy_transaction_narrow_patch_preserves_root_edit_present_before_staging() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "narrow-user".to_string(),
            password: "narrow-password".to_string(),
        });
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["http_proxy"] = Value::String("http://external-before-stage:9090".to_string());
        external["external_before_stage"] = serde_json::json!({"preserved": true});
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        persist_proxy_auth_credential_transaction(dir.path(), &mut candidate).unwrap();

        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["http_proxy"], "http://external-before-stage:9090");
        assert_eq!(root["external_before_stage"]["preserved"], true);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
    }

    #[test]
    fn committed_proxy_transaction_rebases_again_after_manifested_rebase_loses_cas() {
        let _key = crate::encryption::set_test_encryption_key([0x99; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "repeat-user".to_string(),
            password: "repeat-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["external_rebase_generation"] = Value::Number(1_u64.into());
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        assert!(migrate_with_fault(
            dir.path(),
            MigrationFault::AfterExactProxyConfigRebaseManifestExternalWrite,
        )
        .is_err());
        let after_lost_cas: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(after_lost_cas["external_rebase_generation"], 2);
        assert!(after_lost_cas.get("proxy_auth_credential_ref").is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["external_rebase_generation"], 2);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(auth.username, "repeat-user");
        assert_eq!(auth.password, "repeat-password");
    }

    #[test]
    fn committed_proxy_transaction_rejects_new_provider_instance_consumer_before_credential_write()
    {
        let _key = crate::encryption::set_test_encryption_key([0x9a; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "must-not-install".to_string(),
            password: "must-not-install-secret".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn committed_proxy_transaction_rolls_back_installed_credential_for_new_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0xa3; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "partial-user".to_string(),
            password: "partial-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterCredentials),
        )
        .is_err());
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_some());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(
            root["provider_instances"]["racer"]["credential_ref"],
            "proxy.default.auth"
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_abort_rolls_back_config_installed_before_new_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0xa6; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "fully-installed-user".to_string(),
            password: "fully-installed-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterConfig),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(external["proxy_auth_credential_ref"], "proxy.default.auth");
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root.get("proxy_auth_credential_ref").is_none());
        assert_eq!(
            root["provider_instances"]["racer"]["credential_ref"],
            "proxy.default.auth"
        );
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_abort_preserves_a_later_same_ref_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0xa5; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "transaction-user".to_string(),
            password: "transaction-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterCredentials),
        )
        .is_err());

        let reference = credential_ref("proxy", "default", "auth").unwrap();
        let winner = serde_json::to_string(&crate::ProxyAuth {
            username: "winner-user".to_string(),
            password: "winner-password".to_string(),
        })
        .unwrap();
        let store = CredentialStore::open(dir.path());
        let revision = store.revision_unchecked().unwrap();
        store
            .replace_unchecked(
                reference.clone(),
                &winner,
                crate::CredentialSource::User,
                revision,
            )
            .unwrap();
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            store
                .resolve_unchecked(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            winner
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn committed_proxy_transaction_aborts_on_same_domain_root_change() {
        let _key = crate::encryption::set_test_encryption_key([0xa4; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "losing-user".to_string(),
            password: "losing-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["proxy_auth_credential_ref"] = Value::String("proxy.external.auth".to_string());
        external["same_domain_winner"] = Value::Bool(true);
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error
            .to_string()
            .contains("proxy authentication metadata changed"));
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.external.auth");
        assert_eq!(root["same_domain_winner"], true);
        let losing_ref = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&losing_ref)
            .unwrap()
            .is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn backup_only_proxy_auth_is_stored_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0x95; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "backup-user", "password": "backup-secret"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        let stored = CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .unwrap();
        let auth: crate::ProxyAuth = serde_json::from_str(stored.expose()).unwrap();
        assert_eq!(auth.password, "backup-secret");
        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("proxy_auth_encrypted"));
        assert!(!backup.contains("backup-secret"));
        assert!(backup.contains("proxy_auth_credential_ref"));
    }

    #[test]
    fn user_managed_proxy_auth_scrubs_stale_backup_and_keeps_provider_sidecar_ready() {
        let _key = crate::encryption::set_test_encryption_key([0x9f; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let authoritative = serde_json::to_string(&crate::ProxyAuth {
            username: "authoritative-user".to_string(),
            password: "authoritative-password".to_string(),
        })
        .unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical.clone(),
                &authoritative,
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": canonical.as_str(),
                "providers": {
                    "openai": {"model": "root-stale-model"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "api_key": "provider-sidecar-secret",
                    "model": "sidecar-authoritative-model"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let backup_path = dir.path().join("config.json.bak");
        std::fs::write(
            &backup_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": "invalid-stale-ciphertext-must-not-be-decrypted"
            }))
            .unwrap(),
        )
        .unwrap();

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded
            .proxy_auth
            .as_ref()
            .expect("user proxy auth hydrated");
        assert_eq!(auth.username, "authoritative-user");
        assert_eq!(auth.password, "authoritative-password");
        let openai = loaded.providers.openai.as_ref().expect("sidecar loaded");
        assert_eq!(openai.model.as_deref(), Some("sidecar-authoritative-model"));
        assert_eq!(openai.api_key, "provider-sidecar-secret");
        assert_eq!(
            store.resolve(&canonical).unwrap().unwrap().expose(),
            authoritative
        );
        assert_eq!(
            store.status(&canonical).unwrap().source,
            crate::CredentialSource::User
        );
        let backup: Value = serde_json::from_slice(&std::fs::read(&backup_path).unwrap()).unwrap();
        assert!(backup.get("proxy_auth_encrypted").is_none());
        assert_eq!(backup["proxy_auth_credential_ref"], canonical.as_str());
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
    }

    #[test]
    fn backup_only_proxy_auth_shared_with_provider_instance_fails_before_credential_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let shared = CredentialRef::parse("backup.shared.proxy").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        let backup_path = dir.path().join("config.json.bak");
        std::fs::write(
            &backup_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "credential_ref": shared.as_str(),
                        "api_key": same_secret
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let backup_before = std::fs::read(&backup_path).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(std::fs::read(&backup_path).unwrap(), backup_before);
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    fn install_fixture(dir: &Path) {
        let provider = include_bytes!("../tests/fixtures/config_migration/providers-legacy.json");
        let mut mcp: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        // Use real ciphertext for one legacy entry while keeping the fixture
        // stable and human-auditable.
        mcp["data"]["stdio unsafe/name"]["env_encrypted"]["PRIVATE TOKEN"] =
            Value::String(crate::encryption::encrypt("stdio-cipher-secret").unwrap());
        std::fs::write(dir.join(PROVIDERS_FILE), provider).unwrap();
        std::fs::write(dir.join(MCP_FILE), serde_json::to_vec_pretty(&mcp).unwrap()).unwrap();
    }

    fn assert_migrated(dir: &Path) {
        let providers = std::fs::read_to_string(dir.join(PROVIDERS_FILE)).unwrap();
        let mcp = std::fs::read_to_string(dir.join(MCP_FILE)).unwrap();
        let credentials = std::fs::read_to_string(dir.join(CREDENTIALS_FILE)).unwrap();
        for secret in [
            "sk-provider-plain",
            "Bearer mcp-header-secret",
            "stdio-plain-secret",
            "stdio-cipher-secret",
        ] {
            assert!(!providers.contains(secret));
            assert!(!mcp.contains(secret));
            assert!(!credentials.contains(secret));
        }
        assert!(!providers.contains("api_key_encrypted"));
        assert!(!mcp.contains("env_encrypted"));
        assert!(!mcp.contains("headers_encrypted"));
        assert!(providers.contains("credential_ref"));
        assert!(mcp.contains("env_credential_refs"));
        assert!(mcp.contains("header_credential_refs"));
        assert!(providers.contains("provider-unknown-kept"));
        assert!(mcp.contains("mcp-unknown-kept"));

        let store = CredentialStore::open(dir);
        assert_eq!(
            store
                .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-provider-plain"
        );
        assert_eq!(
            store
                .resolve(&credential_ref("mcp", "stdio unsafe/name", "env_PRIVATE TOKEN").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "stdio-cipher-secret"
        );
        assert_no_legacy_plaintext(dir);
        assert!(!std::fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && is_strict_transaction_dir_name(&entry.file_name().to_string_lossy())
            }));
    }

    fn assert_no_legacy_plaintext(dir: &Path) {
        fn visit(path: &Path, needles: &[&str]) {
            for entry in std::fs::read_dir(path).unwrap().filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    visit(&path, needles);
                } else if let Ok(content) = std::fs::read_to_string(&path) {
                    for needle in needles {
                        assert!(
                            !content.contains(needle),
                            "legacy plaintext remained in {}",
                            path.display()
                        );
                    }
                }
            }
        }
        visit(
            dir,
            &[
                "sk-provider-plain",
                "Bearer mcp-header-secret",
                "stdio-plain-secret",
                "stdio-cipher-secret",
            ],
        );
    }

    #[test]
    fn real_legacy_fixtures_migrate_without_secret_or_unknown_field_loss() {
        let _key = crate::encryption::set_test_encryption_key([0x61; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.migrated_credentials >= 3);
        assert_migrated(dir.path());
        assert!(!std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(BACKUP_PREFIX)));
    }

    #[cfg(unix)]
    #[test]
    fn pending_transaction_backups_are_encrypted_owner_only_and_cleaned_on_retry() {
        use std::os::unix::fs::PermissionsExt;

        let _key = crate::encryption::set_test_encryption_key([0x69; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());

        let backup = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(BACKUP_PREFIX)
            })
            .expect("pending transaction has a backup directory")
            .path();
        assert_eq!(
            std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for entry in std::fs::read_dir(&backup).unwrap().filter_map(Result::ok) {
            assert_eq!(
                entry.metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
            let content = std::fs::read_to_string(entry.path()).unwrap();
            if !content.is_empty() {
                assert!(content.contains("ciphertext"));
            }
            for plaintext in [
                "sk-provider-plain",
                "Bearer mcp-header-secret",
                "stdio-plain-secret",
            ] {
                assert!(!content.contains(plaintext));
            }
        }
        for source in [PROVIDERS_FILE, MCP_FILE] {
            assert_eq!(
                std::fs::metadata(dir.path().join(source))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_migrated(dir.path());
    }

    #[test]
    fn crash_before_manifest_is_discarded_and_retried_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0x62; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let mcp_before = std::fs::read(dir.path().join(MCP_FILE)).unwrap();
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterJournal).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(MCP_FILE)).unwrap(),
            mcp_before
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_migrated(dir.path());
    }

    #[test]
    fn crash_after_staging_cleans_only_strict_unreferenced_orphans_on_every_retry() {
        let _key = crate::encryption::set_test_encryption_key([0x6f; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let non_uuid_stage = dir.path().join(format!("{STAGE_PREFIX}not-a-uuid"));
        let non_uuid_backup = dir.path().join(format!("{BACKUP_PREFIX}not-a-uuid"));
        std::fs::create_dir(&non_uuid_stage).unwrap();
        std::fs::create_dir(&non_uuid_backup).unwrap();

        #[cfg(unix)]
        let (external, external_link) = {
            use std::os::unix::fs::symlink;
            let external = tempfile::tempdir().unwrap();
            std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
            let external_link = dir.path().join(format!("{STAGE_PREFIX}{}", Uuid::new_v4()));
            symlink(external.path(), &external_link).unwrap();
            (external, external_link)
        };

        let strict_dir_count = || {
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .filter(|entry| {
                    is_strict_transaction_dir_name(&entry.file_name().to_string_lossy())
                })
                .count()
        };
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterStaging).is_err());
        assert_eq!(strict_dir_count(), 2);
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterStaging).is_err());
        assert_eq!(strict_dir_count(), 2, "orphan pairs must not accumulate");

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(strict_dir_count(), 0);
        assert!(non_uuid_stage.exists());
        assert!(non_uuid_backup.exists());
        #[cfg(unix)]
        {
            assert!(std::fs::symlink_metadata(&external_link).is_ok());
            assert_eq!(
                std::fs::read(external.path().join("must-survive")).unwrap(),
                b"sentinel"
            );
        }
        assert_migrated(dir.path());
    }

    #[test]
    fn non_not_found_manifest_and_journal_read_errors_fail_closed_and_redacted() {
        let _key = crate::encryption::set_test_encryption_key([0x70; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"providers":{"openai":{"model":"root-lkg"}}}"#,
        )
        .unwrap();
        install_fixture(dir.path());
        std::fs::create_dir(dir.path().join(MANIFEST_FILE)).unwrap();

        for error in [
            ensure_provider_mcp_migration_ready(dir.path()).unwrap_err(),
            recover_committed(dir.path(), Some(MigrationFault::None)).unwrap_err(),
        ] {
            let rendered = error.to_string();
            assert!(rendered.contains("migration"));
            assert!(!rendered.contains(dir.path().to_string_lossy().as_ref()));
            assert!(!rendered.contains("sk-provider-plain"));
        }
        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.providers.openai.as_ref().unwrap().model.as_deref(),
            Some("root-lkg"),
            "provider target must not be consumed when manifest cannot be read"
        );

        let journal_dir = tempfile::tempdir().unwrap();
        install_fixture(journal_dir.path());
        std::fs::create_dir(journal_dir.path().join(JOURNAL_FILE)).unwrap();
        for error in [
            discard_uncommitted(journal_dir.path()).unwrap_err(),
            migrate_with_fault(journal_dir.path(), MigrationFault::None).unwrap_err(),
        ] {
            let rendered = error.to_string();
            assert!(rendered.contains("migration"));
            assert!(!rendered.contains(journal_dir.path().to_string_lossy().as_ref()));
            assert!(!rendered.contains("sk-provider-plain"));
        }
    }

    #[test]
    fn non_not_found_provider_and_mcp_read_errors_fail_closed() {
        let _key = crate::encryption::set_test_encryption_key([0x74; 32]);
        for target in [PROVIDERS_FILE, MCP_FILE] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir(dir.path().join(target)).unwrap();

            let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        }
    }

    #[test]
    fn migration_rejects_exhausted_section_and_credential_revisions() {
        let _key = crate::encryption::set_test_encryption_key([0x75; 32]);

        let section_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            section_dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": {"openai": {"api_key": "sk-overflow"}}
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(section_dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert!(!section_dir.path().join(MANIFEST_FILE).exists());
        assert!(
            std::fs::read_to_string(section_dir.path().join(PROVIDERS_FILE))
                .unwrap()
                .contains("sk-overflow")
        );

        let credential_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            credential_dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"sk-credential-overflow"}}"#,
        )
        .unwrap();
        std::fs::write(
            credential_dir.path().join(CREDENTIALS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": {"entries": {}}
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(credential_dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert!(!credential_dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn exhausted_credential_revision_allows_secret_free_cleanup_when_user_value_wins() {
        let _key = crate::encryption::set_test_encryption_key([0x78; 32]);
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                reference.clone(),
                "user-authoritative",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut credentials: Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        credentials["revision"] = Value::from(u64::MAX);
        std::fs::write(
            store.path(),
            serde_json::to_vec_pretty(&credentials).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"stale-legacy","model":"kept"}}"#,
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

        assert_eq!(store.revision().unwrap(), u64::MAX);
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "user-authoritative"
        );
        let providers = std::fs::read_to_string(dir.path().join(PROVIDERS_FILE)).unwrap();
        assert!(!providers.contains("stale-legacy"));
        assert!(providers.contains("credential_ref"));
    }

    #[cfg(unix)]
    #[test]
    fn committed_manifest_rejects_a_symlinked_stage_directory() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x76; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        let stage = dir.path().join(&manifest.stage_dir);
        std::fs::remove_dir_all(&stage).unwrap();
        let external = tempfile::tempdir().unwrap();
        std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
        symlink(external.path(), &stage).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("stage path is invalid"));
        assert_eq!(
            std::fs::read(external.path().join("must-survive")).unwrap(),
            b"sentinel"
        );
    }

    #[cfg(unix)]
    #[test]
    fn exact_transaction_rejects_a_symlinked_backup_directory() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x7d; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-backup-symlink");
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        let backup = dir
            .path()
            .join(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
        std::fs::remove_dir_all(&backup).unwrap();
        let external = tempfile::tempdir().unwrap();
        std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
        symlink(external.path(), &backup).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("backup path is invalid"));
        assert_eq!(
            std::fs::read(external.path().join("must-survive")).unwrap(),
            b"sentinel"
        );
    }

    #[test]
    fn crashes_at_every_post_manifest_durable_boundary_resume_before_reads() {
        for fault in [
            MigrationFault::AfterManifest,
            MigrationFault::AfterCredentials,
            MigrationFault::AfterProviders,
            MigrationFault::AfterMcp,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x63; 32]);
            let dir = tempfile::tempdir().unwrap();
            install_fixture(dir.path());
            assert!(migrate_with_fault(dir.path(), fault).is_err());
            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            assert_migrated(dir.path());
        }
    }

    #[test]
    fn retry_never_overwrites_a_user_replaced_credential() {
        let _key = crate::encryption::set_test_encryption_key([0x64; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace_unchecked(
                reference.clone(),
                "user-newer",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "user-newer"
        );
        assert_migrated_except_provider_value(dir.path());
    }

    #[test]
    fn unversioned_provider_and_mcp_rewrites_advance_migrated_secret_generation() {
        let _key = crate::encryption::set_test_encryption_key([0x6a; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"sk-provider-from-old-binary","model":"old-binary"}}"#,
        )
        .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&provider_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-provider-from-old-binary"
        );

        let mut old_mcp: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        old_mcp["data"]["stdio unsafe/name"]["env"]["PUBLIC TOKEN"] =
            Value::String("mcp-from-old-binary".to_string());
        old_mcp["data"]["stdio unsafe/name"]["env_encrypted"] = serde_json::json!({});
        std::fs::write(
            dir.path().join(MCP_FILE),
            serde_json::to_vec_pretty(&old_mcp).unwrap(),
        )
        .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let mcp_ref = credential_ref("mcp", "stdio unsafe/name", "env_PUBLIC TOKEN").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&mcp_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "mcp-from-old-binary"
        );
    }

    #[test]
    fn section_write_racing_a_committed_manifest_is_rebased_without_lost_update() {
        let _key = crate::encryption::set_test_encryption_key([0x66; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());

        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 99,
                "data": {
                    "openai": {
                        "api_key": "sk-racing-newer",
                        "model": "newer-model",
                        "new_unknown": "must-survive-rebase"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                .unwrap();
        assert_eq!(providers["revision"], 100);
        assert_eq!(providers["data"]["openai"]["model"], "newer-model");
        assert_eq!(
            providers["data"]["openai"]["new_unknown"],
            "must-survive-rebase"
        );
        assert!(providers["data"]["openai"].get("api_key").is_none());
        let store = CredentialStore::open(dir.path());
        assert_eq!(
            store
                .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-racing-newer"
        );
    }

    #[test]
    fn committed_provider_instance_migration_rejects_a_corrupt_root_rebase() {
        let _key = crate::encryption::set_test_encryption_key([0x86; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "work": {"provider_type": "openai", "api_key": "sk-staged"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        std::fs::write(dir.path().join(CONFIG_FILE), b"{concurrent-broken").unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            b"{concurrent-broken"
        );
    }

    #[test]
    fn invalid_root_validation_is_ignored_without_partial_secret_extraction() {
        let _key = crate::encryption::set_test_encryption_key([0x87; 32]);
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path());
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                provider_ref.clone(),
                "sk-sidecar-survives-invalid-root",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "credential_ref": provider_ref.as_str(),
                    "model": "gpt-sidecar"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let invalid_root = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "first": {
                    "provider_type": "openai",
                    "api_key": "must-not-be-partially-extracted"
                },
                "invalid": {
                    "provider_type": "anthropic",
                    "api_key": {"not": "a string"}
                }
            }
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &invalid_root).unwrap();

        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.providers.openai.as_ref().unwrap().api_key,
            "sk-sidecar-survives-invalid-root"
        );
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            invalid_root
        );
        assert!(store
            .resolve(&credential_ref("provider_instance", "first", "api_key").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn rebase_crashes_keep_manifest_stage_pair_valid_and_never_replay_older_secret() {
        for fault in [
            MigrationFault::AfterRebaseCredentialCommit,
            MigrationFault::AfterRebaseStageWrite,
            MigrationFault::AfterRebaseManifest,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x67; 32]);
            let dir = tempfile::tempdir().unwrap();
            install_fixture(dir.path());
            assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
            std::fs::write(
                dir.path().join(PROVIDERS_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "revision": 99,
                    "data": {"openai": {
                        "api_key": "sk-rebase-newer",
                        "model": "rebase-model"
                    }}
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(migrate_with_fault(dir.path(), fault).is_err());
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

            let store = CredentialStore::open(dir.path());
            assert_eq!(
                store
                    .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                "sk-rebase-newer",
                "old staged credential replayed after {fault:?}"
            );
            let providers: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                    .unwrap();
            assert_eq!(providers["revision"], 100);
            assert_eq!(providers["data"]["openai"]["model"], "rebase-model");
        }
    }

    #[test]
    fn manifest_rejects_stage_path_traversal_and_duplicate_targets() {
        let transaction_id = Uuid::new_v4().to_string();
        let file = StagedFile {
            name: CREDENTIALS_FILE.to_string(),
            staged_name: CREDENTIALS_FILE.to_string(),
            sha256: "0".repeat(64),
            original_sha256: None,
            migration_generation: None,
            sensitive: true,
            install_mode: InstallMode::Migration,
            expected_revision: None,
            transaction_base_sha256: None,
            touched_credential_refs: Vec::new(),
            required_credential_refs: Vec::new(),
            touched_env_names: Vec::new(),
        };
        let traversal = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}/../../outside"),
            state: MigrationState::Pending,
            exact_scope: None,
            files: vec![file.clone()],
        };
        assert!(validate_manifest(&traversal).is_err());

        let duplicate = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}"),
            state: MigrationState::Pending,
            exact_scope: None,
            files: vec![file.clone(), file],
        };
        assert!(validate_manifest(&duplicate).is_err());
    }

    fn provider_transaction_candidate(secret: &str) -> (crate::Config, BTreeSet<String>) {
        let mut config = crate::Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(crate::OpenAIConfig {
            api_key: secret.to_string(),
            model: Some("transaction-model".to_string()),
            ..Default::default()
        });
        (config, BTreeSet::from(["openai".to_string()]))
    }

    fn provider_instance_transaction_candidate(
        instance_id: &str,
        secret: &str,
    ) -> (crate::Config, BTreeSet<String>) {
        let mut config = crate::Config::default();
        let instance: crate::ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "label": "Work",
            "api_key": secret,
            "model": "gpt-test",
            "unknown_instance_field": "must-survive"
        }))
        .unwrap();
        config
            .provider_instances
            .insert(instance_id.to_string(), instance);
        (config, BTreeSet::from([instance_id.to_string()]))
    }

    #[test]
    fn legacy_provider_instances_migrate_recoverably_and_scrub_backups() {
        let _key = crate::encryption::set_test_encryption_key([0x81; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("sk-cipher-instance").unwrap();
        let root = serde_json::json!({
            "provider_instances": {
                "work unsafe/id": {
                    "provider_type": "openai",
                    "api_key": "sk-plain-instance",
                    "api_key_encrypted": crate::encryption::encrypt("stale-shadow").unwrap(),
                    "unknown_instance_field": "must-survive"
                },
                "personal": {
                    "provider_type": "anthropic",
                    "api_key_encrypted": ciphertext
                }
            },
            "unknown_root_field": "must-survive"
        });
        let bytes = serde_json::to_vec_pretty(&root).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        std::fs::write(dir.path().join("config.json.bak"), &bytes).unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterConfig).is_err());
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);

        for path in [
            dir.path().join(CONFIG_FILE),
            dir.path().join("config.json.bak"),
        ] {
            let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(value["unknown_root_field"], "must-survive");
            assert_eq!(
                value["provider_instances"]["work unsafe/id"]["unknown_instance_field"],
                "must-survive"
            );
            for id in ["work unsafe/id", "personal"] {
                let instance = &value["provider_instances"][id];
                assert!(instance.get("api_key").is_none());
                assert!(instance.get("api_key_encrypted").is_none());
                assert!(instance.get("credential_ref").is_some());
            }
        }
        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.provider_instances["work unsafe/id"].api_key,
            "sk-plain-instance"
        );
        assert_eq!(
            loaded.provider_instances["personal"].api_key,
            "sk-cipher-instance"
        );
        let credentials = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE)).unwrap();
        assert!(!credentials.contains("sk-plain-instance"));
        assert!(!credentials.contains("sk-cipher-instance"));
    }

    #[test]
    fn migration_rejects_shared_ref_with_different_legacy_secrets_without_writes() {
        let _key = crate::encryption::set_test_encryption_key([0x89; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = "provider_instance.shared.api_key";
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "first": {
                    "provider_type": "openai",
                    "api_key": "sk-first",
                    "credential_ref": shared_ref
                },
                "second": {
                    "provider_type": "anthropic",
                    "api_key": "sk-second",
                    "credential_ref": shared_ref
                }
            }
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn migration_deduplicates_shared_ref_with_the_same_legacy_secret() {
        let _key = crate::encryption::set_test_encryption_key([0x8a; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = CredentialRef::parse("provider_instance.shared.api_key").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "first": {
                        "provider_type": "openai",
                        "api_key": "sk-same",
                        "credential_ref": shared_ref.as_str()
                    },
                    "second": {
                        "provider_type": "anthropic",
                        "api_key": "sk-same",
                        "credential_ref": shared_ref.as_str()
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-same"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        for id in ["first", "second"] {
            assert!(root["provider_instances"][id].get("api_key").is_none());
            assert_eq!(
                root["provider_instances"][id]["credential_ref"],
                shared_ref.as_str()
            );
        }
    }

    #[test]
    fn backup_only_provider_instance_is_stored_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0x85; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let store = CredentialStore::open(dir.path());
        let existing_ref = credential_ref("provider_instance", "shared", "api_key").unwrap();
        store
            .replace(
                existing_ref.clone(),
                "sk-current-winner",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                provider_ref.clone(),
                "sk-built-in-survives-corrupt-root",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        let mcp_ref = credential_ref("mcp", "backup-stdio", "env_TOKEN").unwrap();
        store
            .replace(
                mcp_ref.clone(),
                "mcp-survives-corrupt-root",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "credential_ref": provider_ref.as_str(),
                    "model": "gpt-sidecar"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "personal": {
                        "provider_type": "anthropic",
                        "api_key": "sk-backup-only",
                        "model": "backup-model"
                    },
                    "shared": {
                        "provider_type": "openai",
                        "api_key": "sk-stale-backup"
                    }
                },
                "mcpServers": {
                    "backup-stdio": {
                        "command": "unused-disabled-command",
                        "enabled": false,
                        "env_credential_refs": {"TOKEN": mcp_ref.as_str()},
                        "request_timeout_ms": 100,
                        "healthcheck_interval_ms": 100
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let reference = credential_ref("provider_instance", "personal", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-backup-only"
        );
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&existing_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-current-winner"
        );
        let backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json.bak")).unwrap())
                .unwrap();
        assert!(backup["provider_instances"]["personal"]
            .get("api_key")
            .is_none());
        assert_eq!(
            backup["provider_instances"]["personal"]["credential_ref"],
            reference.as_str()
        );
        assert!(backup["provider_instances"]["shared"]
            .get("api_key")
            .is_none());
        assert_eq!(
            backup["provider_instances"]["shared"]["credential_ref"],
            existing_ref.as_str()
        );

        std::fs::write(dir.path().join(CONFIG_FILE), b"{broken").unwrap();
        let recovered = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            recovered.provider_instances["personal"].api_key,
            "sk-backup-only"
        );
        assert_eq!(
            recovered.provider_instances["personal"].model.as_deref(),
            Some("backup-model")
        );
        assert_eq!(
            recovered.providers.openai.as_ref().unwrap().api_key,
            "sk-built-in-survives-corrupt-root"
        );
        let stdio = match &recovered.mcp.servers[0].transport {
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => stdio,
            other => panic!("expected stdio transport, got {other:?}"),
        };
        assert_eq!(
            stdio.env.get("TOKEN").map(String::as_str),
            Some("mcp-survives-corrupt-root")
        );
    }

    #[test]
    fn provider_instance_transaction_keeps_only_refs_through_generic_saves() {
        let _key = crate::encryption::set_test_encryption_key([0x82; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, instance_intents) =
            provider_instance_transaction_candidate("work", "sk-instance-transaction");
        persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &instance_intents,
            None,
            &BTreeSet::new(),
            None,
            false,
            &BTreeSet::new(),
            false,
            None,
            None,
        )
        .unwrap();

        let reference = credential_ref("provider_instance", "work", "api_key").unwrap();
        assert_eq!(
            candidate.provider_instances["work"].credential_ref.as_ref(),
            Some(&reference)
        );
        candidate.provider_instances.get_mut("work").unwrap().label = Some("Renamed".to_string());
        candidate.save_to_dir(dir.path().to_path_buf()).unwrap();

        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = dir.path().join(format!("config.json{suffix}"));
            let Ok(bytes) = std::fs::read(path) else {
                continue;
            };
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let Some(instance) = value
                .get("provider_instances")
                .and_then(|instances| instances.get("work"))
            else {
                continue;
            };
            assert!(instance.get("api_key").is_none());
            assert!(instance.get("api_key_encrypted").is_none());
            assert_eq!(instance["credential_ref"], reference.as_str());
        }
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-instance-transaction"
        );
    }

    #[test]
    fn provider_instance_delete_clears_custom_ref_and_survives_concurrent_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x83; 32]);
        let dir = tempfile::tempdir().unwrap();
        let custom_ref = CredentialRef::parse("provider_instance.custom.secret").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                custom_ref.clone(),
                "sk-delete-me",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut current = crate::Config::default();
        let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
            serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
        )
        .unwrap();
        instance.credential_ref = Some(custom_ref.clone());
        current
            .provider_instances
            .insert("work".to_string(), instance);
        current.save_to_dir(dir.path().to_path_buf()).unwrap();

        let mut candidate = current.clone();
        candidate.provider_instances.remove("work");
        persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::from(["work".to_string()]),
            None,
            &BTreeSet::new(),
            None,
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterExactCommitCredentialClearRace),
        )
        .unwrap();

        assert!(CredentialStore::open(dir.path())
            .resolve(&custom_ref)
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["provider_instances"].get("work").is_none());
    }

    #[test]
    fn provider_instance_delete_preserves_a_ref_used_by_other_instances_and_mcp() {
        let _key = crate::encryption::set_test_encryption_key([0x88; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = CredentialRef::parse("shared.provider.and.mcp.secret").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared_ref.clone(),
                "sk-shared-survivor",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut current = crate::Config::default();
        for id in ["delete-me", "keep-me"] {
            let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
                serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
            )
            .unwrap();
            instance.credential_ref = Some(shared_ref.clone());
            current.provider_instances.insert(id.to_string(), instance);
        }
        current
            .mcp
            .servers
            .push(bamboo_domain::mcp_config::McpServerConfig {
                id: "shared-stdio".to_string(),
                name: None,
                enabled: false,
                transport: bamboo_domain::mcp_config::TransportConfig::Stdio(
                    bamboo_domain::mcp_config::StdioConfig {
                        command: "unused".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        env: Default::default(),
                        env_encrypted: Default::default(),
                        env_credential_refs: std::collections::HashMap::from([(
                            "TOKEN".to_string(),
                            shared_ref.as_str().to_string(),
                        )]),
                        startup_timeout_ms: 100,
                    },
                ),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: Default::default(),
                allowed_tools: Vec::new(),
                denied_tools: Vec::new(),
            });
        current.save_to_dir(dir.path().to_path_buf()).unwrap();

        let mut candidate = current.clone();
        candidate.provider_instances.remove("delete-me");
        persist_provider_instance_credential_transaction(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::from(["delete-me".to_string()]),
        )
        .unwrap();

        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-shared-survivor"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["provider_instances"].get("delete-me").is_none());
        assert_eq!(
            root["provider_instances"]["keep-me"]["credential_ref"],
            shared_ref.as_str()
        );
        assert_eq!(
            root["mcpServers"]["shared-stdio"]["env_credential_refs"]["TOKEN"],
            shared_ref.as_str()
        );
    }

    #[test]
    fn dangling_provider_instance_ref_fails_closed_without_rewriting_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x84; 32]);
        let dir = tempfile::tempdir().unwrap();
        let reference = credential_ref("provider_instance", "work", "api_key").unwrap();
        let mut config = crate::Config::default();
        let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
            serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
        )
        .unwrap();
        instance.credential_ref = Some(reference.clone());
        config
            .provider_instances
            .insert("work".to_string(), instance);
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        let before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();

        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert!(loaded.provider_instances["work"].api_key.is_empty());
        assert_eq!(
            loaded.provider_instances["work"].credential_ref.as_ref(),
            Some(&reference)
        );
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), before);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
    }

    #[test]
    fn provider_transaction_precommit_failure_leaves_all_originals_unchanged() {
        let _key = crate::encryption::set_test_encryption_key([0x71; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let original_config = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let original_providers = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let original_credentials = std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok();
        let (mut candidate, intents) = provider_transaction_candidate("sk-precommit");

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterStaging),
        )
        .is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original_config
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            original_providers
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok(),
            original_credentials
        );
    }

    #[test]
    fn provider_transaction_resumes_after_credential_or_metadata_boundary() {
        for fault in [
            MigrationFault::AfterCredentials,
            MigrationFault::AfterProviders,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x72; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::Config::default()
                .save_to_dir(dir.path().to_path_buf())
                .unwrap();
            let (mut candidate, intents) = provider_transaction_candidate("sk-resume");
            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(fault),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
            assert_eq!(loaded.provider, "openai");
            assert_eq!(
                loaded.providers.openai.as_ref().unwrap().model.as_deref(),
                Some("transaction-model")
            );
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                "sk-resume"
            );
        }
    }

    #[test]
    fn provider_transaction_credential_cas_loser_never_commits_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x73; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let original_config = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let original_providers = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-loser");
        let error = persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::BeforeExactCommitCredentialRace),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict {
                expected: 0,
                actual: 1
            }
        ));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original_config
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            original_providers
        );
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-winner"
        );
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
    }

    #[test]
    fn provider_transaction_keeps_a_post_commit_credential_winner_and_finishes_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x79; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-transaction-loser");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitCredentialRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-post-commit-winner"
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        let provider = loaded.providers.openai.as_ref().unwrap();
        assert_eq!(provider.model.as_deref(), Some("transaction-model"));
        assert_eq!(provider.credential_ref.as_ref(), Some(&reference));
    }

    #[test]
    fn provider_transaction_finishes_when_a_post_commit_clear_wins_the_same_ref() {
        let _key = crate::encryption::set_test_encryption_key([0x7d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                reference.clone(),
                "previous-openai-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("transaction-loses-to-clear");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitCredentialClearRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        assert!(store.resolve(&reference).unwrap().is_none());
        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                .unwrap();
        let provider_data = providers.get("data").unwrap_or(&providers);
        assert_eq!(
            provider_data["openai"]["model"],
            Value::String("transaction-model".to_string())
        );
        assert_eq!(
            provider_data["openai"]["credential_ref"],
            Value::String(reference.as_str().to_string())
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        assert!(
            loaded.providers.openai.is_none(),
            "missing ref must fail closed"
        );
    }

    #[test]
    fn provider_transaction_merges_an_unrelated_post_commit_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x7a; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-transaction-openai");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitUnrelatedCredentialRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let store = CredentialStore::open(dir.path());
        let openai = credential_ref("provider", "openai", "api_key").unwrap();
        let anthropic = credential_ref("provider", "anthropic", "api_key").unwrap();
        assert_eq!(
            store.resolve(&openai).unwrap().unwrap().expose(),
            "sk-transaction-openai"
        );
        assert_eq!(
            store.resolve(&anthropic).unwrap().unwrap().expose(),
            "concurrent-unrelated-winner"
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded
                .providers
                .openai
                .as_ref()
                .and_then(|provider| provider.credential_ref.as_ref()),
            Some(&openai)
        );
    }

    #[test]
    fn provider_transaction_rebase_resumes_across_each_durable_boundary() {
        for fault in [
            MigrationFault::AfterExactCredentialRebaseStage,
            MigrationFault::AfterExactCredentialRebaseManifest,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x7b; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::Config::default()
                .save_to_dir(dir.path().to_path_buf())
                .unwrap();
            let (mut candidate, intents) = provider_transaction_candidate("sk-rebase-resume");

            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(fault),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let store = CredentialStore::open(dir.path());
            let openai = credential_ref("provider", "openai", "api_key").unwrap();
            let anthropic = credential_ref("provider", "anthropic", "api_key").unwrap();
            assert_eq!(
                store.resolve(&openai).unwrap().unwrap().expose(),
                "sk-rebase-resume"
            );
            assert_eq!(
                store.resolve(&anthropic).unwrap().unwrap().expose(),
                "concurrent-unrelated-winner"
            );
        }
    }

    #[test]
    fn provider_transaction_rejects_a_malformed_higher_revision_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x7c; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-must-not-publish");
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());

        std::fs::write(
            dir.path().join(CREDENTIALS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 1,
                "data": {
                    "entries": {
                        "provider.openai.api_key": {
                            "ciphertext": "",
                            "source": "user",
                            "updated_at": "2026-07-19T00:00:00Z",
                            "key_version": 1
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
    }

    #[test]
    fn provider_transaction_rejects_exhausted_provider_revision_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0x77; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let provider_path = dir.path().join(PROVIDERS_FILE);
        let provider_data: Value =
            serde_json::from_slice(&std::fs::read(&provider_path).unwrap()).unwrap();
        std::fs::write(
            &provider_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": provider_data
            }))
            .unwrap(),
        )
        .unwrap();
        let providers_before = std::fs::read(&provider_path).unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-overflow");

        let error = persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::None),
        )
        .unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert_eq!(std::fs::read(&provider_path).unwrap(), providers_before);
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    fn assert_migrated_except_provider_value(dir: &Path) {
        let providers = std::fs::read_to_string(dir.join(PROVIDERS_FILE)).unwrap();
        let mcp = std::fs::read_to_string(dir.join(MCP_FILE)).unwrap();
        assert!(!providers.contains("sk-provider-plain"));
        assert!(!mcp.contains("mcp-header-secret"));
    }

    #[test]
    fn migration_errors_are_redacted() {
        let _key = crate::encryption::set_test_encryption_key([0x65; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key_encrypted":"definitely-secret-bad-cipher"}}"#,
        )
        .unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        let rendered = error.to_string();
        assert!(!rendered.contains("definitely-secret-bad-cipher"));
        assert!(!rendered.contains(dir.path().to_string_lossy().as_ref()));
    }

    #[test]
    fn legacy_secret_env_migrates_to_ref_and_scrubs_backups_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0x91; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("legacy-env-secret").unwrap();
        let legacy = serde_json::json!({
            "future_root": {"kept": true},
            "env_vars": [{
                "name": "PRIVATE_TOKEN",
                "secret": true,
                "value_encrypted": ciphertext,
                "future_entry": "kept"
            }]
        });
        for name in [CONFIG_FILE, "config.json.bak"] {
            std::fs::write(
                dir.path().join(name),
                serde_json::to_vec_pretty(&legacy).unwrap(),
            )
            .unwrap();
        }

        let first = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(first.migrated_credentials, 1);
        let reference = credential_ref("env", "PRIVATE_TOKEN", "value").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "legacy-env-secret"
        );
        for name in [CONFIG_FILE, "config.json.bak"] {
            let document: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(name)).unwrap()).unwrap();
            let entry = &document["env_vars"][0];
            assert_eq!(entry["credential_ref"], reference.as_str());
            assert_eq!(entry["configured"], true);
            assert!(entry.get("value").is_none());
            assert!(entry.get("value_encrypted").is_none());
            assert_eq!(entry["future_entry"], "kept");
            assert_eq!(document["future_root"]["kept"], true);
        }
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        let second = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(second.migrated_credentials, 0);
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );
    }

    #[test]
    fn legacy_env_plaintext_and_ciphertext_must_agree_before_any_write() {
        let _key = crate::encryption::set_test_encryption_key([0x94; 32]);
        let dir = tempfile::tempdir().unwrap();
        let conflicting = serde_json::json!({
            "env_vars": [{
                "name": "TOKEN", "secret": true, "value": "plain-winner",
                "value_encrypted": crate::encryption::encrypt("different").unwrap()
            }]
        });
        let bytes = serde_json::to_vec_pretty(&conflicting).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), bytes);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());

        let matching = serde_json::json!({
            "env_vars": [{
                "name": "TOKEN", "secret": true, "value": "same",
                "value_encrypted": crate::encryption::encrypt("same").unwrap()
            }]
        });
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&matching).unwrap(),
        )
        .unwrap();
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );
    }

    #[test]
    fn non_secret_env_legacy_ciphertext_is_scrubbed_from_root_and_backup() {
        let _key = crate::encryption::set_test_encryption_key([0x95; 32]);
        let dir = tempfile::tempdir().unwrap();
        let legacy = serde_json::json!({
            "env_vars": [{
                "name": "PUBLIC", "secret": false, "value": "visible",
                "value_encrypted": "must-not-remain"
            }]
        });
        for name in [CONFIG_FILE, "config.json.bak"] {
            std::fs::write(
                dir.path().join(name),
                serde_json::to_vec_pretty(&legacy).unwrap(),
            )
            .unwrap();
        }
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        for name in [CONFIG_FILE, "config.json.bak"] {
            let bytes = std::fs::read_to_string(dir.path().join(name)).unwrap();
            assert!(!bytes.contains("value_encrypted"));
            assert!(!bytes.contains("must-not-remain"));
            assert!(bytes.contains("visible"));
        }
    }

    #[test]
    fn env_exact_transaction_enforces_cas_and_recovers_once_after_manifest() {
        let _key = crate::encryption::set_test_encryption_key([0x92; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "TOKEN".to_string(),
            value: "winner".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let intents = BTreeSet::from(["TOKEN".to_string()]);
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &intents,
            Some(0),
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterManifest),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        assert!(dir.path().join(MANIFEST_FILE).exists());

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let reference = credential_ref("env", "TOKEN", "value").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "winner"
        );
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );

        let stale = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &intents,
            0,
        )
        .unwrap_err();
        assert!(matches!(stale, ConfigStoreError::Conflict { actual, .. } if actual == revision));
    }

    #[test]
    fn env_domain_revision_advances_for_public_metadata_order_and_delete_but_not_noop() {
        let _key = crate::encryption::set_test_encryption_key([0x9a; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "FIRST".to_string(),
            value: "one".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let first_intent = BTreeSet::from(["FIRST".to_string()]);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                0,
            )
            .unwrap(),
            1
        );

        candidate.env_vars[0].description = Some("metadata".to_string());
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                1,
            )
            .unwrap(),
            2
        );
        let stale = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &first_intent,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            stale,
            ConfigStoreError::Conflict { actual: 2, .. }
        ));
        let bytes_before_noop = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                2,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            bytes_before_noop
        );

        candidate.env_vars.push(crate::EnvVarEntry {
            name: "SECOND".to_string(),
            value: "two".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let both = BTreeSet::from(["FIRST".to_string(), "SECOND".to_string()]);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &both,
                2,
            )
            .unwrap(),
            3
        );
        candidate.env_vars.swap(0, 1);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &both,
                3,
            )
            .unwrap(),
            4
        );
        candidate.env_vars.retain(|entry| entry.name != "FIRST");
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                4,
            )
            .unwrap(),
            5
        );
        assert_eq!(CredentialStore::open(dir.path()).revision().unwrap(), 5);
    }

    fn crash_env_transaction_after_credentials(
        dir: &Path,
        name: &str,
        value: &str,
    ) -> CredentialRef {
        std::fs::write(dir.join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: name.to_string(),
            value: value.to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir,
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::from([name.to_string()]),
            Some(0),
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterCredentials),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        credential_ref("env", name, "value").unwrap()
    }

    #[test]
    fn committed_env_recovery_merges_unrelated_name_and_same_name_future_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x97; 32]);
        for same_name_future_metadata in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let reference = crash_env_transaction_after_credentials(dir.path(), "TOKEN", "winner");
            let external = if same_name_future_metadata {
                serde_json::json!({
                    "external_root": true,
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "future_metadata": {"kept": true}
                    }]
                })
            } else {
                serde_json::json!({
                    "external_root": true,
                    "env_vars": [{"name": "OTHER", "value": "external", "secret": false}]
                })
            };
            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&external).unwrap(),
            )
            .unwrap();
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["external_root"], true);
            let entries = root["env_vars"].as_array().unwrap();
            assert!(entries.iter().any(|entry| entry["name"] == "TOKEN"));
            if same_name_future_metadata {
                assert_eq!(entries[0]["future_metadata"]["kept"], true);
            } else {
                assert!(entries.iter().any(|entry| entry["name"] == "OTHER"));
            }
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                "winner"
            );
            let revision = CredentialStore::open(dir.path()).revision().unwrap();
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert_eq!(
                CredentialStore::open(dir.path()).revision().unwrap(),
                revision
            );
        }
    }

    #[test]
    fn same_name_custom_ref_winner_compensates_touched_credential_without_dangling() {
        let _key = crate::encryption::set_test_encryption_key([0x98; 32]);
        let dir = tempfile::tempdir().unwrap();
        let touched = crash_env_transaction_after_credentials(dir.path(), "TOKEN", "staged");
        let custom = credential_ref("env_external", "TOKEN", "value").unwrap();
        CredentialStore::open(dir.path())
            .replace_unchecked(
                custom.clone(),
                "external-winner",
                crate::CredentialSource::User,
                1,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN", "secret": true,
                    "credential_ref": custom.as_str(), "configured": true,
                    "description": "external metadata"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("env metadata changed"));
        assert!(!dir.path().join(MANIFEST_FILE).exists());
        assert!(CredentialStore::open(dir.path())
            .resolve(&touched)
            .unwrap()
            .is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&custom)
                .unwrap()
                .unwrap()
                .expose(),
            "external-winner"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["env_vars"][0]["credential_ref"], custom.as_str());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(root["env_vars"][0]["configured"], true);
    }

    #[test]
    fn env_clear_or_delete_recovery_restores_credential_for_same_name_external_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x99; 32]);
        for delete in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let reference = credential_ref("env", "TOKEN", "value").unwrap();
            CredentialStore::open(dir.path())
                .replace(
                    reference.clone(),
                    "original-secret",
                    crate::CredentialSource::User,
                    0,
                )
                .unwrap();
            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "description": "original"
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            let mut candidate =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            if delete {
                candidate.env_vars.clear();
            } else {
                candidate.env_vars[0].value.clear();
                candidate.env_vars[0].configured = false;
                candidate.env_vars[0].description = Some("transaction-clear".to_string());
            }
            let error = persist_provider_credential_transaction_with_instances_inner(
                dir.path(),
                &mut candidate,
                &BTreeSet::new(),
                &BTreeSet::new(),
                None,
                &BTreeSet::from(["TOKEN".to_string()]),
                Some(1),
                false,
                &BTreeSet::new(),
                false,
                None,
                Some(MigrationFault::AfterCredentials),
            )
            .unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(!std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE))
                .unwrap()
                .contains(reference.as_str()));

            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "description": if delete { "external-readd" } else { "external-clear-winner" }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();

            let recovery = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
            assert!(recovery.to_string().contains("env metadata changed"));
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                "original-secret"
            );
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["env_vars"][0]["configured"], true);
            assert_eq!(
                root["env_vars"][0]["description"],
                if delete {
                    "external-readd"
                } else {
                    "external-clear-winner"
                }
            );
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert!(CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn env_clear_rejects_a_reference_shared_by_another_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0x93; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("shared", "owner", "secret").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN", "secret": true,
                    "credential_ref": shared.as_str(), "configured": true
                }],
                "provider_instances": {"other": {
                    "provider_type": "openai", "model": "gpt-test",
                    "credential_ref": shared.as_str()
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared.clone(),
                "shared-value",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.env_vars.clear();
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        let error = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["TOKEN".to_string()]),
            revision,
        )
        .unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared)
                .unwrap()
                .unwrap()
                .expose(),
            "shared-value"
        );
    }

    #[test]
    fn ownerless_canonical_env_credential_cannot_be_cleared_by_non_secret_create() {
        let _key = crate::encryption::set_test_encryption_key([0x96; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let canonical = credential_ref("env", "TOKEN", "value").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                canonical.clone(),
                "generic-owner",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "TOKEN".to_string(),
            value: "public".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let error = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["TOKEN".to_string()]),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("already in use"));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&canonical)
                .unwrap()
                .unwrap()
                .expose(),
            "generic-owner"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                .unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn legacy_notification_secrets_migrate_to_refs_and_scrub_root_backups() {
        let _key = crate::encryption::set_test_encryption_key([0xa1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let bark_cipher = crate::encryption::encrypt("bark-legacy-secret").unwrap();
        let root = serde_json::json!({
            "notifications": {
                "desktop": {"enabled": true},
                "ntfy": {
                    "enabled": true,
                    "topic": "alerts",
                    "token": "ntfy-legacy-secret",
                    "unknown_channel_field": {"kept": true}
                },
                "bark": {
                    "enabled": true,
                    "device_key_encrypted": bark_cipher
                }
            },
            "unknown_root": {"kept": true}
        });
        let bytes = serde_json::to_vec_pretty(&root).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        std::fs::write(dir.path().join("config.json.bak"), &bytes).unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let migrated = std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap();
        assert!(!migrated.contains("ntfy-legacy-secret"));
        assert!(!migrated.contains("bark-legacy-secret"));
        assert!(!migrated.contains("token_encrypted"));
        assert!(!migrated.contains("device_key_encrypted"));
        let root: Value = serde_json::from_str(&migrated).unwrap();
        assert_eq!(
            root["notifications"]["ntfy"]["credential_ref"],
            "notification.ntfy.token"
        );
        assert_eq!(
            root["notifications"]["bark"]["credential_ref"],
            "notification.bark.device_key"
        );
        assert_eq!(
            root["notifications"]["ntfy"]["unknown_channel_field"]["kept"],
            true
        );
        assert_eq!(root["unknown_root"]["kept"], true);
        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("ntfy-legacy-secret"));
        assert!(!backup.contains("token_encrypted"));

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.notifications.ntfy.token.as_deref(),
            Some("ntfy-legacy-secret")
        );
        assert_eq!(
            loaded.notifications.bark.device_key.as_deref(),
            Some("bark-legacy-secret")
        );
        migrate_provider_mcp_credentials(dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap(),
            migrated
        );
    }

    #[test]
    fn backup_only_notification_secret_is_migrated_and_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0xa4; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), br#"{"unknown_root":true}"#).unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            br#"{"notifications":{"ntfy":{"token":"backup-only-secret"}}}"#,
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();

        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("backup-only-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(
            backup["notifications"]["ntfy"]["credential_ref"],
            "notification.ntfy.token"
        );
        assert_eq!(backup["notifications"]["ntfy"]["configured"], true);
        let reference = credential_ref("notification", "ntfy", "token").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "backup-only-secret"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["unknown_root"], true);
        assert!(root.get("notifications").is_none());
    }

    #[test]
    fn notification_exact_transaction_supports_cas_metadata_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "unknown_root":{"kept":true},
                "notifications":{
                    "future_channel":{"kept":true},
                    "ntfy":{"future_metadata":{"kept":true}}
                }
            }"#,
        )
        .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.enabled = true;
        candidate.notifications.ntfy.topic = "alerts".to_string();
        candidate.notifications.ntfy.token = Some("new-ntfy-secret".to_string());
        candidate.notifications.ntfy.configured = true;
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["ntfy".to_string()]),
            0,
        )
        .unwrap();
        assert_eq!(revision, 1);

        let mut metadata =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        metadata.notifications.ntfy.topic = "renamed".to_string();
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut metadata,
            &BTreeSet::new(),
            1,
        )
        .unwrap();
        assert_eq!(revision, 2);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("notification", "ntfy", "token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "new-ntfy-secret"
        );

        let mut clear =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        clear.notifications.ntfy.token = None;
        clear.notifications.ntfy.configured = false;
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &BTreeSet::from(["ntfy".to_string()]),
            2,
        )
        .unwrap();
        assert_eq!(revision, 3);
        assert!(CredentialStore::open(dir.path())
            .resolve(&credential_ref("notification", "ntfy", "token").unwrap())
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["unknown_root"]["kept"], true);
        assert_eq!(root["notifications"]["future_channel"]["kept"], true);
        assert_eq!(
            root["notifications"]["ntfy"]["future_metadata"]["kept"],
            true
        );
        assert_eq!(root["notifications"]["ntfy"]["configured"], false);
        assert!(root["notifications"]["ntfy"].get("token").is_none());
        assert!(root["notifications"]["ntfy"]
            .get("token_encrypted")
            .is_none());

        let mut stale = clear;
        stale.notifications.ntfy.token = Some("stale-secret".to_string());
        stale.notifications.ntfy.configured = true;
        let error = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut stale,
            &BTreeSet::from(["ntfy".to_string()]),
            2,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict { actual: 3, .. }
        ));
    }

    #[test]
    fn metadata_only_notification_update_does_not_claim_unbound_canonical_ref() {
        let _key = crate::encryption::set_test_encryption_key([0xa7; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let canonical = credential_ref("notification", "ntfy", "token").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                canonical.clone(),
                "unbound-foreign-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-only".to_string();

        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            1,
        )
        .unwrap();

        assert_eq!(revision, 2);
        assert!(candidate.notifications.ntfy.credential_ref.is_none());
        assert!(!candidate.notifications.ntfy.configured);
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(loaded.notifications.ntfy.topic, "metadata-only");
        assert!(loaded.notifications.ntfy.credential_ref.is_none());
        assert!(loaded.notifications.ntfy.token.is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&canonical)
                .unwrap()
                .unwrap()
                .expose(),
            "unbound-foreign-secret"
        );
    }

    #[test]
    fn fresh_metadata_only_notification_update_keeps_unconfigured_refs_absent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-only".to_string();

        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            0,
        )
        .unwrap();

        assert_eq!(revision, 1);
        assert!(candidate.notifications.ntfy.credential_ref.is_none());
        assert!(candidate.notifications.bark.credential_ref.is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["notifications"]["ntfy"]
            .get("credential_ref")
            .is_none());
        assert!(root["notifications"]["bark"]
            .get("credential_ref")
            .is_none());
    }

    #[test]
    fn explicit_notification_domain_reset_drops_unknown_fields_and_refs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "notifications": {
                    "future_channel": {"kept_on_patch": true},
                    "ntfy": {"future_metadata": true}
                }
            }"#,
        )
        .unwrap();
        let mut candidate = crate::Config::default();
        let revision = persist_notification_credential_transaction_at_revision_with_reset(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["ntfy".to_string(), "bark".to_string()]),
            true,
            0,
        )
        .unwrap();

        assert_eq!(revision, 1);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["notifications"].get("future_channel").is_none());
        assert!(root["notifications"]["ntfy"]
            .get("future_metadata")
            .is_none());
        assert!(root["notifications"]["ntfy"]
            .get("credential_ref")
            .is_none());
        assert!(root["notifications"]["bark"]
            .get("credential_ref")
            .is_none());
        assert_eq!(
            candidate.notifications,
            crate::NotificationsConfig::default()
        );
    }

    #[test]
    fn metadata_only_notification_update_rejects_an_existing_shared_ref() {
        let _key = crate::encryption::set_test_encryption_key([0xa8; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = CredentialRef::parse("shared.notification.ref").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": shared,
                "notifications": {
                    "ntfy": {"credential_ref": shared, "configured": true}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared.clone(),
                "shared-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate: crate::Config =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();

        let error = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            1,
        )
        .unwrap_err();

        assert!(error.to_string().contains("shared by another consumer"));
    }

    #[test]
    fn notification_consumer_scan_ignores_plain_strings_but_finds_credential_fields() {
        let reference = "notification.ntfy.token";
        let harmless = serde_json::to_vec(&serde_json::json!({
            "description": reference,
            "nested": {"model": reference},
            "notifications": {"ntfy": {"credential_ref": reference}}
        }))
        .unwrap();
        assert!(
            !notification_document_has_other_consumer(&harmless, reference, "ntfy", true).unwrap()
        );

        let real_consumer = serde_json::to_vec(&serde_json::json!({
            "description": reference,
            "proxy_auth_credential_ref": reference,
            "notifications": {"ntfy": {"credential_ref": reference}}
        }))
        .unwrap();
        assert!(
            notification_document_has_other_consumer(&real_consumer, reference, "ntfy", true)
                .unwrap()
        );
    }

    #[test]
    fn metadata_only_notification_rebase_preserves_unrelated_write_and_bumps_revision() {
        let _key = crate::encryption::set_test_encryption_key([0xa9; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-after-race".to_string();

        let revision = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::new(),
            None,
            true,
            &BTreeSet::new(),
            false,
            Some(0),
            Some(MigrationFault::AfterExactCommitUnrelatedCredentialRace),
        )
        .unwrap();

        assert_eq!(revision, 2);
        assert_eq!(CredentialStore::open(dir.path()).revision().unwrap(), 2);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("provider", "anthropic", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-unrelated-winner"
        );
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(loaded.notifications.ntfy.topic, "metadata-after-race");
        assert!(loaded.notifications.ntfy.credential_ref.is_none());
    }

    #[test]
    fn committed_notification_transaction_rebases_unrelated_root_edit() {
        let _key = crate::encryption::set_test_encryption_key([0xa3; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), br#"{"server":{"port":9562}}"#).unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.bark.enabled = true;
        candidate.notifications.bark.device_key = Some("bark-transaction-secret".to_string());
        candidate.notifications.bark.configured = true;
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::new(),
            None,
            true,
            &BTreeSet::from(["bark".to_string()]),
            false,
            Some(0),
            Some(MigrationFault::AfterManifest),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{"server":{"port":9999},"external":{"kept":true}}"#,
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["server"]["port"], 9999);
        assert_eq!(root["external"]["kept"], true);
        assert_eq!(
            root["notifications"]["bark"]["credential_ref"],
            "notification.bark.device_key"
        );
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("notification", "bark", "device_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "bark-transaction-secret"
        );
    }

    #[test]
    fn broker_plaintext_migrates_to_ref_only_and_hydrates_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0xb1; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{
                "endpoint": "wss://broker.example/ws",
                "token": "broker-secret",
                "future_metadata": {"kept": true}
            }"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join(format!("{BROKER_FILE}.bak")),
            br#"{
                "endpoint": "wss://old-broker.example/ws",
                "token": "stale-backup-secret",
                "backup_metadata": true
            }"#,
        )
        .unwrap();

        let outcome = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        let durable = std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap();
        assert!(!durable.contains("broker-secret"));
        assert!(!durable.contains("\"token\""));
        assert!(!durable.contains("token_encrypted"));
        let root: Value = serde_json::from_str(&durable).unwrap();
        assert_eq!(root["credential_ref"], "broker.external.bearer_token");
        assert_eq!(root["configured"], true);
        assert_eq!(root["future_metadata"]["kept"], true);
        let backup =
            std::fs::read_to_string(dir.path().join(format!("{BROKER_FILE}.bak"))).unwrap();
        assert!(!backup.contains("stale-backup-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(backup["credential_ref"], "broker.external.bearer_token");
        assert_eq!(backup["backup_metadata"], true);

        let reference = credential_ref("broker", "external", "bearer_token").unwrap();
        let store = CredentialStore::open(dir.path());
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "broker-secret"
        );
        let status = store.status(&reference).unwrap();
        assert!(status.configured);
        assert_eq!(status.source, crate::CredentialSource::Migrated);
        let mut runtime: crate::BrokerClientConfig = serde_json::from_str(&durable).unwrap();
        runtime.hydrate_credential_from_store(dir.path()).unwrap();
        assert_eq!(runtime.token, "broker-secret");
        assert!(runtime.configured);
        assert!(!serde_json::to_string(&runtime)
            .unwrap()
            .contains("broker-secret"));
        assert!(!format!("{runtime:?}").contains("broker-secret"));

        let second = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(second.migrated_credentials, 0);
        assert_eq!(
            std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap(),
            durable
        );
    }

    #[test]
    fn broker_conflicting_legacy_fields_fail_without_partial_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb2; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("cipher-winner").unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "plain-winner",
            "token_encrypted": ciphertext,
        }))
        .unwrap();
        std::fs::write(dir.path().join(BROKER_FILE), &original).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn broker_ciphertext_only_migrates_without_persisting_ciphertext() {
        let _key = crate::encryption::set_test_encryption_key([0xb5; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("encrypted-broker-secret").unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "endpoint": "wss://broker.example/ws",
                "token_encrypted": ciphertext,
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        let durable = std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap();
        assert!(!durable.contains("token_encrypted"));
        assert!(!durable.contains(&ciphertext));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "encrypted-broker-secret"
        );
    }

    #[test]
    fn backup_only_broker_secret_is_committed_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0xb6; 32]);
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        std::fs::write(
            &backup_path,
            br#"{
                "endpoint": "wss://recover.example/ws",
                "token": "backup-only-broker-secret",
                "recovery_metadata": true
            }"#,
        )
        .unwrap();

        migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(!dir.path().join(BROKER_FILE).exists());
        let backup = std::fs::read_to_string(&backup_path).unwrap();
        assert!(!backup.contains("backup-only-broker-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(backup["credential_ref"], "broker.external.bearer_token");
        assert_eq!(backup["configured"], true);
        assert_eq!(backup["recovery_metadata"], true);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "backup-only-broker-secret"
        );
    }

    #[test]
    fn committed_broker_migration_rebases_external_metadata_and_recovers() {
        let _key = crate::encryption::set_test_encryption_key([0xb3; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{"endpoint":"wss://old.example/ws","token":"broker-secret"}"#,
        )
        .unwrap();
        let error = migrate_broker_with_fault(dir.path(), MigrationFault::AfterBroker).unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{
                "endpoint":"wss://new.example/ws",
                "token":"broker-secret",
                "external_generation": 2
            }"#,
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(BROKER_FILE)).unwrap()).unwrap();
        assert_eq!(root["endpoint"], "wss://new.example/ws");
        assert_eq!(root["external_generation"], 2);
        assert_eq!(root["credential_ref"], "broker.external.bearer_token");
        assert!(root.get("token").is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "broker-secret"
        );
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
    }

    #[test]
    fn broker_migration_rejects_cross_domain_shared_reference() {
        let _key = crate::encryption::set_test_encryption_key([0xb4; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = "shared.cross_domain.secret";
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN",
                    "secret": true,
                    "credential_ref": shared,
                    "configured": true
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "broker-secret",
            "credential_ref": shared,
        }))
        .unwrap();
        std::fs::write(dir.path().join(BROKER_FILE), &original).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());

        std::fs::write(
            dir.path().join(BROKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "endpoint": "wss://broker.example/ws",
                "credential_ref": shared,
                "configured": true,
            }))
            .unwrap(),
        )
        .unwrap();
        let metadata_only =
            migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(metadata_only
            .to_string()
            .contains("shared by another consumer"));
    }

    #[test]
    fn malformed_broker_does_not_block_provider_migration_or_loading() {
        let _key = crate::encryption::set_test_encryption_key([0xb7; 32]);
        for invalid_broker in [
            b"{ definitely-not-valid-json".as_slice(),
            br#"{"endpoint":42,"credential_ref":false}"#.as_slice(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(BROKER_FILE), invalid_broker).unwrap();
            std::fs::write(
                dir.path().join(PROVIDERS_FILE),
                br#"{
                    "openai": {
                        "api_key": "provider-isolated-secret",
                        "model": "provider-isolated-model"
                    }
                }"#,
            )
            .unwrap();

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert_eq!(outcome.migrated_credentials, 1);
            assert_eq!(
                std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
                invalid_broker
            );
            let loaded =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            let openai = loaded.providers.openai.as_ref().expect("provider loaded");
            assert_eq!(openai.api_key, "provider-isolated-secret");
            assert_eq!(openai.model.as_deref(), Some("provider-isolated-model"));
        }
    }

    #[test]
    fn broker_primary_and_backup_refs_must_match_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb8; 32]);
        let dir = tempfile::tempdir().unwrap();
        let primary = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "primary-secret",
            "credential_ref": "broker.external.primary_token",
        }))
        .unwrap();
        let backup = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://old-broker.example/ws",
            "token": "backup-secret",
            "credential_ref": "broker.external.backup_token",
        }))
        .unwrap();
        let backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        std::fs::write(dir.path().join(BROKER_FILE), &primary).unwrap();
        std::fs::write(&backup_path, &backup).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicts with primary"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            primary
        );
        assert_eq!(std::fs::read(&backup_path).unwrap(), backup);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn backup_only_broker_ref_rejects_config_backup_consumer_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb9; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = "shared.backup.broker_token";
        let broker_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://recover.example/ws",
            "token": "backup-only-secret",
            "credential_ref": shared,
        }))
        .unwrap();
        let config_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "work": {"credential_ref": shared, "configured": true}
            },
            "env_vars": [{
                "name": "SHARED_TOKEN",
                "secret": true,
                "credential_ref": shared,
                "configured": true
            }]
        }))
        .unwrap();
        let broker_backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        let config_backup_path = dir.path().join(format!("{CONFIG_FILE}.bak"));
        std::fs::write(&broker_backup_path, &broker_backup).unwrap();
        std::fs::write(&config_backup_path, &config_backup).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(std::fs::read(&broker_backup_path).unwrap(), broker_backup);
        assert_eq!(std::fs::read(&config_backup_path).unwrap(), config_backup);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }
}
