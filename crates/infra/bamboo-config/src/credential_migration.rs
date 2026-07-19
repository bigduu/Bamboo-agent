//! Recoverable provider/MCP credential extraction.
//!
//! The migration treats `credentials.json`, `providers.json`, and `mcp.json`
//! as one manifest-gated transaction. Candidate bytes are staged and fsynced
//! before the manifest commit point. Startup always finishes a committed
//! manifest before any configuration document is read.

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
const CREDENTIALS_FILE: &str = "credentials.json";
const CONFIG_FILE: &str = "config.json";

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
    files: Vec<StagedFile>,
    /// A credential write that linearized after an exact provider transaction
    /// commit remains authoritative. Recovery then owns only the two metadata
    /// documents and must never replay its older staged credential bytes.
    #[serde(default)]
    credential_superseded: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MigrationState {
    Pending,
    Complete,
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
    AfterRebaseCredentialCommit,
    AfterRebaseStageWrite,
    AfterRebaseManifest,
    BeforeExactCommitCredentialRace,
    AfterExactCommitCredentialRace,
}

/// Extract provider/MCP sidecar secrets into the isolated credential store.
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

/// Fail-closed guard for every production reader of provider/MCP transaction
/// members. A malformed manifest is treated like a pending one: callers must
/// retain their existing runtime rather than guessing which files committed.
pub fn ensure_provider_mcp_migration_ready(data_dir: impl AsRef<Path>) -> ConfigStoreResult<()> {
    let path = data_dir.as_ref().join(MANIFEST_FILE);
    let Some(bytes) = read_optional_migration_file(&path)? else {
        return Ok(());
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| {
        ConfigStoreError::Validation("provider/MCP credential migration is pending".to_string())
    })?;
    validate_manifest(&manifest).map_err(|_| {
        ConfigStoreError::Validation("provider/MCP credential migration is pending".to_string())
    })?;
    if manifest.state == MigrationState::Pending {
        return Err(ConfigStoreError::Validation(
            "provider/MCP credential migration is pending".to_string(),
        ));
    }
    Ok(())
}

/// Persist a legacy provider-key update as one manifest-gated transaction.
/// The caller must pass a detached candidate config and publish it to live
/// memory only after this function succeeds.
pub fn persist_provider_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    intents: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    #[cfg(test)]
    return persist_provider_credential_transaction_inner(data_dir.as_ref(), config, intents, None);
    #[cfg(not(test))]
    persist_provider_credential_transaction_inner(data_dir.as_ref(), config, intents)
}

fn persist_provider_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    intents: &BTreeSet<String>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
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

    let store = CredentialStore::open(data_dir);
    let Some(prepared) = store.prepare_provider_api_key_intents(config, intents)? else {
        return Ok(());
    };
    let credentials_original = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    let providers_original = read_target_or_empty(&data_dir.join(PROVIDERS_FILE))?;
    let config_original = read_target_or_empty(&data_dir.join(CONFIG_FILE))?;
    let (config_bytes, provider_bytes) = config
        .prepare_provider_transaction_documents(&providers_original)
        .map_err(|error| ConfigStoreError::Validation(error.to_string()))?;
    if store.revision()? != prepared.expected_revision {
        return Err(ConfigStoreError::Conflict {
            expected: prepared.expected_revision,
            actual: store.revision()?,
        });
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
        files: staged,
        credential_superseded: false,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;

    #[cfg(test)]
    if fault == Some(MigrationFault::BeforeExactCommitCredentialRace) {
        let reference = credential_ref("provider", "openai", "api_key")?;
        store.replace(
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
                    actual: store.revision()?,
                });
            }
            return Err(ConfigStoreError::Validation(format!(
                "{} changed during provider credential transaction",
                file.name
            )));
        }
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterExactCommitCredentialRace) {
        let reference = credential_ref("provider", "openai", "api_key")?;
        store.replace_unchecked(
            reference,
            "concurrent-post-commit-winner",
            crate::CredentialSource::User,
            prepared.expected_revision,
        )?;
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
    finish_transaction(data_dir, manifest)
}

#[cfg(test)]
fn migrate_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_inner(data_dir, Some(fault))
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
    if providers.is_none() && mcp.is_none() {
        return Ok(CredentialMigrationOutcome {
            migrated_credentials: 0,
            resumed: false,
        });
    }

    let credential_store = CredentialStore::open(data_dir);
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
    for section in providers.into_iter().chain(mcp) {
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
        files: staged,
        credential_superseded: false,
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
    });
    Ok(())
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

fn install_pending(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    validate_manifest(manifest)?;
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
        let staged = std::fs::read(stage_dir.join(&file.staged_name))?;
        if sha256(&staged) != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "staged migration document failed integrity validation".to_string(),
            ));
        }
        if file.install_mode == InstallMode::Exact {
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
                    if file.name == CREDENTIALS_FILE {
                        let actual = CredentialStore::validate_document_bytes(&current)?;
                        let expected = file.expected_revision.unwrap_or(0);
                        if actual > expected {
                            manifest.files.remove(file_index);
                            manifest.credential_superseded = true;
                            write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
                            continue;
                        }
                        return Err(ConfigStoreError::Conflict { expected, actual });
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
        ) {
            return Err(injected_fault());
        }
    }
    Ok(())
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
    manifest.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &manifest)
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
    let exact_members_valid = (manifest.files.len() == 3
        && !manifest.credential_superseded
        && unique.contains(CREDENTIALS_FILE))
        || (manifest.files.len() == 2
            && manifest.credential_superseded
            && !unique.contains(CREDENTIALS_FILE));
    let exact_shape_valid = !exact_transaction
        || (exact_members_valid
            && manifest
                .files
                .iter()
                .all(|file| file.install_mode == InstallMode::Exact)
            && unique.contains(PROVIDERS_FILE)
            && unique.contains(CONFIG_FILE)
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
        || credential_count != usize::from(!manifest.credential_superseded)
        || (manifest.credential_superseded && !exact_transaction)
        || !exact_shape_valid
        || manifest.files.iter().any(|file| {
            !matches!(
                file.name.as_str(),
                PROVIDERS_FILE | MCP_FILE | CREDENTIALS_FILE | CONFIG_FILE
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
                || (file.install_mode == InstallMode::Migration && file.name == CONFIG_FILE)
                || (file.name == CREDENTIALS_FILE && !file.sensitive)
                || (file.name != CREDENTIALS_FILE && file.sensitive)
                || (file.expected_revision.is_some() && file.name != CREDENTIALS_FILE)
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
    if file.name == CREDENTIALS_FILE {
        return false;
    }
    let Some(suffix) = file
        .staged_name
        .strip_prefix(&format!("{}.rebase.", file.name))
    else {
        return false;
    };
    Uuid::parse_str(suffix).is_ok()
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
        };
        let traversal = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}/../../outside"),
            state: MigrationState::Pending,
            files: vec![file.clone()],
            credential_superseded: false,
        };
        assert!(validate_manifest(&traversal).is_err());

        let duplicate = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}"),
            state: MigrationState::Pending,
            files: vec![file.clone(), file],
            credential_superseded: false,
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
}
