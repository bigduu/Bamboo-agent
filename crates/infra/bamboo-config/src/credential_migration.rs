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
    AfterJournal,
    AfterManifest,
    AfterCredentials,
    AfterProviders,
    AfterMcp,
    AfterRebaseCredentialCommit,
    AfterRebaseStageWrite,
    AfterRebaseManifest,
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
    std::fs::create_dir(&stage_dir)?;
    std::fs::create_dir(&backup_dir)?;
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
            &mut staged,
        )?;
    }
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
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
        let status = store.status(&secret.credential_ref)?;
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
    let Ok(original) = std::fs::read(&path) else {
        return Ok(None);
    };
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = revision
        .unwrap_or(0)
        .saturating_add(1)
        .max(minimum_generation);
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
    let Ok(original) = std::fs::read(&path) else {
        return Ok(None);
    };
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = revision
        .unwrap_or(0)
        .saturating_add(1)
        .max(minimum_generation);
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
    staged: &mut Vec<StagedFile>,
) -> ConfigStoreResult<()> {
    AtomicFileStore::new(stage_dir.join(name))
        .sensitive(sensitive)
        .write_bytes_without_backup(candidate)?;
    if let Some(original) = original {
        AtomicFileStore::new(backup_dir.join(name))
            .sensitive(sensitive)
            .write_bytes_without_backup(original)?;
    }
    staged.push(StagedFile {
        name: name.to_string(),
        staged_name: name.to_string(),
        sha256: sha256(candidate),
        original_sha256: original.map(sha256),
        migration_generation,
        sensitive,
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
    let Ok(bytes) = std::fs::read(&path) else {
        return Ok(None);
    };
    let mut manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.state == MigrationState::Complete {
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
        if file.name == CREDENTIALS_FILE {
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
    let minimum_generation = manifest.files[file_index]
        .migration_generation
        .unwrap_or(0)
        .saturating_add(1);
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
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    if stage_dir.exists() {
        std::fs::remove_dir_all(stage_dir)?;
        sync_dir(data_dir)?;
    }
    Ok(())
}

fn discard_uncommitted(data_dir: &Path) -> ConfigStoreResult<()> {
    let journal_path = data_dir.join(JOURNAL_FILE);
    let Ok(bytes) = std::fs::read(&journal_path) else {
        return Ok(());
    };
    let journal: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&journal)?;
    let stage_dir = validated_stage_dir(data_dir, &journal.stage_dir)?;
    if stage_dir.exists() {
        std::fs::remove_dir_all(stage_dir)?;
    }
    remove_file_if_exists(&journal_path)?;
    sync_dir(data_dir)
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
    if manifest.version != MIGRATION_VERSION
        || Uuid::parse_str(&manifest.transaction_id).is_err()
        || manifest.stage_dir != expected_stage
        || manifest.files.is_empty()
        || unique.len() != manifest.files.len()
        || credential_count != 1
        || manifest.files.iter().any(|file| {
            !matches!(
                file.name.as_str(),
                PROVIDERS_FILE | MCP_FILE | CREDENTIALS_FILE
            ) || file.sha256.len() != 64
                || !valid_staged_name(file)
                || (file.name != CREDENTIALS_FILE
                    && file
                        .original_sha256
                        .as_ref()
                        .is_none_or(|hash| hash.len() != 64))
                || (file.name == CREDENTIALS_FILE && !file.sensitive)
                || (file.name != CREDENTIALS_FILE && file.sensitive)
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
    Ok(path)
}

fn remove_file_if_exists(path: &Path) -> ConfigStoreResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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
    }

    #[test]
    fn real_legacy_fixtures_migrate_without_secret_or_unknown_field_loss() {
        let _key = crate::encryption::set_test_encryption_key([0x61; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.migrated_credentials >= 3);
        assert_migrated(dir.path());
        assert!(std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(BACKUP_PREFIX)));
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
            .replace(
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
        };
        let traversal = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}/../../outside"),
            state: MigrationState::Pending,
            files: vec![file.clone()],
        };
        assert!(validate_manifest(&traversal).is_err());

        let duplicate = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}"),
            state: MigrationState::Pending,
            files: vec![file.clone(), file],
        };
        assert!(validate_manifest(&duplicate).is_err());
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
