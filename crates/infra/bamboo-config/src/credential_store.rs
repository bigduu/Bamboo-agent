//! Isolated encrypted credential persistence.
//!
//! Public APIs deliberately expose only metadata. Plaintext is available only
//! through [`CredentialStore::resolve`] for runtime construction and is wrapped
//! in a type whose `Debug` implementation is always redacted.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config_store::{
    AtomicJsonStore, ConfigStoreError, ConfigStoreResult, SectionSourceKind, SectionStatus,
};

const CREDENTIAL_SCHEMA_VERSION: u32 = 1;
const ENCRYPTION_KEY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CredentialRef(String);

impl<'de> Deserialize<'de> for CredentialRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl CredentialRef {
    pub fn parse(value: impl Into<String>) -> ConfigStoreResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 160
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
        if !valid {
            return Err(ConfigStoreError::Validation(
                "credential reference has an invalid format".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSource {
    User,
    Migrated,
    Environment,
    ExternalStore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialStatus {
    pub credential_ref: CredentialRef,
    pub configured: bool,
    pub source: CredentialSource,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Truthful health metadata for a credential document read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialStoreHealth {
    pub revision: u64,
    pub status: SectionStatus,
    pub source: SectionSourceKind,
    pub last_error: Option<String>,
}

impl CredentialStoreHealth {
    pub fn committed(revision: u64) -> Self {
        Self {
            revision,
            status: SectionStatus::Healthy,
            source: SectionSourceKind::File,
            last_error: None,
        }
    }
}

pub struct SecretValue(String);

impl SecretValue {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialEntry {
    ciphertext: String,
    source: CredentialSource,
    updated_at: DateTime<Utc>,
    key_version: u32,
    /// Monotonic source-section revision for migrated records. User-written
    /// entries keep this absent and always outrank migration replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    migration_generation: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CredentialDocument {
    #[serde(default)]
    entries: BTreeMap<CredentialRef, CredentialEntry>,
}

#[derive(Debug)]
pub(crate) struct PreparedCredentialMigration {
    pub bytes: Vec<u8>,
    pub added: usize,
}

#[derive(Debug)]
pub(crate) struct PreparedProviderCredentialUpdate {
    pub bytes: Vec<u8>,
    pub expected_revision: u64,
}

#[derive(Debug, Deserialize)]
struct PreparedCredentialEnvelope {
    schema_version: u32,
    revision: u64,
    data: CredentialDocument,
}

#[derive(Debug, Clone)]
pub struct CredentialStore {
    store: AtomicJsonStore<CredentialDocument>,
}

impl CredentialStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        Self {
            store: AtomicJsonStore::new(
                data_dir.as_ref().join("credentials.json"),
                CREDENTIAL_SCHEMA_VERSION,
            )
            .sensitive(true),
        }
    }

    pub fn path(&self) -> &Path {
        self.store.path()
    }

    pub fn revision(&self) -> ConfigStoreResult<u64> {
        self.ensure_transaction_ready()?;
        self.revision_unchecked()
    }

    pub(crate) fn revision_unchecked(&self) -> ConfigStoreResult<u64> {
        Ok(self.store.load()?.map_or(0, |stored| stored.revision))
    }

    pub fn status(&self, credential_ref: &CredentialRef) -> ConfigStoreResult<CredentialStatus> {
        self.status_with_revision(credential_ref)
            .map(|(_, status)| status)
    }

    pub fn status_with_revision(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        let (status, health) = self.status_with_health(credential_ref)?;
        Ok((health.revision, status))
    }

    pub fn status_with_health(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<(CredentialStatus, CredentialStoreHealth)> {
        self.ensure_transaction_ready()?;
        self.status_with_health_unchecked(credential_ref)
    }

    pub(crate) fn status_unchecked(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<CredentialStatus> {
        self.status_with_health_unchecked(credential_ref)
            .map(|(status, _)| status)
    }

    fn status_with_health_unchecked(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<(CredentialStatus, CredentialStoreHealth)> {
        let (document, health) = self.load_document_with_health()?;
        let status = match document.entries.get(credential_ref) {
            Some(entry) => CredentialStatus {
                credential_ref: credential_ref.clone(),
                configured: true,
                source: entry.source,
                updated_at: Some(entry.updated_at),
            },
            None => CredentialStatus {
                credential_ref: credential_ref.clone(),
                configured: false,
                source: CredentialSource::User,
                updated_at: None,
            },
        };
        Ok((status, health))
    }

    pub fn statuses(&self) -> ConfigStoreResult<Vec<CredentialStatus>> {
        self.statuses_with_revision().map(|(_, statuses)| statuses)
    }

    pub fn statuses_with_revision(&self) -> ConfigStoreResult<(u64, Vec<CredentialStatus>)> {
        let (statuses, health) = self.statuses_with_health()?;
        Ok((health.revision, statuses))
    }

    pub fn statuses_with_health(
        &self,
    ) -> ConfigStoreResult<(Vec<CredentialStatus>, CredentialStoreHealth)> {
        self.ensure_transaction_ready()?;
        let (document, health) = self.load_document_with_health()?;
        let statuses = document
            .entries
            .into_iter()
            .map(|(credential_ref, entry)| CredentialStatus {
                credential_ref,
                configured: true,
                source: entry.source,
                updated_at: Some(entry.updated_at),
            })
            .collect();
        Ok((statuses, health))
    }

    pub fn replace(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.ensure_transaction_ready()?;
        self.replace_unchecked(credential_ref, secret, source, expected_revision)
    }

    pub(crate) fn replace_unchecked(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        if secret.trim().is_empty() || crate::patch::is_masked_api_key(secret) {
            return Err(ConfigStoreError::Validation(
                "credential value must not be empty or a mask; use clear instead".to_string(),
            ));
        }
        let mut document = self.load_document()?;
        let updated_at = Utc::now();
        let ciphertext = crate::encryption::encrypt(secret).map_err(|_| {
            ConfigStoreError::Validation("credential encryption failed".to_string())
        })?;
        document.entries.insert(
            credential_ref.clone(),
            CredentialEntry {
                ciphertext,
                source,
                updated_at,
                key_version: ENCRYPTION_KEY_VERSION,
                migration_generation: None,
            },
        );
        let revision = self
            .store
            .commit(expected_revision, document, validate_document)?;
        Ok((
            revision,
            CredentialStatus {
                credential_ref,
                configured: true,
                source,
                updated_at: Some(updated_at),
            },
        ))
    }

    pub fn clear(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.ensure_transaction_ready()?;
        let mut document = self.load_document()?;
        document.entries.remove(credential_ref);
        let revision = self
            .store
            .commit(expected_revision, document, validate_document)?;
        Ok((
            revision,
            CredentialStatus {
                credential_ref: credential_ref.clone(),
                configured: false,
                source: CredentialSource::User,
                updated_at: None,
            },
        ))
    }

    /// Atomically route explicitly updated built-in provider API keys into the
    /// credential document and replace their legacy ciphertext with stable
    /// references in the runtime config.
    ///
    /// Providers absent from `intents` are never inspected or rewritten. This
    /// is important for compatibility PATCH endpoints: an ordinary metadata
    /// update must preserve the existing credential reference exactly.
    pub(crate) fn prepare_provider_api_key_intents(
        &self,
        config: &mut crate::Config,
        intents: &BTreeSet<String>,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        struct PlannedUpdate {
            provider: &'static str,
            reference: CredentialRef,
            secret: Option<String>,
        }

        let mut updates = Vec::new();
        macro_rules! plan_env {
            ($name:literal, $field:ident) => {
                if intents.contains($name) {
                    if let Some(provider) = config.providers.$field.as_ref() {
                        let reference = credential_ref("provider", $name, "api_key")?;
                        let secret = (!provider.api_key_from_env
                            && !provider.api_key.trim().is_empty())
                        .then(|| provider.api_key.trim().to_string());
                        updates.push(PlannedUpdate {
                            provider: $name,
                            reference,
                            secret,
                        });
                    }
                }
            };
        }
        plan_env!("openai", openai);
        plan_env!("anthropic", anthropic);
        plan_env!("gemini", gemini);
        if intents.contains("bodhi") {
            if let Some(provider) = config.providers.bodhi.as_ref() {
                updates.push(PlannedUpdate {
                    provider: "bodhi",
                    reference: credential_ref("provider", "bodhi", "api_key")?,
                    secret: (!provider.api_key.trim().is_empty())
                        .then(|| provider.api_key.trim().to_string()),
                });
            }
        }

        if updates.is_empty() {
            return Ok(None);
        }

        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for provider update".to_string(),
            ));
        }
        let mut changed = false;
        for update in &updates {
            match update.secret.as_deref() {
                Some(secret) => {
                    let ciphertext = crate::encryption::encrypt(secret).map_err(|_| {
                        ConfigStoreError::Validation("credential encryption failed".to_string())
                    })?;
                    document.entries.insert(
                        update.reference.clone(),
                        CredentialEntry {
                            ciphertext,
                            source: CredentialSource::User,
                            updated_at: Utc::now(),
                            key_version: ENCRYPTION_KEY_VERSION,
                            migration_generation: None,
                        },
                    );
                    changed = true;
                }
                None => {
                    changed |= document.entries.remove(&update.reference).is_some();
                }
            }
        }
        macro_rules! publish_ref {
            ($name:literal, $field:ident) => {
                if let Some(update) = updates.iter().find(|update| update.provider == $name) {
                    if let Some(provider) = config.providers.$field.as_mut() {
                        provider.credential_ref =
                            update.secret.as_ref().map(|_| update.reference.clone());
                        provider.api_key_encrypted = None;
                    }
                }
            };
        }
        publish_ref!("openai", openai);
        publish_ref!("anthropic", anthropic);
        publish_ref!("gemini", gemini);
        publish_ref!("bodhi", bodhi);
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        let revision = if changed {
            health.revision.checked_add(1).ok_or_else(|| {
                ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
            })?
        } else {
            health.revision
        };
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": revision,
            "data": document,
        }))?;
        Ok(Some(PreparedProviderCredentialUpdate {
            bytes,
            expected_revision: health.revision,
        }))
    }

    pub fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<Option<SecretValue>> {
        self.ensure_transaction_ready()?;
        self.resolve_unchecked(credential_ref)
    }

    pub(crate) fn resolve_unchecked(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<Option<SecretValue>> {
        let document = self.load_document()?;
        document
            .entries
            .get(credential_ref)
            .map(|entry| {
                crate::encryption::decrypt(&entry.ciphertext)
                    .map(SecretValue)
                    .map_err(|_| {
                        ConfigStoreError::Validation("credential decryption failed".to_string())
                    })
            })
            .transpose()
    }

    /// Build, but do not install, a credential document for a cross-file
    /// migration. Installation must go through [`Self::commit_migration`] so a
    /// user update racing a staged transaction cannot be overwritten.
    pub(crate) fn prepare_migration(
        &self,
        secrets: Vec<(CredentialRef, String, u64)>,
    ) -> ConfigStoreResult<PreparedCredentialMigration> {
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for migration".to_string(),
            ));
        }
        let mut added = 0;
        let mut changed = false;
        for (credential_ref, secret, input_generation) in secrets {
            if secret.trim().is_empty() || crate::patch::is_masked_api_key(&secret) {
                return Err(ConfigStoreError::Validation(
                    "legacy credential value is invalid".to_string(),
                ));
            }
            // An old binary can rewrite an already-migrated sidecar in the
            // unversioned shape. Its nominal generation is one again, so the
            // secret value itself decides whether this is a no-op or a newer
            // migration. User-managed records remain authoritative.
            let migration_generation = match document.entries.get(&credential_ref) {
                Some(entry) if entry.source != CredentialSource::Migrated => continue,
                Some(entry) => {
                    let existing = crate::encryption::decrypt(&entry.ciphertext).map_err(|_| {
                        ConfigStoreError::Validation(
                            "existing migrated credential is unavailable".to_string(),
                        )
                    })?;
                    if existing == secret {
                        continue;
                    }
                    let next_generation = entry
                        .migration_generation
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| {
                            ConfigStoreError::Validation(
                                "credential migration generation counter exhausted".to_string(),
                            )
                        })?;
                    input_generation.max(next_generation)
                }
                None => input_generation,
            };
            let ciphertext = crate::encryption::encrypt(&secret).map_err(|_| {
                ConfigStoreError::Validation("credential encryption failed".to_string())
            })?;
            let was_new = document
                .entries
                .insert(
                    credential_ref,
                    CredentialEntry {
                        ciphertext,
                        source: CredentialSource::Migrated,
                        updated_at: Utc::now(),
                        key_version: ENCRYPTION_KEY_VERSION,
                        migration_generation: Some(migration_generation),
                    },
                )
                .is_none();
            added += usize::from(was_new);
            changed = true;
        }
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        let revision = if changed {
            health.revision.checked_add(1).ok_or_else(|| {
                ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
            })?
        } else {
            health.revision
        };
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": revision,
            "data": document,
        }))?;
        Ok(PreparedCredentialMigration { bytes, added })
    }

    /// Merge a durable staged migration candidate under the credential store's
    /// revision CAS. User records always win; a migrated record may be rebased
    /// by a newer legacy section write. Replaying the same committed candidate
    /// is a no-op.
    pub(crate) fn commit_migration(&self, staged: &[u8]) -> ConfigStoreResult<()> {
        let prepared: PreparedCredentialEnvelope = serde_json::from_slice(staged)?;
        if prepared.schema_version != CREDENTIAL_SCHEMA_VERSION {
            return Err(ConfigStoreError::Validation(
                "staged credential document has an unsupported schema".to_string(),
            ));
        }
        validate_document(&prepared.data).map_err(ConfigStoreError::Validation)?;
        for _ in 0..16 {
            let (mut current, health) = self.load_document_with_health()?;
            if health.status == SectionStatus::Degraded {
                return Err(ConfigStoreError::Validation(
                    "credential document is unavailable for migration".to_string(),
                ));
            }
            let mut changed = false;
            for (credential_ref, entry) in &prepared.data.entries {
                match current.entries.get(credential_ref) {
                    None => {
                        current
                            .entries
                            .insert(credential_ref.clone(), entry.clone());
                        changed = true;
                    }
                    Some(existing)
                        if existing.source == CredentialSource::Migrated
                            && entry.source == CredentialSource::Migrated
                            && existing.migration_generation.unwrap_or(0)
                                < entry.migration_generation.unwrap_or(0) =>
                    {
                        current
                            .entries
                            .insert(credential_ref.clone(), entry.clone());
                        changed = true;
                    }
                    Some(_) => {}
                }
            }
            if !changed {
                return Ok(());
            }
            match self
                .store
                .commit(health.revision, current, validate_document)
            {
                Ok(_) => return Ok(()),
                Err(ConfigStoreError::Conflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ConfigStoreError::Validation(
            "credential migration could not obtain a stable revision".to_string(),
        ))
    }

    pub(crate) fn validate_document_bytes(bytes: &[u8]) -> ConfigStoreResult<u64> {
        let document: PreparedCredentialEnvelope = serde_json::from_slice(bytes)?;
        if document.schema_version != CREDENTIAL_SCHEMA_VERSION {
            return Err(ConfigStoreError::Validation(
                "credential document has an unsupported schema".to_string(),
            ));
        }
        validate_document(&document.data).map_err(ConfigStoreError::Validation)?;
        Ok(document.revision)
    }

    fn load_document(&self) -> ConfigStoreResult<CredentialDocument> {
        self.load_document_with_health()
            .map(|(document, _)| document)
    }

    fn ensure_transaction_ready(&self) -> ConfigStoreResult<()> {
        let data_dir = self.path().parent().unwrap_or_else(|| Path::new("."));
        crate::ensure_provider_mcp_migration_ready(data_dir)
    }

    fn load_document_with_health(
        &self,
    ) -> ConfigStoreResult<(CredentialDocument, CredentialStoreHealth)> {
        Ok(match self.store.load_validated(validate_document)? {
            Some(stored) if stored.recovered_from_backup => (
                stored.data,
                CredentialStoreHealth {
                    revision: stored.revision,
                    status: SectionStatus::Degraded,
                    source: SectionSourceKind::Backup,
                    last_error: Some(
                        "primary credential document invalid; using last-known-good backup"
                            .to_string(),
                    ),
                },
            ),
            Some(stored) => (
                stored.data,
                CredentialStoreHealth::committed(stored.revision),
            ),
            None => (
                CredentialDocument::default(),
                CredentialStoreHealth {
                    revision: 0,
                    status: SectionStatus::Missing,
                    source: SectionSourceKind::Default,
                    last_error: None,
                },
            ),
        })
    }
}

fn validate_document(document: &CredentialDocument) -> Result<(), String> {
    if document
        .entries
        .values()
        .any(|entry| entry.ciphertext.is_empty())
    {
        return Err("credential ciphertext must not be empty".to_string());
    }
    Ok(())
}

/// Stable credential reference convention used by migrations and section DTOs.
pub fn credential_ref(domain: &str, owner: &str, field: &str) -> ConfigStoreResult<CredentialRef> {
    CredentialRef::parse(format!(
        "{}.{}.{}",
        credential_ref_component(domain),
        credential_ref_component(owner),
        credential_ref_component(field)
    ))
}

/// Encode an arbitrary external identifier into one reference component.
/// Common ASCII identifiers retain the documented readable convention. The
/// reserved `x_` prefix plus hex encoding makes unsafe names injective (for
/// example `a.b`, `a/b`, and the literal `x_612e62` cannot collide).
fn credential_ref_component(value: &str) -> String {
    let readable = !value.is_empty()
        && value.len() <= 43
        && !value.starts_with("x_")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if readable {
        return value.to_string();
    }
    if value.len() <= 20 {
        return format!("x_{}", hex::encode(value.as_bytes()));
    }
    let digest = Sha256::digest(value.as_bytes());
    format!("x_h{}", hex::encode(&digest[..20]))
}

pub fn credentials_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join("credentials.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn replace_resolve_status_and_clear_never_expose_secret_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x31; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        let (revision, status) = store
            .replace(
                reference.clone(),
                "sk-super-secret",
                CredentialSource::User,
                0,
            )
            .unwrap();
        assert_eq!(revision, 1);
        assert!(status.configured);
        assert!(!serde_json::to_string(&status)
            .unwrap()
            .contains("sk-super-secret"));
        let persisted = std::fs::read_to_string(store.path()).unwrap();
        assert!(!persisted.contains("sk-super-secret"));
        let resolved = store.resolve(&reference).unwrap().unwrap();
        assert_eq!(resolved.expose(), "sk-super-secret");
        assert_eq!(format!("{resolved:?}"), "SecretValue([REDACTED])");

        let (revision, status) = store.clear(&reference, 1).unwrap();
        assert_eq!(revision, 2);
        assert!(!status.configured);
        assert!(store.resolve(&reference).unwrap().is_none());
    }

    #[test]
    fn provider_intents_commit_once_and_publish_only_stable_refs() {
        let _key = crate::encryption::set_test_encryption_key([0x32; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let mut config = crate::Config::default();
        config.providers.openai = Some(crate::OpenAIConfig {
            api_key: "sk-openai-secret".to_string(),
            ..Default::default()
        });
        config.providers.anthropic = Some(crate::AnthropicConfig {
            api_key: "sk-anthropic-secret".to_string(),
            ..Default::default()
        });
        let intents = BTreeSet::from(["anthropic".to_string(), "openai".to_string()]);

        crate::persist_provider_credential_transaction(dir.path(), &mut config, &intents).unwrap();
        let revision = store.revision().unwrap();
        assert_eq!(revision, 1, "both secrets must use one CAS commit");

        let openai = config.providers.openai.as_ref().unwrap();
        assert_eq!(
            openai.credential_ref.as_ref().map(CredentialRef::as_str),
            Some("provider.openai.api_key")
        );
        assert!(openai.api_key_encrypted.is_none());
        let anthropic = config.providers.anthropic.as_ref().unwrap();
        assert_eq!(
            anthropic.credential_ref.as_ref().map(CredentialRef::as_str),
            Some("provider.anthropic.api_key")
        );
        assert!(anthropic.api_key_encrypted.is_none());

        assert_eq!(
            store
                .resolve(openai.credential_ref.as_ref().unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-openai-secret"
        );
        assert_eq!(
            store
                .resolve(anthropic.credential_ref.as_ref().unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-anthropic-secret"
        );
        let raw = std::fs::read_to_string(store.path()).unwrap();
        assert!(!raw.contains("sk-openai-secret"));
        assert!(!raw.contains("sk-anthropic-secret"));

        let no_intents = BTreeSet::new();
        crate::persist_provider_credential_transaction(dir.path(), &mut config, &no_intents)
            .unwrap();
        assert_eq!(store.revision().unwrap(), revision);
        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .unwrap()
                .credential_ref
                .as_ref()
                .unwrap()
                .as_str(),
            "provider.openai.api_key"
        );
    }

    #[test]
    fn stale_replace_is_rejected_without_overwriting_newer_secret() {
        let _key = crate::encryption::set_test_encryption_key([0x32; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("mcp", "github", "token").unwrap();
        store
            .replace(reference.clone(), "newest", CredentialSource::User, 0)
            .unwrap();
        assert!(matches!(
            store.replace(reference.clone(), "stale", CredentialSource::User, 0),
            Err(ConfigStoreError::Conflict {
                expected: 0,
                actual: 1
            })
        ));
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "newest"
        );
    }

    #[test]
    fn credential_reference_rejects_paths_and_unbounded_values() {
        assert!(CredentialRef::parse("provider.openai.api_key").is_ok());
        assert!(CredentialRef::parse("../credentials").is_err());
        assert!(CredentialRef::parse("x".repeat(161)).is_err());
        assert!(serde_json::from_str::<CredentialRef>(r#""../credentials""#).is_err());
        assert!(
            serde_json::from_value::<CredentialRef>(serde_json::json!("x".repeat(161))).is_err()
        );
        assert!(
            serde_json::from_value::<crate::OpenAIConfig>(serde_json::json!({
                "credential_ref": "../credentials"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<crate::ProviderInstanceConfig>(serde_json::json!({
                "provider_type": "openai",
                "credential_ref": "x".repeat(161)
            }))
            .is_err()
        );
    }

    #[test]
    fn external_names_map_to_valid_non_colliding_references() {
        let values = ["a.b", "a/b", "x_612e62", "a b", "你好"];
        let references = values
            .iter()
            .map(|name| credential_ref("mcp", name, "header_Authorization").unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(references.len(), values.len());
        assert!(references
            .iter()
            .all(|reference| CredentialRef::parse(reference.as_str()).is_ok()));
        assert_eq!(
            credential_ref("provider", "openai", "api_key")
                .unwrap()
                .as_str(),
            "provider.openai.api_key"
        );
        let long = credential_ref(
            "mcp",
            &format!("server/{}", "界".repeat(40)),
            &format!("header/{}", "x".repeat(100)),
        )
        .unwrap();
        assert!(long.as_str().len() <= 160);
        assert!(CredentialRef::parse(long.as_str()).is_ok());
    }

    #[test]
    fn replace_rejects_whitespace_and_ui_masks_without_writing() {
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        for invalid in ["   ", "********", "****...****", "  ****...****  "] {
            assert!(matches!(
                store.replace(reference.clone(), invalid, CredentialSource::User, 0),
                Err(ConfigStoreError::Validation(_))
            ));
        }
        assert!(!store.path().exists());
        assert_eq!(store.revision().unwrap(), 0);
    }

    #[test]
    fn read_health_distinguishes_missing_file_and_backup_recovery() {
        let _key = crate::encryption::set_test_encryption_key([0x33; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        let (_, missing) = store.status_with_health(&reference).unwrap();
        assert_eq!(missing.revision, 0);
        assert_eq!(missing.status, SectionStatus::Missing);
        assert_eq!(missing.source, SectionSourceKind::Default);

        store
            .replace(reference.clone(), "first", CredentialSource::User, 0)
            .unwrap();
        store
            .replace(reference.clone(), "second", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(store.path(), b"{corrupt").unwrap();
        let (status, recovered) = store.status_with_health(&reference).unwrap();
        assert!(status.configured);
        assert_eq!(recovered.revision, 1);
        assert_eq!(recovered.status, SectionStatus::Degraded);
        assert_eq!(recovered.source, SectionSourceKind::Backup);
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("primary credential document invalid; using last-known-good backup")
        );
    }
}
