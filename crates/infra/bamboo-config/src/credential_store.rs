//! Isolated encrypted credential persistence.
//!
//! Public APIs deliberately expose only metadata. Plaintext is available only
//! through [`CredentialStore::resolve`] for runtime construction and is wrapped
//! in a type whose `Debug` implementation is always redacted.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config_store::{
    AtomicJsonStore, ConfigStoreError, ConfigStoreResult, SectionSourceKind, SectionStatus,
};

const CREDENTIAL_SCHEMA_VERSION: u32 = 1;
const ENCRYPTION_KEY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CredentialRef(String);

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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CredentialDocument {
    #[serde(default)]
    entries: BTreeMap<CredentialRef, CredentialEntry>,
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

    pub fn resolve(
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

    fn load_document(&self) -> ConfigStoreResult<CredentialDocument> {
        self.load_document_with_health()
            .map(|(document, _)| document)
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
    CredentialRef::parse(format!("{domain}.{owner}.{field}"))
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
