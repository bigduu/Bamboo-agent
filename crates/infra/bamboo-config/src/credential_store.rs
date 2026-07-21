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
use serde_json::Value;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    pub revision: u64,
    pub touched_refs: Vec<CredentialRef>,
    pub required_refs: Vec<CredentialRef>,
}

pub(crate) type PersistedConnectCredentialRefs =
    BTreeMap<String, (Option<CredentialRef>, Option<CredentialRef>)>;

#[derive(Debug, Clone, Default)]
pub(crate) struct PersistedAccessCredentialRefs {
    pub password: Option<CredentialRef>,
    pub devices: BTreeMap<String, CredentialRef>,
}

impl PreparedProviderCredentialUpdate {
    pub(crate) fn advance_revision_for_domain_change(&mut self) -> ConfigStoreResult<()> {
        if self.revision != self.expected_revision {
            return Ok(());
        }
        self.revision = self.revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?;
        let mut envelope: Value = serde_json::from_slice(&self.bytes)?;
        envelope["revision"] = Value::from(self.revision);
        self.bytes = serde_json::to_vec_pretty(&envelope)?;
        Ok(())
    }
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

/// Immutable encrypted document retained by the section facade as its
/// last-known-good mutation base. The document is deliberately opaque outside
/// this module so neither ciphertext nor plaintext can escape through section
/// APIs.
#[derive(Clone)]
pub(crate) struct CredentialDocumentLkg {
    revision: u64,
    document: CredentialDocument,
}

impl CredentialDocumentLkg {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }
}

pub(crate) struct CredentialMutation {
    pub revision: u64,
    pub status: CredentialStatus,
    pub statuses: Vec<CredentialStatus>,
    pub lkg: CredentialDocumentLkg,
}

#[derive(Clone, Copy)]
enum CredentialCommitAuthority<'a> {
    RevisionOnly,
    LiveSnapshot(&'a CredentialDocumentLkg),
    RepairLkg(&'a CredentialDocumentLkg),
}

impl CredentialCommitAuthority<'_> {
    fn document(self, expected_revision: u64) -> ConfigStoreResult<Option<CredentialDocument>> {
        let lkg = match self {
            Self::RevisionOnly => return Ok(None),
            Self::LiveSnapshot(lkg) | Self::RepairLkg(lkg) => lkg,
        };
        if lkg.revision != expected_revision {
            return Err(ConfigStoreError::Conflict {
                expected: expected_revision,
                actual: lkg.revision,
            });
        }
        Ok(Some(lkg.document.clone()))
    }
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
        self.with_transaction_lock(|| self.revision_unchecked())
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
        self.with_transaction_lock(|| self.status_with_health_unchecked(credential_ref))
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
        self.with_transaction_lock(|| {
            let (document, health) = self.load_document_with_health()?;
            let statuses = credential_statuses(&document);
            Ok((statuses, health))
        })
    }

    pub(crate) fn snapshot_with_health(
        &self,
    ) -> ConfigStoreResult<(
        CredentialDocumentLkg,
        Vec<CredentialStatus>,
        CredentialStoreHealth,
    )> {
        self.with_transaction_lock(|| self.snapshot_with_health_unchecked())
    }

    /// Snapshot variant for callers that already hold the shared credential
    /// migration lock. Keeping this separate avoids recursively acquiring the
    /// process-wide transaction lock while a facade establishes its stable
    /// open barrier.
    pub(crate) fn snapshot_with_health_unchecked(
        &self,
    ) -> ConfigStoreResult<(
        CredentialDocumentLkg,
        Vec<CredentialStatus>,
        CredentialStoreHealth,
    )> {
        let (document, health) = self.load_document_with_health()?;
        let statuses = credential_statuses(&document);
        let lkg = CredentialDocumentLkg {
            revision: health.revision,
            document,
        };
        Ok((lkg, statuses, health))
    }

    pub fn replace(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.with_transaction_lock(|| {
            self.replace_unchecked(credential_ref, secret, source, expected_revision)
        })
    }

    /// Replace an internally managed rotating credential at the latest durable
    /// revision. This is not an HTTP/client mutation primitive: external
    /// callers must continue to use [`Self::replace`] with explicit CAS. The
    /// migration lock makes the revision read and write one cross-process
    /// operation for caches such as Copilot OAuth.
    pub fn replace_system_managed(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.with_transaction_lock(|| {
            let revision = self.revision_unchecked()?;
            self.replace_unchecked(credential_ref, secret, source, revision)
        })
    }

    pub(crate) fn replace_unchecked(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.replace_with_commit(
            credential_ref,
            secret,
            source,
            expected_revision,
            CredentialCommitAuthority::RevisionOnly,
        )
        .map(|mutation| (mutation.revision, mutation.status))
    }

    pub(crate) fn replace_from_live_snapshot_unchecked(
        &self,
        live_lkg: &CredentialDocumentLkg,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<CredentialMutation> {
        self.replace_with_commit(
            credential_ref,
            secret,
            source,
            expected_revision,
            CredentialCommitAuthority::LiveSnapshot(live_lkg),
        )
    }

    pub(crate) fn replace_from_live_lkg_unchecked(
        &self,
        live_lkg: &CredentialDocumentLkg,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<CredentialMutation> {
        self.replace_with_commit(
            credential_ref,
            secret,
            source,
            expected_revision,
            CredentialCommitAuthority::RepairLkg(live_lkg),
        )
    }

    fn replace_with_commit(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
        authority: CredentialCommitAuthority<'_>,
    ) -> ConfigStoreResult<CredentialMutation> {
        if secret.trim().is_empty() || crate::patch::is_masked_api_key(secret) {
            return Err(ConfigStoreError::Validation(
                "credential value must not be empty or a mask; use clear instead".to_string(),
            ));
        }
        let mut document = match authority.document(expected_revision)? {
            Some(document) => document,
            None => self.load_document()?,
        };
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
        let committed_document = document.clone();
        let revision = match authority {
            CredentialCommitAuthority::RevisionOnly => {
                self.store
                    .commit(expected_revision, document, validate_document)?
            }
            CredentialCommitAuthority::LiveSnapshot(lkg) => self.store.commit_from_live_snapshot(
                expected_revision,
                &lkg.document,
                document,
                validate_document,
            )?,
            CredentialCommitAuthority::RepairLkg(_) => {
                self.store
                    .commit_from_live_lkg(expected_revision, document, validate_document)?
            }
        };
        let statuses = credential_statuses(&committed_document);
        Ok(CredentialMutation {
            revision,
            status: CredentialStatus {
                credential_ref,
                configured: true,
                source,
                updated_at: Some(updated_at),
            },
            statuses,
            lkg: CredentialDocumentLkg {
                revision,
                document: committed_document,
            },
        })
    }

    pub fn clear(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.with_transaction_lock(|| self.clear_unchecked(credential_ref, expected_revision))
    }

    /// Clear an internally managed rotating credential at the latest durable
    /// revision. Client-facing clears remain explicit-CAS via [`Self::clear`].
    pub fn clear_system_managed(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.with_transaction_lock(|| {
            let revision = self.revision_unchecked()?;
            self.clear_unchecked(credential_ref, revision)
        })
    }

    pub(crate) fn clear_unchecked(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.clear_with_commit(
            credential_ref,
            expected_revision,
            CredentialCommitAuthority::RevisionOnly,
        )
        .map(|mutation| (mutation.revision, mutation.status))
    }

    pub(crate) fn clear_from_live_snapshot_unchecked(
        &self,
        live_lkg: &CredentialDocumentLkg,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<CredentialMutation> {
        self.clear_with_commit(
            credential_ref,
            expected_revision,
            CredentialCommitAuthority::LiveSnapshot(live_lkg),
        )
    }

    pub(crate) fn clear_from_live_lkg_unchecked(
        &self,
        live_lkg: &CredentialDocumentLkg,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<CredentialMutation> {
        self.clear_with_commit(
            credential_ref,
            expected_revision,
            CredentialCommitAuthority::RepairLkg(live_lkg),
        )
    }

    fn clear_with_commit(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
        authority: CredentialCommitAuthority<'_>,
    ) -> ConfigStoreResult<CredentialMutation> {
        let mut document = match authority.document(expected_revision)? {
            Some(document) => document,
            None => self.load_document()?,
        };
        document.entries.remove(credential_ref);
        let committed_document = document.clone();
        let revision = match authority {
            CredentialCommitAuthority::RevisionOnly => {
                self.store
                    .commit(expected_revision, document, validate_document)?
            }
            CredentialCommitAuthority::LiveSnapshot(lkg) => self.store.commit_from_live_snapshot(
                expected_revision,
                &lkg.document,
                document,
                validate_document,
            )?,
            CredentialCommitAuthority::RepairLkg(_) => {
                self.store
                    .commit_from_live_lkg(expected_revision, document, validate_document)?
            }
        };
        let statuses = credential_statuses(&committed_document);
        Ok(CredentialMutation {
            revision,
            status: CredentialStatus {
                credential_ref: credential_ref.clone(),
                configured: false,
                source: CredentialSource::User,
                updated_at: None,
            },
            statuses,
            lkg: CredentialDocumentLkg {
                revision,
                document: committed_document,
            },
        })
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
        provider_intents: &BTreeSet<String>,
        provider_instance_intents: &BTreeSet<String>,
        persisted_instance_refs: &BTreeMap<String, CredentialRef>,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        if provider_intents.contains("__proxy_auth") {
            if provider_intents.len() != 1 || !provider_instance_intents.is_empty() {
                return Err(ConfigStoreError::Validation(
                    "proxy auth must be updated in its own credential transaction".to_string(),
                ));
            }
            return self.prepare_proxy_auth_intent(config).map(Some);
        }
        struct PlannedUpdate {
            target: PlannedProviderTarget,
            reference: CredentialRef,
            secret: Option<String>,
            removes_candidate_consumer: bool,
        }

        enum PlannedProviderTarget {
            BuiltIn(&'static str),
            Instance(String),
        }

        let mut updates = Vec::new();
        macro_rules! plan_env {
            ($name:literal, $field:ident) => {
                if provider_intents.contains($name) {
                    if let Some(provider) = config.providers.$field.as_ref() {
                        let reference = credential_ref("provider", $name, "api_key")?;
                        let secret = (!provider.api_key_from_env
                            && !provider.api_key.trim().is_empty())
                        .then(|| provider.api_key.trim().to_string());
                        updates.push(PlannedUpdate {
                            target: PlannedProviderTarget::BuiltIn($name),
                            removes_candidate_consumer: provider.credential_ref.as_ref()
                                == Some(&reference),
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
        if provider_intents.contains("bodhi") {
            if let Some(provider) = config.providers.bodhi.as_ref() {
                let reference = credential_ref("provider", "bodhi", "api_key")?;
                updates.push(PlannedUpdate {
                    target: PlannedProviderTarget::BuiltIn("bodhi"),
                    removes_candidate_consumer: provider.credential_ref.as_ref()
                        == Some(&reference),
                    reference,
                    secret: (!provider.api_key.trim().is_empty())
                        .then(|| provider.api_key.trim().to_string()),
                });
            }
        }

        for instance_id in provider_instance_intents {
            let instance = config.provider_instances.get(instance_id);
            let reference = instance
                .and_then(|instance| instance.credential_ref.clone())
                .or_else(|| persisted_instance_refs.get(instance_id).cloned())
                .unwrap_or(credential_ref("provider_instance", instance_id, "api_key")?);
            let secret = instance
                .filter(|instance| !instance.api_key.trim().is_empty())
                .map(|instance| instance.api_key.trim().to_string());
            updates.push(PlannedUpdate {
                target: PlannedProviderTarget::Instance(instance_id.clone()),
                removes_candidate_consumer: instance
                    .and_then(|instance| instance.credential_ref.as_ref())
                    == Some(&reference),
                reference,
                secret,
            });
        }

        if updates.is_empty() {
            return Ok(None);
        }

        let candidate_ref_counts = config_credential_ref_counts(config)?;
        let mut removed_consumer_counts = BTreeMap::<CredentialRef, usize>::new();
        let mut secrets_by_ref = BTreeMap::<CredentialRef, String>::new();
        for update in &updates {
            if let Some(secret) = update.secret.as_ref() {
                match secrets_by_ref.get(&update.reference) {
                    Some(existing) if existing != secret => {
                        return Err(ConfigStoreError::Validation(
                            "provider updates assign conflicting values to one credential reference"
                                .to_string(),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        secrets_by_ref.insert(update.reference.clone(), secret.clone());
                    }
                }
            } else if update.removes_candidate_consumer {
                *removed_consumer_counts
                    .entry(update.reference.clone())
                    .or_default() += 1;
            }
        }
        let touched_refs = updates
            .iter()
            .map(|update| update.reference.clone())
            .collect::<BTreeSet<_>>();
        let retained_after_update = |reference: &CredentialRef| {
            candidate_ref_counts.get(reference).copied().unwrap_or(0)
                > removed_consumer_counts.get(reference).copied().unwrap_or(0)
        };
        let required_refs = touched_refs
            .iter()
            .filter(|reference| {
                secrets_by_ref.contains_key(*reference) || retained_after_update(reference)
            })
            .cloned()
            .collect::<Vec<_>>();

        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for provider update".to_string(),
            ));
        }
        let mut changed = false;
        for reference in &touched_refs {
            match secrets_by_ref.get(reference).map(String::as_str) {
                Some(secret) => {
                    let ciphertext = crate::encryption::encrypt(secret).map_err(|_| {
                        ConfigStoreError::Validation("credential encryption failed".to_string())
                    })?;
                    document.entries.insert(
                        reference.clone(),
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
                None if !retained_after_update(reference) => {
                    changed |= document.entries.remove(reference).is_some();
                }
                None => {}
            }
        }
        macro_rules! publish_ref {
            ($name:literal, $field:ident) => {
                if let Some(update) = updates.iter().find(|update| {
                    matches!(&update.target, PlannedProviderTarget::BuiltIn(name) if *name == $name)
                }) {
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
        for update in &updates {
            let PlannedProviderTarget::Instance(instance_id) = &update.target else {
                continue;
            };
            if let Some(instance) = config.provider_instances.get_mut(instance_id) {
                instance.credential_ref = update.secret.as_ref().map(|_| update.reference.clone());
                instance.api_key_encrypted = None;
            }
        }
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(&document, &required_refs)?;
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
            revision,
            touched_refs: touched_refs.into_iter().collect(),
            required_refs,
        }))
    }

    /// Prepare the fixed proxy-auth credential update without publishing it.
    /// The caller commits these bytes together with root metadata through the
    /// recoverable exact transaction manifest.
    pub(crate) fn prepare_proxy_auth_intent(
        &self,
        config: &mut crate::Config,
    ) -> ConfigStoreResult<PreparedProviderCredentialUpdate> {
        let canonical = credential_ref("proxy", "default", "auth")?;
        let current_reference = config
            .proxy_auth_credential_ref
            .clone()
            .unwrap_or_else(|| canonical.clone());
        let secret = config
            .proxy_auth
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let counts = config_credential_ref_counts(config)?;
        let non_proxy_consumers = |reference: &CredentialRef| {
            counts.get(reference).copied().unwrap_or(0)
                - usize::from(config.proxy_auth_credential_ref.as_ref() == Some(reference))
        };
        let reference = if non_proxy_consumers(&current_reference) > 0 {
            if non_proxy_consumers(&canonical) > 0 {
                return Err(ConfigStoreError::Validation(
                    "proxy auth credential reference is shared and the canonical reference is occupied"
                        .to_string(),
                ));
            }
            canonical
        } else {
            current_reference
        };
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for proxy auth update".to_string(),
            ));
        }
        let changed = if let Some(secret) = secret.as_deref() {
            let ciphertext = crate::encryption::encrypt(secret).map_err(|_| {
                ConfigStoreError::Validation("credential encryption failed".to_string())
            })?;
            document.entries.insert(
                reference.clone(),
                CredentialEntry {
                    ciphertext,
                    source: CredentialSource::User,
                    updated_at: Utc::now(),
                    key_version: ENCRYPTION_KEY_VERSION,
                    migration_generation: None,
                },
            );
            true
        } else {
            document.entries.remove(&reference).is_some()
        };
        config.proxy_auth_credential_ref = Some(reference.clone());
        config.proxy_auth_encrypted = None;
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        let required_refs = if secret.is_some() {
            vec![reference.clone()]
        } else {
            Vec::new()
        };
        ensure_required_entries(&document, &required_refs)?;
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
        Ok(PreparedProviderCredentialUpdate {
            bytes,
            expected_revision: health.revision,
            revision,
            touched_refs: vec![reference],
            required_refs,
        })
    }

    /// Prepare an exact env credential update. `env_intents` is the complete
    /// set of names explicitly upserted, replaced, converted, or deleted by
    /// the caller; untouched entries and their custom legacy references are
    /// never rewritten.
    pub(crate) fn prepare_env_var_intents(
        &self,
        config: &mut crate::Config,
        env_intents: &BTreeSet<String>,
        persisted_refs: &BTreeMap<String, CredentialRef>,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        if env_intents.is_empty() {
            return Ok(None);
        }
        let mut seen = BTreeSet::new();
        if config
            .env_vars
            .iter()
            .any(|entry| !seen.insert(entry.name.clone()))
        {
            return Err(ConfigStoreError::Validation(
                "environment variable names must be unique".to_string(),
            ));
        }
        let candidate_counts = config_credential_ref_counts(config)?;
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for env update".to_string(),
            ));
        }
        let mut touched_refs = BTreeSet::new();
        let mut required_refs = BTreeSet::new();
        let mut changed = false;
        for name in env_intents {
            let existing_ref = persisted_refs.get(name).cloned();
            let canonical = crate::credential_ref("env", name, "value")?;
            let entry_index = config.env_vars.iter().position(|entry| &entry.name == name);
            let candidate_ref =
                entry_index.and_then(|index| config.env_vars[index].credential_ref.clone());
            if let Some(candidate_ref) = candidate_ref.as_ref() {
                if Some(candidate_ref) != existing_ref.as_ref() {
                    return Err(ConfigStoreError::Validation(
                        "env credential reference is server-managed".to_string(),
                    ));
                }
            }
            let reference = existing_ref.clone().unwrap_or(canonical);
            touched_refs.insert(reference.clone());

            let self_consumer = usize::from(candidate_ref.as_ref() == Some(&reference));
            let retained_consumers = candidate_counts
                .get(&reference)
                .copied()
                .unwrap_or(0)
                .saturating_sub(self_consumer);
            let secret = entry_index
                .filter(|index| config.env_vars[*index].secret)
                .map(|index| config.env_vars[index].value.clone())
                .filter(|value| !value.is_empty());
            let keep_existing = entry_index.is_some_and(|index| {
                let entry = &config.env_vars[index];
                entry.secret && entry.configured && entry.value.is_empty()
            });
            if existing_ref.is_none()
                && (document.entries.contains_key(&reference) || retained_consumers > 0)
            {
                return Err(ConfigStoreError::Validation(
                    "canonical env credential reference is already in use".to_string(),
                ));
            }
            if keep_existing {
                if !document.entries.contains_key(&reference) {
                    return Err(ConfigStoreError::Validation(
                        "configured env credential is unavailable".to_string(),
                    ));
                }
                required_refs.insert(reference.clone());
            } else if let Some(secret) = secret.as_deref() {
                if crate::patch::is_masked_api_key(secret) {
                    return Err(ConfigStoreError::Validation(
                        "env credential value must not be a mask".to_string(),
                    ));
                }
                let ciphertext = crate::encryption::encrypt(secret).map_err(|_| {
                    ConfigStoreError::Validation("credential encryption failed".to_string())
                })?;
                document.entries.insert(
                    reference.clone(),
                    CredentialEntry {
                        ciphertext,
                        source: CredentialSource::User,
                        updated_at: Utc::now(),
                        key_version: ENCRYPTION_KEY_VERSION,
                        migration_generation: None,
                    },
                );
                changed = true;
                required_refs.insert(reference.clone());
            } else if retained_consumers == 0 {
                changed |= document.entries.remove(&reference).is_some();
            }

            if let Some(index) = entry_index {
                let entry = &mut config.env_vars[index];
                entry.value_encrypted = None;
                if entry.secret {
                    entry.credential_ref = Some(reference.clone());
                    entry.configured = keep_existing || secret.is_some();
                } else {
                    entry.credential_ref = None;
                    entry.configured = !entry.value.is_empty();
                }
            }
        }
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(
            &document,
            &required_refs.iter().cloned().collect::<Vec<_>>(),
        )?;
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
            revision,
            touched_refs: touched_refs.into_iter().collect(),
            required_refs: required_refs.into_iter().collect(),
        }))
    }

    /// Prepare an exact notification update. Metadata for both channels is
    /// committed with the credential document; only channels named in
    /// `secret_intents` may replace or clear credential material.
    pub(crate) fn prepare_notification_intents(
        &self,
        config: &mut crate::Config,
        secret_intents: &BTreeSet<String>,
        persisted_refs: &BTreeMap<String, CredentialRef>,
        reset_domain: bool,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        if !secret_intents
            .iter()
            .all(|channel| matches!(channel.as_str(), "ntfy" | "bark"))
        {
            return Err(ConfigStoreError::Validation(
                "notification credential intent is invalid".to_string(),
            ));
        }
        let candidate_counts = config_credential_ref_counts(config)?;
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for notification update".to_string(),
            ));
        }
        let mut touched_refs = BTreeSet::new();
        let mut required_refs = BTreeSet::new();
        let mut changed = false;
        for channel in ["ntfy", "bark"] {
            let existing_ref = persisted_refs.get(channel).cloned();
            let canonical = credential_ref(
                "notification",
                channel,
                if channel == "ntfy" {
                    "token"
                } else {
                    "device_key"
                },
            )?;
            let (candidate_ref, secret, configured) = if channel == "ntfy" {
                (
                    config.notifications.ntfy.credential_ref.clone(),
                    config.notifications.ntfy.token.clone(),
                    config.notifications.ntfy.configured,
                )
            } else {
                (
                    config.notifications.bark.credential_ref.clone(),
                    config.notifications.bark.device_key.clone(),
                    config.notifications.bark.configured,
                )
            };
            if let Some(candidate_ref) = candidate_ref.as_ref() {
                if Some(candidate_ref) != existing_ref.as_ref() {
                    return Err(ConfigStoreError::Validation(
                        "notification credential reference is server-managed".to_string(),
                    ));
                }
            }
            let binds_reference =
                existing_ref.is_some() || secret_intents.contains(channel) || configured;
            if !binds_reference {
                if channel == "ntfy" {
                    config.notifications.ntfy.credential_ref = None;
                    config.notifications.ntfy.token_encrypted = None;
                    config.notifications.ntfy.configured = false;
                } else {
                    config.notifications.bark.credential_ref = None;
                    config.notifications.bark.device_key_encrypted = None;
                    config.notifications.bark.configured = false;
                }
                continue;
            }
            let reference = existing_ref.clone().unwrap_or(canonical);
            if existing_ref.is_none()
                && (document.entries.contains_key(&reference)
                    || candidate_counts.get(&reference).copied().unwrap_or(0) > 0)
            {
                return Err(ConfigStoreError::Validation(
                    "canonical notification credential reference is already in use".to_string(),
                ));
            }
            if secret_intents.contains(channel) {
                touched_refs.insert(reference.clone());
                let value = secret
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                if let Some(value) = value {
                    if crate::patch::is_masked_api_key(value) {
                        return Err(ConfigStoreError::Validation(
                            "notification credential value must not be a mask".to_string(),
                        ));
                    }
                    let ciphertext = crate::encryption::encrypt(value).map_err(|_| {
                        ConfigStoreError::Validation("credential encryption failed".to_string())
                    })?;
                    document.entries.insert(
                        reference.clone(),
                        CredentialEntry {
                            ciphertext,
                            source: CredentialSource::User,
                            updated_at: Utc::now(),
                            key_version: ENCRYPTION_KEY_VERSION,
                            migration_generation: None,
                        },
                    );
                    changed = true;
                } else if !configured {
                    let self_consumers = usize::from(candidate_ref.as_ref() == Some(&reference));
                    let other_consumers = candidate_counts
                        .get(&reference)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(self_consumers);
                    if other_consumers == 0 {
                        changed |= document.entries.remove(&reference).is_some();
                    }
                }
            }
            let configured = if secret_intents.contains(channel) {
                secret
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    || configured
            } else {
                configured
            };
            if configured {
                required_refs.insert(reference.clone());
            }
            if channel == "ntfy" {
                config.notifications.ntfy.credential_ref = (!reset_domain).then_some(reference);
                config.notifications.ntfy.token_encrypted = None;
                config.notifications.ntfy.configured = configured;
            } else {
                config.notifications.bark.credential_ref = (!reset_domain).then_some(reference);
                config.notifications.bark.device_key_encrypted = None;
                config.notifications.bark.configured = configured;
            }
        }
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(
            &document,
            &required_refs.iter().cloned().collect::<Vec<_>>(),
        )?;
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
        let touched_refs = touched_refs.into_iter().collect::<Vec<_>>();
        let required_refs = required_refs
            .into_iter()
            .filter(|reference| touched_refs.contains(reference))
            .collect();
        Ok(Some(PreparedProviderCredentialUpdate {
            bytes,
            expected_revision: health.revision,
            revision,
            touched_refs,
            required_refs,
        }))
    }

    /// Prepare a complete connect metadata replacement plus the explicitly
    /// touched token/app-secret values. Platform ids are the durable identity;
    /// storage refs/configured flags are server-owned and are reconstructed
    /// from the committed connect section rather than trusted from a client.
    pub(crate) fn prepare_connect_intents(
        &self,
        config: &mut crate::Config,
        secret_intents: &crate::patch::ConnectSecretIntents,
        persisted_refs: &PersistedConnectCredentialRefs,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        let mut ids = BTreeSet::new();
        if config.connect.platforms.iter().any(|platform| {
            platform
                .id
                .as_deref()
                .is_none_or(|id| id.trim().is_empty() || !ids.insert(id.to_string()))
        }) {
            return Err(ConfigStoreError::Validation(
                "connect platform ids must be nonempty and unique".to_string(),
            ));
        }
        if secret_intents
            .token
            .iter()
            .chain(secret_intents.app_secret.iter())
            .any(|index| *index >= config.connect.platforms.len())
        {
            return Err(ConfigStoreError::Validation(
                "connect credential intent is invalid".to_string(),
            ));
        }

        let candidate_counts = config_credential_ref_counts(config)?;
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for connect update".to_string(),
            ));
        }
        let mut touched_refs = BTreeSet::new();
        let mut required_refs = BTreeSet::new();
        let mut changed = false;

        // A full-array connect replacement deletes any platform id absent from
        // the candidate. Clear its credentials unless another durable consumer
        // still references the same (legacy/shared) ref.
        for (id, (token_ref, app_secret_ref)) in persisted_refs {
            if ids.contains(id) {
                continue;
            }
            for reference in token_ref.iter().chain(app_secret_ref.iter()) {
                touched_refs.insert(reference.clone());
                if candidate_counts.get(reference).copied().unwrap_or(0) == 0 {
                    changed |= document.entries.remove(reference).is_some();
                }
            }
        }

        for (index, platform) in config.connect.platforms.iter_mut().enumerate() {
            let id = platform
                .id
                .as_deref()
                .expect("validated connect platform id")
                .to_string();
            let persisted = persisted_refs.get(&id).cloned().unwrap_or_default();
            let canonical_token = crate::credential_ref("connect", &id, "token")?;
            let canonical_app_secret = crate::credential_ref("connect", &id, "app_secret")?;

            for (candidate, existing) in [
                (platform.token_credential_ref.as_ref(), persisted.0.as_ref()),
                (
                    platform.app_secret_credential_ref.as_ref(),
                    persisted.1.as_ref(),
                ),
            ] {
                if candidate.is_some() && candidate != existing {
                    return Err(ConfigStoreError::Validation(
                        "connect credential references are server-managed".to_string(),
                    ));
                }
            }

            let fields = [
                (
                    secret_intents.token.contains(&index),
                    &mut platform.token,
                    &mut platform.token_encrypted,
                    &mut platform.token_credential_ref,
                    &mut platform.token_configured,
                    persisted.0,
                    canonical_token,
                ),
                (
                    secret_intents.app_secret.contains(&index),
                    &mut platform.app_secret,
                    &mut platform.app_secret_encrypted,
                    &mut platform.app_secret_credential_ref,
                    &mut platform.app_secret_configured,
                    persisted.1,
                    canonical_app_secret,
                ),
            ];

            for (
                intent,
                plaintext,
                ciphertext,
                metadata_ref,
                configured,
                existing_ref,
                canonical,
            ) in fields
            {
                let reference = existing_ref.clone().unwrap_or(canonical);
                let self_consumer = usize::from(metadata_ref.as_ref() == Some(&reference));
                let other_consumers = candidate_counts
                    .get(&reference)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(self_consumer);
                if existing_ref.is_none()
                    && intent
                    && (document.entries.contains_key(&reference) || other_consumers > 0)
                {
                    return Err(ConfigStoreError::Validation(
                        "canonical connect credential reference is already in use".to_string(),
                    ));
                }

                if intent {
                    touched_refs.insert(reference.clone());
                    let secret = plaintext
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty());
                    if let Some(secret) = secret {
                        if crate::patch::is_masked_api_key(secret) {
                            return Err(ConfigStoreError::Validation(
                                "connect credential value must not be a mask".to_string(),
                            ));
                        }
                        let encrypted = crate::encryption::encrypt(secret).map_err(|_| {
                            ConfigStoreError::Validation("credential encryption failed".to_string())
                        })?;
                        document.entries.insert(
                            reference.clone(),
                            CredentialEntry {
                                ciphertext: encrypted,
                                source: CredentialSource::User,
                                updated_at: Utc::now(),
                                key_version: ENCRYPTION_KEY_VERSION,
                                migration_generation: None,
                            },
                        );
                        changed = true;
                        *metadata_ref = Some(reference.clone());
                        *configured = true;
                        required_refs.insert(reference);
                    } else {
                        if other_consumers == 0 {
                            changed |= document.entries.remove(&reference).is_some();
                        }
                        *metadata_ref = None;
                        *configured = false;
                    }
                } else if let Some(existing_ref) = existing_ref {
                    if !document.entries.contains_key(&existing_ref) {
                        return Err(ConfigStoreError::Validation(
                            "configured connect credential is unavailable".to_string(),
                        ));
                    }
                    *metadata_ref = Some(existing_ref.clone());
                    *configured = true;
                    required_refs.insert(existing_ref);
                } else {
                    *metadata_ref = None;
                    *configured = false;
                }
                *plaintext = None;
                *ciphertext = None;
            }
        }

        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(
            &document,
            &required_refs.iter().cloned().collect::<Vec<_>>(),
        )?;
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
        let touched_refs = touched_refs.into_iter().collect::<Vec<_>>();
        let required_refs = required_refs
            .into_iter()
            .filter(|reference| touched_refs.contains(reference))
            .collect();
        Ok(Some(PreparedProviderCredentialUpdate {
            bytes,
            expected_revision: health.revision,
            revision,
            touched_refs,
            required_refs,
        }))
    }

    /// Prepare access-control password/device verifier mutations. Verifiers are
    /// encoded records in the encrypted store; ordinary access-control config
    /// retains only canonical references and configured metadata.
    pub(crate) fn prepare_access_control_intents(
        &self,
        config: &mut crate::Config,
        password_intent: bool,
        device_intents: &BTreeSet<String>,
        persisted: &PersistedAccessCredentialRefs,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        let candidate_counts = config_credential_ref_counts(config)?;
        let access = config.access_control.get_or_insert_with(Default::default);
        let mut device_ids = BTreeSet::new();
        if access.devices.iter().any(|device| {
            device.device_id.trim().is_empty() || !device_ids.insert(device.device_id.clone())
        }) {
            return Err(ConfigStoreError::Validation(
                "access-control device ids must be nonempty and unique".to_string(),
            ));
        }
        if device_intents
            .iter()
            .any(|id| !device_ids.contains(id) && !persisted.devices.contains_key(id))
        {
            return Err(ConfigStoreError::Validation(
                "access-control credential intent is invalid".to_string(),
            ));
        }
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for access-control update".to_string(),
            ));
        }
        let mut touched_refs = BTreeSet::new();
        let mut required_refs = BTreeSet::new();
        let mut changed = false;

        let password_ref = persisted
            .password
            .clone()
            .unwrap_or(crate::config_crypto::access_password_credential_ref()?);
        if access.password_credential_ref.is_some()
            && access.password_credential_ref.as_ref() != persisted.password.as_ref()
        {
            return Err(ConfigStoreError::Validation(
                "access-control password credential reference is server-managed".to_string(),
            ));
        }
        let password_intent = password_intent
            || (persisted.password.is_none()
                && access.password_hash.is_some()
                && access.password_salt.is_some());
        if password_intent {
            touched_refs.insert(password_ref.clone());
            match (&access.password_hash, &access.password_salt) {
                (Some(hash), Some(salt)) => {
                    let encoded = crate::config_crypto::encode_access_verifier(hash, salt)?;
                    let same = document.entries.get(&password_ref).is_some_and(|entry| {
                        crate::encryption::decrypt(&entry.ciphertext)
                            .is_ok_and(|current| current == encoded)
                    });
                    if !same {
                        let ciphertext = crate::encryption::encrypt(&encoded).map_err(|_| {
                            ConfigStoreError::Validation("credential encryption failed".to_string())
                        })?;
                        document.entries.insert(
                            password_ref.clone(),
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
                    access.password_credential_ref = Some(password_ref.clone());
                    access.password_configured = true;
                    required_refs.insert(password_ref.clone());
                }
                (None, None) if !access.password_enabled => {
                    let self_consumers =
                        usize::from(access.password_credential_ref.as_ref() == Some(&password_ref));
                    if candidate_counts
                        .get(&password_ref)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(self_consumers)
                        == 0
                    {
                        changed |= document.entries.remove(&password_ref).is_some();
                    }
                    access.password_credential_ref = None;
                    access.password_configured = false;
                }
                _ => {
                    return Err(ConfigStoreError::Validation(
                        "access-control password verifier is incomplete".to_string(),
                    ));
                }
            }
        } else if let Some(reference) = persisted.password.as_ref() {
            if !document.entries.contains_key(reference) {
                return Err(ConfigStoreError::Validation(
                    "configured access-control password verifier is unavailable".to_string(),
                ));
            }
            access.password_credential_ref = Some(reference.clone());
            access.password_configured = true;
            required_refs.insert(reference.clone());
        } else {
            access.password_credential_ref = None;
            access.password_configured = false;
        }
        access.password_hash = None;
        access.password_salt = None;

        for removed_id in persisted
            .devices
            .keys()
            .filter(|id| !device_ids.contains(*id))
        {
            let reference = persisted.devices.get(removed_id).expect("known access ref");
            touched_refs.insert(reference.clone());
            if candidate_counts.get(reference).copied().unwrap_or(0) == 0 {
                changed |= document.entries.remove(reference).is_some();
            }
        }
        for device in &mut access.devices {
            let existing = persisted.devices.get(&device.device_id).cloned();
            if device.token_credential_ref.is_some()
                && device.token_credential_ref.as_ref() != existing.as_ref()
            {
                return Err(ConfigStoreError::Validation(
                    "access-control device credential reference is server-managed".to_string(),
                ));
            }
            let reference =
                existing
                    .clone()
                    .unwrap_or(crate::config_crypto::access_device_credential_ref(
                        &device.device_id,
                    )?);
            let verifier_intent = device_intents.contains(&device.device_id)
                || (existing.is_none()
                    && !device.token_hash.is_empty()
                    && !device.token_salt.is_empty());
            if verifier_intent {
                touched_refs.insert(reference.clone());
                if device.token_hash.is_empty() || device.token_salt.is_empty() {
                    return Err(ConfigStoreError::Validation(
                        "access-control device verifier is incomplete".to_string(),
                    ));
                }
                if existing.is_none()
                    && (document.entries.contains_key(&reference)
                        || candidate_counts.get(&reference).copied().unwrap_or(0) > 0)
                {
                    return Err(ConfigStoreError::Validation(
                        "canonical access-control credential reference is already in use"
                            .to_string(),
                    ));
                }
                let encoded = crate::config_crypto::encode_access_verifier(
                    &device.token_hash,
                    &device.token_salt,
                )?;
                let same = document.entries.get(&reference).is_some_and(|entry| {
                    crate::encryption::decrypt(&entry.ciphertext)
                        .is_ok_and(|current| current == encoded)
                });
                if !same {
                    let ciphertext = crate::encryption::encrypt(&encoded).map_err(|_| {
                        ConfigStoreError::Validation("credential encryption failed".to_string())
                    })?;
                    document.entries.insert(
                        reference.clone(),
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
            } else if !document.entries.contains_key(&reference) {
                return Err(ConfigStoreError::Validation(
                    "configured access-control device verifier is unavailable".to_string(),
                ));
            }
            device.token_credential_ref = Some(reference.clone());
            device.token_configured = true;
            device.token_hash.clear();
            device.token_salt.clear();
            required_refs.insert(reference);
        }

        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(
            &document,
            &required_refs.iter().cloned().collect::<Vec<_>>(),
        )?;
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
        let touched_refs = touched_refs.into_iter().collect::<Vec<_>>();
        Ok(Some(PreparedProviderCredentialUpdate {
            bytes,
            expected_revision: health.revision,
            revision,
            touched_refs: touched_refs.clone(),
            required_refs: required_refs
                .into_iter()
                .filter(|reference| touched_refs.contains(reference))
                .collect(),
        }))
    }

    /// Prepare one or more node credential mutations without publishing them.
    /// The caller commits the returned credential envelope together with the
    /// narrowed `cluster_fabric` root domain through the exact transaction
    /// manifest. Empty/masked required secrets keep an existing value;
    /// an empty private-key passphrase is an explicit clear.
    pub(crate) fn prepare_cluster_node_intents(
        &self,
        config: &mut crate::Config,
        node_intents: &BTreeSet<String>,
        persisted_refs: &BTreeMap<String, crate::ClusterNodeCredentialRefs>,
    ) -> ConfigStoreResult<Option<PreparedProviderCredentialUpdate>> {
        if node_intents.is_empty() {
            return Ok(None);
        }
        let mut node_ids = BTreeSet::new();
        if config
            .cluster_fabric
            .nodes
            .iter()
            .any(|node| node.id.trim().is_empty() || !node_ids.insert(node.id.clone()))
        {
            return Err(ConfigStoreError::Validation(
                "cluster node ids must be nonempty and unique".to_string(),
            ));
        }
        let (mut document, health) = self.load_document_with_health()?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for cluster update".to_string(),
            ));
        }
        let mut touched_refs = BTreeSet::new();
        let mut required_refs = BTreeSet::new();
        let mut changed = false;

        for node_id in node_intents {
            let password_ref = crate::cluster_password_credential_ref(node_id)?;
            let private_key_ref = crate::cluster_private_key_credential_ref(node_id)?;
            let passphrase_ref = crate::cluster_passphrase_credential_ref(node_id)?;
            let persisted = persisted_refs.get(node_id).cloned().unwrap_or_default();
            for (actual, canonical) in [
                (persisted.password_credential_ref.as_ref(), &password_ref),
                (
                    persisted.private_key_credential_ref.as_ref(),
                    &private_key_ref,
                ),
                (
                    persisted.passphrase_credential_ref.as_ref(),
                    &passphrase_ref,
                ),
            ] {
                if actual.is_some_and(|actual| actual != canonical) {
                    return Err(ConfigStoreError::Validation(
                        "cluster credential reference is not canonical".to_string(),
                    ));
                }
            }
            if let Some(candidate) = config.cluster_fabric.credential_refs.get(node_id) {
                if candidate != &persisted {
                    return Err(ConfigStoreError::Validation(
                        "cluster credential references are server-managed".to_string(),
                    ));
                }
            }

            enum Action {
                Keep,
                Replace(String),
                Clear,
            }
            let (password, private_key, passphrase) = match config
                .cluster_fabric
                .node(node_id)
                .map(|node| &node.placement)
            {
                Some(crate::NodePlacement::Ssh(target)) => match &target.auth {
                    crate::SshAuth::SystemSshConfig => {
                        (Action::Clear, Action::Clear, Action::Clear)
                    }
                    crate::SshAuth::Password { password, .. } => {
                        let password =
                            if password.is_empty() || crate::patch::is_masked_api_key(password) {
                                if persisted.password_configured {
                                    Action::Keep
                                } else {
                                    return Err(ConfigStoreError::Validation(
                                        "SSH password is required".to_string(),
                                    ));
                                }
                            } else {
                                Action::Replace(password.clone())
                            };
                        (password, Action::Clear, Action::Clear)
                    }
                    crate::SshAuth::PrivateKey {
                        private_key,
                        private_key_path,
                        passphrase,
                        ..
                    } => {
                        let private_key = if private_key.is_empty()
                            || crate::patch::is_masked_api_key(private_key)
                        {
                            if persisted.private_key_configured {
                                Action::Keep
                            } else if private_key_path
                                .as_deref()
                                .is_some_and(|path| !path.trim().is_empty())
                            {
                                Action::Clear
                            } else {
                                return Err(ConfigStoreError::Validation(
                                    "an inline private key or private key path is required"
                                        .to_string(),
                                ));
                            }
                        } else {
                            Action::Replace(private_key.clone())
                        };
                        let passphrase = if crate::patch::is_masked_api_key(passphrase) {
                            if persisted.passphrase_configured {
                                Action::Keep
                            } else {
                                return Err(ConfigStoreError::Validation(
                                    "SSH passphrase mask has no stored value".to_string(),
                                ));
                            }
                        } else if passphrase.is_empty() {
                            Action::Clear
                        } else {
                            Action::Replace(passphrase.clone())
                        };
                        (Action::Clear, private_key, passphrase)
                    }
                },
                Some(crate::NodePlacement::Local) | None => {
                    (Action::Clear, Action::Clear, Action::Clear)
                }
            };

            let mut metadata = crate::ClusterNodeCredentialRefs::default();
            for (action, canonical, existing, reference_slot, configured_slot) in [
                (
                    password,
                    password_ref,
                    persisted.password_credential_ref,
                    &mut metadata.password_credential_ref,
                    &mut metadata.password_configured,
                ),
                (
                    private_key,
                    private_key_ref,
                    persisted.private_key_credential_ref,
                    &mut metadata.private_key_credential_ref,
                    &mut metadata.private_key_configured,
                ),
                (
                    passphrase,
                    passphrase_ref,
                    persisted.passphrase_credential_ref,
                    &mut metadata.passphrase_credential_ref,
                    &mut metadata.passphrase_configured,
                ),
            ] {
                let reference = existing.clone().unwrap_or(canonical);
                match action {
                    Action::Keep => {
                        if !document.entries.contains_key(&reference) {
                            return Err(ConfigStoreError::Validation(
                                "configured cluster credential is unavailable".to_string(),
                            ));
                        }
                        touched_refs.insert(reference.clone());
                        required_refs.insert(reference.clone());
                        *reference_slot = Some(reference);
                        *configured_slot = true;
                    }
                    Action::Replace(value) => {
                        if existing.is_none() && document.entries.contains_key(&reference) {
                            return Err(ConfigStoreError::Validation(
                                "canonical cluster credential reference is already in use"
                                    .to_string(),
                            ));
                        }
                        let same_value = document.entries.get(&reference).is_some_and(|entry| {
                            crate::encryption::decrypt(&entry.ciphertext)
                                .is_ok_and(|current| current == value)
                        });
                        if !same_value {
                            let ciphertext = crate::encryption::encrypt(&value).map_err(|_| {
                                ConfigStoreError::Validation(
                                    "credential encryption failed".to_string(),
                                )
                            })?;
                            document.entries.insert(
                                reference.clone(),
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
                        touched_refs.insert(reference.clone());
                        required_refs.insert(reference.clone());
                        *reference_slot = Some(reference);
                        *configured_slot = true;
                    }
                    Action::Clear => {
                        if let Some(reference) = existing {
                            touched_refs.insert(reference.clone());
                            changed |= document.entries.remove(&reference).is_some();
                        }
                    }
                }
            }
            if metadata.is_empty() {
                config.cluster_fabric.credential_refs.remove(node_id);
            } else {
                config
                    .cluster_fabric
                    .credential_refs
                    .insert(node_id.clone(), metadata);
            }
            if let Some(node) = config.cluster_fabric.node_mut(node_id) {
                if let crate::NodePlacement::Ssh(target) = &mut node.placement {
                    match &mut target.auth {
                        crate::SshAuth::SystemSshConfig => {}
                        crate::SshAuth::Password {
                            password_encrypted, ..
                        } => *password_encrypted = None,
                        crate::SshAuth::PrivateKey {
                            private_key_encrypted,
                            passphrase_encrypted,
                            ..
                        } => {
                            *private_key_encrypted = None;
                            *passphrase_encrypted = None;
                        }
                    }
                }
            }
        }

        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        ensure_required_entries(
            &document,
            &required_refs.iter().cloned().collect::<Vec<_>>(),
        )?;
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
            revision,
            touched_refs: touched_refs.into_iter().collect(),
            required_refs: required_refs.into_iter().collect(),
        }))
    }

    pub fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConfigStoreResult<Option<SecretValue>> {
        self.with_transaction_lock(|| self.resolve_unchecked(credential_ref))
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
        let (document, health) = self.load_document_with_health()?;
        Self::prepare_migration_from_document(secrets, document, health)
    }

    /// Build the same migration candidate from already-captured durable bytes
    /// without opening AtomicJsonStore's lock file. Facade security preflights
    /// use this to remain byte-for-byte zero-write.
    pub(crate) fn prepare_migration_from_snapshot(
        primary: Option<&[u8]>,
        backup: Option<&[u8]>,
        secrets: Vec<(CredentialRef, String, u64)>,
    ) -> ConfigStoreResult<PreparedCredentialMigration> {
        let (document, health) = Self::migration_document_from_snapshot(primary, backup)?;
        Self::prepare_migration_from_document(secrets, document, health)
    }

    pub(crate) fn migration_sources_from_snapshot(
        primary: Option<&[u8]>,
        backup: Option<&[u8]>,
    ) -> ConfigStoreResult<BTreeMap<CredentialRef, CredentialSource>> {
        let (document, health) = Self::migration_document_from_snapshot(primary, backup)?;
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for migration".to_string(),
            ));
        }
        Ok(document
            .entries
            .into_iter()
            .map(|(reference, entry)| (reference, entry.source))
            .collect())
    }

    fn migration_document_from_snapshot(
        primary: Option<&[u8]>,
        backup: Option<&[u8]>,
    ) -> ConfigStoreResult<(CredentialDocument, CredentialStoreHealth)> {
        if let Some(primary) = primary {
            match Self::parse_transaction_document(primary, false) {
                Ok(envelope) => {
                    return Ok((
                        envelope.data,
                        CredentialStoreHealth::committed(envelope.revision),
                    ));
                }
                Err(primary_error) => {
                    if backup.is_some_and(|backup| {
                        Self::parse_transaction_document(backup, false).is_ok()
                    }) {
                        return Err(ConfigStoreError::Validation(
                            "credential document is unavailable for migration".to_string(),
                        ));
                    }
                    return Err(primary_error);
                }
            }
        }
        // AtomicJsonStore treats a missing primary as an empty store and does
        // not resurrect a backup on its own. Match that exact authority rule;
        // backups are consulted only when a present primary is invalid.
        let _ = backup;
        Ok((
            CredentialDocument::default(),
            CredentialStoreHealth {
                revision: 0,
                status: SectionStatus::Missing,
                source: SectionSourceKind::Default,
                last_error: None,
            },
        ))
    }

    fn prepare_migration_from_document(
        secrets: Vec<(CredentialRef, String, u64)>,
        mut document: CredentialDocument,
        health: CredentialStoreHealth,
    ) -> ConfigStoreResult<PreparedCredentialMigration> {
        let mut unique_secrets = BTreeMap::<CredentialRef, (String, u64)>::new();
        for (credential_ref, secret, input_generation) in secrets {
            if secret.trim().is_empty() || crate::patch::is_masked_api_key(&secret) {
                return Err(ConfigStoreError::Validation(
                    "legacy credential value is invalid".to_string(),
                ));
            }
            match unique_secrets.get_mut(&credential_ref) {
                Some((existing, generation)) if existing == &secret => {
                    *generation = (*generation).max(input_generation);
                }
                Some(_) => {
                    return Err(ConfigStoreError::Validation(
                        "conflicting legacy credentials share a credential reference".to_string(),
                    ));
                }
                None => {
                    unique_secrets.insert(credential_ref, (secret, input_generation));
                }
            }
        }
        if health.status == SectionStatus::Degraded {
            return Err(ConfigStoreError::Validation(
                "credential document is unavailable for migration".to_string(),
            ));
        }
        let mut added = 0;
        let mut changed = false;
        for (credential_ref, (secret, input_generation)) in unique_secrets {
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

    /// Materialize the empty credential section when a full modular-layout
    /// transaction needs every section to have a durable document. The caller
    /// holds the shared credential migration lock; ordinary status reads keep
    /// treating an absent store as the revision-zero default.
    pub(crate) fn ensure_initialized_for_section_layout(&self) -> ConfigStoreResult<()> {
        let (document, health) = self.load_document_with_health()?;
        if health.status != SectionStatus::Missing {
            return Ok(());
        }
        validate_document(&document).map_err(ConfigStoreError::Validation)?;
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": 0,
            "data": document,
        }))?;
        if crate::config_store::AtomicFileStore::new(self.path())
            .sensitive(true)
            .write_bytes_if_state(None, &bytes)?
        {
            Ok(())
        } else {
            // Another credential writer won after the missing-state read. Its
            // durable document is authoritative and already initialized.
            Ok(())
        }
    }

    /// Three-way merge an exact provider transaction after another credential
    /// writer won its CAS. Unrelated current entries are always retained. For
    /// each touched ref, a current value different from the transaction base
    /// wins; otherwise the staged set/clear is applied.
    pub(crate) fn merge_exact_transaction_documents(
        original: &[u8],
        staged: &[u8],
        current: &[u8],
        touched_refs: &[String],
        required_refs: &[String],
        preserve_staged_revision_bump: bool,
    ) -> ConfigStoreResult<(Vec<u8>, u64, Vec<String>)> {
        let original = Self::parse_transaction_document(original, true)?;
        let staged = Self::parse_transaction_document(staged, false)?;
        let current_document = Self::parse_transaction_document(current, false)?;
        let expected_revision = current_document.revision;
        let touched_refs = parse_credential_ref_list(touched_refs)?;
        let mut required_refs = parse_credential_ref_list(required_refs)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut merged = current_document.data.clone();
        let mut changed = false;
        let staged_revision_bump =
            preserve_staged_revision_bump && staged.revision > original.revision;

        for reference in touched_refs {
            let original_entry = original.data.entries.get(&reference);
            let current_entry = current_document.data.entries.get(&reference);
            if current_entry != original_entry {
                // A later clear of this exact ref is a valid current winner.
                // Metadata may retain the stable ref (ordinary credential
                // clear already has that behavior), so runtime health degrades
                // instead of leaving the transaction permanently pending.
                if current_entry.is_none() {
                    required_refs.remove(&reference);
                }
                continue;
            }
            match staged.data.entries.get(&reference) {
                Some(entry) if current_entry != Some(entry) => {
                    merged.entries.insert(reference, entry.clone());
                    changed = true;
                }
                None if current_entry.is_some() => {
                    merged.entries.remove(&reference);
                    changed = true;
                }
                Some(_) | None => {}
            }
        }
        validate_document(&merged).map_err(ConfigStoreError::Validation)?;
        let required_refs = required_refs.into_iter().collect::<Vec<_>>();
        ensure_required_entries(&merged, &required_refs)?;
        let remaining_required = required_refs
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<Vec<_>>();
        if !changed && !staged_revision_bump {
            return Ok((current.to_vec(), expected_revision, remaining_required));
        }
        let revision = current_document.revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?;
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": revision,
            "data": merged,
        }))?;
        Ok((bytes, expected_revision, remaining_required))
    }

    /// Compensate an exact transaction without overwriting a later writer.
    /// Only a touched entry that still equals the transaction's initial
    /// staged value is restored to its immutable base value. Unrelated entries
    /// and same-ref winners are retained.
    pub(crate) fn rollback_exact_transaction_documents(
        original: &[u8],
        initial_staged: &[u8],
        current: &[u8],
        touched_refs: &[String],
    ) -> ConfigStoreResult<(Vec<u8>, u64, bool)> {
        let original = Self::parse_transaction_document(original, true)?;
        let initial_staged = Self::parse_transaction_document(initial_staged, false)?;
        let current_document = Self::parse_transaction_document(current, false)?;
        let expected_revision = current_document.revision;
        let touched_refs = parse_credential_ref_list(touched_refs)?;
        let mut rolled_back = current_document.data.clone();
        let mut changed = false;

        for reference in touched_refs {
            let staged_entry = initial_staged.data.entries.get(&reference);
            let current_entry = current_document.data.entries.get(&reference);
            if current_entry != staged_entry {
                continue;
            }
            match original.data.entries.get(&reference) {
                Some(entry) if current_entry != Some(entry) => {
                    rolled_back.entries.insert(reference, entry.clone());
                    changed = true;
                }
                None if current_entry.is_some() => {
                    rolled_back.entries.remove(&reference);
                    changed = true;
                }
                Some(_) | None => {}
            }
        }
        validate_document(&rolled_back).map_err(ConfigStoreError::Validation)?;
        if !changed {
            return Ok((current.to_vec(), expected_revision, false));
        }
        let revision = current_document.revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?;
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": revision,
            "data": rolled_back,
        }))?;
        Ok((bytes, expected_revision, true))
    }

    pub(crate) fn changed_refs_between_documents(
        original: &[u8],
        staged: &[u8],
    ) -> ConfigStoreResult<Vec<String>> {
        let original = Self::parse_transaction_document(original, true)?;
        let staged = Self::parse_transaction_document(staged, false)?;
        let refs = original
            .data
            .entries
            .keys()
            .chain(staged.data.entries.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        Ok(refs
            .into_iter()
            .filter(|reference| {
                original.data.entries.get(reference) != staged.data.entries.get(reference)
            })
            .map(|reference| reference.as_str().to_string())
            .collect())
    }

    pub(crate) fn ensure_required_refs_in_bytes(
        bytes: &[u8],
        required_refs: &[String],
    ) -> ConfigStoreResult<()> {
        let document = Self::parse_transaction_document(bytes, false)?;
        let required_refs = parse_credential_ref_list(required_refs)?;
        ensure_required_entries(&document.data, &required_refs)
    }

    /// Validate the durable credential-section envelope used by the modular
    /// layout completion attestation and return its monotonic revision.
    pub(crate) fn validate_section_document_revision(bytes: &[u8]) -> ConfigStoreResult<u64> {
        Ok(Self::parse_transaction_document(bytes, false)?.revision)
    }

    fn parse_transaction_document(
        bytes: &[u8],
        allow_empty: bool,
    ) -> ConfigStoreResult<PreparedCredentialEnvelope> {
        if bytes.is_empty() && allow_empty {
            return Ok(PreparedCredentialEnvelope {
                schema_version: CREDENTIAL_SCHEMA_VERSION,
                revision: 0,
                data: CredentialDocument::default(),
            });
        }
        let document: PreparedCredentialEnvelope = serde_json::from_slice(bytes)?;
        if document.schema_version != CREDENTIAL_SCHEMA_VERSION {
            return Err(ConfigStoreError::Validation(
                "credential document has an unsupported schema".to_string(),
            ));
        }
        validate_document(&document.data).map_err(ConfigStoreError::Validation)?;
        Ok(document)
    }

    fn load_document(&self) -> ConfigStoreResult<CredentialDocument> {
        self.load_document_with_health()
            .map(|(document, _)| document)
    }

    fn ensure_transaction_ready(&self) -> ConfigStoreResult<()> {
        crate::ensure_provider_mcp_migration_ready(self.data_dir())
    }

    fn data_dir(&self) -> &Path {
        self.path()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }

    pub(crate) fn with_transaction_lock<T>(
        &self,
        operation: impl FnOnce() -> ConfigStoreResult<T>,
    ) -> ConfigStoreResult<T> {
        crate::with_provider_mcp_migration_lock(self.data_dir(), || {
            self.ensure_transaction_ready()?;
            operation()
        })
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

fn credential_statuses(document: &CredentialDocument) -> Vec<CredentialStatus> {
    document
        .entries
        .iter()
        .map(|(credential_ref, entry)| CredentialStatus {
            credential_ref: credential_ref.clone(),
            configured: true,
            source: entry.source,
            updated_at: Some(entry.updated_at),
        })
        .collect()
}

pub(crate) fn config_credential_ref_counts(
    config: &crate::Config,
) -> ConfigStoreResult<BTreeMap<CredentialRef, usize>> {
    let mut counts = BTreeMap::<CredentialRef, usize>::new();
    let mut add = |reference: &CredentialRef| {
        *counts.entry(reference.clone()).or_default() += 1;
    };
    macro_rules! add_provider {
        ($field:ident) => {
            if let Some(reference) = config
                .providers
                .$field
                .as_ref()
                .and_then(|provider| provider.credential_ref.as_ref())
            {
                add(reference);
            }
        };
    }
    add_provider!(openai);
    add_provider!(anthropic);
    add_provider!(gemini);
    add_provider!(bodhi);
    if let Some(reference) = config.proxy_auth_credential_ref.as_ref() {
        add(reference);
    }
    for instance in config.provider_instances.values() {
        if let Some(reference) = instance.credential_ref.as_ref() {
            add(reference);
        }
    }
    for entry in &config.env_vars {
        if let Some(reference) = entry.credential_ref.as_ref() {
            add(reference);
        }
    }
    if let Some(reference) = config.notifications.ntfy.credential_ref.as_ref() {
        add(reference);
    }
    if let Some(reference) = config.notifications.bark.credential_ref.as_ref() {
        add(reference);
    }
    for platform in &config.connect.platforms {
        if let Some(reference) = platform.token_credential_ref.as_ref() {
            add(reference);
        }
        if let Some(reference) = platform.app_secret_credential_ref.as_ref() {
            add(reference);
        }
    }
    if let Some(access) = config.access_control.as_ref() {
        if let Some(reference) = access.password_credential_ref.as_ref() {
            add(reference);
        }
        for device in &access.devices {
            if let Some(reference) = device.token_credential_ref.as_ref() {
                add(reference);
            }
        }
    }
    for server in &config.mcp.servers {
        match &server.transport {
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                for raw_reference in stdio.env_credential_refs.values() {
                    add(&CredentialRef::parse(raw_reference.clone())?);
                }
            }
            bamboo_domain::mcp_config::TransportConfig::Sse(config) => {
                for raw_reference in config
                    .headers
                    .iter()
                    .filter_map(|header| header.credential_ref.as_ref())
                {
                    add(&CredentialRef::parse(raw_reference.clone())?);
                }
            }
            bamboo_domain::mcp_config::TransportConfig::StreamableHttp(config) => {
                for raw_reference in config
                    .headers
                    .iter()
                    .filter_map(|header| header.credential_ref.as_ref())
                {
                    add(&CredentialRef::parse(raw_reference.clone())?);
                }
            }
        }
    }
    Ok(counts)
}

fn parse_credential_ref_list(values: &[String]) -> ConfigStoreResult<Vec<CredentialRef>> {
    values
        .iter()
        .map(|value| CredentialRef::parse(value.clone()))
        .collect()
}

fn ensure_required_entries(
    document: &CredentialDocument,
    required_refs: &[CredentialRef],
) -> ConfigStoreResult<()> {
    for reference in required_refs {
        let entry = document.entries.get(reference).ok_or_else(|| {
            ConfigStoreError::Validation(
                "provider transaction credential is unavailable".to_string(),
            )
        })?;
        crate::encryption::decrypt(&entry.ciphertext).map_err(|_| {
            ConfigStoreError::Validation(
                "provider transaction credential is unavailable".to_string(),
            )
        })?;
    }
    Ok(())
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
    use fs2::FileExt;
    use std::fs::OpenOptions;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn pure_migration_snapshot_matches_primary_and_backup_authority_matrix() {
        let valid = serde_json::to_vec(&serde_json::json!({
            "schema_version": CREDENTIAL_SCHEMA_VERSION,
            "revision": 1,
            "data": {"entries": {}}
        }))
        .unwrap();
        let empty = b"".as_slice();
        let invalid = b"{ invalid".as_slice();

        for backup in [None, Some(empty), Some(valid.as_slice())] {
            assert!(CredentialStore::migration_sources_from_snapshot(None, backup).is_ok());
        }
        for backup in [None, Some(empty), Some(valid.as_slice())] {
            assert!(CredentialStore::migration_sources_from_snapshot(Some(empty), backup).is_err());
            assert!(
                CredentialStore::migration_sources_from_snapshot(Some(invalid), backup).is_err()
            );
            assert!(CredentialStore::migration_sources_from_snapshot(
                Some(valid.as_slice()),
                backup
            )
            .is_ok());
        }
        assert!(CredentialStore::prepare_migration_from_snapshot(
            None,
            Some(valid.as_slice()),
            Vec::new(),
        )
        .is_ok());
    }

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
    fn public_resolve_and_status_wait_out_the_manifest_commit_window() {
        let _key = crate::encryption::set_test_encryption_key([0x33; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                reference.clone(),
                "old-consistent-secret",
                CredentialSource::User,
                0,
            )
            .unwrap();

        let migration_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join(".config-credential-migration.lock"))
            .unwrap();
        migration_lock.lock_exclusive().unwrap();
        std::fs::write(
            dir.path().join("config-credential-migration.json"),
            b"manifest-commit-window",
        )
        .unwrap();
        store
            .replace_unchecked(
                reference.clone(),
                "new-transaction-secret",
                CredentialSource::User,
                1,
            )
            .unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (resolve_result_tx, resolve_result_rx) = mpsc::channel();
        let (status_result_tx, status_result_rx) = mpsc::channel();
        let resolve_store = store.clone();
        let resolve_ref = reference.clone();
        let resolve_started = started_tx.clone();
        let resolve_thread = std::thread::spawn(move || {
            let _key = crate::encryption::set_test_encryption_key([0x33; 32]);
            resolve_started.send(()).unwrap();
            let result = resolve_store
                .resolve(&resolve_ref)
                .map(|value| value.map(|secret| secret.expose().to_string()));
            resolve_result_tx.send(result).unwrap();
        });
        let status_store = store.clone();
        let status_ref = reference.clone();
        let status_thread = std::thread::spawn(move || {
            let _key = crate::encryption::set_test_encryption_key([0x33; 32]);
            started_tx.send(()).unwrap();
            let result = status_store
                .status_with_revision(&status_ref)
                .map(|(revision, status)| (revision, status.configured));
            status_result_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        started_rx.recv().unwrap();
        assert!(
            resolve_result_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "resolve must not observe a transaction member while the manifest lock is held"
        );
        assert!(
            status_result_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "status must not observe a transaction member while the manifest lock is held"
        );

        std::fs::remove_file(dir.path().join("config-credential-migration.json")).unwrap();
        migration_lock.unlock().unwrap();
        assert_eq!(
            resolve_result_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .unwrap(),
            "new-transaction-secret",
            "resolve must read only after the commit window closes"
        );
        assert_eq!(
            status_result_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap(),
            (2, true),
            "status must pair the post-transaction revision and state"
        );
        resolve_thread.join().unwrap();
        status_thread.join().unwrap();
    }

    #[test]
    fn empty_credential_parent_normalizes_to_current_directory() {
        let store = CredentialStore::open(Path::new(""));
        assert_eq!(store.data_dir(), Path::new("."));
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

    #[test]
    fn degraded_backup_revision_can_repair_replace_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x34; 32]);
        let reference = credential_ref("provider", "openai", "api_key").unwrap();

        let replace_dir = TempDir::new().unwrap();
        let replace_store = CredentialStore::open(replace_dir.path());
        replace_store
            .replace(reference.clone(), "first", CredentialSource::User, 0)
            .unwrap();
        replace_store
            .replace(reference.clone(), "second", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(replace_store.path(), b"{corrupt").unwrap();
        let (_, health) = replace_store.status_with_health(&reference).unwrap();
        assert_eq!(health.status, SectionStatus::Degraded);
        assert_eq!(health.revision, 1);
        let (revision, _) = replace_store
            .replace(
                reference.clone(),
                "repaired",
                CredentialSource::User,
                health.revision,
            )
            .unwrap();
        assert_eq!(revision, 3);
        assert_eq!(
            replace_store.resolve(&reference).unwrap().unwrap().expose(),
            "repaired"
        );
        assert!(matches!(
            replace_store.replace(reference.clone(), "stale", CredentialSource::User, 2,),
            Err(ConfigStoreError::Conflict {
                expected: 2,
                actual: 3
            })
        ));
        assert_eq!(
            replace_store.resolve(&reference).unwrap().unwrap().expose(),
            "repaired"
        );

        let clear_dir = TempDir::new().unwrap();
        let clear_store = CredentialStore::open(clear_dir.path());
        clear_store
            .replace(reference.clone(), "first", CredentialSource::User, 0)
            .unwrap();
        clear_store
            .replace(reference.clone(), "second", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(clear_store.path(), b"{corrupt").unwrap();
        let (_, health) = clear_store.status_with_health(&reference).unwrap();
        assert_eq!(health.status, SectionStatus::Degraded);
        assert_eq!(health.revision, 1);
        let (revision, status) = clear_store.clear(&reference, health.revision).unwrap();
        assert_eq!(revision, 3);
        assert!(!status.configured);
        assert!(clear_store.resolve(&reference).unwrap().is_none());
        assert!(matches!(
            clear_store.replace(reference.clone(), "stale", CredentialSource::User, 2,),
            Err(ConfigStoreError::Conflict {
                expected: 2,
                actual: 3
            })
        ));
        assert!(clear_store.resolve(&reference).unwrap().is_none());
    }
}
