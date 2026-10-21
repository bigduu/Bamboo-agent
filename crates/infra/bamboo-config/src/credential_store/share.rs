//! Explicit-sensitive sharing access to a supplied encrypted document. No
//! loader, migration, environment hydration, key discovery, or file access.

use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCredentials {
    schema_version: u32,
    revision: u64,
    #[serde(deserialize_with = "strict_document")]
    data: CredentialDocument,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictEntry {
    ciphertext: String,
    source: CredentialSource,
    updated_at: DateTime<Utc>,
    key_version: u32,
    #[serde(default)]
    migration_generation: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictDocument {
    #[serde(default, deserialize_with = "unique_entries")]
    entries: BTreeMap<CredentialRef, CredentialEntry>,
}

fn strict_document<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<CredentialDocument, D::Error> {
    StrictDocument::deserialize(de).map(|data| CredentialDocument {
        entries: data.entries,
    })
}

fn unique_entries<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<BTreeMap<CredentialRef, CredentialEntry>, D::Error> {
    struct Entries;
    impl<'de> serde::de::Visitor<'de> for Entries {
        type Value = BTreeMap<CredentialRef, CredentialEntry>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unique saved credential entries")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut entries = BTreeMap::new();
            while let Some((reference, entry)) = map.next_entry::<CredentialRef, StrictEntry>()? {
                let value = CredentialEntry {
                    ciphertext: entry.ciphertext,
                    source: entry.source,
                    updated_at: entry.updated_at,
                    key_version: entry.key_version,
                    migration_generation: entry.migration_generation,
                };
                if entries.insert(reference, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate saved credential entry"));
                }
            }
            Ok(entries)
        }
    }
    de.deserialize_map(Entries)
}

pub(crate) struct ShareCredentialSnapshot {
    stored: StoredCredentials,
    original: CredentialDocument,
    original_bytes: Option<Vec<u8>>,
}

fn unavailable() -> ConfigStoreError {
    ConfigStoreError::Validation("saved sharing credentials are unavailable".to_string())
}

impl ShareCredentialSnapshot {
    pub(crate) fn from_bytes(bytes: Option<&[u8]>) -> ConfigStoreResult<Self> {
        let stored = match bytes {
            Some(bytes) => serde_json::from_slice(bytes).map_err(|_| unavailable())?,
            None => StoredCredentials {
                schema_version: CREDENTIAL_SCHEMA_VERSION,
                revision: 0,
                data: CredentialDocument::default(),
            },
        };
        if stored.schema_version != CREDENTIAL_SCHEMA_VERSION {
            return Err(unavailable());
        }
        validate_document(&stored.data).map_err(|_| unavailable())?;
        Ok(Self {
            original: stored.data.clone(),
            stored,
            original_bytes: bytes.map(ToOwned::to_owned),
        })
    }

    pub(crate) fn require_initialized_if_configured(&self) -> ConfigStoreResult<()> {
        if !self.stored.data.entries.is_empty() {
            crate::encryption::require_initialized_key().map_err(|_| unavailable())?;
        }
        Ok(())
    }

    pub(crate) fn resolve_selected(&self, reference: &CredentialRef) -> ConfigStoreResult<String> {
        let entry = self
            .stored
            .data
            .entries
            .get(reference)
            .ok_or_else(unavailable)?;
        if entry.key_version != ENCRYPTION_KEY_VERSION {
            return Err(unavailable());
        }
        crate::encryption::decrypt_initialized(&entry.ciphertext).map_err(|_| unavailable())
    }

    pub(crate) fn insert_share_secret(
        &mut self,
        reference: CredentialRef,
        value: &str,
    ) -> ConfigStoreResult<()> {
        if value.len() > 32 * 1024 || value.is_empty() || crate::patch::is_masked_api_key(value) {
            return Err(unavailable());
        }
        let ciphertext =
            crate::encryption::encrypt_initialized(value).map_err(|_| unavailable())?;
        self.stored.data.entries.insert(
            reference,
            CredentialEntry {
                ciphertext,
                source: CredentialSource::User,
                updated_at: Utc::now(),
                key_version: ENCRYPTION_KEY_VERSION,
                migration_generation: None,
            },
        );
        Ok(())
    }

    pub(crate) fn encode(mut self) -> ConfigStoreResult<Vec<u8>> {
        if self.stored.data == self.original {
            if let Some(bytes) = self.original_bytes {
                return Ok(bytes);
            }
        } else {
            self.stored.revision = self
                .stored
                .revision
                .checked_add(1)
                .ok_or_else(unavailable)?;
        }
        serde_json::to_vec_pretty(&self.stored).map_err(Into::into)
    }
}
