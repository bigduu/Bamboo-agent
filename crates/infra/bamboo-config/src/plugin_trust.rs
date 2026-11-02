//! Publisher policy. Persist decisions, and resolve shipped defaults only at use time.
use crate::config::default_trusted_hosts;
use crate::{is_host_trusted, PluginTrustEnforcement, TrustedKey, JIANDU_PLUGIN_PUBLIC_KEY};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublisherMode {
    #[default]
    FollowDefaults,
    Explicit,
}

/// Stable publisher IDs outlive individual keys. Only reviewed application code
/// supplies this catalog; a label or a network response never grants authority.
pub fn official_plugin_publishers() -> Vec<(&'static str, TrustedKey)> {
    [
        ("jiandu", JIANDU_PLUGIN_PUBLIC_KEY),
        (
            "nova",
            "e3c429e1be50098b12c6f45737abf457189b668535875b5b3e2b4349be86ea59",
        ),
        (
            "magpie",
            "47e971c39cd93adb18cff50e097cb387df49e9c4d33b0ed62f693eabbe7fc66e",
        ),
    ]
    .into_iter()
    .map(|(id, public_key)| {
        (
            id,
            TrustedKey {
                label: format!("{id} (bigduu official)"),
                algorithm: "ed25519".into(),
                public_key: public_key.into(),
            },
        )
    })
    .collect()
}

pub fn publisher_key_id(key: &TrustedKey) -> String {
    format!(
        "{}:{}",
        key.algorithm.to_ascii_lowercase(),
        key.public_key.to_ascii_lowercase()
    )
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PluginTrustConfig {
    pub trusted_hosts: Vec<String>,
    /// In follow mode these are additional custom keys, never a materialized
    /// copy of defaults. In explicit mode this is the complete selected list.
    pub trusted_keys: Vec<TrustedKey>,
    pub enforcement: PluginTrustEnforcement,
    pub publisher_mode: PublisherMode,
    /// Stable publisher ID -> captured denied key identities. The ID denies
    /// future catalog keys; captured identities also deny retired keys even if
    /// copied into the custom list under a different label.
    pub revoked_publishers: BTreeMap<String, BTreeSet<String>>,
}

impl Default for PluginTrustConfig {
    fn default() -> Self {
        Self {
            trusted_hosts: default_trusted_hosts(),
            trusted_keys: Vec::new(),
            enforcement: PluginTrustEnforcement::default(),
            publisher_mode: PublisherMode::FollowDefaults,
            revoked_publishers: BTreeMap::new(),
        }
    }
}

// Presence matters: an old saved list (including []) is explicit. Null is an
// invalid value, never an absent list that could enable default publishers.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl<'de> Deserialize<'de> for PluginTrustConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default = "default_trusted_hosts")]
            trusted_hosts: Vec<String>,
            #[serde(default, deserialize_with = "present")]
            trusted_keys: Option<Vec<TrustedKey>>,
            #[serde(default)]
            enforcement: PluginTrustEnforcement,
            #[serde(default, deserialize_with = "present")]
            publisher_mode: Option<PublisherMode>,
            #[serde(default)]
            revoked_publishers: BTreeMap<String, BTreeSet<String>>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let mode = raw.publisher_mode.unwrap_or(if raw.trusted_keys.is_some() {
            PublisherMode::Explicit
        } else {
            PublisherMode::FollowDefaults
        });
        Ok(Self {
            trusted_hosts: raw.trusted_hosts,
            trusted_keys: raw.trusted_keys.unwrap_or_default(),
            enforcement: raw.enforcement,
            publisher_mode: mode,
            revoked_publishers: raw.revoked_publishers,
        })
    }
}

impl PluginTrustConfig {
    pub fn is_host_trusted(&self, url: &str) -> bool {
        is_host_trusted(url, &self.trusted_hosts)
    }

    pub fn enforcement_is_off(&self) -> bool {
        matches!(self.enforcement, PluginTrustEnforcement::Off)
    }

    pub fn effective_trusted_keys(&self) -> Vec<TrustedKey> {
        self.resolve_with_catalog(&official_plugin_publishers())
    }

    fn resolve_with_catalog(&self, catalog: &[(&str, TrustedKey)]) -> Vec<TrustedKey> {
        let mut denied: BTreeSet<_> = self
            .revoked_publishers
            .values()
            .flatten()
            .map(|id| id.to_ascii_lowercase())
            .collect();
        for (id, key) in catalog {
            if self.revoked_publishers.contains_key(*id) {
                denied.insert(publisher_key_id(key));
            }
        }
        let defaults = catalog
            .iter()
            .filter(|_| self.publisher_mode == PublisherMode::FollowDefaults)
            .map(|(_, key)| key);
        let mut seen = BTreeSet::new();
        defaults
            .chain(self.trusted_keys.iter())
            .filter(|key| {
                let id = publisher_key_id(key);
                !denied.contains(&id) && seen.insert(id)
            })
            .cloned()
            .collect()
    }

    pub fn set_publisher_mode(&mut self, mode: PublisherMode) {
        if self.publisher_mode == mode {
            return;
        }
        self.trusted_keys = match mode {
            PublisherMode::Explicit => self.effective_trusted_keys(),
            PublisherMode::FollowDefaults => {
                let official: BTreeSet<_> = official_plugin_publishers()
                    .iter()
                    .map(|(_, key)| publisher_key_id(key))
                    .collect();
                self.trusted_keys
                    .iter()
                    .filter(|key| !official.contains(&publisher_key_id(key)))
                    .cloned()
                    .collect()
            }
        };
        self.publisher_mode = mode;
    }

    pub fn revoke_official_publisher(&mut self, id: &str) -> bool {
        let Some((_, key)) = official_plugin_publishers()
            .into_iter()
            .find(|(name, _)| *name == id)
        else {
            return false;
        };
        let key_id = publisher_key_id(&key);
        self.revoked_publishers
            .entry(id.into())
            .or_default()
            .insert(key_id.clone());
        self.trusted_keys
            .retain(|key| publisher_key_id(key) != key_id);
        true
    }

    /// Explicit restoration clears this publisher's deny decision only. It
    /// cannot clear a different publisher's revocation of a shared key.
    pub fn trust_official_publisher(&mut self, id: &str) -> bool {
        let Some((_, key)) = official_plugin_publishers()
            .into_iter()
            .find(|(name, _)| *name == id)
        else {
            return false;
        };
        if let Some(denied) = self.revoked_publishers.remove(id) {
            let denied: BTreeSet<_> = denied
                .into_iter()
                .map(|key| key.to_ascii_lowercase())
                .collect();
            // Restore only the reviewed current key. A retired captured key
            // may have been copied into the custom list while it was denied;
            // clearing the record must not silently revive those bytes too.
            self.trusted_keys
                .retain(|key| !denied.contains(&publisher_key_id(key)));
        }
        if self.publisher_mode == PublisherMode::Explicit
            && !self
                .trusted_keys
                .iter()
                .any(|existing| publisher_key_id(existing) == publisher_key_id(&key))
        {
            self.trusted_keys.push(key);
        }
        true
    }

    /// Prepare either a partial compatibility patch or a typed policy
    /// replacement. Old writers cannot accidentally clear new deny records or
    /// keep inheritance enabled when explicitly clearing their key list.
    pub fn prepare_update(&self, incoming: &mut Value) -> serde_json::Result<()> {
        let mut candidate = serde_json::to_value(self)?;
        let object = incoming.as_object().ok_or_else(|| {
            <serde_json::Error as serde::de::Error>::custom("plugin_trust must be an object")
        })?;
        let mut policy = self.clone();
        if let Some(mode) = object.get("publisher_mode") {
            policy.set_publisher_mode(serde_json::from_value(mode.clone())?);
        } else if object.contains_key("trusted_keys") {
            policy.publisher_mode = PublisherMode::Explicit;
        }
        candidate["publisher_mode"] = serde_json::to_value(policy.publisher_mode)?;
        candidate["trusted_keys"] = serde_json::to_value(policy.trusted_keys)?;
        for (key, value) in object {
            candidate[key] = value.clone();
        }
        // Parse before returning: null keys/mode and unknown modes fail closed.
        // Keep the original wire representation after validation. In
        // particular, the CLI's round-trip guard must still see misspelled or
        // unknown nested fields rather than accepting them after serde drops
        // them. The owning typed writer performs canonical serialization.
        let _: Self = serde_json::from_value(candidate.clone())?;
        *incoming = candidate;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
