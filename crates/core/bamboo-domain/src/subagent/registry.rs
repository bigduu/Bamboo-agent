//! Registry that owns the merged set of subagent profiles.
//!
//! Profiles can come from multiple sources, applied in order so that later
//! sources override earlier ones by `id`:
//!
//! 1. Built-in profiles (provided by higher layers — this crate is data-only).
//! 2. User global config (e.g. `~/.bamboo/subagent_profiles.json`).
//! 3. Project-level config.
//! 4. Environment variable override file.
//!
//! Loading from disk is performed by the consumer (typically
//! `bamboo-server`); this module only provides the in-memory data structure
//! and its serde representation, plus deterministic `resolve` semantics.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::SubagentProfile;

/// Default fallback profile id used when the caller specifies an unknown
/// `subagent_type`.
pub const DEFAULT_FALLBACK_PROFILE_ID: &str = "general-purpose";

/// Error returned by registry construction.
#[derive(Debug, Error)]
pub enum SubagentProfileRegistryError {
    #[error("duplicate subagent profile id: {0}")]
    DuplicateId(String),
    #[error("fallback profile id `{0}` is not present in the registry")]
    MissingFallback(String),
}

/// On-disk JSON shape for a subagent profile config file.
///
/// ```json
/// { "profiles": [ { "id": "...", ... }, ... ] }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubagentProfileFile {
    #[serde(default)]
    pub profiles: Vec<SubagentProfile>,
}

/// In-memory registry of subagent profiles.
///
/// The registry is immutable once built. Use [`SubagentProfileRegistryBuilder`]
/// (constructed via [`SubagentProfileRegistry::builder`]) to assemble layers
/// from multiple sources.
#[derive(Debug, Clone)]
pub struct SubagentProfileRegistry {
    profiles: HashMap<String, SubagentProfile>,
    /// Insertion order preserved for stable listing in UI / tool schemas.
    order: Vec<String>,
    fallback_id: String,
}

impl SubagentProfileRegistry {
    /// Begin building a registry. The fallback id defaults to
    /// [`DEFAULT_FALLBACK_PROFILE_ID`] and can be overridden via
    /// [`SubagentProfileRegistryBuilder::fallback_id`].
    pub fn builder() -> SubagentProfileRegistryBuilder {
        SubagentProfileRegistryBuilder::default()
    }

    /// Look up a profile by exact id.
    pub fn get(&self, id: &str) -> Option<&SubagentProfile> {
        self.profiles.get(id)
    }

    /// Resolve a `subagent_type` string to a profile, falling back to the
    /// registered fallback profile (typically `general-purpose`) when the
    /// id is unknown or empty.
    ///
    /// Lookup is case-insensitive on the id.
    pub fn resolve(&self, subagent_type: &str) -> &SubagentProfile {
        let trimmed = subagent_type.trim();
        if !trimmed.is_empty() {
            // Fast path: exact match.
            if let Some(profile) = self.profiles.get(trimmed) {
                return profile;
            }
            // Case-insensitive fallback so `Researcher` matches `researcher`.
            let lower = trimmed.to_ascii_lowercase();
            for id in &self.order {
                if id.eq_ignore_ascii_case(&lower) {
                    if let Some(profile) = self.profiles.get(id) {
                        return profile;
                    }
                }
            }
        }
        self.profiles
            .get(&self.fallback_id)
            .expect("fallback profile always present (validated at build time)")
    }

    /// Iterate over all profiles in their insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &SubagentProfile> {
        self.order
            .iter()
            .filter_map(|id| self.profiles.get(id.as_str()))
    }

    /// Number of registered profiles.
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Return the configured fallback profile id.
    pub fn fallback_id(&self) -> &str {
        &self.fallback_id
    }
}

/// Layered builder for [`SubagentProfileRegistry`].
///
/// Each call to [`Self::extend`] / [`Self::extend_from_file`] applies one
/// layer; entries with an `id` already present **overwrite** the previous
/// definition wholesale (no field-level merge — keeps semantics simple
/// and predictable).
#[derive(Debug, Clone, Default)]
pub struct SubagentProfileRegistryBuilder {
    profiles: HashMap<String, SubagentProfile>,
    order: Vec<String>,
    fallback_id: Option<String>,
}

impl SubagentProfileRegistryBuilder {
    /// Override the fallback profile id (default: `general-purpose`).
    pub fn fallback_id(mut self, id: impl Into<String>) -> Self {
        self.fallback_id = Some(id.into());
        self
    }

    /// Apply a layer of profiles. Later layers overwrite earlier ones by id.
    pub fn extend<I: IntoIterator<Item = SubagentProfile>>(mut self, layer: I) -> Self {
        for profile in layer {
            let id = profile.id.clone();
            if self.profiles.insert(id.clone(), profile).is_none() {
                self.order.push(id);
            }
        }
        self
    }

    /// Convenience: apply a layer parsed from a [`SubagentProfileFile`].
    pub fn extend_from_file(self, file: SubagentProfileFile) -> Self {
        self.extend(file.profiles)
    }

    /// Finalize the registry. Returns an error if the fallback id is not
    /// among the registered profiles.
    pub fn build(self) -> Result<SubagentProfileRegistry, SubagentProfileRegistryError> {
        let fallback_id = self
            .fallback_id
            .unwrap_or_else(|| DEFAULT_FALLBACK_PROFILE_ID.to_string());
        if !self.profiles.contains_key(&fallback_id) {
            return Err(SubagentProfileRegistryError::MissingFallback(fallback_id));
        }
        Ok(SubagentProfileRegistry {
            profiles: self.profiles,
            order: self.order,
            fallback_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subagent::ToolPolicy;

    fn p(id: &str) -> SubagentProfile {
        SubagentProfile {
            id: id.to_string(),
            display_name: id.to_string(),
            description: String::new(),
            system_prompt: format!("prompt for {id}"),
            tools: ToolPolicy::Inherit,
            model_hint: None,
            default_responsibility: None,
            ui: Default::default(),
        }
    }

    #[test]
    fn build_requires_fallback_present() {
        let err = SubagentProfileRegistry::builder()
            .extend(vec![p("researcher")])
            .build()
            .unwrap_err();
        assert!(matches!(
            err,
            SubagentProfileRegistryError::MissingFallback(_)
        ));
    }

    #[test]
    fn resolve_known_id() {
        let reg = SubagentProfileRegistry::builder()
            .extend(vec![p("general-purpose"), p("researcher")])
            .build()
            .unwrap();
        assert_eq!(reg.resolve("researcher").id, "researcher");
    }

    #[test]
    fn resolve_unknown_id_falls_back() {
        let reg = SubagentProfileRegistry::builder()
            .extend(vec![p("general-purpose"), p("researcher")])
            .build()
            .unwrap();
        assert_eq!(reg.resolve("does-not-exist").id, "general-purpose");
        assert_eq!(reg.resolve("").id, "general-purpose");
        assert_eq!(reg.resolve("   ").id, "general-purpose");
    }

    #[test]
    fn resolve_is_case_insensitive() {
        let reg = SubagentProfileRegistry::builder()
            .extend(vec![p("general-purpose"), p("researcher")])
            .build()
            .unwrap();
        assert_eq!(reg.resolve("Researcher").id, "researcher");
        assert_eq!(reg.resolve("RESEARCHER").id, "researcher");
    }

    #[test]
    fn later_layer_overrides_earlier() {
        let mut overridden = p("researcher");
        overridden.system_prompt = "OVERRIDDEN".to_string();

        let reg = SubagentProfileRegistry::builder()
            .extend(vec![p("general-purpose"), p("researcher")])
            .extend(vec![overridden])
            .build()
            .unwrap();

        assert_eq!(reg.resolve("researcher").system_prompt, "OVERRIDDEN");
        // Insertion order preserved across overrides.
        let ids: Vec<&str> = reg.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, vec!["general-purpose", "researcher"]);
    }

    #[test]
    fn extend_from_file_works() {
        let file = SubagentProfileFile {
            profiles: vec![p("general-purpose"), p("tester")],
        };
        let reg = SubagentProfileRegistry::builder()
            .extend_from_file(file)
            .build()
            .unwrap();
        assert!(reg.get("tester").is_some());
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn custom_fallback_id() {
        let reg = SubagentProfileRegistry::builder()
            .fallback_id("researcher")
            .extend(vec![p("researcher"), p("coder")])
            .build()
            .unwrap();
        assert_eq!(reg.fallback_id(), "researcher");
        assert_eq!(reg.resolve("nope").id, "researcher");
    }

    #[test]
    fn file_roundtrip_via_serde() {
        let file = SubagentProfileFile {
            profiles: vec![p("general-purpose"), p("researcher")],
        };
        let json = serde_json::to_string(&file).unwrap();
        let parsed: SubagentProfileFile = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.profiles.len(), 2);
        assert_eq!(parsed.profiles[1].id, "researcher");
    }
}
