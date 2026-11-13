//! Source-validated metadata for host-owned progressive Skill discovery.
//! These helpers do not activate Skills or authorize reading their bodies.

use crate::{SkillError, SkillResult, WorkflowSource};
use bamboo_domain::CapabilityInvocationTarget;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) mod read;
pub use read::{CachedSkillRead, SelectedSkillPage, SelectedSkillReadCache, SelectedSkillSnapshot};
mod render;
pub use render::{
    render_skill_catalog, render_skill_usage_instructions, skill_metadata_budget,
    truncate_skill_utf8_bytes, SkillCatalogRender, SkillCatalogRenderPolicy, SkillMetadataBudget,
    EXPLICIT_SKILL_PROMPT_BYTES,
};
pub(crate) mod source;

use crate::capability_discovery::{AutomaticSkillOutcome, CapabilityDiscoveryIndex};

/// Correlated borrowed data from one current publication. It is not a grant,
/// activation snapshot or retained resource owner.
pub struct CurrentSkillInput<'a> {
    pub selection: &'a crate::WorkflowSelection,
    pub definition: &'a crate::SkillDefinition,
    pub catalog_entry: &'a crate::WorkflowCatalogEntry,
    pub revision: u64,
    pub catalog_revision: u64,
    pub mode: Option<&'a str>,
    pub main_resource: &'a str,
}

/// Owned metadata only; contains no source handles, instruction or policy bytes.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SkillCatalogMetadata {
    pub package: String,
    pub name: String,
    pub description: String,
    pub short_description: Option<String>,
    pub main_resource: String,
    pub source: WorkflowSource,
    pub revision: u64,
    /// Opaque current physical/raw publication identity, never a bearer grant.
    pub identity: String,
    pub(crate) root: String,
    pub(crate) explicit: bool,
    pub(crate) automatic: bool,
}

/// Per-call restrictions supplied by a trusted host, separate from UI selection.
/// None means no extra ceiling; Some(empty) denies every Skill.
#[derive(Debug, Clone, Serialize)]
pub struct SkillCatalogEligibility {
    pub ceiling: Option<BTreeSet<String>>,
    pub explicit: BTreeSet<String>,
    pub disabled: BTreeSet<String>,
    pub deny_all: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkillCatalogSnapshot {
    pub entries: Vec<SkillCatalogMetadata>,
    pub identity: String,
}

/// Rank metadata from the current publication for prompt advertisement only.
/// The caller must supply the authorized subset of that same publication.
pub(crate) fn automatic_skill_prompt_candidates<'a>(
    published: &'a [SkillCatalogMetadata],
    eligible: &[SkillCatalogMetadata],
    query: &str,
) -> (Vec<&'a SkillCatalogMetadata>, AutomaticSkillOutcome) {
    let allowed = eligible
        .iter()
        .map(|entry| (entry.package.as_str(), entry.identity.as_str()))
        .collect::<BTreeSet<_>>();
    let by_package = published
        .iter()
        .map(|entry| (entry.package.as_str(), entry))
        .collect::<std::collections::HashMap<_, _>>();
    let index = CapabilityDiscoveryIndex::for_progressive_skill_matching(published);
    let (decision, matches) = index.match_automatic_skills(query, |candidate| {
        let CapabilityInvocationTarget::Skill { skill_id, .. } = &candidate.invocation_target
        else {
            return false;
        };
        by_package
            .get(skill_id.as_str())
            .is_some_and(|entry| allowed.contains(&(skill_id.as_str(), entry.identity.as_str())))
    });
    let candidates = matches
        .iter()
        .filter_map(|candidate| {
            let CapabilityInvocationTarget::Skill { skill_id, .. } = &candidate.invocation_target
            else {
                return None;
            };
            by_package.get(skill_id.as_str()).copied()
        })
        .collect();
    (candidates, decision.outcome)
}

impl SkillCatalogSnapshot {
    pub(crate) fn new(
        store: u64,
        revision: u64,
        entries: Vec<SkillCatalogMetadata>,
    ) -> SkillResult<Self> {
        let bytes = serde_json::to_vec(&(store, revision, &entries))
            .map_err(|error| SkillError::Validation(error.to_string()))?;
        Ok(Self {
            entries,
            identity: hex::encode(Sha256::digest(bytes)),
        })
    }
}

/// Current eligibility for owned metadata projection and list pagination.
/// Populated malformed restrictions are errors, never a missing ceiling.
pub fn eligible_metadata(
    entries: &[SkillCatalogMetadata],
    access: &SkillCatalogEligibility,
) -> SkillResult<Vec<SkillCatalogMetadata>> {
    let ids = access
        .ceiling
        .iter()
        .flatten()
        .chain(&access.explicit)
        .chain(&access.disabled);
    if ids
        .clone()
        .any(|id| id.len() > 256 || !crate::store::parser::is_valid_skill_id(id))
    {
        return Err(SkillError::Validation(
            "malformed Skill catalog restriction".into(),
        ));
    }
    let mut eligible = entries
        .iter()
        .filter(|entry| {
            !access.deny_all
                && access
                    .ceiling
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&entry.package))
                && !access.disabled.contains(&entry.package)
                && if access.explicit.contains(&entry.package) {
                    entry.explicit
                } else {
                    entry.automatic
                }
        })
        .cloned()
        .collect::<Vec<_>>();
    eligible.sort_by(|a, b| {
        source_rank(a.source)
            .cmp(&source_rank(b.source))
            .then(a.name.cmp(&b.name))
            .then(a.main_resource.cmp(&b.main_resource))
    });
    Ok(eligible)
}

fn source_rank(source: WorkflowSource) -> u8 {
    match source {
        WorkflowSource::Builtin => 0,
        WorkflowSource::Project => 1,
        WorkflowSource::Workspace => 2,
        WorkflowSource::User => 3,
        WorkflowSource::Plugin => 4,
    }
}

#[cfg(test)]
mod catalog_tests;
#[cfg(test)]
mod source_tests;

#[cfg(test)]
mod render_tests;

#[cfg(test)]
mod read_tests;
