//! Source-validated metadata for host-owned progressive Skill discovery.
//! These helpers do not activate Skills or authorize reading their bodies.

use crate::{SkillError, SkillResult, WorkflowSource};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) mod source;

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
