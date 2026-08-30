//! Deterministic, provider-neutral discovery over capability metadata.
//!
//! This module owns no provider wire format and no active capability state. It
//! projects the canonical tool registry and immutable Skill/Workflow catalogs
//! into a small metadata-only index, then ranks bounded queries without I/O.

use std::collections::{BTreeMap, BTreeSet};

use bamboo_domain::{
    canonical_tool_name as canonical_registry_tool_name, resolve_tool_reference_name,
    CapabilityInvocationPolicy, CapabilityInvocationTarget, CapabilityKind, CapabilityLoadingClass,
    CapabilityMatch, CapabilitySource, CapabilityStatus, ClassifiedToolIdentity,
    ClassifiedToolSchema, DiscoverCapabilitiesRequest, DiscoverCapabilitiesResult, ToolSchema,
    MAX_DISCOVERY_QUERY_CHARS, MAX_DISCOVERY_RESULTS,
};
use thiserror::Error;

use crate::{
    WorkflowCatalogEntry, WorkflowCatalogSnapshot, WorkflowKind, WorkflowSource, WorkflowStatus,
};

/// Discovery summaries are bounded independently of the source description so
/// one verbose registry entry cannot dominate a bounded result.
pub const MAX_CAPABILITY_SUMMARY_CHARS: usize = 240;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InvocationEligibility {
    /// A capability is eligible when either catalog invocation mode is enabled.
    #[default]
    Any,
    Explicit,
    Automatic,
}

impl InvocationEligibility {
    fn permits(self, policy: CapabilityInvocationPolicy) -> bool {
        match self {
            Self::Any => policy.explicit || policy.automatic,
            Self::Explicit => policy.explicit,
            Self::Automatic => policy.automatic,
        }
    }
}

/// Host-resolved eligibility for one immutable discovery projection.
///
/// Surface selection and authorization remain host responsibilities. Passing
/// only surface-eligible tool metadata plus these filters prevents discovery
/// from widening what a session can load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityDiscoveryEligibility {
    pub disabled_tool_names: BTreeSet<String>,
    /// `None` means the caller already supplied a surface-scoped registry.
    /// `Some(empty)` exposes no tools. References resolve exact-first against
    /// the projected catalog, then through legacy/builtin aliases.
    pub allowed_tool_names: Option<BTreeSet<String>>,
    pub disabled_skill_ids: BTreeSet<String>,
    /// `None` means no additional allowlist. `Some(empty)` exposes no Skills.
    pub allowed_skill_ids: Option<BTreeSet<String>>,
    pub skill_gateway_available: bool,
    pub workflow_gateway_available: bool,
    pub skill_invocation: InvocationEligibility,
    pub workflow_invocation: InvocationEligibility,
}

impl Default for CapabilityDiscoveryEligibility {
    fn default() -> Self {
        Self {
            disabled_tool_names: BTreeSet::new(),
            allowed_tool_names: None,
            disabled_skill_ids: BTreeSet::new(),
            allowed_skill_ids: None,
            skill_gateway_available: true,
            workflow_gateway_available: true,
            skill_invocation: InvocationEligibility::Any,
            workflow_invocation: InvocationEligibility::Any,
        }
    }
}

/// Parameter-schema-free tool registry projection consumed by discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCapabilityMetadata {
    pub canonical_name: String,
    pub summary: String,
    pub source: CapabilitySource,
    pub aliases: Vec<String>,
    pub available: bool,
}

/// Copy only searchable public metadata out of tool schemas. Parameter schemas
/// are deliberately never cloned, serialized, or retained by discovery.
pub fn project_tool_capability_metadata(schemas: &[ToolSchema]) -> Vec<ToolCapabilityMetadata> {
    schemas
        .iter()
        .filter_map(|schema| {
            let identity = ClassifiedToolIdentity::from_schema_name(&schema.function.name)?;
            if identity.loading_class() == CapabilityLoadingClass::HostOnly {
                return None;
            }
            Some(ToolCapabilityMetadata {
                source: source_for_tool(identity.execution_name()),
                aliases: aliases_for_tool(identity.execution_name()),
                canonical_name: identity.execution_name().to_string(),
                summary: bounded_summary(&schema.function.description),
                available: true,
            })
        })
        .collect()
}

/// Project a classified logical catalog into metadata-only discovery entries.
/// HostOnly schemas never enter the searchable index, while Core and Deferred
/// share the exact execution identity and policy used by provider projection.
pub fn project_classified_tool_capability_metadata(
    catalog: &[ClassifiedToolSchema],
) -> Vec<ToolCapabilityMetadata> {
    catalog
        .iter()
        .filter(|entry| entry.loading_class() != CapabilityLoadingClass::HostOnly)
        .map(|entry| ToolCapabilityMetadata {
            source: source_for_tool(entry.execution_name()),
            aliases: aliases_for_tool(entry.execution_name()),
            canonical_name: entry.execution_name().to_string(),
            summary: bounded_summary(&entry.schema().function.description),
            available: true,
        })
        .collect()
}

#[derive(Debug, Clone)]
struct IndexedCapability {
    value: CapabilityMatch,
    normalized_names: Vec<String>,
    normalized_summary: String,
}

/// Immutable searchable projection. Constructing and querying it performs no
/// filesystem, network, model, execution, authorization, or persistence work.
#[derive(Debug, Clone, Default)]
pub struct CapabilityDiscoveryIndex {
    candidates: Vec<IndexedCapability>,
}

impl CapabilityDiscoveryIndex {
    /// Build discovery from the exact classified catalog already resolved for
    /// the current session/round, while capturing both command catalogs under
    /// the store's publication read lock. Tool allow/disable references are not
    /// applied twice; Skill/Workflow eligibility is still resolved here.
    pub async fn from_resolved_classified_store(
        tool_catalog: &[ClassifiedToolSchema],
        store: &crate::SkillStore,
        eligibility: &CapabilityDiscoveryEligibility,
    ) -> Self {
        let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
        let mut non_tool_eligibility = eligibility.clone();
        non_tool_eligibility.disabled_tool_names.clear();
        non_tool_eligibility.allowed_tool_names = None;
        Self::from_snapshots(
            project_classified_tool_capability_metadata(tool_catalog),
            &skill_catalog,
            &workflow_catalog,
            &non_tool_eligibility,
        )
    }

    /// Legacy compatibility facade for callers that do not yet have a
    /// session-resolved classified catalog. New agent-loop code must use
    /// [`Self::from_resolved_classified_store`] so goal/Skill/disabled eligibility
    /// and provider projection cannot drift from discovery.
    pub async fn from_store(
        tool_schemas: &[ToolSchema],
        store: &crate::SkillStore,
        eligibility: &CapabilityDiscoveryEligibility,
    ) -> Self {
        let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
        Self::from_snapshots(
            project_tool_capability_metadata(tool_schemas),
            &skill_catalog,
            &workflow_catalog,
            eligibility,
        )
    }

    pub fn from_snapshots(
        tools: impl IntoIterator<Item = ToolCapabilityMetadata>,
        skill_catalog: &WorkflowCatalogSnapshot,
        workflow_catalog: &WorkflowCatalogSnapshot,
        eligibility: &CapabilityDiscoveryEligibility,
    ) -> Self {
        let mut projected_tools = BTreeMap::<String, ToolCapabilityMetadata>::new();

        for mut tool in tools {
            let Some(identity) = ClassifiedToolIdentity::from_schema_name(&tool.canonical_name)
            else {
                continue;
            };
            tool.canonical_name = identity.execution_name().to_string();
            if identity.loading_class() == CapabilityLoadingClass::HostOnly || !tool.available {
                continue;
            }
            let key = tool.canonical_name.clone();
            tool.summary = bounded_summary(&tool.summary);
            tool.aliases.sort();
            tool.aliases.dedup();
            match projected_tools.get(&key) {
                Some(existing) if !prefer_tool_projection(&tool, existing) => {}
                _ => {
                    projected_tools.insert(key, tool);
                }
            }
        }

        let disabled_execution_names = eligibility
            .disabled_tool_names
            .iter()
            .filter_map(|reference| {
                resolve_tool_reference_name(reference, |name| projected_tools.contains_key(name))
            })
            .collect::<BTreeSet<_>>();
        let allowed_execution_names = eligibility.allowed_tool_names.as_ref().map(|references| {
            references
                .iter()
                .filter_map(|reference| {
                    resolve_tool_reference_name(reference, |name| {
                        projected_tools.contains_key(name)
                    })
                })
                .collect::<BTreeSet<_>>()
        });
        projected_tools.retain(|name, _| {
            !disabled_execution_names.contains(name)
                && allowed_execution_names
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(name))
        });

        let mut candidates = projected_tools
            .into_values()
            .map(index_tool)
            .collect::<Vec<_>>();

        if eligibility.skill_gateway_available {
            candidates.extend(
                skill_catalog
                    .entries
                    .iter()
                    .filter(|entry| skill_entry_is_eligible(entry, eligibility))
                    .map(|entry| index_catalog_entry(entry, CapabilityKind::Skill)),
            );
        }

        if eligibility.workflow_gateway_available {
            candidates.extend(
                workflow_catalog
                    .entries
                    .iter()
                    .filter(|entry| workflow_entry_is_eligible(entry, eligibility))
                    .map(|entry| index_catalog_entry(entry, CapabilityKind::Workflow)),
            );
        }

        candidates.sort_by(|left, right| {
            left.value
                .kind
                .cmp(&right.value.kind)
                .then_with(|| left.value.capability_ref.cmp(&right.value.capability_ref))
        });
        candidates.dedup_by(|left, right| {
            left.value.kind == right.value.kind
                && left.value.capability_ref == right.value.capability_ref
        });

        Self { candidates }
    }

    pub fn discover(
        &self,
        request: &DiscoverCapabilitiesRequest,
    ) -> Result<DiscoverCapabilitiesResult, CapabilityDiscoveryError> {
        let query = request.query.trim().to_string();
        let query_chars = query.chars().count();
        if query_chars > MAX_DISCOVERY_QUERY_CHARS {
            return Err(CapabilityDiscoveryError::QueryTooLong {
                actual: query_chars,
                maximum: MAX_DISCOVERY_QUERY_CHARS,
            });
        }

        let limit = request.limit.unwrap_or(MAX_DISCOVERY_RESULTS);
        if limit > MAX_DISCOVERY_RESULTS {
            return Err(CapabilityDiscoveryError::LimitTooLarge {
                actual: limit,
                maximum: MAX_DISCOVERY_RESULTS,
            });
        }

        let kinds = match request.kinds.as_ref() {
            Some(kinds) if kinds.len() > 3 => {
                return Err(CapabilityDiscoveryError::TooManyKinds {
                    actual: kinds.len(),
                    maximum: 3,
                });
            }
            Some(kinds) => Some(kinds.iter().copied().collect::<BTreeSet<_>>()),
            None => None,
        };

        let empty = || DiscoverCapabilitiesResult {
            query: query.clone(),
            matches: Vec::new(),
        };
        if query.is_empty() || limit == 0 || kinds.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(empty());
        }

        let normalized_query = normalize_search_text(&query);
        let query_tokens = search_tokens(&normalized_query);
        if normalized_query.is_empty() || query_tokens.is_empty() {
            return Ok(empty());
        }
        let tool_query = query
            .strip_prefix("tool:")
            .or_else(|| query.strip_prefix("tool/"))
            .unwrap_or(&query);
        let resolved_tool_query = resolve_tool_reference_name(tool_query, |name| {
            self.candidates.iter().any(|candidate| {
                matches!(
                    &candidate.value.invocation_target,
                    CapabilityInvocationTarget::Tool { name: registered } if registered == name
                )
            })
        });

        let mut ranked = self
            .candidates
            .iter()
            .filter(|candidate| {
                kinds
                    .as_ref()
                    .is_none_or(|kinds| kinds.contains(&candidate.value.kind))
            })
            .filter_map(|candidate| {
                rank_candidate(
                    candidate,
                    &query,
                    &normalized_query,
                    &query_tokens,
                    resolved_tool_query.as_deref(),
                    None,
                )
                .map(|rank| (rank, candidate))
            })
            .collect::<Vec<_>>();

        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.value.kind.cmp(&right.value.kind))
                .then_with(|| left.value.capability_ref.cmp(&right.value.capability_ref))
        });

        Ok(DiscoverCapabilitiesResult {
            query,
            matches: ranked
                .into_iter()
                .take(limit)
                .map(|(_, candidate)| candidate.value.clone())
                .collect(),
        })
    }

    /// Select one automatic Skill only when the best match is both strong and
    /// semantically unambiguous. Deterministic source/reference tie-breakers
    /// remain useful for generic discovery, but must never turn an ambiguous
    /// automatic activation into an arbitrary choice.
    pub fn discover_unambiguous_automatic_skill(&self, query: &str) -> Option<CapabilityMatch> {
        let query = query
            .trim()
            .chars()
            .take(MAX_DISCOVERY_QUERY_CHARS)
            .collect::<String>();
        if query.is_empty() {
            return None;
        }

        let normalized_query = normalize_search_text(&query);
        let query_tokens = search_tokens(&normalized_query);
        if normalized_query.is_empty() || query_tokens.is_empty() {
            return None;
        }

        let mut ranked = self
            .candidates
            .iter()
            .filter(|candidate| candidate.value.kind == CapabilityKind::Skill)
            .filter_map(|candidate| {
                let normalized_summary = normalize_search_text(&candidate.value.summary);
                rank_candidate(
                    candidate,
                    &query,
                    &normalized_query,
                    &query_tokens,
                    None,
                    Some(&normalized_summary),
                )
                .map(|rank| (rank, candidate))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.value.capability_ref.cmp(&right.value.capability_ref))
        });

        let exact = ranked
            .iter()
            .filter(|(rank, _)| rank.semantic.exact_identity)
            .collect::<Vec<_>>();
        if !exact.is_empty() {
            return (exact.len() == 1).then(|| exact[0].1.value.clone());
        }

        let strong = ranked
            .into_iter()
            .filter(|(rank, candidate)| {
                strong_fuzzy_skill_match(candidate, &query_tokens, rank.semantic)
            })
            .collect::<Vec<_>>();
        let (top_rank, top) = strong.first()?;
        if strong
            .get(1)
            .is_some_and(|(next_rank, _)| next_rank.semantic == top_rank.semantic)
        {
            return None;
        }

        Some(top.value.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CapabilityDiscoveryError {
    #[error("discovery query has {actual} characters; maximum is {maximum}")]
    QueryTooLong { actual: usize, maximum: usize },
    #[error("discovery limit is {actual}; maximum is {maximum}")]
    LimitTooLarge { actual: usize, maximum: usize },
    #[error("discovery kinds has {actual} entries; maximum is {maximum}")]
    TooManyKinds { actual: usize, maximum: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SemanticSearchRank {
    exact_registered_identity: bool,
    exact_identity: bool,
    name_phrase: bool,
    name_token_hits: usize,
    summary_phrase: bool,
    summary_token_hits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SearchRank {
    semantic: SemanticSearchRank,
    source_precedence: u8,
}

fn rank_candidate(
    candidate: &IndexedCapability,
    raw_query: &str,
    normalized_query: &str,
    query_tokens: &BTreeSet<String>,
    resolved_tool_query: Option<&str>,
    normalized_summary_override: Option<&str>,
) -> Option<SearchRank> {
    let exact_registered_identity = matches!(
        (&candidate.value.invocation_target, resolved_tool_query),
        (CapabilityInvocationTarget::Tool { name }, Some(resolved)) if name == resolved
    );
    let exact_identity = candidate
        .normalized_names
        .iter()
        .any(|name| name == normalized_query)
        || candidate
            .value
            .capability_ref
            .eq_ignore_ascii_case(raw_query)
        || tool_alias_exact_match(candidate, raw_query);
    let name_phrase = candidate
        .normalized_names
        .iter()
        .any(|name| name.contains(normalized_query));
    let name_token_hits = candidate
        .normalized_names
        .iter()
        .map(|name| token_hits(name, query_tokens))
        .max()
        .unwrap_or_default();
    let normalized_summary =
        normalized_summary_override.unwrap_or(candidate.normalized_summary.as_str());
    let summary_phrase = normalized_summary.contains(normalized_query);
    let summary_token_hits = token_hits(normalized_summary, query_tokens);

    if !exact_registered_identity
        && !exact_identity
        && !name_phrase
        && name_token_hits == 0
        && !summary_phrase
        && summary_token_hits == 0
    {
        return None;
    }

    Some(SearchRank {
        semantic: SemanticSearchRank {
            exact_registered_identity,
            exact_identity,
            name_phrase,
            name_token_hits,
            summary_phrase,
            summary_token_hits,
        },
        source_precedence: source_precedence(candidate.value.source),
    })
}

fn strong_fuzzy_skill_match(
    candidate: &IndexedCapability,
    query_tokens: &BTreeSet<String>,
    semantic_rank: SemanticSearchRank,
) -> bool {
    let complete_identity_tokens = candidate.normalized_names.iter().any(|name| {
        let identity_tokens = search_tokens(name);
        !identity_tokens.is_empty() && identity_tokens.is_subset(query_tokens)
    });
    complete_identity_tokens || semantic_rank.summary_token_hits >= 2
}

fn tool_alias_exact_match(candidate: &IndexedCapability, raw_query: &str) -> bool {
    if candidate.value.kind != CapabilityKind::Tool {
        return false;
    }
    let query = raw_query
        .strip_prefix("tool:")
        .or_else(|| raw_query.strip_prefix("tool/"))
        .unwrap_or(raw_query);
    let canonical = canonical_registry_tool_name(query);
    matches!(
        &candidate.value.invocation_target,
        CapabilityInvocationTarget::Tool { name } if name == &canonical
    )
}

fn token_hits(haystack: &str, query_tokens: &BTreeSet<String>) -> usize {
    let haystack_tokens = search_tokens(haystack);
    query_tokens
        .iter()
        .filter(|token| haystack_tokens.contains(*token))
        .count()
}

fn index_tool(tool: ToolCapabilityMetadata) -> IndexedCapability {
    let capability_ref = format!("tool:{}", tool.canonical_name);
    let mut normalized_names = vec![
        normalize_search_text(&capability_ref),
        normalize_search_text(&tool.canonical_name),
    ];
    normalized_names.extend(
        tool.aliases
            .iter()
            .map(|alias| normalize_search_text(alias)),
    );
    normalized_names.sort();
    normalized_names.dedup();

    IndexedCapability {
        normalized_summary: normalize_search_text(&tool.summary),
        value: CapabilityMatch {
            capability_ref,
            kind: CapabilityKind::Tool,
            display_name: tool.canonical_name.clone(),
            summary: tool.summary,
            source: tool.source,
            revision: None,
            status: CapabilityStatus::Available,
            invocation_policy: None,
            invocation_target: CapabilityInvocationTarget::Tool {
                name: tool.canonical_name,
            },
        },
        normalized_names,
    }
}

fn index_catalog_entry(entry: &WorkflowCatalogEntry, kind: CapabilityKind) -> IndexedCapability {
    let source = capability_source(entry.source);
    let summary = bounded_summary(&entry.description);
    let capability_ref = match kind {
        CapabilityKind::Skill => format!("skill:{}", entry.id),
        CapabilityKind::Workflow => format!("workflow:{}", entry.id),
        CapabilityKind::Tool => unreachable!("catalog entries are never tool candidates"),
    };
    let policy = invocation_policy(entry);
    let invocation_target = match kind {
        CapabilityKind::Skill => CapabilityInvocationTarget::Skill {
            name: "load_skill".to_string(),
            skill_id: entry.id.clone(),
            source,
            revision: entry.revision,
        },
        CapabilityKind::Workflow => CapabilityInvocationTarget::Workflow {
            name: "workflow_run".to_string(),
            workflow_id: entry.id.clone(),
            source,
            revision: entry.revision,
        },
        CapabilityKind::Tool => unreachable!("catalog entries are never tool candidates"),
    };

    let mut policy_hints = Vec::new();
    if policy.explicit {
        policy_hints.push("explicit");
    }
    if policy.automatic {
        policy_hints.push("automatic");
    }
    let policy_hint = policy_hints.join(" ");
    IndexedCapability {
        normalized_names: vec![
            normalize_search_text(&capability_ref),
            normalize_search_text(&entry.id),
            normalize_search_text(&entry.name),
        ],
        normalized_summary: normalize_search_text(&format!("{summary} {policy_hint}")),
        value: CapabilityMatch {
            capability_ref,
            kind,
            display_name: entry.name.clone(),
            summary,
            source,
            revision: Some(entry.revision),
            status: CapabilityStatus::Valid,
            invocation_policy: Some(policy),
            invocation_target,
        },
    }
}

fn skill_entry_is_eligible(
    entry: &WorkflowCatalogEntry,
    eligibility: &CapabilityDiscoveryEligibility,
) -> bool {
    catalog_entry_is_valid(entry)
        && entry.kind == WorkflowKind::Instruction
        && !entry.legacy
        && !eligibility.disabled_skill_ids.contains(&entry.id)
        && eligibility
            .allowed_skill_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&entry.id))
        && eligibility
            .skill_invocation
            .permits(invocation_policy(entry))
}

fn workflow_entry_is_eligible(
    entry: &WorkflowCatalogEntry,
    eligibility: &CapabilityDiscoveryEligibility,
) -> bool {
    catalog_entry_is_valid(entry)
        && entry.kind == WorkflowKind::Orchestration
        && entry.is_public_workflow()
        && !eligibility.disabled_skill_ids.contains(&entry.id)
        && eligibility
            .workflow_invocation
            .permits(invocation_policy(entry))
}

fn catalog_entry_is_valid(entry: &WorkflowCatalogEntry) -> bool {
    entry.winner
        && entry.status == WorkflowStatus::Valid
        && !entry.id.trim().is_empty()
        && !entry.name.trim().is_empty()
}

fn invocation_policy(entry: &WorkflowCatalogEntry) -> CapabilityInvocationPolicy {
    CapabilityInvocationPolicy {
        explicit: entry.invocation_policy["explicit"].as_bool() == Some(true),
        automatic: entry.invocation_policy["automatic"].as_bool() == Some(true),
    }
}

pub(crate) fn capability_source(source: WorkflowSource) -> CapabilitySource {
    match source {
        WorkflowSource::Builtin => CapabilitySource::Builtin,
        WorkflowSource::Project => CapabilitySource::Project,
        WorkflowSource::Workspace => CapabilitySource::Workspace,
        WorkflowSource::User => CapabilitySource::User,
        WorkflowSource::Plugin => CapabilitySource::Plugin,
    }
}

fn source_precedence(source: CapabilitySource) -> u8 {
    match source {
        CapabilitySource::Workspace => 8,
        CapabilitySource::Project => 7,
        CapabilitySource::User => 6,
        CapabilitySource::Plugin => 5,
        CapabilitySource::Builtin => 4,
        CapabilitySource::Server => 3,
        CapabilitySource::Mcp => 2,
        CapabilitySource::Custom => 1,
    }
}

fn prefer_tool_projection(
    candidate: &ToolCapabilityMetadata,
    existing: &ToolCapabilityMetadata,
) -> bool {
    source_precedence(candidate.source)
        .cmp(&source_precedence(existing.source))
        .then_with(|| candidate.summary.cmp(&existing.summary))
        .then_with(|| candidate.canonical_name.cmp(&existing.canonical_name))
        .is_gt()
}

fn source_for_tool(name: &str) -> CapabilitySource {
    if bamboo_domain::BUILTIN_TOOL_NAMES.contains(&name) {
        CapabilitySource::Builtin
    } else if bamboo_domain::SERVER_CAPABILITY_NAMES.contains(&name) {
        CapabilitySource::Server
    } else if name.starts_with("mcp__") {
        CapabilitySource::Mcp
    } else {
        CapabilitySource::Custom
    }
}

fn aliases_for_tool(canonical_name: &str) -> Vec<String> {
    bamboo_domain::BUILTIN_TOOL_ALIASES
        .iter()
        .filter(|(_, canonical)| *canonical == canonical_name)
        .map(|(alias, _)| (*alias).to_string())
        .collect()
}

pub(crate) fn bounded_summary(value: &str) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CAPABILITY_SUMMARY_CHARS {
        return compact;
    }
    let mut bounded = compact
        .chars()
        .take(MAX_CAPABILITY_SUMMARY_CHARS.saturating_sub(1))
        .collect::<String>();
    bounded.push('…');
    bounded
}

fn normalize_search_text(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_lower_or_digit = false;
    let mut pending_space = false;

    for character in value.chars() {
        if character.is_alphanumeric() {
            if (pending_space || (character.is_uppercase() && previous_was_lower_or_digit))
                && !normalized.is_empty()
            {
                normalized.push(' ');
            }
            for lowercase in character.to_lowercase() {
                normalized.push(lowercase);
            }
            previous_was_lower_or_digit = character.is_lowercase() || character.is_numeric();
            pending_space = false;
        } else {
            pending_space = true;
            previous_was_lower_or_digit = false;
        }
    }

    normalized.trim().to_string()
}

fn search_tokens(normalized: &str) -> BTreeSet<String> {
    normalized
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::FunctionSchema;
    use serde_json::json;

    use crate::{ShadowedWorkflowCandidate, WorkflowCatalogSnapshot};

    fn schema(name: &str, description: &str, parameters: serde_json::Value) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: name.to_string(),
                description: description.to_string(),
                parameters,
            },
        }
    }

    fn catalog_entry(
        id: &str,
        name: &str,
        description: &str,
        kind: WorkflowKind,
        source: WorkflowSource,
        revision: u64,
    ) -> WorkflowCatalogEntry {
        WorkflowCatalogEntry {
            id: id.to_string(),
            name: name.to_string(),
            description: description.to_string(),
            kind,
            source,
            revision,
            content_digest: "digest".to_string(),
            version: "1".to_string(),
            invocation_policy: json!({"explicit": true, "automatic": true}),
            argument_schema: json!({"type": "object"}),
            status: WorkflowStatus::Valid,
            legacy: false,
            migration_status: None,
            last_error: None,
            winner: true,
            shadowed_candidates: Vec::new(),
        }
    }

    fn request(query: &str) -> DiscoverCapabilitiesRequest {
        DiscoverCapabilitiesRequest {
            query: query.to_string(),
            kinds: None,
            limit: None,
        }
    }

    fn index(
        schemas: &[ToolSchema],
        skills: Vec<WorkflowCatalogEntry>,
        workflows: Vec<WorkflowCatalogEntry>,
        eligibility: CapabilityDiscoveryEligibility,
    ) -> CapabilityDiscoveryIndex {
        CapabilityDiscoveryIndex::from_snapshots(
            project_tool_capability_metadata(schemas),
            &WorkflowCatalogSnapshot {
                revision: 11,
                entries: skills,
            },
            &WorkflowCatalogSnapshot {
                revision: 12,
                entries: workflows,
            },
            &eligibility,
        )
    }

    #[test]
    fn one_query_searches_tools_skills_and_workflows_with_typed_targets() {
        let schemas = [schema(
            "GitInspect",
            "Inspect repository status, history, and diffs",
            json!({"type": "object"}),
        )];
        let mut skill = catalog_entry(
            "review-helper",
            "Review Helper",
            "Inspect a change before review",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            7,
        );
        skill.invocation_policy = json!({
            "explicit": true,
            "automatic": true,
            "requires_confirmation": true
        });
        let workflow = catalog_entry(
            "review-pipeline",
            "Review Pipeline",
            "Inspect and review a repository change",
            WorkflowKind::Orchestration,
            WorkflowSource::Workspace,
            9,
        );
        let result = index(
            &schemas,
            vec![skill],
            vec![workflow],
            CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("inspect repository change"))
        .expect("bounded discovery");

        assert_eq!(result.query, "inspect repository change");
        assert_eq!(result.matches.len(), 3);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|candidate| candidate.kind)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                CapabilityKind::Tool,
                CapabilityKind::Skill,
                CapabilityKind::Workflow,
            ])
        );
        assert!(result.matches.iter().any(|candidate| matches!(
            &candidate.invocation_target,
            CapabilityInvocationTarget::Skill {
                name,
                skill_id,
                source: CapabilitySource::User,
                revision: 7,
            } if name == "load_skill" && skill_id == "review-helper"
        )));
        assert_eq!(
            result
                .matches
                .iter()
                .find(|candidate| candidate.capability_ref == "skill:review-helper")
                .and_then(|candidate| candidate.invocation_policy),
            Some(CapabilityInvocationPolicy {
                explicit: true,
                automatic: true,
            })
        );
        assert!(result.matches.iter().any(|candidate| matches!(
            &candidate.invocation_target,
            CapabilityInvocationTarget::Workflow {
                name,
                workflow_id,
                source: CapabilitySource::Workspace,
                revision: 9,
            } if name == "workflow_run" && workflow_id == "review-pipeline"
        )));
    }

    #[test]
    fn kind_filter_limit_and_empty_results_are_bounded() {
        let skill = catalog_entry(
            "inspect-skill",
            "Inspect Skill",
            "Inspect source",
            WorkflowKind::Instruction,
            WorkflowSource::Builtin,
            1,
        );
        let index = index(
            &[schema("InspectTool", "Inspect source", json!({}))],
            vec![skill],
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        );
        let result = index
            .discover(&DiscoverCapabilitiesRequest {
                query: "inspect".to_string(),
                kinds: Some(vec![CapabilityKind::Skill]),
                limit: Some(1),
            })
            .expect("filtered discovery");
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].kind, CapabilityKind::Skill);

        for query in ["", "   ", "no-such-capability"] {
            assert!(index
                .discover(&request(query))
                .expect("typed empty result")
                .matches
                .is_empty());
        }
        assert!(index
            .discover(&DiscoverCapabilitiesRequest {
                query: "inspect".to_string(),
                kinds: Some(Vec::new()),
                limit: None,
            })
            .expect("empty kind set")
            .matches
            .is_empty());
    }

    #[test]
    fn validation_rejects_unbounded_requests() {
        let index = CapabilityDiscoveryIndex::default();
        assert!(matches!(
            index.discover(&request(&"q".repeat(MAX_DISCOVERY_QUERY_CHARS + 1))),
            Err(CapabilityDiscoveryError::QueryTooLong { .. })
        ));
        assert!(matches!(
            index.discover(&DiscoverCapabilitiesRequest {
                query: "query".to_string(),
                kinds: None,
                limit: Some(MAX_DISCOVERY_RESULTS + 1),
            }),
            Err(CapabilityDiscoveryError::LimitTooLarge { .. })
        ));
        assert!(matches!(
            index.discover(&DiscoverCapabilitiesRequest {
                query: "query".to_string(),
                kinds: Some(vec![
                    CapabilityKind::Tool,
                    CapabilityKind::Skill,
                    CapabilityKind::Workflow,
                    CapabilityKind::Tool,
                ]),
                limit: None,
            }),
            Err(CapabilityDiscoveryError::TooManyKinds { .. })
        ));
    }

    #[test]
    fn default_limit_never_returns_more_than_five_matches() {
        let schemas = (0..8)
            .map(|index| schema(&format!("Inspect{index}"), "inspect source", json!({})))
            .collect::<Vec<_>>();
        let result = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("inspect"))
        .expect("default bounded result");

        assert_eq!(result.matches.len(), MAX_DISCOVERY_RESULTS);
    }

    #[test]
    fn exact_alias_wins_and_serialization_is_deterministic() {
        let schemas = [
            schema("Edit", "Apply a patch to a file", json!({})),
            schema("PatchAdvisor", "Explain apply_patch usage", json!({})),
        ];
        let index = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        );
        let first = index.discover(&request("apply_patch")).expect("alias");
        let second = index.discover(&request("apply_patch")).expect("alias");

        assert_eq!(first.matches[0].capability_ref, "tool:Edit");
        assert_eq!(
            serde_json::to_string(&first).expect("serialize"),
            serde_json::to_string(&second).expect("serialize")
        );
    }

    #[test]
    fn exact_registered_identity_ranks_ahead_of_alias_and_case_fallbacks() {
        let index = index(
            &[
                schema("Edit", "builtin edit", json!({})),
                schema("apply_patch", "custom exact patch", json!({})),
                schema("Foo", "upper custom", json!({})),
                schema("foo", "lower custom", json!({})),
            ],
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        );

        let patch = index
            .discover(&request("apply_patch"))
            .expect("exact custom alias query");
        assert_eq!(patch.matches[0].capability_ref, "tool:apply_patch");
        let legacy_patch = index
            .discover(&request("applyPatch"))
            .expect("legacy spelling resolves through exact custom alias");
        assert_eq!(legacy_patch.matches[0].capability_ref, "tool:apply_patch");

        let lower = index.discover(&request("foo")).expect("exact case query");
        assert_eq!(lower.matches[0].capability_ref, "tool:foo");
        let upper = index.discover(&request("Foo")).expect("exact case query");
        assert_eq!(upper.matches[0].capability_ref, "tool:Foo");
    }

    #[test]
    fn serialization_is_byte_identical_across_snapshot_input_order() {
        let tools = vec![
            ToolCapabilityMetadata {
                canonical_name: "InspectBeta".to_string(),
                summary: "inspect source".to_string(),
                source: CapabilitySource::Custom,
                aliases: Vec::new(),
                available: true,
            },
            ToolCapabilityMetadata {
                canonical_name: "InspectAlpha".to_string(),
                summary: "inspect source".to_string(),
                source: CapabilitySource::Custom,
                aliases: Vec::new(),
                available: true,
            },
        ];
        let skills = vec![
            catalog_entry(
                "inspect-beta",
                "Inspect Beta",
                "inspect source",
                WorkflowKind::Instruction,
                WorkflowSource::User,
                2,
            ),
            catalog_entry(
                "inspect-alpha",
                "Inspect Alpha",
                "inspect source",
                WorkflowKind::Instruction,
                WorkflowSource::User,
                1,
            ),
        ];
        let build = |tools: Vec<ToolCapabilityMetadata>, skills: Vec<WorkflowCatalogEntry>| {
            CapabilityDiscoveryIndex::from_snapshots(
                tools,
                &WorkflowCatalogSnapshot {
                    revision: 1,
                    entries: skills,
                },
                &WorkflowCatalogSnapshot::default(),
                &CapabilityDiscoveryEligibility::default(),
            )
            .discover(&request("inspect source"))
            .expect("deterministic discovery")
        };

        let mut reversed_tools = tools.clone();
        reversed_tools.reverse();
        let mut reversed_skills = skills.clone();
        reversed_skills.reverse();
        let first = build(tools, skills);
        let second = build(reversed_tools, reversed_skills);

        assert_eq!(
            serde_json::to_vec(&first).expect("serialize first"),
            serde_json::to_vec(&second).expect("serialize second")
        );
    }

    #[test]
    fn ranking_uses_exact_then_name_summary_source_and_stable_ref() {
        let mut builtin = catalog_entry(
            "review-b",
            "Review",
            "Review a change",
            WorkflowKind::Instruction,
            WorkflowSource::Builtin,
            1,
        );
        let workspace = catalog_entry(
            "review-z",
            "Review",
            "Review a change",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            2,
        );
        let workspace_a = catalog_entry(
            "review-a",
            "Review",
            "Review a change",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            3,
        );
        builtin.description = "Review a change with details".to_string();
        let index = index(
            &[],
            vec![builtin, workspace, workspace_a],
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        );
        let ranked = index.discover(&request("review")).expect("ranked");
        assert_eq!(ranked.matches[0].capability_ref, "skill:review-a");
        assert_eq!(ranked.matches[1].capability_ref, "skill:review-z");

        let exact = index.discover(&request("review-b")).expect("exact");
        assert_eq!(exact.matches[0].capability_ref, "skill:review-b");
    }

    #[test]
    fn automatic_skill_accepts_unique_exact_normalized_identities() {
        let skill = catalog_entry(
            "review-helper",
            "Review Helper",
            "Review a code change carefully",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            7,
        );
        let index = index(
            &[],
            vec![skill],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );

        for query in ["review-helper", "skill:review-helper", "Review_Helper"] {
            let matched = index
                .discover_unambiguous_automatic_skill(query)
                .unwrap_or_else(|| panic!("unique exact match for {query}"));
            assert_eq!(matched.capability_ref, "skill:review-helper");
            assert!(matches!(
                matched.invocation_target,
                CapabilityInvocationTarget::Skill {
                    skill_id,
                    source: CapabilitySource::User,
                    revision: 7,
                    ..
                } if skill_id == "review-helper"
            ));
        }
    }

    #[test]
    fn automatic_skill_accepts_only_unique_strong_fuzzy_match() {
        let react = catalog_entry(
            "react-optimizer",
            "React Optimizer",
            "Improve React and Vite build performance",
            WorkflowKind::Instruction,
            WorkflowSource::Project,
            11,
        );
        let index = index(
            &[],
            vec![react],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );

        let matched = index
            .discover_unambiguous_automatic_skill("please improve the react vite build")
            .expect("multiple distinct summary tokens are a strong fuzzy match");
        assert_eq!(matched.capability_ref, "skill:react-optimizer");
    }

    #[test]
    fn automatic_skill_ignores_a_higher_ranked_weak_candidate() {
        let weak = catalog_entry(
            "only-alpha",
            "only-alpha",
            "only-alpha project skill",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            1,
        );
        let strong = catalog_entry(
            "shared-workflow",
            "shared-workflow",
            "alpha needle workflow",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            2,
        );
        let index = index(
            &[],
            vec![weak, strong],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );

        assert_eq!(
            index
                .discover_unambiguous_automatic_skill("alpha needle")
                .expect("the unique strong summary match")
                .capability_ref,
            "skill:shared-workflow"
        );
    }

    #[test]
    fn automatic_skill_abstains_for_weak_tied_and_missing_matches() {
        let weak = catalog_entry(
            "artifact-helper",
            "Artifact Helper",
            "Handles PDF artifacts",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        let first = catalog_entry(
            "review-first",
            "Review Assistant",
            "Review code changes carefully",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            2,
        );
        let mut second = catalog_entry(
            "review-second",
            "Review Assistant",
            "Review code changes carefully",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            3,
        );
        second.invocation_policy = json!({"explicit": false, "automatic": true});
        let policy_only_index = index(
            &[],
            vec![weak.clone()],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );
        assert!(policy_only_index
            .discover_unambiguous_automatic_skill("please use explicit automatic mode")
            .is_none());
        let index = index(
            &[],
            vec![weak, first, second],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );

        assert!(index.discover_unambiguous_automatic_skill("pdf").is_none());
        assert!(index
            .discover_unambiguous_automatic_skill("please review code changes")
            .is_none());
        assert!(index
            .discover_unambiguous_automatic_skill("explicit review code")
            .is_none());
        assert!(index
            .discover_unambiguous_automatic_skill("unrelated request")
            .is_none());
        assert!(index.discover_unambiguous_automatic_skill("").is_none());
    }

    #[test]
    fn automatic_skill_abstains_when_an_exact_display_name_is_not_unique() {
        let first = catalog_entry(
            "review-first",
            "Shared Review",
            "Review code",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        let second = catalog_entry(
            "review-second",
            "Shared Review",
            "Review another change",
            WorkflowKind::Instruction,
            WorkflowSource::Workspace,
            2,
        );
        let index = index(
            &[],
            vec![first, second],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );

        assert!(index
            .discover_unambiguous_automatic_skill("Shared Review")
            .is_none());
        assert_eq!(
            index
                .discover_unambiguous_automatic_skill("review-first")
                .expect("canonical id remains unique")
                .capability_ref,
            "skill:review-first"
        );
    }

    #[test]
    fn automatic_skill_bounds_long_user_hints_without_failing() {
        let skill = catalog_entry(
            "review",
            "Review",
            "Review code changes",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        let index = index(
            &[],
            vec![skill],
            Vec::new(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );
        let hint = format!("review {}", "界".repeat(MAX_DISCOVERY_QUERY_CHARS + 20));

        assert_eq!(
            index
                .discover_unambiguous_automatic_skill(&hint)
                .expect("bounded prefix keeps the match")
                .capability_ref,
            "skill:review"
        );
        assert!(matches!(
            index.discover(&request(&hint)),
            Err(CapabilityDiscoveryError::QueryTooLong { .. })
        ));
    }

    #[test]
    fn disabled_surface_ineligible_invalid_shadowed_and_unavailable_entries_are_filtered() {
        let mut invalid = catalog_entry(
            "invalid-skill",
            "Inspect Invalid",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        invalid.status = WorkflowStatus::Invalid;
        invalid.last_error = Some("private invalid diagnostic".to_string());
        let mut shadowed = catalog_entry(
            "shadowed-skill",
            "Inspect Shadowed",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            2,
        );
        shadowed.winner = false;
        let disabled = catalog_entry(
            "disabled-skill",
            "Inspect Disabled",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            3,
        );
        let mut denied = catalog_entry(
            "denied-skill",
            "Inspect Denied",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            4,
        );
        denied.invocation_policy = json!({"explicit": false, "automatic": false});
        let allowed = catalog_entry(
            "allowed-skill",
            "Inspect Allowed",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            5,
        );
        let eligibility = CapabilityDiscoveryEligibility {
            disabled_tool_names: BTreeSet::from(["HiddenTool".to_string()]),
            allowed_tool_names: Some(BTreeSet::from(["VisibleTool".to_string()])),
            disabled_skill_ids: BTreeSet::from(["disabled-skill".to_string()]),
            allowed_skill_ids: Some(BTreeSet::from(["allowed-skill".to_string()])),
            workflow_gateway_available: false,
            ..Default::default()
        };
        let workflow = catalog_entry(
            "hidden-workflow",
            "Inspect Workflow",
            "inspect",
            WorkflowKind::Orchestration,
            WorkflowSource::User,
            6,
        );
        let index = index(
            &[
                schema("HiddenTool", "inspect", json!({})),
                schema("SurfaceHiddenTool", "inspect", json!({})),
                schema("VisibleTool", "inspect", json!({})),
            ],
            vec![invalid, shadowed, disabled, denied, allowed],
            vec![workflow],
            eligibility,
        );
        let result = index.discover(&request("inspect")).expect("filtered");
        assert_eq!(
            result
                .matches
                .iter()
                .map(|candidate| candidate.capability_ref.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["skill:allowed-skill", "tool:VisibleTool"])
        );
    }

    #[test]
    fn each_catalog_eligibility_predicate_fails_closed_independently() {
        let is_visible = |entry: WorkflowCatalogEntry,
                          eligibility: CapabilityDiscoveryEligibility| {
            let query = entry.id.clone();
            !index(&[], vec![entry], Vec::new(), eligibility)
                .discover(&request(&query))
                .expect("eligibility query")
                .matches
                .is_empty()
        };

        let eligible = catalog_entry(
            "eligible-skill",
            "Eligible Skill",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        assert!(is_visible(
            eligible.clone(),
            CapabilityDiscoveryEligibility::default()
        ));

        let mut invalid = eligible.clone();
        invalid.status = WorkflowStatus::Invalid;
        assert!(!is_visible(
            invalid,
            CapabilityDiscoveryEligibility::default()
        ));

        let mut shadowed = eligible.clone();
        shadowed.winner = false;
        assert!(!is_visible(
            shadowed,
            CapabilityDiscoveryEligibility::default()
        ));

        assert!(!is_visible(
            eligible.clone(),
            CapabilityDiscoveryEligibility {
                disabled_skill_ids: BTreeSet::from([eligible.id.clone()]),
                ..Default::default()
            }
        ));

        let mut denied = eligible.clone();
        denied.invocation_policy = json!({"explicit": false, "automatic": false});
        assert!(!is_visible(
            denied,
            CapabilityDiscoveryEligibility::default()
        ));

        let mut explicit_only = eligible.clone();
        explicit_only.invocation_policy = json!({"explicit": true, "automatic": false});
        assert!(is_visible(
            explicit_only.clone(),
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Explicit,
                ..Default::default()
            }
        ));
        assert!(!is_visible(
            explicit_only,
            CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            }
        ));

        assert!(!is_visible(
            eligible,
            CapabilityDiscoveryEligibility {
                skill_gateway_available: false,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn tool_disable_and_surface_allowlist_are_independent() {
        let tools = [
            ToolCapabilityMetadata {
                canonical_name: "DisabledTool".to_string(),
                summary: "inspect".to_string(),
                source: CapabilitySource::Custom,
                aliases: Vec::new(),
                available: true,
            },
            ToolCapabilityMetadata {
                canonical_name: "AllowedTool".to_string(),
                summary: "inspect".to_string(),
                source: CapabilitySource::Custom,
                aliases: Vec::new(),
                available: true,
            },
            ToolCapabilityMetadata {
                canonical_name: "SurfaceHiddenTool".to_string(),
                summary: "inspect".to_string(),
                source: CapabilitySource::Custom,
                aliases: Vec::new(),
                available: true,
            },
        ];
        let disabled_result = CapabilityDiscoveryIndex::from_snapshots(
            tools.clone(),
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility {
                disabled_tool_names: BTreeSet::from(["DisabledTool".to_string()]),
                ..Default::default()
            },
        )
        .discover(&request("inspect"))
        .expect("disabled tool eligibility");
        assert_eq!(
            disabled_result
                .matches
                .iter()
                .map(|candidate| candidate.capability_ref.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["tool:AllowedTool", "tool:SurfaceHiddenTool"])
        );

        let surface_result = CapabilityDiscoveryIndex::from_snapshots(
            tools,
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility {
                allowed_tool_names: Some(BTreeSet::from(["AllowedTool".to_string()])),
                ..Default::default()
            },
        )
        .discover(&request("inspect"))
        .expect("tool surface eligibility");

        assert_eq!(surface_result.matches.len(), 1);
        assert_eq!(surface_result.matches[0].capability_ref, "tool:AllowedTool");
    }

    #[test]
    fn unavailable_tool_metadata_and_skill_gateway_fail_closed() {
        let skill = catalog_entry(
            "hidden-skill",
            "Inspect Skill",
            "inspect",
            WorkflowKind::Instruction,
            WorkflowSource::User,
            1,
        );
        let eligibility = CapabilityDiscoveryEligibility {
            skill_gateway_available: false,
            ..Default::default()
        };
        let index = CapabilityDiscoveryIndex::from_snapshots(
            [
                ToolCapabilityMetadata {
                    canonical_name: "UnavailableTool".to_string(),
                    summary: "inspect".to_string(),
                    source: CapabilitySource::Custom,
                    aliases: Vec::new(),
                    available: false,
                },
                ToolCapabilityMetadata {
                    canonical_name: "AvailableTool".to_string(),
                    summary: "inspect".to_string(),
                    source: CapabilitySource::Custom,
                    aliases: Vec::new(),
                    available: true,
                },
            ],
            &WorkflowCatalogSnapshot {
                revision: 1,
                entries: vec![skill],
            },
            &WorkflowCatalogSnapshot::default(),
            &eligibility,
        );
        let result = index.discover(&request("inspect")).expect("fail closed");

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:AvailableTool");
    }

    #[test]
    fn projection_excludes_schemas_paths_diagnostics_and_shadowed_metadata() {
        const SECRET: &str = "PRIVATE-DISCOVERY-SENTINEL";
        const PATH: &str = "/private/hidden/workflow.yaml";
        let schemas = [schema(
            "SafeTool",
            "Inspect safely",
            json!({"secret": SECRET, "path": PATH}),
        )];
        let mut skill = catalog_entry(
            "safe-skill",
            "Safe Skill",
            "Inspect safely",
            WorkflowKind::Instruction,
            WorkflowSource::Project,
            42,
        );
        skill.argument_schema = json!({"secret": SECRET, "path": PATH});
        skill.content_digest = SECRET.to_string();
        skill.last_error = Some(format!("{SECRET} at {PATH}"));
        skill.invocation_policy = json!({
            "explicit": true,
            "automatic": false,
            "credential": SECRET,
            "workspace_path": PATH,
            "input_schema": {"description": SECRET},
            "padding": "x".repeat(16_384)
        });
        skill.shadowed_candidates = vec![ShadowedWorkflowCandidate {
            source: WorkflowSource::Plugin,
            status: WorkflowStatus::Invalid,
            legacy: false,
            migration_status: None,
            last_error: Some(format!("{SECRET} at {PATH}")),
        }];
        let result = index(
            &schemas,
            vec![skill],
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("inspect safely"))
        .expect("safe projection");
        let serialized = serde_json::to_string(&result).expect("serialize result");

        assert!(!serialized.contains(SECRET));
        assert!(!serialized.contains(PATH));
        assert!(serialized.contains("\"revision\":42"));
        assert!(serialized.contains("\"source\":\"project\""));
        assert!(serialized.contains("\"invocation_policy\""));
        assert!(!serialized.contains("credential"));
        assert!(!serialized.contains("workspace_path"));
        assert!(!serialized.contains("input_schema"));
        assert!(serialized.len() < 2_048);
    }

    #[test]
    fn tool_projection_keeps_exact_alias_shadow_separate_and_bounds_summaries() {
        let long = "x".repeat(MAX_CAPABILITY_SUMMARY_CHARS + 40);
        let metadata = project_tool_capability_metadata(&[
            schema("Edit", &long, json!({})),
            schema("apply_patch", "short alias", json!({})),
        ]);
        assert_eq!(metadata.len(), 2);
        let edit = metadata
            .iter()
            .find(|entry| entry.canonical_name == "Edit")
            .expect("builtin Edit projection");
        assert_eq!(edit.source, CapabilitySource::Builtin);
        assert_eq!(edit.aliases, vec!["apply_patch"]);
        let custom_alias = metadata
            .iter()
            .find(|entry| entry.canonical_name == "apply_patch")
            .expect("exact custom alias projection");
        assert_eq!(custom_alias.source, CapabilitySource::Custom);
        assert!(custom_alias.aliases.is_empty());
        let result = CapabilityDiscoveryIndex::from_snapshots(
            metadata,
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("edit"))
        .expect("deduplicated");

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:Edit");
        assert!(result.matches[0].summary.chars().count() <= MAX_CAPABILITY_SUMMARY_CHARS);
    }

    #[test]
    fn framework_source_and_alias_metadata_require_exact_registered_identity() {
        let metadata = project_tool_capability_metadata(&[
            schema("Bash", "canonical builtin", json!({})),
            schema("bash", "custom lowercase", json!({})),
            schema("Project", "canonical server", json!({})),
            schema("project", "custom lowercase", json!({})),
            schema("Edit", "canonical edit", json!({})),
            schema("edit", "custom lowercase", json!({})),
            schema("mcp__alpha__inspect", "canonical mcp alias", json!({})),
            schema("MCP__alpha__inspect", "custom uppercase prefix", json!({})),
        ]);
        let entry = |name: &str| {
            metadata
                .iter()
                .find(|entry| entry.canonical_name == name)
                .unwrap_or_else(|| panic!("missing {name}"))
        };

        assert_eq!(entry("Bash").source, CapabilitySource::Builtin);
        assert_eq!(entry("bash").source, CapabilitySource::Custom);
        assert_eq!(entry("Project").source, CapabilitySource::Server);
        assert_eq!(entry("project").source, CapabilitySource::Custom);
        assert_eq!(entry("Edit").aliases, vec!["apply_patch"]);
        assert!(entry("edit").aliases.is_empty());
        assert_eq!(entry("mcp__alpha__inspect").source, CapabilitySource::Mcp);
        assert_eq!(
            entry("MCP__alpha__inspect").source,
            CapabilitySource::Custom
        );
    }

    #[test]
    fn raw_and_classified_projections_exclude_host_only_and_keep_deferred_tools() {
        let schemas = [
            schema("Bash", "inspect core", json!({})),
            schema("Glob", "inspect deferred", json!({})),
            schema("custom_tool", "inspect custom", json!({})),
            schema("mcp__alpha__inspect", "inspect mcp", json!({})),
            schema("Workspace", "inspect host", json!({})),
            schema("conclusion_with_options", "inspect host", json!({})),
            schema("request_permissions", "inspect host", json!({})),
        ];
        let raw = project_tool_capability_metadata(&schemas);
        let classified = schemas
            .iter()
            .cloned()
            .filter_map(ClassifiedToolSchema::new)
            .collect::<Vec<_>>();
        let typed = project_classified_tool_capability_metadata(&classified);
        let names = |items: &[ToolCapabilityMetadata]| {
            items
                .iter()
                .map(|item| item.canonical_name.clone())
                .collect::<BTreeSet<_>>()
        };

        assert_eq!(names(&raw), names(&typed));
        assert_eq!(
            names(&raw),
            BTreeSet::from([
                "Bash".to_string(),
                "Glob".to_string(),
                "custom_tool".to_string(),
                "mcp__alpha__inspect".to_string(),
            ])
        );
    }

    #[test]
    fn snapshot_boundary_rejects_forged_host_only_metadata() {
        let mut metadata = [
            "Workspace",
            "conclusion_with_options",
            "request_permissions",
            "GetCurrentDir",
            "SetWorkspace",
        ]
        .into_iter()
        .map(|name| ToolCapabilityMetadata {
            canonical_name: name.to_string(),
            summary: "inspect host".to_string(),
            source: CapabilitySource::Custom,
            aliases: Vec::new(),
            available: true,
        })
        .collect::<Vec<_>>();
        metadata.push(ToolCapabilityMetadata {
            canonical_name: "VisibleTool".to_string(),
            summary: "inspect visible".to_string(),
            source: CapabilitySource::Custom,
            aliases: Vec::new(),
            available: true,
        });
        let result = CapabilityDiscoveryIndex::from_snapshots(
            metadata,
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("inspect"))
        .expect("host-only boundary");

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:VisibleTool");
    }

    #[test]
    fn disabled_aliases_use_the_same_canonical_identity_as_discovery() {
        let eligibility = CapabilityDiscoveryEligibility {
            disabled_tool_names: BTreeSet::from([
                "apply_patch".to_string(),
                "FileExists".to_string(),
                "sub_session_manager".to_string(),
            ]),
            ..Default::default()
        };
        let result = index(
            &[
                schema("Edit", "inspect edit", json!({})),
                schema("GetFileInfo", "inspect file", json!({})),
                schema("SubAgent", "inspect agent", json!({})),
                schema("VisibleTool", "inspect visible", json!({})),
            ],
            Vec::new(),
            Vec::new(),
            eligibility,
        )
        .discover(&request("inspect"))
        .expect("canonical disabled filter");

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:VisibleTool");
    }

    #[test]
    fn eligibility_resolves_exact_shadow_before_alias_fallback() {
        let schemas = [
            schema("Edit", "builtin edit", json!({})),
            schema("apply_patch", "custom exact patch", json!({})),
        ];
        let allowed_builtin = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility {
                allowed_tool_names: Some(BTreeSet::from(["Edit".to_string()])),
                ..Default::default()
            },
        )
        .discover(&request("patch"))
        .expect("exact allowed builtin");
        assert_eq!(allowed_builtin.matches.len(), 1);
        assert_eq!(allowed_builtin.matches[0].capability_ref, "tool:Edit");

        let allowed_shadow = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility {
                allowed_tool_names: Some(BTreeSet::from(["apply_patch".to_string()])),
                ..Default::default()
            },
        )
        .discover(&request("patch"))
        .expect("exact allowed custom shadow");
        assert_eq!(allowed_shadow.matches.len(), 1);
        assert_eq!(allowed_shadow.matches[0].capability_ref, "tool:apply_patch");

        let alias_fallback = index(
            &[schema("Edit", "builtin edit", json!({}))],
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility {
                allowed_tool_names: Some(BTreeSet::from(["apply_patch".to_string()])),
                ..Default::default()
            },
        )
        .discover(&request("patch"))
        .expect("unshadowed alias fallback");
        assert_eq!(alias_fallback.matches.len(), 1);
        assert_eq!(alias_fallback.matches[0].capability_ref, "tool:Edit");

        let disabled_shadow = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility {
                disabled_tool_names: BTreeSet::from(["apply_patch".to_string()]),
                ..Default::default()
            },
        )
        .discover(&request("edit"))
        .expect("exact disabled custom shadow");
        assert_eq!(disabled_shadow.matches.len(), 1);
        assert_eq!(disabled_shadow.matches[0].capability_ref, "tool:Edit");
    }

    #[test]
    fn case_distinct_dynamic_and_mcp_identities_do_not_collapse() {
        let metadata = project_tool_capability_metadata(&[
            schema("Foo", "inspect custom", json!({})),
            schema("foo", "inspect custom", json!({})),
            schema("mcp__Alpha__inspect", "inspect mcp", json!({})),
            schema("mcp__alpha__inspect", "inspect mcp", json!({})),
        ]);
        assert_eq!(metadata.len(), 4);

        let eligibility = CapabilityDiscoveryEligibility {
            disabled_tool_names: BTreeSet::from(["mcp__Alpha__inspect".to_string()]),
            ..Default::default()
        };
        let result = CapabilityDiscoveryIndex::from_snapshots(
            metadata,
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &eligibility,
        )
        .discover(&request("inspect"))
        .expect("case-preserving discovery");
        let refs = result
            .matches
            .iter()
            .map(|entry| entry.capability_ref.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(refs.len(), 3);
        assert!(refs.contains("tool:Foo"));
        assert!(refs.contains("tool:foo"));
        assert!(refs.contains("tool:mcp__alpha__inspect"));
        assert!(!refs.contains("tool:mcp__Alpha__inspect"));

        let allowed = CapabilityDiscoveryIndex::from_snapshots(
            project_tool_capability_metadata(&[
                schema("Foo", "inspect custom", json!({})),
                schema("foo", "inspect custom", json!({})),
                schema("mcp__Alpha__inspect", "inspect mcp", json!({})),
                schema("mcp__alpha__inspect", "inspect mcp", json!({})),
            ]),
            &WorkflowCatalogSnapshot::default(),
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility {
                allowed_tool_names: Some(BTreeSet::from([
                    "Foo".to_string(),
                    "mcp__alpha__inspect".to_string(),
                ])),
                ..Default::default()
            },
        )
        .discover(&request("inspect"))
        .expect("case-preserving allowlist");
        let allowed_refs = allowed
            .matches
            .iter()
            .map(|entry| entry.capability_ref.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            allowed_refs,
            BTreeSet::from(["tool:Foo", "tool:mcp__alpha__inspect"])
        );
    }

    #[test]
    fn ordinary_discover_named_function_is_searchable_and_disableable() {
        let schemas = [schema(
            "discover",
            "discover a custom data source",
            json!({}),
        )];
        let visible = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility::default(),
        )
        .discover(&request("discover custom"))
        .expect("ordinary discover-named function");
        assert_eq!(visible.matches.len(), 1);
        assert_eq!(visible.matches[0].capability_ref, "tool:discover");

        let hidden = index(
            &schemas,
            Vec::new(),
            Vec::new(),
            CapabilityDiscoveryEligibility {
                disabled_tool_names: BTreeSet::from(["discover".to_string()]),
                ..Default::default()
            },
        )
        .discover(&request("discover custom"))
        .expect("disabled ordinary function");
        assert!(hidden.matches.is_empty());
    }

    #[tokio::test]
    async fn store_facade_builds_from_atomic_metadata_snapshots() {
        let store = crate::SkillStore::default();
        let catalog = [schema(
            "SafeTool",
            "Inspect safely",
            json!({"private_schema": "must-not-be-read"}),
        )]
        .into_iter()
        .filter_map(ClassifiedToolSchema::new)
        .collect::<Vec<_>>();
        let index = CapabilityDiscoveryIndex::from_resolved_classified_store(
            &catalog,
            &store,
            &CapabilityDiscoveryEligibility::default(),
        )
        .await;

        let result = index
            .discover(&request("inspect safely"))
            .expect("metadata-only store projection");
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:SafeTool");

        let edit_schema = schema("Edit", "Edit safely", json!({}));
        let resolved_catalog = [edit_schema.clone()]
            .into_iter()
            .filter_map(ClassifiedToolSchema::new)
            .collect::<Vec<_>>();
        let disabled_alias = CapabilityDiscoveryEligibility {
            disabled_tool_names: BTreeSet::from(["apply_patch".to_string()]),
            ..Default::default()
        };
        let resolved = CapabilityDiscoveryIndex::from_resolved_classified_store(
            &resolved_catalog,
            &store,
            &disabled_alias,
        )
        .await
        .discover(&request("edit"))
        .expect("resolved catalogs must not be filtered twice");
        assert_eq!(resolved.matches[0].capability_ref, "tool:Edit");

        let legacy = CapabilityDiscoveryIndex::from_store(&[edit_schema], &store, &disabled_alias)
            .await
            .discover(&request("edit"))
            .expect("raw schemas still resolve tool eligibility");
        assert!(legacy.matches.is_empty());
    }
}
