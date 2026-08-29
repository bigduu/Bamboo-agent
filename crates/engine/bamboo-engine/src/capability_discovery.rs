//! Deterministic, provider-neutral discovery over capability metadata.
//!
//! This module owns no provider wire format and no active capability state. It
//! projects the canonical tool registry and immutable Skill/Workflow catalogs
//! into a small metadata-only index, then ranks bounded queries without I/O.

use std::collections::{BTreeMap, BTreeSet};

use bamboo_agent_core::ToolSchema;
use bamboo_domain::{
    CapabilityInvocationPolicy, CapabilityInvocationTarget, CapabilityKind, CapabilityMatch,
    CapabilitySource, CapabilityStatus, DiscoverCapabilitiesRequest, DiscoverCapabilitiesResult,
    DISCOVER_CAPABILITY_NAME, MAX_DISCOVERY_QUERY_CHARS, MAX_DISCOVERY_RESULTS,
};
use bamboo_skills::{
    WorkflowCatalogEntry, WorkflowCatalogSnapshot, WorkflowKind, WorkflowSource, WorkflowStatus,
};
use thiserror::Error;

/// Discovery summaries are bounded independently of the source description so
/// one verbose registry entry cannot dominate a bounded result.
pub const MAX_CAPABILITY_SUMMARY_CHARS: usize = 240;

const SERVER_CAPABILITY_NAMES: &[&str] = &[
    "Project",
    "ask_agent",
    "cluster",
    "compact_context",
    "deploy_agent",
    "ledger",
    "load_skill",
    "memory",
    "notify",
    "read_skill_resource",
    "scheduler",
    "session_history",
    "SubAgent",
    "workflow_run",
];

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
    /// `Some(empty)` exposes no tools. Names are canonicalized before matching.
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
            let canonical_name = canonical_registry_tool_name(&schema.function.name);
            if canonical_name.is_empty() {
                return None;
            }
            Some(ToolCapabilityMetadata {
                source: source_for_tool(&canonical_name),
                aliases: aliases_for_tool(&canonical_name),
                canonical_name,
                summary: bounded_summary(&schema.function.description),
                available: true,
            })
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
    /// Capture both command catalogs under the store's publication read lock and
    /// build one immutable index. `command_catalog_snapshots` is the atomic
    /// counterpart of `skill_catalog_snapshot` + `workflow_catalog_snapshot`;
    /// it returns the same metadata-only entries without opening bundle files.
    pub async fn from_store(
        tool_schemas: &[ToolSchema],
        store: &bamboo_skills::SkillStore,
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
        let disabled_tools = eligibility
            .disabled_tool_names
            .iter()
            .map(|name| canonical_registry_tool_name(name).to_lowercase())
            .collect::<BTreeSet<_>>();
        let allowed_tools = eligibility.allowed_tool_names.as_ref().map(|names| {
            names
                .iter()
                .map(|name| canonical_registry_tool_name(name).to_lowercase())
                .collect::<BTreeSet<_>>()
        });
        let mut projected_tools = BTreeMap::<String, ToolCapabilityMetadata>::new();

        for mut tool in tools {
            tool.canonical_name = canonical_registry_tool_name(&tool.canonical_name);
            if tool.canonical_name.is_empty()
                || tool
                    .canonical_name
                    .eq_ignore_ascii_case(DISCOVER_CAPABILITY_NAME)
                || !tool.available
            {
                continue;
            }
            let key = tool.canonical_name.to_lowercase();
            if disabled_tools.contains(&key)
                || allowed_tools
                    .as_ref()
                    .is_some_and(|allowed| !allowed.contains(&key))
            {
                continue;
            }
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

        let mut ranked = self
            .candidates
            .iter()
            .filter(|candidate| {
                kinds
                    .as_ref()
                    .is_none_or(|kinds| kinds.contains(&candidate.value.kind))
            })
            .filter_map(|candidate| {
                rank_candidate(candidate, &query, &normalized_query, &query_tokens)
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
struct SearchRank {
    exact_identity: bool,
    name_phrase: bool,
    name_token_hits: usize,
    summary_phrase: bool,
    summary_token_hits: usize,
    source_precedence: u8,
}

fn rank_candidate(
    candidate: &IndexedCapability,
    raw_query: &str,
    normalized_query: &str,
    query_tokens: &BTreeSet<String>,
) -> Option<SearchRank> {
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
    let summary_phrase = candidate.normalized_summary.contains(normalized_query);
    let summary_token_hits = token_hits(&candidate.normalized_summary, query_tokens);

    if !exact_identity
        && !name_phrase
        && name_token_hits == 0
        && !summary_phrase
        && summary_token_hits == 0
    {
        return None;
    }

    Some(SearchRank {
        exact_identity,
        name_phrase,
        name_token_hits,
        summary_phrase,
        summary_token_hits,
        source_precedence: source_precedence(candidate.value.source),
    })
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
        CapabilityInvocationTarget::Tool { name } if name.eq_ignore_ascii_case(&canonical)
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

fn capability_source(source: WorkflowSource) -> CapabilitySource {
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
    if bamboo_domain::BUILTIN_TOOL_NAMES
        .iter()
        .any(|builtin| builtin.eq_ignore_ascii_case(name))
    {
        CapabilitySource::Builtin
    } else if SERVER_CAPABILITY_NAMES
        .iter()
        .any(|server| server.eq_ignore_ascii_case(name))
    {
        CapabilitySource::Server
    } else if name.to_ascii_lowercase().starts_with("mcp__") {
        CapabilitySource::Mcp
    } else {
        CapabilitySource::Custom
    }
}

fn canonical_registry_tool_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let unqualified = trimmed.split("::").last().unwrap_or(trimmed);
    let normalized = bamboo_domain::normalize_builtin_alias(unqualified);
    let canonical = bamboo_domain::resolve_alias(normalized).unwrap_or(normalized);
    bamboo_domain::BUILTIN_TOOL_NAMES
        .iter()
        .chain(bamboo_domain::SERVER_TOOL_NAMES.iter())
        .chain(SERVER_CAPABILITY_NAMES.iter())
        .find(|known| known.eq_ignore_ascii_case(canonical))
        .map(|known| (*known).to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn aliases_for_tool(canonical_name: &str) -> Vec<String> {
    bamboo_domain::BUILTIN_TOOL_ALIASES
        .iter()
        .filter(|(_, canonical)| canonical.eq_ignore_ascii_case(canonical_name))
        .map(|(alias, _)| (*alias).to_string())
        .collect()
}

fn bounded_summary(value: &str) -> String {
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
    use bamboo_agent_core::FunctionSchema;
    use bamboo_skills::{ShadowedWorkflowCandidate, WorkflowCatalogSnapshot};
    use serde_json::json;

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
    fn tool_projection_deduplicates_canonical_aliases_and_bounds_summaries() {
        let long = "x".repeat(MAX_CAPABILITY_SUMMARY_CHARS + 40);
        let metadata = project_tool_capability_metadata(&[
            schema("Edit", &long, json!({})),
            schema("apply_patch", "short alias", json!({})),
        ]);
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

    #[tokio::test]
    async fn store_facade_builds_from_atomic_metadata_snapshots() {
        let store = bamboo_skills::SkillStore::default();
        let index = CapabilityDiscoveryIndex::from_store(
            &[schema(
                "SafeTool",
                "Inspect safely",
                json!({"private_schema": "must-not-be-read"}),
            )],
            &store,
            &CapabilityDiscoveryEligibility::default(),
        )
        .await;

        let result = index
            .discover(&request("inspect safely"))
            .expect("metadata-only store projection");
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].capability_ref, "tool:SafeTool");
    }
}
