use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    Session,
    Project,
    Global,
}

impl MemoryScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Project => "project",
            Self::Global => "global",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DurableMemoryType {
    User,
    Feedback,
    Project,
    Reference,
}

impl DurableMemoryType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Feedback => "feedback",
            Self::Project => "project",
            Self::Reference => "reference",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DurableMemoryStatus {
    Active,
    Stale,
    Superseded,
    Contradicted,
    Archived,
}

impl DurableMemoryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Superseded => "superseded",
            Self::Contradicted => "contradicted",
            Self::Archived => "archived",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreatedBy {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DurableMemorySource {
    pub kind: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub message_range: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DurableMemoryRelations {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contradicted_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DurableMemoryRetrieval {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<String>,
    #[serde(default)]
    pub embedding_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableMemoryFrontmatter {
    pub id: String,
    pub title: String,
    #[serde(rename = "type")]
    pub r#type: DurableMemoryType,
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub status: DurableMemoryStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub created_by: CreatedBy,
    pub updated_by: CreatedBy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DurableMemorySource>,
    #[serde(default)]
    pub relations: DurableMemoryRelations,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default)]
    pub retrieval: DurableMemoryRetrieval,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableMemoryDocument {
    pub frontmatter: DurableMemoryFrontmatter,
    pub body: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableContentLocation {
    pub scope: MemoryScope,
    pub project_key: Option<String>,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableMemoryRef {
    pub id: String,
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionState {
    pub version: u32,
    pub session_id: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_extracted_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_compacted_at: Option<String>,
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryQueryOptions {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub max_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_related: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryQueryItem {
    pub id: String,
    pub title: String,
    #[serde(rename = "type")]
    pub r#type: DurableMemoryType,
    pub scope: MemoryScope,
    pub status: DurableMemoryStatus,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub relevance: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryQueryCursor {
    pub value: String,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryQueryResult {
    pub items: Vec<MemoryQueryItem>,
    pub returned_count: usize,
    pub matched_count: usize,
    pub truncated: bool,
    pub remaining_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryInspectResult {
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub total_memories: usize,
    #[serde(default)]
    pub by_type: BTreeMap<String, usize>,
    #[serde(default)]
    pub by_status: BTreeMap<String, usize>,
    #[serde(default)]
    pub recent_ids: Vec<String>,
    #[serde(default)]
    pub view_files: Vec<String>,
    #[serde(default)]
    pub index_files: Vec<String>,
    #[serde(default)]
    pub state_files: Vec<String>,
    #[serde(default)]
    pub stale_candidate_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reindex_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dream_at: Option<String>,
    #[serde(default)]
    pub topic_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryMergeResult {
    pub merged_id: String,
    pub target_scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub appended: bool,
    #[serde(default)]
    pub tags_updated: bool,
    #[serde(default)]
    pub superseded_ids: Vec<String>,
    pub path: PathBuf,
}

/// One atomic piece produced when splitting a multi-topic "blob" memory.
#[derive(Debug, Clone)]
pub struct MemorySplitPiece {
    pub title: String,
    pub r#type: Option<DurableMemoryType>,
    pub content: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemorySplitResult {
    pub source_id: String,
    pub target_scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub new_ids: Vec<String>,
}

/// A lexically-similar existing memory surfaced for duplicate review. Produced by
/// `find_duplicate_candidates`; never auto-merged — the caller (an LLM) judges
/// whether it is the same fact and then writes/merges/splits explicitly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryDuplicateCandidate {
    pub id: String,
    pub title: String,
    pub r#type: DurableMemoryType,
    pub scope: MemoryScope,
    pub score: f64,
    pub snippet: String,
}

/// One memory flagged by the deterministic blob prefilter (no LLM involved).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobScanItem {
    pub id: String,
    pub title: String,
    /// Number of `---`-separated sections beyond the first (merge accretions).
    pub appended_sections: usize,
    pub body_chars: usize,
    pub over_cap: bool,
}

/// Deterministic prefilter report: which active memories look like multi-topic /
/// transcript "blobs" and are worth LLM-driven split. Free to compute; this is the
/// always-on, zero-cost half of the gardener.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobScanReport {
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub scanned: usize,
    pub flagged: usize,
    pub threshold: usize,
    pub items: Vec<BlobScanItem>,
}

/// One member of a near-duplicate cluster surfaced by the deterministic dedup
/// prefilter (no LLM involved).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DuplicateClusterMember {
    pub id: String,
    pub title: String,
    pub r#type: DurableMemoryType,
    pub snippet: String,
}

/// A group of active memories that look like near-duplicates of each other
/// (pairwise content-keyword Jaccard ≥ threshold). NEVER auto-merged — the caller
/// (an LLM) judges whether they are the same fact and then consolidates explicitly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DuplicateCluster {
    pub members: Vec<DuplicateClusterMember>,
    /// Highest pairwise similarity within the cluster (worst-first ranking signal).
    pub max_score: f64,
}

/// Deterministic dedup prefilter report: clusters of near-duplicate active
/// memories worth LLM-driven consolidation. Free to compute; the always-on,
/// zero-cost half of the dedup gardener.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DuplicateScanReport {
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub scanned: usize,
    /// Total active memories that landed in some cluster.
    pub clustered: usize,
    pub threshold: f64,
    pub clusters: Vec<DuplicateCluster>,
}

/// Result of consolidating N near-duplicate memories into one canonical memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryConsolidateResult {
    pub new_id: String,
    pub target_scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub superseded_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryPurgeResult {
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub mode: DurableMemoryStatus,
    pub matched_count: usize,
    #[serde(default)]
    pub updated_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryContradictionResult {
    pub target_id: String,
    pub target_scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub contradicted_ids: Vec<String>,
    #[serde(default)]
    pub missing_ids: Vec<String>,
    pub path: PathBuf,
}
