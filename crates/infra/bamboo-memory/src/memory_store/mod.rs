use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

mod access_log;
pub mod embedding;
pub mod freshness;
mod lexical_bm25;
pub mod paths;
pub mod recall;
pub mod store;
pub mod types;

pub use freshness::{
    memory_age_days, memory_age_label, memory_freshness_text, render_memory_freshness_note,
    FreshnessKind,
};
pub use paths::{
    LegacyProjectMemoryReadRoot, MemoryPathResolver, ProjectMemoryPathResolver, SESSIONS_DIR,
    TOPICS_DIR,
};
pub use recall::{
    select_relevant_memories, shortlist_relevant_memories, MemoryRecallCandidate,
    MemoryRecallOptions, MemoryRecallRerankContext, MemoryRecallSelection, MemoryRecallStrategy,
};
pub use store::MemoryStore;
pub use types::{
    BlobScanItem, BlobScanReport, CreatedBy, DuplicateCluster, DuplicateClusterMember,
    DuplicateScanReport, DurableContentLocation, DurableMemoryDocument, DurableMemoryFrontmatter,
    DurableMemoryRef, DurableMemoryRelations, DurableMemoryRetrieval, DurableMemorySource,
    DurableMemoryStatus, DurableMemoryType, MemoryConsolidateResult, MemoryContradictionResult,
    MemoryDuplicateCandidate, MemoryInspectResult, MemoryMergeResult, MemoryPurgeResult,
    MemoryQueryCursor, MemoryQueryItem, MemoryQueryOptions, MemoryQueryResult, MemoryScope,
    MemorySplitPiece, MemorySplitResult, SessionState, TemporalGranularity,
};

pub const MEMORY_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SESSION_TOPIC: &str = "default";
pub const MAX_SESSION_TOPIC_LEN: usize = 50;
pub const MAX_MEMORY_TITLE_LEN: usize = 160;
pub const MAX_MEMORY_TAGS: usize = 32;
pub const DEFAULT_QUERY_LIMIT: usize = 5;
pub const MAX_QUERY_LIMIT: usize = 20;
pub const DEFAULT_MAX_CHARS: usize = 3_000;
pub const MAX_MAX_CHARS: usize = 6_000;
pub const WRITE_AUDIT_LOG: &str = "write_audit.jsonl";
pub const MERGE_AUDIT_LOG: &str = "merge_audit.jsonl";
pub const PURGE_AUDIT_LOG: &str = "purge_audit.jsonl";
pub const CONTRADICTION_AUDIT_LOG: &str = "contradiction_audit.jsonl";
pub const DREAM_VIEW_FILE: &str = "DREAM_NOTEBOOK.md";
pub const MEMORY_VIEW_FILE: &str = "MEMORY.md";
pub const RECENT_VIEW_FILE: &str = "RECENT.md";
pub const STALE_VIEW_FILE: &str = "STALE.md";
pub const LEXICAL_INDEX_FILE: &str = "lexical.json";
pub const GRAPH_INDEX_FILE: &str = "graph.json";
pub const RECENT_INDEX_FILE: &str = "recent.json";
pub const STALE_CANDIDATES_INDEX_FILE: &str = "stale_candidates.json";
pub const TAXONOMY_INDEX_FILE: &str = "taxonomy.json";

pub fn validate_session_id(session_id: &str) -> io::Result<&str> {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session_id cannot be empty",
        ));
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session_id contains invalid path characters",
        ));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session_id contains unsupported characters",
        ));
    }
    Ok(trimmed)
}

pub fn validate_session_topic(topic: &str) -> io::Result<&str> {
    let trimmed = topic.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "topic cannot be empty",
        ));
    }
    if trimmed.len() > MAX_SESSION_TOPIC_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "topic name too long (max {} chars, got {})",
                MAX_SESSION_TOPIC_LEN,
                trimmed.len()
            ),
        ));
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "topic contains invalid path characters",
        ));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "topic must contain only alphanumeric, dash, or underscore characters",
        ));
    }
    Ok(trimmed)
}

pub fn validate_memory_title(title: &str) -> io::Result<&str> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "title cannot be empty",
        ));
    }
    if trimmed.chars().count() > MAX_MEMORY_TITLE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("title too long (max {} chars)", MAX_MEMORY_TITLE_LEN),
        ));
    }
    Ok(trimmed)
}

pub fn normalize_tag(tag: &str) -> Option<String> {
    let trimmed = tag.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_dash = false;
    for ch in trimmed.chars() {
        let normalized = match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' => ch,
            '-' | '_' | ' ' | '.' | '/' => '-',
            _ => continue,
        };
        if normalized == '-' {
            if prev_dash {
                continue;
            }
            prev_dash = true;
            out.push(normalized);
        } else {
            prev_dash = false;
            out.push(normalized);
        }
    }
    let normalized = out.trim_matches('-').to_string();
    (!normalized.is_empty()).then_some(normalized)
}

pub fn normalize_tags<I, S>(tags: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen = BTreeSet::new();
    for tag in tags {
        if let Some(tag) = normalize_tag(tag.as_ref()) {
            seen.insert(tag);
            if seen.len() >= MAX_MEMORY_TAGS {
                break;
            }
        }
    }
    seen.into_iter().collect()
}

pub fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

pub fn count_chars(value: &str) -> usize {
    value.chars().count()
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub fn derive_summary(content: &str, max_chars: usize) -> String {
    let collapsed = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let (summary, truncated) = truncate_chars(&collapsed, max_chars);
    if truncated {
        format!("{}...", summary.trim_end())
    } else {
        summary
    }
}

pub fn extract_keywords(title: &str, content: &str, tags: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    for tag in tags {
        if let Some(tag) = normalize_tag(tag) {
            seen.insert(tag);
        }
    }

    // CJK-aware tokenization (shared with recall's `lexical_bm25::tokenize`) so a
    // bilingual (中文 + English) library stores Chinese keywords too. The old pass
    // keyed on `is_ascii_alphanumeric`, dropping ALL CJK — Chinese docs got no
    // keyword-boost and (since keywords feed the recall index) full-body Chinese
    // content was never indexed beyond the 240-char summary. English tokens keep
    // their prior lowercased form; each Chinese run contributes char bigrams. (#242)
    let combined = format!("{}\n{}", title, content);
    for token in lexical_bm25::tokenize(&combined) {
        seen.insert(token);
    }

    seen.into_iter().take(128).collect()
}

pub fn detect_entities(title: &str, content: &str) -> Vec<String> {
    let mut entities = BTreeSet::new();
    for token in format!("{}\n{}", title, content)
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '/'))
    {
        let trimmed = token.trim();
        if trimmed.len() < 3 {
            continue;
        }
        let has_upper = trimmed.chars().any(|ch| ch.is_ascii_uppercase());
        let has_separator = trimmed.contains('-') || trimmed.contains('_') || trimmed.contains('/');
        if has_upper || has_separator {
            entities.insert(trimmed.to_string());
        }
    }
    entities.into_iter().take(64).collect()
}

pub fn sanitize_component(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    let mut out = String::with_capacity(trimmed.len());
    let mut prev_dash = false;
    for ch in trimmed.chars() {
        let normalized = match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' => ch,
            _ => '-',
        };
        if normalized == '-' {
            if prev_dash {
                continue;
            }
            prev_dash = true;
            out.push('-');
        } else {
            prev_dash = false;
            out.push(normalized);
        }
    }

    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

pub fn project_key_from_path(path: &Path) -> String {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());

    if let Some(root) = find_git_root(&canonical) {
        if let Some(name) = root.file_name().and_then(|value| value.to_str()) {
            let mut key = sanitize_component(name);
            if let Some(hash) =
                short_stable_hash(&bamboo_config::paths::path_to_display_string(&root))
            {
                key.push('-');
                key.push_str(&hash);
            }
            return key;
        }
    }

    if let Some(name) = canonical.file_name().and_then(|value| value.to_str()) {
        let mut key = sanitize_component(name);
        if let Some(hash) =
            short_stable_hash(&bamboo_config::paths::path_to_display_string(&canonical))
        {
            key.push('-');
            key.push_str(&hash);
        }
        return key;
    }

    let raw = bamboo_config::paths::path_to_display_string(&canonical);
    format!(
        "path-{}",
        short_stable_hash(&raw).unwrap_or_else(|| "unknown".to_string())
    )
}

pub fn find_git_root(start: &Path) -> Option<PathBuf> {
    for ancestor in start.ancestors() {
        let git_dir = ancestor.join(".git");
        if git_dir.is_dir() || git_dir.is_file() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

pub fn short_stable_hash(input: &str) -> Option<String> {
    use std::hash::{Hash, Hasher};

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    trimmed.hash(&mut hasher);
    Some(format!("{:08x}", (hasher.finish() & 0xffff_ffff) as u32))
}

pub fn build_yaml_frontmatter(frontmatter: &DurableMemoryFrontmatter) -> io::Result<String> {
    serde_yaml::to_string(frontmatter).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize memory frontmatter: {error}"),
        )
    })
}

pub fn parse_markdown_document(content: &str) -> io::Result<(DurableMemoryFrontmatter, String)> {
    let trimmed = content.trim_start_matches('\u{feff}');
    let Some(rest) = trimmed.strip_prefix("---\n") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frontmatter start marker",
        ));
    };
    let Some(end_idx) = rest.find("\n---\n") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frontmatter end marker",
        ));
    };
    let yaml = &rest[..end_idx];
    let body = &rest[end_idx + "\n---\n".len()..];
    let frontmatter: DurableMemoryFrontmatter = serde_yaml::from_str(yaml).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse memory frontmatter: {error}"),
        )
    })?;
    Ok((frontmatter, body.trim().to_string()))
}

pub fn render_markdown_document(
    frontmatter: &DurableMemoryFrontmatter,
    body: &str,
) -> io::Result<String> {
    let yaml = build_yaml_frontmatter(frontmatter)?;
    Ok(format!("---\n{}---\n\n{}\n", yaml, body.trim()))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LexicalIndex {
    pub generated_at: String,
    pub items: Vec<LexicalIndexItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexicalIndexItem {
    pub id: String,
    pub title: String,
    pub scope: MemoryScope,
    pub project_key: Option<String>,
    pub r#type: DurableMemoryType,
    pub status: DurableMemoryStatus,
    pub tags: Vec<String>,
    pub keywords: Vec<String>,
    pub entities: Vec<String>,
    pub updated_at: String,
    pub created_at: String,
    pub summary: String,
    /// Optional temporal granularity carried from the source frontmatter so recall
    /// can rank by cache-stability without re-reading every document. Back-compat:
    /// older `lexical.json` index files predate this field and deserialize to `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub granularity: Option<TemporalGranularity>,
    /// Optional dense embedding (title+keywords+summary), populated at index-build
    /// time WHEN a [`embedding::MemoryEmbedder`] backend is configured. Reserved by
    /// L1: absent today (recall is pure BM25) so this seam is inert; when present,
    /// recall adds a β·cosine term. Back-compat: older index files → `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecentIndex {
    pub generated_at: String,
    pub items: Vec<RecentIndexItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentIndexItem {
    pub id: String,
    pub title: String,
    pub updated_at: String,
    pub last_accessed_at: Option<String>,
    pub status: DurableMemoryStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GraphIndex {
    pub generated_at: String,
    pub items: Vec<GraphIndexItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphIndexItem {
    pub id: String,
    pub related: Vec<String>,
    pub supersedes: Vec<String>,
    pub contradicted_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StaleCandidatesIndex {
    pub generated_at: String,
    pub items: Vec<StaleCandidateItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleCandidateItem {
    pub id: String,
    pub title: String,
    pub status: DurableMemoryStatus,
    pub updated_at: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaxonomyIndex {
    pub generated_at: String,
    pub by_type: BTreeMap<String, usize>,
    pub by_status: BTreeMap<String, usize>,
    pub by_scope: BTreeMap<String, usize>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub timestamp: String,
    pub action: String,
    pub scope: MemoryScope,
    pub memory_id: Option<String>,
    pub session_id: Option<String>,
    pub topic: Option<String>,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

pub fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn sort_memories_desc(memories: &mut [DurableMemoryDocument]) {
    memories.sort_by(|left, right| {
        let left_dt =
            parse_rfc3339(&left.frontmatter.updated_at).unwrap_or(DateTime::<Utc>::MIN_UTC);
        let right_dt =
            parse_rfc3339(&right.frontmatter.updated_at).unwrap_or(DateTime::<Utc>::MIN_UTC);
        right_dt
            .cmp(&left_dt)
            .then_with(|| left.frontmatter.id.cmp(&right.frontmatter.id))
    });
}

pub fn match_memory_query(
    doc: &DurableMemoryDocument,
    query: Option<&str>,
    filter_types: Option<&HashSet<DurableMemoryType>>,
    filter_statuses: Option<&HashSet<DurableMemoryStatus>>,
    filter_granularity: Option<&HashSet<TemporalGranularity>>,
) -> Option<f64> {
    if let Some(types) = filter_types {
        if !types.contains(&doc.frontmatter.r#type) {
            return None;
        }
    }
    if let Some(statuses) = filter_statuses {
        if !statuses.contains(&doc.frontmatter.status) {
            return None;
        }
    }
    // Absent filter (`None`) = no filtering, matching every existing memory
    // regardless of whether it carries a granularity — back-compat with pre-#61
    // callers and with memories written before the dimension existed. An ACTIVE
    // filter (issue #61 phase 2, "只看本周的 memory") only matches memories that
    // carry one of the requested granularities; an untagged memory (`None`) never
    // matches an active granularity filter, since the caller asked for a specific
    // time horizon and an untagged memory doesn't claim one.
    if let Some(granularities) = filter_granularity {
        match doc.frontmatter.granularity {
            Some(granularity) if granularities.contains(&granularity) => {}
            _ => return None,
        }
    }

    let Some(query) = query.map(str::trim).filter(|value| !value.is_empty()) else {
        return Some(1.0);
    };

    // CJK-aware tokenization (shared with recall) so a Chinese `search`/`query_scope`
    // query matches (bigrams substring-match the Chinese-preserving title/body).
    // The old ASCII-only pass yielded zero tokens for Chinese → matched everything
    // unranked. (#242)
    let query_tokens = lexical_bm25::tokenize(query);
    if query_tokens.is_empty() {
        return Some(1.0);
    }

    let title = doc.frontmatter.title.to_ascii_lowercase();
    let body = doc.body.to_ascii_lowercase();
    let keywords: HashSet<String> = doc
        .frontmatter
        .retrieval
        .keywords
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect();
    let tags: HashSet<String> = doc
        .frontmatter
        .tags
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect();
    let entities: HashSet<String> = doc
        .frontmatter
        .retrieval
        .entities
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect();

    let mut score = 0.0;
    let mut matched_any = false;
    for token in &query_tokens {
        let mut token_score = 0.0;
        if title.contains(token) {
            token_score += 3.0;
        }
        if keywords.contains(token) {
            token_score += 2.5;
        }
        if tags.contains(token) {
            token_score += 2.0;
        }
        if entities.contains(token) {
            token_score += 1.5;
        }
        if body.contains(token) {
            token_score += 1.0;
        }
        if token_score > 0.0 {
            matched_any = true;
            score += token_score;
        }
    }

    matched_any.then_some(score / query_tokens.len() as f64)
}

pub fn build_memory_markdown_view(
    scope: MemoryScope,
    project_key: Option<&str>,
    docs: &[DurableMemoryDocument],
) -> String {
    let title = match scope {
        MemoryScope::Global => "# Bamboo Memory Index (Global)".to_string(),
        MemoryScope::Project => format!(
            "# Bamboo Memory Index (Project: {})",
            project_key.unwrap_or("unknown")
        ),
        MemoryScope::Session => "# Bamboo Memory Index (Session)".to_string(),
    };
    let mut out = String::new();
    out.push_str(&title);
    out.push_str("\n\n");
    if docs.is_empty() {
        out.push_str("_(empty)_\n");
        return out;
    }

    for doc in docs {
        out.push_str(&format!(
            "- `{}` {} [{} / {}] updated {}\n",
            doc.frontmatter.id,
            doc.frontmatter.title,
            doc.frontmatter.r#type.as_str(),
            doc.frontmatter.status.as_str(),
            doc.frontmatter.updated_at,
        ));
        let summary = derive_summary(&doc.body, 160);
        if !summary.is_empty() {
            out.push_str(&format!("  - {}\n", summary));
        }
    }
    out
}

pub fn build_recent_markdown_view(docs: &[DurableMemoryDocument]) -> String {
    let mut out = String::from("# Recent Memory Updates\n\n");
    if docs.is_empty() {
        out.push_str("_(empty)_\n");
        return out;
    }
    for doc in docs.iter().take(20) {
        out.push_str(&format!(
            "- `{}` {} — {}\n",
            doc.frontmatter.id, doc.frontmatter.title, doc.frontmatter.updated_at
        ));
    }
    out
}

pub fn build_stale_markdown_view(docs: &[DurableMemoryDocument]) -> String {
    let mut out = String::from("# Stale Memory Candidates\n\n");
    let stale: Vec<_> = docs
        .iter()
        .filter(|doc| doc.frontmatter.status != DurableMemoryStatus::Active)
        .collect();
    if stale.is_empty() {
        out.push_str("_(no stale items)_\n");
        return out;
    }
    for doc in stale {
        out.push_str(&format!(
            "- `{}` {} [{}]\n",
            doc.frontmatter.id,
            doc.frontmatter.title,
            doc.frontmatter.status.as_str()
        ));
    }
    out
}

pub fn build_dream_view(existing: Option<&str>) -> String {
    match existing.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => value.to_string(),
        None => "# Bamboo Dream Notebook\n\n_(empty)_\n".to_string(),
    }
}

pub fn parse_query_cursor(cursor: Option<&str>) -> usize {
    cursor
        .and_then(|raw| raw.rsplit(':').next())
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(0)
}

pub fn make_query_cursor(scope: MemoryScope, offset: usize) -> String {
    format!("{}:{}", scope.as_str(), offset)
}

pub fn summary_json(items: usize, total: usize) -> String {
    if total == 0 {
        "No matching memories found.".to_string()
    } else {
        format!("Returned top {} of {} matching memories.", items, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_tags_dedupes_and_sanitizes() {
        let tags = normalize_tags(["User Preference", "user-preference", "release/freeze"]);
        assert_eq!(tags, vec!["release-freeze", "user-preference"]);
    }

    #[test]
    fn project_key_from_path_is_stable() {
        let key = project_key_from_path(Path::new("/tmp/My Project"));
        assert!(key.starts_with("my-project-"));
    }

    #[test]
    fn parse_markdown_document_requires_frontmatter() {
        let result = parse_markdown_document("plain body");
        assert!(result.is_err());
    }

    /// Back-compat: a memory document written before the temporal-granularity
    /// dimension existed (its frontmatter has no `granularity` key) must still
    /// parse, with `granularity` defaulting to `None`.
    #[test]
    fn legacy_frontmatter_without_granularity_parses_to_none() {
        let legacy = "---\n\
id: mem-legacy\n\
title: Legacy memory\n\
type: project\n\
scope: project\n\
project_key: proj-1\n\
status: active\n\
created_at: 2026-01-01T00:00:00Z\n\
updated_at: 2026-01-01T00:00:00Z\n\
created_by:\n  kind: session\n\
updated_by:\n  kind: memory_write\n\
---\n\
Legacy body content.\n";
        let (frontmatter, body) =
            parse_markdown_document(legacy).expect("legacy document should parse");
        assert_eq!(frontmatter.id, "mem-legacy");
        assert_eq!(frontmatter.granularity, None);
        assert_eq!(body, "Legacy body content.");
    }

    /// A granularity set on the frontmatter round-trips through render + parse.
    #[test]
    fn granularity_round_trips_through_render_and_parse() {
        let legacy = "---\n\
id: mem-legacy\n\
title: Legacy memory\n\
type: project\n\
scope: project\n\
project_key: proj-1\n\
status: active\n\
created_at: 2026-01-01T00:00:00Z\n\
updated_at: 2026-01-01T00:00:00Z\n\
created_by:\n  kind: session\n\
updated_by:\n  kind: memory_write\n\
---\n\
Body.\n";
        let (mut frontmatter, body) = parse_markdown_document(legacy).unwrap();
        frontmatter.granularity = Some(TemporalGranularity::Quarter);

        let rendered = render_markdown_document(&frontmatter, &body).unwrap();
        assert!(rendered.contains("granularity: quarter"));

        let (reparsed, _) = parse_markdown_document(&rendered).unwrap();
        assert_eq!(reparsed.granularity, Some(TemporalGranularity::Quarter));
    }

    /// `None` granularity must not emit a `granularity:` key (skip_serializing_if),
    /// so existing documents re-rendered after a load keep their on-disk shape.
    #[test]
    fn none_granularity_is_omitted_on_render() {
        let legacy = "---\n\
id: mem-legacy\n\
title: Legacy memory\n\
type: project\n\
scope: project\n\
project_key: proj-1\n\
status: active\n\
created_at: 2026-01-01T00:00:00Z\n\
updated_at: 2026-01-01T00:00:00Z\n\
created_by:\n  kind: session\n\
updated_by:\n  kind: memory_write\n\
---\n\
Body.\n";
        let (frontmatter, body) = parse_markdown_document(legacy).unwrap();
        let rendered = render_markdown_document(&frontmatter, &body).unwrap();
        assert!(!rendered.contains("granularity"));
    }

    #[test]
    fn temporal_granularity_parse_is_case_insensitive_and_rejects_unknown() {
        assert_eq!(
            TemporalGranularity::parse("Week"),
            Some(TemporalGranularity::Week)
        );
        assert_eq!(
            TemporalGranularity::parse("  YEAR "),
            Some(TemporalGranularity::Year)
        );
        assert_eq!(TemporalGranularity::parse("decade"), None);
    }

    #[test]
    fn cache_stability_rank_orders_coarse_before_fine_and_none_first() {
        use TemporalGranularity::*;
        let rank = TemporalGranularity::cache_stability_rank;
        // None is treated as most stable, then coarsest → finest.
        assert!(rank(None) < rank(Some(Year)));
        assert!(rank(Some(Year)) < rank(Some(Quarter)));
        assert!(rank(Some(Quarter)) < rank(Some(Month)));
        assert!(rank(Some(Month)) < rank(Some(Week)));
        assert!(rank(Some(Week)) < rank(Some(Day)));
    }
}
