//! Background "gardener": LLM-driven remediation of memory-quality problems.
//!
//! Two symmetric passes share one cadence and one cost model:
//! - the **blob gardener** splits multi-topic "blob" memories into atomic pieces;
//! - the **dedup gardener** consolidates near-duplicate memories into one.
//!
//! Cost-controlled by design: each pass is opt-in (default off), has a hard per-run
//! cap, runs on a slow cadence, and — crucially — makes ZERO LLM calls when its
//! deterministic prefilter finds nothing (an idle gardener costs nothing). The
//! *decision* needs the model; the *worklist* (which memories are blobs / which are
//! near-duplicates) is produced for free by `MemoryStore` scan methods.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use bamboo_agent_core::Message;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_llm::{Config, LLMChunk, LLMProvider, LLMRequestOptions, ProviderModelRouter};
use bamboo_memory::auto_dream::{
    build_blob_split_prompt, build_dedup_prompt, parse_dedup_decision, parse_split_pieces,
};
use bamboo_memory::memory_store::{
    BlobScanItem, DuplicateCluster, DurableMemoryStatus, MemoryScope, MemoryStore,
};

use crate::auto_dream::AutoDreamContext;

const GARDENER_TRACING_TARGET: &str = "bamboo.gardener";
const GARDENER_RUNTIME_SESSION_ID: &str = "__gardener__";
/// Upper bound on how many memories the dedup gardener feeds to the model per
/// cluster, to bound prompt size and avoid over-merging a large fuzzy group.
const DEDUP_MAX_MEMBERS_PER_CLUSTER: usize = 5;

const BLOB_SYSTEM_INSTRUCTION: &str = "You are Bamboo's background memory gardener. Split the given memory into atomic pieces and return only the specified JSON. No prose, no markdown fences.";
const DEDUP_SYSTEM_INSTRUCTION: &str = "You are Bamboo's background memory gardener. Decide whether the given memories are the same fact and, if so, consolidate them. Return only the specified JSON. No prose, no markdown fences.";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GardenerRunResult {
    pub scanned: usize,
    pub flagged: usize,
    pub split: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DedupGardenerRunResult {
    pub scanned: usize,
    pub clustered: usize,
    pub consolidated: usize,
    pub superseded: usize,
    pub failed: usize,
}

async fn collect_model_json(
    provider: Arc<dyn LLMProvider>,
    model: &str,
    system_instruction: &str,
    prompt: String,
) -> Result<String, String> {
    let messages = vec![Message::system(system_instruction), Message::user(prompt)];
    let options = LLMRequestOptions {
        session_id: Some(GARDENER_RUNTIME_SESSION_ID.to_string()),
        reasoning_effort: Some(ReasoningEffort::High),
        parallel_tool_calls: None,
        responses: None,
        request_purpose: Some("memory_gardener".to_string()),
        cache: None,
    };

    let mut stream = provider
        .chat_stream_with_options(&messages, &[], Some(8192), model, Some(&options))
        .await
        .map_err(|error| format!("gardener provider call failed: {error}"))?;

    let mut content = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(LLMChunk::Token(text)) => content.push_str(&text),
            Ok(LLMChunk::Done) => break,
            Ok(_) => {}
            Err(error) => {
                if !content.is_empty() {
                    break;
                }
                return Err(format!("gardener stream failed: {error}"));
            }
        }
    }
    Ok(content)
}

/// Mirrors auto_dream's background-model resolution (ProviderModelRef when enabled,
/// else `memory.background_model` / provider fast model). Returns `None` when no
/// background model is configured — the gardener then skips without spending tokens.
fn resolve_background_model(
    ctx: &AutoDreamContext,
    config_snapshot: &Config,
) -> Option<(Arc<dyn LLMProvider>, String)> {
    let provider_ref_enabled = config_snapshot.features.provider_model_ref;
    let model_ref = if provider_ref_enabled {
        config_snapshot
            .defaults
            .as_ref()
            .and_then(|d| d.memory_background.as_ref())
            .or_else(|| {
                config_snapshot
                    .defaults
                    .as_ref()
                    .and_then(|d| d.fast.as_ref())
            })
    } else {
        None
    };

    if let Some(mr) = model_ref {
        let router = ProviderModelRouter::new(ctx.provider_registry.clone());
        match router.route(mr) {
            Ok(routed) => Some((routed, mr.model.clone())),
            Err(error) => {
                tracing::warn!(
                    target: GARDENER_TRACING_TARGET,
                    event = "model_route_failed",
                    "[gardener] failed to route background model ref '{}': {}",
                    mr,
                    error
                );
                None
            }
        }
    } else {
        config_snapshot
            .get_memory_background_model()
            .map(|model| (ctx.provider.clone(), model))
    }
}

pub async fn run_gardener_once(
    ctx: &AutoDreamContext,
) -> Result<Option<GardenerRunResult>, String> {
    let memory = MemoryStore::new(ctx.session_store.bamboo_home_dir());
    run_gardener_once_with_store(ctx, &memory).await
}

async fn run_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<GardenerRunResult>, String> {
    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory.clone().unwrap_or_default();
    if !memory_cfg.gardener_enabled {
        return Ok(None);
    }

    let max_splits = memory_cfg.gardener_max_splits_per_run.max(1);
    let min_sections = memory_cfg.gardener_min_sections;

    // Deterministic prefilter across global + every project scope (zero LLM).
    let mut targets: Vec<(MemoryScope, Option<String>)> = vec![(MemoryScope::Global, None)];
    for key in memory.list_project_keys().await.unwrap_or_default() {
        targets.push((MemoryScope::Project, Some(key)));
    }

    let mut result = GardenerRunResult::default();
    let mut worklist: Vec<(MemoryScope, Option<String>, BlobScanItem)> = Vec::new();
    for (scope, project_key) in &targets {
        let report = memory
            .scan_blob_candidates(*scope, project_key.as_deref(), min_sections, max_splits)
            .await
            .map_err(|error| format!("gardener scan failed: {error}"))?;
        result.scanned += report.scanned;
        result.flagged += report.flagged;
        for item in report.items {
            worklist.push((*scope, project_key.clone(), item));
        }
    }
    worklist.sort_by(|left, right| {
        right
            .2
            .appended_sections
            .cmp(&left.2.appended_sections)
            .then(right.2.body_chars.cmp(&left.2.body_chars))
    });
    worklist.truncate(max_splits);

    // Nothing to do → return WITHOUT resolving/calling the model. Idle = free.
    if worklist.is_empty() {
        return Ok(Some(result));
    }

    let Some((provider, model)) = resolve_background_model(ctx, &config_snapshot) else {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "run_skip",
            reason = "no_background_model",
            "[gardener] skipped: no background model configured"
        );
        return Ok(None);
    };

    for (_scope, project_key, item) in worklist {
        let project_key = project_key.as_deref();
        let Some(doc) = memory
            .get_memory(&item.id, project_key)
            .await
            .map_err(|error| format!("gardener get failed: {error}"))?
        else {
            continue;
        };

        let prompt = build_blob_split_prompt(&doc.frontmatter.title, &doc.body);
        let raw = match collect_model_json(
            provider.clone(),
            &model,
            BLOB_SYSTEM_INSTRUCTION,
            prompt,
        )
        .await
        {
            Ok(text) => text,
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "split_llm_failed", id = %item.id, "{error}");
                result.failed += 1;
                continue;
            }
        };

        let pieces = match parse_split_pieces(&raw) {
            // Require ≥2 pieces: a 0/1-piece answer means the model judged it not a
            // blob, so we leave it untouched rather than churn it.
            Ok(pieces) if pieces.len() >= 2 => pieces,
            Ok(_) => continue,
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "split_parse_failed", id = %item.id, "{error}");
                result.failed += 1;
                continue;
            }
        };

        match memory
            .split_memory(
                &item.id,
                project_key,
                &pieces,
                Some(GARDENER_RUNTIME_SESSION_ID),
                "memory-gardener",
            )
            .await
        {
            Ok(Some(_)) => result.split += 1,
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "split_apply_failed", id = %item.id, "{error}");
                result.failed += 1;
            }
        }
    }

    tracing::info!(
        target: GARDENER_TRACING_TARGET,
        event = "run_complete",
        scanned = result.scanned,
        flagged = result.flagged,
        split = result.split,
        failed = result.failed,
        "[gardener] run complete"
    );
    Ok(Some(result))
}

pub async fn run_dedup_gardener_once(
    ctx: &AutoDreamContext,
) -> Result<Option<DedupGardenerRunResult>, String> {
    let memory = MemoryStore::new(ctx.session_store.bamboo_home_dir());
    run_dedup_gardener_once_with_store(ctx, &memory).await
}

async fn run_dedup_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<DedupGardenerRunResult>, String> {
    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory.clone().unwrap_or_default();
    if !memory_cfg.dedup_gardener_enabled {
        return Ok(None);
    }

    let max_merges = memory_cfg.dedup_gardener_max_merges_per_run.max(1);
    let min_score = memory_cfg.dedup_gardener_min_score;

    // Deterministic prefilter across global + every project scope (zero LLM).
    let mut targets: Vec<(MemoryScope, Option<String>)> = vec![(MemoryScope::Global, None)];
    for key in memory.list_project_keys().await.unwrap_or_default() {
        targets.push((MemoryScope::Project, Some(key)));
    }

    let mut result = DedupGardenerRunResult::default();
    let mut worklist: Vec<(MemoryScope, Option<String>, DuplicateCluster)> = Vec::new();
    for (scope, project_key) in &targets {
        let report = memory
            .scan_duplicate_clusters(
                *scope,
                project_key.as_deref(),
                min_score,
                DEDUP_MAX_MEMBERS_PER_CLUSTER,
                max_merges,
            )
            .await
            .map_err(|error| format!("dedup scan failed: {error}"))?;
        result.scanned += report.scanned;
        result.clustered += report.clusters.len();
        for cluster in report.clusters {
            worklist.push((*scope, project_key.clone(), cluster));
        }
    }
    worklist.sort_by(|left, right| {
        right
            .2
            .max_score
            .partial_cmp(&left.2.max_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(right.2.members.len().cmp(&left.2.members.len()))
    });
    worklist.truncate(max_merges);

    // Nothing to do → return WITHOUT resolving/calling the model. Idle = free.
    if worklist.is_empty() {
        return Ok(Some(result));
    }

    let Some((provider, model)) = resolve_background_model(ctx, &config_snapshot) else {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "dedup_run_skip",
            reason = "no_background_model",
            "[dedup-gardener] skipped: no background model configured"
        );
        return Ok(None);
    };

    for (_scope, project_key, cluster) in worklist {
        let project_key = project_key.as_deref();
        // Re-fetch full bodies; drop members that vanished or were superseded since
        // the scan (e.g. by the blob pass) so we never consolidate a stale set.
        let mut members: Vec<(String, String, String)> = Vec::new();
        for member in &cluster.members {
            if let Some(doc) = memory
                .get_memory(&member.id, project_key)
                .await
                .map_err(|error| format!("dedup get failed: {error}"))?
            {
                if doc.frontmatter.status == DurableMemoryStatus::Active {
                    members.push((
                        doc.frontmatter.id.clone(),
                        doc.frontmatter.title.clone(),
                        doc.body.clone(),
                    ));
                }
            }
        }
        if members.len() < 2 {
            continue;
        }

        let prompt_members: Vec<(String, String)> = members
            .iter()
            .map(|(_, title, body)| (title.clone(), body.clone()))
            .collect();
        let prompt = build_dedup_prompt(&prompt_members);
        let raw = match collect_model_json(
            provider.clone(),
            &model,
            DEDUP_SYSTEM_INSTRUCTION,
            prompt,
        )
        .await
        {
            Ok(text) => text,
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "dedup_llm_failed", "{error}");
                result.failed += 1;
                continue;
            }
        };

        let merged = match parse_dedup_decision(&raw) {
            // `None` = model judged them distinct facts; leave them untouched.
            Ok(Some(piece)) => piece,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "dedup_parse_failed", "{error}");
                result.failed += 1;
                continue;
            }
        };

        let ids: Vec<String> = members.iter().map(|(id, _, _)| id.clone()).collect();
        match memory
            .consolidate_memories(
                &ids,
                project_key,
                &merged,
                Some(GARDENER_RUNTIME_SESSION_ID),
                "memory-dedup-gardener",
            )
            .await
        {
            Ok(Some(_)) => {
                result.consolidated += 1;
                result.superseded += ids.len();
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(target: GARDENER_TRACING_TARGET, event = "dedup_apply_failed", "{error}");
                result.failed += 1;
            }
        }
    }

    tracing::info!(
        target: GARDENER_TRACING_TARGET,
        event = "dedup_run_complete",
        scanned = result.scanned,
        clustered = result.clustered,
        consolidated = result.consolidated,
        superseded = result.superseded,
        failed = result.failed,
        "[dedup-gardener] run complete"
    );
    Ok(Some(result))
}

/// Spawn the recurring gardener loop. No-op cost when disabled: each tick reads
/// config and returns immediately if neither gardener pass is enabled.
pub fn spawn_gardener_task(ctx: AutoDreamContext) {
    tokio::spawn(async move {
        let interval_secs = ctx
            .config
            .read()
            .await
            .memory
            .as_ref()
            .map(|memory| memory.gardener_interval_secs)
            .filter(|secs| *secs > 0)
            // Fall back to the config default (single source of truth for "daily")
            // when memory config is absent or the interval was set to 0.
            .unwrap_or_else(|| {
                bamboo_config::MemoryConfig::default().gardener_interval_secs
            });
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            // Blob remediation first, then dedup: splitting a blob can expose fresh
            // duplicates, and superseded blob sources drop out of the dedup scan.
            if let Err(error) = run_gardener_once(&ctx).await {
                tracing::warn!(
                    target: GARDENER_TRACING_TARGET,
                    event = "run_failed",
                    "[gardener] run failed: {}",
                    error
                );
            }
            if let Err(error) = run_dedup_gardener_once(&ctx).await {
                tracing::warn!(
                    target: GARDENER_TRACING_TARGET,
                    event = "dedup_run_failed",
                    "[dedup-gardener] run failed: {}",
                    error
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream;
    use tokio::sync::RwLock;

    use bamboo_agent_core::storage::Storage;
    use bamboo_llm::{LLMError, LLMStream, ProviderRegistry};
    use bamboo_storage::SessionStoreV2;
    use bamboo_memory::memory_store::DurableMemoryType;

    #[derive(Clone)]
    struct CannedProvider {
        responses: Arc<Mutex<Vec<String>>>,
    }

    impl CannedProvider {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
            }
        }
    }

    #[async_trait]
    impl LLMProvider for CannedProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let mut responses = self.responses.lock().expect("lock poisoned");
            let text = if responses.is_empty() {
                "{\"pieces\":[]}".to_string()
            } else {
                responses.remove(0)
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(text)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[tokio::test]
    async fn gardener_splits_a_global_blob_and_is_capped() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![
            "{\"pieces\":[{\"title\":\"Fact one\",\"type\":\"user\",\"content\":\"Fact one body.\",\"tags\":[]},{\"title\":\"Fact two\",\"type\":\"reference\",\"content\":\"Fact two body.\",\"tags\":[]}]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                gardener_enabled: true,
                gardener_min_sections: 2,
                gardener_max_splits_per_run: 8,
                ..bamboo_config::MemoryConfig::default()
            }),
            ..Config::default()
        }));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));

        let ctx = AutoDreamContext {
            session_store: session_store.clone(),
            storage,
            provider,
            config,
            provider_registry,
        };

        // Seed a blob with 3 `---` accretions in the same bamboo home the gardener reads.
        let memory = MemoryStore::new(session_store.bamboo_home_dir());
        let blob = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "mixed blob",
                "fact one",
                &[],
                Some("s"),
                "t",
                false,
            )
            .await
            .unwrap();
        for extra in ["fact two", "fact three", "fact four"] {
            memory
                .merge_memory(&blob.frontmatter.id, None, extra, &[], Some("s"), "t", &[])
                .await
                .unwrap();
        }

        let result = run_gardener_once(&ctx).await.unwrap().unwrap();
        assert_eq!(result.split, 1);

        let source = memory
            .get_memory(&blob.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.frontmatter.status,
            bamboo_memory::memory_store::DurableMemoryStatus::Superseded
        );
    }

    #[tokio::test]
    async fn gardener_is_noop_when_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config::default()));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
            provider_registry,
        };
        assert_eq!(run_gardener_once(&ctx).await.unwrap(), None);
    }

    #[tokio::test]
    async fn dedup_gardener_consolidates_a_near_duplicate_pair() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![
            "{\"same_fact\":true,\"merged\":{\"title\":\"Mobile release freeze is Tuesday\",\"type\":\"project\",\"content\":\"Mobile release freeze begins Tuesday for the release cut.\",\"tags\":[\"release\"]}}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                dedup_gardener_enabled: true,
                dedup_gardener_min_score: 0.3,
                dedup_gardener_max_merges_per_run: 8,
                ..bamboo_config::MemoryConfig::default()
            }),
            ..Config::default()
        }));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));

        let ctx = AutoDreamContext {
            session_store: session_store.clone(),
            storage,
            provider,
            config,
            provider_registry,
        };

        // Seed two near-duplicate memories in the same bamboo home the gardener reads.
        let memory = MemoryStore::new(session_store.bamboo_home_dir());
        let mut ids = Vec::new();
        for (title, content) in [
            (
                "freeze v1",
                "Mobile release freeze begins Tuesday for the cut.",
            ),
            (
                "freeze v2",
                "Mobile release freeze starts Tuesday for the release cut.",
            ),
        ] {
            let doc = memory
                .write_memory(
                    MemoryScope::Global,
                    None,
                    DurableMemoryType::Project,
                    title,
                    content,
                    &[],
                    Some("s"),
                    "t",
                    false,
                )
                .await
                .unwrap();
            ids.push(doc.frontmatter.id);
        }

        let result = run_dedup_gardener_once(&ctx).await.unwrap().unwrap();
        assert_eq!(result.consolidated, 1);
        assert_eq!(result.superseded, 2);

        // Both originals are superseded by a new canonical memory.
        for id in &ids {
            let source = memory.get_memory(id, None).await.unwrap().unwrap();
            assert_eq!(
                source.frontmatter.status,
                bamboo_memory::memory_store::DurableMemoryStatus::Superseded
            );
        }
    }

    #[tokio::test]
    async fn dedup_gardener_is_noop_when_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config::default()));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
            provider_registry,
        };
        assert_eq!(run_dedup_gardener_once(&ctx).await.unwrap(), None);
    }
}
