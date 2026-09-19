//! Background "gardener": LLM-driven remediation of memory-quality problems, plus
//! two deterministic (no-LLM) maintenance passes.
//!
//! Two symmetric LLM-backed passes share one cadence and one cost model:
//! - the **blob gardener** splits multi-topic "blob" memories into atomic pieces;
//! - the **dedup gardener** consolidates near-duplicate memories into one.
//!
//! Cost-controlled by design: each pass is opt-in (default off), has a hard per-run
//! cap, runs on a slow cadence, and — crucially — makes ZERO LLM calls when its
//! deterministic prefilter finds nothing (an idle gardener costs nothing). The
//! *decision* needs the model; the *worklist* (which memories are blobs / which are
//! near-duplicates) is produced for free by `MemoryStore` scan methods.
//!
//! Two further passes run in the same loop but need no model at all: the
//! **freshness gardener** (issue #61 phase 2) conservatively demotes stale
//! fine-grained memories to Stale, and the **capacity gardener** (L5/#263) bounds
//! each scope's recallable size by archiving the lowest-value overflow. Both are
//! non-destructive (Stale/Archived, never deleted) and on by default.

use std::sync::Arc;
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapacityGardenerRunResult {
    pub scopes_scanned: usize,
    pub archived: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FreshnessGardenerRunResult {
    pub scopes_scanned: usize,
    pub marked_stale: usize,
}

pub(crate) async fn collect_model_json(
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
        required_tool: None,
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
pub(crate) fn resolve_background_model(
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
    let memory = ctx.memory.clone();
    run_gardener_once_with_store(ctx, &memory).await
}

async fn gardener_targets(
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Vec<(MemoryScope, Option<String>, MemoryStore)> {
    let mut targets = vec![(MemoryScope::Global, None, memory.clone())];
    if let Some(resolver) = project_context_resolver {
        match resolver.list_project_ids().await {
            Ok(project_ids) => {
                targets.extend(project_ids.into_iter().map(|project_id| {
                    (
                        MemoryScope::Project,
                        Some(project_id.to_string()),
                        memory.for_project(&project_id),
                    )
                }));
            }
            Err(error) => tracing::warn!(
                target: GARDENER_TRACING_TARGET,
                "failed to resolve first-class Project gardener scopes: {error}"
            ),
        }
    }
    targets
}

async fn count_gardener_memories(
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> usize {
    let mut total = 0;
    for (scope, project_key, target_store) in
        gardener_targets(memory, project_context_resolver).await
    {
        total += target_store
            .count_scope_memories(scope, project_key.as_deref())
            .await
            .unwrap_or(0);
    }
    total
}

async fn run_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<GardenerRunResult>, String> {
    run_gardener_once_with_store_and_resolver(ctx, memory, None).await
}

async fn run_gardener_once_with_store_and_resolver(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<Option<GardenerRunResult>, String> {
    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory().clone().unwrap_or_default();
    if !memory_cfg.gardener_enabled {
        return Ok(None);
    }

    let max_splits = memory_cfg.gardener_max_splits_per_run.max(1);
    let min_sections = memory_cfg.gardener_min_sections;

    // Deterministic prefilter across global + every project scope (zero LLM).
    let targets = gardener_targets(memory, project_context_resolver).await;

    let mut result = GardenerRunResult::default();
    let mut worklist: Vec<(MemoryStore, Option<String>, BlobScanItem)> = Vec::new();
    for (scope, project_key, target_store) in &targets {
        let report = target_store
            .scan_blob_candidates(*scope, project_key.as_deref(), min_sections, max_splits)
            .await
            .map_err(|error| format!("gardener scan failed: {error}"))?;
        result.scanned += report.scanned;
        result.flagged += report.flagged;
        for item in report.items {
            worklist.push((target_store.clone(), project_key.clone(), item));
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

    for (target_store, project_key, item) in worklist {
        let project_key = project_key.as_deref();
        let Some(doc) = target_store
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

        match target_store
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
    let memory = ctx.memory.clone();
    run_dedup_gardener_once_with_store(ctx, &memory).await
}

async fn run_dedup_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<DedupGardenerRunResult>, String> {
    run_dedup_gardener_once_with_store_and_resolver(ctx, memory, None).await
}

async fn run_dedup_gardener_once_with_store_and_resolver(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<Option<DedupGardenerRunResult>, String> {
    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory().clone().unwrap_or_default();
    if !memory_cfg.dedup_gardener_enabled {
        return Ok(None);
    }

    let max_merges = memory_cfg.dedup_gardener_max_merges_per_run.max(1);
    let min_score = memory_cfg.dedup_gardener_min_score;

    // Deterministic prefilter across global + every project scope (zero LLM).
    let targets = gardener_targets(memory, project_context_resolver).await;

    let mut result = DedupGardenerRunResult::default();
    let mut worklist: Vec<(MemoryStore, Option<String>, DuplicateCluster)> = Vec::new();
    for (scope, project_key, target_store) in &targets {
        let report = target_store
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
            worklist.push((target_store.clone(), project_key.clone(), cluster));
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

    for (target_store, project_key, cluster) in worklist {
        let project_key = project_key.as_deref();
        // Re-fetch full bodies; drop members that vanished or were superseded since
        // the scan (e.g. by the blob pass) so we never consolidate a stale set.
        let mut members: Vec<(String, String, String)> = Vec::new();
        for member in &cluster.members {
            if let Some(doc) = target_store
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
        match target_store
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

/// Capacity gardener (L5): bound each scope's RECALLABLE size by archiving the
/// lowest-value overflow OUT of the recall index — never deletes (reversible), no
/// LLM. Off unless `memory_active_capacity > 0`; `Reference`/`User`/`Feedback`
/// memories are exempt. Runs last (after split + dedup), so it counts the
/// post-consolidation library. `Ok(None)` when the feature is off.
pub async fn run_capacity_gardener_once(
    ctx: &AutoDreamContext,
) -> Result<Option<CapacityGardenerRunResult>, String> {
    let memory = ctx.memory.clone();
    run_capacity_gardener_once_with_store(ctx, &memory).await
}

async fn run_capacity_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<CapacityGardenerRunResult>, String> {
    run_capacity_gardener_once_with_store_and_resolver(ctx, memory, None).await
}

async fn run_capacity_gardener_once_with_store_and_resolver(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<Option<CapacityGardenerRunResult>, String> {
    let memory_cfg = ctx.config.read().await.memory().clone().unwrap_or_default();
    let capacity = memory_cfg.memory_active_capacity;
    if capacity == 0 {
        return Ok(None);
    }
    let max_archivals = memory_cfg.capacity_max_archivals_per_run.max(1);

    let targets = gardener_targets(memory, project_context_resolver).await;

    let mut result = CapacityGardenerRunResult::default();
    for (scope, project_key, target_store) in &targets {
        let archived = target_store
            .enforce_scope_capacity(*scope, project_key.as_deref(), capacity, max_archivals)
            .await
            .map_err(|error| format!("capacity enforcement failed: {error}"))?;
        result.scopes_scanned += 1;
        result.archived += archived.len();
    }
    if result.archived > 0 {
        tracing::info!(
            target: GARDENER_TRACING_TARGET,
            event = "capacity_run_complete",
            scopes_scanned = result.scopes_scanned,
            archived = result.archived,
            capacity = capacity,
            "[capacity-gardener] archived {} memories over capacity",
            result.archived
        );
    }
    Ok(Some(result))
}

/// Temporal-granularity freshness gardener (issue #61 phase 2, follow-up to the
/// L5/#263 capacity gardener above): conservatively demotes Active day/week
/// granularity memories to Stale once they cross the documented staleness window
/// (applied by `MemoryStore::expire_stale_granularity`). Deterministic
/// — no LLM, no cost — and non-destructive: it only ever moves Active → Stale,
/// never archives or deletes. Off when `granularity_freshness_gardener_enabled`
/// is false; on by default (like the blob/dedup passes). `Ok(None)` when the
/// feature is off.
pub async fn run_freshness_gardener_once(
    ctx: &AutoDreamContext,
) -> Result<Option<FreshnessGardenerRunResult>, String> {
    let memory = ctx.memory.clone();
    run_freshness_gardener_once_with_store(ctx, &memory).await
}

async fn run_freshness_gardener_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<FreshnessGardenerRunResult>, String> {
    run_freshness_gardener_once_with_store_and_resolver(ctx, memory, None).await
}

async fn run_freshness_gardener_once_with_store_and_resolver(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<Option<FreshnessGardenerRunResult>, String> {
    let memory_cfg = ctx.config.read().await.memory().clone().unwrap_or_default();
    if !memory_cfg.granularity_freshness_gardener_enabled {
        return Ok(None);
    }

    let targets = gardener_targets(memory, project_context_resolver).await;

    let mut result = FreshnessGardenerRunResult::default();
    for (scope, project_key, target_store) in &targets {
        let expired = target_store
            .expire_stale_granularity(*scope, project_key.as_deref())
            .await
            .map_err(|error| format!("granularity freshness pass failed: {error}"))?;
        result.scopes_scanned += 1;
        result.marked_stale += expired.len();
    }
    if result.marked_stale > 0 {
        tracing::info!(
            target: GARDENER_TRACING_TARGET,
            event = "freshness_run_complete",
            scopes_scanned = result.scopes_scanned,
            marked_stale = result.marked_stale,
            "[freshness-gardener] marked {} fine-grained memories stale",
            result.marked_stale
        );
    }
    Ok(Some(result))
}

/// How often the gardener loop polls for the volume trigger. Kept short relative
/// to the (typically daily) time interval so library growth is caught within
/// minutes, not a full interval. Each poll is a cheap count + a config read; the
/// actual passes only run when the time or volume condition fires.
const GARDENER_VOLUME_POLL_SECS: u64 = 300;

/// Decide whether the gardener maintenance pass should run on this poll: on the
/// time interval, OR early once the library has grown by `volume_trigger` memories
/// since the last run (L4). `volume_trigger == 0` disables the volume trigger
/// (time-only). Pure so the trigger logic is unit-testable without wall-clock.
fn should_run_gardener_pass(
    elapsed_secs: u64,
    interval_secs: u64,
    current_count: usize,
    last_run_count: usize,
    volume_trigger: usize,
) -> bool {
    if elapsed_secs >= interval_secs {
        return true;
    }
    volume_trigger > 0 && current_count.saturating_sub(last_run_count) >= volume_trigger
}

/// Run both gardener passes in order. Blob remediation first, then dedup: splitting
/// a blob can expose fresh duplicates, and superseded blob sources drop out of the
/// dedup scan.
async fn run_gardener_passes(
    ctx: &AutoDreamContext,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) {
    let memory = ctx.memory.clone();
    if let Err(error) =
        run_gardener_once_with_store_and_resolver(ctx, &memory, project_context_resolver).await
    {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "run_failed",
            "[gardener] run failed: {}",
            error
        );
    }
    if let Err(error) =
        run_dedup_gardener_once_with_store_and_resolver(ctx, &memory, project_context_resolver)
            .await
    {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "dedup_run_failed",
            "[dedup-gardener] run failed: {}",
            error
        );
    }
    // Freshness pass before capacity: memories it demotes to Stale in this same
    // run are then discounted by `memory_value`'s stale_penalty, so the capacity
    // gardener's overflow scoring (if it runs right after) already reflects any
    // granularity-driven staleness from this pass.
    if let Err(error) =
        run_freshness_gardener_once_with_store_and_resolver(ctx, &memory, project_context_resolver)
            .await
    {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "freshness_run_failed",
            "[freshness-gardener] run failed: {}",
            error
        );
    }
    // Capacity enforcement last, so it counts the post-consolidation library.
    if let Err(error) =
        run_capacity_gardener_once_with_store_and_resolver(ctx, &memory, project_context_resolver)
            .await
    {
        tracing::warn!(
            target: GARDENER_TRACING_TARGET,
            event = "capacity_run_failed",
            "[capacity-gardener] run failed: {}",
            error
        );
    }
}

/// Spawn the recurring gardener loop. Time- AND volume-triggered (L4): it polls on
/// a short cadence and runs the passes when either the configured interval elapsed
/// or enough new memories accumulated since the last run. No-op cost when disabled:
/// each pass reads config and returns immediately if its gardener is off, and the
/// poll itself is just a cheap topic-file count.
pub fn spawn_gardener_task(ctx: AutoDreamContext) {
    spawn_gardener_task_inner(ctx, None);
}

pub fn spawn_gardener_task_with_project_resolver(
    ctx: AutoDreamContext,
    project_context_resolver: std::sync::Arc<crate::project_context::ProjectContextResolver>,
) {
    spawn_gardener_task_inner(ctx, Some(project_context_resolver));
}

fn spawn_gardener_task_inner(
    ctx: AutoDreamContext,
    project_context_resolver: Option<
        std::sync::Arc<crate::project_context::ProjectContextResolver>,
    >,
) {
    tokio::spawn(async move {
        let (interval_secs, volume_trigger) = {
            let guard = ctx.config.read().await;
            let memory = guard.memory().as_ref();
            let interval_secs = memory
                .map(|memory| memory.gardener_interval_secs)
                .filter(|secs| *secs > 0)
                // Fall back to the config default (single source of truth for
                // "daily") when memory config is absent or the interval was 0.
                .unwrap_or_else(|| bamboo_config::MemoryConfig::default().gardener_interval_secs);
            let volume_trigger = memory
                .map(|memory| memory.gardener_volume_trigger)
                .unwrap_or_else(|| bamboo_config::MemoryConfig::default().gardener_volume_trigger);
            (interval_secs, volume_trigger)
        };
        // Poll frequently enough to catch volume growth, but never longer than the
        // time interval itself.
        let poll_secs = GARDENER_VOLUME_POLL_SECS.min(interval_secs.max(1));

        let memory = ctx.memory.clone();
        let mut ticker = tokio::time::interval(Duration::from_secs(poll_secs));
        let mut last_run = Instant::now();
        let mut last_run_count =
            count_gardener_memories(&memory, project_context_resolver.as_deref()).await;
        // Run once on startup (the first `interval` tick fired immediately before
        // this change), then let the time/volume conditions drive it.
        let mut force_first = true;

        loop {
            ticker.tick().await;
            let current_count =
                count_gardener_memories(&memory, project_context_resolver.as_deref()).await;
            let elapsed_secs = last_run.elapsed().as_secs();
            if !force_first
                && !should_run_gardener_pass(
                    elapsed_secs,
                    interval_secs,
                    current_count,
                    last_run_count,
                    volume_trigger,
                )
            {
                continue;
            }
            force_first = false;
            run_gardener_passes(&ctx, project_context_resolver.as_deref()).await;
            last_run = Instant::now();
            // Re-baseline the count AFTER the pass so the gardener's own file writes
            // don't re-trigger it. Superseded/archived sources are kept as tombstone
            // files, so a split or a dedup-consolidation actually GROWS the topic-file
            // count (a new canonical is written, sources retained); only a hard purge
            // shrinks it. Re-baselining absorbs all of that.
            last_run_count =
                count_gardener_memories(&memory, project_context_resolver.as_deref()).await;
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
    use bamboo_memory::memory_store::DurableMemoryType;
    use bamboo_storage::SessionStoreV2;

    fn config_with_memory(memory: bamboo_config::MemoryConfig) -> Config {
        let mut config = Config::default();
        *config.memory_mut() = Some(memory);
        config
    }

    #[test]
    fn gardener_trigger_fires_on_time_or_volume() {
        // Time trigger: elapsed >= interval fires regardless of growth.
        assert!(should_run_gardener_pass(86_400, 86_400, 0, 0, 25));
        assert!(should_run_gardener_pass(90_000, 86_400, 5, 5, 25));
        // Not yet due and not enough growth → no run.
        assert!(!should_run_gardener_pass(60, 86_400, 30, 20, 25));
        // Volume trigger: grew by >= threshold before the interval → early run.
        assert!(should_run_gardener_pass(60, 86_400, 45, 20, 25));
        assert!(should_run_gardener_pass(60, 86_400, 25, 0, 25));
        // Growth below the threshold → no run.
        assert!(!should_run_gardener_pass(60, 86_400, 44, 20, 25));
        // volume_trigger == 0 disables the volume trigger (time-only).
        assert!(!should_run_gardener_pass(60, 86_400, 10_000, 0, 0));
        // Count shrinking (e.g. after a dedup) never underflows into a trigger.
        assert!(!should_run_gardener_pass(60, 86_400, 5, 100, 25));
    }

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
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                gardener_enabled: true,
                gardener_min_sections: 2,
                gardener_max_splits_per_run: 8,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));

        let ctx = AutoDreamContext {
            session_store: session_store.clone(),
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };

        // Seed a blob with 3 `---` accretions in the same bamboo home the gardener reads.
        let memory = MemoryStore::new(temp.path().join("jiandu"));
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
                None,
            )
            .await
            .unwrap();
        for extra in ["fact two", "fact three", "fact four"] {
            memory
                .merge_memory(&blob.frontmatter.id, None, extra, &[], Some("s"), "t", &[])
                .await
                .unwrap();
        }

        let result = run_gardener_once_with_store(&ctx, &memory)
            .await
            .unwrap()
            .unwrap();
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
        // Gardener is ON by default (L4), so disable it explicitly to test the
        // disabled path.
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                gardener_enabled: false,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };
        let memory = MemoryStore::new(temp.path().join("jiandu"));
        assert_eq!(
            run_gardener_once_with_store(&ctx, &memory).await.unwrap(),
            None
        );
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
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                dedup_gardener_enabled: true,
                dedup_gardener_min_score: 0.3,
                dedup_gardener_max_merges_per_run: 8,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));

        let ctx = AutoDreamContext {
            session_store: session_store.clone(),
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };

        // Seed two near-duplicate memories in the same bamboo home the gardener reads.
        let memory = MemoryStore::new(temp.path().join("jiandu"));
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
                    None,
                )
                .await
                .unwrap();
            ids.push(doc.frontmatter.id);
        }

        let result = run_dedup_gardener_once_with_store(&ctx, &memory)
            .await
            .unwrap()
            .unwrap();
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
        // Dedup gardener is ON by default (L4); disable it explicitly here.
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                dedup_gardener_enabled: false,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };
        let memory = MemoryStore::new(temp.path().join("jiandu"));
        assert_eq!(
            run_dedup_gardener_once_with_store(&ctx, &memory)
                .await
                .unwrap(),
            None
        );
    }

    /// L5: the capacity gardener is off (Ok(None)) unless `memory_active_capacity`
    /// is set, and when set it archives the over-capacity overflow.
    #[tokio::test]
    async fn capacity_gardener_off_until_capacity_set_then_archives_overflow() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![]));

        let memory = MemoryStore::new(temp.path().join("jiandu"));
        for title in [
            "Global fact a",
            "Global fact b",
            "Global fact c",
            "Global fact d",
        ] {
            memory
                .write_memory(
                    MemoryScope::Global,
                    None,
                    DurableMemoryType::Project,
                    title,
                    "body content for the memory",
                    &[],
                    Some("s"),
                    "m",
                    false,
                    None,
                )
                .await
                .unwrap();
        }

        // Off by default (capacity == 0) → Ok(None), archives nothing.
        let off_config = Arc::new(RwLock::new(Config::default()));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx_off = AutoDreamContext {
            session_store: session_store.clone(),
            storage: storage.clone(),
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider: provider.clone(),
            config: off_config,
            provider_registry: provider_registry.clone(),
        };
        assert_eq!(
            run_capacity_gardener_once_with_store(&ctx_off, &memory)
                .await
                .unwrap(),
            None
        );

        // Capacity 2 → archive the 2 over-capacity memories.
        let on_config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                memory_active_capacity: 2,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let ctx_on = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config: on_config,
            provider_registry,
        };
        let result = run_capacity_gardener_once_with_store(&ctx_on, &memory)
            .await
            .unwrap()
            .expect("capacity pass should run when enabled");
        assert_eq!(result.archived, 2, "archived the over-capacity overflow");
        assert_eq!(
            memory
                .count_scope_memories(MemoryScope::Global, None)
                .await
                .unwrap(),
            4,
            "archived docs are kept on disk, not deleted"
        );
    }

    /// Issue #61 phase 2: the freshness gardener is gated by
    /// `granularity_freshness_gardener_enabled` (default ON) and, when disabled,
    /// short-circuits to `Ok(None)` without touching any memory — mirroring the
    /// on/off contract the blob/dedup/capacity passes already use.
    #[tokio::test]
    async fn freshness_gardener_is_noop_when_disabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                granularity_freshness_gardener_enabled: false,
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };
        let memory = MemoryStore::new(temp.path().join("jiandu"));
        assert_eq!(
            run_freshness_gardener_once_with_store(&ctx, &memory)
                .await
                .unwrap(),
            None
        );
    }

    /// When enabled (the default), the freshness gardener scans every scope
    /// (global + each project) and reports zero `marked_stale` for memories that
    /// haven't crossed their staleness window yet — confirming the pass is wired
    /// through `MemoryStore::expire_stale_granularity` correctly without relying
    /// on backdated fixtures (the time-threshold math itself is covered by
    /// `bamboo-memory`'s own `expire_stale_granularity_marks_only_expired_day_and_week_memories`
    /// test, which has direct access to backdate a document's `updated_at`).
    #[tokio::test]
    async fn freshness_gardener_runs_when_enabled_and_scans_every_scope() {
        let temp = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider::new(vec![]));

        let memory = MemoryStore::new(temp.path().join("jiandu"));
        memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Freshly written day note",
                "Not yet old enough to expire.",
                &[],
                Some("s"),
                "m",
                false,
                Some(bamboo_memory::memory_store::TemporalGranularity::Day),
            )
            .await
            .unwrap();

        // Enabled by default (matches `MemoryConfig::default()`).
        let config = Arc::new(RwLock::new(Config::default()));
        let provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()));
        let ctx = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp.path().join("jiandu")),
            provider,
            config,
            provider_registry,
        };

        let result = run_freshness_gardener_once_with_store(&ctx, &memory)
            .await
            .unwrap()
            .expect("freshness pass should run when enabled");
        assert!(
            result.scopes_scanned >= 1,
            "at least the global scope is scanned"
        );
        assert_eq!(
            result.marked_stale, 0,
            "a freshly-written day memory has not crossed its staleness window yet"
        );
    }
}
