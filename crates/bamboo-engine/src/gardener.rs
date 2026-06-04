//! Background "gardener": LLM-driven remediation of multi-topic "blob" memories.
//!
//! Cost-controlled by design: opt-in (`memory.gardener_enabled`, default off), a
//! hard per-run split cap, a slow cadence, and — crucially — ZERO LLM calls when
//! the deterministic prefilter finds nothing (an idle gardener costs nothing). The
//! split *decision* needs the model; the *worklist* (which memories are blobs) is
//! produced for free by `MemoryStore::scan_blob_candidates`.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use bamboo_agent_core::Message;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_infrastructure::{
    Config, LLMChunk, LLMProvider, LLMRequestOptions, ProviderModelRouter,
};
use bamboo_memory::auto_dream::{build_blob_split_prompt, parse_split_pieces};
use bamboo_memory::memory_store::{BlobScanItem, MemoryScope, MemoryStore};

use crate::auto_dream::AutoDreamContext;

const GARDENER_TRACING_TARGET: &str = "bamboo.gardener";
const GARDENER_RUNTIME_SESSION_ID: &str = "__gardener__";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GardenerRunResult {
    pub scanned: usize,
    pub flagged: usize,
    pub split: usize,
    pub failed: usize,
}

async fn collect_split_text(
    provider: Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
) -> Result<String, String> {
    let messages = vec![
        Message::system(
            "You are Bamboo's background memory gardener. Split the given memory into atomic pieces and return only the specified JSON. No prose, no markdown fences.",
        ),
        Message::user(prompt),
    ];
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
        let raw = match collect_split_text(provider.clone(), &model, prompt).await {
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

/// Spawn the recurring gardener loop. No-op cost when disabled: each tick reads
/// config and returns immediately if `gardener_enabled` is false.
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
                bamboo_infrastructure::config::MemoryConfig::default().gardener_interval_secs
            });
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if let Err(error) = run_gardener_once(&ctx).await {
                tracing::warn!(
                    target: GARDENER_TRACING_TARGET,
                    event = "run_failed",
                    "[gardener] run failed: {}",
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
    use bamboo_infrastructure::{LLMError, LLMStream, ProviderRegistry, SessionStoreV2};
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp.path().to_path_buf());

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
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                gardener_enabled: true,
                gardener_min_sections: 2,
                gardener_max_splits_per_run: 8,
                ..bamboo_infrastructure::config::MemoryConfig::default()
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp.path().to_path_buf());
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
}
