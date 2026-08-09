//! Process-wide low-priority concurrency budget for auxiliary LLM requests.
//!
//! The key uses the provider allocation identity plus model. Provider registry
//! clones retain the same `Arc` allocation across sessions, while a provider
//! reload receives a fresh identity. Weak semaphore entries disappear once no
//! request is waiting/running, avoiding an unbounded per-reload registry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bamboo_llm::LLMProvider;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AuxiliaryBudgetKey {
    provider_identity: usize,
    model_name: String,
}

struct AuxiliaryBudgetEntry {
    configured_limit: usize,
    semaphore: Weak<Semaphore>,
}

static AUXILIARY_BUDGETS: OnceLock<Mutex<HashMap<AuxiliaryBudgetKey, AuxiliaryBudgetEntry>>> =
    OnceLock::new();

fn provider_identity(provider: &Arc<dyn LLMProvider>) -> usize {
    Arc::as_ptr(provider) as *const () as usize
}

fn semaphore(
    provider: &Arc<dyn LLMProvider>,
    model_name: &str,
    configured_limit: usize,
) -> Arc<Semaphore> {
    let configured_limit = configured_limit.clamp(
        1,
        crate::runtime::config::MAX_AUXILIARY_EVALUATION_MAX_CONCURRENCY,
    );
    let key = AuxiliaryBudgetKey {
        provider_identity: provider_identity(provider),
        model_name: model_name.to_string(),
    };
    let registry = AUXILIARY_BUDGETS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registry.retain(|_, entry| entry.semaphore.strong_count() > 0);

    if let Some(entry) = registry.get(&key) {
        if let Some(semaphore) = entry.semaphore.upgrade() {
            if entry.configured_limit != configured_limit {
                tracing::warn!(
                    provider_identity = key.provider_identity,
                    model = %key.model_name,
                    active_limit = entry.configured_limit,
                    requested_limit = configured_limit,
                    "auxiliary evaluation budget already active with a different limit; retaining the active process-wide limit"
                );
            }
            return semaphore;
        }
    }

    let semaphore = Arc::new(Semaphore::new(configured_limit));
    registry.insert(
        key,
        AuxiliaryBudgetEntry {
            configured_limit,
            semaphore: Arc::downgrade(&semaphore),
        },
    );
    semaphore
}

/// Wait for one low-priority slot for this exact provider allocation/model.
/// Foreground request paths never call this function.
pub(crate) async fn acquire(
    provider: &Arc<dyn LLMProvider>,
    model_name: &str,
    configured_limit: usize,
) -> OwnedSemaphorePermit {
    semaphore(provider, model_name, configured_limit)
        .acquire_owned()
        .await
        .expect("auxiliary evaluation semaphore is never closed")
}

#[cfg(test)]
mod tests {
    use super::acquire;
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct ImmediateProvider;

    #[async_trait::async_trait]
    impl LLMProvider for ImmediateProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    #[tokio::test]
    async fn task_and_gold_sessions_share_provider_model_limit_without_blocking_foreground() {
        let provider: Arc<dyn LLMProvider> = Arc::new(ImmediateProvider);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut handles = Vec::new();

        // Alternate Task and Gold labels across distinct logical sessions. Both
        // production call paths acquire this exact primitive.
        for index in 0..6 {
            let provider = provider.clone();
            let active = active.clone();
            let peak = peak.clone();
            let entered = entered.clone();
            let release = release.clone();
            handles.push(tokio::spawn(async move {
                let _kind = if index % 2 == 0 { "task" } else { "gold" };
                let _session_id = format!("session-{index}");
                let _permit = acquire(&provider, "shared-fast-model", 2).await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                entered.fetch_add(1, Ordering::SeqCst);
                let release_permit = release.acquire().await.expect("release gate stays open");
                release_permit.forget();
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two auxiliary evaluations should enter");
        assert_eq!(peak.load(Ordering::SeqCst), 2);

        // The budget is opt-in at auxiliary dispatch sites, not a wrapper on
        // the provider itself. A foreground call therefore completes while all
        // low-priority slots are occupied.
        let _foreground_stream = tokio::time::timeout(
            Duration::from_millis(100),
            provider.chat_stream(&[], &[], None, "shared-fast-model"),
        )
        .await
        .expect("foreground provider call must not wait for auxiliary permits")
        .expect("foreground provider call succeeds");

        for expected_entered in [4, 6] {
            release.add_permits(2);
            tokio::time::timeout(Duration::from_secs(1), async {
                while entered.load(Ordering::SeqCst) < expected_entered {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("next auxiliary batch should enter");
            assert!(active.load(Ordering::SeqCst) <= 2);
        }
        release.add_permits(2);
        for handle in handles {
            handle.await.expect("auxiliary session joins");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }
}
