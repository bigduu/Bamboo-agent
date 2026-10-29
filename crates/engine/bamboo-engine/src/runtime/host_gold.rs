//! Per-execution proof of the provider selected by an accepted main attempt.
//!
//! The Host supplies a source resolver; observing an Arc is never itself
//! authorization. An absent observation remains a local Gold refusal.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use bamboo_llm::{LLMError, LLMProvider};

/// The Host's explicit classification of the exact selected main provider.
#[doc(hidden)]
#[derive(Clone)]
pub enum HostGoldOrigin {
    Managed {
        provider_name: String,
        origin: Arc<bamboo_llm::provider_registry::ManagedProviderOrigin>,
    },
    /// The selected allocation is the Host's explicit injected provider.
    Injected,
}

/// Resolve source from the exact selected Arc; failure never means injection.
#[doc(hidden)]
pub type HostGoldOriginResolver =
    Arc<dyn Fn(&Arc<dyn LLMProvider>) -> Result<HostGoldOrigin, String> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct MainGoldReceipt {
    pub selected: Arc<dyn LLMProvider>,
    pub origin: HostGoldOrigin,
}

#[derive(Default)]
struct RunState {
    active: Option<Arc<()>>,
    accepted: Option<Result<MainGoldReceipt, String>>,
}

pub(crate) struct HostGoldRun {
    resolver: HostGoldOriginResolver,
    state: Mutex<RunState>,
}

#[derive(Default)]
struct AttemptState {
    sealed: bool,
    observed: Option<Result<MainGoldReceipt, String>>,
}

pub(crate) struct HostGoldAttempt {
    run: Weak<HostGoldRun>,
    token: Arc<()>,
    state: Mutex<AttemptState>,
}

impl HostGoldRun {
    pub(crate) fn new(resolver: HostGoldOriginResolver) -> Arc<Self> {
        Arc::new(Self {
            resolver,
            state: Mutex::new(RunState::default()),
        })
    }

    /// Invalidate the prior receipt before preparation, budget, or dispatch.
    pub(crate) fn begin(self: &Arc<Self>) -> Arc<HostGoldAttempt> {
        let token = Arc::new(());
        let mut state = self.state.lock();
        state.active = Some(token.clone());
        state.accepted = None;
        Arc::new(HostGoldAttempt {
            run: Arc::downgrade(self),
            token,
            state: Mutex::new(AttemptState::default()),
        })
    }

    pub(crate) fn accepted(&self) -> Result<MainGoldReceipt, String> {
        self.state
            .lock()
            .accepted
            .clone()
            .unwrap_or_else(|| Err("Main provider origin was not confirmed for Gold".into()))
    }
}

impl HostGoldAttempt {
    pub(crate) fn observer(self: &Arc<Self>) -> bamboo_llm::provider::MainDispatchObserver {
        let attempt = self.clone();
        Arc::new(move |selected| {
            let Some(run) = attempt.run.upgrade() else {
                return;
            };
            let mut state = attempt.state.lock();
            if state.sealed {
                return;
            }
            if state.observed.is_some() {
                // A nested/overlapping dispatch is ambiguous even if a later
                // callback reports the same Arc or a successful proof.
                state.observed = Some(Err(
                    "Multiple main provider observations are ambiguous for Gold".into(),
                ));
                return;
            }
            state.observed =
                Some((run.resolver)(&selected).map(|origin| MainGoldReceipt { selected, origin }));
        })
    }

    /// Commit only after the matching stream passed the runner's validation.
    pub(crate) fn accept(&self) {
        let Some(run) = self.run.upgrade() else {
            return;
        };
        let observed = {
            let mut state = self.state.lock();
            state.sealed = true;
            state
                .observed
                .take()
                .unwrap_or_else(|| Err("Main provider was not observed for Gold".into()))
        };
        let mut state = run.state.lock();
        if state
            .active
            .as_ref()
            .is_some_and(|token| Arc::ptr_eq(token, &self.token))
        {
            state.accepted = Some(observed);
            state.active = None;
        }
    }
}

/// A failure to prove the source is not permission to use the mutable main.
pub(crate) struct UnprovenGold {
    pub reason: String,
    pub refused: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl LLMProvider for UnprovenGold {
    fn known_pre_dispatch_error(&self) -> Option<LLMError> {
        Some(LLMError::Api(self.reason.clone()))
    }
    fn queue_requires_current_route(&self) -> bool {
        true
    }
    fn local_dispatch_refused(&self) -> bool {
        self.refused.load(std::sync::atomic::Ordering::Acquire)
    }
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[bamboo_agent_core::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<bamboo_llm::LLMStream, LLMError> {
        self.refused
            .store(true, std::sync::atomic::Ordering::Release);
        Err(LLMError::Api(self.reason.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    #[async_trait::async_trait]
    impl LLMProvider for Fake {
        async fn chat_stream(
            &self,
            _: &[bamboo_agent_core::Message],
            _: &[bamboo_agent_core::ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<bamboo_llm::LLMStream, LLMError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[tokio::test]
    async fn main_gold_attempt_commits_only_its_own_observation() {
        let a: Arc<dyn LLMProvider> = Arc::new(Fake);
        let b: Arc<dyn LLMProvider> = Arc::new(Fake);
        let resolver: HostGoldOriginResolver = Arc::new(|_| Ok(HostGoldOrigin::Injected));
        let run = HostGoldRun::new(resolver);
        let old = run.begin();
        (old.observer())(a.clone());
        let current = run.begin();
        old.accept();
        assert!(run.accepted().is_err());
        (current.observer())(b.clone());
        current.accept();
        assert!(Arc::ptr_eq(&run.accepted().unwrap().selected, &b));
        // A late callback from an earlier attempt cannot replace the receipt.
        (old.observer())(a);
        assert!(Arc::ptr_eq(&run.accepted().unwrap().selected, &b));
        let missing = run.begin();
        missing.accept();
        assert!(run.accepted().is_err());
    }

    #[tokio::test]
    async fn main_gold_unscoped_dispatch_never_reuses_prior_receipt() {
        let a: Arc<dyn LLMProvider> = Arc::new(Fake);
        let run = HostGoldRun::new(Arc::new(|_| Ok(HostGoldOrigin::Injected)));
        let first = run.begin();
        bamboo_llm::provider::scope_main_dispatch(first.observer(), async {
            assert!(bamboo_llm::provider::observe_main_dispatch(a.clone()));
        })
        .await;
        first.accept();
        assert!(run.accepted().is_ok());
        let second = run.begin();
        let spawned = tokio::spawn(async move { bamboo_llm::provider::observe_main_dispatch(a) });
        assert!(!spawned.await.unwrap());
        second.accept();
        assert!(run.accepted().is_err());
    }

    #[test]
    fn main_gold_multiple_observations_never_replace_failure_with_success() {
        let a: Arc<dyn LLMProvider> = Arc::new(Fake);
        let b: Arc<dyn LLMProvider> = Arc::new(Fake);
        let denied = a.clone();
        let run = HostGoldRun::new(Arc::new(move |selected| {
            if Arc::ptr_eq(selected, &denied) {
                Err("synthetic origin failure".into())
            } else {
                Ok(HostGoldOrigin::Injected)
            }
        }));
        let attempt = run.begin();
        let observe = attempt.observer();
        observe(a);
        observe(b.clone());
        observe(b);
        attempt.accept();
        assert!(run.accepted().is_err());
    }
}
