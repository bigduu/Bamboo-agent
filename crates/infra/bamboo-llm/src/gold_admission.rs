//! Per-run explicit Gold routing. The Host retains the exact selected account;
//! a later registry refresh must not silently redirect a pending evaluation.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::provider::{LLMError, LLMProvider, LLMRequestOptions, ProviderModelInfo, Result};
use crate::{LLMStream, PromptIR, ProviderRegistry, ProviderVisibleToolFootprint};
use async_trait::async_trait;
use bamboo_domain::{CapabilityLoadingMode, Message, ProviderModelRef, ToolSchema};
use tokio::sync::RwLock;

/// The caller supplies the exact execution provider and its already budgeted
/// delegate. This wrapper checks before entering that delegate, so a local
/// refusal cannot reserve Work tokens or replace the original account.
#[doc(hidden)]
pub fn guarded_gold_target(
    config: Arc<RwLock<bamboo_config::Config>>,
    registry: Arc<ProviderRegistry>,
    target: ProviderModelRef,
    expected: Arc<dyn LLMProvider>,
    delegate: Arc<dyn LLMProvider>,
) -> Arc<dyn LLMProvider> {
    Arc::new(GuardedGoldProvider {
        config,
        registry,
        target,
        expected,
        delegate,
        dispatch_refused: AtomicBool::new(false),
    })
}

/// Created once per evaluation, so the refusal marker cannot be confused with
/// another request or with a subsequent configuration change.
struct GuardedGoldProvider {
    config: Arc<RwLock<bamboo_config::Config>>,
    registry: Arc<ProviderRegistry>,
    target: ProviderModelRef,
    expected: Arc<dyn LLMProvider>,
    delegate: Arc<dyn LLMProvider>,
    dispatch_refused: AtomicBool,
}

impl GuardedGoldProvider {
    fn validate(&self) -> Result<Arc<dyn LLMProvider>> {
        let expected = &self.expected;
        let config = self.config.try_read().map_err(|_| {
            LLMError::Api("Live configuration is busy while selecting a Gold route".into())
        })?;
        let (current, managed) = self
            .registry
            .provider_for_configured_model_with_origin(&config, &self.target)?;
        if !managed {
            return Err(LLMError::Api(
                "Gold provider is no longer config-managed".into(),
            ));
        }
        if !Arc::ptr_eq(&current, expected) {
            return Err(LLMError::Api(
                "Gold provider generation changed during this run".into(),
            ));
        }
        if let Some(error) = expected.known_pre_dispatch_error() {
            return Err(error);
        }
        Ok(expected.clone())
    }

    fn dispatch(&self, model: &str) -> Result<Arc<dyn LLMProvider>> {
        let selected = if model == self.target.model {
            self.validate().map(|_| self.delegate.clone())
        } else {
            Err(LLMError::Api(
                "Gold model changed after route selection".into(),
            ))
        };
        if selected.is_err() {
            self.dispatch_refused.store(true, Ordering::Release);
        }
        selected
    }
}

#[async_trait]
impl LLMProvider for GuardedGoldProvider {
    fn queue_requires_current_route(&self) -> bool {
        true
    }

    // Queue identity is the original concrete target, while the queued
    // snapshot retains this guard and its per-request refusal state.
    fn queued_route_identity(&self) -> crate::provider::QueuedRouteIdentity {
        crate::provider::queued_identity_for(&self.expected)
    }

    fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
        Ok(None)
    }

    fn known_pre_dispatch_error(&self) -> Option<LLMError> {
        self.validate()
            .err()
            .or_else(|| self.delegate.known_pre_dispatch_error())
    }

    fn local_dispatch_refused(&self) -> bool {
        self.dispatch_refused.load(Ordering::Acquire)
    }

    fn resource_allocation_identity(&self) -> Option<usize> {
        Some(
            self.delegate
                .resource_allocation_identity()
                .unwrap_or(Arc::as_ptr(&self.delegate) as *const () as usize),
        )
    }

    async fn supports_vision(&self, model: &str) -> bool {
        self.delegate.supports_vision(model).await
    }
    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        self.delegate.vision_support_override(model).await
    }
    async fn capability_loading_mode(
        &self,
        model: &str,
        required: Option<&str>,
    ) -> CapabilityLoadingMode {
        self.delegate.capability_loading_mode(model, required).await
    }

    async fn provider_visible_tool_footprint(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        model: &str,
        required: Option<&str>,
    ) -> Result<ProviderVisibleToolFootprint> {
        self.validate()?;
        self.delegate
            .provider_visible_tool_footprint(ir, tools, model, required)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        self.dispatch(model)?
            .chat_stream(messages, tools, max, model)
            .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        self.dispatch(model)?
            .chat_stream_with_options(messages, tools, max, model, options)
            .await
    }

    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        self.dispatch(model)?
            .chat_stream_ir(ir, tools, max, model, options)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        self.validate()?;
        self.delegate.list_models().await
    }

    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
        self.validate()?;
        self.delegate.list_model_info().await
    }
}

#[cfg(test)]
mod queued_guard_tests {
    use super::*;
    use crate::provider::{queued_identity_for, queued_snapshot_for, QueuedRouteIdentity};
    use std::collections::HashMap;

    struct UnexpectedCall;
    #[async_trait]
    impl LLMProvider for UnexpectedCall {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            panic!("this synthetic provider must not be called")
        }
    }

    struct SnapshotWrapper(Arc<dyn LLMProvider>);
    #[async_trait]
    impl LLMProvider for SnapshotWrapper {
        fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
            Ok(Some(self.0.clone()))
        }
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            output: Option<u32>,
            model: &str,
        ) -> Result<LLMStream> {
            self.0.chat_stream(messages, tools, output, model).await
        }
    }

    #[tokio::test]
    async fn queued_gold_guards_share_identity_but_not_refusal_state() {
        let expected: Arc<dyn LLMProvider> = Arc::new(UnexpectedCall);
        let config = Arc::new(RwLock::new(bamboo_config::Config::default()));
        let registry = Arc::new(ProviderRegistry::new(HashMap::new(), "fixture".into()));
        let make = || {
            guarded_gold_target(
                config.clone(),
                registry.clone(),
                ProviderModelRef::new("fixture", "gold"),
                expected.clone(),
                expected.clone(),
            )
        };
        let first = make();
        let second = make();
        assert!(!Arc::ptr_eq(&first, &second));
        match (queued_identity_for(&first), queued_identity_for(&second)) {
            (QueuedRouteIdentity::Stable(a), QueuedRouteIdentity::Stable(b)) => {
                assert!(Arc::ptr_eq(&a, &b));
            }
            _ => panic!("the synthetic underlying provider has one stable identity"),
        }
        let snapshot = queued_snapshot_for(first.clone()).unwrap();
        assert!(
            Arc::ptr_eq(&first, &snapshot),
            "retain the exact guarded request"
        );
        assert!(first
            .chat_stream(&[], &[], None, "different")
            .await
            .is_err());
        assert!(first.local_dispatch_refused());
        assert!(
            !second.local_dispatch_refused(),
            "other evaluation must not inherit refusal"
        );
        let nested = queued_snapshot_for(Arc::new(SnapshotWrapper(first))).unwrap();
        assert!(
            nested.local_dispatch_refused(),
            "fixed queued wrapper must forward the original refusal"
        );
    }
}

#[cfg(test)]
mod error_classification_tests {
    use super::*;

    struct ErrorAfterConfigChange {
        config: Arc<RwLock<bamboo_config::Config>>,
    }

    #[async_trait]
    impl LLMProvider for ErrorAfterConfigChange {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            self.config
                .write()
                .await
                .provider_instances
                .get_mut("chat-a")
                .unwrap()
                .enabled = false;
            Err(LLMError::Api("synthetic upstream bootstrap error".into()))
        }
    }

    #[tokio::test]
    async fn upstream_error_does_not_become_refusal_if_config_changes_after_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = bamboo_config::Config::default();
        config.provider = "chat-a".into();
        config.default_provider_instance = Some("chat-a".into());
        config.provider_instances.insert(
            "chat-a".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "enabled":true, "api_key":"synthetic-only",
                "base_url":"http://127.0.0.1:9", "model":"gold", "runtime_models":["gold"]
            }))
            .unwrap(),
        );
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, dir.path().into())
                .await
                .unwrap(),
        );
        let target = ProviderModelRef::new("chat-a", "gold");
        let expected = registry
            .provider_for_configured_model(&config, &target)
            .unwrap();
        let config = Arc::new(RwLock::new(config));
        let delegate: Arc<dyn LLMProvider> = Arc::new(ErrorAfterConfigChange {
            config: config.clone(),
        });
        let guard = guarded_gold_target(config, registry, target, expected, delegate);
        assert!(guard.known_pre_dispatch_error().is_none());
        assert!(guard.chat_stream(&[], &[], None, "gold").await.is_err());
        assert!(
            guard.known_pre_dispatch_error().is_some(),
            "the state did change"
        );
        assert!(
            !guard.local_dispatch_refused(),
            "the guard actually called its delegate"
        );
    }
}
