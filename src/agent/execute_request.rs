//! Ergonomic builder for [`bamboo_engine::ExecuteRequest`].
//!
//! `ExecuteRequest` (TD-6) carries three required fields plus a long tail of
//! optional overrides — split provider fields included (see ergonomic-sdk-plan
//! §C2). Constructing it by hand forces consumers to spell out ~20 `None`s.
//! [`ExecuteRequestBuilder`] requires only `initial_message` + `event_tx` +
//! `cancel_token`, defaulting every optional field to `None` (matching the
//! runtime's current spawn defaults), and exposes fluent setters for the rest.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::ReasoningEffort;
use bamboo_engine::{AuxiliaryModelConfig, ExecuteRequest, ImageFallbackConfig};
use bamboo_infrastructure::LLMProvider;

/// Fluent builder for [`ExecuteRequest`].
///
/// Required: `initial_message`, `event_tx`, `cancel_token` (set at construction).
/// Every other field defaults to `None` and can be overridden via a setter.
pub struct ExecuteRequestBuilder {
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    cancel_token: CancellationToken,

    tools: Option<Arc<dyn ToolExecutor>>,
    provider_override: Option<Arc<dyn LLMProvider>>,
    model: Option<String>,
    provider_name: Option<String>,
    provider_type: Option<String>,
    fast_model: Option<String>,
    fast_model_provider: Option<Arc<dyn LLMProvider>>,
    background_model: Option<String>,
    background_model_provider: Option<Arc<dyn LLMProvider>>,
    summarization_model: Option<String>,
    summarization_model_provider: Option<Arc<dyn LLMProvider>>,
    reasoning_effort: Option<ReasoningEffort>,
    auxiliary_model_resolver: Option<Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>>,
    disabled_tools: Option<BTreeSet<String>>,
    disabled_skill_ids: Option<BTreeSet<String>>,
    selected_skill_ids: Option<Vec<String>>,
    selected_skill_mode: Option<String>,
    image_fallback: Option<ImageFallbackConfig>,
    app_data_dir: Option<PathBuf>,
}

impl ExecuteRequestBuilder {
    /// Create a builder with the three required fields. All optional overrides
    /// default to `None`.
    pub fn new(
        initial_message: impl Into<String>,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            initial_message: initial_message.into(),
            event_tx,
            cancel_token,
            tools: None,
            provider_override: None,
            model: None,
            provider_name: None,
            provider_type: None,
            fast_model: None,
            fast_model_provider: None,
            background_model: None,
            background_model_provider: None,
            summarization_model: None,
            summarization_model_provider: None,
            reasoning_effort: None,
            auxiliary_model_resolver: None,
            disabled_tools: None,
            disabled_skill_ids: None,
            selected_skill_ids: None,
            selected_skill_mode: None,
            image_fallback: None,
            app_data_dir: None,
        }
    }

    /// Override the tool executor for this execution.
    pub fn tools(mut self, v: Arc<dyn ToolExecutor>) -> Self {
        self.tools = Some(v);
        self
    }

    /// Override the LLM provider for this execution.
    pub fn provider_override(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.provider_override = Some(v);
        self
    }

    /// Override the primary model name.
    pub fn model(mut self, v: impl Into<String>) -> Self {
        self.model = Some(v.into());
        self
    }

    /// Override the provider name.
    pub fn provider_name(mut self, v: impl Into<String>) -> Self {
        self.provider_name = Some(v.into());
        self
    }

    /// Override the provider type.
    pub fn provider_type(mut self, v: impl Into<String>) -> Self {
        self.provider_type = Some(v.into());
        self
    }

    /// Override the fast-model name.
    pub fn fast_model(mut self, v: impl Into<String>) -> Self {
        self.fast_model = Some(v.into());
        self
    }

    /// Override the provider used for fast-model calls.
    pub fn fast_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.fast_model_provider = Some(v);
        self
    }

    /// Override the background-model name.
    pub fn background_model(mut self, v: impl Into<String>) -> Self {
        self.background_model = Some(v.into());
        self
    }

    /// Override the provider used for background/memory model calls.
    pub fn background_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.background_model_provider = Some(v);
        self
    }

    /// Override the summarization-model name.
    pub fn summarization_model(mut self, v: impl Into<String>) -> Self {
        self.summarization_model = Some(v.into());
        self
    }

    /// Override the provider used for summarization/compression calls.
    pub fn summarization_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.summarization_model_provider = Some(v);
        self
    }

    /// Set the reasoning effort.
    pub fn reasoning_effort(mut self, v: ReasoningEffort) -> Self {
        self.reasoning_effort = Some(v);
        self
    }

    /// Set the per-round auxiliary-model resolver.
    pub fn auxiliary_model_resolver(
        mut self,
        v: Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>,
    ) -> Self {
        self.auxiliary_model_resolver = Some(v);
        self
    }

    /// Set the disabled tool names (merged with config defaults at runtime).
    pub fn disabled_tools(mut self, v: BTreeSet<String>) -> Self {
        self.disabled_tools = Some(v);
        self
    }

    /// Set the disabled skill ids.
    pub fn disabled_skill_ids(mut self, v: BTreeSet<String>) -> Self {
        self.disabled_skill_ids = Some(v);
        self
    }

    /// Set the explicitly selected skill ids.
    pub fn selected_skill_ids(mut self, v: Vec<String>) -> Self {
        self.selected_skill_ids = Some(v);
        self
    }

    /// Set the skill-selection mode.
    pub fn selected_skill_mode(mut self, v: impl Into<String>) -> Self {
        self.selected_skill_mode = Some(v.into());
        self
    }

    /// Set the image fallback configuration.
    pub fn image_fallback(mut self, v: ImageFallbackConfig) -> Self {
        self.image_fallback = Some(v);
        self
    }

    /// Set the Bamboo application data directory.
    pub fn app_data_dir(mut self, v: PathBuf) -> Self {
        self.app_data_dir = Some(v);
        self
    }

    /// Materialize the underlying [`ExecuteRequest`].
    ///
    /// `gold_config` is intentionally not surfaced as a setter (it is an
    /// internal feature flag); it always defaults to `None`.
    pub fn build(self) -> ExecuteRequest {
        ExecuteRequest {
            initial_message: self.initial_message,
            event_tx: self.event_tx,
            cancel_token: self.cancel_token,
            tools: self.tools,
            provider_override: self.provider_override,
            model: self.model,
            provider_name: self.provider_name,
            provider_type: self.provider_type,
            fast_model: self.fast_model,
            fast_model_provider: self.fast_model_provider,
            background_model: self.background_model,
            background_model_provider: self.background_model_provider,
            summarization_model: self.summarization_model,
            summarization_model_provider: self.summarization_model_provider,
            reasoning_effort: self.reasoning_effort,
            auxiliary_model_resolver: self.auxiliary_model_resolver,
            disabled_tools: self.disabled_tools,
            disabled_skill_ids: self.disabled_skill_ids,
            selected_skill_ids: self.selected_skill_ids,
            selected_skill_mode: self.selected_skill_mode,
            image_fallback: self.image_fallback,
            gold_config: None,
            app_data_dir: self.app_data_dir,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_with_only_required_fields_defaults_optionals_to_none() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let req = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new()).build();

        assert_eq!(req.initial_message, "hello");
        assert!(req.tools.is_none());
        assert!(req.provider_override.is_none());
        assert!(req.model.is_none());
        assert!(req.provider_name.is_none());
        assert!(req.provider_type.is_none());
        assert!(req.fast_model.is_none());
        assert!(req.fast_model_provider.is_none());
        assert!(req.background_model.is_none());
        assert!(req.background_model_provider.is_none());
        assert!(req.summarization_model.is_none());
        assert!(req.summarization_model_provider.is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.auxiliary_model_resolver.is_none());
        assert!(req.disabled_tools.is_none());
        assert!(req.disabled_skill_ids.is_none());
        assert!(req.selected_skill_ids.is_none());
        assert!(req.selected_skill_mode.is_none());
        assert!(req.image_fallback.is_none());
        assert!(req.gold_config.is_none());
        assert!(req.app_data_dir.is_none());
    }

    #[test]
    fn setters_round_trip_into_request() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let mut disabled = BTreeSet::new();
        disabled.insert("Edit".to_string());

        let req = ExecuteRequestBuilder::new("go", tx, CancellationToken::new())
            .model("claude-x")
            .provider_name("anthropic")
            .disabled_tools(disabled.clone())
            .build();

        assert_eq!(req.model.as_deref(), Some("claude-x"));
        assert_eq!(req.provider_name.as_deref(), Some("anthropic"));
        assert_eq!(req.disabled_tools, Some(disabled));
    }
}
