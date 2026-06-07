//! Cohesive model/provider value object shared by the request/spawn param structs.
//!
//! Historically the per-request model selection was a "data clump" of ~8 loose
//! fields (`model`, `provider_name`, `provider_type`, plus three auxiliary
//! role pairs `<role>_model` + `<role>_model_provider`) repeated verbatim across
//! [`ExecuteRequest`](crate::ExecuteRequest),
//! [`ExecuteRequestBuilder`](crate::ExecuteRequestBuilder),
//! `SessionExecutionArgs`, and the server's `ResolvedRunConfig` /
//! `SpawnAgentExecution`. [`ModelRoster`] collapses that clump into one value
//! object so the param structs carry a single cohesive field.
//!
//! ## Lives in `bamboo-engine`
//!
//! [`RoleModel::provider_override`] is an `Arc<dyn LLMProvider>`, and
//! `LLMProvider` lives in `bamboo-infrastructure`. `bamboo-domain` does **not**
//! depend on `bamboo-infrastructure`, so hosting the roster there would force a
//! new runtime dependency just to carry a live provider handle. `bamboo-engine`
//! already depends on infrastructure (and is depended on by the server), making
//! it the natural home for a value object that carries live provider handles.
//!
//! ## Resolution semantics are preserved byte-for-byte
//!
//! The roster carries *intent* only; it performs no `Config` fallback itself.
//! A `None` auxiliary role maps exactly to the old "both name and provider were
//! `None`" state, so the downstream `None → Config::get_*` fallbacks in
//! [`AgentRuntime::execute`](crate::AgentRuntime) are unchanged. The primary
//! `model`/`provider_name`/`provider_type` keep the same request→config cascade
//! at their resolution sites.

use std::sync::Arc;

use bamboo_llm::LLMProvider;

/// A single auxiliary role's model selection: an optional model name plus an
/// optional dedicated provider handle.
///
/// Both fields are independent and optional so the mapping from the old loose
/// `<role>_model: Option<String>` + `<role>_model_provider: Option<Arc<…>>`
/// pair is lossless. A role is represented as `Some(RoleModel)` only when at
/// least one of the two was set; an all-`None` role collapses to `None` on the
/// owning [`ModelRoster`], preserving the old "fall back to `Config`" behavior.
#[derive(Clone, Default)]
pub struct RoleModel {
    /// Model name for this role. `None` defers to the relevant `Config::get_*`
    /// fallback at the resolution site.
    pub name: Option<String>,
    /// Optional dedicated provider handle for this role's LLM calls. `None`
    /// uses the shared agent-loop provider.
    pub provider_override: Option<Arc<dyn LLMProvider>>,
}

impl RoleModel {
    /// Build a role from the old loose pair, collapsing an all-`None` pair to
    /// `None` so it maps exactly onto the previous fallback behavior.
    pub fn from_parts(
        name: Option<String>,
        provider_override: Option<Arc<dyn LLMProvider>>,
    ) -> Option<Self> {
        if name.is_none() && provider_override.is_none() {
            None
        } else {
            Some(Self {
                name,
                provider_override,
            })
        }
    }
}

/// Cohesive value object for a request's primary + auxiliary model selection.
///
/// This replaces the loose model/provider field clump on the per-request param
/// structs. Capability-driven models (`planning_model_name`,
/// `search_model_name`) are intentionally **not** part of the roster — they are
/// resolved separately from `Config` and are not role-driven.
#[derive(Clone, Default)]
pub struct ModelRoster {
    /// Primary (main chat) model name. `None` defers to the config default at
    /// the resolution site (see [`AgentRuntime::execute`](crate::AgentRuntime)).
    pub model: Option<String>,
    /// Provider routing key for the primary model. `None` cascades to
    /// `Config::provider` at the resolution site.
    pub provider_name: Option<String>,
    /// Underlying provider type for the primary model (e.g. `openai`,
    /// `anthropic`, `copilot`). Distinct from `provider_name` so
    /// provider-specific behavior stays correct when the routing key is an
    /// instance id.
    pub provider_type: Option<String>,
    /// Fast/cheap role. `None` falls back to `Config::get_fast_model()`.
    pub fast: Option<RoleModel>,
    /// Memory/background role. `None` falls back to
    /// `Config::get_memory_background_model()`.
    pub background: Option<RoleModel>,
    /// Summarization/compression role. `None` falls back to
    /// `Config::get_task_summary_model()`.
    pub summarization: Option<RoleModel>,
}

impl ModelRoster {
    /// A roster with no overrides — every selection defers to `Config`.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build a roster from a resolved [`GlobalAreaModels`] plus the primary
    /// model selection. Centralizes the fast/background/summarization mapping
    /// (`RoleModel::from_parts(area.model_name, area.provider)`) that the
    /// execute handler and schedule manager previously hand-rolled identically.
    pub fn from_areas(
        model: Option<String>,
        provider_name: Option<String>,
        provider_type: Option<String>,
        areas: crate::model_areas::GlobalAreaModels,
    ) -> Self {
        Self {
            model,
            provider_name,
            provider_type,
            fast: RoleModel::from_parts(
                areas.fast.as_ref().map(|m| m.model_name.clone()),
                areas.fast.map(|m| m.provider),
            ),
            background: RoleModel::from_parts(
                areas.background.as_ref().map(|m| m.model_name.clone()),
                areas.background.map(|m| m.provider),
            ),
            summarization: RoleModel::from_parts(
                areas.summarization.as_ref().map(|m| m.model_name.clone()),
                areas.summarization.map(|m| m.provider),
            ),
        }
    }

    /// Fast-model name override, if any.
    pub fn fast_model(&self) -> Option<String> {
        self.fast.as_ref().and_then(|r| r.name.clone())
    }

    /// Fast-model dedicated provider handle, if any.
    pub fn fast_model_provider(&self) -> Option<Arc<dyn LLMProvider>> {
        self.fast.as_ref().and_then(|r| r.provider_override.clone())
    }

    /// Background-model name override, if any.
    pub fn background_model(&self) -> Option<String> {
        self.background.as_ref().and_then(|r| r.name.clone())
    }

    /// Background-model dedicated provider handle, if any.
    pub fn background_model_provider(&self) -> Option<Arc<dyn LLMProvider>> {
        self.background
            .as_ref()
            .and_then(|r| r.provider_override.clone())
    }

    /// Summarization-model name override, if any.
    pub fn summarization_model(&self) -> Option<String> {
        self.summarization.as_ref().and_then(|r| r.name.clone())
    }

    /// Summarization-model dedicated provider handle, if any.
    pub fn summarization_model_provider(&self) -> Option<Arc<dyn LLMProvider>> {
        self.summarization
            .as_ref()
            .and_then(|r| r.provider_override.clone())
    }

    /// Set the fast role from the old loose pair.
    pub fn set_fast(&mut self, name: Option<String>, provider: Option<Arc<dyn LLMProvider>>) {
        self.fast = RoleModel::from_parts(name, provider);
    }

    /// Set the background role from the old loose pair.
    pub fn set_background(&mut self, name: Option<String>, provider: Option<Arc<dyn LLMProvider>>) {
        self.background = RoleModel::from_parts(name, provider);
    }

    /// Set the summarization role from the old loose pair.
    pub fn set_summarization(
        &mut self,
        name: Option<String>,
        provider: Option<Arc<dyn LLMProvider>>,
    ) {
        self.summarization = RoleModel::from_parts(name, provider);
    }
}
