use std::sync::Arc;

use crate::provider::LLMProvider;

/// A fully resolved model ready for LLM calls.
///
/// Contains both the provider instance and model name string, so call sites
/// don't need to worry about routing — they just use `.provider` and `.model_name`.
///
/// Created by the unified resolver functions in `model_config_helper`.
pub struct ResolvedModel {
    pub provider: Arc<dyn LLMProvider>,
    pub model_name: String,
}
