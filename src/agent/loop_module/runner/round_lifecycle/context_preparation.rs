use crate::agent::core::budget::{
    prepare_hybrid_context, HeuristicTokenCounter, PreparedContext, TokenBudget,
};
use crate::agent::core::{AgentError, Session};
use crate::agent::loop_module::config::AgentLoopConfig;

mod logging;
mod ocr_cache;
mod transforms;

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

pub(super) async fn prepare_round_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
) -> Result<PreparedRoundContext, AgentError> {
    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;

    let budget = super::token_budget::resolve_token_budget(session, config, model_name).await;
    let counter = HeuristicTokenCounter::default();

    let mut prepared_context = prepare_hybrid_context(session, &budget, &counter)
        .map_err(|error| AgentError::Budget(error.to_string()))?;

    transforms::apply_message_transforms(config, &mut prepared_context).await?;
    logging::log_context_truncation(session_id, &prepared_context);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

#[cfg(test)]
mod tests;
