use crate::agent::core::agent::types::CompressionEvent;
use crate::agent::core::budget::{
    prepare_hybrid_context, HeuristicTokenCounter, PreparedContext, TokenBudget,
};
use crate::agent::core::{AgentError, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use chrono::Utc;
use std::collections::HashSet;

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
    record_compression_event(session, &prepared_context, session_id);

    transforms::apply_message_transforms(config, &mut prepared_context).await?;
    logging::log_context_truncation(session_id, &prepared_context);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

fn record_compression_event(
    session: &mut Session,
    prepared_context: &PreparedContext,
    session_id: &str,
) {
    if prepared_context.compressed_message_ids.is_empty() {
        return;
    }

    let compressed_ids: HashSet<&str> = prepared_context
        .compressed_message_ids
        .iter()
        .map(String::as_str)
        .collect();

    let mut changed_indexes = Vec::new();
    for (index, message) in session.messages.iter_mut().enumerate() {
        if !compressed_ids.contains(message.id.as_str()) || message.compressed {
            continue;
        }
        message.compressed = true;
        changed_indexes.push(index);
    }

    if changed_indexes.is_empty() {
        return;
    }

    let event = CompressionEvent::new(changed_indexes.len(), prepared_context.segments_removed);
    let event_id = event.id.clone();
    for index in changed_indexes {
        session.messages[index].compressed_by_event_id = Some(event_id.clone());
    }
    session.compression_events.push(event.clone());
    session.updated_at = Utc::now();

    log::info!(
        "[{}] Context compression event {} recorded: messages={}, segments_removed={}",
        session_id,
        event.id,
        event.messages_compressed,
        event.segments_removed
    );
}

#[cfg(test)]
mod tests;
