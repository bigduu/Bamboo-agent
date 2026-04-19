use bamboo_compression::PreparedContext;

pub(super) fn log_context_truncation(session_id: &str, prepared_context: &PreparedContext) {
    if prepared_context.truncation_occurred {
        tracing::info!(
            "[{}] Context hard-limit fit applied: removed {} segments, using {} tokens of {} ({:.1}%)",
            session_id,
            prepared_context.segments_removed,
            prepared_context.token_usage.total_tokens,
            prepared_context.token_usage.budget_limit,
            prepared_context.token_usage.usage_percentage()
        );
    }
}
