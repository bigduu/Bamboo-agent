//! Token budget management — re-exported from bamboo-compression.

pub use bamboo_compression::{
    HeuristicTokenCounter, TiktokenTokenCounter, TokenCounter, ModelLimitsRegistry, create_budget_for_model,
    MessageSegmenter, HeuristicSummarizer, LlmSummarizer, Summarizer, SummaryManager,
    SummaryTrigger, prepare_hybrid_context, BudgetError, BudgetStrategy, PreparedContext,
    TokenBudget, TokenUsageBreakdown,
};
pub use bamboo_compression::{
    active_messages_for_budget, apply_compression_plan, build_compression_plan_with_summary,
    build_forced_compression_plan_with_summary, build_summary_prompt, compression_summary_message,
    context_window_usage_percent, estimate_context_compression_exposure,
    normalized_trigger_percent, summary_source_messages, CompressionPlan, CompressionPlanError,
    ContextCompressionExposure,
};
