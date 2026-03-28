//! Token budget management for LLM conversations.
//!
//! This module provides token budget management to prevent context window overflow
//! while maintaining conversation continuity. It implements a hybrid approach
//! combining rolling summary with recent message windowing.
//!
//! # Key Components
//!
//! - [`types`]: Core types like `TokenBudget`, `BudgetStrategy`, `PreparedContext`
//! - [`counter`]: Token counting via heuristic estimation (future: tiktoken)
//! - [`segmenter`]: Message segmentation preserving tool-call atomicity
//! - [`limits`]: Model context window limits registry
//! - [`preparation`]: Context preparation with budget enforcement
//! - [`summarizer`]: Conversation summarization for context preservation

pub mod compression_tooling;
pub mod counter;
pub mod limits;
pub mod preparation;
pub mod segmenter;
pub mod summarizer;
pub mod types;

pub use compression_tooling::{
    active_messages_for_budget, apply_compression_plan, build_compression_plan_with_summary,
    build_forced_compression_plan_with_summary, build_summary_prompt, compression_summary_message,
    estimate_context_compression_exposure, summary_source_messages, CompressionPlan,
    ContextCompressionExposure,
};
pub use counter::{HeuristicTokenCounter, TokenCounter};
pub use limits::{create_budget_for_model, ModelLimitsRegistry};
pub use preparation::prepare_hybrid_context;
pub use segmenter::MessageSegmenter;
pub use summarizer::{
    HeuristicSummarizer, LlmSummarizer, Summarizer, SummaryManager, SummaryTrigger,
};
pub use types::{BudgetError, BudgetStrategy, PreparedContext, TokenBudget, TokenUsageBreakdown};
