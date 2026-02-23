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

pub mod counter;
pub mod limits;
pub mod preparation;
pub mod segmenter;
pub mod summarizer;
pub mod types;

pub use counter::{HeuristicTokenCounter, TokenCounter};
pub use limits::{create_budget_for_model, ModelLimitsRegistry};
pub use preparation::prepare_hybrid_context;
pub use segmenter::MessageSegmenter;
pub use summarizer::{HeuristicSummarizer, SummaryManager, SummaryTrigger, Summarizer};
pub use types::{
    BudgetError, BudgetStrategy, PreparedContext, TokenBudget, TokenUsageBreakdown,
};
