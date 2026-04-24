//! Manager traits — abstract interfaces for the agent loop's core responsibilities.
//!
//! Each manager owns one concern:
//! - **PromptManager** — system prompt assembly
//! - **MemoryManager** — external memory recall and compression
//! - **ToolManager** — tool surface, schemas, execution
//! - **LlmManager** — LLM interaction and stream handling
//! - **LifecycleManager** — state machine and round transitions
//! - **MiniLoopExecutor** — cheap model fast decisions

pub mod adapters;
pub mod lifecycle;
pub mod llm;
pub mod memory;
pub mod mini_loop;
pub mod prompt;
pub mod tool;

pub use lifecycle::LifecycleManager;
pub use llm::LlmManager;
pub use memory::MemoryManager;
pub use mini_loop::MiniLoopExecutor;
pub use prompt::PromptManager;
pub use tool::ToolManager;

// Re-export concrete implementations from adapters.
pub use adapters::LLMMiniLoopExecutor;
