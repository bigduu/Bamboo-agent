//! Default adapter implementations — wrap existing runner functions behind manager traits.

mod lifecycle;
mod llm;
mod memory;
mod mini_loop;
mod prompt;
mod tool;

pub use lifecycle::DefaultLifecycleManager;
pub use llm::DefaultLlmManager;
pub use memory::DefaultMemoryManager;
pub use mini_loop::DefaultMiniLoopExecutor;
pub use prompt::DefaultPromptManager;
pub use tool::DefaultToolManager;
