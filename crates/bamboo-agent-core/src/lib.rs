//! Core agent functionality for Bamboo.

pub mod agent;
pub mod composition;
pub mod storage;
pub mod tools;
pub mod workspace_state;

// Re-export commonly used types (mirrors current agent/core/mod.rs)
pub use agent::events::{AgentEvent, TokenUsage};
pub use bamboo_domain::TokenBudgetUsage;
pub use agent::types::{
    CompressionEvent, ConversationSummary, ImageOcrLine, ImageOcrResult, ImageUrlRef, Message,
    MessageContent, MessagePart, MessagePhase, PendingQuestion, PromptMemoryObservability,
    PromptSnapshot, Role, Session, SessionKind,
};
pub use agent::AgentError;
pub use agent::hooks::AgentHook;
pub use agent::types::{
    parse_prompt_external_memory_sections, PromptSnapshotExternalMemoryParts,
};
pub use storage::Storage;
pub use tools::{
    execute_tool_call, finalize_tool_calls, handle_tool_result_with_agentic_support,
    parse_tool_args, parse_tool_args_best_effort, try_parse_agentic_result,
    normalize_tool_name,
    AgenticContext, AgenticTool, AgenticToolResult,
    SmartCodeReviewTool, Tool, ToolCall, ToolCallAccumulator, ToolError, ToolExecutor, ToolGoal,
    ToolHandlingOutcome, ToolResult, ToolSchema, ToolMutability, ToolRegistry, ToolExecutionContext,
    SharedTool, RegistryError, FunctionSchema,
    classify_tool, FunctionCall,
};
