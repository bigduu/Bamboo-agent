//! Core agent functionality for Bamboo.

pub mod agent;
pub mod composition;
pub mod storage;
pub mod tools;
pub mod workspace_state;

// Re-export commonly used types (mirrors current agent/core/mod.rs)
pub use agent::events::{AgentEvent, TokenUsage};
pub use agent::hooks::AgentHook;
pub use agent::types::{parse_prompt_external_memory_sections, PromptSnapshotExternalMemoryParts};
pub use agent::types::{
    CompressionEvent, CompressionTriggerType, ConversationSummary, ImageOcrLine, ImageOcrResult,
    ImageUrlRef, Message, MessageContent, MessagePart, MessagePhase, PendingQuestion,
    PromptMemoryObservability, PromptSnapshot, Role, Session, SessionKind,
};
pub use agent::AgentError;
pub use bamboo_domain::TokenBudgetUsage;
pub use storage::Storage;
pub use tools::{
    classify_tool, execute_tool_call, finalize_tool_calls, handle_tool_result_with_agentic_support,
    normalize_tool_name, parse_tool_args, parse_tool_args_best_effort, try_parse_agentic_result,
    AgenticContext, AgenticTool, AgenticToolResult, FunctionCall, FunctionSchema, RegistryError,
    SharedTool, SmartCodeReviewTool, Tool, ToolCall, ToolCallAccumulator, ToolError,
    ToolExecutionContext, ToolExecutor, ToolGoal, ToolHandlingOutcome, ToolMutability,
    ToolRegistry, ToolResult, ToolSchema,
};
