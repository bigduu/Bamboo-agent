use crate::agent::core::tools::ToolCall;

#[derive(Debug, Clone)]
pub enum LLMChunk {
    Token(String),
    ReasoningToken(String),
    ToolCalls(Vec<ToolCall>),
    Done,
}
