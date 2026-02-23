use thiserror::Error;

#[derive(Error, Debug)]
pub enum AgentError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("LLM error: {0}")]
    LLM(String),

    #[error("Tool error: {0}")]
    Tool(String),

    #[error("Budget error: {0}")]
    Budget(String),

    #[error("Cancelled")]
    Cancelled,
}
