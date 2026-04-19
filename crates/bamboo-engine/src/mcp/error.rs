use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum McpError {
    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Tool execution error: {0}")]
    ToolExecution(String),

    #[error("Server not found: {0}")]
    ServerNotFound(String),

    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Server disconnected")]
    Disconnected,

    #[error("Server already running: {0}")]
    AlreadyRunning(String),

    #[error("Server not running: {0}")]
    NotRunning(String),
}

impl From<serde_json::Error> for McpError {
    fn from(e: serde_json::Error) -> Self {
        McpError::Serialization(e.to_string())
    }
}

impl From<std::io::Error> for McpError {
    fn from(e: std::io::Error) -> Self {
        McpError::Transport(e.to_string())
    }
}

impl From<reqwest::Error> for McpError {
    fn from(e: reqwest::Error) -> Self {
        McpError::Transport(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, McpError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_transport() {
        let error = McpError::Transport("connection failed".to_string());
        assert_eq!(format!("{}", error), "Transport error: connection failed");
    }

    #[test]
    fn test_error_display_protocol() {
        let error = McpError::Protocol("invalid message".to_string());
        assert_eq!(format!("{}", error), "Protocol error: invalid message");
    }

    #[test]
    fn test_error_display_connection() {
        let error = McpError::Connection("timeout".to_string());
        assert_eq!(format!("{}", error), "Connection error: timeout");
    }

    #[test]
    fn test_error_display_timeout() {
        let error = McpError::Timeout("request timed out".to_string());
        assert_eq!(format!("{}", error), "Timeout error: request timed out");
    }

    #[test]
    fn test_error_display_tool_execution() {
        let error = McpError::ToolExecution("tool failed".to_string());
        assert_eq!(format!("{}", error), "Tool execution error: tool failed");
    }

    #[test]
    fn test_error_display_server_not_found() {
        let error = McpError::ServerNotFound("test-server".to_string());
        assert_eq!(format!("{}", error), "Server not found: test-server");
    }

    #[test]
    fn test_error_display_tool_not_found() {
        let error = McpError::ToolNotFound("test-tool".to_string());
        assert_eq!(format!("{}", error), "Tool not found: test-tool");
    }

    #[test]
    fn test_error_display_serialization() {
        let error = McpError::Serialization("invalid JSON".to_string());
        assert_eq!(format!("{}", error), "Serialization error: invalid JSON");
    }

    #[test]
    fn test_error_display_invalid_config() {
        let error = McpError::InvalidConfig("missing field".to_string());
        assert_eq!(format!("{}", error), "Invalid configuration: missing field");
    }

    #[test]
    fn test_error_display_disconnected() {
        let error = McpError::Disconnected;
        assert_eq!(format!("{}", error), "Server disconnected");
    }

    #[test]
    fn test_error_display_already_running() {
        let error = McpError::AlreadyRunning("server1".to_string());
        assert_eq!(format!("{}", error), "Server already running: server1");
    }

    #[test]
    fn test_error_display_not_running() {
        let error = McpError::NotRunning("server1".to_string());
        assert_eq!(format!("{}", error), "Server not running: server1");
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json");
        let mcp_error = McpError::from(json_err.unwrap_err());
        match mcp_error {
            McpError::Serialization(_) => {}
            _ => panic!("Expected Serialization error"),
        }
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let mcp_error = McpError::from(io_err);
        match mcp_error {
            McpError::Transport(_) => {}
            _ => panic!("Expected Transport error"),
        }
    }

    #[test]
    fn test_error_clone() {
        let error = McpError::ServerNotFound("test".to_string());
        let cloned = error.clone();
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn test_error_debug() {
        let error = McpError::Disconnected;
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Disconnected"));
    }
}
