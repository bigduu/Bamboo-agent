use serde::{Deserialize, Serialize};

pub const JSONRPC_VERSION: &str = "2.0";

pub mod methods {
    pub const SEND_MESSAGE: &str = "SendMessage";
    pub const SEND_STREAMING_MESSAGE: &str = "SendStreamingMessage";
    pub const GET_TASK: &str = "GetTask";
    pub const LIST_TASKS: &str = "ListTasks";
    pub const CANCEL_TASK: &str = "CancelTask";
    pub const SUBSCRIBE_TO_TASK: &str = "SubscribeToTask";
    pub const GET_EXTENDED_AGENT_CARD: &str = "GetExtendedAgentCard";
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest<T> {
    pub jsonrpc: &'static str,
    pub id: JsonRpcId,
    pub method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum JsonRpcId {
    String(String),
    Number(i64),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    pub id: Option<JsonRpcId>,
    pub result: Option<T>,
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonrpc_request_uses_pascal_case_methods() {
        let req = JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION,
            id: JsonRpcId::String("req-1".to_string()),
            method: methods::SEND_STREAMING_MESSAGE,
            params: Some(serde_json::json!({"message": {}})),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("SendStreamingMessage"));
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"id\":\"req-1\""));
    }

    #[test]
    fn jsonrpc_error_maps_task_not_found() {
        use crate::a2a::error::map_jsonrpc_error;

        let err = JsonRpcError {
            code: -32001,
            message: "Task not found".to_string(),
            data: None,
        };
        let mapped = map_jsonrpc_error(err, Some("task-abc"));
        match mapped {
            crate::a2a::error::A2AClientError::TaskNotFound(id) => {
                assert_eq!(id, "task-abc");
            }
            other => panic!("expected TaskNotFound, got {:?}", other),
        }
    }

    #[test]
    fn jsonrpc_error_maps_unsupported_operation() {
        use crate::a2a::error::map_jsonrpc_error;

        let err = JsonRpcError {
            code: -32004,
            message: "Streaming not supported".to_string(),
            data: None,
        };
        let mapped = map_jsonrpc_error(err, None);
        match mapped {
            crate::a2a::error::A2AClientError::UnsupportedOperation(msg) => {
                assert_eq!(msg, "Streaming not supported");
            }
            other => panic!("expected UnsupportedOperation, got {:?}", other),
        }
    }
}
