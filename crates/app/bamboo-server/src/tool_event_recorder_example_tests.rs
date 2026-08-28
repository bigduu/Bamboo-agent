use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::ToolExecutionContext;
use bamboo_plugin_protocol::ToolEventPublisher;
use serde_json::json;

use crate::app_state::AppState;
use crate::tools::ToolSurface;

#[tokio::test]
async fn default_app_state_has_no_tool_event_background_work() {
    let data = tempfile::tempdir().unwrap();
    let state = AppState::new(data.path().to_path_buf()).await.unwrap();
    state.wait_for_boot_reconcile_services().await;

    assert!(state.service_manager.list_status().await.is_empty());
    assert!(!state.tool_event_publisher.is_enabled());
    assert!(!state.tool_event_router.is_enabled());
    assert_eq!(
        state
            .tool_event_router
            .registration_and_worker_counts()
            .await,
        (0, 0)
    );
    assert!(!state.tool_event_router.monitor_is_running());

    let path = data.path().join("no-sink-write.txt");
    let call = ToolCall {
        id: "no-sink-write".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Write".to_string(),
            arguments: json!({"file_path": path, "content": "still inert"}).to_string(),
        },
    };
    let result = state
        .tools_for(ToolSurface::Base)
        .execute_with_context(
            &call,
            ToolExecutionContext {
                session_id: Some("no-sink-session"),
                root_session_id: Some("no-sink-root"),
                tool_call_id: &call.id,
                event_tx: None,
                available_tool_schemas: None,
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            },
        )
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(
        state
            .tool_event_router
            .registration_and_worker_counts()
            .await,
        (0, 0)
    );
    assert!(!state.tool_event_router.monitor_is_running());
}
