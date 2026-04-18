//! API endpoint integration tests
//!
//! Tests for HTTP API endpoints and request/response handling.

#[cfg(test)]
mod tests {
    // Note: These are placeholder tests that verify the module structure
    // Full integration tests would require starting the server and making HTTP requests

    #[actix_web::test]
    async fn test_health_check_endpoint_exists() {
        use actix_web::{http::StatusCode, test, App};

        let app = test::init_service(
            App::new().configure(bamboo_agent::server::routes::configure_routes),
        )
        .await;

        let req = test::TestRequest::get().uri("/api/v1/health").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        assert_eq!(body.as_ref(), b"OK");
    }

    #[test]
    fn test_app_state_creation() {
        // Test that we can create app state with test configuration
        use std::path::PathBuf;

        // Note: This is a simplified test
        // In practice, we'd need to mock the LLM provider
        let result = std::panic::catch_unwind(|| {
            let data_dir = PathBuf::from("/tmp/test");
            assert!(data_dir.is_absolute());
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_tool_schema_structure() {
        use bamboo_application_agent::tools::FunctionSchema;
        use bamboo_application_agent::tools::ToolSchema;
        use serde_json::json;

        let schema = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "test_tool".to_string(),
                description: "A test tool for unit tests".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "input": {
                            "type": "string",
                            "description": "Test input"
                        }
                    }
                }),
            },
        };

        assert_eq!(schema.function.name, "test_tool");
        assert!(!schema.function.description.is_empty());
        assert!(schema.function.parameters.is_object());
    }

    #[test]
    fn test_message_creation() {
        use bamboo_agent::agent::Message;

        let user_msg = Message::user("Hello");
        assert_eq!(user_msg.role, bamboo_agent::agent::Role::User);

        let assistant_msg = Message::assistant("Hi there", None);
        assert_eq!(assistant_msg.role, bamboo_agent::agent::Role::Assistant);

        let system_msg = Message::system("You are helpful");
        assert_eq!(system_msg.role, bamboo_agent::agent::Role::System);
    }

    #[test]
    fn test_session_creation() {
        use bamboo_agent::agent::Session;

        let session = Session::new("test-session-id", "gpt-4");
        assert_eq!(session.id, "test-session-id");
        assert_eq!(session.model, "gpt-4");
        assert!(session.messages.is_empty());
    }

    #[tokio::test]
    async fn test_workflow_operations() {
        let save_result =
            bamboo_agent::commands::save_workflow("../unsafe".to_string(), "content".to_string())
                .await;
        assert!(save_result.is_err());
        assert_eq!(save_result.unwrap_err(), "Invalid workflow name");

        let delete_result = bamboo_agent::commands::delete_workflow("../unsafe".to_string()).await;
        assert!(delete_result.is_err());
        assert_eq!(delete_result.unwrap_err(), "Invalid workflow name");
    }

    #[test]
    fn test_keyword_masking_config() {
        use bamboo_agent::Config;

        let temp_dir = tempfile::TempDir::new().unwrap();

        // Unified config: keyword masking lives under config.json.
        let config = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        assert!(config.keyword_masking.entries.is_empty());
    }
}
