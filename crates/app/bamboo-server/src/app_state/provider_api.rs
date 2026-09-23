use super::*;

use crate::tools::ToolSurface;

impl AppState {
    /// Get a clone of the current provider
    ///
    /// Returns a thread-safe reference to the current LLM provider.
    /// This is the preferred way to access the provider for making requests.
    ///
    /// # Returns
    ///
    /// An Arc reference to the current provider implementation.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///     let provider = state.get_provider().await;
    ///
    ///     // Use provider to make LLM requests...
    /// }
    /// ```
    pub async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        // Important: return the reloadable handle, not a snapshot clone of the current provider.
        // This ensures config/provider switches take effect without restarting the server.
        self.provider_handle.clone()
    }

    /// Get a provider for a specific [`ProviderModelRef`].
    ///
    /// Used when `features.provider_model_ref` is enabled to route requests
    /// to the correct provider based on the model reference.
    pub fn get_provider_for_model_ref(
        &self,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, AppError> {
        self.provider_router
            .route(target)
            .map_err(|e| AppError::BadRequest(e.to_string()))
    }

    /// Get the appropriate provider for a named provider endpoint (e.g., "openai", "anthropic").
    ///
    /// Uses the registry when the `provider_model_ref` feature flag is enabled,
    /// otherwise falls back to the default provider.
    pub async fn get_provider_for_endpoint(
        &self,
        provider_name: &str,
    ) -> Result<Arc<dyn LLMProvider>, AppError> {
        let use_registry = {
            let config = self.config.read().await;
            config.features.provider_model_ref
        };

        if use_registry {
            self.provider_registry.get(provider_name).ok_or_else(|| {
                AppError::InternalError(anyhow::anyhow!(
                    "Provider '{}' not found in registry",
                    provider_name
                ))
            })
        } else {
            Ok(self.get_provider().await)
        }
    }

    /// Shutdown all MCP servers gracefully
    ///
    /// Sends shutdown signals to all running MCP server processes
    /// and waits for them to terminate cleanly.
    ///
    /// This should be called during application shutdown to ensure
    /// MCP servers are not left running as orphaned processes. Invoked by
    /// [`crate::server::web_service::WebService::stop`]. #119.
    pub async fn shutdown(&self) {
        tracing::info!("Shutting down MCP servers...");
        // Stop the supervised MCP proxy service (issue #47) so its reconnect
        // supervisor exits cleanly instead of looping after an intended stop.
        self.mcp_proxy_shutdown.cancel();
        self.mcp_manager.shutdown_all().await;
        tracing::info!("MCP servers shut down complete");
    }

    /// Get the tool executor for a specific surface variant.
    ///
    /// Use [`ToolSurface::Root`] for primary sessions,
    /// [`ToolSurface::Child`] for child sessions, etc.
    pub fn tools_for(
        &self,
        surface: ToolSurface,
    ) -> Arc<dyn bamboo_agent_core::tools::ToolExecutor> {
        self.tool_factory.get(surface)
    }

    /// Get all tool schemas from the composite tool executor
    ///
    /// Returns schemas for both built-in tools and MCP-provided tools.
    /// These schemas are used to inform the LLM about available tools.
    ///
    /// # Returns
    ///
    /// Vector of tool schemas in Anthropic's tool definition format.
    pub fn get_all_tool_schemas(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        self.tool_factory.get(ToolSurface::Root).list_tools()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create AppState from a temp dir.
    async fn make_state() -> (tempfile::TempDir, AppState) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");
        (temp_dir, state)
    }

    // ---- get_provider ----

    #[tokio::test]
    async fn get_provider_returns_a_provider() {
        let (_temp, state) = make_state().await;
        let provider = state.get_provider().await;
        let models = provider.list_models().await;
        // Default config has no API keys, so the unconfigured provider
        // returns an auth error — but it should still return a provider handle.
        assert!(
            models.is_err(),
            "Default provider should be UnconfiguredProvider"
        );
    }

    // ---- get_provider_for_endpoint ----

    #[tokio::test]
    async fn endpoint_flag_off_returns_default_provider() {
        let (_temp, state) = make_state().await;
        // Flag is OFF by default
        {
            let config = state.config.read().await;
            assert!(!config.features.provider_model_ref);
        }

        let result = state.get_provider_for_endpoint("openai").await;
        assert!(
            result.is_ok(),
            "Flag OFF should always return default provider"
        );
    }

    #[tokio::test]
    async fn endpoint_flag_on_unknown_provider_returns_error() {
        let (_temp, state) = make_state().await;
        {
            let mut config = state.config.write().await;
            config.features.provider_model_ref = true;
        }

        let result = state.get_provider_for_endpoint("nonexistent").await;
        assert!(result.is_err(), "Flag ON with unknown provider should fail");
    }

    #[tokio::test]
    async fn endpoint_flag_on_copilot_returns_provider() {
        let (_temp, state) = make_state().await;
        {
            let mut config = state.config.write().await;
            config.features.provider_model_ref = true;
        }

        // Copilot is always available (no API key required)
        let result = state.get_provider_for_endpoint("copilot").await;
        assert!(result.is_ok(), "Flag ON with copilot should succeed");
    }

    // ---- get_provider_for_model_ref ----

    #[tokio::test]
    async fn model_ref_unknown_provider_returns_error() {
        let (_temp, state) = make_state().await;
        let target = bamboo_domain::ProviderModelRef::new("nonexistent", "some-model");
        let result = state.get_provider_for_model_ref(&target);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn model_ref_copilot_returns_provider() {
        let (_temp, state) = make_state().await;
        let target = bamboo_domain::ProviderModelRef::new("copilot", "gpt-4o");
        let result = state.get_provider_for_model_ref(&target);
        assert!(result.is_ok(), "copilot provider should be routable");
    }

    // ---- tools_for ----

    #[tokio::test]
    async fn tools_for_root_returns_tool_executor() {
        let (_temp, state) = make_state().await;
        let executor = state.tools_for(ToolSurface::Root);
        let schemas = executor.list_tools();
        assert!(!schemas.is_empty(), "Root tools should not be empty");
    }

    #[tokio::test]
    async fn root_agent_advertises_browser_and_dispatches_it_with_session_context() {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolError, ToolExecutionContext};

        let (_temp, state) = make_state().await;
        let root = state.tools_for(ToolSurface::Root);
        let browser = root
            .list_tools()
            .into_iter()
            .find(|schema| schema.function.name == "browser")
            .expect("root agent browser schema");
        assert!(browser.function.parameters["properties"]
            .get("session_id")
            .is_none());
        assert!(!state
            .tools_for(ToolSurface::Child)
            .list_tools()
            .iter()
            .any(|schema| schema.function.name == "browser"));

        let call = ToolCall {
            id: "browser-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "browser".into(),
                arguments: serde_json::json!({"action":"snapshot","session_id":"other"})
                    .to_string(),
            },
        };
        let mut context = ToolExecutionContext::none(&call.id);
        context.session_id = Some("current-chat");
        assert!(matches!(
            root.execute_with_context(&call, context).await,
            Err(ToolError::InvalidArguments(_))
        ));
    }

    #[tokio::test]
    #[ignore = "requires BAMBOO_BROWSER_TEST_URL and the Playwright Chromium runtime"]
    async fn root_agent_browser_actions_share_the_workbench_page() {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};

        let url = std::env::var("BAMBOO_BROWSER_TEST_URL").expect("fixture URL");
        let (_temp, state) = make_state().await;
        let session_id = "browser-tool-integration";
        let opened = state.browser.open(session_id).await.unwrap();
        let navigated = state
            .browser
            .command(
                session_id,
                "navigate",
                serde_json::json!({"url": url, "expected_epoch": opened["page_epoch"]}),
            )
            .await
            .unwrap();
        let epoch = navigated["page_epoch"].as_u64().unwrap();
        let root = state.tools_for(ToolSurface::Root);

        let dispatch = |action: serde_json::Value| ToolCall {
            id: "browser-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "browser".into(),
                arguments: action.to_string(),
            },
        };
        let click = dispatch(serde_json::json!({
            "action": "click",
            "selector": "#increment",
            "expected_epoch": epoch
        }));
        let mut context = ToolExecutionContext::none(&click.id);
        context.session_id = Some(session_id);
        context.bypass_permissions = true;
        assert!(
            root.execute_with_context(&click, context)
                .await
                .unwrap()
                .success
        );

        let dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(dom["snapshot"].as_str().unwrap().contains("Count 1"));
        let snapshot = dispatch(serde_json::json!({"action":"snapshot"}));
        assert!(root
            .execute_with_context(&snapshot, context)
            .await
            .unwrap()
            .result
            .contains("Count 1"));
        let screenshot = dispatch(serde_json::json!({"action":"screenshot"}));
        let result = root
            .execute_with_context(&screenshot, context)
            .await
            .unwrap();
        assert_eq!(result.images.len(), 1);
        assert_eq!(result.images[0].mime_type, "image/jpeg");
        assert!(result.images[0].data.len() > 1000);
        state.browser.close(session_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn root_browser_host_controls_change_the_same_page_as_the_workbench() {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const PAGE: &str = r#"<!doctype html><html><head><title>Browser tool controls</title>
<style>body{margin:0}#increment{position:absolute;left:10px;top:10px;width:120px;height:40px}#name{position:absolute;left:10px;top:60px}#count{position:absolute;left:10px;top:120px}#typed{position:absolute;left:10px;top:145px}</style>
</head><body><button id="increment">Increment</button><input id="name" aria-label="Name"><output id="count">Count 0</output><output id="typed">Name empty</output>
<script>let count=0;const name=document.querySelector('#name');document.querySelector('#increment').onclick=()=>{document.querySelector('#count').textContent='Count '+(++count)};name.oninput=()=>{document.querySelector('#typed').textContent='Name '+(name.value||'empty')}</script></body></html>"#;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0; 2048];
                    let _ = socket.read(&mut request).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nX-Frame-Options: DENY\r\nConnection: close\r\n\r\n{PAGE}",
                        PAGE.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });

        let (_temp, state) = make_state().await;
        let session_id = "browser-tool-host-controls";
        let root = state.tools_for(ToolSurface::Root);
        let dispatch = |action: serde_json::Value| ToolCall {
            id: "browser-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "browser".into(),
                arguments: action.to_string(),
            },
        };
        macro_rules! run {
            ($args:expr) => {{
                let call = dispatch($args);
                let mut context = ToolExecutionContext::none(&call.id);
                context.session_id = Some(session_id);
                context.bypass_permissions = true;
                root.execute_with_context(&call, context).await.unwrap()
            }};
        }
        let initial = run!(serde_json::json!({"action":"snapshot"}));
        let initial_epoch = initial
            .result
            .lines()
            .next()
            .unwrap()
            .strip_prefix("page_epoch: ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let navigated: serde_json::Value = serde_json::from_str(
            &run!(
                serde_json::json!({"action":"navigate","url":url,"expected_epoch":initial_epoch})
            )
            .result,
        )
        .unwrap();
        let epoch = navigated["page_epoch"].as_u64().unwrap();
        let snapshot = run!(serde_json::json!({"action":"snapshot"}));
        assert!(snapshot.result.contains("Increment"));

        let clicked: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"click_at","x":40,"y":20,"expected_epoch":epoch}))
                .result,
        )
        .unwrap();
        assert_eq!(clicked["page_epoch"], epoch);
        let dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(dom["snapshot"].as_str().unwrap().contains("Count 1"));

        run!(serde_json::json!({"action":"click","selector":"#name","expected_epoch":epoch}));
        run!(serde_json::json!({"action":"type","text":"Lotus","expected_epoch":epoch}));
        run!(serde_json::json!({"action":"key","key":"Backspace","expected_epoch":epoch}));
        let dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(dom["snapshot"].as_str().unwrap().contains("Name Lotu"));
        let screenshot = run!(serde_json::json!({"action":"screenshot"}));
        assert_eq!(screenshot.images.len(), 1);
        assert!(screenshot.images[0].data.len() > 1000);

        let resized: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"viewport","width":640,"height":480,"expected_epoch":epoch})).result,
        )
        .unwrap();
        let resized_epoch = resized["page_epoch"].as_u64().unwrap();
        assert_ne!(resized_epoch, epoch);
        assert_eq!(
            resized["viewport"],
            serde_json::json!({"width":640,"height":480})
        );
        let stale =
            dispatch(serde_json::json!({"action":"click_at","x":40,"y":20,"expected_epoch":epoch}));
        let mut context = ToolExecutionContext::none(&stale.id);
        context.session_id = Some(session_id);
        context.bypass_permissions = true;
        assert!(root.execute_with_context(&stale, context).await.is_err());
        assert_eq!(
            state.browser.state(session_id).await.unwrap()["page_epoch"],
            resized_epoch
        );

        let second_url = format!("{url}?step=2");
        let second: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"navigate","url":second_url,"expected_epoch":resized_epoch})).result,
        )
        .unwrap();
        let back: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"history","direction":"back","expected_epoch":second["page_epoch"]})).result,
        )
        .unwrap();
        assert_eq!(back["url"], url);
        let forward: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"history","direction":"forward","expected_epoch":back["page_epoch"]})).result,
        )
        .unwrap();
        assert_eq!(forward["url"], second_url);
        let reloaded: serde_json::Value = serde_json::from_str(
            &run!(serde_json::json!({"action":"history","direction":"reload","expected_epoch":forward["page_epoch"]})).result,
        )
        .unwrap();
        assert_eq!(reloaded["url"], second_url);
        assert_eq!(
            state.browser.state(session_id).await.unwrap()["page_epoch"],
            reloaded["page_epoch"]
        );
        state.browser.close(session_id).await.unwrap();
        fixture.abort();
    }

    #[tokio::test]
    async fn workflow_run_tool_is_root_only_and_cannot_recursively_dispatch_itself() {
        let (_temp, state) = make_state().await;
        let names = |surface| {
            state
                .tools_for(surface)
                .list_tools()
                .into_iter()
                .map(|schema| schema.function.name)
                .collect::<std::collections::HashSet<_>>()
        };
        assert!(names(ToolSurface::Root).contains("workflow_run"));
        assert!(!names(ToolSurface::Base).contains("workflow_run"));
        assert!(!names(ToolSurface::Child).contains("workflow_run"));
    }

    #[tokio::test]
    async fn workflow_run_root_overlay_advertises_provider_safe_parameters() {
        let (_temp, state) = make_state().await;
        let workflow_run = state
            .tools_for(ToolSurface::Root)
            .list_tools()
            .into_iter()
            .find(|schema| schema.function.name == "workflow_run")
            .expect("root workflow_run overlay");
        let parameters = &workflow_run.function.parameters;

        for combinator in ["oneOf", "anyOf", "allOf"] {
            assert!(
                parameters.get(combinator).is_none(),
                "root workflow_run overlay must not advertise {combinator}"
            );
        }
        let properties = parameters["properties"]
            .as_object()
            .expect("root workflow_run properties");
        assert!(
            !properties.is_empty(),
            "root workflow_run overlay must not advertise empty properties"
        );
        for field in [
            "action",
            "workflow_id",
            "revision",
            "args",
            "budget",
            "run_id",
            "since",
        ] {
            assert!(
                properties.contains_key(field),
                "root workflow_run overlay is missing {field}"
            );
        }
    }

    // ---- get_all_tool_schemas ----

    #[tokio::test]
    async fn get_all_tool_schemas_includes_core_tools() {
        let (_temp, state) = make_state().await;
        let schemas = state.get_all_tool_schemas();
        let names: std::collections::HashSet<&str> =
            schemas.iter().map(|s| s.function.name.as_str()).collect();
        assert!(names.contains("Task"));
        assert!(names.contains("SubAgent"));
    }
}
