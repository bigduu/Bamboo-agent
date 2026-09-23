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
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn root_browser_semantic_targets_share_the_workbench_page() {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{sleep, timeout, Duration};

        const PAGE: &str = r#"<!doctype html><html><head><title>Semantic browser targets</title>
<style>body{font:16px sans-serif}button,input,output,iframe{display:block;margin:8px}iframe{width:420px;height:200px;border:1px solid black}</style>
</head><body><button>Save</button><button>Save</button><button id="continue">Continue</button>
<label for="account">Account</label><input id="account"><button id="reveal">Reveal result</button>
<output id="status">Status idle</output><output id="frame-state">Frame pending</output>
<script>
const status=document.querySelector('#status');const account=document.querySelector('#account');
document.querySelector('#continue').onclick=()=>status.textContent='Continue clicked';
account.oninput=()=>status.textContent='Account '+account.value;
account.onkeydown=e=>{if(e.key==='Enter')status.textContent='Account entered '+account.value};
document.querySelector('#reveal').onclick=()=>status.textContent='Text activated';
window.onmessage=e=>{if(e.data?.kind==='frame-ready')document.querySelector('#frame-state').textContent='Frame ready';if(e.data?.kind==='frame-result')status.textContent=e.data.text};
</script><iframe id="child" title="Child frame" src="/frame"></iframe></body></html>"#;
        const FRAME: &str = r#"<!doctype html><html><body>
<label for="note">Frame note</label><input id="note"><button id="apply">Apply Frame</button>
<a href="/frame-next">Open next frame</a>
<script>document.querySelector('#apply').onclick=()=>parent.postMessage({kind:'frame-result',text:'Frame applied '+document.querySelector('#note').value},'*');parent.postMessage({kind:'frame-ready'},'*')</script>
</body></html>"#;
        const FRAME_NEXT: &str =
            "<!doctype html><html><body><p>Next frame loaded</p></body></html>";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0; 2048];
                    let size = socket.read(&mut request).await.unwrap_or(0);
                    let line = String::from_utf8_lossy(&request[..size]);
                    let body = if line.starts_with("GET /frame-next ") {
                        FRAME_NEXT
                    } else if line.starts_with("GET /frame ") {
                        FRAME
                    } else {
                        PAGE
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });

        let (_temp, state) = make_state().await;
        let session_id = "browser-tool-semantic-targets";
        let root = state.tools_for(ToolSurface::Root);
        let dispatch = |action: serde_json::Value| ToolCall {
            id: "semantic-browser-call".into(),
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
        run!(serde_json::json!({"action":"navigate","url":url,"expected_epoch":initial_epoch}));
        let epoch = timeout(Duration::from_secs(5), async {
            loop {
                let dom = state
                    .browser
                    .command(session_id, "dom", serde_json::json!({}))
                    .await
                    .unwrap();
                if dom["snapshot"].as_str().unwrap().contains("Frame ready") {
                    break dom["page_epoch"].as_u64().unwrap();
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("initial iframe loaded before semantic actions");

        let duplicate = dispatch(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"button","name":"Save"},
            "expected_epoch":epoch
        }));
        let mut context = ToolExecutionContext::none(&duplicate.id);
        context.session_id = Some(session_id);
        context.bypass_permissions = true;
        let ambiguous = root
            .execute_with_context(&duplicate, context)
            .await
            .unwrap_err();
        assert!(ambiguous.to_string().contains("matched 2"), "{ambiguous}");
        let missing = dispatch(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"button","name":"Missing"},
            "expected_epoch":epoch
        }));
        let mut context = ToolExecutionContext::none(&missing.id);
        context.session_id = Some(session_id);
        context.bypass_permissions = true;
        let not_found = root
            .execute_with_context(&missing, context)
            .await
            .unwrap_err();
        assert!(
            not_found.to_string().contains("target not found"),
            "{not_found}"
        );
        let dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(dom["snapshot"].as_str().unwrap().contains("Status idle"));

        run!(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"button","name":"Continue"},
            "expected_epoch":epoch
        }));
        run!(serde_json::json!({
            "action":"fill", "target":{"kind":"label","value":"Account"},
            "text":"Lotus", "expected_epoch":epoch
        }));
        run!(serde_json::json!({
            "action":"press", "target":{"kind":"label","value":"Account"},
            "key":"Enter", "expected_epoch":epoch
        }));
        let pressed = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(pressed["snapshot"]
            .as_str()
            .unwrap()
            .contains("Account entered Lotus"));
        run!(serde_json::json!({
            "action":"fill", "target":{"kind":"label","value":"Account"},
            "text":"", "expected_epoch":epoch
        }));
        let cleared = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(cleared["html"]
            .as_str()
            .unwrap()
            .contains(">Account </output>"));
        run!(serde_json::json!({
            "action":"click", "target":{"kind":"text","value":"Reveal result"},
            "expected_epoch":epoch
        }));
        let revealed = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(revealed["snapshot"]
            .as_str()
            .unwrap()
            .contains("Text activated"));

        run!(serde_json::json!({
            "action":"fill", "target":{"kind":"label","value":"Frame note","frame_selector":"iframe#child"},
            "text":"Lotus", "expected_epoch":epoch
        }));
        run!(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"button","name":"Apply Frame","frame_selector":"iframe#child"},
            "expected_epoch":epoch
        }));
        let dom = timeout(Duration::from_secs(5), async {
            loop {
                let dom = state
                    .browser
                    .command(session_id, "dom", serde_json::json!({}))
                    .await
                    .unwrap();
                if dom["snapshot"]
                    .as_str()
                    .unwrap()
                    .contains("Frame applied Lotus")
                {
                    break dom;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("iframe action reflected in the shared page DOM");
        let root_snapshot = run!(serde_json::json!({"action":"snapshot"}));
        assert!(root_snapshot.result.contains("Frame applied Lotus"));
        assert_eq!(dom["page_epoch"], epoch);
        let root_screenshot = run!(serde_json::json!({"action":"screenshot"}));
        let direct_screenshot = state
            .browser
            .command(session_id, "screenshot", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(root_screenshot.images.len(), 1);
        assert_eq!(root_screenshot.images[0].mime_type, "image/jpeg");
        assert!(root_screenshot.images[0].data.len() > 1000);
        assert_eq!(direct_screenshot["page_epoch"], dom["page_epoch"]);
        assert_eq!(direct_screenshot["url"], dom["url"]);
        assert!(root_screenshot
            .result
            .contains(&format!("page_epoch {epoch}")));
        assert!(root_screenshot.result.contains(&url));

        run!(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"link","name":"Open next frame","frame_selector":"iframe#child"},
            "expected_epoch":epoch
        }));
        let next_epoch = timeout(Duration::from_secs(5), async {
            loop {
                let current = state.browser.state(session_id).await.unwrap();
                let current_epoch = current["page_epoch"].as_u64().unwrap();
                if current_epoch != epoch {
                    assert_eq!(current["url"], url);
                    break current_epoch;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("child frame navigation changed the page epoch");
        assert_ne!(next_epoch, epoch);
        let stale = dispatch(serde_json::json!({
            "action":"click", "target":{"kind":"role","role":"button","name":"Apply Frame","frame_selector":"iframe#child"},
            "expected_epoch":epoch
        }));
        let mut context = ToolExecutionContext::none(&stale.id);
        context.session_id = Some(session_id);
        context.bypass_permissions = true;
        let error = root
            .execute_with_context(&stale, context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("page changed"), "{error}");

        state.browser.close(session_id).await.unwrap();
        fixture.abort();
    }

    async fn browser_regression_fixture(
        routes: &'static [(&'static str, &'static str)],
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0; 2048];
                    let size = socket.read(&mut request).await.unwrap_or(0);
                    let path = String::from_utf8_lossy(&request[..size])
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    let body = routes
                        .iter()
                        .find(|(route, _)| *route == path)
                        .map(|(_, body)| *body)
                        .unwrap_or("not found");
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        (url, fixture)
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn hidden_iframe_navigation_replaces_cached_workbench_frame() {
        use tokio::time::{sleep, timeout, Duration};

        const MAIN: &str = r#"<!doctype html><html><head><title>Hidden frame</title></head><body>
<output id="phase">Waiting for child</output>
<script>window.onmessage=e=>{if(e.data==='old-ready')document.querySelector('#phase').textContent='Old frame ready'}</script>
<iframe id="hidden" style="display:none" src="/old"></iframe></body></html>"#;
        const OLD: &str = r#"<!doctype html><html><body><script>
parent.postMessage('old-ready','*');setTimeout(()=>location.replace('/new'),4000)
</script></body></html>"#;
        const NEW: &str = "<!doctype html><html><body>New hidden frame</body></html>";
        let (url, fixture) =
            browser_regression_fixture(&[("/", MAIN), ("/old", OLD), ("/new", NEW)]).await;
        let (_temp, state) = make_state().await;
        let session_id = "browser-hidden-iframe-epoch";
        let opened = state.browser.open(session_id).await.unwrap();
        state
            .browser
            .command(
                session_id,
                "navigate",
                serde_json::json!({"url":url,"expected_epoch":opened["page_epoch"]}),
            )
            .await
            .unwrap();

        let old_epoch = timeout(Duration::from_secs(5), async {
            loop {
                let dom = state
                    .browser
                    .command(session_id, "dom", serde_json::json!({}))
                    .await
                    .unwrap();
                if dom["snapshot"]
                    .as_str()
                    .unwrap()
                    .contains("Old frame ready")
                {
                    break dom["page_epoch"].as_u64().unwrap();
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("initial child frame loaded");
        let old_frame = timeout(Duration::from_secs(3), async {
            let mut after = 0;
            loop {
                if let Some(frame) = state.browser.frame(session_id, after, 500).await.unwrap() {
                    after = frame.frame_seq;
                    if frame.page_epoch == old_epoch {
                        break frame;
                    }
                }
            }
        })
        .await
        .expect("initial screenshot for old epoch");
        assert!(old_frame.jpeg.len() > 1000);

        let new_epoch = timeout(Duration::from_secs(8), async {
            loop {
                let current = state.browser.state(session_id).await.unwrap();
                let epoch = current["page_epoch"].as_u64().unwrap();
                if epoch != old_epoch {
                    assert_eq!(current["url"], url);
                    break epoch;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("hidden child frame navigated");
        let dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(dom["html"].as_str().unwrap().contains("Old frame ready"));
        assert_eq!(dom["page_epoch"], new_epoch);
        if let Some(cached) = state.browser.frame(session_id, 0, 0).await.unwrap() {
            assert_eq!(cached.page_epoch, new_epoch, "old JPEG remained cached");
        }
        let fresh = state
            .browser
            .frame(session_id, old_frame.frame_seq, 5_000)
            .await
            .unwrap()
            .expect("fresh screenshot after hidden iframe navigation");
        assert_eq!(fresh.page_epoch, new_epoch);
        assert!(fresh.frame_seq > old_frame.frame_seq);
        assert!(fresh.jpeg.len() > 1000);

        state.browser.close(session_id).await.unwrap();
        fixture.abort();
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn waiting_browser_targets_do_not_retarget_after_navigation() {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};
        use tokio::time::{sleep, timeout, Duration};

        const MAIN: &str = r#"<!doctype html><html><head><title>Pay target</title></head><body>
<output id="status">Unpaid</output><output id="phase">Old Pay pending</output>
<script>window.onmessage=e=>{if(e.data==='old-ready')document.querySelector('#phase').textContent='Old Pay ready';if(e.data==='new-ready')document.querySelector('#phase').textContent='New Pay ready';if(e.data==='paid')document.querySelector('#status').textContent='Paid'}</script>
<iframe id="pay" src="/pay-old"></iframe></body></html>"#;
        const PAY_OLD: &str = r#"<!doctype html><html><body><button disabled>Pay</button><script>
parent.postMessage('old-ready','*');setTimeout(()=>location.replace('/pay-new'),2000)
</script></body></html>"#;
        const PAY_NEW: &str = r#"<!doctype html><html><body>
<button onclick="parent.postMessage('paid','*')">Pay</button>
<script>parent.postMessage('new-ready','*')</script></body></html>"#;
        const CSS_OLD: &str = r#"<!doctype html><html><body><button id="pay" disabled>Pay</button>
<script>setTimeout(()=>location.replace('/css-new'),2000)</script></body></html>"#;
        const CSS_NEW: &str = r#"<!doctype html><html><body><button id="pay" onclick="document.querySelector('#status').textContent='CSS paid'">Pay</button><output id="status">CSS unpaid</output></body></html>"#;
        let (url, fixture) = browser_regression_fixture(&[
            ("/", MAIN),
            ("/pay-old", PAY_OLD),
            ("/pay-new", PAY_NEW),
            ("/css-old", CSS_OLD),
            ("/css-new", CSS_NEW),
        ])
        .await;
        let (_temp, state) = make_state().await;
        let session_id = "browser-pinned-target-navigation";
        let root = state.tools_for(ToolSurface::Root);
        let dispatch = |action: serde_json::Value| ToolCall {
            id: "pinned-browser-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "browser".into(),
                arguments: action.to_string(),
            },
        };
        macro_rules! browser_context {
            ($call:expr) => {{
                let mut context = ToolExecutionContext::none(&$call.id);
                context.session_id = Some(session_id);
                context.bypass_permissions = true;
                context
            }};
        }
        let opened = state.browser.open(session_id).await.unwrap();
        state
            .browser
            .command(
                session_id,
                "navigate",
                serde_json::json!({"url":url,"expected_epoch":opened["page_epoch"]}),
            )
            .await
            .unwrap();
        let old_epoch = timeout(Duration::from_secs(5), async {
            loop {
                let dom = state
                    .browser
                    .command(session_id, "dom", serde_json::json!({}))
                    .await
                    .unwrap();
                if dom["snapshot"].as_str().unwrap().contains("Old Pay ready") {
                    break dom["page_epoch"].as_u64().unwrap();
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("disabled Pay loaded in the old frame");
        let old_click = dispatch(serde_json::json!({
            "action":"click",
            "target":{"kind":"role","role":"button","name":"Pay","frame_selector":"iframe#pay"},
            "expected_epoch":old_epoch
        }));
        let error = timeout(
            Duration::from_secs(6),
            root.execute_with_context(&old_click, browser_context!(old_click)),
        )
        .await
        .expect("old disabled Pay action completed")
        .expect_err("old-epoch action must not click Pay in the new frame");
        assert!(
            error.to_string().contains("page changed")
                || error.to_string().contains("not attached")
                || error.to_string().contains("detached"),
            "{error}"
        );
        let new_epoch = timeout(Duration::from_secs(5), async {
            loop {
                let dom = state
                    .browser
                    .command(session_id, "dom", serde_json::json!({}))
                    .await
                    .unwrap();
                if dom["snapshot"].as_str().unwrap().contains("New Pay ready") {
                    assert!(dom["snapshot"].as_str().unwrap().contains("Unpaid"));
                    break dom["page_epoch"].as_u64().unwrap();
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("new enabled Pay loaded without old action side effect");
        assert_ne!(new_epoch, old_epoch);
        let fresh_click = dispatch(serde_json::json!({
            "action":"click",
            "target":{"kind":"role","role":"button","name":"Pay","frame_selector":"iframe#pay"},
            "expected_epoch":new_epoch
        }));
        root.execute_with_context(&fresh_click, browser_context!(fresh_click))
            .await
            .unwrap();
        let paid = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(paid["snapshot"].as_str().unwrap().contains("Paid"));

        let css_old = state
            .browser
            .command(
                session_id,
                "navigate",
                serde_json::json!({"url":format!("{url}css-old"),"expected_epoch":new_epoch}),
            )
            .await
            .unwrap();
        let css_epoch = css_old["page_epoch"].as_u64().unwrap();
        let old_css_click = dispatch(serde_json::json!({
            "action":"click","selector":"#pay","expected_epoch":css_epoch
        }));
        timeout(
            Duration::from_secs(6),
            root.execute_with_context(&old_css_click, browser_context!(old_css_click)),
        )
        .await
        .expect("old disabled CSS action completed")
        .expect_err("old-epoch CSS action must not click Pay on a new page");
        let css_new_epoch = state.browser.state(session_id).await.unwrap()["page_epoch"]
            .as_u64()
            .unwrap();
        assert_ne!(css_new_epoch, css_epoch);
        let css_dom = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(css_dom["snapshot"].as_str().unwrap().contains("CSS unpaid"));
        let fresh_css_click = dispatch(serde_json::json!({
            "action":"click","selector":"#pay","expected_epoch":css_new_epoch
        }));
        root.execute_with_context(&fresh_css_click, browser_context!(fresh_css_click))
            .await
            .unwrap();
        let css_paid = state
            .browser
            .command(session_id, "dom", serde_json::json!({}))
            .await
            .unwrap();
        assert!(css_paid["snapshot"].as_str().unwrap().contains("CSS paid"));

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
