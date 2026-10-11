//! Exercise the real server assembly with reachable stale transport settings.
//! The only listener is this test's loopback probe; no provider, Worker or remote
//! service participates. Native Child execution itself is covered in engine.
use super::AppState;
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{Message, Session};
use bamboo_engine::external_agents::runtime::requires_legacy_child_runtime;
use bamboo_llm::{Config, LLMError, LLMProvider, LLMStream};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Default)]
struct NoNetworkProvider {
    calls: AtomicUsize,
}
#[async_trait]
impl LLMProvider for NoNetworkProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(LLMError::Api(
            "native assembly test must not call a model".into(),
        ))
    }
}

#[tokio::test]
async fn native_app_state_never_connects_reachable_stale_broker_or_creates_worker_fabric() {
    let home = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (stop_probe, mut stopped) = oneshot::channel();
    let probe = tokio::spawn(async move {
        let mut connections = 0usize;
        loop {
            tokio::select! {
                // Drain already accepted/queued connections before acknowledging
                // shutdown, so a late startup connect cannot escape the count.
                biased;
                accepted = listener.accept() => {
                    let (socket, _) = accepted.unwrap();
                    connections += 1;
                    drop(socket);
                }
                _ = &mut stopped => return connections,
            }
        }
    });

    // Valid and reachable old transport settings must not implicitly select a
    // process/remote route or cause credential migration in a native bootstrap.
    let stale_broker = serde_json::to_vec_pretty(&serde_json::json!({
        "endpoint": endpoint,
        "token": "unused-local-test-token"
    }))
    .unwrap();
    let broker_file = home.path().join("broker.json");
    std::fs::write(&broker_file, &stale_broker).unwrap();
    let stale_fabric = home.path().join("leftover-worker-fabric");
    let mut config = Config::default();
    config.subagents_mut().fabric_dir = Some(stale_fabric.to_string_lossy().into());
    config.subagents_mut().broker = Some(bamboo_config::BrokerClientConfig {
        endpoint,
        token: "unused-local-test-token".into(),
        token_encrypted: None,
        credential_ref: None,
        configured: false,
    });
    assert!(!requires_legacy_child_runtime(&config));
    let provider = Arc::new(NoNetworkProvider::default());
    let state = tokio::time::timeout(
        Duration::from_secs(30),
        AppState::new_with_provider(home.path().to_path_buf(), config, provider.clone()),
    )
    .await
    .expect("native startup must not wait for any broker")
    .unwrap();

    assert!(state.embedded_broker.is_none());
    assert!(state.health_monitor.is_none());
    assert!(state.config.read().await.subagents().broker.is_none());
    assert!(state.config.read().await.cluster_fabric.nodes.is_empty());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());
    assert!(!home.path().join("broker").exists());
    assert!(!home.path().join("subagents").exists());
    assert!(!stale_fabric.exists());
    assert_eq!(std::fs::read(&broker_file).unwrap(), stale_broker);

    // Exercise the installed route, rather than merely testing the selection
    // predicate. A fully wired native backend can admit required context without
    // discovering a Worker, creating an Actor, or executing an LLM request.
    let parent = Session::new("assembly-root", "assembly-model");
    let child = Session::new_child_of(
        "assembly-child",
        &parent,
        "assembly-model",
        "native assembly probe",
    );
    assert!(state.external_child_runner.should_handle(&child).await);
    state
        .external_child_runner
        .validate_required_child_context_route(&child)
        .await
        .unwrap();
    assert!(state
        .tools_for(crate::tools::ToolSurface::Root)
        .owns_exact_tool("SubAgent"));

    tokio::time::timeout(Duration::from_secs(15), state.shutdown())
        .await
        .expect("empty native runtime must shut down without transport waits");
    assert!(state.mcp_proxy_shutdown.is_cancelled());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());
    drop(state);
    stop_probe.send(()).unwrap();
    let connections = probe.await.unwrap();
    assert_eq!(connections, 0, "native assembly contacted the stale broker");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&broker_file).unwrap(), stale_broker);
    assert!(!home.path().join("broker").exists());
    assert!(!home.path().join("subagents").exists());
    assert!(!stale_fabric.exists());
}
