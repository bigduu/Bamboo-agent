//! Process-ephemeral, per-run credentials for Codex-as-a-Bamboo-client.
//!
//! Plaintext tokens are returned once to the actor runner and are never stored.
//! The registry keeps only SHA-256 identifiers plus the owning child session.

use std::time::{Duration, Instant};

use dashmap::DashMap;
use rand::RngExt;
use sha2::{Digest, Sha256};

use bamboo_engine::external_agents::actor_adapter::{CodexRunTokenAuthority, IssuedCodexRunToken};

pub(crate) const CODEX_RUN_TOKEN_PREFIX: &str = "bcx1_";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexRunAuthContext {
    pub session_id: String,
}

#[derive(Debug, Clone)]
struct TokenEntry {
    session_id: String,
    expires_at: Instant,
}

/// Shared by AppState, the HTTP auth middleware, and the actor runner.
pub(crate) struct CodexRunTokenRegistry {
    entries: DashMap<String, TokenEntry>,
    ttl: Duration,
}

impl Default for CodexRunTokenRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_TOKEN_TTL)
    }
}

impl CodexRunTokenRegistry {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
        }
    }

    fn token_id(token: &str) -> String {
        hex::encode(Sha256::digest(token.as_bytes()))
    }

    pub(crate) fn verify(&self, token: &str) -> Option<CodexRunAuthContext> {
        if !token.starts_with(CODEX_RUN_TOKEN_PREFIX) {
            return None;
        }
        let token_id = Self::token_id(token);
        let entry = self.entries.get(&token_id)?;
        if Instant::now() >= entry.expires_at {
            drop(entry);
            self.entries.remove(&token_id);
            return None;
        }
        Some(CodexRunAuthContext {
            session_id: entry.session_id.clone(),
        })
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.entries.len()
    }
}

impl CodexRunTokenAuthority for CodexRunTokenRegistry {
    fn issue(&self, session_id: &str) -> Result<IssuedCodexRunToken, String> {
        if session_id.trim().is_empty() {
            return Err("session id is empty".to_string());
        }
        let mut random = [0_u8; 32];
        rand::rng().fill(&mut random);
        let token = format!("{CODEX_RUN_TOKEN_PREFIX}{}", hex::encode(random));
        let token_id = Self::token_id(&token);
        self.entries.insert(
            token_id.clone(),
            TokenEntry {
                session_id: session_id.to_string(),
                expires_at: Instant::now() + self.ttl,
            },
        );
        Ok(IssuedCodexRunToken { token_id, token })
    }

    fn revoke(&self, token_id: &str) {
        self.entries.remove(token_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct CodexE2eProvider {
        sessions: std::sync::Mutex<Vec<Option<String>>>,
    }

    #[async_trait::async_trait]
    impl bamboo_llm::LLMProvider for CodexE2eProvider {
        async fn chat_stream(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(bamboo_llm::types::LLMChunk::Token("PONG".to_string())),
                Ok(bamboo_llm::types::LLMChunk::Done),
            ])))
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[bamboo_agent_core::Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&bamboo_llm::LLMRequestOptions>,
        ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
            self.sessions
                .lock()
                .expect("Codex e2e sessions lock")
                .push(options.and_then(|options| options.session_id.clone()));
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }

        async fn list_models(&self) -> bamboo_llm::provider::Result<Vec<String>> {
            Ok(vec!["gpt-5.4".to_string()])
        }
    }

    #[test]
    fn token_is_session_scoped_and_revocation_is_immediate() {
        let registry = CodexRunTokenRegistry::default();
        let issued = registry.issue("child-570").unwrap();
        assert_eq!(registry.active_count(), 1);
        assert_eq!(
            registry.verify(&issued.token),
            Some(CodexRunAuthContext {
                session_id: "child-570".to_string()
            })
        );

        registry.revoke(&issued.token_id);
        assert_eq!(registry.active_count(), 0);
        assert!(registry.verify(&issued.token).is_none());
    }

    #[test]
    fn expired_token_is_rejected_and_purged() {
        let registry = CodexRunTokenRegistry::new(Duration::ZERO);
        let issued = registry.issue("child-expired").unwrap();
        assert!(registry.verify(&issued.token).is_none());
        assert_eq!(registry.active_count(), 0);
    }

    /// Real-machine contract test for issue #570 mode 4. It launches the
    /// installed Codex CLI against a live Bamboo Responses endpoint, verifies
    /// session propagation and persisted forward metrics, then revokes the
    /// exact run credential and proves the live server rejects it. Normal CI
    /// keeps this ignored because Codex is an external binary; the issue/PR
    /// evidence records an explicit successful invocation.
    #[tokio::test]
    #[ignore = "requires an installed Codex CLI >= 0.144"]
    async fn live_bamboo_codex_completes_records_metrics_and_rejects_revoked_token() {
        use std::process::Stdio;

        use actix_web::{dev::Service as _, web, App, HttpServer};
        use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let data_dir = tempfile::tempdir().unwrap();
        let codex_home = tempfile::tempdir().unwrap();
        let provider = Arc::new(CodexE2eProvider {
            sessions: std::sync::Mutex::new(Vec::new()),
        });
        let state = crate::app_state::AppState::new_with_provider(
            data_dir.path().to_path_buf(),
            bamboo_config::Config::default(),
            provider.clone(),
        )
        .await
        .unwrap();
        let registry = state.codex_run_tokens.clone();
        let metrics = state.metrics_service.clone();
        let issued = registry.issue("codex-live-child-570").unwrap();
        let seen_requests = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let app_state = web::Data::new(state);
        let seen_requests_for_server = seen_requests.clone();
        let server = HttpServer::new(move || {
            let seen_requests = seen_requests_for_server.clone();
            App::new()
                .app_data(app_state.clone())
                .configure(crate::routes::openai_prefixed_routes)
                .wrap_fn(move |request, service| {
                    seen_requests
                        .lock()
                        .expect("seen-requests lock")
                        .push(format!("{} {}", request.method(), request.path()));
                    service.call(request)
                })
        })
        .listen(listener)
        .unwrap()
        .run();
        let server_handle = server.handle();
        let server_task = tokio::spawn(server);
        let live_client = reqwest::Client::new();
        let models = live_client
            .get(format!("http://127.0.0.1:{port}/openai/v1/models"))
            .bearer_auth(&issued.token)
            .send()
            .await
            .expect("live Bamboo models endpoint is reachable");
        assert_eq!(models.status(), reqwest::StatusCode::OK);

        let config = format!(
            r#"model = "gpt-5.4"
model_provider = "bamboo"

[model_providers.bamboo]
name = "Bamboo live e2e"
base_url = "http://127.0.0.1:{port}/openai/v1"
env_key = "BAMBOO_CODEX_PROVIDER_KEY"
wire_api = "responses"
"#
        );
        std::fs::write(codex_home.path().join("config.toml"), config).unwrap();

        let mut command = tokio::process::Command::new("codex");
        command
            .args([
                "exec",
                "--json",
                "--color",
                "never",
                "--sandbox",
                "read-only",
                "--ignore-rules",
                "--skip-git-repo-check",
                "--model",
                "gpt-5.4",
                "-",
            ])
            .env_clear();
        for (key, value) in std::env::vars() {
            if matches!(
                key.as_str(),
                "HOME" | "PATH" | "SHELL" | "TERM" | "LANG" | "TMPDIR" | "USER" | "LOGNAME"
            ) || key.starts_with("LC_")
            {
                command.env(key, value);
            }
        }
        command
            .env("CODEX_HOME", codex_home.path())
            .env("BAMBOO_CODEX_PROVIDER_KEY", &issued.token)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().expect("installed codex binary");
        let child_process_group = child.id().expect("Codex process id");
        let mut stdin = child.stdin.take().unwrap();
        let mut child_stdout = child.stdout.take().unwrap();
        let mut child_stderr = child.stderr.take().unwrap();
        let stdout_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            child_stdout.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            child_stderr.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        stdin
            .write_all(b"Reply with exactly PONG and nothing else.\n")
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();
        drop(stdin);
        let wait = tokio::time::timeout(Duration::from_secs(45), child.wait()).await;
        let timed_out = wait.is_err();
        let status = match wait {
            Ok(status) => status.unwrap(),
            Err(_) => {
                #[cfg(unix)]
                let _ = tokio::process::Command::new("kill")
                    .args(["-TERM", &format!("-{child_process_group}")])
                    .status()
                    .await;
                #[cfg(not(unix))]
                let _ = child.kill().await;
                child.wait().await.unwrap()
            }
        };
        let stdout_bytes = stdout_task.await.unwrap();
        let stderr_bytes = stderr_task.await.unwrap();
        let stdout = String::from_utf8_lossy(&stdout_bytes);
        let stderr = String::from_utf8_lossy(&stderr_bytes);
        assert!(
            !timed_out,
            "Codex timed out; sessions={:?} requests={:?} stdout={stdout} stderr={stderr}",
            provider.sessions.lock().unwrap(),
            seen_requests.lock().unwrap()
        );
        assert!(
            status.success(),
            "Codex failed: status={} stdout={stdout} stderr={stderr}",
            status
        );
        assert!(stdout.contains("\"type\":\"turn.completed\""), "{stdout}");
        assert!(stdout.contains("PONG"), "{stdout}");
        assert_eq!(
            provider.sessions.lock().unwrap().as_slice(),
            [Some("codex-live-child-570".to_string())]
        );

        let mut recorded = Vec::new();
        for _ in 0..100 {
            recorded = metrics
                .forward_requests(bamboo_metrics::types::ForwardMetricsFilter {
                    endpoint: Some("openai.responses".to_string()),
                    limit: Some(20),
                    ..Default::default()
                })
                .await
                .unwrap();
            if recorded.iter().any(|request| {
                request.status == Some(bamboo_metrics::types::ForwardStatus::Success)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            recorded.iter().any(|request| {
                request.endpoint == "openai.responses"
                    && request.status == Some(bamboo_metrics::types::ForwardStatus::Success)
            }),
            "parent metrics must record the Codex Responses request: {recorded:?}"
        );
        assert!(
            !format!("{recorded:?}").contains(&issued.token)
                && !stdout.contains(&issued.token)
                && !stderr.contains(&issued.token),
            "the scoped run token must remain masked from metrics and process output"
        );

        registry.revoke(&issued.token_id);
        let revoked = live_client
            .post(format!("http://127.0.0.1:{port}/openai/v1/responses"))
            .bearer_auth(&issued.token)
            .json(&serde_json::json!({
                "model": "gpt-5.4",
                "input": "PONG",
                "stream": false
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(revoked.status(), reqwest::StatusCode::UNAUTHORIZED);

        server_handle.stop(true).await;
        server_task.await.unwrap().unwrap();
    }
}
