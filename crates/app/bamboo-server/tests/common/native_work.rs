//! Shared setup for native Work integration binaries; no tests or runtime protocol.
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::Message;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_server::AppState;
use bamboo_tickets::ExecutionWorkspace;
use serde_json::json;
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub(crate) const MODEL: &str = "native-work-acceptance-local-gate";
pub(crate) const CALL: &str = "native-work-owned-task";
pub(crate) const RESULT: &str =
    "The real native Task completed; this result still requires owner review.";
pub(crate) const WAIT: Duration = Duration::from_secs(20);

pub(crate) struct ModelRequest {
    pub(crate) messages: Vec<Message>,
    pub(crate) tools: Vec<ToolSchema>,
    pub(crate) reply: oneshot::Sender<LLMChunk>,
}

pub(crate) struct LocalProvider {
    pub(crate) requests: mpsc::UnboundedSender<ModelRequest>,
    pub(crate) calls: AtomicUsize,
    pub(crate) max_requests: usize,
}

#[async_trait]
impl LLMProvider for LocalProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        assert!(
            self.calls.fetch_add(1, Ordering::SeqCst) < self.max_requests,
            "bounded local model"
        );
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(ModelRequest {
                messages: messages.to_vec(),
                tools: tools.to_vec(),
                reply,
            })
            .map_err(|_| LLMError::Api("local observer closed".into()))?;
        let chunk = receive
            .await
            .map_err(|_| LLMError::Api("local provider gate closed".into()))?;
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

pub(crate) async fn host(root: &Path, provider: Arc<LocalProvider>) -> AppState {
    let mut config: Config = serde_json::from_value(json!({
        "provider":"openai",
        "features":{"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"synthetic-unused-local-gate","model":MODEL}},
        "run_budget":{"max_rounds":4},
        "subagents":{"max_concurrent":1}
    }))
    .unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = AppState::new_with_provider_and_memory_store(
        root.to_path_buf(),
        config,
        provider.clone(),
        bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
    )
    .await
    .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([("openai".into(), provider as Arc<dyn LLMProvider>)]),
        "openai".into(),
    ));
    state
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            "-C",
        ])
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

// Same isolated Git identity contract as the existing native Ready fixture.
pub(crate) fn execution_workspace_at(
    root: &Path,
    label: &str,
    content: &str,
) -> ExecutionWorkspace {
    let repo = root.join(format!("repo-{label}"));
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "fixture"]);
    std::fs::write(repo.join("fixture.txt"), content).unwrap();
    git(&repo, &["add", "fixture.txt"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Native fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
    );
    let base_commit = git(&repo, &["rev-parse", "HEAD"]);
    let worktree = root.join(format!("worktree-{label}"));
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "native-work",
            worktree.to_str().unwrap(),
            &base_commit,
        ],
    );
    let worktree = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    ExecutionWorkspace {
        repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
        base_commit,
        branch: "native-work".into(),
        worktree: worktree.clone(),
        write_roots: vec![worktree.clone()],
        claims: BTreeSet::from([format!("worktree:{worktree}")]),
    }
}

pub(crate) fn execution_workspace(root: &Path) -> ExecutionWorkspace {
    execution_workspace_at(root, "default", "disposable native acceptance\n")
}

macro_rules! owner_post {
    ($app:expr, $uri:expr, $body:expr, $status:expr) => {{
        let response = actix_web::test::call_service(
            &$app,
            actix_web::test::TestRequest::post()
                .uri($uri)
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json($body)
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: serde_json::Value = actix_web::test::read_body_json(response).await;
        assert_eq!(status, $status, "{}: {body}", $uri);
        body
    }};
}
