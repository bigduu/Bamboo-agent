//! `deploy_agent` — the AI-callable "spin up a worker myself" tool.
//!
//! Lets a running (root) agent deploy a new broker-agent worker on demand — as a
//! local subprocess, in a Docker container, or on a remote host over SSH — wired
//! to the configured broker. The agent then commands it with `ask_agent` by the
//! returned id. Deployed handles are kept alive in a registry (they are
//! kill-on-drop) and torn down via `action=stop` (or when the server exits).
//!
//! Only registered on the Root surface when a broker is configured.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_broker::{
    AgentDeployment, DeployedAgent, Deployer, DockerDeployer, LocalProcessDeployer, SshDeployer,
};

/// Keeps deployed workers alive (the handles are kill-on-drop) and lets `stop`
/// tear them down. Shared for the server's lifetime.
pub type DeployedRegistry = Arc<Mutex<HashMap<String, Deployed>>>;

/// One live deployment: how it was deployed + the kill-on-drop handle.
pub struct Deployed {
    pub env: String,
    pub handle: DeployedAgent,
}

pub struct DeployAgentTool {
    broker_endpoint: String,
    broker_token: String,
    /// Path to the `bamboo` binary used for local subprocess deploys.
    bamboo_bin: PathBuf,
    registry: DeployedRegistry,
}

impl DeployAgentTool {
    pub fn new(
        broker_endpoint: impl Into<String>,
        broker_token: impl Into<String>,
        bamboo_bin: impl Into<PathBuf>,
        registry: DeployedRegistry,
    ) -> Self {
        Self {
            broker_endpoint: broker_endpoint.into(),
            broker_token: broker_token.into(),
            bamboo_bin: bamboo_bin.into(),
            registry,
        }
    }
}

/// Parameters for `action=deploy`, grouped so the deploy call stays tidy.
#[derive(Debug, Deserialize)]
struct DeployParams {
    /// Worker id (its broker mailbox key). Auto-generated when omitted.
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    role: Option<String>,
    /// `provider:model` for the worker's agent (ignored when `echo`).
    #[serde(default)]
    model: Option<String>,
    /// Where to run it: `local` (default), `docker`, or `ssh`.
    #[serde(default)]
    env: Option<String>,
    /// Docker image (required when `env=docker`).
    #[serde(default)]
    image: Option<String>,
    /// Remote host (required when `env=ssh`).
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
    /// Run the dependency-free echo executor (no LLM) — smoke/testing.
    #[serde(default)]
    echo: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum DeployArgs {
    /// Deploy a new worker and return its id.
    Deploy(DeployParams),
    /// Stop a previously-deployed worker and remove it.
    Stop { id: String },
    /// List currently-deployed workers.
    List,
}

impl DeployAgentTool {
    async fn deploy(&self, params: DeployParams) -> Result<ToolResult, ToolError> {
        let DeployParams {
            id,
            role,
            model,
            env,
            image,
            host,
            workspace,
            echo,
        } = params;
        let id = id.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| {
            format!("agent-{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
        });
        let env = env.unwrap_or_else(|| "local".to_string());

        let deployer: Box<dyn Deployer> = match env.as_str() {
            "local" => Box::new(LocalProcessDeployer::new(self.bamboo_bin.clone())),
            "docker" => {
                let image = image.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                    ToolError::InvalidArguments("env=docker requires `image`".to_string())
                })?;
                // No `--network host`: the worker stays on an isolated bridge
                // network and reaches the host broker via host.docker.internal
                // (DockerDeployer adds the host-gateway alias + the endpoint is
                // rewritten below). Seed the worker from the orchestrator's
                // bamboo home (mounted read-only, copied into the container's
                // writable data dir) so it reads the same config (MCP servers +
                // skills + provider creds).
                Box::new(
                    DockerDeployer::new(image)
                        .mount_home(bamboo_config::paths::resolve_bamboo_dir()),
                )
            }
            "ssh" => {
                let host = host.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                    ToolError::InvalidArguments("env=ssh requires `host`".to_string())
                })?;
                Box::new(SshDeployer::new(host))
            }
            other => {
                return Err(ToolError::InvalidArguments(format!(
                    "unknown env '{other}' (use local|docker|ssh)"
                )))
            }
        };

        // A container cannot reach the host's loopback; for docker, address the
        // broker via host.docker.internal (the deployer maps it to the host
        // gateway). local/ssh keep the configured endpoint as-is.
        let broker_endpoint = if env == "docker" {
            self.broker_endpoint
                .replace("127.0.0.1", "host.docker.internal")
                .replace("localhost", "host.docker.internal")
        } else {
            self.broker_endpoint.clone()
        };

        let deployment = AgentDeployment {
            id: id.clone(),
            role,
            broker_endpoint,
            token: self.broker_token.clone(),
            model,
            workspace,
            echo,
            // Deployed workers proxy MCP to the orchestrator (single MCP host).
            mcp_proxy: Some(bamboo_broker::ORCHESTRATOR_ID.to_string()),
            log_path: None,
        };
        let handle = deployer
            .deploy(&deployment)
            .await
            .map_err(|e| ToolError::Execution(format!("deploy '{id}' ({env}) failed: {e}")))?;

        // Namespace the registry key so an agent-chosen id can never collide
        // with a cluster-fabric node id in the SHARED registry (cross-eviction).
        self.registry.lock().await.insert(
            crate::registry_keys::agent_key(&id),
            Deployed {
                env: env.clone(),
                handle,
            },
        );

        Ok(tool_json(json!({
            "id": id,
            "env": env,
            "status": "deployed",
            "note": format!("worker '{id}' is connecting to the broker; ask it with ask_agent(target=\"{id}\", ...)"),
        })))
    }

    async fn stop(&self, id: String) -> Result<ToolResult, ToolError> {
        match self
            .registry
            .lock()
            .await
            .remove(&crate::registry_keys::agent_key(&id))
        {
            Some(d) => {
                d.handle.shutdown().await;
                Ok(tool_json(json!({ "id": id, "status": "stopped" })))
            }
            None => Ok(tool_json(json!({ "id": id, "status": "not_found" }))),
        }
    }

    async fn list(&self) -> Result<ToolResult, ToolError> {
        let reg = self.registry.lock().await;
        // The registry is shared with the cluster fabric, so show every worker
        // with its source (agent-deployed vs cluster node) and the bare id.
        let agents: Vec<_> = reg
            .iter()
            .map(|(key, d)| {
                let (source, id) = crate::registry_keys::split(key);
                json!({ "id": id, "source": source, "env": d.env })
            })
            .collect();
        Ok(tool_json(json!({ "agents": agents })))
    }
}

fn tool_json(value: serde_json::Value) -> ToolResult {
    ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: None,
        images: Vec::new(),
    }
}

#[async_trait]
impl Tool for DeployAgentTool {
    fn name(&self) -> &str {
        "deploy_agent"
    }

    fn description(&self) -> &str {
        "Spin up a NEW worker agent on demand, anywhere, and manage its lifecycle. This is how you \
         scale yourself out: you deploy a fresh broker-agent, then drive it with ask_agent. The \
         worker connects back to the same message broker you are on, and inherits your MCP servers \
         + skills (via the orchestrator MCP proxy), so it can do real work — not just echo.\n\
         \n\
         THREE PLACEMENTS (action=deploy, pick with `env`):\n\
         - env=local (default) — a subprocess on THIS machine. Fastest; use for extra parallel \
         hands here.\n\
         - env=docker — a container (requires `image`, e.g. \"bamboo:latest\"). Isolated; your \
         bamboo home is mounted so it shares your config. Use for sandboxed or clean-env work.\n\
         - env=ssh — a process on a REMOTE host (requires `host`, e.g. \"user@box\"). Use to run \
         work near other machines/data or to borrow remote compute.\n\
         \n\
         OTHER ACTIONS: action=stop (id=…) tears a worker down and frees it; action=list shows the \
         workers you currently have running. Workers are kept alive until you stop them or the \
         server exits.\n\
         \n\
         WORKED EXAMPLE (scale out, use, tear down):\n\
         1. deploy_agent(action=deploy, env=local, role=\"tester\", model=\"anthropic:claude-opus-4-8\") \
         → returns id \"agent-7f8e9d\".\n\
         2. ask_agent(target=\"agent-7f8e9d\", question=\"Run the full test suite and report \
         failures.\", mode=steer).\n\
         3. deploy_agent(action=list) → confirm it (and any siblings) are running.\n\
         4. deploy_agent(action=stop, id=\"agent-7f8e9d\") → once its work is collected.\n\
         \n\
         Tip: use echo=true to deploy a dependency-free no-LLM worker for a connectivity smoke test \
         before committing to a real model. Returned id is what you pass as ask_agent's `target`."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["deploy", "stop", "list"] },
                "id": { "type": "string", "description": "deploy: worker id (auto if omitted). stop: id to stop." },
                "role": { "type": "string", "description": "deploy: role/profile label." },
                "model": { "type": "string", "description": "deploy: provider:model for the worker." },
                "env": { "type": "string", "enum": ["local", "docker", "ssh"], "description": "deploy: where to run (default local)." },
                "image": { "type": "string", "description": "deploy: docker image (env=docker)." },
                "host": { "type": "string", "description": "deploy: remote host (env=ssh)." },
                "workspace": { "type": "string", "description": "deploy: worker working directory." },
                "echo": { "type": "boolean", "description": "deploy: run the no-LLM echo executor (smoke)." }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: DeployArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid deploy_agent args: {e}")))?;
        match parsed {
            DeployArgs::Deploy(params) => self.deploy(params).await,
            DeployArgs::Stop { id } => self.stop(id).await,
            DeployArgs::List => self.list().await,
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn empty_registry() -> DeployedRegistry {
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()))
    }

    fn tool_with(registry: DeployedRegistry) -> DeployAgentTool {
        // bamboo_bin is never spawned in these tests (we don't drive deploy()).
        DeployAgentTool::new("ws://localhost:0", "test-token", "/bin/true", registry)
    }

    /// A trivial long-running child so the kill/wait path is genuinely exercised.
    fn spawn_sleeper(id: &str, cleanup: Option<Vec<String>>) -> DeployedAgent {
        let child = tokio::process::Command::new("sleep")
            .arg("60")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn sleep");
        DeployedAgent::from_parts(id, child, cleanup)
    }

    /// True while `pid` is a live process (POSIX `kill -0`).
    fn pid_alive(pid: u32) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            // `kill -0` on a reaped pid prints "No such process" to stderr; that
            // stderr is the expected signal, not test noise — silence it.
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn parse(result: ToolResult) -> serde_json::Value {
        serde_json::from_str(&result.result).expect("tool result is JSON")
    }

    #[tokio::test]
    async fn deploy_list_stop_lifecycle_kills_process() {
        let registry = empty_registry();
        let tool = tool_with(registry.clone());

        // (1) register a worker (the registry effect of a successful deploy); list shows it.
        // Use the namespaced key so the tool's stop()/list() find it.
        let agent = spawn_sleeper("w1", None);
        let pid = agent.pid().expect("child has a pid");
        registry.lock().await.insert(
            crate::registry_keys::agent_key("w1"),
            Deployed {
                env: "local".into(),
                handle: agent,
            },
        );
        assert!(
            pid_alive(pid),
            "registered worker process should be running"
        );

        let listed = parse(tool.list().await.unwrap());
        let agents = listed["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0]["id"], "w1");
        assert_eq!(agents[0]["env"], "local");

        // (2) stop: removes the entry AND kills the process (shutdown awaits the child).
        let stopped = parse(tool.stop("w1".to_string()).await.unwrap());
        assert_eq!(stopped["id"], "w1");
        assert_eq!(stopped["status"], "stopped");
        assert!(!pid_alive(pid), "stopped worker process must be killed");

        // (3) list after stop is empty.
        let listed = parse(tool.list().await.unwrap());
        assert!(listed["agents"].as_array().unwrap().is_empty());

        // (4) double-stop (already removed) is a no-op, not a crash.
        let again = parse(tool.stop("w1".to_string()).await.unwrap());
        assert_eq!(again["status"], "not_found");
    }

    #[tokio::test]
    async fn stop_unknown_id_is_not_found_not_a_crash() {
        let tool = tool_with(empty_registry());
        let r = parse(tool.stop("never-deployed".to_string()).await.unwrap());
        assert_eq!(r["status"], "not_found");
    }

    #[tokio::test]
    async fn deployed_agent_shutdown_kills_and_runs_cleanup() {
        // A unique marker the cleanup command will `touch` — proves cleanup ran.
        let marker = std::env::temp_dir().join(format!(
            "bamboo_deploy_cleanup_{}_{:?}.marker",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&marker);

        let agent = spawn_sleeper(
            "cleanup-worker",
            Some(vec![
                "sh".into(),
                "-c".into(),
                format!("touch {}", marker.display()),
            ]),
        );
        let pid = agent.pid().expect("child has a pid");

        agent.shutdown().await;

        assert!(!pid_alive(pid), "shutdown must kill the process");
        assert!(
            marker.exists(),
            "shutdown must run the cleanup command (docker rm -f path)"
        );
        let _ = std::fs::remove_file(&marker);
    }
}
