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

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum DeployArgs {
    /// Deploy a new worker and return its id.
    Deploy {
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
    },
    /// Stop a previously-deployed worker and remove it.
    Stop { id: String },
    /// List currently-deployed workers.
    List,
}

impl DeployAgentTool {
    async fn deploy(
        &self,
        id: Option<String>,
        role: Option<String>,
        model: Option<String>,
        env: Option<String>,
        image: Option<String>,
        host: Option<String>,
        workspace: Option<String>,
        echo: bool,
    ) -> Result<ToolResult, ToolError> {
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
                // Mount the orchestrator's bamboo home read-only so the
                // containerized worker reads the same config — syncing its MCP
                // servers + skills (its build_spec reads that mounted config).
                Box::new(
                    DockerDeployer::new(image)
                        .network("host")
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

        let deployment = AgentDeployment {
            id: id.clone(),
            role,
            broker_endpoint: self.broker_endpoint.clone(),
            token: self.broker_token.clone(),
            model,
            workspace,
            echo,
        };
        let handle = deployer
            .deploy(&deployment)
            .await
            .map_err(|e| ToolError::Execution(format!("deploy '{id}' ({env}) failed: {e}")))?;

        self.registry.lock().await.insert(
            id.clone(),
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
        match self.registry.lock().await.remove(&id) {
            Some(d) => {
                d.handle.shutdown().await;
                Ok(tool_json(json!({ "id": id, "status": "stopped" })))
            }
            None => Ok(tool_json(json!({ "id": id, "status": "not_found" }))),
        }
    }

    async fn list(&self) -> Result<ToolResult, ToolError> {
        let reg = self.registry.lock().await;
        let agents: Vec<_> = reg
            .iter()
            .map(|(id, d)| json!({ "id": id, "env": d.env }))
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
        "Deploy a new worker agent on demand and manage it. action=deploy spins up a broker-agent \
         (env=local subprocess, docker container, or ssh remote host) wired to the message broker \
         and returns its id — then command it with ask_agent(target=<id>). action=stop tears one \
         down; action=list shows running workers. Use this to scale out work to fresh agents, \
         locally or on other machines."
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
            DeployArgs::Deploy {
                id,
                role,
                model,
                env,
                image,
                host,
                workspace,
                echo,
            } => {
                self.deploy(id, role, model, env, image, host, workspace, echo)
                    .await
            }
            DeployArgs::Stop { id } => self.stop(id).await,
            DeployArgs::List => self.list().await,
        }
    }
}
