//! Deployers: bring up a `bamboo broker-agent serve` somewhere — as a local
//! subprocess, in a Docker container, or on a remote host over SSH — pointed at
//! a central broker. This is the orchestrator-side "push" model (the master
//! deploys execution environments; workers dial home to the broker), as opposed
//! to mutual discovery.
//!
//! All three produce the same `bamboo broker-agent serve …` invocation and pass
//! the Bearer token via the `BAMBOO_BROKER_TOKEN` environment variable (never on
//! argv, which is visible in `ps`). The returned [`DeployedAgent`] kills the
//! launched process on drop / `shutdown`.

use async_trait::async_trait;
use std::path::PathBuf;
use tokio::process::Command;

use crate::error::{BrokerError, BrokerResult};

/// What to deploy: one broker-agent's identity + how it reaches the broker.
#[derive(Debug, Clone)]
pub struct AgentDeployment {
    /// Mailbox key / session id the orchestrator will address.
    pub id: String,
    pub role: Option<String>,
    /// Broker endpoint AS THE AGENT WILL REACH IT (e.g. inside Docker this may be
    /// `ws://host.docker.internal:9600`, not `127.0.0.1`).
    pub broker_endpoint: String,
    pub token: String,
    /// `provider:model` for the real executor; ignored when `echo`.
    pub model: Option<String>,
    pub workspace: Option<String>,
    /// Run the dependency-free echo executor (no LLM).
    pub echo: bool,
}

/// Brings up a broker-agent in some environment and returns a handle to it.
#[async_trait]
pub trait Deployer: Send + Sync {
    async fn deploy(&self, agent: &AgentDeployment) -> BrokerResult<DeployedAgent>;
}

/// A running deployment. Killed on drop (`kill_on_drop`); `shutdown` also runs
/// any cleanup (e.g. `docker rm -f`).
pub struct DeployedAgent {
    pub id: String,
    child: tokio::process::Child,
    cleanup: Option<Vec<String>>,
}

impl DeployedAgent {
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Stop the deployment: kill the launched process, then run cleanup if any.
    pub async fn shutdown(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        if let Some(args) = self.cleanup {
            if let Some((bin, rest)) = args.split_first() {
                let _ = Command::new(bin).args(rest).status().await;
            }
        }
    }
}

/// The `broker-agent serve …` argv (token is NOT here — it rides the env).
fn agent_argv(d: &AgentDeployment) -> Vec<String> {
    let mut a = vec![
        "broker-agent".to_string(),
        "serve".to_string(),
        "--broker".to_string(),
        d.broker_endpoint.clone(),
        "--id".to_string(),
        d.id.clone(),
    ];
    if let Some(r) = &d.role {
        a.push("--role".into());
        a.push(r.clone());
    }
    if let Some(m) = &d.model {
        a.push("--model".into());
        a.push(m.clone());
    }
    if let Some(w) = &d.workspace {
        a.push("--workspace".into());
        a.push(w.clone());
    }
    if d.echo {
        a.push("--echo".into());
    }
    a
}

fn spawn_err(e: std::io::Error) -> BrokerError {
    BrokerError::Transport(format!("spawn: {e}"))
}

/// Deploy as a local OS subprocess of the given `bamboo` binary.
pub struct LocalProcessDeployer {
    pub bamboo_bin: PathBuf,
}

impl LocalProcessDeployer {
    pub fn new(bamboo_bin: impl Into<PathBuf>) -> Self {
        Self {
            bamboo_bin: bamboo_bin.into(),
        }
    }
}

#[async_trait]
impl Deployer for LocalProcessDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        let mut cmd = Command::new(&self.bamboo_bin);
        cmd.args(agent_argv(d))
            .env("BAMBOO_BROKER_TOKEN", &d.token)
            .kill_on_drop(true);
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent {
            id: d.id.clone(),
            child,
            cleanup: None,
        })
    }
}

/// Deploy in a Docker container (`docker run --rm …`). The image must contain a
/// `bamboo` binary (path given by `bamboo_in_image`, default `bamboo`).
pub struct DockerDeployer {
    pub image: String,
    pub docker_bin: String,
    pub bamboo_in_image: String,
    /// e.g. `Some("host")` so the container can reach a `127.0.0.1` broker.
    pub network: Option<String>,
    /// Host bamboo home dir to mount read-only at `/root/.bamboo`, so the
    /// containerized worker reads the orchestrator's config — and thereby syncs
    /// its MCP servers + skills. (Trusted-local convenience; it also exposes the
    /// config's secrets to the container — P3 will scope this down.)
    pub mount_home: Option<PathBuf>,
}

impl DockerDeployer {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            docker_bin: "docker".into(),
            bamboo_in_image: "bamboo".into(),
            network: None,
            mount_home: None,
        }
    }
    pub fn network(mut self, net: impl Into<String>) -> Self {
        self.network = Some(net.into());
        self
    }
    pub fn mount_home(mut self, host_bamboo_dir: impl Into<PathBuf>) -> Self {
        self.mount_home = Some(host_bamboo_dir.into());
        self
    }

    fn argv(&self, d: &AgentDeployment, container: &str) -> Vec<String> {
        let mut a = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            container.to_string(),
            "-e".to_string(),
            format!("BAMBOO_BROKER_TOKEN={}", d.token),
        ];
        if let Some(net) = &self.network {
            a.push("--network".into());
            a.push(net.clone());
        }
        if let Some(home) = &self.mount_home {
            a.push("-v".into());
            a.push(format!("{}:/root/.bamboo:ro", home.display()));
        }
        a.push(self.image.clone());
        a.push(self.bamboo_in_image.clone());
        a.extend(agent_argv(d));
        a
    }
}

#[async_trait]
impl Deployer for DockerDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        let container = format!("bamboo-agent-{}", d.id);
        let mut cmd = Command::new(&self.docker_bin);
        cmd.args(self.argv(d, &container)).kill_on_drop(true);
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent {
            id: d.id.clone(),
            child,
            cleanup: Some(vec![
                self.docker_bin.clone(),
                "rm".into(),
                "-f".into(),
                container,
            ]),
        })
    }
}

/// Deploy on a remote host over SSH. `bamboo_on_remote` is the binary path on
/// that host. The token rides as an env prefix in the remote command.
pub struct SshDeployer {
    pub host: String,
    pub ssh_bin: String,
    pub bamboo_on_remote: String,
}

impl SshDeployer {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            ssh_bin: "ssh".into(),
            bamboo_on_remote: "bamboo".into(),
        }
    }

    /// `ssh` argv: the host, then a single remote command string (env prefix +
    /// bamboo + args, each shell-quoted). `-tt` so a local kill propagates.
    fn argv(&self, d: &AgentDeployment) -> Vec<String> {
        let mut remote = format!("BAMBOO_BROKER_TOKEN={}", sh_quote(&d.token));
        remote.push(' ');
        remote.push_str(&sh_quote(&self.bamboo_on_remote));
        for arg in agent_argv(d) {
            remote.push(' ');
            remote.push_str(&sh_quote(&arg));
        }
        vec!["-tt".to_string(), self.host.clone(), remote]
    }
}

#[async_trait]
impl Deployer for SshDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        let mut cmd = Command::new(&self.ssh_bin);
        cmd.args(self.argv(d)).kill_on_drop(true);
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent {
            id: d.id.clone(),
            child,
            cleanup: None,
        })
    }
}

/// Minimal POSIX single-quote escaping for an SSH remote command argument.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dep() -> AgentDeployment {
        AgentDeployment {
            id: "w1".into(),
            role: Some("researcher".into()),
            broker_endpoint: "ws://broker:9600".into(),
            token: "tok".into(),
            model: None,
            workspace: None,
            echo: true,
        }
    }

    #[test]
    fn agent_argv_includes_flags_but_not_token() {
        let a = agent_argv(&dep());
        assert_eq!(&a[0..2], &["broker-agent", "serve"]);
        assert!(a.contains(&"--broker".to_string()));
        assert!(a.contains(&"ws://broker:9600".to_string()));
        assert!(a.contains(&"--id".to_string()) && a.contains(&"w1".to_string()));
        assert!(a.contains(&"--role".to_string()) && a.contains(&"researcher".to_string()));
        assert!(a.contains(&"--echo".to_string()));
        // token must never appear on argv.
        assert!(!a.iter().any(|x| x.contains("tok")));
    }

    #[test]
    fn docker_argv_wraps_with_run_rm_name_env_and_network() {
        let d = DockerDeployer::new("bamboo:latest").network("host");
        let a = d.argv(&dep(), "bamboo-agent-w1");
        assert_eq!(&a[0..4], &["run", "--rm", "--name", "bamboo-agent-w1"]);
        assert!(a.contains(&"-e".to_string()));
        assert!(a.contains(&"BAMBOO_BROKER_TOKEN=tok".to_string()));
        assert!(a.contains(&"--network".to_string()) && a.contains(&"host".to_string()));
        assert!(a.contains(&"bamboo:latest".to_string()));
        // the in-image bamboo + the broker-agent args follow the image.
        let img = a.iter().position(|x| x == "bamboo:latest").unwrap();
        assert_eq!(a[img + 1], "bamboo");
        assert_eq!(a[img + 2], "broker-agent");
    }

    #[test]
    fn ssh_argv_quotes_remote_command_with_env_prefix() {
        let s = SshDeployer::new("gpu-host");
        let a = s.argv(&dep());
        assert_eq!(a[0], "-tt");
        assert_eq!(a[1], "gpu-host");
        let remote = &a[2];
        assert!(remote.starts_with("BAMBOO_BROKER_TOKEN='tok'"));
        assert!(remote.contains("broker-agent"));
        assert!(remote.contains("ws://broker:9600"));
    }

    #[test]
    fn sh_quote_escapes_single_quotes() {
        assert_eq!(sh_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn docker_argv_mounts_home_when_set() {
        let d = DockerDeployer::new("img").mount_home("/home/u/.bamboo");
        let a = d.argv(&dep(), "c");
        assert!(a.contains(&"-v".to_string()));
        assert!(a.iter().any(|x| x == "/home/u/.bamboo:/root/.bamboo:ro"));
    }
}
