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
    /// Proxy MCP to this orchestrator id over the broker (host-bound servers).
    pub mcp_proxy: Option<String>,
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
    /// Build from an already-spawned child process and an optional cleanup
    /// command. Used by the deployers below; also lets integration tests
    /// exercise the registry/shutdown lifecycle with a trivial child instead of
    /// a real docker/ssh deployment.
    pub fn from_parts(
        id: impl Into<String>,
        child: tokio::process::Child,
        cleanup: Option<Vec<String>>,
    ) -> Self {
        Self {
            id: id.into(),
            child,
            cleanup,
        }
    }

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
    if let Some(orchestrator) = &d.mcp_proxy {
        a.push("--mcp-proxy".into());
        a.push(orchestrator.clone());
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
        Ok(DeployedAgent::from_parts(d.id.clone(), child, None))
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
    /// Host bamboo home dir to seed the worker from: mounted read-only at
    /// `/seed`, then config + encryption key + skills are copied into the
    /// container's writable data dir at startup, so the worker reads the
    /// orchestrator's config (MCP servers + skills + provider creds) while
    /// keeping an isolated, writable data dir. (Trusted-local convenience; it
    /// also exposes the config's secrets to the container — P3 will scope this
    /// down.)
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
            // Reach a broker on the host via `host.docker.internal` without
            // `--network host`: Docker Desktop / orbstack provide this name
            // automatically, and the `--add-host` (host-gateway = the host's
            // bridge IP) makes it resolve on native Linux Docker too. The
            // worker's broker endpoint should therefore use host.docker.internal,
            // not the host's loopback, while staying on an isolated bridge net.
            "--add-host".to_string(),
            "host.docker.internal:host-gateway".to_string(),
        ];
        if let Some(net) = &self.network {
            a.push("--network".into());
            a.push(net.clone());
        }
        if let Some(home) = &self.mount_home {
            // Seed the worker from the orchestrator's home, but DON'T run on a
            // read-only mount of it: the worker writes the moment it starts
            // (skill-store builtin sync, session/event persistence), so a
            // `:ro` data dir fails with EROFS. Instead mount the home read-only
            // at /seed and copy just the credentials + skills into the image's
            // writable BAMBOO_DATA_DIR at startup. The worker gets an isolated,
            // fully writable data dir and the orchestrator's home stays pristine
            // (no shared session store, no concurrent-write corruption).
            a.push("-v".into());
            a.push(format!("{}:/seed:ro", home.display()));
            // Override the entrypoint to a shell that seeds /data then execs the
            // in-image bamboo. (Default ENTRYPOINT is `bamboo`; we need the copy
            // step first.) BAMBOO_DATA_DIR comes from the image ENV (/data).
            a.push("--entrypoint".into());
            a.push("/bin/sh".into());
            a.push(self.image.clone());
            let mut script = String::from(
                "BAMBOO_DATA_DIR=\"${BAMBOO_DATA_DIR:-/data}\"; export BAMBOO_DATA_DIR; \
                 mkdir -p \"$BAMBOO_DATA_DIR\"; \
                 for f in config.json .bamboo_encryption_key; do \
                   [ -e \"/seed/$f\" ] && cp -f \"/seed/$f\" \"$BAMBOO_DATA_DIR/\"; \
                 done; \
                 [ -d /seed/skills ] && cp -rf /seed/skills \"$BAMBOO_DATA_DIR/\"; \
                 exec ",
            );
            script.push_str(&sh_quote(&self.bamboo_in_image));
            for arg in agent_argv(d) {
                script.push(' ');
                script.push_str(&sh_quote(&arg));
            }
            a.push("-c".into());
            a.push(script);
        } else {
            // No seed: run the in-image bamboo directly. Override the entrypoint
            // to the bamboo binary, then pass the broker-agent args. The image's
            // default ENTRYPOINT is already `bamboo`, so pushing `bamboo` as the
            // first command arg would double up (`bamboo bamboo broker-agent
            // serve` → unrecognized subcommand).
            a.push("--entrypoint".to_string());
            a.push(self.bamboo_in_image.clone());
            a.push(self.image.clone());
            a.extend(agent_argv(d));
        }
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
        Ok(DeployedAgent::from_parts(
            d.id.clone(),
            child,
            Some(vec![
                self.docker_bin.clone(),
                "rm".into(),
                "-f".into(),
                container,
            ]),
        ))
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
        // Reverse-tunnel the broker port to the remote's loopback (`-R`), so the
        // worker reaches the host broker via 127.0.0.1 over THIS ssh connection —
        // no host-reachable IP and no inbound access to the remote needed (the
        // broker can stay bound to 127.0.0.1). The worker is then pointed at the
        // tunnel mouth on the remote loopback.
        // Same-host ssh (localhost): the worker shares the host's loopback and
        // reaches the broker directly — skip the reverse tunnel, which would only
        // collide with the broker on the same port. Remote hosts get the -R tunnel.
        let host_only = self.host.rsplit('@').next().unwrap_or(self.host.as_str());
        let same_host = matches!(host_only, "localhost" | "127.0.0.1" | "::1");
        let port = if same_host {
            None
        } else {
            broker_port(&d.broker_endpoint)
        };
        let mut a = vec![
            "-tt".to_string(),
            // Trust-on-first-use: accept a NEW host key on first connect but
            // REJECT a known host whose key has changed — closes the silent
            // key-change MITM hole without breaking first deploys. The broker
            // token + ProvisionSpec provider creds ride this connection, so host
            // verification must not silently fall back to the user's SSH config
            // (often `accept-new` or `no`).
            "-o".to_string(),
            "StrictHostKeyChecking=accept-new".to_string(),
        ];
        if let Some(p) = port {
            a.push("-R".to_string());
            a.push(format!("{p}:127.0.0.1:{p}"));
        }
        a.push(self.host.clone());

        let mut tunneled = d.clone();
        if let Some(p) = port {
            tunneled.broker_endpoint = format!("ws://127.0.0.1:{p}");
        }
        let mut remote = format!("BAMBOO_BROKER_TOKEN={}", sh_quote(&d.token));
        remote.push(' ');
        remote.push_str(&sh_quote(&self.bamboo_on_remote));
        for arg in agent_argv(&tunneled) {
            remote.push(' ');
            remote.push_str(&sh_quote(&arg));
        }
        a.push(remote);
        a
    }
}

#[async_trait]
impl Deployer for SshDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        let mut cmd = Command::new(&self.ssh_bin);
        cmd.args(self.argv(d)).kill_on_drop(true);
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent::from_parts(d.id.clone(), child, None))
    }
}

/// Minimal POSIX single-quote escaping for an SSH remote command argument.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Parse the port out of a `ws://host:port[/path]` broker endpoint, for the
/// reverse tunnel (`ssh -R port:127.0.0.1:port`).
fn broker_port(endpoint: &str) -> Option<u16> {
    let after_host = endpoint.rsplit_once(':')?.1;
    after_host.split(['/', '?']).next()?.parse().ok()
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
            mcp_proxy: None,
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
        // --entrypoint bamboo precedes the image; the command after the image is
        // the broker-agent invocation directly (no doubled `bamboo`).
        assert!(a
            .windows(2)
            .any(|w| w == ["--entrypoint".to_string(), "bamboo".to_string()]));
        let img = a.iter().position(|x| x == "bamboo:latest").unwrap();
        assert_eq!(a[img + 1], "broker-agent");
    }

    #[test]
    fn ssh_argv_reverse_tunnels_broker_and_quotes_remote() {
        let s = SshDeployer::new("gpu-host");
        let a = s.argv(&dep()); // dep() broker_endpoint = ws://broker:9600
        assert_eq!(a[0], "-tt");
        // Host-key checking: trust-on-first-use (accept new host keys, reject a
        // changed key on a known host) — must always be in the constructed argv.
        assert!(a.windows(2).any(|w| w
            == [
                "-o".to_string(),
                "StrictHostKeyChecking=accept-new".to_string()
            ]));
        // reverse tunnel: remote loopback :9600 -> host-side broker :9600
        assert!(a
            .windows(2)
            .any(|w| w == ["-R".to_string(), "9600:127.0.0.1:9600".to_string()]));
        assert!(a.contains(&"gpu-host".to_string()));
        // the single remote-command argument is always last.
        let remote = a.last().unwrap();
        assert!(remote.starts_with("BAMBOO_BROKER_TOKEN='tok'"));
        assert!(remote.contains("broker-agent"));
        // the worker connects to the tunnel mouth on the remote loopback,
        // not the host-side endpoint.
        assert!(remote.contains("ws://127.0.0.1:9600"));
        assert!(!remote.contains("ws://broker:9600"));
    }

    #[test]
    fn ssh_argv_skips_reverse_tunnel_for_same_host() {
        // Same-host (localhost) deploy: no -R (it would collide with the broker
        // on the same port); the worker uses the broker endpoint directly.
        let s = SshDeployer::new("localhost");
        let a = s.argv(&dep());
        assert_eq!(a[0], "-tt");
        // Host-key checking still enforced on same-host deploys.
        assert!(a.windows(2).any(|w| w
            == [
                "-o".to_string(),
                "StrictHostKeyChecking=accept-new".to_string()
            ]));
        assert!(a.contains(&"localhost".to_string()));
        assert!(!a.iter().any(|x| x == "-R"));
        let remote = a.last().unwrap();
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
        // Home is mounted read-only at /seed (not the data dir itself).
        assert!(a.contains(&"-v".to_string()));
        assert!(a.iter().any(|x| x == "/home/u/.bamboo:/seed:ro"));
        // Entrypoint is a shell that seeds /data then execs bamboo broker-agent.
        assert!(a
            .windows(2)
            .any(|w| w == ["--entrypoint".to_string(), "/bin/sh".to_string()]));
        let script = a.last().unwrap();
        assert!(script.contains("/seed/config.json") || script.contains("config.json"));
        assert!(script.contains("cp -rf /seed/skills"));
        assert!(script.contains("exec 'bamboo' 'broker-agent' 'serve'"));
        // The broker token must never appear on argv (it rides as -e env).
        assert!(!a
            .iter()
            .any(|x| x.contains("tok") && !x.starts_with("BAMBOO_BROKER_TOKEN=")));
    }
}
