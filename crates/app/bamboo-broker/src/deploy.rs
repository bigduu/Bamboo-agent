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
    /// When set, redirect the worker's stdout+stderr to this file (a LOCAL path
    /// for local deploys, a REMOTE path for ssh/russh) so `tail_log` can read it.
    pub log_path: Option<String>,
}

/// Brings up a broker-agent in some environment and returns a handle to it.
#[async_trait]
pub trait Deployer: Send + Sync {
    async fn deploy(&self, agent: &AgentDeployment) -> BrokerResult<DeployedAgent>;

    /// Connectivity preflight WITHOUT deploying: prove the target is reachable
    /// and (for SSH) the credentials authenticate, returning a short status
    /// string (e.g. remote `uname`). Default: trivially Ok (local/docker).
    async fn preflight(&self) -> BrokerResult<String> {
        Ok("ok".to_string())
    }

    /// Read the last `lines` lines of the worker's log at `log_path` (a local
    /// path for local deploys, a remote path for ssh/russh). Default: unsupported.
    async fn tail_log(&self, _log_path: &str, _lines: usize) -> BrokerResult<String> {
        Err(BrokerError::Transport(
            "log tail is not supported for this deployer".to_string(),
        ))
    }
}

/// A handle to a deployment that is NOT a local child process (e.g. a remote
/// worker reached over an in-process `russh` session). Owning this keeps the
/// remote alive; `shutdown` tears it down (kill the remote process + close the
/// connection/tunnel).
#[async_trait]
pub trait RemoteDeployment: Send + Sync {
    /// Remote OS pid of the launched worker, if the deployer captured one.
    fn remote_pid(&self) -> Option<u32> {
        None
    }
    /// Tear down the remote worker and release the connection/tunnel.
    async fn shutdown(&self);
}

/// A running deployment. Killed on drop (`kill_on_drop`); `shutdown` also runs
/// any cleanup (e.g. `docker rm -f`). Holds either a local child process
/// (local/docker/system-ssh) or an in-process remote handle (russh).
pub struct DeployedAgent {
    pub id: String,
    inner: DeployedInner,
}

enum DeployedInner {
    Process {
        child: tokio::process::Child,
        cleanup: Option<Vec<String>>,
    },
    Remote(Box<dyn RemoteDeployment>),
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
            inner: DeployedInner::Process { child, cleanup },
        }
    }

    /// Build from an in-process remote handle (e.g. a `russh` session that keeps
    /// the reverse tunnel + remote worker alive).
    pub fn from_remote(id: impl Into<String>, handle: Box<dyn RemoteDeployment>) -> Self {
        Self {
            id: id.into(),
            inner: DeployedInner::Remote(handle),
        }
    }

    pub fn pid(&self) -> Option<u32> {
        match &self.inner {
            DeployedInner::Process { child, .. } => child.id(),
            DeployedInner::Remote(h) => h.remote_pid(),
        }
    }

    /// Stop the deployment: kill the launched process / remote worker, then run
    /// cleanup if any.
    pub async fn shutdown(self) {
        match self.inner {
            DeployedInner::Process {
                mut child,
                cleanup,
            } => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                if let Some(args) = cleanup {
                    if let Some((bin, rest)) = args.split_first() {
                        let _ = Command::new(bin).args(rest).status().await;
                    }
                }
            }
            DeployedInner::Remote(h) => h.shutdown().await,
        }
    }
}

/// The `broker-agent serve …` argv (token is NOT here — it rides the env).
pub(crate) fn agent_argv(d: &AgentDeployment) -> Vec<String> {
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
        // Redirect stdout+stderr to the log file so `tail_log` can read it.
        if let Some(log_path) = &d.log_path {
            if let Some(dir) = std::path::Path::new(log_path).parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            if let Ok(file) = std::fs::File::create(log_path) {
                if let Ok(err_file) = file.try_clone() {
                    cmd.stdout(std::process::Stdio::from(file))
                        .stderr(std::process::Stdio::from(err_file));
                }
            }
        }
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent::from_parts(d.id.clone(), child, None))
    }

    async fn tail_log(&self, log_path: &str, lines: usize) -> BrokerResult<String> {
        tail_local_file(log_path, lines).await
    }
}

/// Read the last `lines` lines of a local file (best-effort).
async fn tail_local_file(path: &str, lines: usize) -> BrokerResult<String> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| BrokerError::Transport(format!("read log '{path}': {e}")))?;
    let tail: Vec<&str> = content.lines().rev().take(lines).collect();
    Ok(tail.into_iter().rev().collect::<Vec<_>>().join("\n"))
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

/// A binary to upload (SFTP/scp) to the remote before launch.
#[derive(Debug, Clone)]
pub struct UploadSpec {
    /// Local-on-orchestrator path to the (correct-arch) bamboo binary.
    pub local_path: String,
    /// Absolute remote path to write it to (e.g. `~/.bamboo-deploy/bamboo-<sha8>`).
    pub remote_path: String,
}

/// Deploy on a remote host over SSH. `bamboo_on_remote` is the binary path on
/// that host. The token rides as an env prefix in the remote command.
pub struct SshDeployer {
    pub host: String,
    pub ssh_bin: String,
    pub scp_bin: String,
    pub bamboo_on_remote: String,
    /// SSH port (`-p`); `None` ⇒ the ssh default (22 / ssh-config).
    pub port: Option<u16>,
    /// Identity file (`-i`) for key-based auth via system ssh.
    pub identity_file: Option<String>,
    /// When set, upload this binary (hash-skip) before launch and run it as the
    /// remote bamboo (overrides `bamboo_on_remote`).
    pub upload: Option<UploadSpec>,
}

impl SshDeployer {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            ssh_bin: "ssh".into(),
            scp_bin: "scp".into(),
            bamboo_on_remote: "bamboo".into(),
            port: None,
            identity_file: None,
            upload: None,
        }
    }

    pub fn with_port(mut self, port: Option<u16>) -> Self {
        // 22 is the ssh default; don't bother passing `-p 22`.
        self.port = port.filter(|p| *p != 22);
        self
    }

    pub fn with_identity(mut self, identity: Option<String>) -> Self {
        self.identity_file = identity.filter(|s| !s.trim().is_empty());
        self
    }

    pub fn with_upload(mut self, upload: Option<UploadSpec>) -> Self {
        if let Some(u) = &upload {
            self.bamboo_on_remote = u.remote_path.clone();
        }
        self.upload = upload;
        self
    }

    /// Common ssh connection flags (host-key TOFU, optional port/identity) shared
    /// by the control commands (hash check, chmod) and the launch.
    fn ssh_conn_flags(&self) -> Vec<String> {
        let mut a = vec![
            "-o".to_string(),
            "StrictHostKeyChecking=accept-new".to_string(),
        ];
        if let Some(p) = self.port {
            a.push("-p".into());
            a.push(p.to_string());
        }
        if let Some(id) = &self.identity_file {
            a.push("-i".into());
            a.push(id.clone());
        }
        a
    }

    /// Run a one-shot remote command over ssh, returning its stdout (trimmed).
    async fn ssh_capture(&self, remote_cmd: &str) -> BrokerResult<String> {
        let mut args = self.ssh_conn_flags();
        args.push(self.host.clone());
        args.push(remote_cmd.to_string());
        let out = Command::new(&self.ssh_bin)
            .args(args)
            .output()
            .await
            .map_err(spawn_err)?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Upload the binary if the remote copy is absent or its hash differs
    /// (idempotent redeploy), then `chmod +x`. No-op when `upload` is unset.
    async fn upload_if_needed(&self) -> BrokerResult<()> {
        let Some(spec) = &self.upload else {
            return Ok(());
        };

        // Local hash (shell out; portable across mac/linux orchestrators).
        let local_hash = file_sha256(&spec.local_path).await;
        // Remote hash (sha256sum on Linux nodes, shasum on macOS). Empty if absent.
        let remote_hash = self
            .ssh_capture(&format!(
                "sha256sum {p} 2>/dev/null || shasum -a 256 {p} 2>/dev/null || true",
                p = sh_quote(&spec.remote_path)
            ))
            .await
            .unwrap_or_default();
        let remote_hash = remote_hash.split_whitespace().next().unwrap_or("");

        if let Some(local) = &local_hash {
            if remote_hash == local && !remote_hash.is_empty() {
                return Ok(()); // already present & identical — skip the upload.
            }
        }

        // Ensure the remote dir exists.
        if let Some(dir) = spec.remote_path.rsplit_once('/').map(|(d, _)| d) {
            if !dir.is_empty() {
                let _ = self
                    .ssh_capture(&format!("mkdir -p {}", sh_quote(dir)))
                    .await;
            }
        }

        // scp upload to a temp path, then atomic rename + chmod +x.
        let tmp = format!("{}.upload", spec.remote_path);
        let mut scp_args = vec![
            "-o".to_string(),
            "StrictHostKeyChecking=accept-new".to_string(),
        ];
        if let Some(p) = self.port {
            // scp uses uppercase -P for the port.
            scp_args.push("-P".into());
            scp_args.push(p.to_string());
        }
        if let Some(id) = &self.identity_file {
            scp_args.push("-i".into());
            scp_args.push(id.clone());
        }
        scp_args.push(spec.local_path.clone());
        scp_args.push(format!("{}:{}", self.host, tmp));
        let status = Command::new(&self.scp_bin)
            .args(scp_args)
            .status()
            .await
            .map_err(spawn_err)?;
        if !status.success() {
            return Err(BrokerError::Transport(format!(
                "scp upload to {} failed (status {status})",
                self.host
            )));
        }
        self.ssh_capture(&format!(
            "chmod +x {tmp} && mv -f {tmp} {dst}",
            tmp = sh_quote(&tmp),
            dst = sh_quote(&spec.remote_path)
        ))
        .await?;
        Ok(())
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
        if let Some(p) = self.port {
            a.push("-p".to_string());
            a.push(p.to_string());
        }
        if let Some(id) = &self.identity_file {
            a.push("-i".to_string());
            a.push(id.clone());
        }
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
        // Redirect the worker's output to its log file on the remote.
        if let Some(log_path) = &d.log_path {
            remote.push_str(&format!(" > {} 2>&1", sh_quote(log_path)));
        }
        a.push(remote);
        a
    }
}

#[async_trait]
impl Deployer for SshDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        // Upload the binary first (hash-skip) so the remote bamboo exists.
        self.upload_if_needed().await?;
        let mut cmd = Command::new(&self.ssh_bin);
        cmd.args(self.argv(d)).kill_on_drop(true);
        let child = cmd.spawn().map_err(spawn_err)?;
        Ok(DeployedAgent::from_parts(d.id.clone(), child, None))
    }

    /// `uname -s -m` over ssh (proves reachability + the key/agent authenticate).
    async fn preflight(&self) -> BrokerResult<String> {
        let out = self.ssh_capture("uname -s -m").await?;
        if out.trim().is_empty() {
            return Err(BrokerError::Transport(format!(
                "ssh preflight to {} produced no output (unreachable or auth failed)",
                self.host
            )));
        }
        Ok(out)
    }

    async fn tail_log(&self, log_path: &str, lines: usize) -> BrokerResult<String> {
        self.ssh_capture(&format!(
            "tail -n {lines} {} 2>/dev/null || true",
            sh_quote(log_path)
        ))
        .await
    }
}

/// Compute the SHA-256 of a local file by shelling out (portable across the
/// mac/linux orchestrator host). Returns `None` if neither tool is available.
async fn file_sha256(path: &str) -> Option<String> {
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "sha256sum {p} 2>/dev/null || shasum -a 256 {p} 2>/dev/null",
            p = sh_quote(path)
        ))
        .output()
        .await
        .ok()?;
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Minimal POSIX single-quote escaping for an SSH remote command argument.
pub(crate) fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Parse the port out of a `ws://host:port[/path]` broker endpoint, for the
/// reverse tunnel (`ssh -R port:127.0.0.1:port`).
pub(crate) fn broker_port(endpoint: &str) -> Option<u16> {
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
            log_path: None,
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
    fn ssh_argv_includes_port_and_identity_when_set() {
        let s = SshDeployer::new("user@gpu-host")
            .with_port(Some(2222))
            .with_identity(Some("/keys/id_ed25519".into()));
        let a = s.argv(&dep());
        assert!(a.windows(2).any(|w| w == ["-p".to_string(), "2222".to_string()]));
        assert!(a
            .windows(2)
            .any(|w| w == ["-i".to_string(), "/keys/id_ed25519".to_string()]));
    }

    #[test]
    fn with_port_omits_default_22() {
        let s = SshDeployer::new("h").with_port(Some(22));
        assert_eq!(s.port, None, "port 22 is the ssh default; not passed");
        let a = s.argv(&dep());
        assert!(!a.iter().any(|x| x == "-p"));
    }

    #[test]
    fn with_upload_points_remote_binary_at_uploaded_path() {
        let s = SshDeployer::new("user@box").with_upload(Some(UploadSpec {
            local_path: "/local/bamboo".into(),
            remote_path: ".bamboo-deploy/bamboo".into(),
        }));
        assert_eq!(s.bamboo_on_remote, ".bamboo-deploy/bamboo");
        // The launch command runs the uploaded binary, not a PATH `bamboo`.
        let remote = s.argv(&dep()).last().unwrap().clone();
        assert!(remote.contains("'.bamboo-deploy/bamboo'"));
    }

    #[test]
    fn with_identity_ignores_blank() {
        let s = SshDeployer::new("h").with_identity(Some("   ".into()));
        assert_eq!(s.identity_file, None);
    }

    #[test]
    fn ssh_argv_appends_log_redirect_when_set() {
        let s = SshDeployer::new("user@box");
        let mut d = dep();
        d.log_path = Some(".bamboo-deploy/node-x.log".into());
        let remote = s.argv(&d).last().unwrap().clone();
        assert!(
            remote.trim_end().ends_with("> '.bamboo-deploy/node-x.log' 2>&1"),
            "got: {remote}"
        );
    }

    #[tokio::test]
    async fn tail_local_file_returns_last_lines() {
        let path = std::env::temp_dir().join("bamboo-tail-test.log");
        tokio::fs::write(&path, "l1\nl2\nl3\nl4\nl5\n").await.unwrap();
        let out = tail_local_file(path.to_str().unwrap(), 2).await.unwrap();
        assert_eq!(out, "l4\nl5");
        let _ = tokio::fs::remove_file(&path).await;
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
