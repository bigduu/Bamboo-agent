//! `RusshDeployer` — deploy a `broker-agent` worker over an in-process SSH
//! connection ([`russh`]) using STORED credentials (password / private key),
//! which system `ssh` cannot accept cleanly.
//!
//! It does everything the system-`ssh` path does, in-process:
//! 1. connect + host-key TOFU (pin the fingerprint; reject a changed key),
//! 2. authenticate with the decrypted password or private key,
//! 3. SFTP-upload the binary (hash-skip) + `chmod +x`,
//! 4. open a **reverse tunnel** (`tcpip_forward`) so the worker reaches the
//!    127.0.0.1-bound broker over the SSH connection — no inbound port on the
//!    remote — bridging each forwarded connection to the local broker,
//! 5. `exec` the worker pointed at the tunnel mouth.
//!
//! The returned [`DeployedAgent`] owns the live `russh` session: keeping it
//! alive keeps the tunnel + worker up; `shutdown`/`shutdown_with_timeout`
//! signal the remote worker and POLL for it to exit — keeping the session and
//! reverse tunnel up throughout, so an in-flight reply can drain through it —
//! before hard-killing and disconnecting (session-bound lifetime, like the
//! system-ssh path — true survive-restart durability is a later phase). #489.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use russh::client::{self, Handle, Msg};
use russh::keys::{ssh_key, HashAlg, PrivateKey, PrivateKeyWithHashAlg};
use russh::{Channel, ChannelMsg};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::deploy::{
    agent_argv, broker_port, sh_quote, AgentDeployment, DeployedAgent, Deployer, RemoteDeployment,
    UploadSpec,
};
use crate::error::{BrokerError, BrokerResult};

/// Interval between `pgrep` liveness polls during the graceful-drain window in
/// [`RusshHandle::shutdown_with_timeout`]. Short enough to notice a fast exit
/// promptly, long enough not to hammer the SSH connection with exec requests.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Decrypted SSH credentials for the russh path (the fabric layer decrypts the
/// at-rest ciphertext before constructing the deployer).
pub enum RusshAuth {
    Password(String),
    PrivateKey {
        /// Inline PEM (OpenSSH private key).
        pem: String,
        passphrase: Option<String>,
    },
}

/// Deploy over `russh` with stored credentials + SFTP upload + reverse tunnel.
pub struct RusshDeployer {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub auth: RusshAuth,
    /// Pinned host-key fingerprint (`SHA256:…`). `None` ⇒ trust-on-first-use:
    /// accept and record (read it back via [`RusshDeployer::observed_fingerprint`]).
    pub expected_fingerprint: Option<String>,
    /// Binary to upload before launch; when set, the worker runs it.
    pub upload: Option<UploadSpec>,
    /// Remote bamboo path when not uploading (assume pre-installed).
    pub bamboo_on_remote: String,
    observed: Arc<Mutex<Option<String>>>,
}

impl RusshDeployer {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        auth: RusshAuth,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            auth,
            expected_fingerprint: None,
            upload: None,
            bamboo_on_remote: "bamboo".to_string(),
            observed: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_fingerprint(mut self, fp: Option<String>) -> Self {
        self.expected_fingerprint = fp.filter(|s| !s.trim().is_empty());
        self
    }

    pub fn with_upload(mut self, upload: Option<UploadSpec>) -> Self {
        if let Some(u) = &upload {
            self.bamboo_on_remote = u.remote_path.clone();
        }
        self.upload = upload;
        self
    }

    /// The host-key fingerprint observed at the last connect (for TOFU pinning).
    pub async fn observed_fingerprint(&self) -> Option<String> {
        self.observed.lock().await.clone()
    }

    /// A shared handle to the observed-fingerprint cell, so callers can read the
    /// TOFU-observed fingerprint AFTER the deployer has been boxed as
    /// `Box<dyn Deployer>` and consumed by `deploy`.
    pub fn observed_cell(&self) -> Arc<Mutex<Option<String>>> {
        self.observed.clone()
    }

    /// Connect + host-key TOFU + authenticate, returning the live session.
    /// `broker_local` is only used to bridge reverse-tunnel connections (unused
    /// during preflight, where no `tcpip_forward` is requested).
    async fn connect_and_auth(&self, broker_local: String) -> BrokerResult<Handle<FabricHandler>> {
        let config = Arc::new(client::Config::default());
        let handler = FabricHandler {
            expected: self.expected_fingerprint.clone(),
            observed: self.observed.clone(),
            broker_local,
        };
        let mut session = client::connect(config, (self.host.as_str(), self.port), handler)
            .await
            .map_err(|e| {
                transport(format!(
                    "russh connect {}:{} failed: {e}",
                    self.host, self.port
                ))
            })?;

        let authed = match &self.auth {
            RusshAuth::Password(pw) => session
                .authenticate_password(&self.username, pw)
                .await
                .map_err(|e| transport(format!("password auth error: {e}")))?
                .success(),
            RusshAuth::PrivateKey { pem, passphrase } => {
                let key = parse_private_key(pem, passphrase.as_deref())?;
                let hash = session
                    .best_supported_rsa_hash()
                    .await
                    .ok()
                    .flatten()
                    .flatten();
                session
                    .authenticate_publickey(
                        &self.username,
                        PrivateKeyWithHashAlg::new(Arc::new(key), hash),
                    )
                    .await
                    .map_err(|e| transport(format!("publickey auth error: {e}")))?
                    .success()
            }
        };
        if !authed {
            return Err(transport(format!(
                "russh authentication failed for {}@{}",
                self.username, self.host
            )));
        }
        Ok(session)
    }
}

/// russh client handler: enforces host-key TOFU and bridges reverse-tunnel
/// (forwarded-tcpip) connections to the local broker.
struct FabricHandler {
    expected: Option<String>,
    observed: Arc<Mutex<Option<String>>>,
    /// Local broker address to bridge forwarded connections to (`127.0.0.1:9600`).
    broker_local: String,
}

impl client::Handler for FabricHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        let fp = server_public_key.fingerprint(HashAlg::Sha256).to_string();
        *self.observed.lock().await = Some(fp.clone());
        match &self.expected {
            // Known host: the key must match the pin (reject a changed key = MITM).
            Some(expected) => Ok(&fp == expected),
            // First contact: trust-on-first-use (the caller records the fp).
            None => Ok(true),
        }
    }

    async fn server_channel_open_forwarded_tcpip(
        &mut self,
        channel: Channel<Msg>,
        _connected_address: &str,
        _connected_port: u32,
        _originator_address: &str,
        _originator_port: u32,
        reply: client::ChannelOpenHandle,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        // russh 0.62 requires the handler to explicitly accept (or reject) the
        // forwarded channel; dropping the reply would auto-reject it.
        reply.accept().await;
        // A worker on the remote dialed the tunnel mouth; splice it to the broker.
        let broker = self.broker_local.clone();
        tokio::spawn(async move {
            match TcpStream::connect(&broker).await {
                Ok(mut tcp) => {
                    let mut stream = channel.into_stream();
                    let _ = tokio::io::copy_bidirectional(&mut stream, &mut tcp).await;
                }
                Err(e) => {
                    tracing::warn!("russh forwarded-tcpip: broker connect {broker} failed: {e}");
                }
            }
        });
        Ok(())
    }
}

/// Live remote handle: owns the `russh` session (keeping the tunnel + worker
/// alive). `shutdown`/`shutdown_with_timeout` signal the worker by id, poll for
/// its exit through the still-open tunnel, then hard-kill + disconnect (#489).
struct RusshHandle {
    session: Mutex<Option<Handle<FabricHandler>>>,
    worker_id: String,
    /// Parent-resolved spec file on the remote (holds provider keys + the broker
    /// token). Removed on shutdown, and — being globally unique — the safest
    /// `pkill` anchor (the truncated worker_id can substring-collide across nodes).
    remote_spec_path: Option<String>,
}

#[async_trait]
impl RemoteDeployment for RusshHandle {
    /// #489: `pkill` only SIGNALS the worker — it does not wait for it to
    /// exit. The previous implementation disconnected (tearing down the SSH
    /// session and, with it, the reverse tunnel the worker drains its
    /// in-flight Ask/Task/Run reply through) immediately after issuing the
    /// signal, severing the graceful drain #488 added mid-reply. Fix: signal,
    /// then POLL the remote for the worker's exit — keeping the session/tunnel
    /// alive — for up to `timeout` (mirrors `DeployedAgent::shutdown_with_timeout`'s
    /// local-process SIGTERM-then-poll pattern), THEN hard-kill as a fallback
    /// and only then disconnect.
    async fn shutdown_with_timeout(&self, timeout: Duration) {
        let Some(session) = self.session.lock().await.take() else {
            return;
        };
        // Anchored on the unique spec path when present — the truncated
        // worker_id can substring-collide across nodes.
        let kill_anchor = self.remote_spec_path.as_deref().unwrap_or(&self.worker_id);

        // 1. SIGTERM (pkill's default signal): give the worker's own shutdown
        //    handler a chance to drain cleanly, same as the local-process path.
        let term_cmd = format!("pkill -f {}", sh_quote(kill_anchor));
        if let Ok(channel) = session.channel_open_session().await {
            let _ = channel.exec(false, term_cmd).await;
        }

        // 2. Poll for the worker's exit within the grace window. The session
        //    (and therefore the reverse tunnel) stays open the entire time, so
        //    a reply the worker is still draining through it can complete.
        //    Best-effort: a poll that errors (e.g. a transient exec hiccup) is
        //    treated as "still alive" rather than aborting the wait early.
        poll_until_exited(
            || async {
                exec_capture(
                    &session,
                    &format!(
                        "pgrep -f {} >/dev/null 2>&1 && echo 1 || echo 0",
                        sh_quote(kill_anchor)
                    ),
                )
                .await
                .map(|out| out.trim() == "1")
                .unwrap_or(true)
            },
            timeout,
            POLL_INTERVAL,
        )
        .await;

        // 3. Hard-kill fallback (harmless no-op if it already exited above) +
        //    remove the creds-bearing spec file, THEN disconnect — only now is
        //    the reverse tunnel torn down.
        let mut cmd = format!("pkill -9 -f {}", sh_quote(kill_anchor));
        if let Some(spec) = &self.remote_spec_path {
            cmd.push_str(&format!("; rm -f {}", sh_quote(spec)));
        }
        if let Ok(channel) = session.channel_open_session().await {
            let _ = channel.exec(false, cmd).await;
        }
        let _ = session
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await;
    }
}

/// Poll `is_alive` every `interval` until it reports `false` (exited on its
/// own) or `timeout` elapses (still alive at the deadline — caller should
/// escalate to a hard kill). Returns `true` iff it exited within the window.
///
/// Factored out of [`RusshHandle::shutdown_with_timeout`] so the bounded-grace
/// polling behavior is unit-testable without a live SSH session — see the
/// `poll_until_exited_*` tests below. Mirrors the shape of the local-process
/// poll loop in [`DeployedAgent::shutdown_with_timeout`], just driven by an
/// arbitrary async liveness check instead of `Child::try_wait`.
async fn poll_until_exited<F, Fut>(mut is_alive: F, timeout: Duration, interval: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !is_alive().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

fn transport(msg: impl std::fmt::Display) -> BrokerError {
    BrokerError::Transport(msg.to_string())
}

#[async_trait]
impl Deployer for RusshDeployer {
    async fn deploy(&self, d: &AgentDeployment) -> BrokerResult<DeployedAgent> {
        let bport = broker_port(&d.broker_endpoint)
            .ok_or_else(|| transport(format!("no broker port in '{}'", d.broker_endpoint)))?;
        let broker_local = format!("127.0.0.1:{bport}");

        // 1–2. Connect + host-key TOFU + authenticate.
        let session = self.connect_and_auth(broker_local).await?;

        // 3. Upload the binary (hash-skip) + chmod +x.
        if let Some(spec) = &self.upload {
            upload_if_needed(&session, spec).await?;
        }

        // 4. Reverse tunnel: ask the server to listen on its loopback :bport and
        //    forward connections back (handled in `server_channel_open_forwarded_tcpip`).
        session
            .tcpip_forward("127.0.0.1", bport as u32)
            .await
            .map_err(|e| {
                transport(format!(
                    "reverse tunnel (tcpip_forward {bport}) failed: {e}"
                ))
            })?;

        // 4b. Upload the parent-built ProvisionSpec (if any) so the worker reads
        //     it via --spec-file instead of self-resolving from the remote's config.
        let remote_spec_path = if let Some(spec_json) = &d.spec_json {
            let path = format!("/tmp/bamboo-spec-{}.json", d.id);
            sftp_write_bytes(&session, &path, spec_json.as_bytes()).await?;
            // The spec holds provider API keys + the broker token: restrict it to
            // the owner (default umask would leave it world-readable) — it's
            // removed on shutdown.
            let _ = exec_capture(&session, &format!("chmod 600 {}", sh_quote(&path))).await;
            Some(path)
        } else {
            None
        };

        // 5. Launch the worker pointed at the tunnel mouth on the remote loopback.
        let mut tunneled = d.clone();
        tunneled.broker_endpoint = format!("ws://127.0.0.1:{bport}");
        let remote_cmd = build_launch_cmd(
            &tunneled,
            &self.bamboo_on_remote,
            remote_spec_path.as_deref(),
        );
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| transport(format!("open launch channel failed: {e}")))?;
        channel
            .exec(true, remote_cmd)
            .await
            .map_err(|e| transport(format!("exec launch failed: {e}")))?;
        // Drain the launch channel in the background so the worker isn't blocked
        // on a full window; it ends when the session closes.
        tokio::spawn(async move {
            let mut channel = channel;
            while let Some(msg) = channel.wait().await {
                if let ChannelMsg::ExitStatus { .. } = msg {
                    // The worker exec returned (it normally runs until killed).
                }
            }
        });

        Ok(DeployedAgent::from_remote(
            d.id.clone(),
            Box::new(RusshHandle {
                session: Mutex::new(Some(session)),
                worker_id: d.id.clone(),
                remote_spec_path,
            }),
        ))
    }

    /// Connect + auth + `uname -s -m`, then disconnect. No deploy, no tunnel —
    /// proves the node is reachable and the stored credentials work.
    async fn preflight(&self) -> BrokerResult<String> {
        let session = self.connect_and_auth(String::new()).await?;
        let uname = exec_capture(&session, "uname -s -m").await?;
        let _ = session
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await;
        Ok(uname)
    }

    async fn tail_log(&self, log_path: &str, lines: usize) -> BrokerResult<String> {
        let session = self.connect_and_auth(String::new()).await?;
        let out = exec_capture(
            &session,
            &format!("tail -n {lines} {} 2>/dev/null || true", sh_quote(log_path)),
        )
        .await?;
        let _ = session
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await;
        Ok(out)
    }
}

/// Build the remote launch command: `BAMBOO_BROKER_TOKEN=… <bamboo> broker-agent serve …`.
fn build_launch_cmd(
    d: &AgentDeployment,
    bamboo_on_remote: &str,
    spec_file: Option<&str>,
) -> String {
    let mut cmd = format!("BAMBOO_BROKER_TOKEN={}", sh_quote(&d.token));
    cmd.push(' ');
    cmd.push_str(&sh_quote(bamboo_on_remote));
    for arg in agent_argv(d) {
        cmd.push(' ');
        cmd.push_str(&sh_quote(&arg));
    }
    // Parent-resolved spec uploaded next to the binary: the worker reads it
    // instead of self-resolving from this host's (possibly absent) local config.
    if let Some(path) = spec_file {
        cmd.push_str(" --spec-file ");
        cmd.push_str(&sh_quote(path));
    }
    // Redirect the worker's output to its log file on the remote.
    if let Some(log_path) = &d.log_path {
        cmd.push_str(&format!(" > {} 2>&1", sh_quote(log_path)));
    }
    cmd
}

/// SFTP-write `bytes` to `remote_path` (CREATE|TRUNCATE). Used to upload a
/// parent-built ProvisionSpec so the remote worker reads it via `--spec-file`.
async fn sftp_write_bytes(
    session: &Handle<FabricHandler>,
    remote_path: &str,
    bytes: &[u8],
) -> BrokerResult<()> {
    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| transport(format!("open sftp channel failed: {e}")))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| transport(format!("request sftp subsystem failed: {e}")))?;
    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| transport(format!("sftp init failed: {e}")))?;
    if let Some((dir, _)) = remote_path.rsplit_once('/') {
        if !dir.is_empty() {
            let _ = sftp.create_dir(dir).await;
        }
    }
    let mut file = sftp
        .open_with_flags(
            remote_path.to_string(),
            OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
        )
        .await
        .map_err(|e| transport(format!("sftp open '{remote_path}' failed: {e}")))?;
    file.write_all(bytes)
        .await
        .map_err(|e| transport(format!("sftp write '{remote_path}' failed: {e}")))?;
    file.shutdown()
        .await
        .map_err(|e| transport(format!("sftp flush '{remote_path}' failed: {e}")))?;
    Ok(())
}

/// Parse an inline OpenSSH private key, decrypting with the passphrase if needed.
fn parse_private_key(pem: &str, passphrase: Option<&str>) -> BrokerResult<PrivateKey> {
    let key = PrivateKey::from_openssh(pem)
        .map_err(|e| transport(format!("invalid private key: {e}")))?;
    if key.is_encrypted() {
        let pass = passphrase
            .ok_or_else(|| transport("private key is encrypted but no passphrase was provided"))?;
        key.decrypt(pass)
            .map_err(|e| transport(format!("private key decrypt failed: {e}")))
    } else {
        Ok(key)
    }
}

/// SFTP-upload the binary if the remote copy is absent or differs (hash-skip),
/// then `chmod +x`.
async fn upload_if_needed(session: &Handle<FabricHandler>, spec: &UploadSpec) -> BrokerResult<()> {
    // Remote hash (Linux: sha256sum; macOS: shasum). Empty if absent.
    let remote_hash = exec_capture(
        session,
        &format!(
            "sha256sum {p} 2>/dev/null || shasum -a 256 {p} 2>/dev/null || true",
            p = sh_quote(&spec.remote_path)
        ),
    )
    .await
    .unwrap_or_default();
    let remote_hash = remote_hash
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string();

    let bytes = tokio::fs::read(&spec.local_path)
        .await
        .map_err(|e| transport(format!("read artifact '{}': {e}", spec.local_path)))?;
    let local_hash = sha256_hex(&bytes);

    if !remote_hash.is_empty() && remote_hash == local_hash {
        return Ok(()); // already present & identical — skip the upload.
    }

    // Open an SFTP subsystem channel.
    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| transport(format!("open sftp channel failed: {e}")))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| transport(format!("request sftp subsystem failed: {e}")))?;
    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| transport(format!("sftp init failed: {e}")))?;

    // Ensure the remote dir exists (ignore "already exists").
    if let Some((dir, _)) = spec.remote_path.rsplit_once('/') {
        if !dir.is_empty() {
            let _ = sftp.create_dir(dir).await;
        }
    }

    let tmp = format!("{}.upload", spec.remote_path);
    // `SftpSession::write` opens WRITE-only (no CREATE) — fails for a new file.
    // Open explicitly with CREATE|TRUNCATE and stream the bytes.
    let mut file = sftp
        .open_with_flags(
            tmp.clone(),
            OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
        )
        .await
        .map_err(|e| transport(format!("sftp open '{tmp}' failed: {e}")))?;
    file.write_all(&bytes)
        .await
        .map_err(|e| transport(format!("sftp write '{tmp}' failed: {e}")))?;
    file.shutdown()
        .await
        .map_err(|e| transport(format!("sftp flush '{tmp}' failed: {e}")))?;
    drop(file);
    drop(sftp); // close the sftp channel before the control exec.

    // chmod +x + atomic rename into place.
    exec_capture(
        session,
        &format!(
            "chmod +x {tmp} && mv -f {tmp} {dst}",
            tmp = sh_quote(&tmp),
            dst = sh_quote(&spec.remote_path)
        ),
    )
    .await?;
    Ok(())
}

/// Run a one-shot remote command and return its stdout (trimmed).
async fn exec_capture(session: &Handle<FabricHandler>, cmd: &str) -> BrokerResult<String> {
    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| transport(format!("open exec channel failed: {e}")))?;
    channel
        .exec(true, cmd)
        .await
        .map_err(|e| transport(format!("exec '{cmd}' failed: {e}")))?;
    let mut channel = channel;
    let mut out = Vec::new();
    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { ref data } => out.extend_from_slice(data),
            ChannelMsg::ExitStatus { .. } => {}
            ChannelMsg::Eof | ChannelMsg::Close => break,
            _ => {}
        }
    }
    Ok(String::from_utf8_lossy(&out).trim().to_string())
}

/// SHA-256 of a byte slice as lowercase hex (matches `sha256sum`/`shasum`).
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dep() -> AgentDeployment {
        AgentDeployment {
            id: "node-abc".into(),
            role: Some("worker".into()),
            broker_endpoint: "ws://127.0.0.1:9600".into(),
            token: "tok".into(),
            model: None,
            workspace: None,
            echo: true,
            mcp_proxy: Some("bamboo-orchestrator".into()),
            log_path: None,
            spec_json: None,
        }
    }

    #[test]
    fn launch_cmd_carries_token_in_env_and_runs_remote_binary() {
        let cmd = build_launch_cmd(&dep(), ".bamboo-deploy/bamboo", None);
        assert!(cmd.starts_with("BAMBOO_BROKER_TOKEN='tok'"));
        assert!(cmd.contains("'.bamboo-deploy/bamboo'"));
        assert!(cmd.contains("'broker-agent'") && cmd.contains("'serve'"));
        assert!(cmd.contains("'--echo'"));
    }

    #[test]
    fn launch_cmd_appends_log_redirect_when_set() {
        let mut d = dep();
        d.log_path = Some(".bamboo-deploy/node-abc.log".to_string());
        let cmd = build_launch_cmd(&d, ".bamboo-deploy/bamboo", None);
        assert!(
            cmd.trim_end()
                .ends_with("> '.bamboo-deploy/node-abc.log' 2>&1"),
            "got: {cmd}"
        );
    }

    #[test]
    fn launch_cmd_appends_spec_file_when_set() {
        let cmd = build_launch_cmd(&dep(), ".bamboo-deploy/bamboo", Some("/tmp/spec.json"));
        assert!(cmd.contains("--spec-file '/tmp/spec.json'"), "got: {cmd}");
    }

    #[test]
    fn with_upload_points_remote_binary_at_uploaded_path() {
        let d = RusshDeployer::new("h", 22, "u", RusshAuth::Password("p".into())).with_upload(
            Some(UploadSpec {
                local_path: "/local/bamboo".into(),
                remote_path: ".bamboo-deploy/bamboo".into(),
            }),
        );
        assert_eq!(d.bamboo_on_remote, ".bamboo-deploy/bamboo");
    }

    #[test]
    fn sha256_matches_known_vector() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn parse_private_key_rejects_garbage() {
        assert!(parse_private_key("not a key", None).is_err());
    }

    // #489: `poll_until_exited` backs `RusshHandle::shutdown_with_timeout`'s
    // grace-window wait. It can't be exercised against a real remote without a
    // live SSH session (see `tests/russh_live.rs`, gated behind
    // `BAMBOO_RUSSH_LIVE=1`), so these tests drive it directly with a fake
    // liveness check instead — the polling/timeout behavior is identical
    // either way, since `poll_until_exited` doesn't know or care that the real
    // liveness check is an SSH `pgrep` exec.

    /// A worker that exits partway through the grace window: `is_alive` flips
    /// to `false` after a couple of polls. Must return `true` (exited on its
    /// own) well before the timeout elapses.
    #[tokio::test]
    async fn poll_until_exited_returns_true_once_alive_check_goes_false() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls2 = calls.clone();
        let start = tokio::time::Instant::now();
        let exited = poll_until_exited(
            move || {
                let calls = calls2.clone();
                async move {
                    // Alive for the first two checks, exited by the third.
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await;
        assert!(exited, "must report exited once is_alive returns false");
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) >= 3,
            "must have polled past the two `alive` responses"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "must return promptly once the worker exits, not wait out the full timeout"
        );
    }

    /// A worker that never exits (e.g. ignores the signal, mirroring the
    /// SIGTERM-ignoring case #49 tests for the local-process path): the poll
    /// must be BOUNDED — return `false` once `timeout` elapses rather than
    /// hanging forever — so the caller's hard-kill fallback always runs.
    #[tokio::test]
    async fn poll_until_exited_returns_false_when_still_alive_at_deadline() {
        let exited = tokio::time::timeout(
            Duration::from_secs(5),
            poll_until_exited(
                || async { true }, // always "still alive"
                Duration::from_millis(150),
                Duration::from_millis(20),
            ),
        )
        .await
        .expect("poll_until_exited must be bounded by its own timeout");
        assert!(!exited, "must report NOT exited once the deadline elapses");
    }

    /// A worker that's already gone by the first check: must return
    /// immediately without waiting a full `interval`.
    #[tokio::test]
    async fn poll_until_exited_returns_true_immediately_when_already_dead() {
        let start = tokio::time::Instant::now();
        let exited = poll_until_exited(
            || async { false },
            Duration::from_secs(5),
            Duration::from_secs(10), // deliberately long — must not be waited on
        )
        .await;
        assert!(exited);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "first is_alive()==false must short-circuit, not wait an interval"
        );
    }
}
