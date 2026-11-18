use std::{
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

const LOG_TAIL_BYTES: u64 = 16 * 1024;
const DISCOVERY_READ_BYTES: u64 = 64 * 1024;
const STARTUP_BUDGET: Duration = Duration::from_secs(30);

struct Launch {
    id: uuid::Uuid,
    generation: usize,
    log: PathBuf,
    log_start: u64,
}

pub struct Host(pub Child, Launch);

pub fn start_host(data: &Path, generation: usize) -> Host {
    start_on_port(data, generation, 0)
}

fn start_on_port(data: &Path, generation: usize, port: u16) -> Host {
    let path = data.join(format!("host-{generation}.log"));
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    let launch = Launch {
        id: uuid::Uuid::new_v4(),
        generation,
        log: path,
        log_start: log.metadata().unwrap().len(),
    };
    let process = Command::new(env!("CARGO_BIN_EXE_bamboo"))
        .args([
            "serve",
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--workers",
            "1",
            "--data-dir",
        ])
        .arg(data)
        .current_dir(data)
        .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap_or_else(|error| {
            panic!(
                "Host spawn failed: launch={} generation={} log={}: {error}",
                launch.id,
                launch.generation,
                launch.log.display()
            )
        });
    Host(process, launch)
}

fn current_log_tail(path: &Path, start: u64) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length < start {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Host log was truncated after this launch started",
        ));
    }
    file.seek(SeekFrom::Start(
        start.max(length.saturating_sub(LOG_TAIL_BYTES)),
    ))?;
    let mut bytes = Vec::new();
    file.take(LOG_TAIL_BYTES).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn announced_address(log: &[u8]) -> Option<SocketAddr> {
    String::from_utf8_lossy(log).lines().find_map(|line| {
        let (_, address) = line.split_once("Unified server running on http://")?;
        let address: SocketAddr = address.trim().parse().ok()?;
        (address.is_ipv4() && address.ip().is_loopback() && address.port() != 0).then_some(address)
    })
}

struct ListenerDiscovery {
    offset: u64,
    pending: Vec<u8>,
    address: Option<SocketAddr>,
}

impl ListenerDiscovery {
    fn new(offset: u64) -> Self {
        Self {
            offset,
            pending: Vec::new(),
            address: None,
        }
    }

    fn read(&mut self, path: &Path) -> std::io::Result<Option<SocketAddr>> {
        if self.address.is_some() {
            return Ok(self.address);
        }
        let mut file = std::fs::File::open(path)?;
        if file.metadata()?.len() < self.offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Host log was truncated during listener discovery",
            ));
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.take(DISCOVERY_READ_BYTES).read_to_end(&mut bytes)?;
        self.offset += bytes.len() as u64;
        self.pending.extend_from_slice(&bytes);
        if let Some(end) = self.pending.iter().rposition(|byte| *byte == b'\n') {
            self.address = announced_address(&self.pending[..=end]);
            self.pending = self.pending[end + 1..].to_vec();
        }
        // Retain a split announcement/address, without buffering an arbitrary
        // long unrelated line. Accept addresses only after its newline arrives.
        if self.pending.len() > 256 {
            self.pending.drain(..self.pending.len() - 256);
        }
        Ok(self.address)
    }
}

impl Host {
    pub fn generation(&self) -> usize {
        self.1.generation
    }

    fn diagnostics(&mut self, reason: &str) -> String {
        let exit = match self.0.try_wait() {
            Ok(Some(status)) => status.to_string(),
            Ok(None) => "running".to_string(),
            Err(error) => format!("unavailable: {error}"),
        };
        let tail = match current_log_tail(&self.1.log, self.1.log_start) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) => format!("current-launch log unavailable: {error}"),
        };
        format!(
            "{reason}: pid={} launch={} generation={} exit={} log={} start={}\nCurrent-launch log tail (at most {LOG_TAIL_BYTES} bytes):\n{tail}",
            self.0.id(),
            self.1.id,
            self.1.generation,
            exit,
            self.1.log.display(),
            self.1.log_start
        )
    }

    pub async fn ready(&mut self, client: &reqwest::Client) -> Result<String, String> {
        let mut discovery = ListenerDiscovery::new(self.1.log_start);
        let ready = tokio::time::timeout(STARTUP_BUDGET, async {
            loop {
                match self.0.try_wait() {
                    Ok(None) => {}
                    Ok(Some(_)) => return Err(self.diagnostics("Host exited before readiness")),
                    Err(error) => {
                        return Err(self.diagnostics(&format!("Host status failed: {error}")))
                    }
                }
                let address = discovery
                    .read(&self.1.log)
                    .map_err(|error| self.diagnostics(&format!("Host log read failed: {error}")))?;
                if let Some(address) = address {
                    let base = format!("http://{address}/api/v1");
                    if client
                        .get(format!("{base}/health"))
                        .send()
                        .await
                        .is_ok_and(|response| response.status().is_success())
                    {
                        match self.0.try_wait() {
                            Ok(None) => return Ok(base),
                            Ok(Some(_)) => {
                                return Err(self.diagnostics("Host exited during health probe"))
                            }
                            Err(error) => {
                                return Err(
                                    self.diagnostics(&format!("Host status failed: {error}"))
                                )
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await;
        ready.unwrap_or_else(|_| Err(self.diagnostics("Host readiness timed out")))
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", self.diagnostics("Host before panic teardown"));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{web, App, HttpResponse, HttpServer};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn isolated_data(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(
            path.join("config.json"),
            serde_json::to_vec(&serde_json::json!({
                "provider":"openai",
                "providers":{"openai":{"api_key":"fixture-key","base_url":"http://127.0.0.1:9/v1","model":"fixture"}},
                "defaults":{"chat":{"provider":"openai","model":"fixture"}}
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .pool_max_idle_per_host(0)
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
    }

    async fn healthy_peer() -> (SocketAddr, Arc<AtomicUsize>, actix_web::dev::ServerHandle) {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let server = HttpServer::new(move || {
            let calls = observed.clone();
            App::new().default_service(web::to(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { HttpResponse::Ok().body("OK") }
            }))
        })
        .workers(1)
        .bind(("127.0.0.1", 0))
        .unwrap();
        let address = server.addrs()[0];
        let server = server.run();
        let handle = server.handle();
        actix_web::rt::spawn(server);
        (address, calls, handle)
    }

    #[actix_web::test]
    async fn ephemeral_hosts_publish_owned_addresses_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let first_data = temp.path().join("first");
        let second_data = temp.path().join("second");
        isolated_data(&first_data);
        isolated_data(&second_data);
        let (peer, calls, handle) = healthy_peer().await;
        let stale = format!("STALE_LAUNCH\nUnified server running on http://{peer}\n");
        std::fs::write(first_data.join("host-1.log"), &stale).unwrap();
        let mut first = start_host(&first_data, 1);
        let mut second = start_host(&second_data, 1);
        let client = client();
        let (first_base, second_base) = tokio::join!(first.ready(&client), second.ready(&client));
        let first_base = first_base.unwrap();
        let second_base = second_base.unwrap();
        assert_ne!(first_base, second_base);
        assert_ne!(first_base, format!("http://{peer}/api/v1"));
        let first_launch = first.1.id;
        drop(first);
        std::fs::write(
            first_data.join("host-2.log"),
            format!(
                "{stale}Unified server running on {}\n",
                first_base.trim_end_matches("/api/v1")
            ),
        )
        .unwrap();
        let mut restarted = start_host(&first_data, 2);
        assert_ne!(first_launch, restarted.1.id);
        let restarted_base = restarted.ready(&client).await.unwrap();
        assert_ne!(restarted_base, second_base);
        assert_ne!(restarted_base, format!("http://{peer}/api/v1"));
        assert_eq!(calls.load(Ordering::SeqCst), 0, "never probe a stale peer");
        drop(restarted);
        drop(second);
        handle.stop(true).await;
    }

    #[actix_web::test]
    async fn occupied_port_reports_current_launch_exit_without_probing_peer() {
        let temp = tempfile::tempdir().unwrap();
        isolated_data(temp.path());
        let (peer, calls, handle) = healthy_peer().await;
        std::fs::write(
            temp.path().join("host-1.log"),
            format!("STALE_LAUNCH\nUnified server running on http://{peer}\n"),
        )
        .unwrap();
        let mut host = start_on_port(temp.path(), 1, peer.port());
        let error = host.ready(&client()).await.unwrap_err();
        assert!(error.contains("Host exited before readiness"), "{error}");
        for evidence in [
            format!("pid={}", host.0.id()),
            format!("launch={}", host.1.id),
            "generation=1".to_string(),
            "exit=exit status:".to_string(),
            "host-1.log".to_string(),
            "Failed to bind listener".to_string(),
        ] {
            assert!(error.contains(&evidence), "missing {evidence}: {error}");
        }
        assert!(!error.contains("STALE_LAUNCH"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(host);
        handle.stop(true).await;
    }

    #[test]
    fn startup_log_is_bounded_and_excludes_previous_launches() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("host.log");
        let old = b"STALE\nUnified server running on http://127.0.0.1:45678\n";
        std::fs::write(&path, old).unwrap();
        let start = old.len() as u64;
        assert!(announced_address(&current_log_tail(&path, start).unwrap()).is_none());
        for invalid in [
            "127.0.0.1:0",
            "192.0.2.1:45678",
            "[::1]:45678",
            "127.0.0.1:45678 trailing text",
        ] {
            assert!(announced_address(
                format!("Unified server running on http://{invalid}\n").as_bytes()
            )
            .is_none());
        }
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        use std::io::Write;
        log.write_all(&[b'x'; (LOG_TAIL_BYTES * 2) as usize])
            .unwrap();
        log.write_all(b"\nUnified server running on http://127.0.0.1:45679\n")
            .unwrap();
        let tail = current_log_tail(&path, start).unwrap();
        assert!(tail.len() as u64 <= LOG_TAIL_BYTES);
        assert!(!String::from_utf8_lossy(&tail).contains("STALE"));
        assert_eq!(
            announced_address(&tail),
            Some("127.0.0.1:45679".parse().unwrap())
        );
        assert!(current_log_tail(&path, u64::MAX).is_err());
    }

    #[test]
    fn discovery_retains_announcements_across_large_logs_and_split_writes() {
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("host.log");
        let stale = b"Unified server running on http://127.0.0.1:45678\n";
        std::fs::write(&path, stale).unwrap();
        let mut discovery = ListenerDiscovery::new(stale.len() as u64);
        assert_eq!(discovery.read(&path).unwrap(), None);
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        log.write_all(&[b'x'; (DISCOVERY_READ_BYTES * 2) as usize])
            .unwrap();
        log.write_all(b"\nUnified server running on http://127.0.0.1:45679\n")
            .unwrap();
        log.write_all(&[b'x'; (DISCOVERY_READ_BYTES * 2) as usize])
            .unwrap();
        assert!(announced_address(&current_log_tail(&path, stale.len() as u64).unwrap()).is_none());
        let expected = Some("127.0.0.1:45679".parse().unwrap());
        let mut found = None;
        for _ in 0..8 {
            found = discovery.read(&path).unwrap();
            if found.is_some() {
                break;
            }
        }
        assert_eq!(found, expected);
        log.write_all(b"\nUnified server running on http://127.0.0.1:45680\n")
            .unwrap();
        assert_eq!(discovery.read(&path).unwrap(), expected);

        let mut split = ListenerDiscovery::new(log.metadata().unwrap().len());
        log.write_all(b"Unified server running on http://127.0.0.1:456")
            .unwrap();
        assert_eq!(split.read(&path).unwrap(), None);
        log.write_all(b"81\n").unwrap();
        assert_eq!(
            split.read(&path).unwrap(),
            Some("127.0.0.1:45681".parse().unwrap())
        );
    }
}
