//! Cooperative shutdown for a desktop host's own Bamboo process.

use actix_server::Server;
use actix_web::web;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

use crate::app_state::planned_update::{HostUpdatePreparation, HostUpdateReadiness};
use crate::app_state::AppState;

use super::planned_update_handoff::{self, PublishedHandoff, UpdateResumeReport};

/// Private, per-process directory supplied by a host using `serve --parent-pid`.
/// Bamboo creates `ready`; the owner requests shutdown by creating an empty
/// `stop` file and then waits for Bamboo to exit.
pub const MANAGED_SHUTDOWN_DIR_ENV: &str = "BAMBOO_MANAGED_SHUTDOWN_DIR";

const UPDATE_CAPABILITY: &str = "planned-update-v1";
const UPDATE_COMMAND: &str = "planned-update-command.json";
const UPDATE_RESULT: &str = "planned-update-result.json";
const UPDATE_VERSION: u8 = 1;
const COMMAND_BYTES: u64 = 1024;
const PREPARE_LEASE: Duration = Duration::from_secs(120);
const CONTROL_POLL: Duration = Duration::from_millis(100);

pub(super) struct ManagedShutdown {
    directory: PathBuf,
}

/// Keeps the committed Host sealed until the entrypoint drains HTTP and finishes
/// cleanup. An interrupted drain leaves the intent without authorization.
pub(super) struct ManagedShutdownCommit {
    _host: HostUpdatePreparation,
    handoff: PublishedHandoff,
    directory: PathBuf,
    result: UpdateResult,
}

impl ManagedShutdownCommit {
    pub(super) fn finalize(self) -> io::Result<()> {
        self.handoff.authorize()?;
        if let Err(error) = publish_result(&self.directory, &self.result) {
            tracing::warn!(
                %error,
                request = %self.result.request_id,
                "Cannot publish the shutdown reply after authorizing the planned update"
            );
        }
        Ok(())
    }
}

impl ManagedShutdown {
    pub(super) fn new(directory: PathBuf) -> io::Result<Self> {
        if !directory.is_absolute() || !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed shutdown requires an absolute, private directory",
            ));
        }
        for name in [
            "ready",
            "stop",
            UPDATE_CAPABILITY,
            UPDATE_COMMAND,
            UPDATE_RESULT,
        ] {
            match fs::symlink_metadata(directory.join(name)) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error),
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "managed shutdown directory already contains a marker",
                    ));
                }
            }
        }
        Ok(Self { directory })
    }

    pub(super) async fn run(
        self,
        server: Server,
        state: Option<web::Data<AppState>>,
        committed: &mut Option<ManagedShutdownCommit>,
    ) -> io::Result<()> {
        if state.is_some() {
            File::create_new(self.directory.join(UPDATE_CAPABILITY))?;
        }
        File::create_new(self.directory.join("ready"))?;
        let handle = server.handle();
        tokio::pin!(server);

        tokio::select! {
            result = &mut server => result,
            update = await_shutdown(&self.directory, state.as_ref().map(web::Data::get_ref)) => {
                *committed = update;
                tracing::info!("Managed Bamboo shutdown requested");
                let ((), result) = tokio::join!(handle.stop(true), server);
                result
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateCommand {
    version: u8,
    request_id: Uuid,
    sequence: u64,
    command: UpdateCommandKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum UpdateCommandKind {
    Prepare,
    Abort,
    Commit,
    Resume,
}

#[derive(Serialize)]
struct UpdateResult {
    version: u8,
    request_id: Uuid,
    sequence: u64,
    #[serde(flatten)]
    status: UpdateStatus,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum UpdateStatus {
    Busy {
        pending_sessions: Vec<String>,
        message: String,
    },
    Ready,
    Revoked,
    Aborted,
    Committing {
        pending_sessions: Vec<String>,
        message: String,
    },
    Committed,
    Resumed {
        report: UpdateResumeReport,
    },
    Error {
        message: String,
    },
}

impl UpdateStatus {
    fn busy(message: impl Into<String>) -> Self {
        Self::Busy {
            pending_sessions: Vec::new(),
            message: message.into(),
        }
    }

    fn committing(message: impl Into<String>) -> Self {
        Self::Committing {
            pending_sessions: Vec::new(),
            message: message.into(),
        }
    }
}

enum UpdatePhase {
    Idle,
    Preparing {
        request: Uuid,
        host: HostUpdatePreparation,
        deadline: Instant,
    },
    Committing {
        request: Uuid,
        host: HostUpdatePreparation,
        handoff: Option<PublishedHandoff>,
    },
}

struct UpdateProtocol<'a> {
    state: &'a AppState,
    phase: UpdatePhase,
    revoked: Option<Uuid>,
}

impl UpdateProtocol<'_> {
    fn expire_preparation(&mut self) {
        if let UpdatePhase::Preparing {
            request, deadline, ..
        } = &self.phase
        {
            if Instant::now() >= *deadline {
                self.revoked = Some(*request);
                self.phase = UpdatePhase::Idle;
            }
        }
    }

    async fn process(&mut self, request: Uuid, command: UpdateCommandKind) -> UpdateStatus {
        let result = match command {
            UpdateCommandKind::Prepare => self.prepare(request).await,
            UpdateCommandKind::Abort => self.abort(request),
            UpdateCommandKind::Commit => self.commit(request).await,
            UpdateCommandKind::Resume => self.resume(request).await,
        };
        match result {
            Ok(status) => status,
            Err(error) => UpdateStatus::Error {
                message: error.to_string(),
            },
        }
    }

    async fn prepare(&mut self, request: Uuid) -> io::Result<UpdateStatus> {
        if self.revoked == Some(request) {
            return Ok(UpdateStatus::Revoked);
        }
        if matches!(self.phase, UpdatePhase::Idle) {
            if !self.state.supports_managed_planned_update() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "The running Host cannot prepare a planned update",
                ));
            }
            let host = match self.state.begin_planned_update().await {
                Ok(host) => host,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(UpdateStatus::busy(error.to_string()));
                }
                Err(error) => return Err(error),
            };
            self.phase = UpdatePhase::Preparing {
                request,
                host,
                deadline: Instant::now() + PREPARE_LEASE,
            };
        }
        match &mut self.phase {
            UpdatePhase::Preparing {
                request: owner,
                host,
                deadline,
            } if request == *owner => {
                *deadline = Instant::now() + PREPARE_LEASE;
                match host.observe().await {
                    Ok(HostUpdateReadiness::Ready) => Ok(UpdateStatus::Ready),
                    Ok(HostUpdateReadiness::Busy {
                        pending_sessions,
                        message,
                    }) => Ok(UpdateStatus::Busy {
                        pending_sessions,
                        message,
                    }),
                    Err(error) => {
                        self.revoked = Some(request);
                        self.phase = UpdatePhase::Idle;
                        Err(error)
                    }
                }
            }
            UpdatePhase::Committing { request: owner, .. } if request == *owner => Ok(
                UpdateStatus::committing("Waiting for the committed update to close"),
            ),
            _ => Ok(UpdateStatus::busy(
                "Another update owns the running process",
            )),
        }
    }

    fn abort(&mut self, request: Uuid) -> io::Result<UpdateStatus> {
        match &self.phase {
            UpdatePhase::Preparing { request: owner, .. } if *owner == request => {
                self.revoked = Some(request);
                self.phase = UpdatePhase::Idle;
                Ok(UpdateStatus::Aborted)
            }
            UpdatePhase::Idle => {
                self.revoked = Some(request);
                Ok(UpdateStatus::Aborted)
            }
            UpdatePhase::Committing { request: owner, .. } if *owner == request => {
                Err(io::Error::other("The update already committed"))
            }
            _ => Err(io::Error::other("Another update owns the running process")),
        }
    }

    async fn commit(&mut self, request: Uuid) -> io::Result<UpdateStatus> {
        if let UpdatePhase::Preparing {
            request: owner,
            host,
            ..
        } = &mut self.phase
        {
            if *owner != request {
                return Err(io::Error::other("Another update owns the running process"));
            }
            let mut handoff = None;
            let profile = &self.state.app_data_dir;
            let result = host
                .commit(|roots| {
                    handoff = Some(planned_update_handoff::publish(profile, request, roots)?);
                    Ok(())
                })
                .await;
            let error = match result {
                Ok(()) => None,
                Err(error) if handoff.is_some() => Some(error),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(UpdateStatus::busy(error.to_string()));
                }
                Err(error) => {
                    self.revoked = Some(request);
                    self.phase = UpdatePhase::Idle;
                    return Err(error);
                }
            };
            let old = std::mem::replace(&mut self.phase, UpdatePhase::Idle);
            let UpdatePhase::Preparing { host, .. } = old else {
                unreachable!("the update was preparing when its intent was published");
            };
            self.phase = UpdatePhase::Committing {
                request,
                host,
                handoff,
            };
            if let Some(error) = error {
                tracing::warn!(%error, %request, "Cannot confirm the committed update yet");
                return Ok(UpdateStatus::committing(error.to_string()));
            }
        }

        match &mut self.phase {
            UpdatePhase::Committing {
                request: owner,
                host,
                handoff,
            } if request == *owner => match host.observe_committed().await {
                Ok(HostUpdateReadiness::Ready) if handoff.is_some() => Ok(UpdateStatus::Committed),
                Ok(HostUpdateReadiness::Ready) => Err(io::Error::other(
                    "The committed update intent is unavailable",
                )),
                Ok(HostUpdateReadiness::Busy {
                    pending_sessions,
                    message,
                }) => Ok(UpdateStatus::Committing {
                    pending_sessions,
                    message,
                }),
                Err(error) => Ok(UpdateStatus::committing(error.to_string())),
            },
            UpdatePhase::Idle => Ok(UpdateStatus::Revoked),
            _ => Err(io::Error::other("Another update owns the running process")),
        }
    }

    async fn resume(&self, request: Uuid) -> io::Result<UpdateStatus> {
        if !matches!(self.phase, UpdatePhase::Idle) {
            return Err(io::Error::other("The process is preparing an update"));
        }
        Ok(UpdateStatus::Resumed {
            report: planned_update_handoff::resume(self.state, request).await?,
        })
    }
}

fn read_command(path: &Path) -> io::Result<UpdateCommand> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > COMMAND_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid desktop update command",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(COMMAND_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > COMMAND_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Desktop update command is too large",
        ));
    }
    let request: UpdateCommand = serde_json::from_slice(&bytes)?;
    if request.version != UPDATE_VERSION || request.request_id.is_nil() || request.sequence == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid desktop update command identity",
        ));
    }
    Ok(request)
}

fn publish_result(directory: &Path, result: &UpdateResult) -> io::Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    serde_json::to_writer(file.as_file_mut(), result)?;
    file.persist(directory.join(UPDATE_RESULT))?;
    Ok(())
}

async fn await_shutdown(
    directory: &Path,
    state: Option<&AppState>,
) -> Option<ManagedShutdownCommit> {
    let stop = directory.join("stop");
    let Some(state) = state else {
        while !stop_requested(&stop) {
            tokio::time::sleep(CONTROL_POLL).await;
        }
        return None;
    };

    let mut protocol = UpdateProtocol {
        state,
        phase: UpdatePhase::Idle,
        revoked: None,
    };
    let command_path = directory.join(UPDATE_COMMAND);
    let mut sequence = 0;
    let mut pending = None;
    loop {
        protocol.expire_preparation();
        if !matches!(protocol.phase, UpdatePhase::Committing { .. }) && stop_requested(&stop) {
            return None;
        }
        if pending.is_none() {
            if let Ok(command) = read_command(&command_path) {
                if command.sequence > sequence {
                    sequence = command.sequence;
                    let result = UpdateResult {
                        version: UPDATE_VERSION,
                        request_id: command.request_id,
                        sequence,
                        status: protocol.process(command.request_id, command.command).await,
                    };
                    if matches!(result.status, UpdateStatus::Committed) {
                        let UpdatePhase::Committing {
                            host,
                            handoff: Some(handoff),
                            ..
                        } = protocol.phase
                        else {
                            unreachable!("a committed reply belongs to a committing Host");
                        };
                        return Some(ManagedShutdownCommit {
                            _host: host,
                            handoff,
                            directory: directory.to_path_buf(),
                            result,
                        });
                    }
                    pending = Some(result);
                }
            }
        }
        if let Some(result) = &mut pending {
            if protocol.revoked == Some(result.request_id)
                && matches!(
                    result.status,
                    UpdateStatus::Ready | UpdateStatus::Busy { .. }
                )
            {
                result.status = UpdateStatus::Revoked;
            }
            match publish_result(directory, result) {
                Ok(()) => pending = None,
                Err(error) => tracing::debug!(%error, "Cannot publish desktop update result yet"),
            }
        }
        tokio::time::sleep(CONTROL_POLL).await;
    }
}

fn stop_requested(marker: &Path) -> bool {
    fs::symlink_metadata(marker).is_ok_and(|metadata| metadata.is_file() && metadata.len() == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{web, App, HttpResponse, HttpServer};
    use std::net::TcpListener;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[test]
    fn refuses_to_reuse_an_existing_marker() {
        let directory = tempfile::tempdir().unwrap();
        File::create_new(directory.path().join("stop")).unwrap();
        assert!(ManagedShutdown::new(directory.path().to_path_buf()).is_err());
    }

    #[test]
    fn validates_bounded_versioned_update_commands() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(UPDATE_COMMAND);
        let request_id = Uuid::new_v4();
        let mut command = serde_json::json!({
            "version": UPDATE_VERSION,
            "request_id": request_id,
            "sequence": 1,
            "command": "prepare",
        });
        fs::write(&path, command.to_string()).unwrap();
        let accepted = read_command(&path).unwrap();
        assert_eq!(accepted.request_id, request_id);
        assert_eq!(accepted.sequence, 1);
        assert!(matches!(accepted.command, UpdateCommandKind::Prepare));

        command["sequence"] = serde_json::json!(0);
        fs::write(&path, command.to_string()).unwrap();
        assert!(read_command(&path).is_err());

        fs::write(&path, vec![b' '; COMMAND_BYTES as usize + 1]).unwrap();
        assert!(read_command(&path).is_err());
    }

    #[actix_web::test]
    async fn stop_marker_allows_a_serving_request_to_finish() {
        let directory = tempfile::tempdir().unwrap();
        let shutdown = ManagedShutdown::new(directory.path().to_path_buf()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let in_handler = entered.clone();
        let allow_finish = finish.clone();
        let server = HttpServer::new(move || {
            let entered = in_handler.clone();
            let finish = allow_finish.clone();
            App::new().route(
                "/",
                web::get().to(move || {
                    let entered = entered.clone();
                    let finish = finish.clone();
                    async move {
                        entered.notify_one();
                        finish.notified().await;
                        HttpResponse::Ok().body("finished")
                    }
                }),
            )
        })
        .disable_signals()
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let stopping = actix_web::rt::spawn(async move {
            let mut committed = None;
            shutdown.run(server, None, &mut committed).await
        });
        let request = actix_web::rt::spawn(async move {
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!("http://{address}/"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(directory.path().join("ready").is_file());
        File::create_new(directory.path().join("stop")).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::TcpStream::connect(address).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!stopping.is_finished());
        finish.notify_one();
        assert_eq!(request.await.unwrap(), "finished");
        tokio::time::timeout(Duration::from_secs(5), stopping)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
