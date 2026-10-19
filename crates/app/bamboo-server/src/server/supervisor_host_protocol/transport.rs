//! The opt-in stdio channel is only a request transport. Bamboo's own provider
//! confirms every intent; pipes, launch IDs and parent identity grant nothing.
use super::{
    adapter::{Adapter, Connection},
    canonical::{CanonicalScope, ScopeStore},
    provider::{ConfirmationProvider, NativeConfirmation},
    state::{FrameBuffer, FRAME_PREFIX, MAX_FRAME_BYTES, MAX_REQUESTS_PER_LAUNCH},
    wire::{self, Request, Response, VERSION},
};
use crate::app_state::AppState;
use actix_web::web;
use std::{
    io::{self, Read, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

struct Input {
    request: Request,
    busy_on_arrival: bool,
}
struct Output {
    bytes: Vec<u8>,
    ack: oneshot::Sender<io::Result<()>>,
}
struct FrameWriter {
    queue: std::sync::mpsc::SyncSender<Output>,
    connection: Connection,
}
impl FrameWriter {
    async fn send(&self, response: &Response) -> io::Result<()> {
        let payload = serde_json::to_vec(response)?;
        let bytes = [FRAME_PREFIX, payload.as_slice(), b"\n"].concat();
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(io::Error::other("native response too large"));
        }
        let (ack, completion) = oneshot::channel();
        self.queue
            .try_send(Output { bytes, ack })
            .map_err(|_| io::Error::other("native output unavailable"))?;
        tokio::select! {biased;
            _=self.connection.closed()=>Err(io::Error::other("native channel closed")),
            result=tokio::time::timeout(Duration::from_secs(5),completion)=>result.map_err(|_|io::Error::other("native output timed out"))?.map_err(|_|io::Error::other("native output closed"))?,
        }
    }
}
fn writer<W: Write + Send + 'static>(
    mut out: W,
    connection: Connection,
) -> io::Result<FrameWriter> {
    let (queue, receiver) = std::sync::mpsc::sync_channel::<Output>(1);
    let link = connection.clone();
    std::thread::Builder::new()
        .name("bamboo-native-scope-output".into())
        .spawn(move || {
            for message in receiver {
                let result = if link.is_open() {
                    out.write_all(&message.bytes).and_then(|_| out.flush())
                } else {
                    Err(io::Error::other("native channel closed"))
                };
                let failed = result.is_err();
                let _ = message.ack.send(result);
                if failed {
                    link.close();
                    break;
                }
            }
        })?;
    Ok(FrameWriter { queue, connection })
}
fn reader<R: Read + Send + 'static>(
    mut input: R,
    connection: Connection,
    inflight: Arc<AtomicBool>,
) -> io::Result<mpsc::Receiver<Input>> {
    let (sender, receiver) = mpsc::channel(2);
    std::thread::Builder::new()
        .name("bamboo-native-scope-input".into())
        .spawn(move || {
            let mut frame = FrameBuffer::default();
            let mut bytes = [0_u8; 4096];
            'read: loop {
                let count = match input.read(&mut bytes) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => count,
                };
                for byte in &bytes[..count] {
                    let payload = match frame.push(*byte) {
                        Ok(None) => continue,
                        Ok(Some(payload)) => payload,
                        Err(_) => break 'read,
                    };
                    let request = match wire::decode(&payload) {
                        Ok(request) => request,
                        Err(_) => break 'read,
                    };
                    let busy_on_arrival = matches!(&request, Request::Apply { .. })
                        && inflight.swap(true, Ordering::SeqCst);
                    if sender
                        .try_send(Input {
                            request,
                            busy_on_arrival,
                        })
                        .is_err()
                    {
                        break 'read;
                    }
                }
            }
            let _ = frame.eof();
            connection.close();
        })?;
    Ok(receiver)
}

pub(in crate::server) struct NativeChannel {
    connection: Connection,
    task: tokio::task::JoinHandle<()>,
}
impl NativeChannel {
    pub(in crate::server) fn start(state: web::Data<AppState>) -> io::Result<Self> {
        let connection = Connection::new();
        let link = connection.clone();
        let task = tokio::spawn(async move {
            let _ = serve(
                std::io::stdin(),
                std::io::stdout(),
                CanonicalScope(state),
                NativeConfirmation,
                link.clone(),
            )
            .await;
            link.close();
        });
        Ok(Self { connection, task })
    }
    pub(in crate::server) async fn shutdown(self) {
        self.connection.close();
        let _ = self.task.await;
    }
}

async fn serve<
    R: Read + Send + 'static,
    W: Write + Send + 'static,
    S: ScopeStore,
    P: ConfirmationProvider,
>(
    input: R,
    output: W,
    store: S,
    provider: P,
    connection: Connection,
) -> io::Result<()> {
    let launch_id = Uuid::new_v4();
    let mut adapter = Adapter::new(launch_id, store, provider)
        .map_err(|_| io::Error::other("native launch unavailable"))?;
    let output = writer(output, connection.clone())?;
    output
        .send(&Response::Hello {
            version: VERSION,
            launch_id,
            max_frame_bytes: MAX_FRAME_BYTES,
            max_requests_per_launch: MAX_REQUESTS_PER_LAUNCH,
            owner_authorization_available: false,
            authority_profile: adapter.profile(),
        })
        .await?;
    let inflight = Arc::new(AtomicBool::new(false));
    let mut input = reader(input, connection.clone(), inflight.clone())?;
    loop {
        let next = tokio::select! {biased;_=connection.closed()=>None,frame=input.recv()=>frame};
        let Some(next) = next else {
            break;
        };
        match next.request {
            Request::HelloAck {
                launch_id: observed,
            } => {
                if adapter.acknowledge(observed).is_err() {
                    break;
                }
                output
                    .send(&Response::Ready {
                        version: VERSION,
                        launch_id,
                        transport_ready: adapter.transport_ready(),
                        owner_authorization_available: false,
                        authority_profile: adapter.profile(),
                    })
                    .await?;
            }
            Request::Apply {
                launch_id: observed,
                request_id,
                intent,
            } => {
                let Some(processed) = adapter
                    .apply(
                        observed,
                        request_id,
                        intent,
                        next.busy_on_arrival,
                        &connection,
                    )
                    .await
                else {
                    break;
                };
                if !connection.is_open() {
                    break;
                }
                output
                    .send(&Response::Result {
                        version: VERSION,
                        launch_id,
                        request_id,
                        outcome: processed.outcome.clone(),
                    })
                    .await?;
                if adapter.flushed(processed).is_err() {
                    break;
                }
                if !next.busy_on_arrival {
                    inflight.store(false, Ordering::SeqCst);
                }
            }
        }
    }
    let _ = adapter.disconnect();
    connection.close();
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::{
        canonical::{Refused, ScopeContext},
        confirmation::{ConfirmationRequest, Decision},
        state::Submission,
        wire::{AuthorityProfile, Outcome},
    };
    use super::*;
    use async_trait::async_trait;
    use bamboo_domain::SupervisorManagementRequest;
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::AtomicUsize;
    struct NoStore(Arc<AtomicUsize>);
    #[async_trait]
    impl ScopeStore for NoStore {
        async fn prepare(&self, _: &SupervisorManagementRequest) -> Result<ScopeContext, Refused> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Refused::new("supervisor_missing", 404))
        }
        async fn apply(&self, _: &Submission<SupervisorManagementRequest>) -> Outcome {
            panic!("no approval or storage submission")
        }
    }
    struct NoProvider;
    #[async_trait]
    impl ConfirmationProvider for NoProvider {
        fn profile(&self) -> AuthorityProfile {
            AuthorityProfile::new(Some("native_confirmation_no_gui"))
        }
        async fn confirm(&self, _: &ConfirmationRequest) -> Decision {
            panic!("no native window")
        }
    }
    fn read_frame(reader: &mut BufReader<UnixStream>) -> serde_json::Value {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with(std::str::from_utf8(FRAME_PREFIX).unwrap()));
        serde_json::from_str(line.trim_start_matches(std::str::from_utf8(FRAME_PREFIX).unwrap()))
            .unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn actual_pipe_handshake_never_grants_or_bootstraps_and_eof_stops_channel() {
        let (mut client_input, input) = UnixStream::pair().unwrap();
        let (output, client_output) = UnixStream::pair().unwrap();
        client_output
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let link = Connection::new();
        let end = link.clone();
        let worker = tokio::spawn(serve(
            input,
            output,
            NoStore(calls.clone()),
            NoProvider,
            link,
        ));
        let client = tokio::task::spawn_blocking(move || {
            let mut out = BufReader::new(client_output);
            let hello = read_frame(&mut out);
            assert_eq!(hello["authority_profile"]["confirmation_available"], false);
            let launch = hello["launch_id"].clone();
            let send = |stream: &mut UnixStream, value: serde_json::Value| {
                let bytes = [
                    FRAME_PREFIX,
                    serde_json::to_vec(&value).unwrap().as_slice(),
                    b"\n",
                ]
                .concat();
                stream.write_all(&bytes).unwrap();
            };
            send(
                &mut client_input,
                serde_json::json!({"kind":"hello_ack","version":1,"launch_id":launch}),
            );
            let ready = read_frame(&mut out);
            assert_eq!(ready["transport_ready"], true);
            assert_eq!(ready["owner_authorization_available"], false);
            send(
                &mut client_input,
                serde_json::json!({"kind":"apply","version":1,"launch_id":launch,"request_id":Uuid::new_v4(),"intent":{"supervisor":{"session_id":"bamboo-default-supervisor","incarnation_id":Uuid::new_v4()},"expected_state_revision":0,"mutation":{"kind":"configure_project_scope","allowed_projects":[]}}}),
            );
            let refused = read_frame(&mut out);
            assert_eq!(refused["outcome"]["code"], "supervisor_missing");
            drop(client_input);
        });
        client.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!end.is_open());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
