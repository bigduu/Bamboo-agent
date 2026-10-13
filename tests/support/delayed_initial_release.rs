//! Fixture-only real broker and transparent proxy for one initial-input request.
//! Receipts and unrelated frames are always forwarded unchanged.
use bamboo_broker::{BrokerCore, BrokerFrame, BrokerServer, ClientFrame};
use bamboo_subagent::proto::{
    InitialInputControl, InitialInputRelease, InitialInputReleaseRequest,
};
use bamboo_subagent::{InboxKind, MsgId, RunSpec};
use futures::{SinkExt, StreamExt};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

const TOKEN: &str = "fixture-initial-release";

#[derive(Default)]
struct Observations {
    requests: Vec<InitialInputReleaseRequest>,
    releases: Vec<InitialInputRelease>,
    runs: Vec<(MsgId, RunSpec)>,
    delayed_identity: Option<(MsgId, InitialInputReleaseRequest)>,
}

fn request_matches_run(
    seen: &Observations,
    correlation: &MsgId,
    request: &InitialInputReleaseRequest,
) -> bool {
    seen.runs.iter().any(|(id, run)| {
        id == correlation
            && run.activation_run_id.as_deref() == Some(request.activation_run_id.as_str())
            && run.execution_epoch == request.execution_epoch
            && run
                .logical_session
                .as_ref()
                .is_some_and(|identity| identity.session_id == request.child_id)
            && run.initial_session_messages.iter().any(|delivery| {
                delivery.envelope.id.as_str() == request.envelope_id
                    && delivery.canonical_claim_generation == request.generation
            })
    })
}

pub struct ReleaseProxy {
    pub endpoint: String,
    observations: Arc<Mutex<Observations>>,
    held: Arc<AtomicBool>,
    sent: Arc<AtomicBool>,
    release: watch::Sender<bool>,
    proxy: JoinHandle<()>,
    broker: JoinHandle<()>,
}
impl ReleaseProxy {
    pub async fn start(root: &Path, hold_first_request: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("ws://{}", listener.local_addr().unwrap());
        let broker = Arc::new(BrokerServer::new(Arc::new(BrokerCore::new(root)), TOKEN));
        let broker = tokio::spawn(async move {
            broker.serve(listener).await.unwrap();
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let observations = Arc::new(Mutex::new(Observations::default()));
        let held = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicBool::new(false));
        let (release, watch) = watch::channel(false);
        let captured = observations.clone();
        let hold = held.clone();
        let delivered = sent.clone();
        let proxy = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (socket,_)=accepted.unwrap();
                        let upstream=upstream.clone();
                        let observations=captured.clone();
                        let held=hold.clone();
                        let sent=delivered.clone();
                        let mut release=watch.clone();
                        connections.spawn(async move {
                            // Reachability probes open TCP without a WS handshake.
                            let Ok(mut downstream)=accept_async(socket).await else{return;};
                            let Ok((mut upstream,_))=connect_async(upstream).await else{return;};
                            let mut delayed:Vec<Message>=Vec::new();
                            loop {
                                tokio::select! {
                                    biased;
                                    changed=release.changed(),if !delayed.is_empty()=>{
                                        if changed.is_err(){break;}
                                        if *release.borrow_and_update() {
                                            // Preserve every original frame, including retransmissions.
                                            for frame in delayed.drain(..) {
                                                if downstream.send(frame).await.is_err(){return;}
                                                sent.store(true,Ordering::SeqCst);
                                            }
                                        }
                                    }
                                    frame=downstream.next()=>{
                                        let Some(Ok(frame))=frame else{break;};
                                        if let Message::Text(text)=&frame {
                                            if let Ok(ClientFrame::Deliver{message,..})=serde_json::from_str(text) {
                                                match message.kind {
                                                    InboxKind::Run=>{
                                                        if let Ok(run)=serde_json::from_value::<RunSpec>(message.body) {
                                                            let mut seen=observations.lock().unwrap();
                                                            if seen.runs.len()<8 && !seen.runs.iter().any(|(id,_)|id==&message.id) {
                                                                seen.runs.push((message.id,run));
                                                            }
                                                        }
                                                    }
                                                    InboxKind::Steer=>{
                                                        if let (Some(correlation),Ok(InitialInputControl::Release{release}))=
                                                            (message.correlation_id,InitialInputControl::decode(message.body)) {
                                                            let mut seen=observations.lock().unwrap();
                                                            if request_matches_run(&seen,&correlation,&release.request)
                                                                && seen.releases.len()<8 && !seen.releases.contains(&release) {
                                                                seen.releases.push(release);
                                                            }
                                                        }
                                                    }
                                                    _=>{}
                                                }
                                            }
                                        }
                                        if upstream.send(frame).await.is_err(){break;}
                                    }
                                    frame=upstream.next()=>{
                                        let Some(Ok(frame))=frame else{break;};
                                        let request=if let Message::Text(text)=&frame {
                                            match serde_json::from_str::<BrokerFrame>(text) {
                                                Ok(BrokerFrame::Message{message}) if message.kind==InboxKind::SessionMessageAdmitted=>
                                                    match (message.correlation_id,InitialInputControl::decode(message.body)) {
                                                        (Some(correlation),Ok(InitialInputControl::Request{request}))=>Some((correlation,request)),
                                                        _=>None,
                                                    },
                                                _=>None,
                                            }
                                        } else {None};
                                        let should_hold=if let Some((correlation,request))=request {
                                            let mut seen=observations.lock().unwrap();
                                            if request_matches_run(&seen,&correlation,&request) {
                                                if seen.requests.len()<8 && !seen.requests.contains(&request) {seen.requests.push(request.clone());}
                                                if hold_first_request && !*release.borrow() {
                                                    let exact=seen.delayed_identity.get_or_insert_with(||(correlation.clone(),request.clone()));
                                                    *exact==(correlation,request)
                                                } else {false}
                                            } else {false}
                                        } else {false};
                                        if should_hold {
                                            assert!(delayed.len()<16,"bounded 60-second initial-release retransmissions");
                                            delayed.push(frame);
                                            held.store(true,Ordering::SeqCst);
                                            continue;
                                        }
                                        if downstream.send(frame).await.is_err(){break;}
                                    }
                                }
                            }
                        });
                    }
                    _=connections.join_next(),if !connections.is_empty()=>{}
                }
            }
        });
        Self {
            endpoint,
            observations,
            held,
            sent,
            release,
            proxy,
            broker,
        }
    }
    pub fn configure_host(&self, data: &Path) {
        std::fs::write(
            data.join("broker.json"),
            serde_json::to_vec(&serde_json::json!({"endpoint":self.endpoint,"token":TOKEN}))
                .unwrap(),
        )
        .unwrap();
    }
    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }
    pub fn was_released(&self) -> bool {
        self.sent.load(Ordering::SeqCst)
    }
    pub fn requests(&self) -> Vec<InitialInputReleaseRequest> {
        self.observations.lock().unwrap().requests.clone()
    }
    pub fn releases(&self) -> Vec<InitialInputRelease> {
        self.observations.lock().unwrap().releases.clone()
    }
    pub fn release(&self) {
        self.release.send_replace(true);
    }
}
impl Drop for ReleaseProxy {
    fn drop(&mut self) {
        self.proxy.abort();
        self.broker.abort();
    }
}
