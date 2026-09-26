//! Real default APIs and physical filesystem publication barriers.
use std::sync::{Condvar, Mutex as StdMutex};

use super::*;
use bamboo_domain::session::provider_transcript::{
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin,
};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryEntry, ActorDirectoryPort,
    CompressionEvent, CompressionTriggerType, ConversationSummary, Message,
    SessionAuthorityConflict, SessionInboxAdmissionState,
};
use chrono::Duration as ChronoDuration;

const ID: &str = "default-context-fence";
const DEADLINE: Duration = Duration::from_secs(10);
const RECORD: &str = "actor-authority.json";
const MARKER: &str = "actor-authority.initialized.json";

fn claim(id: &str) -> ActorActivationClaim {
    let now = Utc::now();
    ActorActivationClaim {
        actor_id: id.into(),
        run_id: "run".into(),
        lease_owner: "owner".into(),
        lease_expires_at: now + ChronoDuration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}

fn native(session: &mut Session, text: &str) {
    session.provider_transcript = Default::default();
    session
        .activate_provider_transcript_route(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            &"a".repeat(64),
        )
        .unwrap();
    // Native groups require a complete discovery pair, not a standalone text item.
    let items = [
        (ProviderTranscriptAuthor::Model, serde_json::json!({"type":"text","text":text})),
        (ProviderTranscriptAuthor::Model, serde_json::json!({
            "type":"server_tool_use", "id":"search-id", "name":"tool_search_tool_regex", "input":{"pattern":"weather"}
        })),
        (ProviderTranscriptAuthor::ToolResult, serde_json::json!({
            "type":"tool_search_tool_result", "tool_use_id":"search-id", "content":{
                "type":"tool_search_tool_search_result", "tool_references":[{"type":"tool_reference","tool_name":"get_weather"}]
            }
        })),
    ].into_iter().map(|(author, payload)| {
        ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Anthropic, ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider, author, payload,
        ).unwrap()
    }).collect();
    session
        .append_provider_transcript_group(
            session.messages[0].id.clone(),
            Some("native-group"),
            items,
        )
        .unwrap();
}

fn context() -> Session {
    let mut session = Session::new(ID, "model");
    session.add_message(Message::user("original"));
    session.add_message(Message::tool_result("tool-id", "tool-result"));
    session.conversation_summary = Some(ConversationSummary::new("summary", 2, 10));
    session.compression_events.push(CompressionEvent::new(
        1,
        1,
        80.0,
        40.0,
        10,
        CompressionTriggerType::Auto,
        0.5,
        None,
        0,
    ));
    session.model_context_state = Some(Default::default());
    session
        .runtime_metadata
        .get_or_insert_with(Default::default)
        .session_inbox_admission = Some(SessionInboxAdmissionState::default());
    native(&mut session, "native-original");
    session
}

fn snapshot(directory: &Path) -> Vec<Option<Vec<u8>>> {
    ["session.json", RUNTIME_SIDECAR_FILE, RECORD, MARKER]
        .map(|f| std::fs::read(directory.join(f)).ok())
        .into()
}

fn rejected(error: io::Error) {
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(error
        .get_ref()
        .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
    assert!(error
        .to_string()
        .contains("Unfenced Actor context mutation"));
    assert!(!error.to_string().starts_with("Task control-plane"));
}

#[tokio::test]
async fn activated_finished_failed_cancelled_retired_protect_all_published_context() {
    for status in ["running", "finished", "failed", "cancelled", "retired"] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let base = context();
        store.save_session(&base).await.unwrap();
        let activation = store.claim_activation(&claim(ID)).await.unwrap();
        store
            .start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
        match status {
            "finished" => {
                store
                    .finish_activation(
                        &activation.fence(),
                        Utc::now(),
                        ActorActivationFinish::Succeeded,
                    )
                    .await
                    .unwrap();
            }
            "failed" => {
                store
                    .finish_activation(
                        &activation.fence(),
                        Utc::now(),
                        ActorActivationFinish::Failed,
                    )
                    .await
                    .unwrap();
            }
            "cancelled" => {
                store
                    .finish_activation(
                        &activation.fence(),
                        Utc::now(),
                        ActorActivationFinish::Cancelled,
                    )
                    .await
                    .unwrap();
            }
            "retired" => {
                store.retire_actor(ID, Utc::now()).await.unwrap();
            }
            _ => {}
        }
        let directory = home.path().join("sessions").join(ID);
        let original = snapshot(&directory);
        let mut changes = Vec::new();
        let mut s = base.clone();
        s.messages[0].content = "changed same id".into();
        changes.push((true, s));
        let mut s = base.clone();
        s.messages.swap(0, 1);
        changes.push((true, s));
        let mut s = base.clone();
        s.messages[0].role = Role::Assistant;
        changes.push((true, s));
        let mut s = base.clone();
        s.messages[1].content = "changed tool payload".into();
        changes.push((true, s));
        let mut s = base.clone();
        native(&mut s, "changed native payload");
        changes.push((true, s));
        let mut s = base.clone();
        s.runtime_metadata
            .as_mut()
            .unwrap()
            .session_inbox_admission
            .as_mut()
            .unwrap()
            .last_admitted_sequence = 1;
        changes.push((true, s));
        let mut s = base.clone();
        s.conversation_summary.as_mut().unwrap().content = "changed summary".into();
        changes.push((false, s));
        let mut s = base.clone();
        s.compression_events[0].messages_compressed += 1;
        changes.push((false, s));
        let mut s = base.clone();
        s.model_context_state.as_mut().unwrap().state_revision += 1;
        changes.push((false, s));
        for (main_only, incoming) in changes {
            rejected(store.save_session(&incoming).await.unwrap_err());
            if !main_only {
                rejected(store.save_runtime_state(&incoming).await.unwrap_err());
            }
            assert_eq!(
                snapshot(&directory),
                original,
                "rejection changed durable bytes: {status}"
            );
        }
        // Runtime-only discards these fields. It must compare its real projection.
        let mut ignored = base.clone();
        ignored.messages.clear();
        ignored.provider_transcript = Default::default();
        ignored
            .runtime_metadata
            .as_mut()
            .unwrap()
            .session_inbox_admission = None;
        store.save_runtime_state(&ignored).await.unwrap();
        let loaded = store.load_session(ID).await.unwrap().unwrap();
        assert_eq!(loaded.messages[0].content, "original");
        assert_eq!(loaded.provider_transcript, base.provider_transcript);
        assert!(loaded
            .runtime_metadata
            .unwrap()
            .session_inbox_admission
            .is_some());
        let mut control = base.clone();
        control.title = "changed title".into();
        control.metadata_version += 1;
        store.save_session(&control).await.unwrap();
        assert_eq!(
            store.load_session(ID).await.unwrap().unwrap().title,
            "changed title"
        );
        let attachment = directory.join("attachments/sentinel");
        std::fs::write(&attachment, b"keep").unwrap();
        let before_clear = snapshot(&directory);
        rejected(store.clear_session(ID).await.unwrap_err());
        assert_eq!(std::fs::read(attachment).unwrap(), b"keep");
        assert_eq!(snapshot(&directory), before_clear);
    }
}

#[tokio::test]
async fn legacy_and_matching_inert_cold_allow_full_runtime_and_clear() {
    for cold in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut session = context();
        store.save_session(&session).await.unwrap();
        if cold {
            store.ensure_actor(ID).await.unwrap();
        }
        session.messages[0].content = "allowed".into();
        store.save_session(&session).await.unwrap();
        session.conversation_summary.as_mut().unwrap().content = "allowed summary".into();
        store.save_runtime_state(&session).await.unwrap();
        let directory = home.path().join("sessions").join(ID);
        std::fs::write(directory.join("attachments/sentinel"), b"remove").unwrap();
        assert!(store.clear_session(ID).await.unwrap());
        assert!(!directory.join("attachments/sentinel").exists());
        assert!(store
            .load_session(ID)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty());
    }
}

#[tokio::test]
async fn unknown_and_attempt_zero_retired_do_not_repair_or_authorize_context_deltas() {
    for row in [
        "marker-only",
        "record-only",
        "bad-record",
        "bad-marker",
        "record-birth",
        "marker-birth",
        "contradictory",
        "schema",
        "nonregular",
        "retired-zero",
    ] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let base = context();
        store.save_session(&base).await.unwrap();
        store.ensure_actor(ID).await.unwrap();
        let dir = home.path().join("sessions").join(ID);
        let record = dir.join(RECORD);
        let marker = dir.join(MARKER);
        match row {
            "marker-only" => std::fs::remove_file(&record).unwrap(),
            "record-only" => std::fs::remove_file(&marker).unwrap(),
            "bad-record" => std::fs::write(&record, b"{").unwrap(),
            "bad-marker" => std::fs::write(&marker, b"{").unwrap(),
            "nonregular" => {
                std::fs::remove_file(&record).unwrap();
                std::fs::create_dir(&record).unwrap();
            }
            "retired-zero" => {
                store.retire_actor(ID, Utc::now()).await.unwrap();
            }
            "marker-birth" => {
                let mut v: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
                v["session_created_at"] =
                    serde_json::to_value(Utc::now() + ChronoDuration::days(1)).unwrap();
                std::fs::write(&marker, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            _ => {
                let mut entry: ActorDirectoryEntry =
                    serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
                match row {
                    "record-birth" => entry.actor.session_created_at += ChronoDuration::days(1),
                    "contradictory" => entry.actor.current_attempt = 1,
                    "schema" => entry.schema_version += 1,
                    _ => unreachable!(),
                };
                std::fs::write(&record, serde_json::to_vec(&entry).unwrap()).unwrap();
            }
        }
        let before = snapshot(&dir);
        let mut incoming = base.clone();
        incoming.messages[0].content = "reject".into();
        rejected(store.save_session(&incoming).await.unwrap_err());
        incoming = base.clone();
        incoming.conversation_summary = None;
        rejected(store.save_runtime_state(&incoming).await.unwrap_err());
        rejected(store.clear_session(ID).await.unwrap_err());
        assert_eq!(snapshot(&dir), before, "authority repaired: {row}");
        // Ambiguous authority does not block an otherwise valid exact-context control-plane save.
        let mut control = base.clone();
        control.title = "control only".into();
        control.metadata_version += 1;
        store.save_session(&control).await.unwrap();
    }
}

#[tokio::test]
async fn stale_index_child_fallback_and_missing_runtime_cannot_reconstruct_activated_context() {
    let home = tempfile::tempdir().unwrap();
    let first = SessionStoreV2::new(home.path().into()).await.unwrap();
    let parent = Session::new("parent", "model");
    first.save_session(&parent).await.unwrap();
    let stale = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut child = Session::new_child_of(ID, &parent, "model", "child");
    child.add_message(Message::user("durable"));
    first.save_session(&child).await.unwrap();
    first.claim_activation(&claim(ID)).await.unwrap();
    let dir = home.path().join("sessions/parent/children").join(ID);
    let before = snapshot(&dir);
    child.messages[0].content = "fallback must reject".into();
    rejected(stale.save_runtime_state(&child).await.unwrap_err());
    assert_eq!(snapshot(&dir), before);
    let mut current = first.load_session(ID).await.unwrap().unwrap();
    std::fs::remove_file(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
    let before = snapshot(&dir);
    current.title = "unchanged context, unavailable runtime".into();
    rejected(first.save_session(&current).await.unwrap_err());
    rejected(first.save_runtime_state(&current).await.unwrap_err());
    assert_eq!(snapshot(&dir), before);
}

#[derive(Debug, Default)]
struct Latch {
    entered: bool,
    released: bool,
}
#[derive(Debug)]
pub(super) struct DefaultWriteHook {
    file: &'static str,
    phase: DurableWritePhase,
    fail: bool,
    state: StdMutex<Latch>,
    wake: Condvar,
}
impl DefaultWriteHook {
    pub(super) fn install(
        store: &SessionStoreV2,
        file: &'static str,
        phase: DurableWritePhase,
        fail: bool,
    ) -> Arc<Self> {
        let hook = Arc::new(Self {
            file,
            phase,
            fail,
            state: StdMutex::new(Latch::default()),
            wake: Condvar::new(),
        });
        *store.default_write_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    pub(super) fn visit(&self, path: &Path, phase: DurableWritePhase) -> io::Result<()> {
        if path.file_name().and_then(|n| n.to_str()) != Some(self.file) || phase != self.phase {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if state.entered {
            return Ok(());
        }
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
        if self.fail {
            Err(io::Error::other("injected default publication failure"))
        } else {
            Ok(())
        }
    }
    pub(super) fn wait(&self) {
        let s = self.state.lock().unwrap();
        let (s, t) = self
            .wake
            .wait_timeout_while(s, DEADLINE, |s| !s.entered)
            .unwrap();
        assert!(
            s.entered && !t.timed_out(),
            "actual filesystem job never reached barrier"
        );
    }
    pub(super) fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}
struct Release(Arc<DefaultWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Cancelled(std::sync::mpsc::Sender<()>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn physical_locks_held(store: &SessionStoreV2) {
    for path in [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(ID),
    ] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }
}

#[derive(Clone, Copy)]
enum Writer {
    Full,
    Runtime,
    Clear,
}
async fn write(store: &SessionStoreV2, session: &Session, writer: Writer) -> io::Result<()> {
    match writer {
        Writer::Full => store.save_session(session).await,
        Writer::Runtime => store.save_runtime_state(session).await,
        Writer::Clear => store.clear_session(&session.id).await.map(|_| ()),
    }
}
async fn stores(home: &Path) -> (Arc<SessionStoreV2>, Arc<SessionStoreV2>, Session) {
    let first = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let base = context();
    first.save_session(&base).await.unwrap();
    first.ensure_actor(ID).await.unwrap();
    let second = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    (first, second, base)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_abort_keeps_started_default_jobs_locked_until_successor_claim() {
    for (writer, file) in [
        (Writer::Full, "session.json"),
        (Writer::Runtime, "runtime.json"),
        (Writer::Clear, "attachments"),
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, mut incoming) = stores(home.path()).await;
        incoming.conversation_summary.as_mut().unwrap().content = "pre-activation update".into();
        let hook = DefaultWriteHook::install(&first, file, DurableWritePhase::BeforeReplace, false);
        let _release = Release(hook.clone());
        let job = {
            let first = first.clone();
            tokio::spawn(async move { write(&first, &incoming, writer).await })
        };
        hook.wait();
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        physical_locks_held(&first);
        let successor = {
            let second = second.clone();
            tokio::spawn(async move { second.claim_activation(&claim(ID)).await })
        };
        assert!(tokio::time::timeout(Duration::from_millis(50), async {
            while !successor.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err());
        physical_locks_held(&first);
        hook.release();
        let activation = tokio::time::timeout(DEADLINE, successor)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(activation.attempt, 1);
        let dir = home.path().join("sessions").join(ID);
        let after = snapshot(&dir);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        assert_eq!(
            reopened.inspect_actor(ID).await.unwrap().activation,
            Some(activation)
        );
        assert_eq!(snapshot(&dir), after);
        assert!(reopened.load_session(ID).await.unwrap().is_some());
    }
}

#[test]
fn runtime_shutdown_keeps_each_started_default_job_physically_locked() {
    for (writer, file) in [
        (Writer::Full, "session.json"),
        (Writer::Runtime, "runtime.json"),
        (Writer::Clear, "attachments"),
    ] {
        let home = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (first, second, mut incoming) = runtime.block_on(stores(home.path()));
        incoming.conversation_summary.as_mut().unwrap().content = "shutdown update".into();
        let hook = DefaultWriteHook::install(&first, file, DurableWritePhase::BeforeReplace, false);
        let _release = Release(hook.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        let job = {
            let first = first.clone();
            runtime.spawn(async move {
                let _cancelled = Cancelled(tx);
                write(&first, &incoming, writer).await
            })
        };
        hook.wait();
        runtime.shutdown_timeout(Duration::from_millis(20));
        rx.recv_timeout(DEADLINE).unwrap();
        physical_locks_held(&first);
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let successor = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                assert!(job.await.unwrap_err().is_cancelled());
                start_tx.send(()).unwrap();
                let a = second.claim_activation(&claim(ID)).await.unwrap();
                done_tx.send(a).unwrap();
            });
        });
        start_rx.recv_timeout(DEADLINE).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        physical_locks_held(&first);
        hook.release();
        let activation = done_rx.recv_timeout(DEADLINE).unwrap();
        successor.join().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
            assert_eq!(
                reopened.inspect_actor(ID).await.unwrap().activation,
                Some(activation)
            );
            assert!(reopened.load_session(ID).await.unwrap().is_some());
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_errors_preserve_actual_confirmation_boundary_and_release_guards() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, mut incoming) = stores(home.path()).await;
        let path = home.path().join("sessions").join(ID).join("session.json");
        let before = std::fs::read(&path).unwrap();
        incoming.messages[0].content = "replacement".into();
        let hook = DefaultWriteHook::install(&first, "session.json", phase, true);
        let _release = Release(hook.clone());
        let job = {
            let first = first.clone();
            tokio::spawn(async move { first.save_session(&incoming).await })
        };
        hook.wait();
        physical_locks_held(&first);
        hook.release();
        assert!(job.await.unwrap().is_err());
        if phase == DurableWritePhase::BeforeReplace {
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            assert_ne!(std::fs::read(&path).unwrap(), before);
        }
        assert_eq!(
            second.claim_activation(&claim(ID)).await.unwrap().attempt,
            1
        );
        for file in std::fs::read_dir(path.parent().unwrap()).unwrap() {
            assert!(!file
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("durable.tmp."));
        }
    }
}
