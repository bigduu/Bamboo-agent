//! Real canonical chains and the old ports' actual publication/recovery barriers.
use super::actor_transcript_tests::{Release, TranscriptWriteHook};
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFence, ActorDirectoryPort, Message,
    SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits, SessionInboxOwnedClaim,
    SessionInboxPort, SessionMessageEnvelope, Storage,
};
use chrono::Duration as LeaseDuration;
use std::collections::BTreeMap;

const ID: &str = "lineage-leaf";
#[derive(Clone, Copy, Debug)]
enum Port {
    Transcript,
    InputNew,
    InputAlready,
}
struct Fixture {
    home: PathBuf,
    store: Arc<SessionStoreV2>,
    other: Arc<SessionStoreV2>,
    inbox: crate::FileSessionInbox,
    current: Session,
    fence: ActorActivationFence,
    claim: SessionInboxOwnedClaim,
}
impl Fixture {
    fn target(&self) -> PathBuf {
        self.home.join("sessions/root/children").join(ID)
    }
    fn middle(&self) -> PathBuf {
        self.home.join("sessions/root/children/middle")
    }
    fn input(&self, current: &Session) -> ActorInputCheckpoint {
        ActorInputCheckpoint {
            fence: self.fence.clone(),
            claim: self.claim.clone(),
            expected_created_at: current.created_at,
            expected_messages: current.messages.clone(),
            expected_provider_transcript: current.provider_transcript.clone(),
            expected_admission: current
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.session_inbox_admission.clone()),
        }
    }
    fn transcript(&self) -> ActorTranscriptAppend {
        ActorTranscriptAppend {
            fence: self.fence.clone(),
            expected_created_at: self.current.created_at,
            expected_messages: self.current.messages.clone(),
            expected_provider_transcript: self.current.provider_transcript.clone(),
            messages: vec![Message::assistant("new output", None)],
            native_groups: vec![],
        }
    }
    async fn run(&self, port: Port) -> std::result::Result<Session, String> {
        match port {
            Port::Transcript => self
                .store
                .append_actor_transcript(self.transcript())
                .await
                .map_err(|e| e.to_string()),
            Port::InputNew | Port::InputAlready => self
                .inbox
                .checkpoint_actor_input(self.input(&self.current))
                .await
                .map(|result| {
                    assert_eq!(
                        result.status,
                        if matches!(port, Port::InputAlready) {
                            ActorInputCheckpointStatus::AlreadyCheckpointed
                        } else {
                            ActorInputCheckpointStatus::NewCheckpoint
                        }
                    );
                    result.session
                })
                .map_err(|e| e.to_string()),
        }
    }
}
async fn fixture(home: &Path, port: Port) -> Arc<Fixture> {
    let store = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let mut root = Session::new("root", "model");
    root.set_project_id_meta("project-a");
    store.save_session(&root).await.unwrap();
    let mut middle = Session::new_child_of("middle", &root, "model", "middle");
    middle.set_project_id_meta("project-a");
    middle.metadata_version = 2;
    store.save_session(&middle).await.unwrap();
    let mut leaf = Session::new_child_of(ID, &middle, "model", "leaf");
    leaf.set_project_id_meta("project-a");
    leaf.messages = vec![Message::system("system"), Message::user("original input")];
    store.save_session(&leaf).await.unwrap();
    let target = home.join("sessions/root/children").join(ID);
    let main = target.join("session.json");
    let raw = std::fs::read_to_string(&main).unwrap().replacen(
        '\n',
        "\n\"future_control\": {\"lexeme\":1e0,\"escape\":\"\\u0061\"},",
        1,
    );
    std::fs::write(main, raw).unwrap();
    std::fs::create_dir_all(target.join("attachments")).unwrap();
    std::fs::write(target.join("attachments/unchanged.bin"), b"attachment").unwrap();
    let inbox = crate::FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input(ID, "owned input"),
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = SessionInboxLeaseRequest {
        consumer: SessionInboxConsumerId::new(),
        now: Utc::now(),
        duration: LeaseDuration::minutes(5),
    };
    let claim = inbox
        .claim_owned(ID, 1, None, &lease)
        .await
        .unwrap()
        .remove(0);
    let activation = store
        .claim_activation(&ActorActivationClaim {
            actor_id: ID.into(),
            run_id: "lineage-run".into(),
            lease_owner: claim.lease.consumer.as_str().into(),
            lease_expires_at: claim.lease.expires_at,
            inbox_generation: claim.claim.generation,
            placement_ref: None,
            now: lease.now,
        })
        .await
        .unwrap();
    let fence = activation.fence();
    store.start_activation(&fence, Utc::now()).await.unwrap();
    let current = store.load_session(ID).await.unwrap().unwrap();
    // Open the independent Store before parking a started job; no constructor
    // recovery is hidden behind a publication barrier.
    let other = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let mut f = Fixture {
        home: home.into(),
        store,
        other,
        inbox,
        current,
        fence,
        claim,
    };
    if matches!(port, Port::InputAlready) {
        f.current = f
            .inbox
            .checkpoint_actor_input(f.input(&f.current))
            .await
            .unwrap()
            .session;
    }
    Arc::new(f)
}
fn files(directory: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(base: &Path, directory: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if !directory.exists() {
            return;
        }
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let kind = std::fs::symlink_metadata(&path).unwrap().file_type();
            if kind.is_dir() {
                collect(base, &path, result);
            } else if kind.is_file() {
                result.insert(
                    path.strip_prefix(base).unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    collect(directory, directory, &mut result);
    result
}
fn ancestor_files(f: &Fixture) -> BTreeMap<PathBuf, Vec<u8>> {
    files(&f.home.join("sessions/root"))
        .into_iter()
        .filter(|(path, _)| !path.starts_with(Path::new("children").join(ID)))
        .collect()
}
fn no_temps(directory: &Path) {
    assert!(!std::fs::read_dir(directory).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("durable.tmp")));
}
async fn reject_unchanged(f: &Fixture, port: Port) {
    let before = files(&f.home.join("sessions"));
    let index = std::fs::read(f.home.join("sessions.json")).unwrap();
    assert!(
        f.run(port).await.is_err(),
        "accepted invalid chain: {port:?}"
    );
    assert_eq!(files(&f.home.join("sessions")), before);
    assert_eq!(std::fs::read(f.home.join("sessions.json")).unwrap(), index);
    no_temps(&f.target());
    assert!(
        f.target()
            .join("inbox/cur")
            .join(&f.claim.claim.claim_id)
            .exists(),
        "checkpoint must not ACK the current owned envelope"
    );
}
#[tokio::test]
async fn checkpoint_lineage_lawful_nested_transcript_input_and_already_preserve_control_planes() {
    for port in [Port::Transcript, Port::InputNew, Port::InputAlready] {
        let home = tempfile::tempdir().unwrap();
        let f = fixture(home.path(), port).await;
        let before = files(&f.target());
        let ancestors = (
            files(&home.path().join("sessions/root")),
            files(&f.middle()),
        );
        let current = f.run(port).await.unwrap();
        assert_eq!(current.id, ID);
        assert_eq!(current.created_at, f.current.created_at);
        assert_eq!(current.provider_transcript, f.current.provider_transcript);
        let after = files(&f.target());
        for (name, bytes) in &before {
            if name != Path::new("session.json") {
                assert_eq!(after.get(name), Some(bytes), "changed protected {name:?}");
            }
        }
        if matches!(port, Port::InputAlready) {
            assert_eq!(after, before);
        } else {
            assert_eq!(current.messages.len(), f.current.messages.len() + 1);
        }
        // Compare only ancestor-owned files, excluding the legitimately updated target.
        assert_eq!(files(&f.middle()), ancestors.1);
        let root = files(&home.path().join("sessions/root"));
        for (path, bytes) in ancestors.0 {
            if !path.starts_with(Path::new("children").join(ID)) {
                assert_eq!(root.get(&path), Some(&bytes));
            }
        }
        assert!(f
            .target()
            .join("inbox/cur")
            .join(&f.claim.claim.claim_id)
            .exists());
    }
}
#[tokio::test]
async fn checkpoint_lineage_deleted_middle_project_birth_rollback_and_gap_reject_all_old_ports() {
    for port in [Port::Transcript, Port::InputNew, Port::InputAlready] {
        for change in 0..5 {
            let home = tempfile::tempdir().unwrap();
            let f = fixture(home.path(), port).await;
            match change {
                0 => {
                    assert!(f.other.delete_session("middle").await.unwrap());
                    assert!(f.other.load_session("middle").await.unwrap().is_none());
                }
                1 | 4 => {
                    let mut root = f.other.load_session("root").await.unwrap().unwrap();
                    for _ in 0..if change == 4 { 2 } else { 1 } {
                        if change == 1 {
                            root.set_project_id_meta("project-b");
                        }
                        root.metadata_version += 1;
                        f.other.save_session(&root).await.unwrap();
                    }
                }
                2 | 3 => {
                    // Explicit physical corruption: coherent Main/Runtime pair,
                    // not a legal application writer or a new birth protocol.
                    for name in ["session.json", RUNTIME_SIDECAR_FILE] {
                        let path = f.middle().join(name);
                        let mut session: Session =
                            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                        if change == 2 {
                            session.created_at += LeaseDuration::nanoseconds(1);
                            assert!(session.created_at < f.current.created_at);
                        } else {
                            session.metadata_version -= 1;
                        }
                        let bytes = if name == "session.json" {
                            compact_main::serialize_main(&session).unwrap()
                        } else {
                            serde_json::to_vec(&session).unwrap()
                        };
                        std::fs::write(path, bytes).unwrap();
                    }
                }
                _ => unreachable!(),
            }
            assert!(
                f.target().join("session.json").exists(),
                "grandchild remains physically present"
            );
            reject_unchanged(&f, port).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_lineage_real_before_replace_detects_legal_parent_write_and_all_witness_files() {
    for port in [Port::Transcript, Port::InputNew] {
        for change in 0..5 {
            let home = tempfile::tempdir().unwrap();
            let f = fixture(home.path(), port).await;
            let before = files(&f.target());
            let hook = TranscriptWriteHook::install(
                &f.store,
                DurableWritePhase::BeforeReplace,
                false,
                false,
            );
            let _release = Release(hook.clone());
            let task = tokio::spawn({
                let f = f.clone();
                async move { f.run(port).await }
            });
            hook.entered();
            if change == 0 {
                // Real independent canonical writer; exact parent Session lock,
                // shared L/Task, no target lock reacquisition or fake hook writer.
                let mut middle = f.other.load_session("middle").await.unwrap().unwrap();
                middle.metadata_version += 1;
                middle.title = "lawful newer parent".into();
                f.other.save_session(&middle).await.unwrap();
            } else {
                let path = match change {
                    1 => f.middle().join("session.json"),
                    2 => f.middle().join(RUNTIME_SIDECAR_FILE),
                    3 => home
                        .path()
                        .join("sessions/root")
                        .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE),
                    _ => f.middle().join(supervisor_proof::SUPERVISOR_PROOF_FILE),
                };
                if change == 4 {
                    assert!(!path.exists());
                    std::fs::write(path, b"").unwrap();
                } else {
                    let mut bytes = std::fs::read(&path).unwrap();
                    bytes.push(b'\n');
                    std::fs::write(path, bytes).unwrap();
                }
            }
            let after_deliberate_parent_change = ancestor_files(&f);
            hook.release();
            assert!(
                task.await.unwrap().is_err(),
                "published after ancestor drift {port:?}/{change}"
            );
            assert_eq!(files(&f.target()), before);
            assert_eq!(ancestor_files(&f), after_deliberate_parent_change);
            no_temps(&f.target());
        }
    }
}
#[tokio::test]
async fn checkpoint_lineage_input_unconfirmed_reload_already_rejects_deleted_parent_without_ack() {
    let home = tempfile::tempdir().unwrap();
    let f = fixture(home.path(), Port::InputNew).await;
    let original = std::fs::read(f.target().join("session.json")).unwrap();
    let hook = TranscriptWriteHook::install(&f.store, DurableWritePhase::AfterReplace, true, false);
    hook.release();
    assert!(matches!(
        f.inbox
            .checkpoint_actor_input(f.input(&f.current))
            .await
            .unwrap_err(),
        ActorInputCheckpointError::OutcomeUnconfirmed(_)
    ));
    assert_ne!(
        std::fs::read(f.target().join("session.json")).unwrap(),
        original
    );
    assert!(f.other.delete_session("middle").await.unwrap());
    let cold = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let current = cold.load_session(ID).await.unwrap().unwrap();
    let request = f.input(&current);
    let inbox = crate::FileSessionInbox::new(cold, SessionInboxLimits::default());
    let before = files(&home.path().join("sessions"));
    assert!(inbox.checkpoint_actor_input(request).await.is_err());
    assert_eq!(files(&home.path().join("sessions")), before);
    assert!(f
        .target()
        .join("inbox/cur")
        .join(&f.claim.claim.claim_id)
        .exists());
    no_temps(&f.target());
}
