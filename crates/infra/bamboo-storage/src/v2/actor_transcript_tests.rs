//! Actual main/sidecar files, independent Stores and physical publication barriers.
use super::actor_transcript::*;
use super::*;
use bamboo_domain::session::provider_transcript::{
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin, ProviderTranscriptState,
};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryEntry, ActorDirectoryError,
    ActorDirectoryPort, Message, MessagePhase,
};
use chrono::Duration as ChronoDuration;
use serde_json::{json, value::RawValue};
use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex as StdMutex};

pub(super) const ID: &str = "append-actor";
const DEADLINE: Duration = Duration::from_secs(10);
#[derive(Debug, Default)]
struct Latch {
    entered: bool,
    released: bool,
}
#[derive(Debug)]
pub(super) struct TranscriptWriteHook {
    phase: DurableWritePhase,
    fail: bool,
    corrupt_readback: bool,
    latch: StdMutex<Latch>,
    changed: Condvar,
}
impl TranscriptWriteHook {
    pub(super) fn install(
        store: &SessionStoreV2,
        phase: DurableWritePhase,
        fail: bool,
        corrupt_readback: bool,
    ) -> Arc<Self> {
        let hook = Arc::new(Self {
            phase,
            fail,
            corrupt_readback,
            latch: StdMutex::new(Latch::default()),
            changed: Condvar::new(),
        });
        *store.transcript_write_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
    pub(super) fn visit(&self, phase: DurableWritePhase) -> io::Result<()> {
        if phase != self.phase {
            return Ok(());
        }
        let mut latch = self.latch.lock().unwrap();
        if latch.entered {
            return Ok(());
        }
        latch.entered = true;
        self.changed.notify_all();
        while !latch.released {
            latch = self.changed.wait(latch).unwrap();
        }
        if self.fail {
            Err(io::Error::other("injected transcript publication failure"))
        } else {
            Ok(())
        }
    }
    pub(super) fn before_readback(&self, path: &Path) -> io::Result<()> {
        if self.corrupt_readback {
            std::fs::write(path, b"invalid readback")?;
        }
        Ok(())
    }
    pub(super) fn entered(&self) {
        let latch = self.latch.lock().unwrap();
        let (latch, timeout) = self
            .changed
            .wait_timeout_while(latch, DEADLINE, |s| !s.entered)
            .unwrap();
        assert!(
            latch.entered && !timeout.timed_out(),
            "no actual Before/AfterReplace barrier"
        );
    }
    pub(super) fn release(&self) {
        self.latch.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
pub(super) struct Release(pub(super) Arc<TranscriptWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn items(family: ProviderFamily, tag: &str) -> Vec<ProviderTranscriptItem> {
    let (protocol, payloads) = if family == ProviderFamily::Anthropic {
        (
            ProviderProtocol::AnthropicMessages2023_06_01,
            vec![
                (
                    ProviderTranscriptAuthor::Model,
                    json!({"type":"text","text":"search"}),
                ),
                (
                    ProviderTranscriptAuthor::Model,
                    json!({"type":"server_tool_use","id":tag,"name":"tool_search_tool_regex","input":{"pattern":"weather"}}),
                ),
                (
                    ProviderTranscriptAuthor::ToolResult,
                    json!({"type":"tool_search_tool_result","tool_use_id":tag,"content":{"type":"tool_search_tool_search_result","tool_references":[{"type":"tool_reference","tool_name":"get_weather"}]}}),
                ),
            ],
        )
    } else {
        (
            ProviderProtocol::OpenAiResponsesV1,
            vec![
                (
                    ProviderTranscriptAuthor::Model,
                    json!({"type":"tool_search_call","id":format!("{tag}-call"),"execution":"server","call_id":tag,"status":"completed","arguments":{"query":"orders"}}),
                ),
                (
                    ProviderTranscriptAuthor::ToolResult,
                    json!({"type":"tool_search_output","id":format!("{tag}-output"),"execution":"server","call_id":tag,"status":"completed","tools":[{"type":"function","name":"get_orders"}]}),
                ),
                (
                    ProviderTranscriptAuthor::Model,
                    json!({"type":"message","id":format!("{tag}-message"),"role":"assistant","status":"completed","content":[{"type":"output_text","text":"done","annotations":[]}]}),
                ),
            ],
        )
    };
    payloads
        .into_iter()
        .map(|(author, payload)| {
            ProviderTranscriptItem::try_from_payload(
                family,
                protocol,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        })
        .collect()
}
pub(super) fn claim(run: &str, expires: DateTime<Utc>) -> ActorActivationClaim {
    ActorActivationClaim {
        actor_id: ID.into(),
        run_id: run.into(),
        lease_owner: run.into(),
        lease_expires_at: expires,
        inbox_generation: 7,
        placement_ref: None,
        now: Utc::now(),
    }
}
fn request(session: &Session, fence: bamboo_domain::ActorActivationFence) -> ActorTranscriptAppend {
    let mut message = Message::assistant("appended 🪷", None);
    message.id = "append-message".into();
    message.phase = Some(MessagePhase::FinalAnswer);
    ActorTranscriptAppend {
        fence,
        expected_created_at: session.created_at,
        expected_messages: session.messages.clone(),
        expected_provider_transcript: session.provider_transcript.clone(),
        messages: vec![message],
        native_groups: vec![],
    }
}
pub(super) fn actor_dir(home: &Path, child: bool) -> PathBuf {
    if child {
        home.join("sessions/root/children").join(ID)
    } else {
        home.join("sessions").join(ID)
    }
}
pub(super) fn files(directory: &Path) -> Vec<Option<Vec<u8>>> {
    [
        "session.json",
        RUNTIME_SIDECAR_FILE,
        "actor-authority.json",
        "actor-authority.initialized.json",
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        "attachments/unchanged.bin",
    ]
    .map(|name| std::fs::read(directory.join(name)).ok())
    .into()
}
pub(super) fn raw_field<'a>(raw: &'a str, key: &str) -> &'a str {
    let fields: BTreeMap<String, &RawValue> = serde_json::from_str(raw).unwrap();
    fields[key].get()
}
fn raw_entries(raw: &str) -> Vec<&str> {
    serde_json::from_str::<Vec<&RawValue>>(raw)
        .unwrap()
        .into_iter()
        .map(RawValue::get)
        .collect()
}
pub(super) async fn setup(
    home: &Path,
    child: bool,
    family: ProviderFamily,
) -> (
    Arc<SessionStoreV2>,
    Arc<SessionStoreV2>,
    Session,
    ActorTranscriptAppend,
) {
    let store = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let root = Session::new(if child { "root" } else { ID }, "model");
    if child {
        store.save_session(&root).await.unwrap();
    }
    let mut session = if child {
        Session::new_child_of(ID, &root, "model", "child")
    } else {
        root
    };
    session.messages = vec![
        Message::system("system"),
        Message::user("user"),
        Message::assistant("prior", None),
    ];
    session
        .metadata
        .insert("disabled_tools".into(), "[\"Write\"]".into());
    session.model_context_state = Some(Default::default());
    session
        .runtime_metadata
        .get_or_insert_with(Default::default)
        .session_inbox_admission = Some(Default::default());
    session
        .activate_provider_transcript_route(
            family,
            if family == ProviderFamily::Anthropic {
                ProviderProtocol::AnthropicMessages2023_06_01
            } else {
                ProviderProtocol::OpenAiResponsesV1
            },
            &"a".repeat(64),
        )
        .unwrap();
    session
        .append_provider_transcript_group(
            session.messages[2].id.clone(),
            Some("old-group"),
            items(family, "old-search"),
        )
        .unwrap();
    store.save_session(&session).await.unwrap();
    let directory = actor_dir(home, child);
    std::fs::create_dir_all(directory.join("attachments")).unwrap();
    std::fs::write(directory.join("attachments/unchanged.bin"), b"attachment").unwrap();
    let main = directory.join("session.json");
    let raw = std::fs::read_to_string(&main).unwrap().replacen(
        '\n',
        "\n\"future_control\": { \"number\":1e0, \"escaped\":\"\\u0061\" },",
        1,
    );
    std::fs::write(main, raw).unwrap();
    let activation = store
        .claim_activation(&claim("old-owner", Utc::now() + ChronoDuration::minutes(5)))
        .await
        .unwrap();
    let second = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let req = request(&session, activation.fence());
    (store, second, session, req)
}
pub(super) fn no_temps(directory: &Path) {
    assert!(!std::fs::read_dir(directory).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("durable.tmp")));
}
async fn unchanged_rejection(
    store: &SessionStoreV2,
    directory: &Path,
    request: ActorTranscriptAppend,
) -> ActorTranscriptAppendError {
    let before = files(directory);
    let error = store.append_actor_transcript(request).await.unwrap_err();
    assert_eq!(files(directory), before);
    no_temps(directory);
    error
}

#[tokio::test]
async fn root_child_six_native_kinds_preserve_exact_raw_prefix_and_control_plane() {
    for child in [false, true] {
        for family in [
            ProviderFamily::OpenAi,
            ProviderFamily::Copilot,
            ProviderFamily::Anthropic,
        ] {
            let home = tempfile::tempdir().unwrap();
            let (store, second, _, mut req) = setup(home.path(), child, family).await;
            let directory = actor_dir(home.path(), child);
            let before = files(&directory);
            let original = std::str::from_utf8(before[0].as_ref().unwrap()).unwrap();
            assert!(compact_main::validate_full_main(original.as_bytes())
                .unwrap()
                .is_some());
            // Unknown native keys and original group lexical JSON are also retained.
            let native_raw = raw_field(original, "provider_transcript");
            let decorated = native_raw.replacen('{', "{\"future_native\":1e0,", 1);
            let decorated = decorated.replacen(
                "\"anchor_message_id\"",
                "\"future_group\":{\"escaped\":\"\\u0062\",\"number\":1.00},\"anchor_message_id\"",
                1,
            );
            std::fs::write(
                directory.join("session.json"),
                original.replacen(native_raw, &decorated, 1),
            )
            .unwrap();
            let before = files(&directory);
            let original = std::str::from_utf8(before[0].as_ref().unwrap()).unwrap();
            req.native_groups.push(ActorTranscriptGroupAppend {
                anchor_message_id: req.messages[0].id.clone(),
                id_hint: Some("new-group".into()),
                items: items(family, "new-search"),
            });
            let committed = store.append_actor_transcript(req).await.unwrap();
            let after = files(&directory);
            assert_eq!(&after[1..], &before[1..]);
            let published = std::str::from_utf8(after[0].as_ref().unwrap()).unwrap();
            assert_eq!(
                raw_field(published, compact_main::MEMBER),
                raw_field(original, compact_main::MEMBER)
            );
            assert!(compact_main::validate_full_main(published.as_bytes())
                .unwrap()
                .is_some());
            assert_eq!(
                raw_field(published, "future_control"),
                raw_field(original, "future_control")
            );
            let old_messages = raw_entries(raw_field(original, "messages"));
            assert_eq!(
                &raw_entries(raw_field(published, "messages"))[..old_messages.len()],
                old_messages
            );
            let old_native = raw_field(original, "provider_transcript");
            let new_native = raw_field(published, "provider_transcript");
            assert_eq!(
                raw_field(new_native, "future_native"),
                raw_field(old_native, "future_native")
            );
            let old_groups = raw_entries(raw_field(old_native, "groups"));
            assert_eq!(
                &raw_entries(raw_field(new_native, "groups"))[..old_groups.len()],
                old_groups
            );
            let actual: Session = serde_json::from_str(published).unwrap();
            let side: Session = serde_json::from_slice(after[1].as_ref().unwrap()).unwrap();
            let actual = overlay_runtime_sidecar(actual, Some(side));
            assert_eq!(
                serde_json::to_value(&committed).unwrap(),
                serde_json::to_value(actual).unwrap()
            );
            assert_eq!(committed.messages.len(), old_messages.len() + 1);
            assert_eq!(
                committed.provider_transcript.groups().len(),
                old_groups.len() + 1
            );
            let reopened = second.load_session(ID).await.unwrap().unwrap();
            assert_eq!(reopened.messages.last().unwrap().content, "appended 🪷");
            no_temps(&directory);
        }
    }
}

#[tokio::test]
async fn complete_prefix_and_closed_payload_subset_reject_without_authority_refresh() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    let mut cases = Vec::new();
    let mut r = base.clone();
    r.expected_messages.pop();
    cases.push(r);
    let mut r = base.clone();
    r.expected_messages.swap(0, 1);
    cases.push(r);
    let mut r = base.clone();
    r.expected_messages[1].content.push('!');
    cases.push(r);
    let mut r = base.clone();
    r.expected_messages[1].role = Role::Assistant;
    cases.push(r);
    let mut r = base.clone();
    r.expected_provider_transcript = Default::default();
    cases.push(r);
    let mut r = base.clone();
    r.messages[0].id = " ".into();
    cases.push(r);
    let mut r = base.clone();
    r.messages[0].id = r.expected_messages[0].id.clone();
    cases.push(r);
    let mut r = base.clone();
    r.messages.push(r.messages[0].clone());
    cases.push(r);
    let mut r = base.clone();
    r.messages.clear();
    cases.push(r);
    for role in [Role::User, Role::System, Role::Tool] {
        let mut r = base.clone();
        r.messages[0].role = role;
        cases.push(r);
    }
    for change in 0..12 {
        let mut r = base.clone();
        let m = &mut r.messages[0];
        match change {
            0 => m.tool_calls = Some(vec![]),
            1 => m.tool_call_id = Some("id".into()),
            2 => m.tool_success = Some(false),
            3 => m.reasoning = Some(String::new()),
            4 => m.reasoning_signature = Some(String::new()),
            5 => m.content_parts = Some(vec![]),
            6 => m.image_ocr = Some(vec![]),
            7 => m.metadata = Some(json!(null)),
            8 => m.compressed = true,
            9 => m.compressed_by_event_id = Some("event".into()),
            10 => m.never_compress = true,
            _ => m.compression_level = 1,
        }
        cases.push(r);
    }
    for r in cases {
        unchanged_rejection(&store, &directory, r).await;
    }
    // One unseen metadata revision must be observed without writing the Actor record.
    let mut side: Session =
        serde_json::from_slice(&std::fs::read(directory.join(RUNTIME_SIDECAR_FILE)).unwrap())
            .unwrap();
    side.metadata_version += 1;
    std::fs::write(
        directory.join(RUNTIME_SIDECAR_FILE),
        serde_json::to_vec(&side).unwrap(),
    )
    .unwrap();
    let record = std::fs::read(directory.join("actor-authority.json")).unwrap();
    store.append_actor_transcript(base).await.unwrap();
    assert_eq!(
        std::fs::read(directory.join("actor-authority.json")).unwrap(),
        record
    );
}

#[tokio::test]
async fn native_route_anchor_validator_and_counter_fail_closed() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, session, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    let group = ActorTranscriptGroupAppend {
        anchor_message_id: base.messages[0].id.clone(),
        id_hint: None,
        items: items(ProviderFamily::OpenAi, "new"),
    };
    let mut cases = Vec::new();
    let mut r = base.clone();
    let mut g = group.clone();
    g.anchor_message_id = session.messages[2].id.clone();
    r.native_groups.push(g);
    cases.push(r);
    let mut r = base.clone();
    let mut g = group.clone();
    g.items.swap(0, 1);
    r.native_groups.push(g);
    cases.push(r);
    let mut r = base.clone();
    let mut g = group.clone();
    g.items.remove(1);
    r.native_groups.push(g);
    cases.push(r);
    let mut r = base.clone();
    let mut g = group.clone();
    g.items = items(ProviderFamily::Anthropic, "other");
    r.native_groups.push(g);
    cases.push(r);
    let mut r = base.clone();
    let mut g = group.clone();
    g.items = vec![g.items.pop().unwrap()];
    r.native_groups.push(g);
    cases.push(r);
    for (kind, author, payload) in [
        (
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"function_call","id":"fc","call_id":"call","name":"Read","arguments":"{}"}),
        ),
        (
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"reasoning","id":"reason","summary":[]}),
        ),
        (
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"tool_search_call","id":"client","call_id":"client-call","status":"completed","execution":"client","arguments":{"query":"orders"}}),
        ),
        (
            ProviderTranscriptOrigin::HostToolSearch,
            ProviderTranscriptAuthor::ToolResult,
            json!({"type":"tool_search_output","execution":"client","call_id":"host-call","status":"completed","tools":[]}),
        ),
        (
            ProviderTranscriptOrigin::DeveloperContext,
            ProviderTranscriptAuthor::Host,
            json!({"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"Read","description":"read","parameters":{"type":"object","properties":{},"additionalProperties":false},"strict":true}]}),
        ),
    ] {
        let mut r = base.clone();
        let mut g = group.clone();
        g.items.push(
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                kind,
                author,
                payload,
            )
            .unwrap(),
        );
        r.native_groups.push(g);
        cases.push(r);
    }
    for r in cases {
        unchanged_rejection(&store, &directory, r).await;
    }
    let original = std::fs::read(directory.join("session.json")).unwrap();
    for change in [
        "unbound",
        "automatic-unbound",
        "revision-max",
        "sequence-max",
    ] {
        let mut main: Session = serde_json::from_slice(&original).unwrap();
        if change.contains("unbound") {
            main.provider_transcript = ProviderTranscriptState::default();
            if change == "automatic-unbound" {
                main.append_provider_transcript_group(
                    main.messages[2].id.clone(),
                    None,
                    items(ProviderFamily::OpenAi, "legacy"),
                )
                .unwrap();
            }
        } else {
            let mut state = serde_json::to_value(&main.provider_transcript).unwrap();
            state[if change == "revision-max" {
                "state_revision"
            } else {
                "next_sequence"
            }] = json!(u64::MAX);
            main.provider_transcript = serde_json::from_value(state).unwrap();
        }
        std::fs::write(
            directory.join("session.json"),
            serde_json::to_vec(&main).unwrap(),
        )
        .unwrap();
        let mut r = request(&main, base.fence.clone());
        r.native_groups.push(group.clone());
        unchanged_rejection(&store, &directory, r).await;
    }
}

#[tokio::test]
async fn full_fence_birth_and_terminal_authority_reject_each_foreign_component() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    for field in 0..7 {
        let mut r = base.clone();
        match field {
            0 => r.fence.schema_version += 1,
            1 => r.fence.activation_id.push('x'),
            2 => r.fence.attempt += 1,
            3 => r.fence.run_id.push('x'),
            4 => r.fence.lease_owner.push('x'),
            5 => r.fence.lease_epoch += 1,
            _ => r.expected_created_at += ChronoDuration::seconds(1),
        }
        unchanged_rejection(&store, &directory, r).await;
    }
    for finish in [
        ActorActivationFinish::Succeeded,
        ActorActivationFinish::Failed,
        ActorActivationFinish::Cancelled,
    ] {
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
        store
            .start_activation(&base.fence, Utc::now())
            .await
            .unwrap();
        store
            .finish_activation(&base.fence, Utc::now(), finish)
            .await
            .unwrap();
        unchanged_rejection(&store, &actor_dir(home.path(), false), base).await;
    }
    store.retire_actor(ID, Utc::now()).await.unwrap();
    unchanged_rejection(&store, &directory, base.clone()).await;
    store.delete_session_recursive(ID, false).await.unwrap();
    let replacement = store.recreate_root_session(ID, "model").await.unwrap();
    assert!(replacement.created_at > base.expected_created_at);
    store
        .claim_activation(&claim("new-birth", Utc::now() + ChronoDuration::minutes(5)))
        .await
        .unwrap();
    unchanged_rejection(&store, &directory, base).await;
}

#[tokio::test]
async fn pure_reader_rejects_missing_corrupt_duplicate_or_contradictory_source_without_repair() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    for name in [
        "session.json",
        RUNTIME_SIDECAR_FILE,
        "actor-authority.json",
        "actor-authority.initialized.json",
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
    ] {
        let path = directory.join(name);
        let original = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        unchanged_rejection(&store, &directory, base.clone()).await;
        std::fs::write(&path, b"invalid JSON").unwrap();
        unchanged_rejection(&store, &directory, base.clone()).await;
        #[cfg(unix)]
        {
            let target = home.path().join("symlink-sentinel");
            std::fs::write(&target, &original).unwrap();
            std::fs::remove_file(&path).unwrap();
            std::os::unix::fs::symlink(&target, &path).unwrap();
            unchanged_rejection(&store, &directory, base.clone()).await;
            assert_eq!(std::fs::read(target).unwrap(), original);
            std::fs::remove_file(&path).unwrap();
        }
        std::fs::write(&path, &original).unwrap();
    }
    let path = directory.join("session.json");
    let original = std::fs::read_to_string(&path).unwrap();
    let mut ordinary: Session = serde_json::from_str(&original).unwrap();
    ordinary.supervisor_management = Some(
        serde_json::from_value(json!({
            "schema_version":1, "incarnation_id":Uuid::new_v4(), "revision":1,
            "allowed_projects":[], "links":{}
        }))
        .unwrap(),
    );
    std::fs::write(&path, serde_json::to_vec(&ordinary).unwrap()).unwrap();
    let error = unchanged_rejection(&store, &directory, base.clone()).await;
    assert!(error
        .to_string()
        .contains("Ordinary Sessions cannot hold Supervisor management state"));
    for corrupted in [
        original.replacen('{', "{\"id\":\"foreign\",", 1),
        original.replacen(
            "\"next_sequence\":",
            "\"next_sequence\":0,\"next_sequence\":",
            1,
        ),
        original.replacen("\"role\":", "\"role\":\"tool\",\"role\":", 1),
    ] {
        std::fs::write(&path, corrupted).unwrap();
        unchanged_rejection(&store, &directory, base.clone()).await;
    }
    let mut main: serde_json::Value = serde_json::from_str(&original).unwrap();
    main["messages"][0]
        .as_object_mut()
        .unwrap()
        .remove("created_at");
    std::fs::write(&path, serde_json::to_vec(&main).unwrap()).unwrap();
    unchanged_rejection(&store, &directory, base.clone()).await;
    std::fs::write(&path, original).unwrap();
    let side_path = directory.join(RUNTIME_SIDECAR_FILE);
    let mut side: Session = serde_json::from_slice(&std::fs::read(&side_path).unwrap()).unwrap();
    side.created_at += ChronoDuration::seconds(1);
    std::fs::write(side_path, serde_json::to_vec(&side).unwrap()).unwrap();
    unchanged_rejection(&store, &directory, base).await;
}

pub(super) fn lock_paths(store: &SessionStoreV2) -> [PathBuf; 3] {
    [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(ID),
    ]
}
pub(super) fn physically_held(store: &SessionStoreV2) {
    for path in lock_paths(store) {
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
pub(super) fn released(store: &SessionStoreV2) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let all = lock_paths(store).iter().all(|path| {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            if FileExt::try_lock_exclusive(&file).is_err() {
                return false;
            }
            FileExt::unlock(&file).unwrap();
            true
        });
        if all {
            return;
        }
        assert!(Instant::now() < deadline, "owned guards never released");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_expiry_after_actual_before_replace_barrier_is_rejected() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    let actor = directory.join("actor-authority.json");
    let mut entry: ActorDirectoryEntry =
        serde_json::from_slice(&std::fs::read(&actor).unwrap()).unwrap();
    let expires = Utc::now() + ChronoDuration::milliseconds(200);
    entry.activation.as_mut().unwrap().lease_expires_at = expires;
    std::fs::write(actor, serde_json::to_vec(&entry).unwrap()).unwrap();
    let original = files(&directory);
    let hook = TranscriptWriteHook::install(&store, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let writer = {
        let store = store.clone();
        tokio::spawn(async move { store.append_actor_transcript(base).await })
    };
    hook.entered();
    physically_held(&store);
    while Utc::now() <= expires {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    hook.release();
    assert!(matches!(
        writer.await.unwrap(),
        Err(ActorTranscriptAppendError::Authority(
            ActorDirectoryError::StaleFence
        ))
    ));
    assert_eq!(files(&directory), original);
    no_temps(&directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn predecessor_commit_and_successor_takeover_serialize_across_independent_stores() {
    let home = tempfile::tempdir().unwrap();
    let (first, second, session, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    first
        .start_activation(&base.fence, Utc::now())
        .await
        .unwrap();
    let old = base.clone();
    let hook = TranscriptWriteHook::install(&first, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let append = {
        let first = first.clone();
        tokio::spawn(async move { first.append_actor_transcript(base).await })
    };
    hook.entered();
    physically_held(&first);
    let finish = {
        let second = second.clone();
        let fence = old.fence.clone();
        tokio::spawn(async move {
            second
                .finish_activation(&fence, Utc::now(), ActorActivationFinish::Succeeded)
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!finish.is_finished());
    hook.release();
    let committed = append.await.unwrap().unwrap();
    finish.await.unwrap().unwrap();
    let successor = second
        .claim_activation(&claim("successor", Utc::now() + ChronoDuration::minutes(5)))
        .await
        .unwrap();
    unchanged_rejection(&first, &actor_dir(home.path(), false), old).await;
    let mut next = request(&committed, successor.fence());
    next.messages[0].id = "successor-message".into();
    let published = second.append_actor_transcript(next).await.unwrap();
    assert_eq!(published.messages.len(), session.messages.len() + 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successor_started_before_old_append_fences_the_already_waiting_writer() {
    let home = tempfile::tempdir().unwrap();
    let (first, second, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    let record = directory.join("actor-authority.json");
    let mut entry: ActorDirectoryEntry =
        serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    entry.activation.as_mut().unwrap().lease_expires_at = Utc::now() - ChronoDuration::seconds(1);
    std::fs::write(record, serde_json::to_vec(&entry).unwrap()).unwrap();
    let original_main = std::fs::read(directory.join("session.json")).unwrap();
    let hook =
        TranscriptWriteHook::install(&second, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let takeover = {
        let second = second.clone();
        tokio::spawn(async move {
            second
                .claim_activation(&claim(
                    "replacement-owner",
                    Utc::now() + ChronoDuration::minutes(5),
                ))
                .await
        })
    };
    hook.entered();
    physically_held(&second);
    let append = {
        let first = first.clone();
        tokio::spawn(async move { first.append_actor_transcript(base).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!append.is_finished());
    hook.release();
    takeover.await.unwrap().unwrap();
    assert!(matches!(
        append.await.unwrap(),
        Err(ActorTranscriptAppendError::Authority(
            ActorDirectoryError::StaleFence
        ))
    ));
    assert_eq!(
        std::fs::read(directory.join("session.json")).unwrap(),
        original_main
    );
    no_temps(&directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_abort_keeps_all_guards_until_the_complete_job_commits() {
    let home = tempfile::tempdir().unwrap();
    let (store, second, session, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
    let directory = actor_dir(home.path(), false);
    let original = files(&directory);
    let hook = TranscriptWriteHook::install(&store, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let append = {
        let store = store.clone();
        tokio::spawn(async move { store.append_actor_transcript(base).await })
    };
    hook.entered();
    physically_held(&store);
    append.abort();
    assert!(append.await.unwrap_err().is_cancelled());
    physically_held(&store);
    hook.release();
    released(&store);
    assert_eq!(&files(&directory)[1..], &original[1..]);
    assert_eq!(
        second
            .load_session(ID)
            .await
            .unwrap()
            .unwrap()
            .messages
            .len(),
        session.messages.len() + 1
    );
    no_temps(&directory);
}

#[test]
fn whole_runtime_shutdown_does_not_release_blocking_mutation_guards() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (first, second, session, base) =
        runtime.block_on(setup(home.path(), false, ProviderFamily::OpenAi));
    let hook = TranscriptWriteHook::install(&first, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let worker = first.clone();
    let caller = runtime.spawn(async move { worker.append_actor_transcript(base).await });
    hook.entered();
    physically_held(&first);
    runtime.shutdown_background();
    drop(caller);
    physically_held(&first);
    hook.release();
    released(&first);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let committed = runtime.block_on(second.load_session(ID)).unwrap().unwrap();
    assert_eq!(committed.messages.len(), session.messages.len() + 1);
    no_temps(&actor_dir(home.path(), false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn before_replace_failure_is_unchanged_after_replace_and_readback_are_unconfirmed() {
    for (phase, corrupt) in [
        (DurableWritePhase::BeforeReplace, false),
        (DurableWritePhase::AfterReplace, false),
        (DurableWritePhase::AfterReplace, true),
    ] {
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, base) = setup(home.path(), false, ProviderFamily::OpenAi).await;
        let directory = actor_dir(home.path(), false);
        let original = files(&directory);
        let hook = TranscriptWriteHook::install(&store, phase, !corrupt, corrupt);
        let _release = Release(hook.clone());
        let append = {
            let store = store.clone();
            tokio::spawn(async move { store.append_actor_transcript(base).await })
        };
        hook.entered();
        hook.release();
        let error = append.await.unwrap().unwrap_err();
        if phase == DurableWritePhase::BeforeReplace {
            assert!(matches!(
                error,
                ActorTranscriptAppendError::BeforePublication(_)
            ));
            assert_eq!(files(&directory), original);
        } else {
            assert!(matches!(
                error,
                ActorTranscriptAppendError::OutcomeUnconfirmed(_)
            ));
            assert_ne!(files(&directory)[0], original[0]);
        }
        assert_eq!(&files(&directory)[1..], &original[1..]);
        no_temps(&directory);
        released(&store);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_main_append_preserves_legacy_absence_and_rejects_initial_or_final_damage() {
    for damage_at_barrier in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, req) = setup(home.path(), false, ProviderFamily::OpenAi).await;
        let directory = actor_dir(home.path(), false);
        let raw = std::fs::read_to_string(directory.join("session.json")).unwrap();
        let compact = raw_field(&raw, compact_main::MEMBER);
        // Trusted fault changes only the flat revision; valid prefix is retained.
        let damaged = raw.replacen("\"metadata_version\": 0", "\"metadata_version\": 7", 1);
        assert_ne!(damaged, raw);
        assert_eq!(raw_field(&damaged, compact_main::MEMBER), compact);
        if damage_at_barrier {
            let hook = TranscriptWriteHook::install(
                &store,
                DurableWritePhase::BeforeReplace,
                false,
                false,
            );
            let _release = Release(hook.clone());
            let append = tokio::spawn({
                let store = store.clone();
                async move { store.append_actor_transcript(req).await }
            });
            tokio::task::spawn_blocking({
                let hook = hook.clone();
                move || hook.entered()
            })
            .await
            .unwrap();
            std::fs::write(directory.join("session.json"), &damaged).unwrap();
            let before = files(&directory);
            hook.release();
            assert!(matches!(
                append.await.unwrap().unwrap_err(),
                ActorTranscriptAppendError::InvalidSource
            ));
            assert_eq!(files(&directory), before);
        } else {
            std::fs::write(directory.join("session.json"), &damaged).unwrap();
            assert!(matches!(
                unchanged_rejection(&store, &directory, req).await,
                ActorTranscriptAppendError::InvalidSource
            ));
        }
        no_temps(&directory);
    }
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, req) = setup(home.path(), false, ProviderFamily::Anthropic).await;
    let directory = actor_dir(home.path(), false);
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("session.json")).unwrap()).unwrap();
    value.as_object_mut().unwrap().remove(compact_main::MEMBER);
    std::fs::write(
        directory.join("session.json"),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
    store.append_actor_transcript(req).await.unwrap();
    let raw = std::fs::read(directory.join("session.json")).unwrap();
    assert!(compact_main::validate_full_main(&raw).unwrap().is_none());
}

#[tokio::test]
async fn actor_glob_pair_preserves_physical_prefix_and_rejects_every_unsupported_tail() {
    let home = tempfile::tempdir().unwrap();
    let (store, second, _, mut req) = setup(home.path(), true, ProviderFamily::OpenAi).await;
    let call = bamboo_domain::ToolCall {
        id: "actual-glob-call".into(),
        tool_type: "function".into(),
        function: bamboo_domain::FunctionCall {
            name: "Glob".into(),
            arguments: r#"{"pattern":"marker.txt","limit":1}"#.into(),
        },
    };
    req.messages = vec![
        Message::assistant("Inspecting the assigned path", Some(vec![call])),
        Message::tool_result("actual-glob-call", "marker.txt"),
        Message::assistant("verified", None),
    ];
    req.messages[1].metadata = Some(json!({
        "elapsed_ms": 17, "is_mutating": false, "auto_approved": true,
        "tool_name": "Glob", "success": true,
    }));
    let directory = actor_dir(home.path(), true);
    let before = files(&directory);
    for change in 0..22 {
        let mut bad = req.clone();
        match change {
            0 => {
                bad.messages[0].tool_calls.as_mut().unwrap()[0]
                    .function
                    .name = "Bash".into()
            }
            1 => bad.messages[1].tool_call_id = Some("foreign-call".into()),
            2 => bad.messages[1].tool_success = Some(false),
            3 => bad.messages[0].tool_calls.as_mut().unwrap()[0].id.clear(),
            4 => bad.messages.swap(0, 1),
            5 => {
                bad.messages.pop();
            }
            6 => bad.messages.push(bad.messages[1].clone()),
            7 => bad.messages[2].role = Role::User,
            8 => bad.messages[0].reasoning = Some("hidden".into()),
            9 => bad.messages[1].metadata = Some(json!({"authority":true})),
            10 => bad.messages[2].compressed = true,
            11 => {
                bad.messages[0].tool_calls.as_mut().unwrap()[0]
                    .function
                    .arguments = "invalid".into()
            }
            12 => {
                bad.messages[1]
                    .metadata
                    .as_mut()
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove("elapsed_ms");
            }
            13 => bad.messages[1].metadata.as_mut().unwrap()["private"] = json!(true),
            14 => bad.messages[1].metadata.as_mut().unwrap()["auto_approved"] = json!(false),
            15 => bad.messages[1].metadata.as_mut().unwrap()["is_mutating"] = json!(true),
            16 => bad.messages[1].metadata.as_mut().unwrap()["tool_name"] = json!("Bash"),
            17 => bad.messages[1].metadata.as_mut().unwrap()["success"] = json!(false),
            18 => bad.messages[1].metadata.as_mut().unwrap()["elapsed_ms"] = json!(-1),
            19 => bad.messages[1].metadata.as_mut().unwrap()["elapsed_ms"] = json!({"ms":17}),
            20 => bad.messages[0].metadata = req.messages[1].metadata.clone(),
            _ => bad.messages[2].metadata = req.messages[1].metadata.clone(),
        }
        unchanged_rejection(&store, &directory, bad).await;
    }
    let actual = store.append_actor_transcript(req.clone()).await.unwrap();
    let after = files(&directory);
    assert_eq!(
        before[1..],
        after[1..],
        "Runtime/proof/attachments unchanged"
    );
    assert_eq!(
        raw_field(
            std::str::from_utf8(before[0].as_ref().unwrap()).unwrap(),
            "provider_transcript"
        ),
        raw_field(
            std::str::from_utf8(after[0].as_ref().unwrap()).unwrap(),
            "provider_transcript"
        )
    );
    assert_eq!(
        serde_json::to_value(&actual.messages[req.expected_messages.len()..]).unwrap(),
        serde_json::to_value(&req.messages).unwrap()
    );
    let cold = second.load_session(ID).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&cold.messages).unwrap(),
        serde_json::to_value(&actual.messages).unwrap()
    );
}
