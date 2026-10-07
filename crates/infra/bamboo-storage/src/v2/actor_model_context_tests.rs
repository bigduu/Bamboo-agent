//! Real files/owned locks. Candidate mirrors below test representation only;
//! the Engine fixture separately calls the actual reconciliation generator.
use super::super::actor_transcript_tests::{
    actor_dir, claim, files, no_temps, physically_held, raw_field, released, Release,
    TranscriptWriteHook, ID,
};
use super::*;
use bamboo_domain::{
    ActorActivationFinish, ActorDirectoryPort, ContextBlockPriority, ContextBlockStability,
    Message, ModelContextEvent, ModelContextResetReason, SessionMessageId,
};
use chrono::Duration as ChronoDuration;
use serde_json::json;

fn block(text: &str) -> ContextBlock {
    ContextBlock::new(
        ContextBlockType::TaskSnapshot,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Task",
        text,
    )
}
fn admission(session: &mut Session) -> &mut bamboo_domain::SessionInboxAdmissionState {
    session
        .runtime_metadata
        .get_or_insert_with(Default::default)
        .session_inbox_admission
        .get_or_insert_with(Default::default)
}
fn ledger(session: &Session) -> &ModelContextState {
    session.model_context_state.as_ref().unwrap()
}
async fn assert_already(store: &SessionStoreV2, request: ActorModelContextCheckpoint) {
    assert!(matches!(
        store.checkpoint_actor_model_context(request).await.unwrap(),
        ActorModelContextOutcome::Already(_)
    ));
}
fn event(
    state: &mut ModelContextState,
    id: &str,
    block: Option<&ContextBlock>,
    ty: ContextBlockType,
) {
    let previous = state.baselines.get(&ty);
    let revision = previous.map_or(1, |p| p.revision + 1);
    let supersedes = previous.map(|p| p.revision);
    let digest = block.map_or_else(
        || removed_model_context_sha256(ty),
        model_context_block_sha256,
    );
    let event_id =
        deterministic_model_context_event_id(id, state.prefix_epoch, ty, revision, &digest);
    let (kind, text) = match block {
        Some(b) => (
            ModelContextEventKind::Snapshot,
            render_model_context_snapshot(
                &event_id,
                state.prefix_epoch,
                state.next_sequence,
                b,
                revision,
                supersedes,
            ),
        ),
        None => (
            ModelContextEventKind::Removed,
            render_model_context_removal(
                &event_id,
                state.prefix_epoch,
                state.next_sequence,
                ty,
                revision,
                supersedes.unwrap(),
            ),
        ),
    };
    let boundary = if state.events.is_empty() {
        None
    } else {
        state
            .transcript_item_sha256
            .last()
            .map(|hash| anchor(state.transcript_item_sha256.len() - 1, hash).unwrap())
    };
    state.events.push(ModelContextEvent {
        id: event_id,
        epoch: state.prefix_epoch,
        sequence: state.next_sequence,
        anchor_message_id: boundary,
        block_type: ty,
        revision,
        supersedes_revision: supersedes,
        kind,
        content_sha256: digest.clone(),
        rendered_text: text,
    });
    state.baselines.insert(
        ty,
        ContextBlockBaseline {
            revision,
            content_sha256: digest,
        },
    );
    state.next_sequence += 1;
}
fn snapshot(state: &mut ModelContextState, block: &ContextBlock) {
    event(state, ID, Some(block), block.block_type);
}
fn next(
    session: &Session,
    fence: ActorActivationFence,
    blocks: Vec<ContextBlock>,
) -> ActorModelContextCheckpoint {
    let mut candidate = ledger(session).clone();
    candidate.state_revision += 1;
    for b in &blocks {
        snapshot(&mut candidate, b);
    }
    ActorModelContextCheckpoint {
        fence,
        expected: session.clone(),
        candidate,
        snapshot_blocks: blocks,
    }
}
fn already(session: &Session, fence: ActorActivationFence) -> ActorModelContextCheckpoint {
    ActorModelContextCheckpoint {
        fence,
        expected: session.clone(),
        candidate: ledger(session).clone(),
        snapshot_blocks: vec![],
    }
}
fn checkpoint(
    store: &Arc<SessionStoreV2>,
    request: ActorModelContextCheckpoint,
) -> impl std::future::Future<Output = Result<ActorModelContextOutcome>> {
    let store = store.clone();
    async move { store.checkpoint_actor_model_context(request).await }
}
fn padded_leaf(leaf: &str) -> String {
    format!(
        "{}{} }}",
        &leaf[..leaf.len() - 1],
        " ".repeat(LEAF_LIMIT - leaf.len() - 1)
    )
}
async fn claim_fence(store: &SessionStoreV2, owner: &str) -> ActorActivationFence {
    let expires = Utc::now() + ChronoDuration::minutes(5);
    store
        .claim_activation(&claim(owner, expires))
        .await
        .unwrap()
        .fence()
}
type Fixture = (
    Arc<SessionStoreV2>,
    Arc<SessionStoreV2>,
    Session,
    ActorModelContextCheckpoint,
);
async fn setup(home: &Path, child: bool) -> Fixture {
    setup_chain(home, child, false).await
}
async fn setup_chain(home: &Path, child: bool, nested: bool) -> Fixture {
    let first = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let mut root = Session::new(if child { "root" } else { ID }, "model");
    if nested {
        root.set_project_id_meta("project-a");
    }
    if child {
        first.save_session(&root).await.unwrap();
    }
    let parent = if nested {
        let mut middle = Session::new_child_of("middle", &root, "model", "middle");
        middle.set_project_id_meta("project-a");
        first.save_session(&middle).await.unwrap();
        middle
    } else {
        root
    };
    let mut session = if child {
        Session::new_child_of(ID, &parent, "model", "child")
    } else {
        parent
    };
    if nested {
        session.set_project_id_meta("project-a");
    }
    session.messages = vec![
        Message::system("system"),
        Message::user("history 🪷".repeat(20000)),
    ];
    session
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &"a".repeat(64),
        )
        .unwrap();
    let mut ledger = ModelContextState {
        state_revision: 1,
        cache_scope_sha256: Some("c".repeat(64)),
        transcript_item_sha256: vec!["b".repeat(64)],
        ..Default::default()
    };
    snapshot(&mut ledger, &block("v1"));
    session.model_context_state = Some(ledger);
    admission(&mut session).record(SessionMessageId::parse("admitted").unwrap(), 1);
    first.save_session(&session).await.unwrap();
    let dir = actor_dir(home, child);
    std::fs::create_dir_all(dir.join("attachments")).unwrap();
    std::fs::write(
        dir.join("attachments/unchanged.bin"),
        b"unchanged attachment",
    )
    .unwrap();
    for name in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = dir.join(name);
        let raw = std::fs::read_to_string(&path).unwrap().replacen(
            '\n',
            "\n\"unknown.future\": { \"lexeme\":1e0, \"escape\":\"\\u0061\" },",
            1,
        );
        std::fs::write(path, raw).unwrap();
    }
    let fence = claim_fence(&first, "ledger-owner").await;
    first.start_activation(&fence, Utc::now()).await.unwrap();
    let second = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let current = load(&second, ID).await;
    let request = next(&current, fence, vec![block("v2")]);
    (first, second, current, request)
}
async fn rejected(
    store: &SessionStoreV2,
    dir: &Path,
    request: ActorModelContextCheckpoint,
) -> Error {
    let old = files(dir);
    let error = store
        .checkpoint_actor_model_context(request)
        .await
        .unwrap_err();
    assert_eq!(old, files(dir));
    no_temps(dir);
    error
}
fn committed(outcome: ActorModelContextOutcome) -> Session {
    match outcome {
        ActorModelContextOutcome::Committed(s) | ActorModelContextOutcome::Already(s) => s,
    }
}
async fn apply(store: &SessionStoreV2, request: ActorModelContextCheckpoint) -> Session {
    committed(store.checkpoint_actor_model_context(request).await.unwrap())
}
async fn load(store: &SessionStoreV2, id: &str) -> Session {
    store.load_session(id).await.unwrap().unwrap()
}
fn rewrite_leaf(path: &Path, leaf: &str) {
    let raw = std::fs::read_to_string(path).unwrap();
    let fields = object(&raw).unwrap().0;
    let output = splice(&raw, vec![(*fields.get(LEDGER).unwrap(), leaf.into())]).unwrap();
    std::fs::write(path, output).unwrap();
}
#[tokio::test]
async fn root_child_raw_leaf_only_commit_remove_and_already_preserve_every_other_byte() {
    for child in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, mut expected, request) = setup(home.path(), child).await;
        // This changes only the serde-skipped admission index, not durable JSON.
        let admission = admission(&mut expected);
        let index_before = admission.clone();
        admission.rebuild_index();
        assert_ne!(*admission, index_before);
        let mut request = request;
        assert_eq!(
            serde_json::to_value(&expected).unwrap(),
            serde_json::to_value(&request.expected).unwrap()
        );
        request.expected = expected;
        let dir = actor_dir(home.path(), child);
        let before = files(&dir);
        let index = std::fs::read(home.path().join("sessions.json")).unwrap();
        let old_side = String::from_utf8(before[1].clone().unwrap()).unwrap();
        let fence = request.fence.clone();
        let actual = apply(&first, request).await;
        assert_eq!(ledger(&actual).events.len(), 2);
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(load(&second, ID).await).unwrap()
        );
        let after = files(&dir);
        assert_eq!(after[0], before[0]);
        assert_eq!(&after[2..], &before[2..]);
        let new_side = String::from_utf8(after[1].clone().unwrap()).unwrap();
        let normalized = splice(
            &new_side,
            vec![(
                *object(&new_side).unwrap().0.get(LEDGER).unwrap(),
                raw_field(&old_side, LEDGER).into(),
            )],
        )
        .unwrap();
        assert_eq!(normalized, old_side);
        let mut removal = next(&actual, fence.clone(), vec![]);
        event(
            &mut removal.candidate,
            ID,
            None,
            ContextBlockType::TaskSnapshot,
        );
        let removed = apply(&second, removal).await;
        let resurrected = apply(&first, next(&removed, fence.clone(), vec![block("v3")])).await;
        let retry = already(&resurrected, fence);
        let snapshot = files(&dir);
        assert_already(&second, retry).await;
        assert_eq!(snapshot, files(&dir));
        let mut unowned = resurrected;
        unowned.model_context_state.as_mut().unwrap().state_revision += 1;
        assert!(first.save_session(&unowned).await.is_err());
        assert!(first.save_runtime_state(&unowned).await.is_err());
        assert_eq!(snapshot, files(&dir));
        assert_eq!(
            index,
            std::fs::read(home.path().join("sessions.json")).unwrap()
        );
    }
}
#[tokio::test]
async fn current_ancestor_delete_birth_project_and_gap_reject_new_and_already() {
    for change in 0..4 {
        let home = tempfile::tempdir().unwrap();
        let (store, _, current, request) = setup_chain(home.path(), true, true).await;
        let middle_dir = home.path().join("sessions/root/children/middle");
        match change {
            0 => {
                assert!(store.delete_session("middle").await.unwrap());
                assert!(store.load_session("middle").await.unwrap().is_none());
            }
            1 | 3 => {
                let mut root = load(&store, "root").await;
                for _ in 0..if change == 3 { 2 } else { 1 } {
                    if change == 1 {
                        root.set_project_id_meta("project-b");
                    }
                    root.metadata_version += 1;
                    store.save_session(&root).await.unwrap();
                }
            }
            _ => {
                // Trusted source fault: both parent files agree on another birth.
                for name in ["session.json", RUNTIME_SIDECAR_FILE] {
                    let path = middle_dir.join(name);
                    let mut middle: Session =
                        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    middle.created_at += ChronoDuration::nanoseconds(1);
                    assert!(middle.created_at < current.created_at);
                    let bytes = if name == "session.json" {
                        compact_main::serialize_main(&middle).unwrap()
                    } else {
                        serde_json::to_vec(&middle).unwrap()
                    };
                    std::fs::write(path, bytes).unwrap();
                }
            }
        }
        assert!(store.load_session(ID).await.unwrap().is_some());
        let root_dir = home.path().join("sessions/root");
        let before = (files(&root_dir), files(&middle_dir));
        for rejected_request in [request.clone(), already(&current, request.fence.clone())] {
            rejected(&store, &actor_dir(home.path(), true), rejected_request).await;
        }
        assert_eq!(before, (files(&root_dir), files(&middle_dir)));
    }
}
#[tokio::test]
async fn representation_full_expected_witness_and_complete_fence_reject_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, request) = setup(home.path(), false).await;
    let dir = actor_dir(home.path(), false);
    for change in 0..30 {
        let mut r = request.clone();
        let n = &mut r.candidate;
        match change {
            0 => n.schema_version += 1,
            1 => n.state_revision += 1,
            2 => n.prefix_epoch += 1,
            3 => n.next_sequence += 1,
            4 => n.cache_scope_sha256 = None,
            5 => n.transcript_item_sha256.push("d".repeat(64)),
            6 => n.last_reset_reason = Some(ModelContextResetReason::Compression),
            7 => n.events[0].rendered_text.push('x'),
            8 => n.events[1].id.push('x'),
            9 => n.events[1].epoch += 1,
            10 => n.events[1].sequence += 1,
            11 => n.events[1].anchor_message_id = None,
            12 => n.events[1].revision += 1,
            13 => n.events[1].supersedes_revision = None,
            14 => n.events[1].content_sha256 = "A".repeat(64),
            15 => n.events[1].rendered_text.push('x'),
            16 => {
                n.baselines.clear();
            }
            17 => r.snapshot_blocks.clear(),
            18 => r.snapshot_blocks.push(block("duplicate")),
            19 => r.snapshot_blocks[0].metadata = Some(json!(null)),
            20 => r.snapshot_blocks[0].content.push('x'),
            21 => r.expected.messages[0].content.push('x'),
            22 => {
                r.expected.metadata.insert("unseen".into(), "value".into());
            }
            23 => r.expected.created_at += ChronoDuration::seconds(1),
            24 => r.fence.activation_id.push('x'),
            25 => r.fence.lease_epoch += 1,
            26 => r.fence.run_id.push('x'),
            27 => r.fence.lease_owner.push('x'),
            28 => r.fence.attempt += 1,
            _ => r.fence.schema_version += 1,
        }
        rejected(&store, &dir, r).await;
    }
    // Suffix must be strictly ordered and each block type appear only once.
    let mut task = block("third");
    task.block_type = ContextBlockType::Workspace;
    let mut r = next(
        &request.expected,
        request.fence.clone(),
        vec![block("v2"), task],
    );
    rejected(&store, &dir, r.clone()).await;
    r.candidate.events.swap(1, 2);
    rejected(&store, &dir, r).await;
}
#[tokio::test]
async fn strict_raw_source_and_physical_runtime_controls_fail_closed_even_when_overlay_hides_them()
{
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, request) = setup(home.path(), false).await;
    let dir = actor_dir(home.path(), false);
    let path = dir.join(RUNTIME_SIDECAR_FILE);
    let original = std::fs::read_to_string(&path).unwrap();
    let base: Session = serde_json::from_str(&original).unwrap();
    for change in 0..8 {
        let mut s = base.clone();
        match change {
            0 => s.messages.push(Message::user("hidden")),
            1 => s.provider_transcript = request.expected.provider_transcript.clone(),
            2 => {
                s.runtime_metadata
                    .get_or_insert_with(Default::default)
                    .session_inbox_admission = Some(Default::default())
            }
            3 => s.model_context_state = None,
            4 => s.model_context_state.as_mut().unwrap().cache_scope_sha256 = None,
            5 => s.model_context_state.as_mut().unwrap().state_revision = u64::MAX,
            6 => s.model_context_state.as_mut().unwrap().events[0]
                .id
                .push('x'),
            _ => s.created_at += ChronoDuration::seconds(1),
        }
        std::fs::write(&path, serde_json::to_vec(&s).unwrap()).unwrap();
        rejected(&store, &dir, request.clone()).await;
    }
    for raw in [
        original.replacen(
            "\"state_revision\":",
            "\"state_revision\":1,\"state_revision\":",
            1,
        ),
        original.replacen("\"baselines\":", "\"unknown_ledger\":0,\"baselines\":", 1),
        original.replacen("\"revision\":", "\"unknown_event\":0,\"revision\":", 1),
    ] {
        std::fs::write(&path, raw).unwrap();
        rejected(&store, &dir, request.clone()).await;
    }
    std::fs::write(&path, &original).unwrap();
    let main = dir.join("session.json");
    let original_main = std::fs::read(&main).unwrap();
    let mut value: Session = serde_json::from_slice(&original_main).unwrap();
    value.provider_transcript = ProviderTranscriptState::default();
    std::fs::write(&main, compact_main::serialize_main(&value).unwrap()).unwrap();
    rejected(&store, &dir, request).await;
}
#[test]
fn independent_numeric_bounds_before_decode_render_and_encode() {
    let state = ModelContextState {
        state_revision: 1,
        cache_scope_sha256: Some("c".repeat(64)),
        ..Default::default()
    };
    let mut writer = BoundedBytes {
        bytes: Vec::new(),
        limit: LEAF_LIMIT,
        used: 0,
    };
    writer.write_all(&vec![b'x'; LEAF_LIMIT]).unwrap();
    assert!(writer.write_all(b"x").is_err());
    assert_eq!(writer.bytes.len(), LEAF_LIMIT);
    writer.used = usize::MAX;
    assert!(writer.write_all(b"x").is_err());
    let mut exact_json = state.clone();
    snapshot(&mut exact_json, &block("small"));
    exact_json.events[0].rendered_text.clear();
    let overhead = bounded_json(&exact_json).unwrap().len();
    let controls = (LEAF_LIMIT - overhead) / 6;
    let tail = (LEAF_LIMIT - overhead) % 6;
    exact_json.events[0].rendered_text = "\0".repeat(controls) + &"x".repeat(tail);
    assert_eq!(bounded_json(&exact_json).unwrap().len(), LEAF_LIMIT);
    exact_json.events[0].rendered_text.push('x');
    assert!(bounded_json(&exact_json).is_err());
    let mut expansion = state.clone();
    snapshot(&mut expansion, &block("small"));
    expansion.events[0].rendered_text = "\0".repeat(MAX_MODEL_CONTEXT_RENDERED_BYTES);
    assert!(validate_state(ID, &expansion).is_ok());
    assert!(bounded_json(&expansion).is_err());
    expansion.events[0].rendered_text.push('x');
    assert!(validate_state(ID, &expansion).is_err());
    let mut events = state.clone();
    for i in 0..MAX_MODEL_CONTEXT_EVENTS {
        snapshot(&mut events, &block(&i.to_string()));
    }
    assert!(validate_state(ID, &events).is_ok());
    snapshot(&mut events, &block("overflow"));
    assert!(validate_state(ID, &events).is_err());
    // A legal ledger with whitespace pad has an exact raw 4MiB leaf span.
    let leaf = bounded_json(&state).unwrap();
    let padded = padded_leaf(&leaf);
    assert_eq!(padded.len(), LEAF_LIMIT);
    assert!(strict_leaf(&padded).is_ok());
    assert!(strict_leaf(&format!("{} }}", &padded[..padded.len() - 1])).is_err());
}
#[tokio::test]
async fn actual_raw_cap_exact_boundary_and_witness_pretrim_limits() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _, request) = setup(home.path(), false).await;
    let dir = actor_dir(home.path(), false);
    for name in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = dir.join(name);
        let original = std::fs::read(&path).unwrap();
        let leaf = raw_field(std::str::from_utf8(&original).unwrap(), LEDGER);
        let padded = padded_leaf(leaf);
        rewrite_leaf(&path, &padded);
        let retry = already(&request.expected, request.fence.clone());
        assert_already(&store, retry).await;
        rewrite_leaf(&path, &format!("{} }}", &padded[..padded.len() - 1]));
        rejected(&store, &dir, request.clone()).await;
        std::fs::write(path, original).unwrap();
    }
    let mut r = request.clone();
    // Padding trimmed by the renderer still counts against the raw witness cap.
    let initial = r.snapshot_blocks[0].title.len() + r.snapshot_blocks[0].content.len();
    r.snapshot_blocks[0]
        .content
        .push_str(&" ".repeat(WITNESS_LIMIT - initial));
    assert!(validate_request(&r, &source(&dir, ID, SessionKind::Root, ID).unwrap()).is_ok());
    r.snapshot_blocks[0].content.push(' ');
    rejected(&store, &dir, r).await;
    let mut r = request.clone();
    r.snapshot_blocks = vec![block("v2"); MAX_MODEL_CONTEXT_EVENTS + 1];
    rejected(&store, &dir, r).await;
    let side = dir.join(RUNTIME_SIDECAR_FILE);
    let mut saturated = ledger(&request.expected).clone();
    saturated.state_revision = u64::MAX;
    rewrite_leaf(&side, &bounded_json(&saturated).unwrap());
    let mut r = request;
    r.expected.model_context_state = Some(saturated);
    r.candidate.state_revision = u64::MAX;
    assert!(matches!(
        rejected(&store, &dir, r).await,
        Error::Unsupported
    ));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_before_replace_expiry_and_source_change_reject_without_publication() {
    for change in 0..3 {
        let expire = change == 0;
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, request) = setup(home.path(), change == 2).await;
        let dir = actor_dir(home.path(), change == 2);
        let expires = Utc::now() + ChronoDuration::milliseconds(500);
        if expire {
            let path = dir.join("actor-authority.json");
            let mut entry: bamboo_domain::ActorDirectoryEntry =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            entry.activation.as_mut().unwrap().lease_expires_at = expires;
            std::fs::write(path, serde_json::to_vec(&entry).unwrap()).unwrap();
        }
        let hook =
            TranscriptWriteHook::install(&store, DurableWritePhase::BeforeReplace, false, false);
        let _release = Release(hook.clone());
        let task = tokio::spawn(checkpoint(&store, request));
        hook.entered();
        physically_held(&store);
        if expire {
            while Utc::now() <= expires {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            let path = if change == 2 {
                home.path().join("sessions/root/session.json")
            } else {
                dir.join("session.json")
            };
            let raw = std::fs::read_to_string(&path).unwrap() + " ";
            std::fs::write(path, raw).unwrap();
        }
        let before = files(&dir);
        hook.release();
        assert!(task.await.unwrap().is_err());
        assert_eq!(before, files(&dir));
        no_temps(&dir);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_and_successor_are_serialized_by_actual_l_task_session_guards() {
    let home = tempfile::tempdir().unwrap();
    let (first, second, _, request) = setup(home.path(), false).await;
    let old_fence = request.fence.clone();
    let hook = TranscriptWriteHook::install(&first, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let caller = tokio::spawn(checkpoint(&first, request));
    hook.entered();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    physically_held(&first);
    let finish = tokio::spawn({
        let second = second.clone();
        let f = old_fence.clone();
        async move {
            second
                .finish_activation(&f, Utc::now(), ActorActivationFinish::Succeeded)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!finish.is_finished());
    hook.release();
    finish.await.unwrap().unwrap();
    released(&first);
    let new_fence = claim_fence(&second, "replacement").await;
    second
        .start_activation(&new_fence, Utc::now())
        .await
        .unwrap();
    let current = load(&second, ID).await;
    let dir = actor_dir(home.path(), false);
    rejected(&first, &dir, next(&current, old_fence, vec![block("v3")])).await;
    let retry = already(&current, new_fence);
    assert_already(&second, retry).await;
}
#[test]
fn runtime_stop_keeps_started_complete_std_job_locked_until_commit() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (first, second, _, request) = runtime.block_on(setup(home.path(), false));
    let hook = TranscriptWriteHook::install(&first, DurableWritePhase::BeforeReplace, false, false);
    let _release = Release(hook.clone());
    let caller = runtime.spawn(checkpoint(&first, request));
    hook.entered();
    runtime.shutdown_background();
    drop(caller);
    physically_held(&first);
    hook.release();
    released(&first);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .block_on(second.load_session(ID))
            .unwrap()
            .unwrap()
            .model_context_state
            .unwrap()
            .events
            .len(),
        2
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postreplace_unconfirmed_requires_reload_and_current_exact_already() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, _, request) = setup(home.path(), false).await;
        let fence = request.fence.clone();
        let hook = TranscriptWriteHook::install(&first, phase, true, false);
        let _release = Release(hook.clone());
        let caller = tokio::spawn(checkpoint(&first, request));
        hook.entered();
        hook.release();
        let error = caller.await.unwrap().unwrap_err();
        if phase == DurableWritePhase::BeforeReplace {
            assert!(matches!(error, Error::BeforePublication(_)));
        } else {
            assert!(matches!(error, Error::OutcomeUnconfirmed(_)));
        }
        let actual = load(&second, ID).await;
        assert_eq!(
            ledger(&actual).events.len(),
            if phase == DurableWritePhase::BeforeReplace {
                1
            } else {
                2
            }
        );
        let retry = already(&actual, fence);
        assert_already(&second, retry).await;
    }
}
