//! Real Main/cur files, actual Actor transitions, physical barriers and cold Stores.
use super::actor_transcript::{object, splice};
use super::actor_transcript_tests::TranscriptWriteHook;
use super::*;
use bamboo_domain::session::provider_transcript::{
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin,
};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, Message,
    SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits, SessionInboxPort,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, Storage,
};
use chrono::Duration as LeaseDuration;
use serde_json::json;
use std::collections::BTreeMap;

const ID: &str = "input-actor";
const DEADLINE: Duration = Duration::from_secs(10);
struct Fixture {
    store: Arc<SessionStoreV2>,
    other: Arc<SessionStoreV2>,
    inbox: crate::FileSessionInbox,
    request: ActorInputCheckpoint,
    directory: PathBuf,
}
impl Fixture {
    fn request_from(&self, session: &Session) -> ActorInputCheckpoint {
        request(
            session,
            self.request.fence.clone(),
            self.request.claim.clone(),
        )
    }
}
fn request(
    session: &Session,
    fence: bamboo_domain::ActorActivationFence,
    claim: bamboo_domain::SessionInboxOwnedClaim,
) -> ActorInputCheckpoint {
    ActorInputCheckpoint {
        fence,
        claim,
        expected_created_at: session.created_at,
        expected_messages: session.messages.clone(),
        expected_provider_transcript: session.provider_transcript.clone(),
        expected_admission: session
            .runtime_metadata
            .as_ref()
            .and_then(|r| r.session_inbox_admission.clone()),
    }
}
fn activation_claim(
    claim: &bamboo_domain::SessionInboxOwnedClaim,
    run: &str,
    now: DateTime<Utc>,
) -> ActorActivationClaim {
    ActorActivationClaim {
        actor_id: claim.claim.envelope.target_session_id.clone(),
        run_id: run.into(),
        lease_owner: claim.lease.consumer.as_str().into(),
        lease_expires_at: claim.lease.expires_at,
        inbox_generation: claim.claim.generation,
        placement_ref: None,
        now,
    }
}
fn lease_request(consumer: SessionInboxConsumerId, now: DateTime<Utc>) -> SessionInboxLeaseRequest {
    SessionInboxLeaseRequest {
        consumer,
        now,
        duration: LeaseDuration::minutes(5),
    }
}
async fn fixture(home: &Path, child: bool, start: bool) -> Fixture {
    let store = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let parent = Session::new(if child { "parent" } else { ID }, "model");
    if child {
        store.save_session(&parent).await.unwrap();
    }
    let mut session = if child {
        Session::new_child_of(ID, &parent, "model", "child")
    } else {
        parent
    };
    session.messages = vec![
        Message::system("system"),
        Message::user("prior"),
        Message::assistant("prior output", None),
    ];
    session
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &"a".repeat(64),
        )
        .unwrap();
    let items = [
        (ProviderTranscriptAuthor::Model, json!({"type":"tool_search_call","id":"old-call","execution":"server","call_id":"old","status":"completed","arguments":{"query":"orders"}})),
        (ProviderTranscriptAuthor::ToolResult, json!({"type":"tool_search_output","id":"old-output","execution":"server","call_id":"old","status":"completed","tools":[{"type":"function","name":"get_orders"}]})),
        (ProviderTranscriptAuthor::Model, json!({"type":"message","id":"old-message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"done","annotations":[]}]})),
    ].into_iter().map(|(author,payload)| ProviderTranscriptItem::try_from_payload(ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1, ProviderTranscriptOrigin::Provider, author, payload).unwrap()).collect();
    session
        .append_provider_transcript_group(session.messages[2].id.clone(), None, items)
        .unwrap();
    store.save_session(&session).await.unwrap();
    let directory = home.join(if child {
        "sessions/parent/children/input-actor"
    } else {
        "sessions/input-actor"
    });
    let main = directory.join("session.json");
    let raw = std::fs::read_to_string(&main).unwrap().replacen(
        '\n',
        "\n\"future_control\":{\"number\":1e0,\"escaped\":\"\\u0061\"},",
        1,
    );
    std::fs::write(&main, raw).unwrap();
    let inbox = crate::FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let envelope = SessionMessageEnvelope::user_input(ID, "exact input 🪷");
    inbox
        .deliver_with_activation_intent(
            &envelope,
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = lease_request(SessionInboxConsumerId::new(), Utc::now());
    let claim = inbox
        .claim_owned(ID, 1, None, &lease)
        .await
        .unwrap()
        .remove(0);
    let activation = store
        .claim_activation(&activation_claim(&claim, "first-run", lease.now))
        .await
        .unwrap();
    if start {
        store
            .start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
    }
    let current = store.load_session(ID).await.unwrap().unwrap();
    // Constructor recovery is not allowed to race the parked publication.
    let other = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    Fixture {
        store,
        other,
        inbox,
        request: request(&current, activation.fence(), claim),
        directory,
    }
}
fn protected(f: &Fixture) -> BTreeMap<String, Vec<u8>> {
    let mut result = BTreeMap::new();
    for name in [
        "runtime.json",
        "actor-authority.json",
        "actor-authority.initialized.json",
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
    ] {
        let path = f.directory.join(name);
        if path.exists() {
            result.insert(name.into(), std::fs::read(path).unwrap());
        }
    }
    let main = std::fs::read_to_string(f.directory.join("session.json")).unwrap();
    let fields = object(&main).unwrap().0;
    for name in [
        "provider_transcript",
        "future_control",
        compact_main::MEMBER,
    ] {
        result.insert(name.into(), fields[name].get().as_bytes().to_vec());
    }
    let dir = f.directory.join("inbox");
    for name in [
        "generation",
        "activation-generation",
        "interrupt-generation",
    ] {
        result.insert(name.into(), std::fs::read(dir.join(name)).unwrap());
    }
    result.insert(
        "cur".into(),
        std::fs::read(dir.join("cur").join(&f.request.claim.claim.claim_id)).unwrap(),
    );
    result
}
async fn reject_unchanged(f: &Fixture, request: ActorInputCheckpoint) -> ActorInputCheckpointError {
    let main = std::fs::read(f.directory.join("session.json")).unwrap();
    let protected = protected(f);
    let error = f.inbox.checkpoint_actor_input(request).await.unwrap_err();
    assert_eq!(
        std::fs::read(f.directory.join("session.json")).unwrap(),
        main
    );
    assert_eq!(self::protected(f), protected);
    error
}
fn lock_paths(f: &Fixture) -> [PathBuf; 4] {
    [
        f.store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        f.store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        f.store.session_write_lock_path(ID),
        f.directory.join("inbox/.session-inbox.lock"),
    ]
}
fn release_locks(f: &Fixture) {
    let paths = lock_paths(f);
    let end = Instant::now() + DEADLINE;
    loop {
        let free = paths.iter().all(|p| {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(p)
                .unwrap();
            if FileExt::try_lock_exclusive(&file).is_err() {
                return false;
            }
            FileExt::unlock(&file).unwrap();
            true
        });
        if free {
            break;
        }
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test]
async fn unowned_actor_claim_seed_then_confirmation_preserves_cursor_and_ack_order() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
    let parent = Session::new("seed-parent", "model");
    let mut child = Session::new_child_of("seed-child", &parent, "model", "task");
    child.add_message(Message::user("initial task"));
    store.save_session(&parent).await.unwrap();
    store.save_session(&child).await.unwrap();
    store.ensure_actor(&child.id).await.unwrap();
    let inbox = crate::FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let envelope = SessionMessageEnvelope::user_input(&child.id, "warm correction");
    let receipt = inbox.deliver(&envelope).await.unwrap();
    inbox
        .mark_activation_eligible(
            &child.id,
            receipt.generation,
            bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
        )
        .await
        .unwrap();
    let claim = inbox
        .claim_for_turn(&child.id, 1, Some("warm-run"))
        .await
        .unwrap()
        .remove(0);
    let now = Utc::now();
    let activation = store
        .claim_activation(&ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: "warm-run".into(),
            lease_owner: "remote-host".into(),
            lease_expires_at: now + LeaseDuration::seconds(80),
            inbox_generation: 0,
            placement_ref: Some(bamboo_domain::ActorPlacementRef {
                class: bamboo_domain::ActorPlacementClass::Remote,
                lease_id: "slot-1".into(),
                slot_epoch: Some(1),
            }),
            now,
        })
        .await
        .unwrap();
    let fence = activation.fence();
    store.start_activation(&fence, now).await.unwrap();
    let request = |session: &Session| ActorClaimContextSeed {
        fence: fence.clone(),
        expected_created_at: session.created_at,
        expected_messages: session.messages.clone(),
        expected_provider_transcript: session.provider_transcript.clone(),
        expected_admission: session.session_inbox_admission().cloned(),
        claims: vec![claim.clone()],
    };

    let mut unfenced = child.clone();
    unfenced.add_message(envelope.to_provider_message().unwrap());
    assert!(store.save_session(&unfenced).await.is_err());
    let seeded = store
        .seed_actor_claim_context(inbox.clone(), request(&child))
        .await
        .unwrap();
    assert_eq!(seeded.messages.len(), child.messages.len() + 1);
    assert!(seeded
        .messages
        .iter()
        .any(|message| bamboo_domain::is_matching_session_message(message, &envelope)));
    assert!(!seeded
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&envelope.id)));
    assert!(!inbox.was_admitted(&child.id, &envelope.id).await.unwrap());
    assert_eq!(inbox.inspect(&child.id).await.unwrap().claimed, 1);

    let confirmed = store
        .confirm_actor_claim_context(inbox.clone(), request(&seeded))
        .await
        .unwrap();
    assert!(confirmed
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&envelope.id)));
    assert!(!inbox.was_admitted(&child.id, &envelope.id).await.unwrap());
    inbox.ack(&child.id, &claim).await.unwrap();
    assert!(inbox.was_admitted(&child.id, &envelope.id).await.unwrap());
    store
        .finish_activation(&fence, Utc::now(), ActorActivationFinish::Succeeded)
        .await
        .unwrap();
    let reopened = SessionStoreV2::new(temp.path().into()).await.unwrap();
    let durable = reopened.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(durable.messages.len(), 2);
    assert!(durable
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&envelope.id)));
    assert!(reopened
        .confirm_actor_claim_context(inbox, request(&durable))
        .await
        .is_err());
}
fn held(f: &Fixture) {
    for path in lock_paths(f) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert_eq!(
            FileExt::try_lock_exclusive(&file).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
struct Release(Arc<TranscriptWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn actor_input_root_child_cold_new_already_and_complete_prefix() {
    for child in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let f = fixture(home.path(), child, true).await;
        let before = protected(&f);
        let old = std::fs::read_to_string(f.directory.join("session.json")).unwrap();
        let committed = f
            .inbox
            .checkpoint_actor_input(f.request.clone())
            .await
            .unwrap();
        assert_eq!(committed.status, ActorInputCheckpointStatus::NewCheckpoint);
        assert_eq!(committed.envelope, f.request.claim.claim.envelope);
        assert_eq!(committed.generation, f.request.claim.claim.generation);
        assert_eq!(protected(&f), before);
        let raw = std::fs::read_to_string(f.directory.join("session.json")).unwrap();
        let old_fields = object(&old).unwrap().0;
        let new_fields = object(&raw).unwrap().0;
        let prefix = old_fields["messages"].get();
        assert!(new_fields["messages"]
            .get()
            .starts_with(&prefix[..prefix.len() - 1]));
        let user = committed.session.messages.last().unwrap();
        assert_eq!(user.id, committed.envelope.id.as_str());
        assert!(!bamboo_domain::is_matching_session_message(
            user,
            &committed.envelope
        ));
        assert!(committed
            .session
            .session_inbox_admission()
            .unwrap()
            .contains(&committed.envelope.id));
        assert!(matches!(
            reject_unchanged(&f, f.request.clone()).await,
            ActorInputCheckpointError::PrefixConflict
        ));
        let cold = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let current = cold.load_session(ID).await.unwrap().unwrap();
        let req = f.request_from(&current);
        let inbox = crate::FileSessionInbox::new(cold, SessionInboxLimits::default());
        let recovered = inbox.checkpoint_actor_input(req).await.unwrap();
        assert_eq!(
            recovered.status,
            ActorInputCheckpointStatus::AlreadyCheckpointed
        );
        assert_eq!(
            serde_json::to_value(recovered.session).unwrap(),
            serde_json::to_value(committed.session).unwrap()
        );
        assert_eq!(
            std::fs::read(f.directory.join("session.json")).unwrap(),
            raw.as_bytes()
        );
        assert!(
            !f.directory.join("inbox/admitted").exists()
                || std::fs::read_dir(f.directory.join("inbox/admitted"))
                    .unwrap()
                    .next()
                    .is_none()
        );
    }
}

#[tokio::test]
async fn actor_input_reserved_and_current_authority_prefix_claim_rejections() {
    let home = tempfile::tempdir().unwrap();
    let f = fixture(home.path(), false, false).await;
    assert!(matches!(
        reject_unchanged(&f, f.request.clone()).await,
        ActorInputCheckpointError::Actor(_)
    ));
    f.store
        .start_activation(&f.request.fence, Utc::now())
        .await
        .unwrap();
    for kind in 0..9 {
        let mut bad = f.request.clone();
        match kind {
            0 => bad.expected_created_at += LeaseDuration::seconds(1),
            1 => bad.expected_messages.clear(),
            2 => bad.expected_provider_transcript = Default::default(),
            3 => bad.expected_admission = Some(Default::default()),
            4 => bad.claim.lease.consumer = SessionInboxConsumerId::new(),
            5 => bad.claim.lease.expires_at += LeaseDuration::seconds(1),
            6 => {
                bad.claim.claim.activation_policy =
                    bamboo_domain::SessionActivationPolicy::InterruptSpecificWait
            }
            7 => bad.claim.claim.generation += 1,
            _ => bad.fence.lease_owner = "foreign-owner".into(),
        }
        reject_unchanged(&f, bad).await;
    }
    let actor = f.directory.join("actor-authority.json");
    let original = std::fs::read(&actor).unwrap();
    std::fs::write(&actor, b"invalid actor record").unwrap();
    reject_unchanged(&f, f.request.clone()).await;
    std::fs::write(&actor, original).unwrap();
    let marker = f.directory.join("actor-authority.initialized.json");
    let original = std::fs::read(&marker).unwrap();
    std::fs::remove_file(&marker).unwrap();
    reject_unchanged(&f, f.request.clone()).await;
    std::fs::write(marker, original).unwrap();
    let proof = f
        .directory
        .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    let original = std::fs::read(&proof).unwrap();
    std::fs::write(&proof, b"pending").unwrap();
    reject_unchanged(&f, f.request.clone()).await;
    std::fs::write(proof, original).unwrap();
    let runtime = f.directory.join("runtime.json");
    let original = std::fs::read(&runtime).unwrap();
    let mut side: Session = serde_json::from_slice(&original).unwrap();
    side.runtime_metadata
        .get_or_insert_with(Default::default)
        .session_inbox_admission = Some(Default::default());
    std::fs::write(&runtime, serde_json::to_vec(&side).unwrap()).unwrap();
    let current = f.store.load_session(ID).await.unwrap().unwrap();
    assert!(current.session_inbox_admission().is_none());
    assert!(matches!(
        reject_unchanged(&f, f.request_from(&current)).await,
        ActorInputCheckpointError::Unsupported
    ));
    std::fs::write(runtime, original).unwrap();
    let mut unsupported = SessionMessageEnvelope::user_input(ID, "multipart unsupported");
    if let bamboo_domain::SessionMessageBody::Content(content) = &mut unsupported.body {
        content.parts.push(bamboo_domain::MessagePart::Text {
            text: "part".into(),
        });
    }
    f.inbox
        .deliver_with_activation_intent(
            &unsupported,
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = lease_request(f.request.claim.lease.consumer.clone(), Utc::now());
    let claim = f
        .inbox
        .claim_owned(ID, 10, None, &lease)
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.claim.envelope.id == unsupported.id)
        .unwrap();
    let mut req = f.request.clone();
    req.claim = claim;
    assert!(matches!(
        reject_unchanged(&f, req).await,
        ActorInputCheckpointError::Unsupported
    ));
}

#[tokio::test]
async fn actor_input_replacement_owner_same_generation_intent_policy_and_cursor_eviction() {
    let home = tempfile::tempdir().unwrap();
    let mut f = fixture(home.path(), false, true).await;
    let committed = f
        .inbox
        .checkpoint_actor_input(f.request.clone())
        .await
        .unwrap();
    let stale = f.request_from(&committed.session);
    f.store
        .finish_activation(
            &f.request.fence,
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let other = crate::FileSessionInbox::new(f.other.clone(), SessionInboxLimits::default());
    let lease = lease_request(
        SessionInboxConsumerId::new(),
        f.request.claim.lease.expires_at,
    );
    let claim = other
        .claim_owned(ID, 1, None, &lease)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claim.claim.generation, f.request.claim.claim.generation);
    assert_eq!(claim.claim.envelope, f.request.claim.claim.envelope);
    assert_eq!(
        claim.claim.activation_policy,
        f.request.claim.claim.activation_policy
    );
    assert_ne!(claim.lease.incarnation, f.request.claim.lease.incarnation);
    assert!(other
        .ack_owned(ID, &f.request.claim, lease.now)
        .await
        .is_err());
    let activation = f
        .other
        .claim_activation(&activation_claim(&claim, "replacement", lease.now))
        .await
        .unwrap();
    f.other
        .start_activation(&activation.fence(), lease.now)
        .await
        .unwrap();
    f.request.claim = claim.clone();
    reject_unchanged(&f, stale).await;
    let main = f.directory.join("session.json");
    let mut current: Session = serde_json::from_slice(&std::fs::read(&main).unwrap()).unwrap();
    for seq in 0..=bamboo_domain::SESSION_INBOX_ADMITTED_CAPACITY {
        current.session_inbox_admission_mut().record(
            SessionMessageId::parse(format!("later-{seq}")).unwrap(),
            seq as u64 + 2,
        );
    }
    assert!(!current
        .session_inbox_admission()
        .unwrap()
        .contains(&claim.claim.envelope.id));
    let raw = std::fs::read_to_string(&main).unwrap();
    let fields = object(&raw).unwrap().0;
    let runtime = object(fields["runtime_metadata"].get()).unwrap().0;
    let changed = super::actor_transcript::splice(
        &raw,
        vec![(
            runtime["session_inbox_admission"],
            serde_json::to_string(current.session_inbox_admission().unwrap()).unwrap(),
        )],
    )
    .unwrap();
    std::fs::write(&main, changed).unwrap();
    let recovered = other
        .checkpoint_actor_input(request(&current, activation.fence(), claim))
        .await
        .unwrap();
    assert_eq!(
        recovered.status,
        ActorInputCheckpointStatus::AlreadyCheckpointed
    );
    assert_eq!(
        recovered.session.messages.len(),
        committed.session.messages.len()
    );
}

#[tokio::test]
async fn actor_input_dedupe_raw_closed_record_missing_duplicate_and_same_id_conflict() {
    let home = tempfile::tempdir().unwrap();
    let f = fixture(home.path(), false, true).await;
    f.inbox
        .checkpoint_actor_input(f.request.clone())
        .await
        .unwrap();
    let main = f.directory.join("session.json");
    let original = std::fs::read_to_string(&main).unwrap();
    let messages = object(&original).unwrap().0["messages"];
    for kind in 0..8 {
        let mut value: serde_json::Value = serde_json::from_str(&original).unwrap();
        let user = value["messages"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap();
        let record = &mut user["metadata"]["_bamboo_owned_input_checkpoint"];
        match kind {
            0 => {
                user["metadata"]
                    .as_object_mut()
                    .unwrap()
                    .remove("_bamboo_owned_input_checkpoint");
            }
            1 => {
                record.as_object_mut().unwrap().remove("activation_intent");
            }
            2 => record["generation"] = json!(99),
            3 => record["activation_policy"] = json!("interrupt_specific_wait"),
            4 => record["future"] = json!(true),
            5 => user["content"] = json!("different"),
            6 => {
                value["messages"].as_array_mut().unwrap().pop();
            }
            _ => record["session_created_at"] = json!(Utc::now()),
        }
        let replacement = serde_json::to_string(&value["messages"]).unwrap();
        let changed = splice(&original, vec![(messages, replacement)]).unwrap();
        std::fs::write(&main, changed).unwrap();
        let current = f.store.load_session(ID).await.unwrap().unwrap();
        assert!(matches!(
            reject_unchanged(&f, f.request_from(&current)).await,
            ActorInputCheckpointError::Unsupported
        ));
    }
    let duplicate = original.replacen("\"generation\":1", "\"generation\":1,\"generation\":1", 1);
    assert_ne!(duplicate, original);
    std::fs::write(&main, duplicate).unwrap();
    let current = f.store.load_session(ID).await.unwrap().unwrap();
    assert!(matches!(
        reject_unchanged(&f, f.request_from(&current)).await,
        ActorInputCheckpointError::Unsupported
    ));
    std::fs::write(main, original).unwrap();
}

#[tokio::test]
async fn actor_input_mixed_staged_immediate_never_promotes_sibling_or_prefix() {
    let home = tempfile::tempdir().unwrap();
    let f = fixture(home.path(), false, true).await;
    f.inbox
        .checkpoint_actor_input(f.request.clone())
        .await
        .unwrap();
    f.inbox
        .ack_owned(ID, &f.request.claim, Utc::now())
        .await
        .unwrap();
    let staged = SessionMessageEnvelope::user_input(ID, "staged sibling");
    f.inbox.deliver(&staged).await.unwrap();
    let mut immediate = SessionMessageEnvelope::user_input(ID, "immediate peer");
    immediate.kind = SessionMessageKind::PeerMessage;
    immediate.source = bamboo_domain::SessionMessageSource::Session {
        session_id: "peer".into(),
    };
    f.inbox
        .deliver_with_activation_intent(
            &immediate,
            bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = lease_request(f.request.claim.lease.consumer.clone(), Utc::now());
    let next = f.inbox.claim_owned(ID, 10, None, &lease).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].claim.envelope, immediate);
    let current = f.store.load_session(ID).await.unwrap().unwrap();
    let mut req = request(&current, f.request.fence.clone(), next[0].clone());
    let dir = f.directory.join("inbox");
    let act = std::fs::read(dir.join("activation-generation")).unwrap();
    let committed = f.inbox.checkpoint_actor_input(req.clone()).await.unwrap();
    assert_eq!(
        std::fs::read(dir.join("activation-generation")).unwrap(),
        act
    );
    assert!(!committed
        .session
        .messages
        .iter()
        .any(|m| m.id == staged.id.as_str()));
    assert_eq!(
        f.inbox
            .claim_owned(ID, 10, None, &lease)
            .await
            .unwrap()
            .len(),
        1
    ); // only the held immediate, never staged
    req.expected_messages = committed.session.messages;
    req.expected_admission = committed
        .session
        .runtime_metadata
        .unwrap()
        .session_inbox_admission;
    assert_eq!(
        f.inbox.checkpoint_actor_input(req).await.unwrap().status,
        ActorInputCheckpointStatus::AlreadyCheckpointed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_input_final_barrier_rejects_expired_actor_or_inbox_and_renewed_token() {
    for actor_expiry in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let mut f = fixture(home.path(), false, true).await;
        let expires = Utc::now() + LeaseDuration::milliseconds(250);
        let path = if actor_expiry {
            f.directory.join("actor-authority.json")
        } else {
            f.directory
                .join("inbox/cur")
                .join(&f.request.claim.claim.claim_id)
        };
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if actor_expiry {
            value["activation"]["lease_expires_at"] = json!(expires);
        } else {
            value["session_inbox_lease"]["token"]["expires_at"] = json!(expires);
            f.request.claim.lease.expires_at = expires;
        }
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
        let original = std::fs::read(f.directory.join("session.json")).unwrap();
        let hook =
            TranscriptWriteHook::install(&f.store, DurableWritePhase::BeforeReplace, false, false);
        let _release = Release(hook.clone());
        let inbox = f.inbox.clone();
        let req = f.request.clone();
        let writer = tokio::spawn(async move { inbox.checkpoint_actor_input(req).await });
        hook.entered();
        held(&f);
        while Utc::now() <= expires {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        hook.release();
        assert!(writer.await.unwrap().is_err());
        assert_eq!(
            std::fs::read(f.directory.join("session.json")).unwrap(),
            original
        );
    }
    let home = tempfile::tempdir().unwrap();
    let mut f = fixture(home.path(), false, true).await;
    let lease = SessionInboxLeaseRequest {
        consumer: f.request.claim.lease.consumer.clone(),
        now: Utc::now(),
        duration: LeaseDuration::minutes(10),
    };
    let renewed = f
        .inbox
        .renew_owned(ID, &f.request.claim, &lease)
        .await
        .unwrap();
    reject_unchanged(&f, f.request.clone()).await;
    f.request.claim = renewed;
    assert_eq!(
        f.inbox
            .checkpoint_actor_input(f.request)
            .await
            .unwrap()
            .status,
        ActorInputCheckpointStatus::NewCheckpoint
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_input_before_fault_unchanged_after_rename_unconfirmed_requires_reload() {
    for (phase, corrupt) in [
        (DurableWritePhase::BeforeReplace, false),
        (DurableWritePhase::AfterReplace, false),
        (DurableWritePhase::AfterReplace, true),
    ] {
        let home = tempfile::tempdir().unwrap();
        let f = fixture(home.path(), false, true).await;
        let original = std::fs::read(f.directory.join("session.json")).unwrap();
        let hook = TranscriptWriteHook::install(&f.store, phase, !corrupt, corrupt);
        hook.release();
        let error = f
            .inbox
            .checkpoint_actor_input(f.request.clone())
            .await
            .unwrap_err();
        if phase == DurableWritePhase::BeforeReplace {
            assert!(matches!(
                error,
                ActorInputCheckpointError::BeforePublication(_)
            ));
            assert_eq!(
                std::fs::read(f.directory.join("session.json")).unwrap(),
                original
            );
        } else {
            assert!(matches!(
                error,
                ActorInputCheckpointError::OutcomeUnconfirmed(_)
            ));
            if corrupt {
                assert_eq!(
                    std::fs::read(f.directory.join("session.json")).unwrap(),
                    b"invalid readback"
                );
                continue;
            }
            assert!(matches!(
                reject_unchanged(&f, f.request.clone()).await,
                ActorInputCheckpointError::PrefixConflict
            ));
            let cold = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
            let current = cold.load_session(ID).await.unwrap().unwrap();
            let inbox = crate::FileSessionInbox::new(cold, SessionInboxLimits::default());
            assert_eq!(
                inbox
                    .checkpoint_actor_input(f.request_from(&current))
                    .await
                    .unwrap()
                    .status,
                ActorInputCheckpointStatus::AlreadyCheckpointed
            );
        }
        assert!(!std::fs::read_dir(&f.directory).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("durable.tmp")));
    }
}

#[test]
fn actor_input_started_job_abort_and_whole_runtime_hold_joint_physical_locks() {
    for shutdown in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let mut runtime = Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        );
        let f = runtime
            .as_ref()
            .unwrap()
            .block_on(fixture(home.path(), false, true));
        let hook =
            TranscriptWriteHook::install(&f.store, DurableWritePhase::BeforeReplace, false, false);
        let _release = Release(hook.clone());
        let inbox = f.inbox.clone();
        let req = f.request.clone();
        let writer = runtime
            .as_ref()
            .unwrap()
            .spawn(async move { inbox.checkpoint_actor_input(req).await });
        hook.entered();
        held(&f);
        if shutdown {
            runtime.take().unwrap().shutdown_background();
            drop(writer);
        } else {
            writer.abort();
            runtime.as_ref().unwrap().block_on(writer).unwrap_err();
        }
        held(&f);
        hook.release();
        release_locks(&f);
        drop(runtime);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let current = runtime.block_on(f.other.load_session(ID)).unwrap().unwrap();
        assert_eq!(
            current.messages.len(),
            f.request.expected_messages.len() + 1
        );
        let other = crate::FileSessionInbox::new(f.other.clone(), SessionInboxLimits::default());
        assert_eq!(
            runtime
                .block_on(other.checkpoint_actor_input(f.request_from(&current)))
                .unwrap()
                .status,
            ActorInputCheckpointStatus::AlreadyCheckpointed
        );
    }
}

#[tokio::test]
async fn actor_input_actual_sealed_supervisor_is_unsupported_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let receipt = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let id = receipt.session_id;
    let inbox = crate::FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let envelope = SessionMessageEnvelope::user_input(&id, "not an ordinary grant");
    inbox
        .deliver_with_activation_intent(
            &envelope,
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = lease_request(SessionInboxConsumerId::new(), Utc::now());
    let claim = inbox
        .claim_owned(&id, 1, None, &lease)
        .await
        .unwrap()
        .remove(0);
    let activation = store
        .claim_activation(&activation_claim(&claim, "supervisor", lease.now))
        .await
        .unwrap();
    store
        .start_activation(&activation.fence(), Utc::now())
        .await
        .unwrap();
    let session = store.load_session(&id).await.unwrap().unwrap();
    let dir = home.path().join("sessions").join(&id);
    let before = std::fs::read(dir.join("session.json")).unwrap();
    std::fs::remove_dir_all(dir.join("inbox")).unwrap();
    assert!(matches!(
        inbox
            .checkpoint_actor_input(request(&session, activation.fence(), claim))
            .await,
        Err(ActorInputCheckpointError::InvalidAuthority(_))
    ));
    assert_eq!(std::fs::read(dir.join("session.json")).unwrap(), before);
    assert!(!dir.join("inbox").exists());
}
