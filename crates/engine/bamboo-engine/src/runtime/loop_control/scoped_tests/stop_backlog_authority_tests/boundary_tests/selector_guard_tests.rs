//! Green-side isolation and forensic preservation at the actual Stop CP seam.
use super::*;
use std::path::{Path, PathBuf};

fn inbox_directory(h: &Fixture) -> PathBuf {
    // Fixture passes _dir/sessions as Bamboo home; V2 adds its sessions tree.
    h._dir
        .path()
        .join("sessions")
        .join("sessions")
        .join(TARGET)
        .join("inbox")
}

async fn raw_pending(dir: &Path, id: &str) -> (String, Vec<u8>) {
    let mut entries = tokio::fs::read_dir(dir.join("new")).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let bytes = tokio::fs::read(entry.path()).await.unwrap();
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if value.pointer("/body/id").and_then(Value::as_str) == Some(id) {
            return (entry.file_name().to_string_lossy().into_owned(), bytes);
        }
    }
    panic!("the real delivered tail must remain in new before Stop CP");
}

#[tokio::test]
async fn scoped_stop_selector_preserves_unmatched_corruption_without_blocking_real_scope_retirement(
) {
    let (mut h, mut binding) = native_fixture().await;
    h.start_and_admit().await;
    let command = stop(&h);
    let queued_stop = h.send(&command).await.unwrap();
    let tail = h.update("Only this genuine matching carrier may be retired");
    let queued_tail = h.send(&tail).await.unwrap();
    let tail_id = queued_tail["receipt_id"].as_str().unwrap().to_owned();
    let tail_generation = queued_tail["generation"].as_u64().unwrap();
    let dir = inbox_directory(&h);
    let gate = install_gate(&mut h, GatePhase::BeforeCheckpoint);
    let control = gate.clone();
    let injector = async move {
        control.entered.notified().await;
        let (tail_name, tail_bytes) = raw_pending(&dir, &tail_id).await;
        let mut foreign = serde_json::from_slice::<Value>(&tail_bytes).unwrap();
        foreign["body"]["body"]["data"]["binding"]["execution_id"] =
            serde_json::json!("other-execution-not-this-owner");
        // A foreign candidate's invalid lease must not be interpreted as a
        // reason to alter it or fail this binding's valid Stop retirement.
        foreign["session_inbox_lease"] = serde_json::json!({"epoch":"invalid"});
        foreign["kind"] = serde_json::json!("corrupt_foreign_kind");
        let mut user = serde_json::from_slice::<Value>(&tail_bytes).unwrap();
        user["body"]["source"] = serde_json::json!({"type":"user"});
        user["body"]["kind"] = serde_json::json!("user_input");
        user["body"]["body"] = serde_json::json!({"type":"content","text":false});
        user["kind"] = serde_json::json!("corrupt_user_kind");
        let injected = vec![
            (
                dir.join("new").join("not-an-ordered-generation.json"),
                b"foreign malformed name".to_vec(),
            ),
            (
                dir.join("new")
                    .join(format!("{:020}-foreign-bytes.json", tail_generation + 1)),
                b"{unparseable foreign body".to_vec(),
            ),
            (
                dir.join("new")
                    .join(format!("{:020}-foreign-lease.json", tail_generation + 2)),
                serde_json::to_vec(&foreign).unwrap(),
            ),
            (
                dir.join("new")
                    .join(format!("{:020}-user-body.json", tail_generation + 3)),
                serde_json::to_vec(&user).unwrap(),
            ),
        ];
        for (path, bytes) in &injected {
            tokio::fs::write(path, bytes).await.unwrap();
        }
        // Insert only after the real claim has returned; ordinary claim-time
        // corruption handling must not be mistaken for the cleanup selector.
        control.release.notify_one();
        (dir, tail_name, tail_bytes, injected)
    };
    let (drained, (dir, tail_name, tail_bytes, injected)) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(h.drain(1), injector)
        })
        .await
        .expect("post-claim corruption injection must cross the real Stop checkpoint gate");
    assert!(!gate.armed.load(Ordering::SeqCst));
    drained.expect("unmatched corrupt transports must not block identifiable scope retirement");
    for (path, bytes) in &injected {
        assert_eq!(
            tokio::fs::read(path).await.unwrap(),
            *bytes,
            "foreign/User evidence is neither rewritten nor quarantined"
        );
    }
    assert_eq!(
        tokio::fs::read(dir.join("cancelled").join(&tail_name))
            .await
            .unwrap(),
        tail_bytes
    );
    assert!(!tokio::fs::try_exists(dir.join("new").join(&tail_name))
        .await
        .unwrap());
    let durable = h.durable().await;
    assert!(consumption(&durable, &command).configured);
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(!cursor.contains_str(&tail_id));
    assert_eq!(
        cursor.last_admitted_sequence,
        queued_stop["generation"].as_u64().unwrap()
    );
    // Do not invoke inspect/claim on intentionally corrupt foreign records.
    // Remove only our injected fixtures after byte-for-byte preservation proof.
    for (path, _) in injected {
        tokio::fs::remove_file(path).await.unwrap();
    }
    assert!(!h.admit().await);
    close_native(&mut h, &mut binding).await;
}

#[tokio::test]
async fn scoped_stop_tombstone_collision_preserves_existing_bytes_and_fails_before_stop_ack() {
    let (mut h, mut binding) = native_fixture().await;
    h.start_and_admit().await;
    let command = stop(&h);
    let queued_stop = h.send(&command).await.unwrap();
    let stop_id =
        bamboo_domain::SessionMessageId::parse(queued_stop["receipt_id"].as_str().unwrap())
            .unwrap();
    let tail = h.update("Collision cannot overwrite existing transport evidence");
    let queued_tail = h.send(&tail).await.unwrap();
    let tail_id = queued_tail["receipt_id"].as_str().unwrap().to_owned();
    let dir = inbox_directory(&h);
    let gate = install_gate(&mut h, GatePhase::BeforeCheckpoint);
    let control = gate.clone();
    let injector = async move {
        control.entered.notified().await;
        let (name, original) = raw_pending(&dir, &tail_id).await;
        let cancelled = dir.join("cancelled");
        tokio::fs::create_dir_all(&cancelled).await.unwrap();
        let existing = b"preexisting immutable forensic collision".to_vec();
        let destination = cancelled.join(&name);
        tokio::fs::write(&destination, &existing).await.unwrap();
        control.release.notify_one();
        (dir, name, original, destination, existing, tail_id)
    };
    let (result, (dir, name, original, destination, existing, tail_id)) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(h.drain(1), injector)
        })
        .await
        .expect("post-claim tombstone collision must cross the actual checkpoint gate");
    assert!(!gate.armed.load(Ordering::SeqCst));
    assert!(
        result.is_err(),
        "a collision is not a successful retirement"
    );
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), existing);
    assert_eq!(
        tokio::fs::read(dir.join("new").join(name)).await.unwrap(),
        original
    );
    let durable = h.durable().await;
    assert!(
        consumption(&durable, &command).configured,
        "real Stop CP is not rolled back"
    );
    assert_eq!(scoped_loop_consumptions(&durable).unwrap().len(), 2);
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(cursor.contains(&stop_id));
    assert!(!cursor.contains_str(&tail_id));
    assert_eq!(
        cursor.last_admitted_sequence,
        queued_stop["generation"].as_u64().unwrap()
    );
    assert!(
        !h.inbox.was_admitted(TARGET, &stop_id).await.unwrap(),
        "Stop ACK remains after tail retirement"
    );
    let tail_id = bamboo_domain::SessionMessageId::parse(tail_id).unwrap();
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    assert!(!h
        .session
        .metadata
        .contains_key(bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY));
    // Restore only the deliberately injected forensic fixture for cleanup.
    tokio::fs::remove_file(destination).await.unwrap();
    close_native(&mut h, &mut binding).await;
}
