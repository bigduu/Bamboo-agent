//! Real account queue, journal claim, and V2 Root authority publication.
//! Production execution creates the capability in the #1488 Host fixture;
//! these focused port tests exercise its lifetime after queue admission.

use std::{
    fs::File,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bamboo_agent_core::{storage::Storage, AgentEvent, Message, Session};
use bamboo_domain::{ActorActivationClaim, ActorDirectoryPort, RootActorRuntimeWrite};
use bamboo_storage::SessionStoreV2;
use chrono::{Duration as ChronoDuration, Utc};
use fs2::FileExt;
use tokio::sync::{broadcast::error::TryRecvError, oneshot};

use crate::events::{journal, AccountEventSink, ChangeEvent};

const ROOT: &str = "queued-root-account-writer";
const WAIT: Duration = Duration::from_secs(8);

/// The existing account writer's actual cross-process journal claim.
struct JournalClaim(File);

impl JournalClaim {
    fn acquire(events: &Path) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(events.join(".account-journal.lock"))
            .unwrap();
        FileExt::lock_exclusive(&file).unwrap();
        Self(file)
    }
}

impl Drop for JournalClaim {
    fn drop(&mut self) {
        FileExt::unlock(&self.0).unwrap();
    }
}

/// Release on assertion failure too: a blocked physical job must never strand
/// the test runtime's blocking executor during unwinding.
struct CallbackRelease(Arc<(Mutex<bool>, Condvar)>);

impl CallbackRelease {
    fn release(&self) {
        let (ready, changed) = self.0.as_ref();
        *ready.lock().unwrap() = true;
        changed.notify_all();
    }
}

impl Drop for CallbackRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct Fixture {
    a: Arc<SessionStoreV2>,
    b: Arc<SessionStoreV2>,
    root: Session,
    sink: Arc<AccountEventSink>,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("bamboo");
        let a = Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
        let mut root = Session::new(ROOT, "controlled-model");
        root.add_message(Message::user("Root account publication fixture"));
        a.save_session(&root).await.unwrap();
        // The independent Store discovers the existing canonical Root through
        // normal initialization, without manufactured index or authority files.
        let b = Arc::new(SessionStoreV2::new(data).await.unwrap());
        let sink = AccountEventSink::new(temp.path().join("events")).unwrap();
        Self {
            a,
            b,
            root,
            sink,
            _temp: temp,
        }
    }

    async fn running_owner(
        &self,
        store: &SessionStoreV2,
        label: &str,
        lease: Duration,
    ) -> RootActorRuntimeWrite {
        let now = Utc::now();
        let activation = tokio::time::timeout(
            WAIT,
            store.claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: format!("run-{label}"),
                lease_owner: format!("host-{label}"),
                lease_expires_at: now + ChronoDuration::from_std(lease).unwrap(),
                inbox_generation: 0,
                placement_ref: None,
                now,
            }),
        )
        .await
        .expect("Actor claim must not wait behind the blocked account journal")
        .unwrap();
        store
            .start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
        RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: self.root.created_at,
        }
    }

    async fn expire_a(&self, owner: &RootActorRuntimeWrite) {
        tokio::time::timeout(WAIT, async {
            loop {
                let entry = self.a.inspect_actor(ROOT).await.unwrap();
                let activation = entry.activation.unwrap();
                assert_eq!(activation.fence(), owner.fence);
                if activation.lease_expires_at <= Utc::now() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the persisted A lease expires in actual wall time");
    }
}

async fn confirmed(receipt: oneshot::Receiver<bool>) -> bool {
    tokio::time::timeout(WAIT, receipt)
        .await
        .expect("account writer confirms its final publication")
        .expect("actual writer returns a result rather than dropping the receipt")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_root_terminal_and_history_reject_expired_reclaimed_owner_before_append() {
    for event in [
        AgentEvent::Complete {
            usage: Default::default(),
        },
        AgentEvent::SessionHistoryCommitted {
            session_id: ROOT.into(),
        },
    ] {
        let fixture = Fixture::new().await;
        let mut live = fixture.sink.subscribe();
        let claim = JournalClaim::acquire(fixture.sink.events_dir());
        let a = fixture
            .running_owner(&fixture.a, "a", Duration::from_secs(2))
            .await;
        fixture
            .a
            .validate_fence(&a.fence, Utc::now())
            .await
            .unwrap();
        let mut receipt = fixture
            .sink
            .record_root_actor(fixture.a.clone(), a.clone(), Some(ROOT), &event)
            .expect("current A enters the existing writer queue");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            matches!(receipt.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "queue admission must not confirm before journal append/broadcast"
        );
        assert!(journal::read_since(fixture.sink.events_dir(), 0)
            .unwrap()
            .is_empty());
        assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));

        fixture.expire_a(&a).await;
        // B must acquire the real Root authority while the account journal is
        // still blocked. Taking Root guards before the journal claim deadlocks
        // this finite assertion and violates the publication lock order.
        let b = fixture
            .running_owner(&fixture.b, "b", Duration::from_secs(30))
            .await;
        assert!(b.fence.attempt > a.fence.attempt);
        assert!(b.fence.lease_epoch > a.fence.lease_epoch);
        assert_ne!(b.fence.lease_owner, a.fence.lease_owner);
        drop(claim);

        assert!(
            !confirmed(receipt).await,
            "A was accepted into the queue while current, but is obsolete at actual publication"
        );
        assert_eq!(fixture.sink.latest_seq(), 0);
        assert!(
            journal::read_since(fixture.sink.events_dir(), 0)
                .unwrap()
                .is_empty(),
            "rejected A must not allocate a sequence or append"
        );
        assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));

        let receipt = fixture
            .sink
            .record_root_actor(fixture.b.clone(), b.clone(), Some(ROOT), &event)
            .expect("replacement B enters the same existing writer queue");
        assert!(confirmed(receipt).await);
        let frame = tokio::time::timeout(WAIT, live.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.seq, 1);
        assert_eq!(frame.session_id.as_deref(), Some(ROOT));
        assert_eq!(
            serde_json::to_value(&frame.event).unwrap(),
            serde_json::to_value(&event).unwrap()
        );
        let durable = journal::read_since(fixture.sink.events_dir(), 0).unwrap();
        assert_eq!(
            durable.len(),
            1,
            "only B appends; rejected A is not retried"
        );
        assert_eq!(durable[0].seq, 1);
        assert_eq!(
            serde_json::to_value(&durable[0]).unwrap(),
            serde_json::to_value(frame.as_ref()).unwrap()
        );
        assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(
            fixture
                .b
                .inspect_actor(ROOT)
                .await
                .unwrap()
                .activation
                .unwrap()
                .fence(),
            b.fence,
            "obsolete queue processing must not finish or renew B's activation"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_root_receipt_and_producer_keeps_queued_publication_owned_and_single() {
    let fixture = Fixture::new().await;
    let a = fixture
        .running_owner(&fixture.a, "a", Duration::from_secs(30))
        .await;
    let mut live = fixture.sink.subscribe();
    let events_dir = fixture.sink.events_dir().to_path_buf();
    let claim = JournalClaim::acquire(&events_dir);
    let event = AgentEvent::SessionHistoryCommitted {
        session_id: ROOT.into(),
    };
    let receipt = fixture
        .sink
        .record_root_actor(fixture.a.clone(), a, Some(ROOT), &event)
        .expect("current owner enters the existing writer queue");
    // Cancellation of a producer's wait does not cancel the already owned
    // publication. This tests queue/job lifetime, not a simulated runtime
    // shutdown or a physical-guard barrier that the public API cannot expose.
    drop(receipt);
    let Fixture {
        sink, _temp: temp, ..
    } = fixture;
    drop(sink);
    drop(claim);

    let frame = tokio::time::timeout(WAIT, live.recv())
        .await
        .expect("the detached writer retains its payload after producer drop")
        .unwrap();
    assert_eq!(frame.seq, 1);
    assert!(
        matches!(&frame.event, AgentEvent::SessionHistoryCommitted { session_id } if session_id == ROOT)
    );
    assert!(
        matches!(
            tokio::time::timeout(WAIT, live.recv()).await.unwrap(),
            Err(tokio::sync::broadcast::error::RecvError::Closed)
        ),
        "the closed producer queue drains once, without automatic retry"
    );
    let durable = journal::read_since(&events_dir, 0).unwrap();
    assert_eq!(durable.len(), 1);
    assert_eq!(
        serde_json::to_value(&durable[0]).unwrap(),
        serde_json::to_value(frame.as_ref()).unwrap()
    );
    drop(temp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_final_publication_keeps_real_guards_until_fresh_validator_rejects() {
    let fixture = Fixture::new().await;
    let a = fixture
        .running_owner(&fixture.a, "a", Duration::from_secs(2))
        .await;
    let expiry = fixture
        .a
        .inspect_actor(ROOT)
        .await
        .unwrap()
        .activation
        .unwrap()
        .lease_expires_at;
    let mut live = fixture.sink.subscribe();
    let events_dir = fixture.sink.events_dir().to_path_buf();
    let claim = JournalClaim::acquire(&events_dir);
    let blocked = Arc::new((Mutex::new(false), Condvar::new()));
    let release = CallbackRelease(blocked.clone());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (finished_tx, finished_rx) = oneshot::channel();
    let storage = fixture.a.clone();
    let sink = fixture.sink.clone();
    let owner = a.clone();
    let caller = tokio::spawn(async move {
        storage
            .publish_root_actor_runtime_event(
                &owner,
                Box::new(move |validate| {
                    // This is the actual final publication port used by the
                    // account writer. Both the journal File and V2's physical
                    // Root guards belong to the started callback, not its waiter.
                    let _claim = claim;
                    validate()?;
                    entered_tx.send(()).unwrap();
                    let mut ready = blocked.0.lock().unwrap();
                    while !*ready {
                        ready = blocked.1.wait(ready).unwrap();
                    }
                    drop(ready);

                    let result = (|| {
                        let (mut journal, max_seq) =
                            journal::EventJournal::open_for_locked_append(events_dir.clone())?;
                        let _observed = journal::read_since(&events_dir, 0)?;
                        // Expiry occurred while the callback was held. This
                        // must be a fresh check after scan, not a captured bool.
                        validate()?;
                        let frame = ChangeEvent {
                            seq: max_seq.checked_add(1).unwrap(),
                            ts: Utc::now(),
                            session_id: Some(ROOT.into()),
                            event: AgentEvent::SessionHistoryCommitted {
                                session_id: ROOT.into(),
                            },
                        };
                        journal.append_synced(&frame)?;
                        validate()?;
                        let _ = sink.broadcast.send(Arc::new(frame));
                        Ok(())
                    })();
                    let rejected = result.as_ref().err().is_some_and(|error: &std::io::Error| {
                        error.kind() == std::io::ErrorKind::WouldBlock
                            && error.get_ref().is_some_and(|cause| {
                                cause.is::<bamboo_domain::SessionAuthorityConflict>()
                            })
                    });
                    let _ = finished_tx.send(rejected);
                    result
                }),
            )
            .await
    });
    tokio::time::timeout(WAIT, entered_rx)
        .await
        .expect("real Root publication callback starts while owner is valid")
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // Reading ActorDirectory through its lock would wait behind this callback.
    // Use its captured, actual persisted deadline, without changing clocks,
    // renewing/finishing A, or editing the authority sidecar.
    tokio::time::timeout(WAIT, async {
        while Utc::now() < expiry {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let b_store = fixture.b.clone();
    let (claim_started_tx, claim_started_rx) = oneshot::channel();
    let b_claim = tokio::spawn(async move {
        let now = Utc::now();
        claim_started_tx.send(()).unwrap();
        b_store
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "run-b".into(),
                lease_owner: "host-b".into(),
                lease_expires_at: now + ChronoDuration::seconds(30),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
    });
    claim_started_rx.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), async {
            while !b_claim.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "caller cancellation cannot release physical Root guards from the started job"
    );
    assert!(journal::read_since(fixture.sink.events_dir(), 0)
        .unwrap()
        .is_empty());
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    release.release();
    assert!(
        tokio::time::timeout(WAIT, finished_rx)
            .await
            .unwrap()
            .unwrap(),
        "fresh final validator rejects the expired owner's actual effect"
    );
    let b = tokio::time::timeout(WAIT, b_claim)
        .await
        .expect("B acquires authority after the retained callback exits")
        .unwrap()
        .unwrap();
    assert!(b.attempt > a.fence.attempt && b.lease_epoch > a.fence.lease_epoch);
    assert!(journal::read_since(fixture.sink.events_dir(), 0)
        .unwrap()
        .is_empty());
    assert_eq!(fixture.sink.latest_seq(), 0);
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_root_queue_entry_does_not_fail_same_id_legacy_confirmation() {
    let fixture = Fixture::new().await;
    let mut live = fixture.sink.subscribe();
    let claim = JournalClaim::acquire(fixture.sink.events_dir());
    let a = fixture
        .running_owner(&fixture.a, "a", Duration::from_secs(2))
        .await;
    let event = AgentEvent::WorkflowActivated {
        event_id: "root-and-legacy-confirmation-fixture".into(),
        session_id: ROOT.into(),
        workflow_id: "controlled-workflow".into(),
        revision: 1,
        invoked_by: "user".into(),
    };
    let root_receipt = fixture
        .sink
        .record_root_actor(fixture.a.clone(), a.clone(), Some(ROOT), &event)
        .expect("A's Root queue entry precedes the legacy producer");
    let confirmation_id = super::event_confirmation_id(&event).unwrap();
    let sink = fixture.sink.clone();
    let legacy_event = event.clone();
    let legacy =
        tokio::spawn(async move { sink.record_confirmed(Some(ROOT), &legacy_event).await });
    // Observe the existing private waiter registry rather than guessing when
    // record_confirmed has started. Its queue entry follows A on the same queue.
    tokio::time::timeout(WAIT, async {
        loop {
            let registered = fixture
                .sink
                .confirmation_waiters
                .lock()
                .unwrap()
                .get(&confirmation_id)
                .is_some_and(|waiters| !waiters.is_empty());
            if registered {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("legacy confirmation is registered while A waits behind the journal claim");
    assert!(!legacy.is_finished());
    fixture.expire_a(&a).await;
    let b = fixture
        .running_owner(&fixture.b, "b", Duration::from_secs(30))
        .await;
    assert!(b.fence.attempt > a.fence.attempt);
    drop(claim);

    assert!(!confirmed(root_receipt).await);
    assert!(
        tokio::time::timeout(WAIT, legacy).await.unwrap().unwrap(),
        "obsolete Root failure belongs to its receipt; the later lawful legacy entry succeeds"
    );
    let frame = tokio::time::timeout(WAIT, live.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.seq, 1);
    assert_eq!(
        serde_json::to_value(&frame.event).unwrap(),
        serde_json::to_value(&event).unwrap()
    );
    let durable = journal::read_since(fixture.sink.events_dir(), 0).unwrap();
    assert_eq!(durable.len(), 1);
    assert_eq!(
        serde_json::to_value(&durable[0]).unwrap(),
        serde_json::to_value(frame.as_ref()).unwrap()
    );
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    assert!(fixture
        .sink
        .confirmation_waiters
        .lock()
        .unwrap()
        .get(&confirmation_id)
        .is_none());
}
