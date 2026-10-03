//! Actual AppState/HTTP runtimes, stopped after owned claim before checkpoint.
//! The provider lives outside both Hosts; no Actor lease is manufactured here.
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_domain::{ActorDirectoryEntry, Session, SessionInboxPort, SessionMessageId, Storage};
use bamboo_server::{configure_routes, AppState};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use chrono::Utc;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const ROOT: &str = "owned-root-restart";
const ID: &str = "owned-input-restart-once";
const INPUT: &str = "OWNED_RESTART_INPUT: return one plain answer without tools";
const REPLY: &str = "OWNED_RESTART_COMPLETED";
const WAIT: Duration = Duration::from_secs(45);

struct Probe {
    home: PathBuf,
    calls: AtomicUsize,
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    if body["model"] == "owned-root" {
        let count = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| {
                message["role"] == "user"
                    && message["content"]
                        .as_str()
                        .is_some_and(|text| text.contains(INPUT))
            })
            .count();
        assert_eq!(count, 1, "one provider-visible typed input after recovery");
        let store = Arc::new(SessionStoreV2::new(probe.home.clone()).await.unwrap());
        let canonical = store.load_session(ROOT).await.unwrap().unwrap();
        assert_eq!(
            canonical
                .messages
                .iter()
                .filter(|message| message.id == ID)
                .count(),
            1
        );
        assert!(canonical
            .session_inbox_admission()
            .unwrap()
            .contains(&SessionMessageId::parse(ID).unwrap()));
        let inbox = FileSessionInbox::new(store, Default::default());
        assert!(
            inbox
                .was_admitted(ROOT, &SessionMessageId::parse(ID).unwrap())
                .await
                .unwrap(),
            "actual owned ACK must precede provider execution"
        );
        assert_eq!(
            probe.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "the stopped Host never enters provider and B enters once"
        );
    }
    let answer = if body["model"] == "owned-root" {
        REPLY
    } else {
        "auxiliary"
    };
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"id":"restart-fixture","object":"chat.completion.chunk",
            "choices":[{"index":0,"delta":{"content":answer},"finish_reason":"stop"}]})
        ))
}

struct Host {
    child: Option<std::process::Child>,
    base: String,
    cut: PathBuf,
}

impl Host {
    async fn start(home: &Path, pause: bool) -> Self {
        let id = uuid::Uuid::new_v4();
        let ready = home.join(format!("fixture-ready-{id}.json"));
        let cut = home.join(format!("fixture-claimed-{id}.json"));
        let log = std::fs::File::create(home.join(format!("fixture-host-{id}.log"))).unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "host_fixture_subprocess_entry",
                "--nocapture",
            ])
            .env("BAMBOO_OWNED_RESTART_FIXTURE_HOME", home)
            .env("BAMBOO_OWNED_RESTART_FIXTURE_READY", &ready)
            .env("BAMBOO_OWNED_RESTART_FIXTURE_CUT", &cut)
            .env(
                "BAMBOO_OWNED_RESTART_FIXTURE_PAUSE",
                if pause { "1" } else { "0" },
            )
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .unwrap();
        let mut host = Self {
            child: Some(child),
            base: String::new(),
            cut,
        };
        host.base = tokio::time::timeout(WAIT, async {
            loop {
                assert!(
                    host.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "fixture Host exited"
                );
                if let Ok(bytes) = std::fs::read(&ready) {
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    break value["base"].as_str().unwrap().to_owned();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        host
    }

    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap();
            let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
                .await
                .unwrap();
            assert!(
                !status.success(),
                "this fixture removes the whole owned Host process"
            );
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// This is a test executable's entrypoint, never a production environment hook.
#[actix_web::test]
#[ignore = "invoked only by the parent fixture in its isolated data root"]
async fn host_fixture_subprocess_entry() {
    let Some(home) = std::env::var_os("BAMBOO_OWNED_RESTART_FIXTURE_HOME") else {
        return;
    };
    let home = PathBuf::from(home);
    let ready = PathBuf::from(std::env::var_os("BAMBOO_OWNED_RESTART_FIXTURE_READY").unwrap());
    let cut = PathBuf::from(std::env::var_os("BAMBOO_OWNED_RESTART_FIXTURE_CUT").unwrap());
    let state = web::Data::new(
        AppState::new_with_memory_store(
            home.clone(),
            bamboo_memory::memory_store::MemoryStore::new(home.join("jiandu")),
        )
        .await
        .unwrap(),
    );
    if std::env::var("BAMBOO_OWNED_RESTART_FIXTURE_PAUSE").as_deref() == Ok("1") {
        let (mut reached, mut release) = state
            .session_store
            .pause_full_save_before_filesystem_commit_for_test(ROOT);
        let held = state.clone();
        let directory = home.join("sessions").join(ROOT);
        actix_web::rt::spawn(async move {
            for _ in 0..6 {
                tokio::time::timeout(WAIT, reached.wait()).await.unwrap();
                let claimed = std::fs::read_dir(directory.join("inbox/cur"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                if !claimed.is_empty() {
                    std::fs::write(&cut, b"{\"claimed\":true}").unwrap();
                    // Keep the actual named checkpoint held until process loss.
                    std::future::pending::<()>().await;
                }
                let next = held
                    .session_store
                    .pause_full_save_before_filesystem_commit_for_test(ROOT);
                tokio::time::timeout(WAIT, release.wait()).await.unwrap();
                (reached, release) = next;
            }
            panic!("owned checkpoint cut did not appear within six full saves");
        });
    }
    let factory = state.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(factory.clone())
            .configure(configure_routes)
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let base = format!("http://{}/api/v1", server.addrs()[0]);
    let running = server.run();
    std::fs::write(ready, serde_json::to_vec(&json!({"base":base})).unwrap()).unwrap();
    running.await.unwrap();
}

async fn deliver(client: &reqwest::Client, host: &Host) {
    let response = client
        .post(format!("{}/sessions/{ROOT}/guidance", host.base))
        .json(&json!({"id":ID,"text":INPUT,"mode":"after_round"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::ACCEPTED,
        "guidance: {}",
        response.text().await.unwrap()
    );
}

#[actix_web::test]
async fn claimed_owned_input_survives_real_host_process_loss_and_restarts_once() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("data");
    std::fs::create_dir_all(&home).unwrap();
    let probe = web::Data::new(Probe {
        home: home.clone(),
        calls: AtomicUsize::new(0),
    });
    let factory_probe = probe.clone();
    let provider_server = HttpServer::new(move || {
        App::new()
            .app_data(factory_probe.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"owned-root"},{"id":"owned-auxiliary"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", provider_server.addrs()[0]);
    let provider_server = provider_server.run();
    let provider_handle = provider_server.handle();
    actix_web::rt::spawn(provider_server);
    std::fs::write(
        home.join("config.json"),
        serde_json::to_vec(&json!({
            "provider":"openai", "features":{"provider_model_ref":true},
            "providers":{"openai":{"api_key":"fixture","base_url":provider_url,
                "model":"owned-root","fast_model":"owned-auxiliary"}},
            "defaults":{"chat":{"provider":"openai","model":"owned-root"},
                "fast":{"provider":"openai","model":"owned-auxiliary"}},
            "subagents":{"runtime":"actor","executor":"bamboo_runtime"}
        }))
        .unwrap(),
    )
    .unwrap();
    let original = SessionStoreV2::new(home.clone()).await.unwrap();
    let mut root = Session::new(ROOT, "owned-root");
    root.add_message(bamboo_domain::Message::user("original Root transcript"));
    original.save_session(&root).await.unwrap();
    drop(original);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(WAIT)
        .build()
        .unwrap();
    let mut a = Host::start(&home, true).await;
    // A cold guidance request can await activation. Keep that actual HTTP
    // request in flight while observing the deliberately held checkpoint.
    let sending_client = client.clone();
    let sending_base = a.base.clone();
    let sending = tokio::spawn(async move {
        sending_client
            .post(format!("{sending_base}/sessions/{ROOT}/guidance"))
            .json(&json!({"id":ID,"text":INPUT,"mode":"after_round"}))
            .send()
            .await
    });
    tokio::time::timeout(WAIT, async {
        while !a.cut.exists() {
            assert!(a.child.as_mut().unwrap().try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let directory = home.join("sessions").join(ROOT);
    let cur: Vec<_> = std::fs::read_dir(directory.join("inbox/cur"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        cur.len(),
        1,
        "the real consumer has claimed one owned input"
    );
    let transport: Value = serde_json::from_slice(&std::fs::read(&cur[0]).unwrap()).unwrap();
    assert_eq!(transport["kind"], "session_envelope_owned_v3");
    let lease = &transport["session_inbox_lease"]["token"];
    assert!(lease["epoch"].as_u64().unwrap() > 0);
    let input_expiry: chrono::DateTime<Utc> =
        serde_json::from_value(lease["expires_at"].clone()).unwrap();
    let a_owner: ActorDirectoryEntry =
        serde_json::from_slice(&std::fs::read(directory.join("actor-authority.json")).unwrap())
            .unwrap();
    let a_activation = a_owner.activation.unwrap();
    assert!(input_expiry <= a_activation.lease_expires_at);
    let main: Session =
        serde_json::from_slice(&std::fs::read(directory.join("session.json")).unwrap()).unwrap();
    assert!(!main.messages.iter().any(|message| message.id == ID));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    a.stop().await;
    // The caller may lose the response during Host shutdown. The physical
    // owned transport above, rather than this HTTP receipt, proves delivery.
    let _ = tokio::time::timeout(WAIT, sending).await.unwrap().unwrap();
    assert!(
        cur[0].exists(),
        "Host shutdown preserves the uncheckpointed typed transport"
    );
    let mut b = Host::start(&home, false).await;
    tokio::time::timeout(WAIT, async {
        loop {
            let store = SessionStoreV2::new(home.clone()).await.unwrap();
            let session = store.load_session(ROOT).await.unwrap().unwrap();
            if session
                .messages
                .iter()
                .any(|message| message.content == REPLY)
                && session.last_run_status().as_deref() == Some("completed")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("B recovers after actual Root and Inbox lease expiry");
    let canonical = SessionStoreV2::new(home.clone())
        .await
        .unwrap()
        .load_session(ROOT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        canonical
            .messages
            .iter()
            .filter(|message| message.id == ID)
            .count(),
        1
    );
    assert!(
        Utc::now() >= input_expiry,
        "recovery uses actual Inbox expiry"
    );
    let b_owner: ActorDirectoryEntry =
        serde_json::from_slice(&std::fs::read(directory.join("actor-authority.json")).unwrap())
            .unwrap();
    assert!(b_owner.activation.unwrap().attempt > a_activation.attempt);
    let admitted: Vec<_> = std::fs::read_dir(directory.join("inbox/admitted"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(admitted.len(), 1);
    let receipt: Value = serde_json::from_slice(&std::fs::read(&admitted[0]).unwrap()).unwrap();
    assert!(
        receipt["session_inbox_lease"]["token"]["epoch"]
            .as_u64()
            .unwrap()
            > lease["epoch"].as_u64().unwrap()
    );
    deliver(&client, &b).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        SessionStoreV2::new(home.clone())
            .await
            .unwrap()
            .load_session(ROOT)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.id == ID)
            .count(),
        1
    );
    b.stop().await;
    provider_handle.stop(false).await;
}
