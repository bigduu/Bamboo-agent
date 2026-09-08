//! Four genuine process lifetimes share only completed durable writes.

use super::*;
use std::path::Path;

const CHILD_TEST: &str =
    "agent::approval_replay_tests::supervisor::process::supervisor_approval_process_child";
const PROCESS_HOME: &str = "BAMBOO_SUPERVISOR_APPROVAL_TEST_HOME";
const PROCESS_STAGE: &str = "BAMBOO_SUPERVISOR_APPROVAL_TEST_STAGE";

struct StageProcess(std::process::Child);
impl Drop for StageProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn fixture_at(home: &Path) -> Fixture {
    let directory = tempfile::tempdir_in(home).unwrap();
    let store = Arc::new(SessionStoreV2::new(home.to_path_buf()).await.unwrap());
    let persistence = Arc::new(LockedSessionStore::new(store.clone()));
    let repo = SessionRepository::new(SessionCache::default(), store.clone(), persistence.clone());
    Fixture {
        directory,
        store,
        persistence,
        repo,
        probe: Arc::new(Probe::default()),
    }
}

fn canonical_config(home: &Path) -> Arc<PermissionConfig> {
    let section = bamboo_tools::permission::PermissionSection::open(home).unwrap();
    let snapshot = section.snapshot();
    let config = Arc::new(PermissionConfig::new());
    config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
    assert_eq!(config.mode(), PermissionMode::Default);
    config
}

async fn run_stage(home: &Path, stage: u32) {
    let fixture = fixture_at(home).await;
    if stage == 1 {
        bamboo_tools::permission::storage::PermissionStorage::new(home)
            .save(alias_config().as_ref())
            .await
            .unwrap();
        setup_supervisor(&fixture).await;
    }
    let config = canonical_config(home);
    let id = bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID;
    let session = fixture.reload(id).await;
    let original = bamboo_domain::SupervisorReference {
        session_id: id.into(),
        incarnation_id: match session.authority_identity {
            bamboo_domain::SessionAuthorityIdentity::Supervisor { incarnation_id } => {
                incarnation_id
            }
            _ => panic!("real bootstrap must retain its authority"),
        },
    };
    if stage != 1 {
        let request = serde_json::from_value::<PermissionRequest>(
            session.messages[result_index(&session)]
                .metadata
                .as_ref()
                .unwrap()["permission_request"]
                .clone(),
        )
        .unwrap();
        assert!(!config.consume_once_for_generation(
            id,
            CALL,
            &request.request_generation,
            request.permission_type,
            &request.resource
        ));
        assert!(config
            .decision_receipt(id, CALL, &request.request_generation)
            .is_none());
    }
    let agent = protected_agent_with(&fixture, config, &["Bash"], true, None);
    match stage {
        1 => {
            *fixture.probe.next_call.lock().unwrap() = Some(alias_call());
            let output = events(agent.run_stream(session, "attach the permitted Root")).await;
            assert!(output
                .iter()
                .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
        }
        2 => {
            let request = current_request(&session);
            assert_eq!(request.permission_type, PermissionType::ExecuteCommand);
            fixture.approve(&request).await;
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        }
        3 => {
            assert!(session.pending_question.is_none());
            let output = events(agent.resume_stream(session)).await;
            assert!(output
                .iter()
                .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        }
        4 => {
            let request = current_request(&session);
            assert_eq!(request.permission_type, PermissionType::WriteFile);
            let waiting = events(agent.resume_stream(session)).await;
            assert!(matches!(
                waiting.as_slice(),
                [AgentEvent::NeedClarification { .. }]
            ));
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
            let answered = fixture.approve(&request).await;
            let output = events(agent.resume_stream(answered)).await;
            assert!(!output.iter().any(|event| matches!(
                event,
                AgentEvent::NeedClarification { .. } | AgentEvent::Error { .. }
            )));
            assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
            assert!(
                SupervisorSessionService::new(fixture.store.clone())
                    .inspect_link(&original, TARGET)
                    .await
                    .unwrap()
                    .authorized
            );
            let completed = fixture.reload(id).await;
            assert!(completed.pending_question.is_none());
            assert!(!completed
                .metadata
                .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
            let again = events(agent.resume_stream(completed)).await;
            assert!(!again
                .iter()
                .any(|event| matches!(event, AgentEvent::ToolLifecycle { .. })));
            assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
        }
        _ => panic!("unexpected child stage"),
    }
    // Read back the completed save before terminating P1-P3 without destructors.
    let durable = fixture.reload(id).await;
    let scope = SupervisorSessionService::new(fixture.store.clone())
        .inspect_scope(&original)
        .await
        .unwrap();
    assert_eq!(scope.state_revision, if stage == 4 { 2 } else { 1 });
    assert_eq!(durable.pending_question.is_some(), stage == 1 || stage == 3);
    assert_eq!(
        fixture.probe.actions.load(Ordering::SeqCst),
        usize::from(stage == 4)
    );
    let evidence = json!({
        "stage":stage, "pid":std::process::id(), "reference":original,
        "authority":authority(&durable), "result_id":durable.messages[result_index(&durable)].id,
        "pending":durable.pending_question.is_some(), "revision":scope.state_revision,
        "actions":fixture.probe.actions.load(Ordering::SeqCst),
    });
    std::fs::write(
        home.join(format!("stage-{stage}.json")),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    println!("SUPERVISOR_APPROVAL_STAGE {evidence}");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    if stage < 4 {
        std::process::exit((40 + stage) as i32);
    }
}

#[test]
fn supervisor_approval_process_child() {
    let Some(home) = std::env::var_os(PROCESS_HOME) else {
        return;
    };
    let stage: u32 = std::env::var(PROCESS_STAGE).unwrap().parse().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_stage(Path::new(&home), stage));
}

#[tokio::test]
async fn supervisor_approval_survives_four_processes_and_a_durable_repark() {
    let home = tempfile::tempdir().unwrap();
    let mut evidence = Vec::<Value>::new();
    let mut pids = std::collections::BTreeSet::new();
    let mut target_before = None;
    for stage in 1..=4 {
        let log_path = home.path().join(format!("stage-{stage}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let mut child = StageProcess(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", CHILD_TEST, "--nocapture"])
                .env(PROCESS_HOME, home.path())
                .env(PROCESS_STAGE, stage.to_string())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let pid = child.0.id();
        assert_ne!(pid, std::process::id());
        assert!(
            pids.insert(pid),
            "each stage must execute in a distinct process"
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            if tokio::time::Instant::now() >= deadline {
                child.0.kill().unwrap();
                child.0.wait().unwrap();
                panic!(
                    "process stage {stage} timed out: {}",
                    std::fs::read_to_string(&log_path).unwrap()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        let output = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(
            status.code(),
            Some(if stage < 4 { 40 + stage } else { 0 }),
            "stage {stage}: {output}"
        );
        let proof: Value = serde_json::from_slice(
            &std::fs::read(home.path().join(format!("stage-{stage}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(proof["pid"], pid);
        assert_eq!(proof["stage"], stage);
        println!("verified process stage {stage}: {proof}");
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let reference: bamboo_domain::SupervisorReference =
            serde_json::from_value(proof["reference"].clone()).unwrap();
        let durable = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*authority(&durable), proof["authority"]);
        assert_eq!(durable.pending_question.is_some(), stage == 1 || stage == 3);
        let target =
            serde_json::to_value(store.load_session(TARGET).await.unwrap().unwrap()).unwrap();
        if let Some(before) = &target_before {
            assert_eq!(&target, before);
        } else {
            target_before = Some(target);
        }
        let service = SupervisorSessionService::new(store);
        assert_eq!(
            service
                .inspect_scope(&reference)
                .await
                .unwrap()
                .state_revision,
            if stage == 4 { 2 } else { 1 }
        );
        if stage == 4 {
            assert!(
                service
                    .inspect_link(&reference, TARGET)
                    .await
                    .unwrap()
                    .authorized
            );
        }
        evidence.push(proof);
    }
    assert_eq!(evidence[0]["authority"], evidence[1]["authority"]);
    assert_eq!(evidence[2]["authority"], evidence[3]["authority"]);
    assert_ne!(
        evidence[0]["authority"]["request_generation"],
        evidence[2]["authority"]["request_generation"]
    );
    let mut a = evidence[0]["authority"].clone();
    let mut b = evidence[2]["authority"].clone();
    a.as_object_mut().unwrap().remove("request_generation");
    b.as_object_mut().unwrap().remove("request_generation");
    assert_eq!(a, b, "repark changes only the permission generation");
}
