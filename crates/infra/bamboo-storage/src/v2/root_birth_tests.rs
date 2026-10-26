//! Real V2 writers and retained canonical authority; no provider or mock store.

use super::default_actor_context_tests::DefaultWriteHook;
use super::*;
use bamboo_domain::{
    host_session_create_identity, HostScopeSessionObservation, HostSessionCreateScope,
    HostSessionScopeBinding, HostSessionScopeToken, SessionCallerReference,
    SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
    HOST_SESSION_SCOPE_VERSION,
};
use std::collections::{BTreeMap, BTreeSet};

const DEADLINE: Duration = Duration::from_secs(10);

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    supervisor: SupervisorReference,
    existing: Session,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let supervisor = SupervisorReference::from(
            &store
                .get_or_create_default_supervisor("model")
                .await
                .unwrap(),
        );
        let mut target = Session::new("authority-target", "model");
        target.set_project_id_meta("project-a");
        store.save_session(&target).await.unwrap();
        for (revision, mutation) in [
            (
                0,
                SupervisorManagementMutation::ConfigureProjectScope {
                    allowed_projects: BTreeSet::from(["project-a".parse().unwrap()]),
                },
            ),
            (
                1,
                SupervisorManagementMutation::Attach {
                    target_session_id: target.id.clone(),
                },
            ),
        ] {
            store
                .mutate_supervisor_management(&SupervisorManagementRequest {
                    supervisor: supervisor.clone(),
                    expected_state_revision: revision,
                    mutation,
                })
                .await
                .unwrap();
        }
        let existing = Session::new("unrelated-existing-root", "model");
        store.save_session(&existing).await.unwrap();
        Self {
            home,
            store,
            supervisor,
            existing,
        }
    }

    async fn route(&self, kind: usize) -> (Session, Route) {
        let mut candidate = Session::new(format!("new-root-{kind}"), "model");
        candidate.set_project_id_meta("project-a");
        candidate.set_workspace_path_meta(self.home.path().join("workspace").to_string_lossy());
        let route = match kind {
            0 => Route::Ordinary,
            1 => {
                let caller = SessionCallerReference::from(&self.existing);
                let operation = Uuid::new_v4();
                candidate.id = host_session_create_identity(&caller, operation).unwrap().1;
                candidate.root_session_id = candidate.id.clone();
                let token = self
                    .store
                    .install_host_session_scope(
                        0,
                        HostSessionScopeBinding {
                            schema_version: HOST_SESSION_SCOPE_VERSION,
                            grant_id: Uuid::new_v4(),
                            revision: 1,
                            caller: HostScopeSessionObservation::capture(&self.existing, vec![])
                                .unwrap(),
                            targets: BTreeMap::new(),
                            creates: BTreeMap::from([(
                                operation,
                                HostSessionCreateScope {
                                    operation_id: operation,
                                    project_id: "project-a".parse().unwrap(),
                                    workspace_path: candidate.workspace_path_meta().unwrap().into(),
                                    created_target_rights: BTreeSet::new(),
                                },
                            )]),
                        },
                    )
                    .await
                    .unwrap();
                Route::Host {
                    token,
                    caller,
                    operation,
                }
            }
            2 => Route::Supervisor(self.supervisor.clone()),
            _ => unreachable!(),
        };
        (candidate, route)
    }
}

#[derive(Clone)]
enum Route {
    Ordinary,
    Host {
        token: HostSessionScopeToken,
        caller: SessionCallerReference,
        operation: Uuid,
    },
    Supervisor(SupervisorReference),
}

impl Route {
    async fn publish(&self, store: &SessionStoreV2, candidate: &Session) -> io::Result<()> {
        match self {
            Self::Ordinary => store.save_session(candidate).await,
            Self::Host {
                token,
                caller,
                operation,
            } => {
                store
                    .create_host_scoped_root(token, caller, *operation, Some(1), candidate)
                    .await
            }
            Self::Supervisor(reference) => store
                .create_and_attach_supervised_root(reference, 2, candidate)
                .await
                .map(|_| ()),
        }
    }
}

#[tokio::test]
async fn root_birth_existing_root_progress_and_scope_reads_remain_shared() {
    let f = Fixture::new().await;
    let _authority = f
        .store
        .lock_supervisor_followup(&f.supervisor, "authority-target")
        .await
        .unwrap();
    let mut progress = f.existing.clone();
    progress.title = "existing Root progressed while another scope is retained".into();
    progress.metadata_version += 1;
    tokio::time::timeout(DEADLINE, f.store.save_session(&progress))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        f.store
            .load_root_authority(&progress.id)
            .await
            .unwrap()
            .unwrap()
            .title,
        progress.title
    );
    let observations = [HostScopeSessionObservation::capture(&progress, vec![]).unwrap()];
    let scope = tokio::time::timeout(
        DEADLINE,
        f.store
            .lock_host_scope_observations(&observations, None, &[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!scope.retains_exclusive_lifecycle());
}

#[tokio::test]
async fn root_birth_all_entrypoints_wait_for_retained_shared_authority() {
    for kind in 0..3 {
        let f = Fixture::new().await;
        let (candidate, route) = f.route(kind).await;
        let authority = f
            .store
            .lock_supervisor_followup(&f.supervisor, "authority-target")
            .await
            .unwrap();
        let mut publication = Box::pin(route.publish(&f.store, &candidate));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), publication.as_mut())
                .await
                .is_err(),
            "route {kind}"
        );
        assert!(!f.store.sessions_dir.join(&candidate.id).exists());
        drop(authority);
        tokio::time::timeout(DEADLINE, publication)
            .await
            .unwrap()
            .unwrap();
        let actual = f
            .store
            .load_root_authority(&candidate.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.created_at, candidate.created_at);
        assert_eq!(actual.kind, SessionKind::Root);
    }
}

#[tokio::test]
async fn root_birth_final_shared_writer_guard_and_missing_title_fail_closed() {
    let f = Fixture::new().await;
    let root = Session::new("missing-root-source", "model");
    let lifecycle = f.store.lock_session_lifecycle_shared().await.unwrap();
    let task = f.store.lock_runtime_task_sidecar_shared().await.unwrap();
    let session = f
        .store
        .acquire_session_maintenance_lock(&root.id)
        .await
        .unwrap();
    let guards = DefaultWriterGuards::shared(lifecycle, task, session);
    let full = f
        .store
        .save_session_after_lock(&root, Instant::now(), &guards, None)
        .await
        .unwrap_err();
    assert_eq!(full.kind(), io::ErrorKind::WouldBlock);
    let runtime = f
        .store
        .save_runtime_state_after_lock(
            &root,
            &SessionStoreV2::root_rel_path(&root.id),
            Instant::now(),
            &guards,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(runtime.kind(), io::ErrorKind::WouldBlock);
    assert!(!f.store.sessions_dir.join(&root.id).exists());
    drop(guards);
    assert_eq!(
        f.store.save_manual_title(&root).await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(!f.store.sessions_dir.join(&root.id).exists());
    // A genuinely new runtime save still takes its existing full-save fallback
    // before acquiring physical guards, and therefore uses the birth fence.
    f.store.save_runtime_state(&root).await.unwrap();
    assert_eq!(
        f.store
            .load_root_authority(&root.id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        root.created_at
    );
    // A dangling index must not convert a runtime-only checkpoint into birth.
    fs::remove_dir_all(f.store.sessions_dir.join(&root.id))
        .await
        .unwrap();
    assert_eq!(
        f.store.save_runtime_state(&root).await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(!f.store.sessions_dir.join(&root.id).exists());
}

struct Release(Arc<DefaultWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn lifecycle_shared_available(home: &Path) -> bool {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(home.join(SESSION_LIFECYCLE_LOCK_FILE))
        .unwrap();
    match FileExt::try_lock_shared(&file) {
        Ok(()) => {
            FileExt::unlock(&file).unwrap();
            true
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => false,
        Err(error) => panic!("unexpected lifecycle lock error: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_birth_cancel_retains_exclusive_owner_and_pending_proof_stays_closed() {
    for kind in 0..3 {
        let f = Fixture::new().await;
        let (candidate, route) = f.route(kind).await;
        let hook = DefaultWriteHook::install(
            &f.store,
            "session.json",
            DurableWritePhase::BeforeReplace,
            false,
        );
        let _release = Release(hook.clone());
        let worker = {
            let store = f.store.clone();
            let route = route.clone();
            let candidate = candidate.clone();
            tokio::spawn(async move { route.publish(&store, &candidate).await })
        };
        hook.wait();
        assert!(
            !lifecycle_shared_available(f.home.path()),
            "route {kind} did not retain exclusive lifecycle"
        );
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert!(
            !lifecycle_shared_available(f.home.path()),
            "cancel released route {kind}'s running physical job"
        );
        hook.release();
        tokio::time::timeout(DEADLINE, async {
            while !lifecycle_shared_available(f.home.path()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let directory = f.store.sessions_dir.join(&candidate.id);
        assert!(directory.join("session.json").is_file());
        assert!(directory.join(RUNTIME_SIDECAR_FILE).is_file());
        // The owned Main job completes, but cancellation did not commit the
        // enclosing Root authority proof. Existing recovery must not invent
        // that acknowledgement from matching files, including on a cold Store.
        let before = (
            fs::read(directory.join("session.json")).await.unwrap(),
            fs::read(directory.join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap(),
        );
        let reopened = SessionStoreV2::new(f.home.path().into()).await.unwrap();
        assert!(reopened.load_root_authority(&candidate.id).await.is_err());
        assert!(route.publish(&f.store, &candidate).await.is_err());
        assert_eq!(
            fs::read(directory.join("session.json")).await.unwrap(),
            before.0
        );
        assert_eq!(
            fs::read(directory.join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap(),
            before.1
        );
    }
}
