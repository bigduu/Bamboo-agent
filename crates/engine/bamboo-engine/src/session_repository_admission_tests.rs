//! Canonical admission through actual Repository-issued Root Actor bindings.
use super::*;
use bamboo_domain::{
    ActorActivationFinish, ActorDirectoryPort, Message, RootActorExecutionBinding,
    RuntimeSessionPersistence, SessionAuthorityConflict,
};
use bamboo_storage::SessionStoreV2;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(8);

fn repository(storage: Arc<dyn Storage>, directory: Arc<SessionStoreV2>) -> SessionRepository {
    SessionRepository::new(
        Arc::default(),
        storage.clone(),
        Arc::new(LockedSessionStore::new(storage)),
    )
    .with_root_actor_directory(directory)
}

async fn finish(binding: &mut RootActorExecutionBinding) {
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            chrono::Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
}

#[tokio::test]
async fn bound_root_admission_reads_full_canonical_history_with_stale_index() {
    let home = tempfile::tempdir().unwrap();
    let stale = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let publisher = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let mut root = Session::new("bound-root-admission-stale-index", "model");
    root.add_message(Message::assistant("completed iteration", None));
    root.add_message(Message::user("pending repeat input"));
    root.metadata
        .insert("runtime.admission_fixture".into(), "durable-current".into());
    publisher.save_session(&root).await.unwrap();
    let host = repository(stale.clone(), publisher.clone());
    let mut cache_sentinel = root.clone();
    cache_sentinel.messages.clear();
    cache_sentinel.add_message(Message::user("cache must not be published by admission"));
    host.cache.insert(
        root.id.clone(),
        Arc::new(crate::SessionSnapshot::new(cache_sentinel.clone())),
    );
    let mut binding = host
        .bind_root_actor_execution(&root, "canonical-admission-run")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stale.load_session(&root.id).await.unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let main = home
        .path()
        .join("sessions")
        .join(&root.id)
        .join("session.json");
    let runtime = main.with_file_name("runtime.json");
    let before = [
        std::fs::read(&main).unwrap(),
        std::fs::read(&runtime).unwrap(),
        std::fs::read(publisher.index_path()).unwrap(),
    ];
    let loaded = binding
        .persistence
        .load_runtime_admission_snapshot(&root.id)
        .await
        .expect("bound Root admission must use the full canonical reader")
        .unwrap();
    assert_eq!(loaded.created_at, binding.owner.created_at);
    assert_eq!(
        serde_json::to_value(&loaded.messages).unwrap(),
        serde_json::to_value(&root.messages).unwrap()
    );
    assert_eq!(
        loaded.messages.last().unwrap().content,
        "pending repeat input"
    );
    assert_eq!(
        loaded.metadata["runtime.admission_fixture"],
        "durable-current"
    );
    assert!(binding
        .persistence
        .load_runtime_admission_snapshot("another-root")
        .await
        .is_err());
    assert_eq!(
        serde_json::to_value(read_cached_session(host.cache(), &root.id).unwrap()).unwrap(),
        serde_json::to_value(cache_sentinel).unwrap()
    );
    assert_eq!(
        [
            std::fs::read(&main).unwrap(),
            std::fs::read(&runtime).unwrap(),
            std::fs::read(publisher.index_path()).unwrap(),
        ],
        before
    );
    assert_eq!(
        stale.load_session(&root.id).await.unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    finish(&mut binding).await;
    assert!(binding
        .persistence
        .load_runtime_admission_snapshot(&root.id)
        .await
        .is_err());
}

struct PausedRootAdmissionStorage {
    inner: Arc<SessionStoreV2>,
    reached: tokio::sync::Barrier,
    release: tokio::sync::Barrier,
}

impl PausedRootAdmissionStorage {
    async fn pause(&self, loaded: Option<Session>) -> std::io::Result<Option<Session>> {
        self.reached.wait().await;
        self.release.wait().await;
        Ok(loaded)
    }
}

#[async_trait::async_trait]
impl Storage for PausedRootAdmissionStorage {
    fn supports_root_actor_runtime_write(&self) -> bool {
        true
    }
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn load_root_authority(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_root_authority(id).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.pause(self.inner.load_session(id).await?).await
    }
    async fn load_execution_admission_snapshot(
        &self,
        id: &str,
    ) -> std::io::Result<Option<Session>> {
        self.pause(self.inner.load_execution_admission_snapshot(id).await?)
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_root_admission_rechecks_fence_after_snapshot_materialization() {
    for rebirth in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut root = Session::new("bound-root-admission-race", "model");
        root.add_message(Message::user("original Root history"));
        store.save_session(&root).await.unwrap();
        let paused = Arc::new(PausedRootAdmissionStorage {
            inner: store.clone(),
            reached: tokio::sync::Barrier::new(2),
            release: tokio::sync::Barrier::new(2),
        });
        let host = repository(paused.clone(), store.clone());
        let mut binding = host
            .bind_root_actor_execution(&root, "original-admission-run")
            .await
            .unwrap()
            .unwrap();
        let reader = binding.persistence.clone();
        let id = root.id.clone();
        let reading =
            tokio::spawn(async move { reader.load_runtime_admission_snapshot(&id).await });
        tokio::time::timeout(WAIT, paused.reached.wait())
            .await
            .expect("actual bound admission reached its materialized snapshot");
        store
            .finish_activation(
                &binding.owner.fence,
                chrono::Utc::now(),
                ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        binding.disarm_abandonment();
        if rebirth {
            assert!(store.delete_session(&root.id).await.unwrap());
            let mut recreated = Session::new(&root.id, "new-model");
            assert_ne!(recreated.created_at, root.created_at);
            recreated.add_message(Message::user("new Root private history"));
            store.save_session(&recreated).await.unwrap();
            root = recreated;
        }
        let replacement_host = repository(store.clone(), store.clone());
        let mut replacement = replacement_host
            .bind_root_actor_execution(&root, "replacement-admission-run")
            .await
            .unwrap()
            .unwrap();
        let main = home
            .path()
            .join("sessions")
            .join(&root.id)
            .join("session.json");
        let runtime = main.with_file_name("runtime.json");
        let before = [
            std::fs::read(&main).unwrap(),
            std::fs::read(&runtime).unwrap(),
            std::fs::read(store.index_path()).unwrap(),
        ];
        paused.release.wait().await;
        let result = tokio::time::timeout(WAIT, reading).await.unwrap().unwrap();
        let error = result.expect_err("superseded Root admission cannot release a snapshot");
        assert!(error
            .get_ref()
            .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
        assert!(host.cache().is_empty());
        assert_eq!(
            [
                std::fs::read(&main).unwrap(),
                std::fs::read(&runtime).unwrap(),
                std::fs::read(store.index_path()).unwrap(),
            ],
            before
        );
        assert_eq!(
            store
                .inspect_actor(&root.id)
                .await
                .unwrap()
                .activation
                .unwrap()
                .fence(),
            replacement.owner.fence
        );
        finish(&mut replacement).await;
    }
}

struct FaultyRootAdmissionStorage {
    inner: Arc<SessionStoreV2>,
    fault: &'static str,
}

#[async_trait::async_trait]
impl Storage for FaultyRootAdmissionStorage {
    fn supports_root_actor_runtime_write(&self) -> bool {
        true
    }
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn load_root_authority(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_root_authority(id).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_session(id).await
    }
    async fn load_execution_admission_snapshot(
        &self,
        id: &str,
    ) -> std::io::Result<Option<Session>> {
        if self.fault == "io" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected canonical read failure",
            ));
        }
        if self.fault == "absent" {
            return Ok(None);
        }
        let mut loaded = self
            .inner
            .load_execution_admission_snapshot(id)
            .await?
            .unwrap();
        match self.fault {
            "id" => loaded.id = "another-root".into(),
            "birth" => loaded.created_at += chrono::Duration::seconds(1),
            "kind" => loaded.kind = bamboo_domain::SessionKind::Child,
            _ => unreachable!(),
        }
        Ok(Some(loaded))
    }
}

#[tokio::test]
async fn bound_root_admission_rejects_changed_identity_and_read_failures() {
    for fault in ["id", "birth", "kind", "absent", "io"] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let root = Session::new("bound-root-admission-fault", "model");
        store.save_session(&root).await.unwrap();
        let storage = Arc::new(FaultyRootAdmissionStorage {
            inner: store.clone(),
            fault,
        });
        let host = repository(storage, store.clone());
        let mut binding = host
            .bind_root_actor_execution(&root, "fault-admission-run")
            .await
            .unwrap()
            .unwrap();
        let main = home
            .path()
            .join("sessions")
            .join(&root.id)
            .join("session.json");
        let runtime = main.with_file_name("runtime.json");
        let before = [
            std::fs::read(&main).unwrap(),
            std::fs::read(&runtime).unwrap(),
            std::fs::read(store.index_path()).unwrap(),
        ];
        let error = binding
            .persistence
            .load_runtime_admission_snapshot(&root.id)
            .await
            .expect_err("invalid bound Root admission must remain rejected");
        if fault == "io" {
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        } else {
            assert!(error
                .get_ref()
                .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
        }
        assert!(host.cache().is_empty());
        assert_eq!(
            [
                std::fs::read(&main).unwrap(),
                std::fs::read(&runtime).unwrap(),
                std::fs::read(store.index_path()).unwrap(),
            ],
            before,
            "fault={fault}"
        );
        finish(&mut binding).await;
    }
}
