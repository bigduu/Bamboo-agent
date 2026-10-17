//! GREEN-only: this uses the newly added target retirement port. Do not put it
//! in old-API RED; an unavailable method is never a behavioral RED receipt.
use super::*;

struct CleanupFailureStore {
    inner: Arc<bamboo_storage::SessionStoreV2>,
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
}
#[async_trait]
impl Storage for CleanupFailureStore {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_session(id).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn save_runtime_state(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_runtime_state(session).await
    }
    async fn load_runtime_control_plane(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_control_plane(id).await
    }
    async fn load_root_authority(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_root_authority(id).await
    }
    async fn load_execution_admission_snapshot(
        &self,
        id: &str,
    ) -> std::io::Result<Option<Session>> {
        self.inner.load_execution_admission_snapshot(id).await
    }
    async fn get_or_create_default_supervisor(
        &self,
        model: &str,
    ) -> std::io::Result<bamboo_domain::SupervisorBootstrapReceipt> {
        self.inner.get_or_create_default_supervisor(model).await
    }
    async fn inspect_supervisor_scope(
        &self,
        supervisor: &SupervisorReference,
    ) -> std::io::Result<bamboo_domain::SupervisorScopeObservation> {
        self.inner.inspect_supervisor_scope(supervisor).await
    }
    async fn mutate_supervisor_management(
        &self,
        request: &bamboo_domain::SupervisorManagementRequest,
    ) -> std::io::Result<bamboo_domain::SupervisorManagementReceipt> {
        self.inner.mutate_supervisor_management(request).await
    }
    async fn inspect_supervisor_link(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
    ) -> std::io::Result<bamboo_domain::SupervisorLinkObservation> {
        self.inner.inspect_supervisor_link(supervisor, target).await
    }
    async fn load_scoped_loop_session(
        &self,
        binding: &bamboo_domain::ScopedLoopControlBinding,
        activation: Arc<dyn bamboo_domain::SessionActivationPort>,
    ) -> std::io::Result<Option<Session>> {
        self.inner
            .load_scoped_loop_session(binding, activation)
            .await
    }
    async fn checkpoint_scoped_loop(
        &self,
        request: bamboo_domain::ScopedLoopControlCheckpoint<'_>,
    ) -> std::io::Result<()> {
        self.inner.checkpoint_scoped_loop(request).await
    }
    fn supports_scoped_loop_transport_retirement(&self) -> bool {
        true
    }
    async fn retire_scoped_loop_transport(
        &self,
        _request: bamboo_domain::storage::ScopedLoopTransportRetirement<'_>,
    ) -> std::io::Result<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Err(std::io::Error::other(
            "controlled target retirement I/O failure",
        ))
    }
}

#[tokio::test]
async fn scoped_cleanup_io_error_keeps_closed_owner_and_cancel_wins_before_complete() {
    for cancelled in [false, true] {
        let store_slot = Arc::new(std::sync::OnceLock::<Arc<CleanupFailureStore>>::new());
        let capture = store_slot.clone();
        let mut f = ScopedPublicFixture::new_with_storage_transform(false, move |inner| {
            let wrapped = Arc::new(CleanupFailureStore {
                inner,
                entered: Notify::new(),
                release: Notify::new(),
                calls: AtomicUsize::new(0),
            });
            assert!(capture.set(wrapped.clone()).is_ok());
            wrapped
        })
        .await;
        let failing = store_slot.get().unwrap().clone();
        let provider = Arc::new(ExitProvider {
            exit: Exit::NoStartComplete,
            storage: f.storage.clone(),
            entered: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
        });
        provider.release.notify_one();
        let (tx, mut rx) = mpsc::channel(1024);
        let mut request = f.request(tx);
        request.provider_override = Some(provider.clone());
        let cancel = request.cancel_token.clone();
        let lease = f.agent.begin_direct_execution(TARGET_ID).await.unwrap();
        let run_id = f.router.current_run_id(TARGET_ID).await.unwrap();
        let result = {
            let run = f
                .agent
                .execute_direct_registered(&mut f.session, request, lease);
            tokio::pin!(run);
            let mut complete = 0;
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    tokio::select! {
                        _ = failing.entered.notified() => break,
                        event = rx.recv() => if let Some(event) = event { complete += usize::from(matches!(event, AgentEvent::Complete { .. })); },
                        result = &mut run => panic!("retirement gate was not reached: {result:?}"),
                    }
                }
                assert_eq!(complete, 0, "success may not publish before retirement");
                assert_eq!(f.router.current_run_id(TARGET_ID).await.as_deref(), Some(run_id.as_str()));
                if cancelled { cancel.cancel(); }
                failing.release.notify_one();
                loop {
                    tokio::select! {
                        result = &mut run => { assert_eq!(complete, 0, "cleanup failure cannot publish Complete"); break result; },
                        event = rx.recv() => if let Some(event) = event { complete += usize::from(matches!(event, AgentEvent::Complete { .. })); },
                    }
                }
            }).await.expect("controlled cleanup error exits boundedly")
        };
        let error = result.expect_err("physical cleanup failure must reject successful completion");
        assert_eq!(error.is_cancelled(), cancelled, "{error:?}");
        assert!(provider.calls.load(Ordering::SeqCst) > 0);
        assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
        assert!(
            failing.calls.load(Ordering::SeqCst) >= 2,
            "done=false must retry during actual registration finish"
        );
        assert_eq!(
            f.router.current_run_id(TARGET_ID).await.as_deref(),
            Some(run_id.as_str())
        );
        let mut complete = 0;
        while let Ok(event) = rx.try_recv() {
            complete += usize::from(matches!(event, AgentEvent::Complete { .. }));
        }
        assert_eq!(complete, 0);
        f.inbox
            .deliver(&bamboo_domain::SessionMessageEnvelope::user_input(
                TARGET_ID,
                "genuine User cannot overtake unresolved cleanup",
            ))
            .await
            .unwrap();
        assert!(f.agent.begin_direct_execution(TARGET_ID).await.is_err());
        f.assert_no_successor();
    }
}
