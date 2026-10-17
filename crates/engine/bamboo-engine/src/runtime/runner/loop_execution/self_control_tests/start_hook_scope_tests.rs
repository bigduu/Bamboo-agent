//! Start installs its generation during result apply. Its own native hook must
//! already observe the active restriction, before the next sibling dispatch.
use super::*;

struct NewlyStartedPolicyHook {
    observed: Mutex<Vec<bool>>,
    storage: Arc<dyn Storage>,
}

#[async_trait]
impl bamboo_agent_core::AgentHook for NewlyStartedPolicyHook {
    fn point(&self) -> bamboo_domain::AgentHookPoint {
        bamboo_domain::AgentHookPoint::AfterToolExecution
    }

    fn may_suspend(&self) -> bool {
        false
    }

    fn matches(&self, payload: &bamboo_domain::HookPayload) -> bool {
        matches!(payload, bamboo_domain::HookPayload::ToolResult { tool_call_id, .. }
            if tool_call_id == "start")
    }

    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        _: &bamboo_domain::HookPayload,
        session: &Session,
    ) -> bamboo_domain::HookResult {
        let state = read_loop_state(session)
            .unwrap()
            .expect("Start was applied");
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_loop_state(&durable).unwrap(), Some(state.clone()));
        assert_eq!(
            (state.iterations_started, state.iterations_completed),
            (0, 0)
        );
        assert_eq!(state.control.as_ref().unwrap().operations.len(), 1);
        self.observed
            .lock()
            .unwrap()
            .push(bamboo_agent_core::tools::suspension_allowed());
        bamboo_domain::HookResult::Continue
    }
}

#[tokio::test]
async fn self_control_newly_started_generation_scopes_its_own_post_tool_hook() {
    let mut fixture = Fixture::new(2, 6).await;
    let hook = Arc::new(NewlyStartedPolicyHook {
        observed: Mutex::new(Vec::new()),
        storage: fixture.storage.clone(),
    });
    let mut hooks = crate::runtime::HookRunner::new();
    hooks.register(hook.clone());
    fixture.config.as_mut().unwrap().hook_runner = Arc::new(hooks);
    let provider = Arc::new(ScriptedProvider::new(
        Script::TwoIterations,
        fixture.storage.clone(),
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_terminal_integrity(&fixture, &provider, &events, 3, 1);
    assert_eq!(
        read_loop_state(&fixture.session).unwrap().unwrap().status,
        LoopStatus::Completed
    );
    assert_eq!(
        *hook.observed.lock().unwrap(),
        [false],
        "Start's own post-tool callback must see the newly active host restriction"
    );
    fixture.close().await;
}
