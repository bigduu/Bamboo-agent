//! Positive, immutable native tool ceiling at the callable boundary.
use super::*;
use bamboo_agent_core::tools::{
    input_guard::{check_parsed_tool_input, check_raw_tool_input},
    parse_tool_args_best_effort, Tool, ToolCall, ToolError, ToolExecutionContext, ToolOutcome,
    ToolResult, ToolSchema,
};
use std::collections::BTreeSet;

pub(super) struct LocalChildTools {
    inner: Arc<dyn ToolExecutor>,
    allowed: BTreeSet<String>,
    child_id: String,
    read_only: bool,
    nested_allowed: bool,
    subagent: Option<Arc<dyn Tool>>,
    store: Arc<bamboo_storage::SessionStoreV2>,
    owner: ChildActorRuntimeWrite,
}
impl LocalChildTools {
    pub fn new(
        inner: Arc<dyn ToolExecutor>,
        names: Vec<String>,
        session: &Session,
        store: Arc<bamboo_storage::SessionStoreV2>,
        owner: ChildActorRuntimeWrite,
        disabled: Option<&[String]>,
    ) -> Result<Self, AgentError> {
        let mut allowed: BTreeSet<_> = names.iter().cloned().collect();
        if allowed.len() != names.len() || allowed.iter().any(|name| !inner.owns_exact_tool(name)) {
            return Err(failure("native tool ceiling changed"));
        }
        let denied = disabled.unwrap_or_default().iter().cloned().chain(
            session
                .metadata
                .get("disabled_tools")
                .map(|raw| serde_json::from_str::<Vec<String>>(raw))
                .transpose()
                .map_err(failure)?
                .unwrap_or_default(),
        );
        let mut subagent_denied = false;
        for denied in denied {
            // Resolve against all exact identities before applying restrictions:
            // an unavailable exact foreign::Read must never fall through to Read.
            if let Some(name) = bamboo_domain::resolve_tool_reference_name(&denied, |name| {
                name == "SubAgent" || inner.owns_exact_tool(name)
            }) {
                subagent_denied |= name == "SubAgent";
                allowed.remove(&name);
            }
        }
        let read_only = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.read_only || state.plan_mode.is_some());
        if read_only {
            allowed.retain(|name| {
                matches!(
                    name.as_str(),
                    "Read" | "Glob" | "Grep" | "GetFileInfo" | "ViewImage"
                )
            });
        }
        let strict = bamboo_domain::ChildContextBinding::from_session(session)
            .map_err(failure)?
            .is_some()
            || crate::session_app::child_session::named_profile::has_named_profile(session)
            || session
                .metadata
                .contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY);
        Ok(Self {
            store,
            owner,
            inner,
            allowed,
            child_id: session.id.clone(),
            read_only,
            nested_allowed: !strict && !read_only && !subagent_denied,
            subagent: None,
        })
    }

    /// Install only the application's already-bound logical Child tool. Strict,
    /// read-only and explicitly disabled routes cannot widen their fixed ceiling.
    pub fn with_canonical_subagent(mut self, subagent: Option<Arc<dyn Tool>>) -> Self {
        if self.nested_allowed {
            if let Some(tool) = subagent.filter(|tool| tool.name() == "SubAgent") {
                self.allowed.insert("SubAgent".into());
                self.subagent = Some(tool);
            }
        }
        self
    }

    fn registered(&self, name: &str) -> bool {
        self.subagent
            .as_ref()
            .is_some_and(|tool| name == tool.name())
            || self.inner.owns_exact_tool(name)
    }

    fn check(&self, call: &ToolCall, ctx: &ToolExecutionContext<'_>) -> Result<String, ToolError> {
        if ctx.session_id != Some(self.child_id.as_str()) || ctx.tool_call_id != call.id {
            return Err(ToolError::Execution(
                "local Child tool owner changed".into(),
            ));
        }
        bamboo_domain::resolve_tool_reference_name(&call.function.name, |name| {
            self.registered(name)
        })
        .filter(|name| self.allowed.contains(name))
        .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))
    }

    fn check_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<(), ToolError> {
        if self.check(call, ctx)? != name {
            return Err(ToolError::NotFound(call.function.name.clone()));
        }
        Ok(())
    }

    async fn check_live(&self) -> Result<(), ToolError> {
        self.store
            .publish_child_actor_runtime_event(&self.owner, Box::new(|validate| validate()))
            .await
            .map_err(|error| {
                ToolError::Execution(format!("local Child tool fence rejected: {error}"))
            })
    }

    async fn execute_selected(
        &self,
        call: &ToolCall,
        name: &str,
        mut ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.check_exact(call, name, &ctx)?;
        self.check_live().await?;
        ctx.plan_read_only |= self.read_only;
        let invoke = async {
            if let Some(subagent) = self.subagent.as_ref().filter(|tool| tool.name() == name) {
                check_raw_tool_input(name, &call.function.arguments)?;
                let args = ctx
                    .pre_parsed_args
                    .cloned()
                    .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
                check_parsed_tool_input(name, &args)?;
                if let Some(outcome) = self
                    .inner
                    .check_permissions_for_resolved(call, name, &args, &ctx)
                    .await?
                {
                    return Ok(outcome);
                }
                // A permission review can await a parent decision. Recheck the
                // exact canonical owner immediately before invoking its Child port.
                self.check_live().await?;
                subagent.invoke(args, ctx.to_tool_ctx()).await
            } else {
                self.inner
                    .execute_exact_with_context_outcome(call, name, ctx)
                    .await
            }
        };
        match bamboo_tools::LocalTaskResources::current() {
            Some(scope) => scope.guard_tool_invocation(invoke).await,
            None => invoke.await,
        }
    }
}

#[async_trait]
impl ToolExecutor for LocalChildTools {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "local Child requires execution context".into(),
        ))
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let name = self.check(call, &ctx)?;
        self.execute_selected(call, &name, ctx).await
    }
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.execute_selected(call, name, ctx).await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ctx)
            .await
            .map(ToolOutcome::into_tool_result)
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        let name = self.check(call, ctx)?;
        self.check_permissions_for_exact(call, &name, ctx).await
    }
    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.check_exact(call, name, ctx)?;
        self.check_live().await?;
        let mut ctx = *ctx;
        ctx.plan_read_only |= self.read_only;
        if self
            .subagent
            .as_ref()
            .is_some_and(|tool| tool.name() == name)
        {
            let args = ctx
                .pre_parsed_args
                .cloned()
                .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
            self.inner
                .check_permissions_for_resolved(call, name, &args, &ctx)
                .await
        } else {
            self.inner
                .check_permissions_for_exact(call, name, &ctx)
                .await
        }
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.check_exact(call, name, ctx)?;
        self.check_live().await?;
        let mut ctx = *ctx;
        ctx.plan_read_only |= self.read_only;
        self.inner
            .check_permissions_for_resolved(call, name, args, &ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut schemas: Vec<_> = self
            .inner
            .list_tools()
            .into_iter()
            .filter(|schema| {
                self.allowed.contains(&schema.function.name)
                    && !self
                        .subagent
                        .as_ref()
                        .is_some_and(|tool| tool.name() == schema.function.name)
            })
            .collect();
        if let Some(subagent) = &self.subagent {
            schemas.push(subagent.to_schema());
        }
        schemas
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        self.allowed.contains(name) && self.registered(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if !self.allowed.contains(name) {
            return None;
        }
        if self
            .subagent
            .as_ref()
            .is_some_and(|tool| tool.name() == name)
        {
            Some(self)
        } else {
            self.inner.exact_tool_owner(name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, ToolCtx};
    use bamboo_domain::{ActorActivationClaim, ActorDirectoryPort};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Base {
        executions: AtomicUsize,
        gates: AtomicUsize,
        deny: AtomicBool,
        readonly: AtomicBool,
        stall: AtomicBool,
        entered: tokio::sync::Notify,
    }
    const NAMES: &[&str] = &["Read", "Write", "Bash", "Task", "foreign::Read"];
    #[async_trait]
    impl ToolExecutor for Base {
        async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("requires context")
        }
        async fn execute_with_context_outcome(
            &self,
            _: &ToolCall,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            self.readonly.store(ctx.plan_read_only, Ordering::SeqCst);
            if self.stall.load(Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(completed())
        }
        async fn check_permissions_for(
            &self,
            _: &ToolCall,
            _: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            self.gates.fetch_add(1, Ordering::SeqCst);
            if self.deny.load(Ordering::SeqCst) {
                Err(ToolError::Execution("permission denied".into()))
            } else {
                Ok(None)
            }
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            NAMES
                .iter()
                .map(|name| {
                    serde_json::from_value(json!({"type":"function", "function": {
                "name":name,"description":"fixture","parameters":{}}}))
                    .unwrap()
                })
                .collect()
        }
        fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
            NAMES.contains(&name).then_some(self as &dyn ToolExecutor)
        }
    }
    fn completed() -> ToolOutcome {
        ToolOutcome::Completed(ToolResult {
            success: true,
            result: "fixture".into(),
            display_preference: None,
            images: vec![],
        })
    }
    struct Subagent(AtomicUsize);
    #[async_trait]
    impl Tool for Subagent {
        fn name(&self) -> &str {
            "SubAgent"
        }
        fn description(&self) -> &str {
            "canonical fixture"
        }
        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object"})
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(completed())
        }
    }
    struct Fixture {
        store: Arc<bamboo_storage::SessionStoreV2>,
        child: Session,
        owner: ChildActorRuntimeWrite,
        base: Arc<Base>,
        _home: tempfile::TempDir,
    }
    impl Fixture {
        async fn new() -> Self {
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(home.path().into())
                    .await
                    .unwrap(),
            );
            let root = Session::new("tool-root", "model");
            store.save_session(&root).await.unwrap();
            let child = Session::new_child_of("tool-child", &root, "model", "Child");
            store.save_session(&child).await.unwrap();
            let now = chrono::Utc::now();
            let activation = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: child.id.clone(),
                    run_id: "tool-run".into(),
                    lease_owner: "local-tool-host".into(),
                    lease_expires_at: now + chrono::Duration::minutes(1),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                })
                .await
                .unwrap();
            store
                .start_activation(&activation.fence(), now)
                .await
                .unwrap();
            let owner = ChildActorRuntimeWrite::from_session(activation.fence(), &child).unwrap();
            Self {
                store,
                child,
                owner,
                base: Arc::default(),
                _home: home,
            }
        }
        fn tools(&self, session: &Session, names: &[&str], denied: &[String]) -> LocalChildTools {
            LocalChildTools::new(
                self.base.clone(),
                names.iter().map(|name| (*name).into()).collect(),
                session,
                self.store.clone(),
                self.owner.clone(),
                Some(denied),
            )
            .unwrap()
        }
    }
    fn call(name: &str) -> ToolCall {
        ToolCall {
            id: "fixture-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: "{}".into(),
            },
        }
    }

    #[tokio::test]
    async fn cancelled_custom_same_name_write_cannot_certify_resource_stop() {
        let f = Fixture::new().await;
        f.base.stall.store(true, Ordering::SeqCst);
        let tools = f.tools(&f.child, &["Write"], &[]);
        let scope =
            bamboo_tools::LocalTaskResources::new(&f.child.id, "tool-run", f.child.created_at);
        let owned = scope.clone();
        let child_id = f.child.id.clone();
        let waiting = tokio::spawn(async move {
            let call = call("Write");
            let mut ctx = ToolExecutionContext::none(&call.id);
            ctx.session_id = Some(&child_id);
            owned
                .scope(tools.execute_with_context_outcome(&call, ctx))
                .await
        });
        f.base.entered.notified().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert!(scope.close_and_drain().await.is_err());
    }

    #[tokio::test]
    async fn denied_exact_foreign_identity_cannot_fall_through_to_allowed_native_alias() {
        let f = Fixture::new().await;
        let tools = f.tools(&f.child, &["Read"], &[]);
        let foreign = call("foreign::Read");
        let mut ctx = ToolExecutionContext::none(&foreign.id);
        ctx.session_id = Some(&f.child.id);
        assert!(tools
            .execute_with_context_outcome(&foreign, ctx)
            .await
            .is_err());
        assert!(tools.check_permissions_for(&foreign, &ctx).await.is_err());
        assert!(tools
            .execute_exact_with_context_outcome(&foreign, "Read", ctx)
            .await
            .is_err());
        assert_eq!(f.base.executions.load(Ordering::SeqCst), 0);
        let alias = call("legacy::Read");
        assert!(
            tools
                .execute_with_context_outcome(&alias, ctx)
                .await
                .unwrap()
                .into_tool_result()
                .success
        );
        assert_eq!(f.base.executions.load(Ordering::SeqCst), 1);
        assert!(std::ptr::addr_eq(
            tools.exact_tool_owner("Read").unwrap(),
            f.base.as_ref() as &dyn ToolExecutor
        ));
    }

    #[tokio::test]
    async fn readonly_child_denies_direct_side_effect_calls_in_auto_and_bypass() {
        let f = Fixture::new().await;
        let mut child = f.child.clone();
        child.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("tool-run"));
        child.agent_runtime_state.as_mut().unwrap().read_only = true;
        let tools = f
            .tools(&child, NAMES, &[])
            .with_canonical_subagent(Some(Arc::new(Subagent(AtomicUsize::new(0)))));
        for (auto, bypass) in [(false, false), (true, false), (false, true)] {
            for name in [
                "Write",
                "Bash",
                "Task",
                "foreign::Read",
                "SubAgent",
                "legacy::Bash",
            ] {
                let denied = call(name);
                let mut ctx = ToolExecutionContext::none(&denied.id);
                ctx.session_id = Some(&child.id);
                ctx.auto_approve_permissions = auto;
                ctx.bypass_permissions = bypass;
                assert!(
                    tools
                        .execute_with_context_outcome(&denied, ctx)
                        .await
                        .is_err(),
                    "{name}"
                );
                assert!(
                    tools.check_permissions_for(&denied, &ctx).await.is_err(),
                    "{name}"
                );
            }
        }
        assert_eq!(f.base.executions.load(Ordering::SeqCst), 0);
        let read = call("Read");
        let mut ctx = ToolExecutionContext::none(&read.id);
        ctx.session_id = Some(&child.id);
        tools
            .execute_with_context_outcome(&read, ctx)
            .await
            .unwrap();
        assert!(f.base.readonly.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn canonical_subagent_overlay_keeps_permission_gate_and_strict_ceiling() {
        let f = Fixture::new().await;
        let subagent = Arc::new(Subagent(AtomicUsize::new(0)));
        let tools = f
            .tools(&f.child, NAMES, &[])
            .with_canonical_subagent(Some(subagent.clone()));
        assert!(tools.owns_exact_tool("SubAgent"));
        let nested = call("SubAgent");
        let mut ctx = ToolExecutionContext::none(&nested.id);
        ctx.session_id = Some(&f.child.id);
        f.base.deny.store(true, Ordering::SeqCst);
        assert!(tools
            .execute_with_context_outcome(&nested, ctx)
            .await
            .is_err());
        assert_eq!(subagent.0.load(Ordering::SeqCst), 0);
        f.base.deny.store(false, Ordering::SeqCst);
        tools
            .execute_with_context_outcome(&nested, ctx)
            .await
            .unwrap();
        assert_eq!(subagent.0.load(Ordering::SeqCst), 1);
        for key in [
            crate::session_app::child_session::named_profile::NAMED_PROFILE_BINDING_KEY,
            crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY,
        ] {
            let mut strict = f.child.clone();
            strict.metadata.insert(key.into(), "fixture".into());
            let strict = f
                .tools(&strict, &["Read"], &[])
                .with_canonical_subagent(Some(subagent.clone()));
            assert!(!strict.owns_exact_tool("SubAgent"));
            assert!(strict
                .execute_with_context_outcome(&nested, ctx)
                .await
                .is_err());
        }
        let disabled = f
            .tools(&f.child, NAMES, &["legacy::SubAgent".into()])
            .with_canonical_subagent(Some(subagent));
        assert!(!disabled.owns_exact_tool("SubAgent"));
    }

    #[tokio::test]
    async fn permission_only_paths_validate_live_owner_before_inner_gate() {
        let f = Fixture::new().await;
        let tools = f.tools(&f.child, NAMES, &[]);
        let read = call("Read");
        let mut ctx = ToolExecutionContext::none(&read.id);
        ctx.session_id = Some(&f.child.id);
        tools.check_permissions_for(&read, &ctx).await.unwrap();
        assert_eq!(f.base.gates.load(Ordering::SeqCst), 1);
        f.store
            .finish_activation(
                &f.owner.fence,
                chrono::Utc::now(),
                bamboo_domain::ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        assert!(tools.check_permissions_for(&read, &ctx).await.is_err());
        assert!(tools
            .check_permissions_for_exact(&read, "Read", &ctx)
            .await
            .is_err());
        assert!(tools
            .check_permissions_for_resolved(&read, "Read", &json!({}), &ctx)
            .await
            .is_err());
        assert_eq!(f.base.gates.load(Ordering::SeqCst), 1);
    }
}
