//! Shared concrete Session readers and fresh Root entry for Host defaults.
//!
//! Installing this surface grants nothing. Every operation still needs its
//! original caller, final Host scope and native permission checks. Restricted
//! Child executors retain their separately admitted tool ceiling.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FrameworkSessionToolGuard, FrameworkSessionToolKind as Kind, HostFrameworkSessionTool, Tool,
    ToolCall, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult,
    ToolSchema,
};
use bamboo_domain::{SessionCallerReference, Storage};
use bamboo_storage::SessionStoreV2;
use serde_json::json;
use tokio::sync::RwLock;

use crate::session_control::root_creation::{SessionRootCreatePort, SupervisedRootCreateRequest};
use crate::session_control::versioned::{SpawnSessionTool, VersionedRootTools};
use crate::{OverlayToolExecutor, SessionInspectorTool};

/// Compose the same actual reader and fresh-operation instances for Base,
/// scheduler/default execution and SDK defaults. Explicit caller-selected tool
/// sets need not call this factory. The supplied permission executor belongs to
/// the same Host assembly as `base`; no registry reconstruction grants authority.
pub fn assemble_framework_session_tools(
    base: Arc<dyn ToolExecutor>,
    permission_executor: Arc<bamboo_tools::BuiltinToolExecutor>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<bamboo_llm::Config>>,
    root_creator: Option<Arc<dyn SessionRootCreatePort>>,
) -> Arc<dyn ToolExecutor> {
    assemble_framework_session_tools_with_message(
        base,
        permission_executor,
        session_store,
        storage,
        config,
        root_creator,
        None,
    )
}

/// A messaging proof requires the actual typed Host instance, not discovery by
/// name/contract in an arbitrary executor or mutable registry.
pub fn assemble_framework_session_tools_with_message(
    base: Arc<dyn ToolExecutor>,
    permission_executor: Arc<bamboo_tools::BuiltinToolExecutor>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<bamboo_llm::Config>>,
    root_creator: Option<Arc<dyn SessionRootCreatePort>>,
    message: Option<Arc<crate::SessionMessageTool>>,
) -> Arc<dyn ToolExecutor> {
    let base: Arc<dyn ToolExecutor> = match &message {
        Some(tool) => Arc::new(OverlayToolExecutor::new(base, tool.clone())),
        None => base,
    };
    let history = Arc::new(SessionInspectorTool::host_routed(
        session_store.clone(),
        storage.clone(),
    ));
    let tools: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(base, history.clone()));
    let current = Arc::new(SessionInspectorTool::current(
        session_store,
        storage.clone(),
    ));
    let tools: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(tools, current.clone()));
    let spawn = Arc::new(SpawnSessionTool::new_with_caller_port(
        root_creator.unwrap_or_else(|| Arc::new(UnconfiguredRootCreator)),
        storage.clone(),
        config.clone(),
        tools.clone(),
    ));
    let inner: Arc<dyn ToolExecutor> = Arc::new(VersionedRootTools::new(
        tools,
        spawn.clone(),
        storage,
        permission_executor,
    ));
    let guard = Arc::new(LiveConfigurationGuard {
        config,
        installed: inner.clone(),
    });
    let mut instances: Vec<(Kind, Arc<dyn Tool>)> = vec![
        (Kind::History, history),
        (Kind::CurrentHistory, current),
        (Kind::SpawnRoot, spawn),
    ];
    if let Some(message) = message {
        instances.push((Kind::Message, message));
    }
    let proofs = instances
        .into_iter()
        .map(|(kind, tool)| {
            (
                kind,
                HostFrameworkSessionTool::from_host_instance(
                    kind,
                    tool,
                    inner.clone(),
                    guard.clone(),
                )
                .expect("Host assembly owns its just-constructed immutable Session instance"),
            )
        })
        .collect();
    Arc::new(ProvenSessionTools { inner, proofs })
}

struct LiveConfigurationGuard {
    config: Arc<RwLock<bamboo_llm::Config>>,
    installed: Arc<dyn ToolExecutor>,
}
#[async_trait]
impl FrameworkSessionToolGuard for LiveConfigurationGuard {
    async fn check(
        &self,
        kind: Kind,
        _: &ToolCall,
        _: &serde_json::Value,
        _: &ToolCtx,
    ) -> Result<(), ToolError> {
        let config = self.config.read().await;
        if config.disabled_tool_references().iter().any(|reference| {
            bamboo_domain::resolve_session_tool_reference(Some(2), reference, |name| {
                self.installed.owns_exact_tool(name)
            })
            .as_deref()
                == Some(kind.execution_name())
        }) {
            return Err(ToolError::Execution(format!(
                "framework_session_disabled: {} is disabled by this Host",
                kind.execution_name()
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ProvenSessionTools {
    inner: Arc<dyn ToolExecutor>,
    proofs: std::collections::BTreeMap<Kind, HostFrameworkSessionTool>,
}
impl ProvenSessionTools {
    fn proof(&self, name: &str) -> Option<&HostFrameworkSessionTool> {
        self.proofs
            .get(&Kind::from_execution_name(name)?)
            .filter(|proof| proof.is_current())
    }
}
#[async_trait]
impl ToolExecutor for ProvenSessionTools {
    fn for_session(&self, session: &bamboo_domain::Session) -> Option<Arc<dyn ToolExecutor>> {
        let inner = self
            .inner
            .for_session(session)
            .unwrap_or_else(|| self.inner.clone());
        let proofs = self
            .proofs
            .iter()
            .filter_map(|(kind, proof)| {
                proof.rebind(inner.clone()).ok().map(|proof| (*kind, proof))
            })
            .collect();
        Some(Arc::new(Self { inner, proofs }))
    }
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.inner.execute(call).await
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.inner.execute_with_context_outcome(call, ctx).await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.inner.execute_with_context(call, ctx).await
    }
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.inner
            .execute_exact_with_context_outcome(call, name, ctx)
            .await
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner.check_permissions_for(call, ctx).await
    }
    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner
            .check_permissions_for_exact(call, name, ctx)
            .await
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner
            .check_permissions_for_resolved(call, name, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.inner.list_tools()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        self.inner.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if self.proof(name).is_some() {
            Some(self)
        } else {
            self.inner.exact_tool_owner(name)
        }
    }
    fn local_execution_contract(&self, name: &str) -> Option<&'static str> {
        self.proof(name)
            .and_then(HostFrameworkSessionTool::execution_contract)
    }
    fn local_tool_instance(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.proof(name).map(HostFrameworkSessionTool::instance)
    }
    fn local_framework_session_tool(&self, name: &str) -> Option<HostFrameworkSessionTool> {
        self.proof(name).cloned()
    }
    fn call_parallel_classification(
        &self,
        call: &ToolCall,
    ) -> (bamboo_agent_core::ToolMutability, bool) {
        self.inner.call_parallel_classification(call)
    }
    fn tool_mutability(&self, name: &str) -> bamboo_agent_core::ToolMutability {
        self.inner.tool_mutability(name)
    }
    fn tool_concurrency_safe(&self, name: &str) -> bool {
        self.inner.tool_concurrency_safe(name)
    }
    fn tool_guidance(&self) -> Option<String> {
        self.inner.tool_guidance()
    }
}

struct UnconfiguredRootCreator;

#[async_trait]
impl SessionRootCreatePort for UnconfiguredRootCreator {
    async fn scope(&self, _: &SessionCallerReference) -> Result<ToolResult, ToolError> {
        Ok(ToolResult::text(false, json!({
            "action":"scope", "code":"session_root_creator_unconfigured",
            "message":"This Host has not installed a caller-scoped Root creation port. No Project, workspace or creation authority is inferred."
        }).to_string()))
    }

    async fn create(
        &self,
        _: &SessionCallerReference,
        _: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "session_root_creator_unconfigured: no caller-scoped Root creation port is installed"
                .into(),
        ))
    }
}
