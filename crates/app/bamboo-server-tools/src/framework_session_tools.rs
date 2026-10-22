//! Shared concrete Session readers and fresh Root entry for Host defaults.
//!
//! Installing this surface grants nothing. Every operation still needs its
//! original caller, final Host scope and native permission checks. Restricted
//! Child executors retain their separately admitted tool ceiling.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolExecutor, ToolResult};
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
    let history = Arc::new(SessionInspectorTool::host_routed(
        session_store.clone(),
        storage.clone(),
    ));
    let tools: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(base, history));
    let current = Arc::new(SessionInspectorTool::current(
        session_store,
        storage.clone(),
    ));
    let tools: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(tools, current));
    let spawn = Arc::new(SpawnSessionTool::new_with_caller_port(
        root_creator.unwrap_or_else(|| Arc::new(UnconfiguredRootCreator)),
        storage.clone(),
        config,
        tools.clone(),
    ));
    Arc::new(VersionedRootTools::new(
        tools,
        spawn,
        storage,
        permission_executor,
    ))
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
