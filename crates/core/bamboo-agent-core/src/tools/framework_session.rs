//! In-process Host proof for a concrete framework Session tool instance.
//!
//! Neither a schema name nor an execution-contract string can mint this value.
//! The trusted Host factory binds an actual Tool Arc and its complete native
//! permission chain. This is deliberately not a transport or persisted grant.

use super::{
    Tool, ToolCall, ToolClass, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome,
    ToolSchema,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FrameworkSessionToolKind {
    History,
    CurrentHistory,
    SpawnRoot,
    Message,
}
impl FrameworkSessionToolKind {
    pub const ALL: [Self; 4] = [
        Self::History,
        Self::CurrentHistory,
        Self::SpawnRoot,
        Self::Message,
    ];
    pub const fn execution_name(self) -> &'static str {
        match self {
            Self::History => "session_history",
            Self::CurrentHistory => "session_history_current",
            Self::SpawnRoot => "spawn_session",
            Self::Message => "session_message",
        }
    }
    pub fn from_execution_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.execution_name() == name)
    }
    pub const fn is_reader(self) -> bool {
        matches!(self, Self::History | Self::CurrentHistory)
    }
}

/// Restriction-only checks, separate from the one native permission evaluation.
#[async_trait]
pub trait FrameworkSessionToolGuard: Send + Sync {
    async fn check(
        &self,
        kind: FrameworkSessionToolKind,
        call: &ToolCall,
        args: &Value,
        ctx: &ToolCtx,
    ) -> Result<(), ToolError>;
}

#[derive(Clone)]
pub struct HostFrameworkSessionTool {
    kind: FrameworkSessionToolKind,
    tool: Arc<dyn Tool>,
    permissions: Arc<dyn ToolExecutor>,
    guard: Arc<dyn FrameworkSessionToolGuard>,
}
impl HostFrameworkSessionTool {
    /// Trusted Rust Host construction only. Supply the instance constructed by
    /// the Host, never recover authorization from a model/configuration name.
    pub fn from_host_instance(
        kind: FrameworkSessionToolKind,
        tool: Arc<dyn Tool>,
        permissions: Arc<dyn ToolExecutor>,
        guard: Arc<dyn FrameworkSessionToolGuard>,
    ) -> Result<Self, ToolError> {
        let proof = Self {
            kind,
            tool,
            permissions,
            guard,
        };
        if proof.tool.name() != kind.execution_name() || !proof.is_current() {
            return Err(ToolError::Execution(
                "framework_session_instance_unavailable".into(),
            ));
        }
        Ok(proof)
    }
    pub fn kind(&self) -> FrameworkSessionToolKind {
        self.kind
    }
    pub fn schema(&self) -> ToolSchema {
        self.tool.to_schema()
    }
    pub fn classify(&self, args: &Value) -> ToolClass {
        self.tool.classify(args)
    }
    pub fn execution_contract(&self) -> Option<&'static str> {
        self.tool.execution_contract()
    }
    pub fn instance(&self) -> Arc<dyn Tool> {
        self.tool.clone()
    }
    pub fn is_current(&self) -> bool {
        self.permissions
            .tool_instance_for_exact(self.kind.execution_name())
            .is_some_and(|current| Arc::ptr_eq(&current, &self.tool))
    }
    pub fn rebind(&self, permissions: Arc<dyn ToolExecutor>) -> Result<Self, ToolError> {
        Self::from_host_instance(
            self.kind,
            self.tool.clone(),
            permissions,
            self.guard.clone(),
        )
    }
    pub async fn check_restrictions(
        &self,
        call: &ToolCall,
        args: &Value,
        ctx: &ToolCtx,
    ) -> Result<(), ToolError> {
        if !self.is_current() {
            return Err(ToolError::Execution(
                "framework_session_instance_changed".into(),
            ));
        }
        if ctx.plan_read_only && self.classify(args).mutability == super::ToolMutability::Mutating {
            return Err(ToolError::Execution(
                "framework_session_read_only: mutating Session operation is blocked".into(),
            ));
        }
        self.guard.check(self.kind, call, args, ctx).await?;
        if !self.is_current() {
            return Err(ToolError::Execution(
                "framework_session_instance_changed".into(),
            ));
        }
        Ok(())
    }
    pub async fn check_permissions(
        &self,
        call: &ToolCall,
        args: &Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.check_restrictions(call, args, &ctx.to_tool_ctx())
            .await?;
        self.permissions
            .check_permissions_for_resolved(call, self.kind.execution_name(), args, ctx)
            .await
    }
    /// The caller rechecks its live Actor immediately before this invocation,
    /// after the one native gate and restriction checks have awaited. No second
    /// permission evaluation or alias resolution happens here.
    pub async fn invoke_after_checks(
        &self,
        args: Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        if !self.is_current() {
            return Err(ToolError::Execution(
                "framework_session_instance_changed".into(),
            ));
        }
        self.tool.invoke(args, ctx).await
    }
}
