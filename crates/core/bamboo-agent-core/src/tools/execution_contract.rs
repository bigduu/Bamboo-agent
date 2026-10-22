//! Call-bound retained semantics. These observations grant no tool permission.

use super::{ToolCall, ToolError, ToolExecutor};

pub const FRAMEWORK_ROOT_SPAWN_CONTRACT_V1: &str = "bamboo.spawn_root.v1";
const MAX_CONTRACT_ID_BYTES: usize = 128;

pub fn valid_execution_contract(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTRACT_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetainedToolExecution {
    tool_call: ToolCall,
    execution_name: String,
    contract_known: bool,
    execution_contract: Option<String>,
}

impl RetainedToolExecution {
    pub fn new(
        tool_call: &ToolCall,
        execution_name: &str,
        contract_known: bool,
        execution_contract: Option<&str>,
    ) -> Result<Self, ToolError> {
        if execution_name.trim().is_empty()
            || execution_contract.is_some_and(|id| !valid_execution_contract(id))
            || (!contract_known && execution_contract.is_some())
        {
            return Err(ToolError::Execution(
                "retained tool execution provenance is malformed".into(),
            ));
        }
        Ok(Self {
            tool_call: tool_call.clone(),
            execution_name: execution_name.into(),
            contract_known,
            execution_contract: execution_contract.map(str::to_owned),
        })
    }

    /// Capture before any awaited permission gate, never after a tool returns.
    /// A catalog name without a concrete exact owner remains unknown.
    pub fn capture(
        tool_call: &ToolCall,
        execution_name: &str,
        executor: &dyn ToolExecutor,
    ) -> Result<Self, ToolError> {
        let owner = executor.exact_tool_owner(execution_name);
        Self::new(
            tool_call,
            execution_name,
            owner.is_some(),
            owner.and_then(|owner| owner.local_execution_contract(execution_name)),
        )
    }

    pub fn contract_known(&self) -> bool {
        self.contract_known
    }

    pub fn execution_contract(&self) -> Option<&str> {
        self.execution_contract.as_deref()
    }

    pub fn execution_name(&self) -> &str {
        &self.execution_name
    }

    /// Apply the historical exact selector while checking an installed Host
    /// instance. A current same-name Root must not attest a retained custom tool.
    pub fn framework_instance_is_available(&self, executor: &dyn ToolExecutor) -> bool {
        RETAINED_TOOL_OWNER_LOOKUP.sync_scope(self.clone(), || {
            executor
                .framework_session_tool_for_exact(&self.execution_name)
                .is_some_and(|proof| proof.is_current())
        })
    }

    /// Use the same retained selector for existence and semantic provenance.
    /// The owner pointer is diagnostic only; execution stays in the full chain.
    pub fn owner_is_available(&self, executor: &dyn ToolExecutor) -> bool {
        RETAINED_TOOL_OWNER_LOOKUP.sync_scope(self.clone(), || {
            executor.owns_exact_tool(&self.execution_name)
                && (!self.contract_known
                    || (executor.exact_tool_owner(&self.execution_name).is_some()
                        && executor.execution_contract_for_exact(&self.execution_name)
                            == self.execution_contract.as_deref()))
        })
    }
}

tokio::task_local! {
    static RETAINED_TOOL_EXECUTION: Option<RetainedToolExecution>;
    // Lookup is synchronous and never leaks into a nested asynchronous call.
    static RETAINED_TOOL_OWNER_LOOKUP: RetainedToolExecution;
}

pub async fn with_retained_tool_execution<F: std::future::Future>(
    execution: Option<RetainedToolExecution>,
    dispatch: F,
) -> F::Output {
    RETAINED_TOOL_EXECUTION.scope(execution, dispatch).await
}

/// Only the exact retained call may use its semantic selector. A changed same-ID
/// call fails closed; a new nested call gets no inherited execution contract.
pub fn retained_tool_execution_for(
    call: &ToolCall,
    execution_name: &str,
) -> Result<Option<RetainedToolExecution>, ToolError> {
    let retained = retained_tool_execution_for_call(call)?;
    if retained
        .as_ref()
        .is_some_and(|retained| retained.execution_name != execution_name)
    {
        return Err(ToolError::Execution(
            "retained tool call or exact owner changed before dispatch".into(),
        ));
    }
    Ok(retained)
}

/// Resolve the already-bound complete call before looking at its historical
/// owner. A new nested call inherits no selector; changed same-ID calls reject.
pub fn retained_tool_execution_for_call(
    call: &ToolCall,
) -> Result<Option<RetainedToolExecution>, ToolError> {
    let retained = RETAINED_TOOL_EXECUTION
        .try_with(Clone::clone)
        .ok()
        .flatten();
    match retained {
        Some(retained) if retained.tool_call.id == call.id => {
            if retained.tool_call != *call {
                return Err(ToolError::Execution(
                    "retained tool call or exact owner changed before dispatch".into(),
                ));
            }
            Ok(Some(retained))
        }
        _ => Ok(None),
    }
}

/// Used only during the synchronous owner query above, not from an ambient
/// replay invocation. Fresh nested calls therefore inspect their own catalog.
pub fn retained_tool_owner_lookup(name: &str) -> Option<RetainedToolExecution> {
    RETAINED_TOOL_OWNER_LOOKUP
        .try_with(Clone::clone)
        .ok()
        .filter(|retained| retained.execution_name == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{FunctionCall, ToolResult, ToolSchema};

    struct Owner;
    #[async_trait::async_trait]
    impl ToolExecutor for Owner {
        async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("metadata inspection must not execute")
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
        fn owns_exact_tool(&self, name: &str) -> bool {
            name == "spawn_session"
        }
        fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
            self.owns_exact_tool(name)
                .then_some(self as &dyn ToolExecutor)
        }
        fn local_execution_contract(&self, _: &str) -> Option<&'static str> {
            Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
        }
    }
    struct Opaque;
    #[async_trait::async_trait]
    impl ToolExecutor for Opaque {
        async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("metadata inspection must not execute")
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
        fn owns_exact_tool(&self, name: &str) -> bool {
            name == "spawn_session"
        }
    }
    fn call() -> ToolCall {
        ToolCall {
            id: "parked".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "default::spawnSession".into(),
                arguments: "{ \"instruction\": \"same bytes\" }".into(),
            },
        }
    }

    #[test]
    fn self_owner_local_hook_does_not_recurse_or_trust_opaque_catalog_names() {
        let execution = RetainedToolExecution::capture(&call(), "spawn_session", &Owner).unwrap();
        assert!(execution.contract_known());
        assert_eq!(
            execution.execution_contract(),
            Some(FRAMEWORK_ROOT_SPAWN_CONTRACT_V1)
        );
        assert!(execution.owner_is_available(&Owner));
        assert!(!execution.owner_is_available(&Opaque));
        let unknown = RetainedToolExecution::capture(&call(), "spawn_session", &Opaque).unwrap();
        assert!(!unknown.contract_known());
        assert_eq!(unknown.execution_contract(), None);
        assert!(retained_tool_owner_lookup("spawn_session").is_none());
    }

    #[tokio::test]
    async fn retained_selection_is_call_bound_and_does_not_leak_to_nested_lookup_or_spawn() {
        let call = call();
        let execution = RetainedToolExecution::capture(&call, "spawn_session", &Owner).unwrap();
        with_retained_tool_execution(Some(execution.clone()), async {
            assert_eq!(
                retained_tool_execution_for(&call, "spawn_session").unwrap(),
                Some(execution)
            );
            assert!(retained_tool_owner_lookup("spawn_session").is_none());
            let mut nested = call.clone();
            nested.id = "new-call".into();
            assert!(retained_tool_execution_for(&nested, "spawn_session")
                .unwrap()
                .is_none());
            let mut changed = call.clone();
            changed.function.arguments = "{}".into();
            assert!(retained_tool_execution_for(&changed, "spawn_session").is_err());
            assert!(retained_tool_execution_for(&call, "SubAgent").is_err());
            let child_call = call.clone();
            assert!(tokio::spawn(async move {
                retained_tool_execution_for(&child_call, "spawn_session")
                    .unwrap()
                    .is_none()
            })
            .await
            .unwrap());
        })
        .await;
        assert!(retained_tool_execution_for(&call, "spawn_session")
            .unwrap()
            .is_none());
    }
}
