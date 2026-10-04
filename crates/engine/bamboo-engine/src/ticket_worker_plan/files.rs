//! File execution uses the existing native permission gate and HostBridge.
//! The Worker never invokes an ambient filesystem tool for a Ticket Run.
use super::*;
use bamboo_agent_core::tools::{
    ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_subagent::executor::HostBridge;
use bamboo_tickets::{FileOperation, FileReply};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

pub const TICKET_FILE_ACTION: &str = "_ticket_workspace_file_v1";

/// Read-only preflight for an explicitly prepared Git worktree. This never
/// creates branches or workspaces, and runs outside the Ticket transaction lock.
pub async fn verify_workspace(workspace: bamboo_tickets::ExecutionWorkspace) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        use std::{path::Path, process::Command};
        fn git(path: &str, args: &[&str]) -> Result<String> {
            let output = Command::new("git")
                .env("GIT_OPTIONAL_LOCKS", "0")
                .args(["-C", path])
                .args(args)
                .output()?;
            if !output.status.success() {
                return Err(Error::ScopeDenied(
                    "Git workspace identity cannot be verified".into(),
                ));
            }
            String::from_utf8(output.stdout)
                .map(|s| s.trim().to_owned())
                .map_err(|_| Error::ScopeDenied("Git workspace identity is not UTF-8".into()))
        }
        let root = Path::new(&workspace.worktree).canonicalize()?;
        let marker = std::fs::symlink_metadata(root.join(".git"))?;
        if !marker.is_file()
            || marker.file_type().is_symlink()
            || Path::new(&git(
                &workspace.worktree,
                &["rev-parse", "--show-toplevel"],
            )?)
            .canonicalize()?
                != root
            || git(&workspace.worktree, &["rev-parse", "HEAD"])? != workspace.base_commit
            || git(&workspace.worktree, &["symbolic-ref", "--short", "HEAD"])? != workspace.branch
            || Path::new(&git(
                &workspace.worktree,
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
            )?)
            .canonicalize()?
                != Path::new(&git(
                    &workspace.repo,
                    &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                )?)
                .canonicalize()?
        {
            return Err(Error::ScopeDenied(
                "Assignment must use its exact isolated repo/base/branch/worktree".into(),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::AuthorityUnavailable("workspace preflight stopped".into()))?
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRequest {
    tool: String,
    arguments: Value,
    expected_sha256: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    file_path: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    file_path: String,
    content: String,
}

fn operation(request: &FileRequest) -> Result<FileOperation> {
    match request.tool.as_str() {
        "Read" if request.expected_sha256.is_none() => {
            let args: ReadArgs = serde_json::from_value(request.arguments.clone())?;
            Ok(FileOperation::Read {
                file_path: args.file_path,
            })
        }
        "Write" => {
            let args: WriteArgs = serde_json::from_value(request.arguments.clone())?;
            Ok(FileOperation::Write {
                file_path: args.file_path,
                content: args.content,
                expected_sha256: request.expected_sha256.clone(),
            })
        }
        _ => Err(Error::ScopeDenied(
            "unsupported Ticket file operation".into(),
        )),
    }
}

pub fn apply_host_file_request(
    service: &TicketService,
    caller: &Session,
    run_id: &str,
    args: &Value,
    call_id: &str,
    ceiling: &[String],
) -> Result<FileReply> {
    let body = args
        .as_object()
        .filter(|o| o.len() == 1)
        .and_then(|o| o.get(TICKET_FILE_ACTION))
        .ok_or_else(|| Error::ScopeDenied("invalid file callback".into()))?;
    let request: FileRequest = serde_json::from_value(body.clone())?;
    if !ceiling.contains(&request.tool) {
        return Err(Error::ScopeDenied(
            "file tool outside immutable native ceiling".into(),
        ));
    }
    let assignment = caller
        .metadata
        .get(TICKET_LOCAL_PLAN_KEY)
        .ok_or_else(|| Error::ScopeDenied("Child has no Ticket capability".into()))?;
    let plan = TicketWorkerPlan::from_receipt_identity(Arc::new(service.clone()), assignment)?;
    if caller.kind != SessionKind::Child
        || caller.id != plan.session_id
        || run_id != plan.run_id
        || caller.parent_session_id.as_deref() != Some(plan.supervisor_session_id.as_str())
    {
        return Err(Error::ScopeDenied("file callback Run changed".into()));
    }
    service.workspace_file(&plan.authority, call_id, &operation(&request)?)
}

pub struct RemoteFileExecutor {
    inner: Arc<dyn ToolExecutor>,
    host: HostBridge,
    session_id: String,
    tools: BTreeSet<String>,
    reads: Mutex<BTreeMap<String, String>>,
    calls: Mutex<BTreeMap<String, FileRequest>>,
}
impl RemoteFileExecutor {
    pub fn new(
        inner: Arc<dyn ToolExecutor>,
        host: HostBridge,
        session_id: String,
        tools: &[String],
    ) -> Result<Self> {
        bamboo_tickets::validate_native_tool_ceiling(tools.iter().map(String::as_str))?;
        Ok(Self {
            inner,
            host,
            session_id,
            tools: tools.iter().cloned().collect(),
            reads: Mutex::new(BTreeMap::new()),
            calls: Mutex::new(BTreeMap::new()),
        })
    }
    fn permitted(&self, call: &ToolCall) -> std::result::Result<(), ToolError> {
        if self.tools.contains(&call.function.name) {
            Ok(())
        } else {
            Err(ToolError::NotFound(call.function.name.clone()))
        }
    }
}
#[async_trait::async_trait]
impl ToolExecutor for RemoteFileExecutor {
    async fn execute(&self, _: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "Ticket file execution needs a bound Run context".into(),
        ))
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        self.permitted(call)?;
        if ctx.session_id != Some(self.session_id.as_str()) || ctx.tool_call_id != call.id {
            return Err(ToolError::Execution("Ticket file context changed".into()));
        }
        if call.function.name == "Task" {
            return self.inner.execute_with_context_outcome(call, ctx).await;
        }
        let args: Value = serde_json::from_str(&call.function.arguments)
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;
        if let Some(outcome) = self
            .inner
            .check_permissions_for_resolved(call, &call.function.name, &args, &ctx)
            .await?
        {
            return Ok(outcome);
        }
        let path = args
            .get("file_path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidArguments("file_path missing".into()))?;
        let hash = if call.function.name == "Write" {
            self.reads.lock().expect("read tracker").get(path).cloned()
        } else {
            None
        };
        let mut request = FileRequest {
            tool: call.function.name.clone(),
            arguments: args.clone(),
            expected_sha256: hash.clone(),
        };
        operation(&request).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;
        {
            let mut calls = self.calls.lock().expect("file call identities");
            if let Some(original) = calls.get(&call.id) {
                if original.tool != request.tool || original.arguments != request.arguments {
                    return Err(ToolError::Execution("idempotency_conflict".into()));
                }
                request = original.clone();
            } else {
                if calls.len() >= 32 {
                    return Err(ToolError::Execution("file call budget exceeded".into()));
                }
                calls.insert(call.id.clone(), request.clone());
            }
        }
        let result=self.host.subagent_call(json!({(TICKET_FILE_ACTION):{"tool":request.tool,"arguments":request.arguments,"expected_sha256":request.expected_sha256}}),&call.id).await.map_err(ToolError::Execution)?;
        let reply: FileReply =
            serde_json::from_value(result).map_err(|e| ToolError::Execution(e.to_string()))?;
        self.reads
            .lock()
            .expect("read tracker")
            .insert(path.into(), reply.sha256.clone());
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: serde_json::to_string(&reply)
                .map_err(|e| ToolError::Execution(e.to_string()))?,
            display_preference: None,
            images: Vec::new(),
        }))
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ctx)
            .await
            .map(ToolOutcome::into_tool_result)
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
        self.permitted(call)?;
        self.inner.check_permissions_for(call, ctx).await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut schemas = self.inner.list_tools();
        schemas.retain(|s| self.tools.contains(&s.function.name));
        for schema in &mut schemas {
            match schema.function.name.as_str() {
                "Read" => {
                    schema.function.description="Read one complete UTF-8 file inside this Assignment's write roots. Both raw content and the encoded result must fit 16 KiB. No symlinks, hardlinks, .git, .bamboo control directories, traversal or directory listing.".into();
                    schema.function.parameters = json!({"type":"object","additionalProperties":false,"required":["file_path"],"properties":{"file_path":{"type":"string"}}});
                }
                "Write" => {
                    schema.function.description="Atomically replace one UTF-8 file, 1–16384 bytes, inside this Assignment's write roots, excluding .git and .bamboo control directories. Read an existing file first; a changed file conflicts. New files may be created in existing directories. Unknown effects cannot be automatically retried.".into();
                    schema.function.parameters = json!({"type":"object","additionalProperties":false,"required":["file_path","content"],"properties":{"file_path":{"type":"string"},"content":{"type":"string","minLength":1,"maxLength":16384}}});
                }
                _ => {}
            }
        }
        schemas
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;
    struct Gate(std::sync::atomic::AtomicBool);
    #[async_trait::async_trait]
    impl ToolExecutor for Gate {
        async fn execute(&self, _: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            panic!("ambient file execution must never occur")
        }
        async fn check_permissions_for(
            &self,
            _: &ToolCall,
            _: &ToolExecutionContext<'_>,
        ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
            if self.0.load(std::sync::atomic::Ordering::SeqCst) {
                Err(ToolError::Execution("policy denied".into()))
            } else {
                Ok(None)
            }
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }
    #[tokio::test]
    async fn permission_gate_and_frozen_raw_call_precede_host_file_execution() {
        let (bridge, mut requests) = HostBridge::channel();
        let gate = Arc::new(Gate(std::sync::atomic::AtomicBool::new(true)));
        let executor = RemoteFileExecutor::new(
            gate.clone(),
            bridge,
            "child".into(),
            &["Task".into(), "Read".into(), "Write".into()],
        )
        .unwrap();
        let call = ToolCall {
            id: "write".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Write".into(),
                arguments: json!({"file_path":"/fixture/code","content":"new"}).to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some("child");
        assert!(executor
            .execute_with_context_outcome(&call, ctx)
            .await
            .is_err());
        assert!(requests.try_recv().is_err());
        let mut shell = call.clone();
        shell.function.name = "Bash".into();
        assert!(executor
            .execute_with_context_outcome(&shell, ctx)
            .await
            .is_err());
        assert!(requests.try_recv().is_err());
        gate.0.store(false, std::sync::atomic::Ordering::SeqCst);
        let pump = tokio::spawn(async move {
            let mut original = None;
            for _ in 0..2 {
                let request = requests.recv().await.unwrap();
                let body = request.body["args"][TICKET_FILE_ACTION].clone();
                if let Some(prior) = &original {
                    assert_eq!(
                        &body, prior,
                        "same raw Write must retain its original hash after success"
                    );
                } else {
                    assert!(body["expected_sha256"].is_null());
                    original = Some(body);
                }
                request
                    .reply
                    .send(
                        json!({"result":{"content":null,"sha256":"a".repeat(64),"artifact":null}}),
                    )
                    .unwrap();
            }
        });
        assert!(
            executor
                .execute_with_context_outcome(&call, ctx)
                .await
                .unwrap()
                .into_tool_result()
                .success
        );
        assert!(
            executor
                .execute_with_context_outcome(&call, ctx)
                .await
                .unwrap()
                .into_tool_result()
                .success
        );
        let mut changed = call.clone();
        changed.function.arguments =
            json!({"file_path":"/fixture/code","content":"different"}).to_string();
        assert!(executor
            .execute_with_context_outcome(&changed, ctx)
            .await
            .is_err());
        pump.await.unwrap();
    }
}
