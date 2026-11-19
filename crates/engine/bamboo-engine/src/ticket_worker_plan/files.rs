//! File execution uses the native permission gate and Ticket authority.
//! Remote Workers use HostBridge; local tasks call the same Host endpoint in an
//! owned blocking task. Neither route invokes ambient filesystem tools.
use super::*;
use bamboo_agent_core::tools::{
    ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_subagent::executor::HostBridge;
use bamboo_tickets::{FileOperation, FileReply};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
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
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default = "default_read_page_bytes")]
    max_bytes: usize,
    #[serde(default)]
    expected_sha256: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WholeReadArgs {
    file_path: String,
}
#[derive(Clone, Copy)]
enum ReadMode {
    Whole,
    Paged,
}
fn default_read_page_bytes() -> usize {
    12000
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    file_path: String,
    content: String,
}

fn operation(request: &FileRequest, read_mode: ReadMode) -> Result<FileOperation> {
    match request.tool.as_str() {
        "Read" if request.expected_sha256.is_none() => match read_mode {
            ReadMode::Whole => {
                let args: WholeReadArgs = serde_json::from_value(request.arguments.clone())?;
                Ok(FileOperation::Read {
                    file_path: args.file_path,
                })
            }
            ReadMode::Paged => {
                let args: ReadArgs = serde_json::from_value(request.arguments.clone())?;
                Ok(FileOperation::ReadPage {
                    file_path: args.file_path,
                    offset_bytes: args.offset_bytes,
                    max_bytes: args.max_bytes,
                    expected_sha256: args.expected_sha256,
                })
            }
        },
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
    if caller.kind != SessionKind::Child {
        return Err(Error::ScopeDenied(
            "remote Work files require a Child".into(),
        ));
    }
    apply_file_request(
        service,
        caller,
        run_id,
        args,
        call_id,
        ceiling,
        ReadMode::Whole,
    )
}

fn apply_file_request(
    service: &TicketService,
    caller: &Session,
    run_id: &str,
    args: &Value,
    call_id: &str,
    ceiling: &[String],
    read_mode: ReadMode,
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
    if !plan.matches_dispatch_session(caller) || run_id != plan.run_id {
        return Err(Error::ScopeDenied("file callback Run changed".into()));
    }
    service.workspace_file(&plan.authority, call_id, &operation(&request, read_mode)?)
}

enum FileEndpoint {
    Remote(HostBridge),
    Local {
        service: Arc<TicketService>,
        child: Box<Session>,
        run_id: String,
        ceiling: Vec<String>,
    },
}

impl FileEndpoint {
    fn read_mode(&self) -> ReadMode {
        match self {
            Self::Local { .. } => ReadMode::Paged,
            Self::Remote(_) => ReadMode::Whole,
        }
    }

    fn local_resources(
        &self,
    ) -> std::result::Result<Option<bamboo_tools::LocalTaskResources>, ToolError> {
        let Self::Local { child, run_id, .. } = self else {
            return Ok(None);
        };
        let resources = bamboo_tools::LocalTaskResources::current().ok_or_else(|| {
            ToolError::Execution("local Ticket files require an owned execution scope".into())
        })?;
        let identity = resources.identity();
        if identity.session_id != child.id
            || identity.run_id != *run_id
            || identity.child_birth != child.created_at
        {
            return Err(ToolError::Execution(
                "local Ticket resource identity changed".into(),
            ));
        }
        Ok(Some(resources))
    }

    async fn execute(
        &self,
        arguments: Value,
        call_id: &str,
    ) -> std::result::Result<FileReply, ToolError> {
        match self {
            Self::Remote(host) => {
                let result = host
                    .subagent_call(arguments, call_id)
                    .await
                    .map_err(ToolError::Execution)?;
                serde_json::from_value(result)
                    .map_err(|error| ToolError::Execution(error.to_string()))
            }
            Self::Local {
                service,
                child,
                run_id,
                ceiling,
            } => {
                let resources = self.local_resources()?.expect("checked local endpoint");
                let service = service.clone();
                let child = child.as_ref().clone();
                let run_id = run_id.clone();
                let ceiling = ceiling.clone();
                let call_id = call_id.to_string();
                run_owned_file_request(resources, move || {
                    apply_file_request(
                        &service,
                        &child,
                        &run_id,
                        &arguments,
                        &call_id,
                        &ceiling,
                        ReadMode::Paged,
                    )
                })
                .await
            }
        }
    }
}

async fn run_owned_file_request(
    resources: bamboo_tools::LocalTaskResources,
    operation: impl FnOnce() -> Result<FileReply> + Send + 'static,
) -> std::result::Result<FileReply, ToolError> {
    // The permission gate has already run in the caller's phase scope. The
    // registry owns this task and its actual blocking JoinHandle until the
    // authority-backed operation returns, even if its awaiting caller drops.
    let receiver = resources
        .spawn_owned(async move {
            tokio::task::spawn_blocking(operation)
                .await
                .map_err(|error| ToolError::Execution(format!("Ticket file task failed: {error}")))?
                .map_err(|error| ToolError::Execution(error.to_string()))
        })
        .map_err(|error| ToolError::Execution(error.to_string()))?;
    receiver
        .await
        .map_err(|error| ToolError::Execution(error.to_string()))?
}

const FILE_RETRY_WINDOW: usize = 32;

struct RecentEntries<T>(VecDeque<(String, T)>);
impl<T> Default for RecentEntries<T> {
    fn default() -> Self {
        Self(VecDeque::new())
    }
}
impl<T> RecentEntries<T> {
    fn get(&self, key: &str) -> Option<&T> {
        self.0
            .iter()
            .find(|(id, _)| id == key)
            .map(|(_, value)| value)
    }
    fn insert(&mut self, key: String, value: T) {
        if let Some(index) = self.0.iter().position(|(id, _)| id == &key) {
            self.0.remove(index);
        }
        if self.0.len() == FILE_RETRY_WINDOW {
            self.0.pop_front();
        }
        self.0.push_back((key, value));
    }
}

#[derive(Default)]
struct FileCallState {
    retained: BTreeMap<String, FileRequest>,
    read_requests: RecentEntries<FileRequest>,
    hashes: RecentEntries<String>,
}
impl FileCallState {
    fn freeze(
        &mut self,
        id: &str,
        mut request: FileRequest,
        read_mode: ReadMode,
    ) -> std::result::Result<FileRequest, ToolError> {
        if let Some(original) = self.retained.get(id).or_else(|| self.read_requests.get(id)) {
            if original.tool != request.tool || original.arguments != request.arguments {
                return Err(ToolError::Execution("idempotency_conflict".into()));
            }
            request = original.clone();
        }
        if matches!(read_mode, ReadMode::Paged) && request.tool == "Read" {
            // Reads have no external effect and still recheck current authority
            // at the Host. Only their recent raw identities need retry tracking.
            self.read_requests.insert(id.into(), request.clone());
        } else if !self.retained.contains_key(id) {
            // Never evict immutable Write inputs or reinterpret their retries
            // after another Read changes the remembered source hash.
            if self.retained.len() >= FILE_RETRY_WINDOW {
                return Err(ToolError::Execution("file call budget exceeded".into()));
            }
            self.retained.insert(id.into(), request.clone());
        }
        Ok(request)
    }
}

pub struct RemoteFileExecutor {
    inner: Arc<dyn ToolExecutor>,
    endpoint: FileEndpoint,
    session_id: String,
    tools: BTreeSet<String>,
    state: Mutex<FileCallState>,
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
            endpoint: FileEndpoint::Remote(host),
            session_id,
            tools: tools.iter().cloned().collect(),
            state: Mutex::new(FileCallState::default()),
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
        self.endpoint.local_resources()?;
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
            self.state
                .lock()
                .expect("file retry state")
                .hashes
                .get(path)
                .cloned()
        } else {
            None
        };
        let request = FileRequest {
            tool: call.function.name.clone(),
            arguments: args.clone(),
            expected_sha256: hash.clone(),
        };
        let read_mode = self.endpoint.read_mode();
        operation(&request, read_mode).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;
        let request = self
            .state
            .lock()
            .expect("file retry state")
            .freeze(&call.id, request, read_mode)?;
        let reply = self
            .endpoint
            .execute(
                json!({(TICKET_FILE_ACTION): {
                    "tool": request.tool,
                    "arguments": request.arguments,
                    "expected_sha256": request.expected_sha256,
                }}),
                &call.id,
            )
            .await?;
        self.state
            .lock()
            .expect("file retry state")
            .hashes
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
                    if matches!(self.endpoint.read_mode(), ReadMode::Whole) {
                        schema.function.description="Read one complete small UTF-8 file inside this Assignment's write roots. Both the source and encoded reply must fit 16 KiB. Remote Actor Work paging and large-source reads are unsupported: only file_path is accepted; no offsets, partial preview or directory listing. No symlinks, hardlinks, .git or workspace .bamboo controls.".into();
                        schema.function.parameters = json!({"type":"object","additionalProperties":false,"required":["file_path"],"properties":{"file_path":{"type":"string"}}});
                        continue;
                    }
                    schema.function.description="Read a bounded UTF-8 source page inside this Assignment's write roots (source at most 1 MiB, encoded reply at most 16 KiB). First call needs only file_path; default max_bytes=12000. The reply's page gives exact byte offsets, source line numbers and EOF. For continuation pass page.next_offset_bytes as offset_bytes and the previous full-file sha256 as expected_sha256; changed source is rejected. A page is not the whole file unless page.eof=true and page.offset_bytes=0. No symlinks, hardlinks, .git, workspace .bamboo controls, traversal or directory listing.".into();
                    schema.function.parameters = json!({"type":"object","additionalProperties":false,"required":["file_path"],"properties":{
                        "file_path":{"type":"string"},
                        "offset_bytes":{"type":"integer","minimum":0,"maximum":bamboo_tickets::READ_SOURCE_BYTES_LIMIT,"description":"Use the previous page.next_offset_bytes; defaults to zero."},
                        "max_bytes":{"type":"integer","minimum":1,"maximum":bamboo_tickets::FILE_BYTES_LIMIT,"description":"Maximum raw UTF-8 bytes this page; defaults to 12000. JSON escaping can shorten the page."},
                        "expected_sha256":{"type":["string","null"],"pattern":"^[0-9a-f]{64}$","description":"Full-file sha256 from the prior reply, required for every nonzero offset."}
                    }});
                }
                "Write" => {
                    schema.function.description="Create one absent UTF-8 file, 1–16384 bytes, in an existing directory inside this Assignment's write roots, excluding .git and .bamboo control directories. Existing-file replacement is unsupported even after Read; use a new path. Installation cannot overwrite an externally created file. Unknown effects cannot be automatically retried.".into();
                    schema.function.parameters = json!({"type":"object","additionalProperties":false,"required":["file_path","content"],"properties":{"file_path":{"type":"string"},"content":{"type":"string","minLength":1,"maxLength":16384}}});
                }
                _ => {}
            }
        }
        schemas
    }
}

/// Same-process Ticket file executor. The immutable snapshot is supplied by
/// the Host's exact canonical Child activation, never by tool arguments.
/// Parsing, policy checks, schemas and idempotent raw-call identity are shared
/// with the remote route; only its authority transport differs.
pub struct LocalFileExecutor {
    executor: RemoteFileExecutor,
}

impl LocalFileExecutor {
    pub fn new(
        inner: Arc<dyn ToolExecutor>,
        service: Arc<TicketService>,
        canonical_child_snapshot: Session,
        run_id: String,
        ceiling: &[String],
    ) -> Result<Self> {
        bamboo_tickets::validate_native_tool_ceiling(ceiling.iter().map(String::as_str))?;
        let assignment = canonical_child_snapshot
            .metadata
            .get(TICKET_LOCAL_PLAN_KEY)
            .ok_or_else(|| Error::ScopeDenied("local Work file plan missing".into()))?;
        let plan = TicketWorkerPlan::from_receipt_identity(service.clone(), assignment)?;
        plan.require_session_identity(&canonical_child_snapshot)?;
        if plan.run_id != run_id {
            return Err(Error::ScopeDenied(
                "local file executor requires its exact Work Run".into(),
            ));
        }
        Ok(Self {
            executor: RemoteFileExecutor {
                session_id: canonical_child_snapshot.id.clone(),
                inner,
                endpoint: FileEndpoint::Local {
                    service,
                    child: Box::new(canonical_child_snapshot),
                    run_id,
                    ceiling: ceiling.to_vec(),
                },
                tools: ceiling.iter().cloned().collect(),
                state: Mutex::new(FileCallState::default()),
            },
        })
    }
}

#[async_trait::async_trait]
impl ToolExecutor for LocalFileExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        self.executor.execute(call).await
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        self.executor.execute_with_context_outcome(call, ctx).await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolResult, ToolError> {
        self.executor.execute_with_context(call, ctx).await
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
        self.executor.check_permissions_for(call, ctx).await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.executor.list_tools()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        self.executor.tools.contains(name) && self.executor.inner.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if !self.owns_exact_tool(name) {
            None
        } else if matches!(name, "Read" | "Write") {
            Some(self)
        } else {
            self.executor.inner.exact_tool_owner(name)
        }
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
            ["Task", "Read", "Write", "Bash"]
                .into_iter()
                .map(|name| {
                    serde_json::from_value(json!({
                        "type": "function",
                        "function": {"name": name, "description": "fixture", "parameters": {}}
                    }))
                    .unwrap()
                })
                .collect()
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

    fn local_file_fixture() -> (
        tempfile::TempDir,
        Arc<TicketService>,
        Session,
        std::path::PathBuf,
    ) {
        use bamboo_tickets::{
            Contract, ExecutionWorkspace, RuntimeReceipt, ScopeBinding, TicketKind,
        };
        let dir = tempfile::tempdir().unwrap();
        // Match the default Host-managed worktree location. Native permission,
        // resource ownership and scoped file I/O must work through this ancestor.
        let workspace = dir.path().join(".bamboo/worktrees/worker");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let path = workspace.to_string_lossy().into_owned();
        let binding = ScopeBinding {
            scope_id: "local-files".into(),
            supervisor_session_id: "root".into(),
            binding_revision: 1,
        };
        let service =
            Arc::new(TicketService::open(dir.path().join("authority"), binding.clone()).unwrap());
        let host = Authority::from_verified_host(
            binding.clone(),
            Principal::Supervisor {
                session_id: "root".into(),
            },
        );
        let command = service
            .prepare_command(
                &host,
                "start",
                vec![
                    Operation::Create {
                        temp_id: "w".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        depends_on: BTreeSet::new(),
                        contract: Contract {
                            title: "bounded local files".into(),
                            objective: "create one artifact".into(),
                            constraints: vec![],
                            acceptance: vec!["artifact".into()],
                            user_acceptance_required: true,
                            allowed_tools: BTreeSet::from([
                                "Task".into(),
                                "Read".into(),
                                "Write".into(),
                            ]),
                        },
                    },
                    Operation::Ready {
                        work_id: "w".into(),
                    },
                    Operation::Start {
                        work_id: "w".into(),
                        temp_id: "a".into(),
                        workspace: Some(ExecutionWorkspace {
                            repo: "fixture-repo".into(),
                            base_commit: "a".repeat(40),
                            branch: "worker".into(),
                            worktree: path.clone(),
                            write_roots: vec![path.clone()],
                            claims: BTreeSet::from([format!("worktree:{path}")]),
                        }),
                    },
                ],
            )
            .unwrap();
        let receipt = service.execute(&host, &command).unwrap();
        let assignment_id = receipt.ids["a"].clone();
        let (_, snapshot) = service.published().unwrap();
        let assignment = &snapshot.assignments[&assignment_id];
        let receipt = RuntimeReceipt {
            dispatch_key: assignment.dispatch_key.clone(),
            spec_hash: snapshot.intents[&assignment.dispatch_key].spec_hash.clone(),
            run_id: "run".into(),
            session_id: "child".into(),
        };
        let runtime = Authority::from_verified_host(binding, Principal::Runtime);
        let command = service
            .prepare_command(
                &runtime,
                "admit",
                vec![
                    Operation::Admitted {
                        assignment_id: assignment_id.clone(),
                        receipt,
                    },
                    Operation::Running {
                        assignment_id: assignment_id.clone(),
                    },
                ],
            )
            .unwrap();
        service.execute(&runtime, &command).unwrap();
        let mut child = Session::new_child("child", "root", "fixture-model", "fixture");
        child.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("run"));
        TicketWorkerPlan::from_runtime_receipt(service.clone(), &assignment_id)
            .unwrap()
            .bind_session(&mut child)
            .unwrap();
        (dir, service, child, workspace)
    }

    fn file_call(id: &str, tool: &str, args: Value) -> ToolCall {
        ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: tool.into(),
                arguments: args.to_string(),
            },
        }
    }

    #[cfg(unix)]
    async fn assert_complete_local_read(source_len: usize) {
        use sha2::Digest;
        let (_dir, service, child, workspace) = local_file_fixture();
        let source = vec![b'x'; source_len];
        let expected_sha256: String = sha2::Sha256::digest(&source)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let expected_pages = source_len.div_ceil(12000);
        let path = workspace.join("large-source.txt");
        std::fs::write(&path, &source).unwrap();
        let executor = LocalFileExecutor::new(
            Arc::new(Gate(std::sync::atomic::AtomicBool::new(false))),
            service,
            child.clone(),
            "run".into(),
            &["Task".into(), "Read".into()],
        )
        .unwrap();
        let resources = bamboo_tools::LocalTaskResources::new(&child.id, "run", child.created_at);
        let mut offset = 0;
        let mut sha256: Option<String> = None;
        let mut observed = Vec::new();
        let mut eof = false;
        for page_number in 1..=expected_pages {
            let call = file_call(
                &format!("large-source-page-{page_number}"),
                "Read",
                json!({"file_path":path,"offset_bytes":offset,"expected_sha256":sha256}),
            );
            let mut ctx = ToolExecutionContext::none(&call.id);
            ctx.session_id = Some(&child.id);
            let result = resources
                .scope(executor.execute_with_context_outcome(&call, ctx))
                .await
                .unwrap_or_else(|error| {
                    panic!("page {page_number} at byte {offset} must continue to EOF: {error}")
                });
            let reply: FileReply = serde_json::from_str(&result.into_tool_result().result).unwrap();
            assert_eq!(reply.sha256, expected_sha256);
            if let Some(prior) = &sha256 {
                assert_eq!(&reply.sha256, prior);
            }
            sha256 = Some(reply.sha256);
            let page = reply.page.unwrap();
            assert_eq!(page.offset_bytes, offset);
            observed.extend_from_slice(reply.content.unwrap().as_bytes());
            eof = page.eof;
            if eof {
                assert_eq!(page_number, expected_pages);
                break;
            }
            offset = page.next_offset_bytes.unwrap();
        }
        assert!(eof);
        assert_eq!(observed, source);
        {
            let state = executor.executor.state.lock().unwrap();
            assert_eq!(state.read_requests.0.len(), FILE_RETRY_WINDOW);
            assert_eq!(state.hashes.0.len(), 1);
            assert!(state.retained.is_empty());
        }
        resources.close_and_drain().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_read_continuations_reach_eof_beyond_the_retry_window() {
        assert_complete_local_read(600_000).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_maximum_source_reaches_eof_without_a_lifetime_call_quota() {
        assert_complete_local_read(bamboo_tickets::READ_SOURCE_BYTES_LIMIT).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_files_use_ticket_authority_and_preserve_raw_call_idempotency() {
        let (_dir, service, child, workspace) = local_file_fixture();
        let gate = Arc::new(Gate(std::sync::atomic::AtomicBool::new(true)));
        let executor = LocalFileExecutor::new(
            gate.clone(),
            service,
            child.clone(),
            "run".into(),
            &["Task".into(), "Read".into(), "Write".into()],
        )
        .unwrap();
        let resources = bamboo_tools::LocalTaskResources::new(&child.id, "run", child.created_at);
        let path = workspace.join("artifact.txt");
        let call = file_call(
            "write",
            "Write",
            json!({"file_path":path,"content":"artifact"}),
        );
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&child.id);
        assert!(resources
            .scope(executor.execute_with_context_outcome(&call, ctx))
            .await
            .unwrap_err()
            .to_string()
            .contains("policy denied"));
        assert!(!path.exists());
        gate.0.store(false, std::sync::atomic::Ordering::SeqCst);
        let first = resources
            .scope(executor.execute_with_context_outcome(&call, ctx))
            .await
            .unwrap();
        let first: FileReply = serde_json::from_str(&first.into_tool_result().result).unwrap();
        let second = resources
            .scope(executor.execute_with_context_outcome(&call, ctx))
            .await
            .unwrap();
        let second: FileReply = serde_json::from_str(&second.into_tool_result().result).unwrap();
        assert_eq!(first, second);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "artifact");
        for index in 0..96 {
            let source_path = workspace.join(format!("read-source-{index}.txt"));
            std::fs::write(&source_path, "source").unwrap();
            let read = file_call(
                &format!("rotating-read-{index}"),
                "Read",
                json!({"file_path":source_path}),
            );
            let mut read_ctx = ToolExecutionContext::none(&read.id);
            read_ctx.session_id = Some(&child.id);
            resources
                .scope(executor.execute_with_context_outcome(&read, read_ctx))
                .await
                .unwrap();
        }
        {
            let state = executor.executor.state.lock().unwrap();
            assert_eq!(
                state.retained.len(),
                1,
                "Read rotation cannot evict Write identity"
            );
            assert_eq!(state.read_requests.0.len(), FILE_RETRY_WINDOW);
            assert_eq!(state.hashes.0.len(), FILE_RETRY_WINDOW);
        }
        let recent_conflict = file_call("rotating-read-95", "Read", json!({"file_path":path}));
        let mut recent_ctx = ToolExecutionContext::none(&recent_conflict.id);
        recent_ctx.session_id = Some(&child.id);
        assert!(resources
            .scope(executor.execute_with_context_outcome(&recent_conflict, recent_ctx))
            .await
            .unwrap_err()
            .to_string()
            .contains("idempotency_conflict"));
        let changed = file_call(
            "write",
            "Write",
            json!({"file_path":path,"content":"changed"}),
        );
        assert!(resources
            .scope(executor.execute_with_context_outcome(&changed, ctx))
            .await
            .unwrap_err()
            .to_string()
            .contains("idempotency_conflict"));
        let read = file_call("read", "Read", json!({"file_path":path}));
        let mut read_ctx = ToolExecutionContext::none(&read.id);
        read_ctx.session_id = Some(&child.id);
        let result = resources
            .scope(executor.execute_with_context_outcome(&read, read_ctx))
            .await
            .unwrap();
        let reply: FileReply = serde_json::from_str(&result.into_tool_result().result).unwrap();
        assert_eq!(reply.content.as_deref(), Some("artifact"));
        assert_eq!(reply.sha256, first.sha256);
        let replay = resources
            .scope(executor.execute_with_context_outcome(&call, ctx))
            .await
            .unwrap();
        let replay: FileReply = serde_json::from_str(&replay.into_tool_result().result).unwrap();
        assert_eq!(
            replay, first,
            "Write retry retains its original hash despite many Reads and a fresh source hash"
        );
        gate.0.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(resources
            .scope(executor.execute_with_context_outcome(&read, read_ctx))
            .await
            .unwrap_err()
            .to_string()
            .contains("policy denied"));
        gate.0.store(false, std::sync::atomic::Ordering::SeqCst);
        let outside = workspace.parent().unwrap().join("outside.txt");
        let denied = file_call(
            "outside",
            "Write",
            json!({"file_path":outside,"content":"escape"}),
        );
        let mut denied_ctx = ToolExecutionContext::none(&denied.id);
        denied_ctx.session_id = Some(&child.id);
        assert!(resources
            .scope(executor.execute_with_context_outcome(&denied, denied_ctx))
            .await
            .is_err());
        assert!(!outside.exists());
        resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_files_require_exact_resource_birth_run_and_ceiling() {
        let (_dir, service, child, workspace) = local_file_fixture();
        let executor = LocalFileExecutor::new(
            Arc::new(Gate(std::sync::atomic::AtomicBool::new(false))),
            service,
            child.clone(),
            "run".into(),
            &["Task".into(), "Read".into()],
        )
        .unwrap();
        let call = file_call(
            "write",
            "Write",
            json!({"file_path":workspace.join("denied"),"content":"denied"}),
        );
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&child.id);
        let resources = bamboo_tools::LocalTaskResources::new(&child.id, "run", child.created_at);
        assert!(matches!(
            resources
                .scope(executor.execute_with_context_outcome(&call, ctx))
                .await,
            Err(ToolError::NotFound(_))
        ));
        let read = file_call(
            "read",
            "Read",
            json!({"file_path":workspace.join("denied")}),
        );
        let mut ctx = ToolExecutionContext::none(&read.id);
        ctx.session_id = Some(&child.id);
        assert!(executor
            .execute_with_context_outcome(&read, ctx)
            .await
            .unwrap_err()
            .to_string()
            .contains("owned execution scope"));
        for wrong in [
            bamboo_tools::LocalTaskResources::new(&child.id, "wrong-run", child.created_at),
            bamboo_tools::LocalTaskResources::new(
                &child.id,
                "run",
                child.created_at + chrono::Duration::seconds(1),
            ),
            bamboo_tools::LocalTaskResources::new("another-child", "run", child.created_at),
        ] {
            assert!(wrong
                .scope(executor.execute_with_context_outcome(&read, ctx))
                .await
                .unwrap_err()
                .to_string()
                .contains("resource identity changed"));
            wrong.close_and_drain().await.unwrap();
        }
        resources.close_and_drain().await.unwrap();
        assert!(!workspace.join("denied").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_read_pages_consume_large_source_with_full_hash_and_owned_scope() {
        let (_dir, service, child, workspace) = local_file_fixture();
        let source = "const sectionId = \"分组🌿\";\n".repeat(5000);
        let path = workspace.join("large-source.tsx");
        std::fs::write(&path, &source).unwrap();
        let before = service.published().unwrap().1.seq;
        let executor = LocalFileExecutor::new(
            Arc::new(Gate(std::sync::atomic::AtomicBool::new(false))),
            service.clone(),
            child.clone(),
            "run".into(),
            &["Task".into(), "Read".into()],
        )
        .unwrap();
        let resources = bamboo_tools::LocalTaskResources::new(&child.id, "run", child.created_at);
        let mut arguments = json!({"file_path":path});
        let mut content = String::new();
        let mut calls = 0;
        loop {
            let call = file_call(&format!("page-{calls}"), "Read", arguments);
            let mut ctx = ToolExecutionContext::none(&call.id);
            ctx.session_id = Some(&child.id);
            let result = resources
                .scope(executor.execute_with_context_outcome(&call, ctx))
                .await
                .unwrap()
                .into_tool_result();
            let reply: FileReply = serde_json::from_str(&result.result).unwrap();
            assert_eq!(
                reply.sha256,
                bamboo_tickets::content_hash(source.as_bytes())
            );
            let page = reply.page.unwrap();
            content.push_str(reply.content.as_deref().unwrap());
            calls += 1;
            if let Some(next) = page.next_offset_bytes {
                arguments =
                    json!({"file_path":path,"offset_bytes":next,"expected_sha256":reply.sha256});
            } else {
                assert!(page.eof);
                break;
            }
        }
        assert!(calls > 1);
        assert_eq!(content, source);
        assert_eq!(service.published().unwrap().1.seq, before);
        resources.close_and_drain().await.unwrap();
    }

    #[test]
    fn read_page_request_codec_keeps_strict_types_and_legacy_file_path_only_input() {
        let request = |arguments| FileRequest {
            tool: "Read".into(),
            arguments,
            expected_sha256: None,
        };
        assert!(matches!(
            operation(&request(json!({"file_path":"/source"})), ReadMode::Paged).unwrap(),
            FileOperation::ReadPage {
                offset_bytes: 0,
                max_bytes: 12000,
                expected_sha256: None,
                ..
            }
        ));
        for args in [
            json!({"file_path":"/source","offset_bytes":-1}),
            json!({"file_path":"/source","max_bytes":1.5}),
            json!({"file_path":"/source","offset":1}),
            json!({"file_path":"/source","expected_sha256":true}),
        ] {
            assert!(operation(&request(args), ReadMode::Paged).is_err());
        }
        assert!(matches!(
            operation(&request(json!({"file_path":"/source"})), ReadMode::Whole).unwrap(),
            FileOperation::Read { .. }
        ));
        for args in [
            json!({"file_path":"/source","offset_bytes":0}),
            json!({"file_path":"/source","max_bytes":12000}),
            json!({"file_path":"/source","expected_sha256":null}),
            json!({"file_path":"/source","local":true}),
            json!({"file_path":"/source","paged":true}),
        ] {
            assert!(operation(&request(args), ReadMode::Whole).is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn remote_read_rejects_paging_before_bridge_and_at_the_host() {
        let (_dir, service, child, workspace) = local_file_fixture();
        let tools = ["Task".into(), "Read".into()];
        let (bridge, mut requests) = HostBridge::channel();
        let remote = RemoteFileExecutor::new(
            Arc::new(Gate(std::sync::atomic::AtomicBool::new(false))),
            bridge,
            child.id.clone(),
            &tools,
        )
        .unwrap();
        let path = workspace.join("small.txt");
        std::fs::write(&path, "complete source").unwrap();
        let request = |arguments| json!({(TICKET_FILE_ACTION):{"tool":"Read","arguments":arguments,"expected_sha256":null}});
        let before = service.published().unwrap().1.seq;
        let reply = apply_host_file_request(
            &service,
            &child,
            "run",
            &request(json!({"file_path":path})),
            "whole",
            &tools,
        )
        .unwrap();
        assert_eq!(reply.content.as_deref(), Some("complete source"));
        assert!(reply.page.is_none());
        for (index, fields) in [
            json!({"offset_bytes":0}),
            json!({"max_bytes":12000}),
            json!({"expected_sha256":null}),
            json!({"local":true}),
            json!({"paged":true}),
            json!({"read_only":true}),
        ]
        .into_iter()
        .enumerate()
        {
            let mut arguments = fields;
            arguments["file_path"] = json!(path);
            let id = format!("unsupported-page-{index}");
            let call = file_call(&id, "Read", arguments.clone());
            let mut ctx = ToolExecutionContext::none(&call.id);
            ctx.session_id = Some(&child.id);
            assert!(remote
                .execute_with_context_outcome(&call, ctx)
                .await
                .is_err());
            assert!(
                requests.try_recv().is_err(),
                "unsupported paging must not consume a Host callback"
            );
            assert!(apply_host_file_request(
                &service,
                &child,
                "run",
                &request(arguments),
                &id,
                &tools
            )
            .is_err());
        }
        let large = workspace.join("large.txt");
        std::fs::write(&large, vec![b'x'; 600_000]).unwrap();
        assert!(
            apply_host_file_request(
                &service,
                &child,
                "run",
                &request(json!({"file_path":large})),
                "oversized-whole",
                &tools
            )
            .is_err(),
            "remote must reject an unsupported whole source without a partial result"
        );
        assert_eq!(service.published().unwrap().1.seq, before);
    }

    #[tokio::test]
    async fn dropped_local_file_awaiter_cannot_detach_blocking_mutation() {
        let resources = bamboo_tools::LocalTaskResources::new("child", "run", chrono::Utc::now());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completion = completed.clone();
        let owned = resources.clone();
        let waiter = tokio::spawn(async move {
            run_owned_file_request(owned, move || {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                completion.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(FileReply {
                    content: None,
                    sha256: "a".repeat(64),
                    artifact: None,
                    page: None,
                })
            })
            .await
        });
        started_rx.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        let draining = resources.clone();
        let drain = tokio::spawn(async move { draining.close_and_drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        assert!(!completed.load(std::sync::atomic::Ordering::SeqCst));
        finish_tx.send(()).unwrap();
        drain.await.unwrap().unwrap();
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn local_paging_and_remote_whole_reads_keep_the_same_tool_ceiling() {
        let (_dir, service, child, _workspace) = local_file_fixture();
        let tools = ["Task".into(), "Read".into(), "Write".into()];
        let inner: Arc<dyn ToolExecutor> =
            Arc::new(Gate(std::sync::atomic::AtomicBool::new(false)));
        let local =
            LocalFileExecutor::new(inner.clone(), service, child.clone(), "run".into(), &tools)
                .unwrap();
        let (bridge, _requests) = HostBridge::channel();
        let remote = RemoteFileExecutor::new(inner, bridge, child.id, &tools).unwrap();
        for name in ["Write", "Task"] {
            let find = |schemas: Vec<ToolSchema>| {
                schemas
                    .into_iter()
                    .find(|s| s.function.name == name)
                    .unwrap()
            };
            assert_eq!(
                serde_json::to_value(find(local.list_tools())).unwrap(),
                serde_json::to_value(find(remote.list_tools())).unwrap()
            );
        }
        assert_eq!(local.list_tools().len(), 3);
        assert_eq!(remote.list_tools().len(), 3);
        assert!(!local.owns_exact_tool("Bash"));
        assert!(!remote.owns_exact_tool("Bash"));
        assert!(local.exact_tool_owner("Read").is_some());
        assert!(local.exact_tool_owner("Write").is_some());
        let read = local
            .list_tools()
            .into_iter()
            .find(|schema| schema.function.name == "Read")
            .unwrap();
        assert_eq!(read.function.parameters["additionalProperties"], false);
        assert_eq!(read.function.parameters["required"], json!(["file_path"]));
        assert!(read.function.parameters["properties"]
            .get("offset_bytes")
            .is_some());
        let read = remote
            .list_tools()
            .into_iter()
            .find(|s| s.function.name == "Read")
            .unwrap();
        assert_eq!(
            read.function.parameters["properties"],
            json!({"file_path":{"type":"string"}})
        );
        assert_eq!(read.function.parameters["additionalProperties"], false);
    }
}
