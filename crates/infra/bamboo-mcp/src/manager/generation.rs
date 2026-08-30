use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bamboo_agent_core::{FunctionSchema, ToolSchema};

use super::ServerRuntime;
use crate::error::{McpError, Result, ToolRegistrationError};
use crate::protocol::McpProtocolClient;
use crate::tool_index::{IndexState, ResolvedIndexAlias, ServerToolCatalog};
use crate::types::{McpTool, RuntimeInfo, ToolAlias};

/// Monotonic identity of one structural server publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationId(u64);

impl PublicationId {
    pub fn get(self) -> u64 {
        self.0
    }

    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Monotonic identity of one transport connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeId(u64);

impl RuntimeId {
    pub fn get(self) -> u64 {
        self.0
    }

    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FenceState {
    Open,
    Retiring,
    Closed,
}

#[derive(Debug)]
struct PublicationFence {
    state: FenceState,
    active_calls: usize,
}

struct RuntimeFence {
    state: FenceState,
    active_calls: usize,
    client: Option<Arc<McpProtocolClient>>,
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One exact transport connection. Passive snapshots may retain this allocation,
/// but retirement removes its client and aborts owned tasks synchronously, so a
/// snapshot never keeps transport resources alive.
pub(super) struct TransportRuntime {
    pub(super) runtime_id: RuntimeId,
    pub(super) runtime: ServerRuntime,
    fence: Mutex<RuntimeFence>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl fmt::Debug for TransportRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportRuntime")
            .field("runtime_id", &self.runtime_id)
            .field("server_id", &self.runtime.config.id)
            .field("state", &mutex_lock(&self.fence).state)
            .finish_non_exhaustive()
    }
}

impl TransportRuntime {
    pub(super) fn new(
        runtime_id: RuntimeId,
        runtime: ServerRuntime,
        client: McpProtocolClient,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime_id,
            runtime,
            fence: Mutex::new(RuntimeFence {
                state: FenceState::Open,
                active_calls: 0,
                client: Some(Arc::new(client)),
            }),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn client_if_open(&self) -> Result<Arc<McpProtocolClient>> {
        let fence = mutex_lock(&self.fence);
        if fence.state != FenceState::Open {
            return Err(McpError::StalePublication {
                publication_id: 0,
                runtime_id: self.runtime_id.get(),
            });
        }
        fence.client.clone().ok_or(McpError::StalePublication {
            publication_id: 0,
            runtime_id: self.runtime_id.get(),
        })
    }

    pub(super) fn install_tasks(
        &self,
        handles: impl IntoIterator<Item = tokio::task::JoinHandle<()>>,
    ) {
        let mut tasks = mutex_lock(&self.tasks);
        debug_assert!(tasks.is_empty(), "one task set is owned by each runtime");
        tasks.extend(handles);
    }

    /// Close transport admission, synchronously detach transport ownership, and
    /// abort every generation-specific background task. Active execution leases
    /// retain their exact client until they finish.
    pub(super) fn retire(&self) {
        let detached_client = {
            let mut fence = mutex_lock(&self.fence);
            if fence.state == FenceState::Closed {
                return;
            }
            fence.state = FenceState::Retiring;
            let client = fence.client.take();
            if fence.active_calls == 0 {
                fence.state = FenceState::Closed;
            }
            client
        };
        for handle in mutex_lock(&self.tasks).drain(..) {
            handle.abort();
        }
        drop(detached_client);
    }

    #[cfg(test)]
    pub(super) fn fence_state(&self) -> FenceState {
        mutex_lock(&self.fence).state
    }

    #[cfg(test)]
    pub(super) fn active_calls(&self) -> usize {
        mutex_lock(&self.fence).active_calls
    }
}

/// Immutable catalog publication for one server.
pub(crate) struct ServerPublication {
    pub(super) publication_id: PublicationId,
    pub(super) server_id: String,
    pub(super) runtime: Arc<TransportRuntime>,
    pub(super) catalog: ServerToolCatalog,
    tools: BTreeMap<String, McpTool>,
    schemas: Vec<ToolSchema>,
    fence: Mutex<PublicationFence>,
}

impl fmt::Debug for ServerPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerPublication")
            .field("publication_id", &self.publication_id)
            .field("runtime_id", &self.runtime.runtime_id)
            .field("server_id", &self.server_id)
            .field("tool_count", &self.tools.len())
            .field("state", &mutex_lock(&self.fence).state)
            .finish()
    }
}

impl ServerPublication {
    pub(super) fn new(
        publication_id: PublicationId,
        runtime: Arc<TransportRuntime>,
        catalog: ServerToolCatalog,
        discovered_tools: &[McpTool],
    ) -> Result<Arc<Self>> {
        let mut discovered = BTreeMap::new();
        for tool in discovered_tools {
            discovered.insert(tool.name.clone(), tool.clone());
        }

        let mut tools = BTreeMap::new();
        let mut schemas = Vec::new();
        for alias in catalog.aliases() {
            let tool = discovered
                .get(&alias.original_name)
                .ok_or(ToolRegistrationError::ProviderSchemaUnavailable)?;
            tools.insert(alias.original_name.clone(), tool.clone());
            schemas.push(ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: alias.alias,
                    description: tool.description.clone(),
                    parameters: tool.parameters.clone(),
                },
            });
        }

        Ok(Arc::new(Self {
            publication_id,
            server_id: catalog.server_id().to_string(),
            runtime,
            catalog,
            tools,
            schemas,
            fence: Mutex::new(PublicationFence {
                state: FenceState::Open,
                active_calls: 0,
            }),
        }))
    }

    pub(super) fn tool(&self, original_name: &str) -> Option<McpTool> {
        self.tools.get(original_name).cloned()
    }

    pub(super) fn tool_count(&self) -> usize {
        self.tools.len()
    }

    pub(super) fn schemas(&self) -> &[ToolSchema] {
        &self.schemas
    }

    pub(super) fn close_admission(&self) {
        let mut fence = mutex_lock(&self.fence);
        if fence.state == FenceState::Open {
            fence.state = FenceState::Retiring;
            if fence.active_calls == 0 {
                fence.state = FenceState::Closed;
            }
        }
    }

    pub(super) fn retire_with_runtime(&self) {
        // Admission always locks publication before runtime. Preserve that order
        // while closing both fences so check+increment cannot race retirement.
        let detached_client = {
            let mut publication = mutex_lock(&self.fence);
            let mut runtime = mutex_lock(&self.runtime.fence);
            publication.state = FenceState::Retiring;
            runtime.state = FenceState::Retiring;
            let client = runtime.client.take();
            if runtime.active_calls == 0 {
                runtime.state = FenceState::Closed;
            }
            if publication.active_calls == 0 {
                publication.state = FenceState::Closed;
            }
            client
        };
        for handle in mutex_lock(&self.runtime.tasks).drain(..) {
            handle.abort();
        }
        drop(detached_client);
    }

    fn admit(self: &Arc<Self>) -> Result<AdmissionLease> {
        let mut publication = mutex_lock(&self.fence);
        let mut runtime = mutex_lock(&self.runtime.fence);
        if publication.state != FenceState::Open || runtime.state != FenceState::Open {
            return Err(McpError::StalePublication {
                publication_id: self.publication_id.get(),
                runtime_id: self.runtime.runtime_id.get(),
            });
        }
        let client = runtime.client.clone().ok_or(McpError::StalePublication {
            publication_id: self.publication_id.get(),
            runtime_id: self.runtime.runtime_id.get(),
        })?;
        publication.active_calls = publication.active_calls.saturating_add(1);
        runtime.active_calls = runtime.active_calls.saturating_add(1);
        drop(runtime);
        drop(publication);
        Ok(AdmissionLease {
            publication: self.clone(),
            client,
        })
    }

    #[cfg(test)]
    pub(super) fn fence_state(&self) -> FenceState {
        mutex_lock(&self.fence).state
    }

    #[cfg(test)]
    pub(super) fn active_calls(&self) -> usize {
        mutex_lock(&self.fence).active_calls
    }
}

struct AdmissionLease {
    publication: Arc<ServerPublication>,
    client: Arc<McpProtocolClient>,
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        let mut publication = mutex_lock(&self.publication.fence);
        let mut runtime = mutex_lock(&self.publication.runtime.fence);
        publication.active_calls = publication.active_calls.saturating_sub(1);
        runtime.active_calls = runtime.active_calls.saturating_sub(1);
        if publication.state == FenceState::Retiring && publication.active_calls == 0 {
            publication.state = FenceState::Closed;
        }
        if runtime.state == FenceState::Retiring && runtime.active_calls == 0 {
            runtime.state = FenceState::Closed;
        }
    }
}

/// A passive, generation-pinned exact MCP dispatch ticket.
#[derive(Clone)]
pub struct ResolvedMcpCall {
    canonical_name: String,
    original_name: String,
    authority: Arc<GenerationAuthority>,
    publication: Arc<ServerPublication>,
}

impl fmt::Debug for ResolvedMcpCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedMcpCall")
            .field("canonical_name", &self.canonical_name)
            .field("publication_id", &self.publication.publication_id)
            .field("runtime_id", &self.publication.runtime.runtime_id)
            .finish_non_exhaustive()
    }
}

impl ResolvedMcpCall {
    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub fn original_name(&self) -> &str {
        &self.original_name
    }

    pub fn server_id(&self) -> &str {
        &self.publication.server_id
    }

    pub fn publication_id(&self) -> PublicationId {
        self.publication.publication_id
    }

    pub fn runtime_id(&self) -> RuntimeId {
        self.publication.runtime.runtime_id
    }

    pub(super) fn expected(&self) -> ExpectedPublication {
        ExpectedPublication {
            publication: self.publication.clone(),
        }
    }

    pub(super) fn belongs_to(&self, authority: &Arc<GenerationAuthority>) -> bool {
        GenerationAuthority::same_authority(&self.authority, authority)
    }

    pub(super) fn admit(&self) -> Result<AdmittedMcpCall> {
        let lease = self.publication.admit()?;
        Ok(AdmittedMcpCall {
            resolved: self.clone(),
            lease,
        })
    }
}

/// Internal active lease for one generation-pinned MCP call. It is
/// intentionally non-cloneable: consuming execution or Drop releases exactly
/// one admission.
pub(super) struct AdmittedMcpCall {
    pub(super) resolved: ResolvedMcpCall,
    lease: AdmissionLease,
}

impl AdmittedMcpCall {
    pub(super) fn client(&self) -> &McpProtocolClient {
        &self.lease.client
    }

    pub(super) fn runtime(&self) -> &Arc<TransportRuntime> {
        &self.resolved.publication.runtime
    }
}

/// Opaque authority carried by notification, health, QoS and reconnect work.
#[derive(Clone)]
pub(super) struct ExpectedPublication {
    pub(super) publication: Arc<ServerPublication>,
}

impl ExpectedPublication {
    pub(super) fn server_id(&self) -> &str {
        &self.publication.server_id
    }

    pub(super) fn runtime(&self) -> &Arc<TransportRuntime> {
        &self.publication.runtime
    }
}

pub(crate) struct McpRuntimeGeneration {
    pub(crate) revision: u64,
    pub(crate) servers: BTreeMap<String, Arc<ServerPublication>>,
    pub(crate) index: Arc<IndexState>,
    schemas: Arc<[ToolSchema]>,
}

impl fmt::Debug for McpRuntimeGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRuntimeGeneration")
            .field("revision", &self.revision)
            .field("server_count", &self.servers.len())
            .field("schema_count", &self.schemas.len())
            .finish()
    }
}

impl McpRuntimeGeneration {
    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self {
            revision: 0,
            servers: BTreeMap::new(),
            index: Arc::new(IndexState::default()),
            schemas: Arc::from([]),
        })
    }

    pub(crate) fn plan(
        base: &Arc<Self>,
        replacements: &[Arc<ServerPublication>],
        removals: &[String],
        ledger_relationship_limit: usize,
        require_runtime: bool,
    ) -> Result<Arc<Self>> {
        let catalogs = replacements
            .iter()
            .map(|publication| publication.catalog.clone())
            .collect::<Vec<_>>();
        let index =
            base.index
                .plan_catalog_update(&catalogs, removals, ledger_relationship_limit)?;
        Self::with_index(base, replacements, removals, index, require_runtime)
    }

    pub(crate) fn with_index(
        base: &Arc<Self>,
        replacements: &[Arc<ServerPublication>],
        removals: &[String],
        index: IndexState,
        require_runtime: bool,
    ) -> Result<Arc<Self>> {
        let mut servers = base.servers.clone();
        for server_id in removals {
            servers.remove(server_id);
        }
        for publication in replacements {
            servers.insert(publication.server_id.clone(), publication.clone());
        }

        let mut schemas = Vec::new();
        for alias in index.all_aliases() {
            let Some(publication) = servers.get(&alias.server_id) else {
                if require_runtime {
                    return Err(ToolRegistrationError::ProviderSchemaUnavailable.into());
                }
                continue;
            };
            let Some(schema) = publication
                .schemas()
                .iter()
                .find(|schema| schema.function.name == alias.alias)
            else {
                return Err(ToolRegistrationError::ProviderSchemaUnavailable.into());
            };
            schemas.push(schema.clone());
        }

        Ok(Arc::new(Self {
            revision: index.revision(),
            servers,
            index: Arc::new(index),
            schemas: schemas.into(),
        }))
    }

    fn resolve(
        &self,
        authority: &Arc<GenerationAuthority>,
        reference: &str,
    ) -> Option<ResolvedMcpCall> {
        let ResolvedIndexAlias {
            canonical_alias,
            server_id,
            original_name,
        } = self.index.resolve(reference)?;
        let publication = self.servers.get(&server_id)?.clone();
        publication.tool(&original_name)?;
        Some(ResolvedMcpCall {
            canonical_name: canonical_alias,
            original_name,
            authority: authority.clone(),
            publication,
        })
    }

    fn resolve_server_tool(
        &self,
        authority: &Arc<GenerationAuthority>,
        server_id: &str,
        original_name: &str,
    ) -> Option<ResolvedMcpCall> {
        let publication = self.servers.get(server_id)?.clone();
        publication.tool(original_name)?;
        let canonical_name = publication
            .catalog
            .aliases()
            .into_iter()
            .find(|alias| alias.original_name == original_name)?
            .alias;
        Some(ResolvedMcpCall {
            canonical_name,
            original_name: original_name.to_string(),
            authority: authority.clone(),
            publication,
        })
    }
}

pub(crate) struct GenerationAuthority {
    cell: RwLock<Arc<McpRuntimeGeneration>>,
    pub(crate) ledger_relationship_limit: usize,
}

impl GenerationAuthority {
    pub(crate) fn new(ledger_relationship_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            cell: RwLock::new(McpRuntimeGeneration::empty()),
            ledger_relationship_limit,
        })
    }

    fn read(&self) -> RwLockReadGuard<'_, Arc<McpRuntimeGeneration>> {
        self.cell
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Arc<McpRuntimeGeneration>> {
        self.cell
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn generation(&self) -> Arc<McpRuntimeGeneration> {
        self.read().clone()
    }

    pub(crate) fn same_authority(left: &Arc<Self>, right: &Arc<Self>) -> bool {
        Arc::ptr_eq(left, right)
    }

    pub(crate) fn is_current(&self, expected: &Arc<McpRuntimeGeneration>) -> bool {
        Arc::ptr_eq(&self.read(), expected)
    }

    /// The reconcile lock proves exclusivity and callers validate the base before
    /// a durable boundary. Replacement is therefore intentionally infallible.
    #[cfg(test)]
    pub(crate) fn replace_prevalidated(
        &self,
        expected: &Arc<McpRuntimeGeneration>,
        next: Arc<McpRuntimeGeneration>,
    ) {
        self.replace_prevalidated_with(expected, next, || {});
    }

    /// Run fence retirement and the Arc swap while snapshot acquisition is
    /// excluded. Readers therefore pin either an open old generation or the
    /// complete new generation, never a listed old alias after its fence closed.
    pub(crate) fn replace_prevalidated_with(
        &self,
        expected: &Arc<McpRuntimeGeneration>,
        next: Arc<McpRuntimeGeneration>,
        before_swap: impl FnOnce(),
    ) {
        let mut current = self.write();
        assert!(
            Arc::ptr_eq(&current, expected),
            "MCP generation writer invariant violated"
        );
        before_swap();
        *current = next;
    }

    #[cfg(test)]
    pub(crate) fn try_replace(
        &self,
        expected: &Arc<McpRuntimeGeneration>,
        next: Arc<McpRuntimeGeneration>,
    ) -> bool {
        let mut current = self.write();
        if !Arc::ptr_eq(&current, expected) {
            return false;
        }
        *current = next;
        true
    }
}

/// One immutable generation pin. All multi-field reads and exact resolution are
/// served from this Arc without consulting live manager state again.
#[derive(Clone)]
pub struct McpRuntimeSnapshot {
    authority: Arc<GenerationAuthority>,
    generation: Arc<McpRuntimeGeneration>,
}

impl McpRuntimeSnapshot {
    pub(crate) fn new(
        authority: Arc<GenerationAuthority>,
        generation: Arc<McpRuntimeGeneration>,
    ) -> Self {
        Self {
            authority,
            generation,
        }
    }

    pub fn revision(&self) -> u64 {
        self.generation.revision
    }

    pub fn list_tools(&self) -> Vec<ToolSchema> {
        self.generation.schemas.to_vec()
    }

    pub fn aliases(&self) -> Vec<ToolAlias> {
        self.generation.index.all_aliases()
    }

    pub fn contains_exact_alias(&self, alias: &str) -> bool {
        self.generation.index.contains_exact(alias)
    }

    pub fn lookup(&self, alias: &str) -> Option<ToolAlias> {
        self.generation.index.lookup(alias)
    }

    pub fn resolve_call(&self, reference: &str) -> Option<ResolvedMcpCall> {
        self.generation.resolve(&self.authority, reference)
    }

    pub fn server_ids(&self) -> Vec<String> {
        self.generation.servers.keys().cloned().collect()
    }

    pub fn contains_server(&self, server_id: &str) -> bool {
        self.generation.servers.contains_key(server_id)
    }

    pub fn server_info(&self, server_id: &str) -> Option<RuntimeInfo> {
        let publication = self.generation.servers.get(server_id)?;
        let mut info = publication.runtime.runtime.info.try_read().ok()?.clone();
        info.tool_count = publication.tool_count();
        Some(info)
    }

    pub fn tool(&self, server_id: &str, original_name: &str) -> Option<McpTool> {
        self.generation.servers.get(server_id)?.tool(original_name)
    }

    pub fn connected_server_instructions(&self) -> Vec<(String, String)> {
        self.generation
            .servers
            .iter()
            .filter_map(|(server_id, publication)| {
                let info = publication.runtime.runtime.info.try_read().ok()?;
                (info.status == crate::types::ServerStatus::Ready)
                    .then(|| info.instructions.clone())
                    .flatten()
                    .map(|instructions| (server_id.clone(), instructions))
            })
            .collect()
    }

    pub(super) fn expected_server(&self, server_id: &str) -> Option<ExpectedPublication> {
        Some(ExpectedPublication {
            publication: self.generation.servers.get(server_id)?.clone(),
        })
    }

    pub(super) fn expected_runtime(
        &self,
        runtime: &Arc<TransportRuntime>,
    ) -> Option<ExpectedPublication> {
        self.generation
            .servers
            .values()
            .find(|publication| Arc::ptr_eq(&publication.runtime, runtime))
            .cloned()
            .map(|publication| ExpectedPublication { publication })
    }

    pub(super) fn resolve_server_tool(
        &self,
        server_id: &str,
        original_name: &str,
    ) -> Option<ResolvedMcpCall> {
        self.generation
            .resolve_server_tool(&self.authority, server_id, original_name)
    }
}

pub(super) fn admit_resolved(
    authority: &Arc<GenerationAuthority>,
    resolved: &ResolvedMcpCall,
) -> Result<AdmittedMcpCall> {
    if !resolved.belongs_to(authority) {
        return Err(McpError::ForeignRuntimeAuthority);
    }
    resolved.admit()
}
