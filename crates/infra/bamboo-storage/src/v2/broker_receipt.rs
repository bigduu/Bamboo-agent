//! Host-private receipts for broker Event/Outcome deletion.
//!
//! This file is deliberately outside `Session` and its runtime sidecar. A
//! worker, model, or ordinary Session writer cannot assert that the Host has
//! checkpointed a broker Outcome by setting metadata.

use super::*;
use bamboo_domain::{
    ParentQuestion, ParentQuestionOutcome, ParentQuestionResolution, SessionMessageId,
    PARENT_QUESTION_REQUEST_KEY,
};

const RECEIPTS_FILE: &str = "broker-terminal-receipts.v1.json";
const MAX_RECEIPTS: usize = 32;
const MAX_IDS: usize = 4096;
const MAX_RECEIPT_SCAN_ROOTS: usize = 4096;
const MAX_RECEIPT_SCAN_CHILDREN: usize = 16384;
const MAX_RECEIPT_LEDGER_BYTES: u64 = 32 * 1024 * 1024;
const COMPLETION_SOURCE_KEY: &str = "runtime.child_completion_source_v1";

/// The durable, bounded Host proof for a strict terminal receipt. This is not
/// Worker-supplied evidence: only the Host-only prepare/commit APIs can publish
/// it. The sequence is the Host's independently applied contiguous frontier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerTerminalCompleteness {
    pub version: u32,
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub parent_session_id: String,
    pub root_session_id: String,
    pub project_id: Option<String>,
    pub spawn_depth: u32,
    pub activation_run_id: String,
    pub execution_epoch: u64,
    pub contiguous_applied_seq: u64,
    pub message_count: usize,
    pub messages_sha256: String,
}

/// Rust-only live Host evidence. Deliberately not deserializable from Worker
/// JSON or Session metadata. The caller must derive the frontier from applied
/// Events, never copy a Worker's claimed final sequence into this constructor.
#[derive(Clone)]
pub struct HostTerminalCompleteness {
    proof: BrokerTerminalCompleteness,
}

impl HostTerminalCompleteness {
    pub fn from_verified_host(
        session: &Session,
        activation_run_id: &str,
        execution_epoch: u64,
        contiguous_applied_seq: u64,
    ) -> io::Result<Self> {
        validate_session_id(&session.id)?;
        bamboo_domain::ActorSession::from_session(session)
            .map_err(|_| invalid("invalid Host terminal completeness lineage"))?;
        if session.kind != SessionKind::Child
            || session
                .parent_session_id
                .as_deref()
                .is_none_or(str::is_empty)
            || session.root_session_id.is_empty()
            || activation_run_id.is_empty()
            || activation_run_id.len() > 128
            || execution_epoch == 0
        {
            return Err(invalid("invalid Host terminal completeness identity"));
        }
        Ok(Self {
            proof: BrokerTerminalCompleteness {
                version: 1,
                session_id: session.id.clone(),
                created_at: session.created_at,
                parent_session_id: session.parent_session_id.clone().expect("validated parent"),
                root_session_id: session.root_session_id.clone(),
                project_id: session.project_id_meta(),
                spawn_depth: session.spawn_depth,
                activation_run_id: activation_run_id.into(),
                execution_epoch,
                contiguous_applied_seq,
                message_count: session.messages.len(),
                messages_sha256: digest_messages(&session.messages)?,
            },
        })
    }

    pub fn execution_epoch(&self) -> u64 {
        self.proof.execution_epoch
    }

    pub fn contiguous_applied_seq(&self) -> u64 {
        self.proof.contiguous_applied_seq
    }
}

/// Rust-only Host evidence; Session metadata and Worker JSON cannot construct
/// this value. The embedding Host first verifies its authoritative question
/// receipt, then freezes this exact Child/run/transcript for broker storage.
pub struct HostToolYield {
    session_id: String,
    created_at: DateTime<Utc>,
    activation_run_id: String,
    messages_sha256: String,
    call_id: String,
}

impl HostToolYield {
    pub fn from_verified_host(session: &Session, run_id: &str, call_id: &str) -> io::Result<Self> {
        if !exact_yield_tool(&session.messages, call_id) {
            return Err(invalid("Host yielded tool pair is not exact"));
        }
        Ok(Self {
            session_id: session.id.clone(),
            created_at: session.created_at,
            activation_run_id: run_id.into(),
            messages_sha256: digest_messages(&session.messages)?,
            call_id: call_id.into(),
        })
    }
}

pub struct BrokerTerminalRoute<'a> {
    pub activation_run_id: &'a str,
    pub broker_identity: &'a str,
    pub parent_mailbox: &'a str,
    pub broker_correlation_id: &'a str,
    pub message_ids: &'a [String],
}

fn exact_yield_tool(messages: &[Message], call_id: &str) -> bool {
    messages.last().is_some_and(|last| {
        last.role == Role::Tool
            && last.tool_call_id.as_deref() == Some(call_id)
            && last.tool_success == Some(true)
            && messages
                .iter()
                .flat_map(|m| m.tool_calls.iter().flatten())
                .filter(|call| call.id == call_id)
                .count()
                == 1
            && messages
                .iter()
                .flat_map(|m| m.tool_calls.iter().flatten())
                .any(|call| call.id == call_id && call.function.name == "Task")
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerTerminalReceipt {
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub parent_session_id: String,
    pub root_session_id: String,
    pub project_id: Option<String>,
    pub activation_run_id: String,
    pub broker_identity: String,
    /// Exact parent mailbox that received this Run's Event/Outcome frames.
    /// This is mandatory: an old receipt without a route cannot prove where
    /// an idempotent broker ACK was applied.
    pub parent_mailbox: String,
    pub broker_correlation_id: String,
    pub message_ids: Vec<String>,
    pub message_count: usize,
    pub messages_sha256: String,
    pub terminal_status: String,
    #[serde(default)]
    pub terminal_error: Option<String>,
    /// Exact Host-issued request marker at prepare time. The initial durable
    /// Child may not have this marker until the same Run's final checkpoint.
    #[serde(default)]
    pub parent_question_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_yield_call_id: Option<String>,
    /// Independent strict marker. Missing proof must never reinterpret a strict
    /// receipt as legacy. None retains the explicit legacy receipt behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_execution_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_completeness: Option<BrokerTerminalCompleteness>,
    committed: bool,
}

#[derive(Default, Serialize, Deserialize)]
struct ReceiptLedger {
    #[serde(default)]
    receipts: Vec<BrokerTerminalReceipt>,
    /// Latest ACKed transcript prefix. Ordinary writers may append but may
    /// never erase an answer whose broker Outcome has already been deleted.
    #[serde(default)]
    acknowledged_anchor: Option<BrokerTerminalReceipt>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn digest_messages(messages: &[Message]) -> io::Result<String> {
    // Convert through Value so object keys (including message metadata) have
    // canonical ordering after a cold JSON reload.
    let value = serde_json::to_value(messages).map_err(|_| invalid("invalid broker transcript"))?;
    let bytes = serde_json::to_vec(&value).map_err(|_| invalid("invalid broker transcript"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(bytes)))
}

fn valid_parent_mailbox(mailbox: &str) -> bool {
    !mailbox.is_empty()
        && mailbox.len() <= 256
        && mailbox != "."
        && mailbox != ".."
        && mailbox
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
}

fn identity_matches(receipt: &BrokerTerminalReceipt, session: &Session) -> bool {
    session.kind == SessionKind::Child
        && receipt.session_id == session.id
        && receipt.created_at == session.created_at
        && session.parent_session_id.as_deref() == Some(receipt.parent_session_id.as_str())
        && receipt.root_session_id == session.root_session_id
        && receipt.project_id == session.project_id_meta()
        && valid_parent_mailbox(&receipt.parent_mailbox)
}

// This is only the receipt-bound projection of the engine's completion source.
// The engine still validates the complete source before publishing completion.
#[derive(Deserialize)]
struct PendingCompletionIdentity {
    activation_run_id: String,
    child_session_id: String,
    child_created_at: DateTime<Utc>,
    parent_session_id: String,
    project_id: Option<String>,
    status: String,
    error_sha256: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingCompletionEnvelope {
    pending_confirmation: PendingCompletionIdentity,
}

fn pending_completion_matches(receipt: &BrokerTerminalReceipt, session: &Session) -> bool {
    let Some(raw) = session.metadata.get(COMPLETION_SOURCE_KEY) else {
        return false;
    };
    if raw.len() > 8192 {
        return false;
    }
    let Ok(envelope) = serde_json::from_str::<PendingCompletionEnvelope>(raw) else {
        return false;
    };
    completion_identity_matches(receipt, &envelope.pending_confirmation)
}

fn completion_identity_matches(
    receipt: &BrokerTerminalReceipt,
    source: &PendingCompletionIdentity,
) -> bool {
    let expected_error = match receipt.terminal_error.as_ref() {
        Some(error) => match serde_json::to_vec(error) {
            Ok(bytes) => Some(format!("{:x}", Sha256::digest(bytes))),
            Err(_) => return false,
        },
        None => None,
    };
    source.activation_run_id == receipt.activation_run_id
        && source.child_session_id == receipt.session_id
        && source.child_created_at == receipt.created_at
        && source.parent_session_id == receipt.parent_session_id
        && source.project_id == receipt.project_id
        && source.status == receipt.terminal_status
        && source.error_sha256 == expected_error
}

fn staged_terminal_matches(receipt: &BrokerTerminalReceipt, session: &Session) -> bool {
    session.last_run_status().as_deref() == Some("running")
        && session.last_run_error().is_none()
        && pending_completion_matches(receipt, session)
}

fn completeness_matches_receipt(receipt: &BrokerTerminalReceipt) -> bool {
    match (
        receipt.required_execution_epoch,
        receipt.terminal_completeness.as_ref(),
    ) {
        (None, None) => true,
        (Some(epoch), Some(proof)) => {
            proof.version == 1
                && epoch != 0
                && proof.execution_epoch == epoch
                && proof.session_id == receipt.session_id
                && proof.created_at == receipt.created_at
                && proof.parent_session_id == receipt.parent_session_id
                && proof.root_session_id == receipt.root_session_id
                && proof.project_id == receipt.project_id
                && proof.activation_run_id == receipt.activation_run_id
                && proof.message_count <= receipt.message_count
        }
        _ => false,
    }
}

fn completeness_prefix_matches(
    receipt: &BrokerTerminalReceipt,
    session: &Session,
) -> io::Result<bool> {
    if !completeness_matches_receipt(receipt) {
        return Ok(false);
    }
    let Some(proof) = receipt.terminal_completeness.as_ref() else {
        return Ok(true);
    };
    if proof.spawn_depth != session.spawn_depth {
        return Ok(false);
    }
    // The SDK may append Host messages after Event drain. Both the earlier
    // applied-event prefix and the final prepared receipt remain immutable.
    let mut prefix_receipt = receipt.clone();
    prefix_receipt.message_count = proof.message_count;
    prefix_receipt.messages_sha256 = proof.messages_sha256.clone();
    prefix_receipt.tool_yield_call_id = None;
    prefix_matches(&prefix_receipt, session)
}

fn prefix_matches(receipt: &BrokerTerminalReceipt, session: &Session) -> io::Result<bool> {
    if !identity_matches(receipt, session) || session.messages.len() < receipt.message_count {
        return Ok(false);
    }
    if receipt.terminal_status == "suspended"
        && session.metadata.get(PARENT_QUESTION_REQUEST_KEY)
            != receipt.parent_question_request.as_ref()
    {
        return Ok(false);
    }
    let prefix = &session.messages[..receipt.message_count];
    if receipt
        .tool_yield_call_id
        .as_deref()
        .is_some_and(|id| !exact_yield_tool(prefix, id))
    {
        return Ok(false);
    }
    if digest_messages(prefix)? == receipt.messages_sha256 {
        return Ok(true);
    }
    // A suspended direct-parent question has one deliberately mutable Tool
    // result. The parent's answer CAS replaces its placeholder after the Host
    // has ACKed the suspended Outcome. Prove that restoring only that content
    // yields the exact original broker transcript; all other bytes stay fixed.
    if receipt.terminal_status != "suspended" {
        return Ok(false);
    }
    let Some(question) = session
        .metadata
        .get(PARENT_QUESTION_REQUEST_KEY)
        .and_then(|raw| serde_json::from_str::<ParentQuestion>(raw).ok())
    else {
        return Ok(false);
    };
    let Some(resolution) = ParentQuestionResolution::from_orphan_child(session, &question.id)
    else {
        return Ok(false);
    };
    let ParentQuestionOutcome::Answer { text } = &resolution.outcome else {
        return Ok(false);
    };
    if resolution.request != question {
        return Ok(false);
    }
    let mut original = prefix.to_vec();
    let mut pairs = original
        .iter_mut()
        .filter(|message| message.id == question.tool_result_message_id);
    let Some(pair) = pairs.next() else {
        return Ok(false);
    };
    if pairs.next().is_some() || pair.content != text.as_str() {
        return Ok(false);
    }
    pair.content = format!("Clarification needed: {}", question.question);
    if SessionMessageId::stable(
        "direct-parent-question-result-v1",
        &serde_json::to_value(pair).map_err(|_| invalid("invalid parent question pair"))?,
    ) != question.paired_result_digest
    {
        return Ok(false);
    }
    Ok(digest_messages(&original)? == receipt.messages_sha256)
}

async fn read_ledger(dir: &Path) -> io::Result<ReceiptLedger> {
    match fs::read(dir.join(RECEIPTS_FILE)).await {
        Ok(bytes) => {
            let ledger: ReceiptLedger = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("invalid Host broker receipt ledger"))?;
            if ledger
                .receipts
                .iter()
                .chain(ledger.acknowledged_anchor.iter())
                .any(|receipt| {
                    !valid_parent_mailbox(&receipt.parent_mailbox)
                        || !completeness_matches_receipt(receipt)
                })
            {
                return Err(invalid(
                    "invalid Host broker receipt route or completeness proof",
                ));
            }
            Ok(ledger)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ReceiptLedger::default()),
        Err(error) => Err(error),
    }
}

async fn write_ledger(dir: &Path, ledger: &ReceiptLedger) -> io::Result<()> {
    #[cfg(test)]
    if RECEIPT_WRITE_FAILURES.lock().unwrap().remove(dir) {
        return Err(io::Error::other(
            "injected Host broker receipt write failure",
        ));
    }
    let bytes = serde_json::to_vec(ledger).map_err(|_| invalid("invalid Host broker receipt"))?;
    durable_atomic_write(&dir.join(RECEIPTS_FILE), &bytes).await
}

#[cfg(test)]
static RECEIPT_WRITE_FAILURES: std::sync::LazyLock<std::sync::Mutex<HashSet<PathBuf>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

async fn real_receipt_directory(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(invalid(
            "broker receipt scan encountered a non-directory or symlink",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn real_receipt_file(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(invalid(
            "broker receipt scan encountered a non-file or symlink",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

impl SessionStoreV2 {
    /// Find every Child with a durable broker Outcome awaiting ACK, including
    /// one whose successor Run was never started. The physical tree, not this
    /// process's potentially stale `sessions.json` snapshot, is the source of
    /// candidates. Malformed ledgers are isolated to their Child; a canonical
    /// Session identity ambiguity still stops startup recovery rather than
    /// allowing an ACK against the wrong Child.
    ///
    /// The discovered canonical entries repair the rebuildable index only
    /// after the whole scan has passed, so the existing receipt recovery and
    /// ACK APIs can address them on an independently reopened store.
    pub async fn discover_unconfirmed_broker_terminal_children(&self) -> io::Result<Vec<Session>> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _runtime_task = self.lock_runtime_task_sidecar_shared().await?;
        if !real_receipt_directory(&self.sessions_dir).await? {
            return Ok(Vec::new());
        }

        let mut seen_ids = HashSet::new();
        let mut roots = 0usize;
        let mut children = 0usize;
        let mut candidates = Vec::new();
        let mut root_entries = fs::read_dir(&self.sessions_dir).await?;
        while let Some(root_entry) = root_entries.next_entry().await? {
            let root_dir = root_entry.path();
            let root_meta = fs::symlink_metadata(&root_dir).await?;
            if root_meta.file_type().is_symlink() {
                return Err(invalid("broker receipt scan encountered a symlinked Root"));
            }
            if root_meta.file_type().is_file() {
                continue;
            }
            if !root_meta.file_type().is_dir() {
                return Err(invalid(
                    "broker receipt scan encountered an invalid Root entry",
                ));
            }
            roots += 1;
            if roots > MAX_RECEIPT_SCAN_ROOTS {
                return Err(invalid("broker receipt Root scan limit exceeded"));
            }
            let root_id = root_entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("broker receipt Root ID is not UTF-8"))?;
            validate_session_id(&root_id)?;
            if !seen_ids.insert(root_id.clone()) {
                return Err(invalid("ambiguous broker receipt Session ID"));
            }
            let children_dir = root_dir.join("children");
            if !real_receipt_directory(&children_dir).await? {
                continue;
            }
            let mut child_entries = fs::read_dir(&children_dir).await?;
            while let Some(child_entry) = child_entries.next_entry().await? {
                let child_dir = child_entry.path();
                let child_meta = fs::symlink_metadata(&child_dir).await?;
                if child_meta.file_type().is_symlink() {
                    return Err(invalid("broker receipt scan encountered a symlinked Child"));
                }
                if child_meta.file_type().is_file() {
                    continue;
                }
                if !child_meta.file_type().is_dir() {
                    return Err(invalid(
                        "broker receipt scan encountered an invalid Child entry",
                    ));
                }
                children += 1;
                if children > MAX_RECEIPT_SCAN_CHILDREN {
                    return Err(invalid("broker receipt Child scan limit exceeded"));
                }
                let child_id = child_entry
                    .file_name()
                    .into_string()
                    .map_err(|_| invalid("broker receipt Child ID is not UTF-8"))?;
                validate_session_id(&child_id)?;
                if !seen_ids.insert(child_id.clone()) {
                    return Err(invalid("ambiguous broker receipt Session ID"));
                }
                let receipt_path = child_dir.join(RECEIPTS_FILE);
                if !real_receipt_file(&receipt_path).await? {
                    continue;
                }
                if fs::symlink_metadata(&receipt_path).await?.len() > MAX_RECEIPT_LEDGER_BYTES {
                    tracing::warn!(child_id = %child_id, "Broker receipt ledger exceeds scan limit; skipping Child recovery");
                    continue;
                }
                let ledger = match read_ledger(&child_dir).await {
                    Ok(ledger) => ledger,
                    Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                        tracing::warn!(child_id = %child_id, %error, "Invalid broker receipt ledger; skipping Child recovery");
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if ledger.receipts.is_empty() {
                    continue;
                }
                if ledger.receipts.len() > MAX_RECEIPTS {
                    tracing::warn!(child_id = %child_id, "Broker receipt ledger has too many receipts; skipping Child recovery");
                    continue;
                }
                if !real_receipt_file(&child_dir.join("session.json")).await? {
                    return Err(invalid("broker receipt Child canonical files are invalid"));
                }
                // Absence is valid for a legacy Child; a present sidecar must
                // be a regular file before the strict loader reads it.
                real_receipt_file(&child_dir.join(RUNTIME_SIDECAR_FILE)).await?;
                let session = self
                    .load_session_from_dir_strict(
                        &child_dir,
                        &child_id,
                        SessionKind::Child,
                        &root_id,
                    )
                    .await?
                    .ok_or_else(|| invalid("broker receipt Child is unavailable"))?;
                bamboo_domain::ActorSession::from_session(&session)
                    .map_err(|_| invalid("broker receipt Child lineage is invalid"))?;
                real_receipt_file(&root_dir.join(RUNTIME_SIDECAR_FILE)).await?;
                if !real_receipt_file(&root_dir.join("session.json")).await?
                    || self
                        .load_session_from_dir_strict(
                            &root_dir,
                            &root_id,
                            SessionKind::Root,
                            &root_id,
                        )
                        .await?
                        .is_none()
                {
                    return Err(invalid("broker receipt Root is unavailable"));
                }
                if ledger
                    .receipts
                    .iter()
                    .chain(ledger.acknowledged_anchor.iter())
                    .any(|receipt| !identity_matches(receipt, &session))
                {
                    tracing::warn!(child_id = %child_id, "Broker receipt identity mismatch; skipping Child recovery");
                    continue;
                }
                let attachments_dir = child_dir.join("attachments");
                let has_attachments = if real_receipt_directory(&attachments_dir).await? {
                    fs::read_dir(&attachments_dir)
                        .await?
                        .next_entry()
                        .await?
                        .is_some()
                } else {
                    false
                };
                candidates.push((
                    session,
                    Self::child_rel_path(&root_id, &child_id),
                    has_attachments,
                ));
            }
        }
        for (session, rel_path, has_attachments) in &candidates {
            self.upsert_index_from_session_inner(
                session,
                rel_path.clone(),
                true,
                Some(*has_attachments),
            )
            .await?;
        }
        Ok(candidates
            .into_iter()
            .map(|(session, _, _)| session)
            .collect())
    }

    async fn broker_receipt_dir(&self, session_id: &str) -> io::Result<PathBuf> {
        let path = self
            .session_json_path(session_id)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Child session missing"))?;
        Ok(path
            .parent()
            .expect("session.json has a parent")
            .to_path_buf())
    }

    /// Prepare the exact Host-owned Run receipt before the final Child save.
    /// The separate durable record survives a crash after the Session save but
    /// before the broker ACK. It is not yet permission to ACK.
    pub async fn prepare_broker_terminal_receipt(
        &self,
        session: &Session,
        activation_run_id: &str,
        broker_identity: &str,
        parent_mailbox: &str,
        broker_correlation_id: &str,
        message_ids: &[String],
    ) -> io::Result<()> {
        self.prepare_terminal_receipt(
            session,
            BrokerTerminalRoute {
                activation_run_id,
                broker_identity,
                parent_mailbox,
                broker_correlation_id,
                message_ids,
            },
            None,
            None,
        )
        .await
    }

    /// Prepare a strict receipt using an independently derived, live Host
    /// frontier. This does not authorize ACK until explicit strict commit.
    pub async fn prepare_broker_terminal_receipt_with_completeness(
        &self,
        session: &Session,
        route: BrokerTerminalRoute<'_>,
        completeness: &HostTerminalCompleteness,
    ) -> io::Result<()> {
        self.prepare_terminal_receipt(session, route, None, Some(completeness))
            .await
    }

    pub async fn prepare_broker_tool_yield_receipt(
        &self,
        session: &Session,
        route: BrokerTerminalRoute<'_>,
        proof: &HostToolYield,
    ) -> io::Result<()> {
        self.prepare_terminal_receipt(session, route, Some(proof), None)
            .await
    }

    pub async fn prepare_broker_tool_yield_receipt_with_completeness(
        &self,
        session: &Session,
        route: BrokerTerminalRoute<'_>,
        proof: &HostToolYield,
        completeness: &HostTerminalCompleteness,
    ) -> io::Result<()> {
        self.prepare_terminal_receipt(session, route, Some(proof), Some(completeness))
            .await
    }

    async fn prepare_terminal_receipt(
        &self,
        session: &Session,
        route: BrokerTerminalRoute<'_>,
        proof: Option<&HostToolYield>,
        completeness: Option<&HostTerminalCompleteness>,
    ) -> io::Result<()> {
        let BrokerTerminalRoute {
            activation_run_id,
            broker_identity,
            parent_mailbox,
            broker_correlation_id,
            message_ids,
        } = route;
        if let Some(proof) = proof {
            if proof.session_id != session.id
                || proof.created_at != session.created_at
                || proof.activation_run_id != activation_run_id
                || proof.messages_sha256 != digest_messages(&session.messages)?
                || !exact_yield_tool(&session.messages, &proof.call_id)
                || session.last_run_status().as_deref() != Some("completed")
            {
                return Err(invalid("Host tool yield proof changed"));
            }
        }
        validate_session_id(&session.id)?;
        if session.kind != SessionKind::Child
            || activation_run_id.is_empty()
            || activation_run_id.len() > 128
            || uuid::Uuid::parse_str(broker_identity).is_err()
            || !valid_parent_mailbox(parent_mailbox)
            || broker_correlation_id.is_empty()
            || broker_correlation_id.len() > 128
            || message_ids.is_empty()
            || message_ids.len() > MAX_IDS
            || message_ids.iter().any(|id| id.is_empty() || id.len() > 128)
            || message_ids.iter().collect::<HashSet<_>>().len() != message_ids.len()
            || session.messages.is_empty()
            || !matches!(
                session.last_run_status().as_deref(),
                Some("completed" | "suspended" | "error" | "cancelled")
            )
        {
            return Err(invalid("invalid prepared broker terminal receipt"));
        }
        let terminal_error = session.last_run_error();
        let failed = matches!(
            session.last_run_status().as_deref(),
            Some("error" | "cancelled")
        );
        if failed
            != terminal_error
                .as_ref()
                .is_some_and(|error| !error.is_empty())
        {
            return Err(invalid(
                "Child broker terminal error proof missing or unexpected",
            ));
        }
        if proof.is_none()
            && session.last_run_status().as_deref() == Some("completed")
            && !session.messages.last().is_some_and(|message| {
                message.role == Role::Assistant && !message.content.trim().is_empty()
            })
        {
            return Err(invalid("completed Child has no terminal transcript reply"));
        }
        let _guard = self
            .acquire_session_write_lock(&session.id, SaveKind::Runtime)
            .await?;
        let durable = self
            .load_session_unlocked(&session.id)
            .await?
            .ok_or_else(|| invalid("canonical Child missing for broker receipt"))?;
        if durable.kind != SessionKind::Child
            || durable.id != session.id
            || durable.created_at != session.created_at
            || durable.parent_session_id != session.parent_session_id
            || durable.root_session_id != session.root_session_id
            || durable.project_id_meta() != session.project_id_meta()
            || durable.messages.len() > session.messages.len()
            || digest_messages(&durable.messages)?
                != digest_messages(&session.messages[..durable.messages.len()])?
        {
            return Err(invalid("stale Child broker receipt identity or transcript"));
        }
        let dir = self.broker_receipt_dir(&session.id).await?;
        let mut ledger = read_ledger(&dir).await?;
        if let Some(anchor) = ledger.acknowledged_anchor.as_ref() {
            if !prefix_matches(anchor, session)? || !completeness_prefix_matches(anchor, session)? {
                return Err(invalid("Child broker transcript anchor changed"));
            }
        }
        let receipt = BrokerTerminalReceipt {
            session_id: session.id.clone(),
            created_at: session.created_at,
            parent_session_id: session
                .parent_session_id
                .clone()
                .ok_or_else(|| invalid("Child parent missing"))?,
            root_session_id: session.root_session_id.clone(),
            project_id: session.project_id_meta(),
            activation_run_id: activation_run_id.to_owned(),
            broker_identity: broker_identity.to_owned(),
            parent_mailbox: parent_mailbox.to_owned(),
            broker_correlation_id: broker_correlation_id.to_owned(),
            message_ids: message_ids.to_vec(),
            message_count: session.messages.len(),
            messages_sha256: digest_messages(&session.messages)?,
            terminal_status: session
                .last_run_status()
                .ok_or_else(|| invalid("Child terminal status missing"))?,
            terminal_error,
            tool_yield_call_id: proof.map(|proof| proof.call_id.clone()),
            parent_question_request: (session.last_run_status().as_deref() == Some("suspended"))
                .then(|| session.metadata.get(PARENT_QUESTION_REQUEST_KEY).cloned())
                .flatten(),
            required_execution_epoch: completeness.map(HostTerminalCompleteness::execution_epoch),
            terminal_completeness: completeness.map(|host| host.proof.clone()),
            committed: false,
        };
        if !completeness_prefix_matches(&receipt, session)? {
            return Err(invalid("Host terminal completeness proof changed"));
        }
        if let Some(existing) = ledger.receipts.iter().find(|item| {
            item.created_at == receipt.created_at
                && item.activation_run_id == receipt.activation_run_id
        }) {
            if existing != &receipt
                && !(existing.committed && {
                    let mut committed = receipt.clone();
                    committed.committed = true;
                    existing == &committed
                })
            {
                return Err(invalid("conflicting broker receipt for Child Run"));
            }
            return Ok(());
        }
        if ledger.receipts.len() >= MAX_RECEIPTS {
            return Err(invalid("too many unconfirmed Child broker receipts"));
        }
        ledger.receipts.push(receipt);
        write_ledger(&dir, &ledger).await
    }

    /// Read back the canonical Session under the same cross-process lock and
    /// promote a prepared receipt only when its exact reply/status survived.
    pub async fn commit_broker_terminal_receipt(
        &self,
        session: &Session,
        activation_run_id: &str,
    ) -> io::Result<BrokerTerminalReceipt> {
        self.commit_terminal_receipt(session, activation_run_id, None)
            .await
    }

    /// Commit only the exact proof retained by the live Host after Event drain.
    /// A cold recovery may re-ACK a committed proof but cannot mint this commit.
    pub async fn commit_broker_terminal_receipt_with_completeness(
        &self,
        session: &Session,
        activation_run_id: &str,
        completeness: &HostTerminalCompleteness,
    ) -> io::Result<BrokerTerminalReceipt> {
        self.commit_terminal_receipt(session, activation_run_id, Some(completeness))
            .await
    }

    async fn commit_terminal_receipt(
        &self,
        session: &Session,
        activation_run_id: &str,
        completeness: Option<&HostTerminalCompleteness>,
    ) -> io::Result<BrokerTerminalReceipt> {
        let _guard = self
            .acquire_session_write_lock(&session.id, SaveKind::Runtime)
            .await?;
        let durable = self
            .load_session_unlocked(&session.id)
            .await?
            .ok_or_else(|| invalid("canonical Child missing at broker confirmation"))?;
        let dir = self.broker_receipt_dir(&session.id).await?;
        let mut ledger = read_ledger(&dir).await?;
        let receipt = ledger
            .receipts
            .iter_mut()
            .find(|item| {
                item.created_at == session.created_at && item.activation_run_id == activation_run_id
            })
            .ok_or_else(|| invalid("prepared broker receipt missing"))?;
        if receipt.terminal_completeness.as_ref() != completeness.map(|host| &host.proof)
            || receipt.required_execution_epoch
                != completeness.map(HostTerminalCompleteness::execution_epoch)
        {
            return Err(invalid(
                "live Host terminal completeness proof missing or changed",
            ));
        }
        let terminal_checkpoint_matches = durable.last_run_status().as_deref()
            == Some(receipt.terminal_status.as_str())
            && durable.last_run_error() == receipt.terminal_error;
        // Keep public status non-terminal throughout the transcript/proof gap.
        // Only a live caller carrying the same exact pending envelope and the
        // intended terminal status may confirm this staged canonical save.
        let staged_checkpoint_matches = staged_terminal_matches(receipt, &durable)
            && session.last_run_status().as_deref() == Some(receipt.terminal_status.as_str())
            && session.last_run_error() == receipt.terminal_error
            && pending_completion_matches(receipt, session)
            && durable.metadata.get(COMPLETION_SOURCE_KEY)
                == session.metadata.get(COMPLETION_SOURCE_KEY);
        if !identity_matches(receipt, session)
            || !prefix_matches(receipt, &durable)?
            || !completeness_prefix_matches(receipt, &durable)?
            || !(terminal_checkpoint_matches || staged_checkpoint_matches)
        {
            return Err(invalid(
                "Child broker terminal was not canonically checkpointed",
            ));
        }
        let must_publish = !receipt.committed;
        if must_publish {
            receipt.committed = true;
        }
        let confirmed = receipt.clone();
        if must_publish {
            write_ledger(&dir, &ledger).await?;
        }
        // ACK eligibility includes durable ledger readback while the same
        // cross-process lock still excludes ordinary Session writers.
        if !read_ledger(&dir)
            .await?
            .receipts
            .iter()
            .any(|receipt| receipt == &confirmed)
        {
            return Err(invalid("committed Host broker receipt readback changed"));
        }
        Ok(confirmed)
    }

    /// Publish only the pending completion observed by this caller. A concurrent
    /// successor must never receive an older Run's status/source, even if its
    /// transcript still extends the committed receipt prefix.
    pub async fn publish_broker_terminal_source(
        &self,
        receipt: &BrokerTerminalReceipt,
        expected: &Session,
        published_source: &str,
    ) -> io::Result<Session> {
        if !receipt.committed || published_source.len() > 4096 {
            return Err(invalid(
                "broker terminal source requires a committed bounded receipt",
            ));
        }
        let published: serde_json::Value = serde_json::from_str(published_source)
            .map_err(|_| invalid("invalid published broker terminal source"))?;
        let published_identity: PendingCompletionIdentity = serde_json::from_str(published_source)
            .map_err(|_| invalid("invalid published broker terminal source identity"))?;
        if !completion_identity_matches(receipt, &published_identity) {
            return Err(invalid(
                "published broker terminal source does not match receipt",
            ));
        }
        let mut published_expected = expected.clone();
        published_expected
            .metadata
            .insert(COMPLETION_SOURCE_KEY.into(), published_source.into());
        published_expected.set_last_run_status(&receipt.terminal_status);
        if let Some(error) = receipt.terminal_error.as_ref() {
            published_expected.set_last_run_error(error);
        } else {
            published_expected.clear_last_run_error();
        }
        if expected
            .metadata
            .get(COMPLETION_SOURCE_KEY)
            .map(String::as_str)
            != Some(published_source)
        {
            if !staged_terminal_matches(receipt, expected) {
                return Err(invalid(
                    "broker terminal source is not the expected pending Run",
                ));
            }
            let pending: serde_json::Value = serde_json::from_str(
                expected
                    .metadata
                    .get(COMPLETION_SOURCE_KEY)
                    .expect("validated pending source"),
            )
            .map_err(|_| invalid("invalid pending broker terminal source"))?;
            if pending.get("pending_confirmation") != Some(&published) {
                return Err(invalid(
                    "broker terminal published source differs from pending source",
                ));
            }
        } else if expected.last_run_status().as_deref() != Some(receipt.terminal_status.as_str())
            || expected.last_run_error() != receipt.terminal_error
        {
            return Err(invalid("published broker terminal source status changed"));
        }

        let started = Instant::now();
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let writer = self
            .acquire_session_write_lock(&receipt.session_id, SaveKind::Full)
            .await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, writer);
        let current = self
            .load_session_unlocked(&receipt.session_id)
            .await?
            .ok_or_else(|| invalid("canonical Child missing at broker source publication"))?;
        let dir = self.broker_receipt_dir(&receipt.session_id).await?;
        let ledger = read_ledger(&dir).await?;
        if !ledger
            .receipts
            .iter()
            .chain(ledger.acknowledged_anchor.iter())
            .any(|item| item == receipt && item.committed)
            || !identity_matches(receipt, &current)
            || !prefix_matches(receipt, &current)?
            || !completeness_prefix_matches(receipt, &current)?
        {
            return Err(invalid(
                "broker terminal publication lost its exact committed receipt",
            ));
        }
        let canonical_value = serde_json::to_value(&current)
            .map_err(|_| invalid("invalid canonical broker terminal snapshot"))?;
        let published_value = serde_json::to_value(&published_expected)
            .map_err(|_| invalid("invalid expected broker terminal snapshot"))?;
        // A duplicate reconciler may observe exactly the first reconciler's
        // publication. Any other intervening runtime/input/source change fails.
        if canonical_value == published_value {
            return Ok(current);
        }
        if canonical_value
            != serde_json::to_value(expected)
                .map_err(|_| invalid("invalid expected pending broker terminal snapshot"))?
            || !staged_terminal_matches(receipt, &current)
        {
            return Err(invalid(
                "broker terminal source snapshot changed before publication",
            ));
        }
        self.save_session_after_lock(&published_expected, started, &guards, None)
            .await?;
        let confirmed = self
            .load_session_unlocked(&receipt.session_id)
            .await?
            .ok_or_else(|| invalid("published broker terminal source disappeared"))?;
        if serde_json::to_value(&confirmed)
            .map_err(|_| invalid("invalid broker terminal publication readback"))?
            != published_value
        {
            return Err(invalid(
                "broker terminal source publication readback changed",
            ));
        }
        Ok(confirmed)
    }

    /// On reconnect, return only receipts whose Host transcript proof still
    /// matches. Only a legacy prepared receipt may be promoted after a crash
    /// between final save and ACK. Strict receipts require an earlier explicit
    /// live commit. Any uncertain old Run blocks a successor from consuming
    /// its mailbox, rather than silently stranding that Run's Outcome.
    pub async fn recover_broker_terminal_receipts(
        &self,
        session: &Session,
    ) -> io::Result<Vec<BrokerTerminalReceipt>> {
        let _guard = self
            .acquire_session_write_lock(&session.id, SaveKind::Runtime)
            .await?;
        let durable = self
            .load_session_unlocked(&session.id)
            .await?
            .ok_or_else(|| invalid("canonical Child missing at broker recovery"))?;
        let dir = self.broker_receipt_dir(&session.id).await?;
        let mut ledger = read_ledger(&dir).await?;
        if let Some(anchor) = ledger.acknowledged_anchor.as_ref() {
            if !prefix_matches(anchor, &durable)? || !completeness_prefix_matches(anchor, &durable)?
            {
                return Err(invalid("ACKed Child broker transcript anchor changed"));
            }
        }
        let mut changed = false;
        for receipt in &mut ledger.receipts {
            if !identity_matches(receipt, session)
                || !prefix_matches(receipt, &durable)?
                || !completeness_prefix_matches(receipt, &durable)?
            {
                return Err(invalid("unverified old Child broker receipt"));
            }
            if !receipt.committed {
                if receipt.required_execution_epoch.is_some() {
                    return Err(invalid(
                        "strict Child broker completeness was not live committed",
                    ));
                }
                if durable.last_run_status().as_deref() != Some(receipt.terminal_status.as_str())
                    || durable.last_run_error() != receipt.terminal_error
                {
                    return Err(invalid("old Child broker terminal status is unverified"));
                }
                receipt.committed = true;
                changed = true;
            }
        }
        if changed {
            write_ledger(&dir, &ledger).await?;
        }
        Ok(ledger.receipts)
    }

    /// Remove a receipt only after a correlated broker ACK result for every
    /// listed MsgId. If this write fails, a later retry re-ACKs idempotently.
    pub async fn clear_acknowledged_broker_terminal_receipt(
        &self,
        session_id: &str,
        created_at: DateTime<Utc>,
        activation_run_id: &str,
        parent_mailbox: &str,
    ) -> io::Result<()> {
        if !valid_parent_mailbox(parent_mailbox) {
            return Err(invalid("invalid parent mailbox for broker receipt ACK"));
        }
        let _guard = self
            .acquire_session_write_lock(session_id, SaveKind::Runtime)
            .await?;
        let dir = self.broker_receipt_dir(session_id).await?;
        let mut ledger = read_ledger(&dir).await?;
        let Some(index) = ledger.receipts.iter().position(|item| {
            item.session_id == session_id
                && item.created_at == created_at
                && item.activation_run_id == activation_run_id
                && item.committed
        }) else {
            return Ok(());
        };
        if ledger.receipts[index].parent_mailbox != parent_mailbox {
            return Err(invalid("broker receipt ACK parent mailbox changed"));
        }
        let receipt = ledger.receipts.remove(index);
        if ledger
            .acknowledged_anchor
            .as_ref()
            .is_none_or(|anchor| anchor.message_count < receipt.message_count)
        {
            ledger.acknowledged_anchor = Some(receipt);
        }
        write_ledger(&dir, &ledger).await?;
        Ok(())
    }

    pub(super) async fn reject_rewriting_broker_receipts(
        &self,
        candidate: &Session,
        durable: &Session,
        dir: &Path,
        answer_permit: Option<&ParentQuestion>,
    ) -> io::Result<()> {
        let ledger = read_ledger(dir).await?;
        for receipt in ledger
            .receipts
            .iter()
            .chain(ledger.acknowledged_anchor.iter())
        {
            // A failed strict proof publication must let the SDK persist its
            // conservative error projection instead of leaving public success
            // behind. This never grants ACK: the receipt remains uncommitted,
            // and both exact transcript prefixes are still protected below.
            let strict_failure_projection = receipt.required_execution_epoch.is_some()
                && candidate.last_run_status().as_deref() == Some("error")
                && candidate
                    .last_run_error()
                    .is_some_and(|error| !error.is_empty());
            if !identity_matches(receipt, durable)
                || !prefix_matches(receipt, candidate)?
                || !completeness_prefix_matches(receipt, candidate)?
                || (!receipt.committed
                    && !strict_failure_projection
                    && !staged_terminal_matches(receipt, candidate)
                    && (candidate.last_run_status().as_deref()
                        != Some(receipt.terminal_status.as_str())
                        || candidate.last_run_error() != receipt.terminal_error))
            {
                return Err(invalid(
                    "ordinary Session save would rewrite Host broker receipt",
                ));
            }
            // A Child-local resolution marker is not authority to rewrite an
            // ACKed transcript. While the durable prefix is still the original
            // Worker result, only the dedicated parent-answer CAS below may
            // change it. Once that CAS has saved the answer, ordinary writers
            // may preserve the exact durable bytes but may not change them.
            let candidate_prefix = digest_messages(&candidate.messages[..receipt.message_count])?;
            if durable.messages.len() < receipt.message_count {
                if candidate_prefix != receipt.messages_sha256 {
                    return Err(invalid(
                        "uncommitted broker transcript prefix was rewritten",
                    ));
                }
            } else {
                let durable_prefix = digest_messages(&durable.messages[..receipt.message_count])?;
                if durable_prefix != candidate_prefix
                    && (durable_prefix != receipt.messages_sha256
                        || !answer_permit.is_some_and(|question| {
                            receipt.terminal_status == "suspended"
                                && receipt.parent_question_request.as_deref()
                                    == candidate
                                        .metadata
                                        .get(PARENT_QUESTION_REQUEST_KEY)
                                        .map(String::as_str)
                                && ParentQuestionResolution::from_orphan_child(
                                    candidate,
                                    &question.id,
                                )
                                .is_some_and(|resolution| {
                                    resolution.request == *question
                                        && matches!(
                                            resolution.outcome,
                                            ParentQuestionOutcome::Answer { .. }
                                        )
                                })
                        }))
                {
                    return Err(invalid("broker transcript answer requires parent CAS"));
                }
            }
            if receipt.terminal_status == "suspended" {
                if candidate.metadata.get(PARENT_QUESTION_REQUEST_KEY)
                    != receipt.parent_question_request.as_ref()
                {
                    return Err(invalid(
                        "ordinary Session save would change the direct-parent question",
                    ));
                }
                let question = durable
                    .metadata
                    .get(PARENT_QUESTION_REQUEST_KEY)
                    .and_then(|raw| serde_json::from_str::<ParentQuestion>(raw).ok());
                if let Some(question) = question {
                    let previous =
                        ParentQuestionResolution::from_orphan_child(durable, &question.id);
                    let next = ParentQuestionResolution::from_orphan_child(candidate, &question.id);
                    // Once the parent answer is saved, no ordinary writer may
                    // revert or change it. Before that transition, only the
                    // original pending question can accept its typed answer.
                    if previous.as_ref().is_some_and(|resolution| {
                        matches!(&resolution.outcome, ParentQuestionOutcome::Answer { .. })
                    }) && next != previous
                        || next.as_ref().is_some_and(|resolution| {
                            matches!(&resolution.outcome, ParentQuestionOutcome::Answer { .. })
                        }) && previous.is_none()
                            && ParentQuestion::for_orphan_pending(durable).as_ref()
                                != Some(&question)
                    {
                        return Err(invalid(
                            "ordinary Session save would change a direct-parent answer",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn reject_broker_receipts_without_main(&self, dir: &Path) -> io::Result<()> {
        let ledger = read_ledger(dir).await?;
        if !ledger.receipts.is_empty() || ledger.acknowledged_anchor.is_some() {
            return Err(invalid(
                "canonical Child transcript missing while Host broker receipt exists",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        FunctionCall, PendingQuestionSource, Storage, ToolCall, PARENT_QUESTION_RESOLUTION_KEY,
    };

    const TEST_PARENT_MAILBOX: &str = "p-broker-receipt-child";

    async fn fixture() -> io::Result<(tempfile::TempDir, SessionStoreV2, Session)> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let root = Session::new("broker-receipt-root", "model");
        store.save_session(&root).await?;
        let mut child = Session::new_child_of("broker-receipt-child", &root, "model", "task");
        child.add_message(Message::user("work"));
        store.save_session(&child).await?;
        Ok((home, store, child))
    }

    async fn completed_receipt(store: &SessionStoreV2, child: &Session) -> io::Result<Session> {
        let mut completed = child.clone();
        completed.add_message(Message::assistant("finished", None));
        completed.set_last_run_status("completed");
        store
            .prepare_broker_terminal_receipt(
                &completed,
                "recovery-run",
                &uuid::Uuid::new_v4().to_string(),
                TEST_PARENT_MAILBOX,
                "recovery-correlation",
                &["recovery-outcome".into()],
            )
            .await?;
        store.save_session(&completed).await?;
        Ok(completed)
    }

    fn strict_completed(
        child: &Session,
        seq: u64,
    ) -> io::Result<(Session, HostTerminalCompleteness)> {
        let mut completed = child.clone();
        completed.add_message(Message::assistant("strict finished", None));
        completed.set_last_run_status("completed");
        let proof = HostTerminalCompleteness::from_verified_host(&completed, "strict-run", 7, seq)?;
        Ok((completed, proof))
    }

    async fn prepare_strict(
        store: &SessionStoreV2,
        completed: &Session,
        proof: &HostTerminalCompleteness,
    ) -> io::Result<()> {
        store
            .prepare_broker_terminal_receipt_with_completeness(
                completed,
                BrokerTerminalRoute {
                    activation_run_id: "strict-run",
                    broker_identity: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    parent_mailbox: TEST_PARENT_MAILBOX,
                    broker_correlation_id: "strict-correlation",
                    message_ids: &["strict-event".into(), "strict-outcome".into()],
                },
                proof,
            )
            .await
    }

    fn pending_terminal(completed: &Session) -> (Session, Session, String) {
        let source = serde_json::json!({
            "activation_run_id": "strict-run",
            "child_session_id": completed.id,
            "child_created_at": completed.created_at,
            "parent_session_id": completed.parent_session_id,
            "project_id": completed.project_id_meta(),
            "status": completed.last_run_status(),
            "error_sha256": completed.last_run_error().map(|error| {
                format!("{:x}", Sha256::digest(serde_json::to_vec(&error).unwrap()))
            }),
        });
        let published = serde_json::to_string(&source).unwrap();
        let mut intended = completed.clone();
        intended.metadata.insert(
            COMPLETION_SOURCE_KEY.into(),
            serde_json::to_string(&serde_json::json!({"pending_confirmation": source})).unwrap(),
        );
        let mut staged = intended.clone();
        staged.set_last_run_status("running");
        staged.clear_last_run_error();
        (staged, intended, published)
    }

    #[tokio::test]
    async fn staged_running_receipt_requires_live_commit_before_source_publication(
    ) -> io::Result<()> {
        for strict in [false, true] {
            let (home, store, child) = fixture().await?;
            let (completed, proof) = strict_completed(&child, 2)?;
            if strict {
                prepare_strict(&store, &completed, &proof).await?;
            } else {
                store
                    .prepare_broker_terminal_receipt(
                        &completed,
                        "strict-run",
                        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        TEST_PARENT_MAILBOX,
                        "strict-correlation",
                        &["strict-outcome".into()],
                    )
                    .await?;
            }
            let (staged, intended, published) = pending_terminal(&completed);
            store.save_session(&staged).await?;
            let expected = store.load_session(&staged.id).await?.unwrap();
            assert_eq!(expected.last_run_status().as_deref(), Some("running"));
            drop(store);
            let store = SessionStoreV2::new(home.path().into()).await?;
            assert!(store
                .recover_broker_terminal_receipts(&expected)
                .await
                .is_err());

            let mut mismatched = intended.clone();
            mismatched
                .metadata
                .get_mut(COMPLETION_SOURCE_KEY)
                .unwrap()
                .push(' ');
            let wrong = if strict {
                store
                    .commit_broker_terminal_receipt_with_completeness(
                        &mismatched,
                        "strict-run",
                        &proof,
                    )
                    .await
            } else {
                store
                    .commit_broker_terminal_receipt(&mismatched, "strict-run")
                    .await
            };
            assert!(wrong.is_err());
            let committed = if strict {
                store
                    .commit_broker_terminal_receipt_with_completeness(
                        &intended,
                        "strict-run",
                        &proof,
                    )
                    .await?
            } else {
                store
                    .commit_broker_terminal_receipt(&intended, "strict-run")
                    .await?
            };
            assert_eq!(
                store
                    .load_session(&staged.id)
                    .await?
                    .unwrap()
                    .last_run_status()
                    .as_deref(),
                Some("running")
            );
            assert_eq!(
                store.recover_broker_terminal_receipts(&expected).await?,
                vec![committed.clone()]
            );
            let published_session = store
                .publish_broker_terminal_source(&committed, &expected, &published)
                .await?;
            assert_eq!(
                published_session.last_run_status().as_deref(),
                Some("completed")
            );
            assert_eq!(
                published_session.metadata.get(COMPLETION_SOURCE_KEY),
                Some(&published)
            );
            // Lost publication/ACK responses may repeat the exact operation.
            let duplicate = store
                .publish_broker_terminal_source(&committed, &expected, &published)
                .await?;
            assert_eq!(
                serde_json::to_value(&duplicate).unwrap(),
                serde_json::to_value(&published_session).unwrap()
            );
            store
                .clear_acknowledged_broker_terminal_receipt(
                    &completed.id,
                    completed.created_at,
                    "strict-run",
                    TEST_PARENT_MAILBOX,
                )
                .await?;
            assert!(store
                .publish_broker_terminal_source(&committed, &expected, &published)
                .await
                .is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn staged_running_receipt_rejects_wrong_pending_identity_run_status_or_shape(
    ) -> io::Result<()> {
        let (_home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 2)?;
        prepare_strict(&store, &completed, &proof).await?;
        let (staged, _, _) = pending_terminal(&completed);
        for field in [
            "activation_run_id",
            "child_session_id",
            "child_created_at",
            "parent_session_id",
            "project_id",
            "status",
            "error_sha256",
            "shape",
        ] {
            let mut wrong = staged.clone();
            let mut raw: serde_json::Value =
                serde_json::from_str(wrong.metadata.get(COMPLETION_SOURCE_KEY).unwrap()).unwrap();
            if field == "shape" {
                raw["unexpected"] = true.into();
            } else if field == "child_created_at" {
                raw["pending_confirmation"][field] = "2000-01-01T00:00:00Z".into();
            } else {
                raw["pending_confirmation"][field] = "wrong".into();
            }
            wrong.metadata.insert(
                COMPLETION_SOURCE_KEY.into(),
                serde_json::to_string(&raw).unwrap(),
            );
            assert!(store.save_session(&wrong).await.is_err(), "{field}");
        }
        let mut wrong = staged.clone();
        wrong.set_last_run_error("running cannot carry the terminal error");
        assert!(store.save_session(&wrong).await.is_err());
        store.save_session(&staged).await?;
        Ok(())
    }

    #[tokio::test]
    async fn stale_terminal_source_publication_cannot_overwrite_successor_snapshot(
    ) -> io::Result<()> {
        for replace_source in [false, true] {
            let (_home, store, child) = fixture().await?;
            let (completed, proof) = strict_completed(&child, 2)?;
            prepare_strict(&store, &completed, &proof).await?;
            let (staged, intended, published) = pending_terminal(&completed);
            store.save_session(&staged).await?;
            let committed = store
                .commit_broker_terminal_receipt_with_completeness(&intended, "strict-run", &proof)
                .await?;
            let expected = store.load_session(&staged.id).await?.unwrap();
            let mut successor = expected.clone();
            successor.add_message(Message::user("successor input"));
            if replace_source {
                successor
                    .metadata
                    .insert(COMPLETION_SOURCE_KEY.into(), "successor source".into());
                successor.set_last_run_status("error");
                successor.set_last_run_error("successor error");
            }
            store.save_session(&successor).await?;
            let before = store.load_session(&successor.id).await?.unwrap();
            assert!(store
                .publish_broker_terminal_source(&committed, &expected, &published)
                .await
                .is_err());
            let after = store.load_session(&successor.id).await?.unwrap();
            assert_eq!(
                serde_json::to_value(before).unwrap(),
                serde_json::to_value(after).unwrap()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn strict_prepared_receipt_never_cold_promotes_even_after_exact_checkpoint(
    ) -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 3)?;
        prepare_strict(&store, &completed, &proof).await?;
        // A matching proposed transcript is not evidence of a canonical save.
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof,)
            .await
            .is_err());
        store.save_session(&completed).await?;
        // Neither the legacy commit port nor a cold status/length match can
        // replace the live Host's exact completeness commit.
        assert!(store
            .commit_broker_terminal_receipt(&completed, "strict-run")
            .await
            .is_err());
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&completed)
            .await
            .is_err());
        let dir = reopened.broker_receipt_dir(&completed.id).await?;
        assert!(!read_ledger(&dir).await?.receipts[0].committed);
        Ok(())
    }

    #[tokio::test]
    async fn strict_proof_prepare_and_commit_write_failures_fail_closed() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 2)?;
        let dir = store.broker_receipt_dir(&child.id).await?;
        RECEIPT_WRITE_FAILURES.lock().unwrap().insert(dir.clone());
        assert!(prepare_strict(&store, &completed, &proof).await.is_err());
        assert!(read_ledger(&dir).await?.receipts.is_empty());
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof,)
            .await
            .is_err());

        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        RECEIPT_WRITE_FAILURES.lock().unwrap().insert(dir.clone());
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof,)
            .await
            .is_err());
        assert!(!read_ledger(&dir).await?.receipts[0].committed);
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&completed)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn strict_uncommitted_receipt_allows_only_fail_closed_error_projection() -> io::Result<()>
    {
        let (home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 2)?;
        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        let dir = store.broker_receipt_dir(&completed.id).await?;
        RECEIPT_WRITE_FAILURES.lock().unwrap().insert(dir.clone());
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof)
            .await
            .is_err());

        let mut failed = completed.clone();
        failed.set_last_run_status("error");
        assert!(store.save_session(&failed).await.is_err());
        failed.set_last_run_error("Host strict proof publication failed");
        let mut rewritten = failed.clone();
        rewritten.messages[0].content = "changed work".into();
        assert!(store.save_session(&rewritten).await.is_err());
        store.save_session(&failed).await?;
        let canonical = store.load_session(&failed.id).await?.unwrap();
        assert_eq!(
            digest_messages(&canonical.messages)?,
            digest_messages(&completed.messages)?
        );
        assert_eq!(canonical.last_run_status().as_deref(), Some("error"));
        assert!(!read_ledger(&dir).await?.receipts[0].committed);
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&failed, "strict-run", &proof)
            .await
            .is_err());
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&failed)
            .await
            .is_err());
        assert_eq!(
            reopened
                .load_session(&failed.id)
                .await?
                .unwrap()
                .last_run_status()
                .as_deref(),
            Some("error")
        );
        Ok(())
    }

    #[tokio::test]
    async fn strict_committed_proof_cold_reopens_exactly_and_zero_frontier_is_valid(
    ) -> io::Result<()> {
        for seq in [0, 4] {
            let (home, store, child) = fixture().await?;
            let (completed, proof) = strict_completed(&child, seq)?;
            prepare_strict(&store, &completed, &proof).await?;
            store.save_session(&completed).await?;
            let committed = store
                .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof)
                .await?;
            assert_eq!(committed.required_execution_epoch, Some(7));
            assert_eq!(
                committed
                    .terminal_completeness
                    .as_ref()
                    .unwrap()
                    .contiguous_applied_seq,
                seq
            );
            assert_eq!(
                store
                    .commit_broker_terminal_receipt_with_completeness(
                        &completed,
                        "strict-run",
                        &proof,
                    )
                    .await?,
                committed
            );
            drop(store);
            let reopened = SessionStoreV2::new(home.path().into()).await?;
            assert_eq!(
                reopened
                    .recover_broker_terminal_receipts(&completed)
                    .await?,
                vec![committed.clone()]
            );
            // A lost ACK result can safely replay precisely the same MsgIds.
            assert_eq!(
                reopened
                    .recover_broker_terminal_receipts(&completed)
                    .await?,
                vec![committed]
            );
            reopened
                .clear_acknowledged_broker_terminal_receipt(
                    &completed.id,
                    completed.created_at,
                    "strict-run",
                    TEST_PARENT_MAILBOX,
                )
                .await?;
            assert!(reopened
                .recover_broker_terminal_receipts(&completed)
                .await?
                .is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn strict_live_proof_rejects_wrong_identity_run_epoch_and_conflicting_frontier(
    ) -> io::Result<()> {
        let (_home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 2)?;
        assert!(
            HostTerminalCompleteness::from_verified_host(&completed, "strict-run", 0, 0).is_err()
        );
        for field in [
            "id", "birth", "parent", "root", "project", "depth", "run", "digest",
        ] {
            let mut wrong = proof.clone();
            match field {
                "id" => wrong.proof.session_id = "other-child".into(),
                "birth" => wrong.proof.created_at += chrono::Duration::milliseconds(1),
                "parent" => wrong.proof.parent_session_id = "other-parent".into(),
                "root" => wrong.proof.root_session_id = "other-root".into(),
                "project" => wrong.proof.project_id = Some("other-project".into()),
                "depth" => wrong.proof.spawn_depth += 1,
                "run" => wrong.proof.activation_run_id = "other-run".into(),
                "digest" => wrong.proof.messages_sha256 = "other-digest".into(),
                _ => unreachable!(),
            }
            assert!(
                prepare_strict(&store, &completed, &wrong).await.is_err(),
                "{field}"
            );
        }
        prepare_strict(&store, &completed, &proof).await?;
        // Exact duplicate prepare is idempotent; any new claim for this Run is not.
        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        for field in ["epoch", "seq", "run", "birth"] {
            let mut wrong = proof.clone();
            match field {
                "epoch" => wrong.proof.execution_epoch += 1,
                "seq" => wrong.proof.contiguous_applied_seq += 1,
                "run" => wrong.proof.activation_run_id = "other-run".into(),
                "birth" => wrong.proof.created_at += chrono::Duration::milliseconds(1),
                _ => unreachable!(),
            }
            assert!(
                prepare_strict(&store, &completed, &wrong).await.is_err(),
                "{field}"
            );
            assert!(
                store
                    .commit_broker_terminal_receipt_with_completeness(
                        &completed,
                        "strict-run",
                        &wrong,
                    )
                    .await
                    .is_err(),
                "{field}"
            );
        }
        let mut wrong_session = completed.clone();
        wrong_session.root_session_id = "other-root".into();
        assert!(store
            .commit_broker_terminal_receipt_with_completeness(&wrong_session, "strict-run", &proof,)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn strict_cold_recovery_rejects_missing_proof_or_corrupt_identity_run_epoch(
    ) -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 2)?;
        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof)
            .await?;
        let dir = store.broker_receipt_dir(&completed.id).await?;
        let original = fs::read(dir.join(RECEIPTS_FILE)).await?;
        drop(store);
        for field in [
            "missing",
            "marker",
            "version",
            "missing_version",
            "epoch",
            "run",
            "birth",
            "parent",
            "digest",
        ] {
            let mut raw: serde_json::Value = serde_json::from_slice(&original).unwrap();
            let receipt = &mut raw["receipts"][0];
            match field {
                "missing" => {
                    receipt
                        .as_object_mut()
                        .unwrap()
                        .remove("terminal_completeness");
                }
                "marker" => {
                    receipt
                        .as_object_mut()
                        .unwrap()
                        .remove("required_execution_epoch");
                }
                "version" => receipt["terminal_completeness"]["version"] = 2.into(),
                "missing_version" => {
                    receipt["terminal_completeness"]
                        .as_object_mut()
                        .unwrap()
                        .remove("version");
                }
                "epoch" => receipt["terminal_completeness"]["execution_epoch"] = 8.into(),
                "run" => receipt["terminal_completeness"]["activation_run_id"] = "other-run".into(),
                "birth" => {
                    receipt["terminal_completeness"]["created_at"] = "2000-01-01T00:00:00Z".into()
                }
                "parent" => {
                    receipt["terminal_completeness"]["parent_session_id"] = "other-parent".into()
                }
                "digest" => {
                    receipt["terminal_completeness"]["messages_sha256"] = "other-digest".into()
                }
                _ => unreachable!(),
            }
            fs::write(dir.join(RECEIPTS_FILE), serde_json::to_vec(&raw).unwrap()).await?;
            let reopened = SessionStoreV2::new(home.path().into()).await?;
            assert!(
                reopened
                    .recover_broker_terminal_receipts(&completed)
                    .await
                    .is_err(),
                "{field}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn strict_proof_allows_host_suffix_and_later_append_but_rejects_prefix_mutation(
    ) -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let (mut completed, proof) = strict_completed(&child, 1)?;
        completed.add_message(Message::assistant("Host final annotation", None));
        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        let committed = store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof)
            .await?;
        assert!(
            committed.message_count
                > committed
                    .terminal_completeness
                    .as_ref()
                    .unwrap()
                    .message_count
        );
        let mut successor = completed.clone();
        successor.add_message(Message::user("next task"));
        successor.set_last_run_status("running");
        store.save_session(&successor).await?;
        let mut mutated = successor.clone();
        mutated.messages[0].content = "mutated assignment".into();
        assert!(store.save_session(&mutated).await.is_err());
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert_eq!(
            reopened
                .recover_broker_terminal_receipts(&successor)
                .await?,
            vec![committed]
        );
        // Even bypassing ordinary writer protections cannot make a changed
        // same-length canonical prefix eligible for ACK after restart.
        let main = reopened.session_json_path(&mutated.id).await?.unwrap();
        fs::write(&main, compact_main::serialize_main(&mutated)?).await?;
        drop(reopened);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&mutated)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn strict_committed_receipt_requires_canonical_checkpoint_to_remain_present(
    ) -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let (completed, proof) = strict_completed(&child, 1)?;
        prepare_strict(&store, &completed, &proof).await?;
        store.save_session(&completed).await?;
        store
            .commit_broker_terminal_receipt_with_completeness(&completed, "strict-run", &proof)
            .await?;
        fs::remove_file(store.session_json_path(&completed.id).await?.unwrap()).await?;
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&completed)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_receipt_without_strict_fields_retains_checkpoint_promotion() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let completed = completed_receipt(&store, &child).await?;
        let mut failed = completed.clone();
        failed.set_last_run_status("error");
        failed.set_last_run_error("legacy terminal changed");
        assert!(store.save_session(&failed).await.is_err());
        let dir = store.broker_receipt_dir(&completed.id).await?;
        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join(RECEIPTS_FILE)).await?).unwrap();
        assert!(raw["receipts"][0].get("required_execution_epoch").is_none());
        assert!(raw["receipts"][0].get("terminal_completeness").is_none());
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        let recovered = reopened
            .recover_broker_terminal_receipts(&completed)
            .await?;
        assert_eq!(recovered.len(), 1);
        assert!(recovered[0].committed);
        assert_eq!(recovered[0].required_execution_epoch, None);
        assert_eq!(recovered[0].terminal_completeness, None);
        Ok(())
    }

    #[tokio::test]
    async fn startup_scan_finds_unacked_child_without_successor_after_reopen() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let completed = completed_receipt(&store, &child).await?;
        drop(store);

        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let found = reopened
            .discover_unconfirmed_broker_terminal_children()
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, completed.id);
        assert_eq!(
            digest_messages(&found[0].messages)?,
            digest_messages(&completed.messages)?
        );
        let receipts = reopened.recover_broker_terminal_receipts(&found[0]).await?;
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].activation_run_id, "recovery-run");
        reopened
            .clear_acknowledged_broker_terminal_receipt(
                &completed.id,
                completed.created_at,
                "recovery-run",
                TEST_PARENT_MAILBOX,
            )
            .await?;
        assert!(reopened
            .discover_unconfirmed_broker_terminal_children()
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn startup_scan_repairs_stale_index_from_canonical_child() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let completed = completed_receipt(&store, &child).await?;
        let mut index: SessionsIndex =
            serde_json::from_slice(&fs::read(store.index_path()).await?).unwrap();
        index.sessions.remove(&completed.id);
        fs::write(store.index_path(), serde_json::to_vec(&index).unwrap()).await?;
        drop(store);

        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert!(reopened.get_index_entry(&completed.id).await.is_none());
        let found = reopened
            .discover_unconfirmed_broker_terminal_children()
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, completed.id);
        assert!(reopened.get_index_entry(&completed.id).await.is_some());
        assert_eq!(
            reopened
                .recover_broker_terminal_receipts(&found[0])
                .await?
                .len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_scan_rejects_ambiguous_child_and_isolates_forged_receipt_identity(
    ) -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        completed_receipt(&store, &child).await?;
        let duplicate = home
            .path()
            .join("sessions/another-root/children")
            .join(&child.id);
        fs::create_dir_all(&duplicate).await?;
        assert!(store
            .discover_unconfirmed_broker_terminal_children()
            .await
            .is_err());
        fs::remove_dir_all(home.path().join("sessions/another-root")).await?;

        let dir = home
            .path()
            .join("sessions")
            .join(&child.root_session_id)
            .join("children")
            .join(&child.id);
        let mut ledger = read_ledger(&dir).await?;
        ledger.receipts[0].root_session_id = "forged-root".into();
        write_ledger(&dir, &ledger).await?;
        assert!(store
            .discover_unconfirmed_broker_terminal_children()
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn startup_scan_isolates_bad_ledgers_and_recovers_healthy_child() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        completed_receipt(&store, &child).await?;
        let root = store.load_session(&child.root_session_id).await?.unwrap();

        let mut legacy =
            Session::new_child_of("broker-receipt-legacy-child", &root, "model", "task");
        legacy.add_message(Message::user("legacy work"));
        store.save_session(&legacy).await?;
        completed_receipt(&store, &legacy).await?;
        let legacy_path = home
            .path()
            .join("sessions")
            .join(&root.id)
            .join("children")
            .join(&legacy.id)
            .join(RECEIPTS_FILE);
        let mut old: serde_json::Value =
            serde_json::from_slice(&fs::read(&legacy_path).await?).unwrap();
        old["receipts"][0]
            .as_object_mut()
            .unwrap()
            .remove("parent_mailbox");
        fs::write(&legacy_path, serde_json::to_vec(&old).unwrap()).await?;

        let corrupt_path = home
            .path()
            .join("sessions")
            .join(&root.id)
            .join("children")
            .join(&child.id)
            .join(RECEIPTS_FILE);
        fs::write(&corrupt_path, b"{invalid-ledger").await?;

        let mut healthy =
            Session::new_child_of("broker-receipt-healthy-child", &root, "model", "task");
        healthy.add_message(Message::user("healthy work"));
        store.save_session(&healthy).await?;
        let completed = completed_receipt(&store, &healthy).await?;
        drop(store);

        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let found = reopened
            .discover_unconfirmed_broker_terminal_children()
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, completed.id);
        assert_eq!(
            reopened
                .recover_broker_terminal_receipts(&found[0])
                .await?
                .len(),
            1
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn startup_scan_rejects_symlinked_receipt_path() -> io::Result<()> {
        use std::os::unix::fs::symlink;

        let (home, store, child) = fixture().await?;
        completed_receipt(&store, &child).await?;
        let dir = home
            .path()
            .join("sessions")
            .join(&child.root_session_id)
            .join("children")
            .join(&child.id);
        let receipt = dir.join(RECEIPTS_FILE);
        let target = dir.join("moved-receipts.json");
        fs::rename(&receipt, &target).await?;
        symlink(&target, &receipt)?;
        assert!(store
            .discover_unconfirmed_broker_terminal_children()
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn broker_receipt_ack_requires_the_original_parent_mailbox() -> io::Result<()> {
        let (_home, store, child) = fixture().await?;
        let completed = completed_receipt(&store, &child).await?;
        let committed = store
            .commit_broker_terminal_receipt(&completed, "recovery-run")
            .await?;
        assert_eq!(committed.parent_mailbox, TEST_PARENT_MAILBOX);
        assert!(store
            .clear_acknowledged_broker_terminal_receipt(
                &completed.id,
                completed.created_at,
                "recovery-run",
                "other-parent-mailbox",
            )
            .await
            .is_err());
        assert_eq!(
            store
                .recover_broker_terminal_receipts(&completed)
                .await?
                .len(),
            1
        );
        store
            .clear_acknowledged_broker_terminal_receipt(
                &completed.id,
                completed.created_at,
                "recovery-run",
                TEST_PARENT_MAILBOX,
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn tool_yield_receipt_requires_host_proof_and_retains_exact_cold_anchor() -> io::Result<()>
    {
        use bamboo_domain::{FunctionCall, ToolCall};
        let (home, store, mut child) = fixture().await?;
        child.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "question-call".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Task".into(),
                    arguments: "{\"question\":{\"prompt\":\"own question\"}}".into(),
                },
            }]),
        ));
        child.add_message(Message::tool_result_with_status(
            "question-call",
            "exact Host question result",
            true,
        ));
        child.set_last_run_status("completed");
        // An arbitrary metadata flag alone does not change the ordinary API.
        child
            .metadata
            .insert("ticket.worker.question_yield.v1".into(), "true".into());
        let broker = uuid::Uuid::new_v4().to_string();
        let ids = ["question-event".into(), "question-outcome".into()];
        let route = || BrokerTerminalRoute {
            activation_run_id: "question-run",
            broker_identity: &broker,
            parent_mailbox: TEST_PARENT_MAILBOX,
            broker_correlation_id: "question-correlation",
            message_ids: &ids,
        };
        assert!(store
            .prepare_broker_terminal_receipt(
                &child,
                "question-run",
                &broker,
                TEST_PARENT_MAILBOX,
                "question-correlation",
                &ids
            )
            .await
            .is_err());
        let proof = HostToolYield::from_verified_host(&child, "question-run", "question-call")?;
        let mut forged = child.clone();
        forged.messages.last_mut().unwrap().content = "different question".into();
        assert!(store
            .prepare_broker_tool_yield_receipt(&forged, route(), &proof)
            .await
            .is_err());
        store
            .prepare_broker_tool_yield_receipt(&child, route(), &proof)
            .await?;
        assert!(store
            .commit_broker_terminal_receipt(&child, "question-run")
            .await
            .is_err());
        store.save_session(&child).await?;
        let confirmed = store
            .commit_broker_terminal_receipt(&child, "question-run")
            .await?;
        assert_eq!(
            confirmed.tool_yield_call_id.as_deref(),
            Some("question-call")
        );
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await?;
        assert_eq!(
            reopened
                .recover_broker_terminal_receipts(&child)
                .await?
                .len(),
            1
        );
        reopened
            .clear_acknowledged_broker_terminal_receipt(
                &child.id,
                child.created_at,
                "question-run",
                TEST_PARENT_MAILBOX,
            )
            .await?;
        assert!(reopened.save_session(&forged).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_broker_receipt_without_parent_mailbox_fails_closed() -> io::Result<()> {
        let (home, store, child) = fixture().await?;
        let completed = completed_receipt(&store, &child).await?;
        let dir = home
            .path()
            .join("sessions")
            .join(&child.root_session_id)
            .join("children")
            .join(&child.id);
        let path = dir.join(RECEIPTS_FILE);
        let mut old: serde_json::Value = serde_json::from_slice(&fs::read(&path).await?).unwrap();
        old["receipts"][0]
            .as_object_mut()
            .unwrap()
            .remove("parent_mailbox");
        fs::write(&path, serde_json::to_vec(&old).unwrap()).await?;
        drop(store);

        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert!(reopened
            .discover_unconfirmed_broker_terminal_children()
            .await?
            .is_empty());
        assert!(reopened
            .recover_broker_terminal_receipts(&completed)
            .await
            .is_err());
        assert!(reopened
            .clear_acknowledged_broker_terminal_receipt(
                &completed.id,
                completed.created_at,
                "recovery-run",
                TEST_PARENT_MAILBOX,
            )
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn prepared_terminal_requires_exact_checkpoint_and_anchors_acked_transcript(
    ) -> io::Result<()> {
        let (home, store, stale) = fixture().await?;
        let mut completed = stale.clone();
        completed.add_message(Message::assistant("finished", None));
        completed.set_last_run_status("completed");
        let broker_id = uuid::Uuid::new_v4().to_string();
        store
            .prepare_broker_terminal_receipt(
                &completed,
                "host-run-1",
                &broker_id,
                TEST_PARENT_MAILBOX,
                "broker-run-1",
                &["event-1".into(), "outcome-1".into()],
            )
            .await?;
        assert!(store
            .commit_broker_terminal_receipt(&completed, "host-run-1")
            .await
            .is_err());
        assert!(store.save_session(&stale).await.is_err());

        store.save_session(&completed).await?;
        let committed = store
            .commit_broker_terminal_receipt(&completed, "host-run-1")
            .await?;
        assert_eq!(committed.broker_identity, broker_id);
        assert_eq!(committed.message_ids, ["event-1", "outcome-1"]);

        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let recovered = reopened
            .recover_broker_terminal_receipts(&completed)
            .await?;
        assert_eq!(recovered.len(), 1);
        reopened
            .clear_acknowledged_broker_terminal_receipt(
                &completed.id,
                completed.created_at,
                "host-run-1",
                TEST_PARENT_MAILBOX,
            )
            .await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&completed)
            .await?
            .is_empty());
        assert!(reopened.save_session(&stale).await.is_err());
        let mut successor = completed.clone();
        successor.add_message(Message::user("next"));
        successor.set_last_run_status("running");
        reopened.save_session(&successor).await?;
        let main = reopened.session_json_path(&successor.id).await?.unwrap();
        fs::remove_file(main).await?;
        assert!(reopened.save_session(&stale).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn failed_final_save_keeps_old_run_unconfirmed_after_reopen() -> io::Result<()> {
        let (home, store, stale) = fixture().await?;
        let mut completed = stale.clone();
        completed.add_message(Message::assistant("answer", None));
        completed.set_last_run_status("completed");
        store
            .prepare_broker_terminal_receipt(
                &completed,
                "host-run-2",
                &uuid::Uuid::new_v4().to_string(),
                TEST_PARENT_MAILBOX,
                "broker-run-2",
                &["outcome-2".into()],
            )
            .await?;
        drop(store);
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&stale)
            .await
            .is_err());
        assert!(reopened
            .commit_broker_terminal_receipt(&completed, "host-run-2")
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn acked_suspended_question_allows_only_its_typed_parent_answer() -> io::Result<()> {
        let (home, store, mut child) = fixture().await?;
        let mut parent = store
            .load_session("broker-receipt-root")
            .await?
            .expect("parent exists");
        child.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "question-call".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "AskUserQuestion".into(),
                    arguments: "{}".into(),
                },
            }]),
        ));
        child.add_message(Message::tool_result_with_status(
            "question-call",
            "Clarification needed: Which option?",
            true,
        ));
        child.set_pending_question_with_source(
            "question-call".into(),
            "AskUserQuestion".into(),
            "Which option?".into(),
            vec!["A".into(), "B".into()],
            false,
            PendingQuestionSource::DirectParent,
        );
        child.metadata.insert(
            "runtime.suspend_reason".into(),
            "awaiting_clarification".into(),
        );
        let question = ParentQuestion::issue_at(&parent, &child, Utc::now()).unwrap();
        child.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&question).unwrap(),
        );
        child.set_last_run_status("suspended");
        store
            .prepare_broker_terminal_receipt(
                &child,
                "question-run",
                &uuid::Uuid::new_v4().to_string(),
                TEST_PARENT_MAILBOX,
                "question-correlation",
                &["question-outcome".into()],
            )
            .await?;
        store.save_session(&child).await?;
        store
            .commit_broker_terminal_receipt(&child, "question-run")
            .await?;
        store
            .clear_acknowledged_broker_terminal_receipt(
                &child.id,
                child.created_at,
                "question-run",
                TEST_PARENT_MAILBOX,
            )
            .await?;
        assert!(store
            .answer_parent_question(&question, "A", |_| {})
            .await?
            .is_none());
        parent.add_message(question.envelope().to_provider_message().unwrap());
        store.save_session(&parent).await?;

        let mut unproven = child.clone();
        unproven.messages.last_mut().unwrap().content = "A".into();
        assert!(store.save_session(&unproven).await.is_err());

        let mut forged_parent = parent.clone();
        forged_parent.created_at += chrono::Duration::milliseconds(1);
        let forged_question =
            ParentQuestion::issue_at(&forged_parent, &child, question.issued_at).unwrap();
        let mut forged = unproven.clone();
        forged.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&forged_question).unwrap(),
        );
        forged.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            serde_json::to_string(
                &ParentQuestionResolution::answered(&forged_question, Utc::now(), "A").unwrap(),
            )
            .unwrap(),
        );
        assert!(store.save_session(&forged).await.is_err());

        let mut answered = unproven;
        let resolution = ParentQuestionResolution::answered(&question, Utc::now(), "A").unwrap();
        answered.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            serde_json::to_string(&resolution).unwrap(),
        );
        assert!(store.save_session(&answered).await.is_err());
        let (answered, wrote) = store
            .answer_parent_question(&question, "A", |_| {})
            .await?
            .expect("durable parent request authorizes the exact answer");
        assert!(wrote);
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert!(reopened
            .recover_broker_terminal_receipts(&answered)
            .await?
            .is_empty());

        assert!(reopened.save_session(&child).await.is_err());
        let mut changed_answer = answered.clone();
        changed_answer.messages.last_mut().unwrap().content = "B".into();
        changed_answer.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            serde_json::to_string(
                &ParentQuestionResolution::answered(&question, Utc::now(), "B").unwrap(),
            )
            .unwrap(),
        );
        assert!(reopened.save_session(&changed_answer).await.is_err());
        let mut changed_history = answered.clone();
        changed_history.messages[0].content = "rewritten assignment".into();
        assert!(reopened.save_session(&changed_history).await.is_err());

        let mut successor = answered;
        successor.add_message(Message::user("continue"));
        successor.set_last_run_status("running");
        reopened.save_session(&successor).await?;
        Ok(())
    }

    #[tokio::test]
    async fn worker_error_and_cancelled_receipts_require_the_exact_durable_error() -> io::Result<()>
    {
        for status in ["error", "cancelled"] {
            let (home, store, stale) = fixture().await?;
            let mut terminal = stale.clone();
            terminal.set_last_run_status(status);
            terminal.set_last_run_error(format!("worker {status}"));
            let broker_id = uuid::Uuid::new_v4().to_string();
            store
                .prepare_broker_terminal_receipt(
                    &terminal,
                    "host-run",
                    &broker_id,
                    TEST_PARENT_MAILBOX,
                    "broker-run",
                    &["outcome".into()],
                )
                .await?;
            assert!(store
                .commit_broker_terminal_receipt(&terminal, "host-run")
                .await
                .is_err());
            let mut wrong_error = terminal.clone();
            wrong_error.set_last_run_error("different failure");
            assert!(store.save_session(&wrong_error).await.is_err());
            store.save_session(&terminal).await?;
            let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
            let receipts = reopened.recover_broker_terminal_receipts(&terminal).await?;
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].terminal_status, status);
            let expected_error = format!("worker {status}");
            assert_eq!(
                receipts[0].terminal_error.as_deref(),
                Some(expected_error.as_str())
            );
        }
        Ok(())
    }
}
