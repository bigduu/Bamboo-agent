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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerTerminalReceipt {
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub parent_session_id: String,
    pub root_session_id: String,
    pub project_id: Option<String>,
    pub activation_run_id: String,
    pub broker_identity: String,
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

fn identity_matches(receipt: &BrokerTerminalReceipt, session: &Session) -> bool {
    session.kind == SessionKind::Child
        && receipt.session_id == session.id
        && receipt.created_at == session.created_at
        && session.parent_session_id.as_deref() == Some(receipt.parent_session_id.as_str())
        && receipt.root_session_id == session.root_session_id
        && receipt.project_id == session.project_id_meta()
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
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|_| invalid("invalid Host broker receipt ledger")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ReceiptLedger::default()),
        Err(error) => Err(error),
    }
}

async fn write_ledger(dir: &Path, ledger: &ReceiptLedger) -> io::Result<()> {
    let bytes = serde_json::to_vec(ledger).map_err(|_| invalid("invalid Host broker receipt"))?;
    durable_atomic_write(&dir.join(RECEIPTS_FILE), &bytes).await
}

impl SessionStoreV2 {
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
        broker_correlation_id: &str,
        message_ids: &[String],
    ) -> io::Result<()> {
        validate_session_id(&session.id)?;
        if session.kind != SessionKind::Child
            || activation_run_id.is_empty()
            || activation_run_id.len() > 128
            || uuid::Uuid::parse_str(broker_identity).is_err()
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
        if session.last_run_status().as_deref() == Some("completed")
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
            if !prefix_matches(anchor, session)? {
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
            broker_correlation_id: broker_correlation_id.to_owned(),
            message_ids: message_ids.to_vec(),
            message_count: session.messages.len(),
            messages_sha256: digest_messages(&session.messages)?,
            terminal_status: session
                .last_run_status()
                .ok_or_else(|| invalid("Child terminal status missing"))?,
            terminal_error,
            parent_question_request: (session.last_run_status().as_deref() == Some("suspended"))
                .then(|| session.metadata.get(PARENT_QUESTION_REQUEST_KEY).cloned())
                .flatten(),
            committed: false,
        };
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
        if !prefix_matches(receipt, &durable)?
            || durable.last_run_status().as_deref() != Some(receipt.terminal_status.as_str())
            || durable.last_run_error() != receipt.terminal_error
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
        Ok(confirmed)
    }

    /// On reconnect, return only receipts whose Host transcript proof still
    /// matches. A prepared receipt may be promoted after a crash between final
    /// save and ACK. Any uncertain old Run blocks a successor from consuming
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
            if !prefix_matches(anchor, &durable)? {
                return Err(invalid("ACKed Child broker transcript anchor changed"));
            }
        }
        let mut changed = false;
        for receipt in &mut ledger.receipts {
            if !identity_matches(receipt, session) || !prefix_matches(receipt, &durable)? {
                return Err(invalid("unverified old Child broker receipt"));
            }
            if !receipt.committed {
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
    ) -> io::Result<()> {
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
            if !identity_matches(receipt, durable)
                || !prefix_matches(receipt, candidate)?
                || (!receipt.committed
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
                "question-correlation",
                &["question-outcome".into()],
            )
            .await?;
        store.save_session(&child).await?;
        store
            .commit_broker_terminal_receipt(&child, "question-run")
            .await?;
        store
            .clear_acknowledged_broker_terminal_receipt(&child.id, child.created_at, "question-run")
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
