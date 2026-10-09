//! Pure loaded-only conversion plan, applied by defaults SDK runner setup.

use bamboo_agent_core::{Message, Role};
use bamboo_skills::runtime_metadata::*;
use bamboo_skills::{
    ActiveWorkflow, DurableWorkflowActivation, SkillActivationDescriptor, SkillActivationSnapshot,
    WorkflowActivationStatus, WorkflowCatalogEntry, WorkflowKind, WorkflowSelection,
    WorkflowStatus, ACTIVE_WORKFLOW_METADATA_KEY, ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
    MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES, WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    WORKFLOW_CONTEXT_CACHE_METADATA_KEY, WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
    WORKFLOW_SELECTION_METADATA_KEY,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

const OBSOLETE_INSTRUCTION_KEYS: &[&str] = &[
    ACTIVE_WORKFLOW_METADATA_KEY,
    ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
    WORKFLOW_SELECTION_METADATA_KEY,
    SKILL_RUNTIME_ACTIVATION_GENERATION_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
    SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
    SKILL_RUNTIME_SELECTED_CATALOG_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,
    SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    SKILL_RUNTIME_SELECTION_TRACE_KEY,
    SKILL_RUNTIME_SELECTION_COUNT_KEY,
    SKILL_RUNTIME_ACTIVATION_ERROR_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
    LAST_LOADED_SKILL_ID_METADATA_KEY,
    LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LAST_RESOURCE_READ_SUMMARY_METADATA_KEY,
    WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
    WORKFLOW_CONTEXT_CACHE_METADATA_KEY,
];

#[derive(Debug, Default)]
pub struct LegacySkillHistoryPlan {
    /// Insert after this original message, once the entire mixed batch closed.
    pub insert_after: Option<usize>,
    pub message: Option<Message>,
    pub remove_metadata: Vec<String>,
    /// Unsupported old forms retain all original messages and metadata.
    pub unsupported: Option<String>,
}

fn unsupported(reason: &str) -> LegacySkillHistoryPlan {
    LegacySkillHistoryPlan {
        unsupported: Some(reason.into()),
        ..Default::default()
    }
}

fn decode<T: DeserializeOwned>(
    metadata: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>, String> {
    metadata
        .get(key)
        .map(|raw| {
            if raw.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
                return Err(format!(
                    "legacy Instruction metadata exceeds 512 KiB: {key}"
                ));
            }
            serde_json::from_str(raw).map_err(|error| format!("invalid {key}: {error}"))
        })
        .transpose()
}

/// Validate original typed and receipt evidence on every invocation, including
/// retries whose deterministic ordinary message already exists. Identity is
/// only deduplication; it never substitutes for the loaded anchor.
pub fn plan_legacy_skill_history(
    messages: &[Message],
    metadata: &HashMap<String, String>,
) -> Result<LegacySkillHistoryPlan, String> {
    let Some(active) = decode::<ActiveWorkflow>(metadata, ACTIVE_WORKFLOW_METADATA_KEY)? else {
        return Ok(unsupported("no typed active workflow"));
    };
    if active.kind != WorkflowKind::Instruction {
        return Ok(unsupported(
            "Orchestration is outside Instruction conversion",
        ));
    }
    if OBSOLETE_INSTRUCTION_KEYS.iter().any(|key| {
        metadata
            .get(*key)
            .is_some_and(|raw| raw.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES)
    }) {
        return Err("populated legacy Instruction metadata exceeds 512 KiB".into());
    }
    if active.status != WorkflowActivationStatus::Active {
        return Ok(unsupported("workflow is not successfully active"));
    }
    let Some(durable) =
        decode::<DurableWorkflowActivation>(metadata, ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)?
    else {
        return Ok(unsupported("no durable loaded snapshot"));
    };
    if durable.active != active
        || durable.snapshot.skills.len() != 1
        || durable.snapshot.catalog_revision == 0
    {
        return Err("active and durable Instruction evidence disagree".into());
    }
    let entry = durable
        .snapshot
        .skills
        .get(&active.id)
        .ok_or("active Instruction is absent from durable snapshot")?;
    if active.id.is_empty()
        || active.revision == 0
        || entry.definition.id != active.id
        || entry.catalog_entry.id != active.id
        || entry.revision != active.revision
        || entry.catalog_entry.revision != active.revision
        || entry.catalog_entry.source != active.source
        || entry.catalog_entry.kind != WorkflowKind::Instruction
        || !entry.catalog_entry.winner
        || entry.catalog_entry.status != WorkflowStatus::Valid
    {
        return Err("durable Instruction identity is inconsistent".into());
    }
    bamboo_domain::validate_schema(&entry.catalog_entry.argument_schema, &active.args)?;
    let Some(pinned) =
        decode::<SkillActivationSnapshot>(metadata, SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)?
    else {
        return Ok(unsupported("no original pinned snapshot"));
    };
    let Some(catalog) =
        decode::<Vec<WorkflowCatalogEntry>>(metadata, SKILL_RUNTIME_SELECTED_CATALOG_KEY)?
    else {
        return Ok(unsupported("no original selected catalog"));
    };
    if pinned.skills.is_empty()
        || pinned.skills.len() > 32
        || pinned.skills.len() != catalog.len()
        || pinned.catalog_revision != durable.snapshot.catalog_revision
        || pinned.selected_skill_mode != durable.snapshot.selected_skill_mode
    {
        return Err("original catalog/snapshot generation or mode disagree".into());
    }
    if catalog
        .iter()
        .any(|entry| entry.kind != WorkflowKind::Instruction)
    {
        return Ok(unsupported("mixed Orchestration pin remains untouched"));
    }
    let mut catalog_ids = BTreeSet::new();
    for candidate in &catalog {
        let original = pinned
            .skills
            .get(&candidate.id)
            .ok_or("catalog candidate is not pinned")?;
        if !catalog_ids.insert(&candidate.id)
            || candidate != &original.catalog_entry
            || original.definition.id != candidate.id
            || original.revision != candidate.revision
            || original.revision == 0
            || !candidate.winner
            || candidate.status != WorkflowStatus::Valid
        {
            return Err("original selected catalog and snapshot disagree".into());
        }
    }
    let original = pinned
        .skills
        .get(&active.id)
        .ok_or("active Instruction was not originally pinned")?;
    if original.definition != entry.definition
        || original.catalog_entry != entry.catalog_entry
        || original.revision != entry.revision
        || original.resources != entry.resources
    {
        return Err("loaded durable payload differs from original pin".into());
    }
    let descriptor = SkillActivationDescriptor {
        catalog_revision: pinned.catalog_revision,
        skill_revisions: pinned
            .skills
            .iter()
            .map(|(id, entry)| (id.clone(), entry.revision))
            .collect(),
        selected_skill_mode: pinned.selected_skill_mode.clone(),
    };
    if !validate_pinned_activation_metadata(metadata, Some(&descriptor), Some(&active.id))? {
        return Ok(unsupported("legacy lazy pin has no validated generation"));
    }
    if let Some(ids) = decode::<Vec<String>>(metadata, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)? {
        let unique = ids.iter().collect::<BTreeSet<_>>();
        if unique.len() != ids.len() || unique != descriptor.skill_revisions.keys().collect() {
            return Err("populated selected IDs contradict original snapshot".into());
        }
    }
    for key in [
        SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,
        SELECTED_SKILL_MODE_METADATA_KEY,
    ] {
        if metadata
            .get(key)
            .is_some_and(|mode| Some(mode.as_str()) != pinned.selected_skill_mode.as_deref())
        {
            return Err("populated historical mode contradicts loaded snapshot".into());
        }
    }
    if let Some(selection) = decode::<WorkflowSelection>(metadata, WORKFLOW_SELECTION_METADATA_KEY)?
    {
        if selection.id != active.id
            || selection.source != active.source
            || selection.revision != active.revision
            || selection.args != active.args
        {
            return Err("populated typed selection contradicts loaded activation".into());
        }
    }
    if let Some(ids) = decode::<Vec<String>>(metadata, LOADED_SKILL_IDS_METADATA_KEY)? {
        if !ids.contains(&active.id) {
            return Err("loaded IDs contradict active Instruction".into());
        }
    }
    if metadata
        .get(LAST_LOADED_SKILL_ID_METADATA_KEY)
        .is_some_and(|id| id != &active.id)
    {
        return Err("last loaded ID contradicts active Instruction".into());
    }
    if let Some(event) = decode::<Value>(metadata, WORKFLOW_ACTIVATION_EVENT_METADATA_KEY)? {
        if event["type"] != "workflow.activated"
            || event["workflow_id"] != active.id
            || event["revision"].as_u64() != Some(active.revision)
        {
            return Err("populated activation event contradicts loaded Instruction".into());
        }
    }

    let mut anchors = Vec::new();
    for (index, message) in messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role == Role::Assistant)
    {
        for call in message
            .tool_calls
            .iter()
            .flatten()
            .filter(|call| call.function.name == "load_skill")
        {
            if call.function.arguments.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
                return Err("legacy load arguments exceed bounds".into());
            }
            let args: Value = serde_json::from_str(&call.function.arguments)
                .map_err(|error| error.to_string())?;
            if args["skill_id"].as_str() != Some(&active.id) {
                continue;
            }
            if call.id.is_empty()
                || messages
                    .iter()
                    .flat_map(|m| m.tool_calls.iter().flatten())
                    .filter(|other| other.id == call.id)
                    .count()
                    != 1
            {
                return Err("legacy load tool-call ID is empty or reused".into());
            }
            let results = messages
                .iter()
                .enumerate()
                .filter(|(_, result)| result.tool_call_id.as_deref() == Some(&call.id))
                .collect::<Vec<_>>();
            if results.len() != 1 {
                return Ok(unsupported("load receipt is missing or ambiguous"));
            }
            let (receipt_index, receipt) = results[0];
            if receipt.role != Role::Tool
                || receipt_index <= index
                || receipt.tool_success != Some(true)
            {
                continue;
            }
            if receipt.content.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
                return Err("legacy load receipt exceeds bounds".into());
            }
            let receipt_value: Value =
                serde_json::from_str(&receipt.content).map_err(|error| error.to_string())?;
            if receipt_value["activation_status"] != "active" {
                continue;
            }
            if receipt_value["skill_id"] != active.id
                || receipt_value["revision"].as_u64() != Some(active.revision)
                || receipt_value["source"] != active.source.as_str()
                || receipt_value["kind"] != "instruction"
            {
                return Err("successful active receipt contradicts typed Instruction".into());
            }
            anchors.push((index, receipt_index, receipt));
        }
    }
    if anchors.len() != 1 {
        return Ok(unsupported(
            "five-field load receipt cannot identify a unique episode",
        ));
    }
    let (call_index, _, receipt) = anchors[0];
    if receipt.id.is_empty()
        || messages
            .iter()
            .filter(|message| message.id == receipt.id)
            .count()
            != 1
        || messages[call_index].created_at > active.activated_at
        || receipt.created_at < active.activated_at
    {
        return Ok(unsupported(
            "load receipt cannot bracket the typed activation episode",
        ));
    }
    let calls = messages[call_index]
        .tool_calls
        .as_ref()
        .ok_or("originating batch is missing")?;
    let mut pending = calls
        .iter()
        .map(|call| call.id.as_str())
        .collect::<BTreeSet<_>>();
    if pending.len() != calls.len() || pending.contains("") {
        return Err("originating batch has reused tool IDs".into());
    }
    for call in calls {
        if messages
            .iter()
            .flat_map(|m| m.tool_calls.iter().flatten())
            .filter(|other| other.id == call.id)
            .count()
            != 1
            || messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some(&call.id))
                .count()
                != 1
        {
            return Err("originating mixed batch has missing or reused results".into());
        }
    }
    let mut closed = None;
    for (index, result) in messages.iter().enumerate().skip(call_index + 1) {
        if result.role != Role::Tool {
            return Ok(unsupported("originating mixed batch is incomplete"));
        }
        if !pending.remove(result.tool_call_id.as_deref().unwrap_or("")) {
            return Err("foreign result interrupts originating batch".into());
        }
        if pending.is_empty() {
            closed = Some(index);
            break;
        }
    }
    let Some(closed) = closed else {
        return Ok(unsupported("originating mixed batch has not closed"));
    };
    // Same Bamboo WorkflowRuntime projection as prompt_envelope.rs, borrowing
    // historical content. No current filesystem or immutable resource-map copy.
    let dynamic = active
        .dynamic_context
        .iter()
        .map(|block| {
            serde_json::json!({
                "provider_id": block.provider_id, "provenance": block.provenance,
                "status": block.status, "content": block.content, "diagnostic": block.diagnostic,
            })
        })
        .collect::<Vec<_>>();
    let content = format!("workflow_id: {}\nsource: {:?}\nrevision: {}\nargs: {}\ncontext_fingerprint: {}\n\n### Instructions\n{}\n\n### Dynamic Context\n{}",
        active.id, active.source, active.revision, active.args,
        active.context_fingerprint.as_deref().unwrap_or("unavailable"), entry.definition.prompt,
        serde_json::to_string(&dynamic).map_err(|error| error.to_string())?);
    if content.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
        return Err("historical projection exceeds 512 KiB".into());
    }
    let mut hash = Sha256::new();
    hash.update((receipt.id.len() as u64).to_be_bytes());
    hash.update(receipt.id.as_bytes());
    hash.update(content.as_bytes());
    let id = hex::encode(hash.finalize());
    let existing = messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.id == id)
        .collect::<Vec<_>>();
    let mut message = Message::assistant(content, None);
    message.id = id;
    message.created_at = receipt.created_at;
    if !existing.is_empty()
        && (existing.len() != 1
            || existing[0].0 != closed + 1
            || serde_json::to_value(existing[0].1).map_err(|error| error.to_string())?
                != serde_json::to_value(&message).map_err(|error| error.to_string())?)
    {
        return Err("deterministic ordinary history identity collides".into());
    }
    let remove_metadata = OBSOLETE_INSTRUCTION_KEYS
        .iter()
        .filter(|key| metadata.contains_key(**key))
        .map(|key| (*key).to_owned())
        .collect();
    Ok(LegacySkillHistoryPlan {
        insert_after: Some(closed),
        message: existing.is_empty().then_some(message),
        remove_metadata,
        unsupported: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_app::skill_input::tests::{selection, snapshot};
    use bamboo_agent_core::{FunctionCall, Session, ToolCall};
    use bamboo_skills::activation::{DynamicContextBlock, WorkflowInvokedBy};
    use chrono::{Duration, Utc};
    use serde_json::json;

    fn call(id: &str, name: &str, args: Value) -> ToolCall {
        ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: args.to_string(),
            },
        }
    }

    fn fixture() -> (
        Vec<Message>,
        HashMap<String, String>,
        DurableWorkflowActivation,
    ) {
        let now = Utc::now();
        let active = ActiveWorkflow {
            id: "review".into(),
            source: bamboo_skills::WorkflowSource::Workspace,
            revision: 7,
            kind: WorkflowKind::Instruction,
            args: selection().args,
            invoked_by: WorkflowInvokedBy::User,
            activated_at: now,
            status: WorkflowActivationStatus::Active,
            diagnostic: None,
            context_fingerprint: Some("immutable-fingerprint".into()),
            dynamic_context: vec![DynamicContextBlock {
                provider_id: "old-provider".into(),
                tool: "old-tool".into(),
                provenance: "historical provenance".into(),
                generated_at: now - Duration::seconds(1),
                expires_at: None,
                status: WorkflowActivationStatus::Active,
                stop_on_failure: false,
                content: "PRIVATE old dynamic context".into(),
                diagnostic: None,
            }],
        };
        let durable = DurableWorkflowActivation {
            active,
            snapshot: snapshot(),
        };
        let mut metadata = HashMap::from([
            (
                ACTIVE_WORKFLOW_METADATA_KEY.into(),
                serde_json::to_string(&durable.active).unwrap(),
            ),
            (
                ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.into(),
                serde_json::to_string(&durable).unwrap(),
            ),
            (
                SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.into(),
                serde_json::to_string(&durable.snapshot).unwrap(),
            ),
            (
                SKILL_RUNTIME_SELECTED_CATALOG_KEY.into(),
                serde_json::to_string(&vec![durable.snapshot.skills["review"]
                    .catalog_entry
                    .clone()])
                .unwrap(),
            ),
            (SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.into(), "9".into()),
            (
                SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.into(),
                json!({"review":7}).to_string(),
            ),
            (SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY.into(), "code".into()),
            (
                WORKFLOW_SELECTION_METADATA_KEY.into(),
                serde_json::to_string(&selection()).unwrap(),
            ),
            (
                LOADED_SKILL_IDS_METADATA_KEY.into(),
                json!(["review"]).to_string(),
            ),
            (LAST_LOADED_SKILL_ID_METADATA_KEY.into(), "review".into()),
            (
                SELECTED_SKILL_IDS_METADATA_KEY.into(),
                json!(["configured-other"]).to_string(),
            ),
            (
                "workflow.run_ids.v1".into(),
                json!(["orchestration-run"]).to_string(),
            ),
        ]);
        metadata.insert(
            WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY.into(),
            serde_json::to_string(&durable.active.dynamic_context).unwrap(),
        );
        let mut assistant = Message::assistant(
            "original assistant commentary",
            Some(vec![
                call("load-id", "load_skill", json!({"skill_id":"review"})),
                call("other-id", "read", json!({"file":"other"})),
            ]),
        );
        assistant.created_at = now - Duration::seconds(1);
        let mut receipt=Message::tool_result("load-id",json!({"skill_id":"review","revision":7,"source":"workspace","kind":"instruction","activation_status":"active"}).to_string());
        receipt.created_at = now + Duration::seconds(1);
        let mut other = Message::tool_result("other-id", "original mixed result");
        other.created_at = now + Duration::seconds(2);
        (
            vec![
                Message::user("original User input"),
                assistant,
                receipt,
                other,
                Message::assistant("later answer", None),
            ],
            metadata,
            durable,
        )
    }

    fn sync_durable(metadata: &mut HashMap<String, String>, durable: &DurableWorkflowActivation) {
        metadata.insert(
            ACTIVE_WORKFLOW_METADATA_KEY.into(),
            serde_json::to_string(&durable.active).unwrap(),
        );
        metadata.insert(
            ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.into(),
            serde_json::to_string(durable).unwrap(),
        );
        metadata.insert(
            SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.into(),
            serde_json::to_string(&durable.snapshot).unwrap(),
        );
        metadata.insert(
            SKILL_RUNTIME_SELECTED_CATALOG_KEY.into(),
            serde_json::to_string(&vec![durable.snapshot.skills["review"]
                .catalog_entry
                .clone()])
            .unwrap(),
        );
    }

    #[test]
    fn legacy_skill_history_projects_loaded_contents_after_complete_mixed_batch() {
        let (messages, metadata, _) = fixture();
        let originals = serde_json::to_value(&messages).unwrap();
        let original_metadata = metadata.clone();
        let plan = plan_legacy_skill_history(&messages, &metadata).unwrap();
        assert_eq!(plan.insert_after, Some(3));
        assert!(plan.unsupported.is_none());
        let message = plan.message.unwrap();
        assert_eq!(message.role, Role::Assistant);
        assert_eq!(message.created_at, messages[2].created_at);
        assert!(message.tool_calls.is_none());
        assert!(message.tool_call_id.is_none());
        assert!(message
            .content
            .contains("### Instructions\nPRIVATE Instructions"));
        assert!(message.content.contains("PRIVATE old dynamic context"));
        assert!(message.content.contains("historical provenance"));
        assert!(message.content.contains("src/main.rs"));
        assert!(message.content.contains("immutable-fingerprint"));
        assert!(!message.content.contains("RESOURCE_MUST_NOT_APPEAR"));
        let mut session = Session::new("history-fixture", "model");
        session.messages = messages.clone();
        session.metadata = metadata.clone();
        let block =
            super::super::prompt_envelope::build_active_workflow_context_block(&session).unwrap();
        assert_eq!(message.content, block.content);
        assert!(plan
            .remove_metadata
            .contains(&ACTIVE_WORKFLOW_METADATA_KEY.into()));
        assert!(!plan
            .remove_metadata
            .contains(&SELECTED_SKILL_IDS_METADATA_KEY.into()));
        assert!(!plan.remove_metadata.contains(&"workflow.run_ids.v1".into()));
        assert_eq!(serde_json::to_value(messages).unwrap(), originals);
        assert_eq!(metadata, original_metadata);
    }

    #[test]
    fn legacy_skill_history_retry_validates_anchor_and_exact_ordinary_identity() {
        let (messages, metadata, _) = fixture();
        let first = plan_legacy_skill_history(&messages, &metadata).unwrap();
        let again = plan_legacy_skill_history(&messages, &metadata).unwrap();
        assert_eq!(
            serde_json::to_value(&first.message).unwrap(),
            serde_json::to_value(again.message).unwrap()
        );
        let mut applied = messages.clone();
        applied.insert(first.insert_after.unwrap() + 1, first.message.unwrap());
        let replay = plan_legacy_skill_history(&applied, &metadata).unwrap();
        assert!(replay.message.is_none());
        assert!(replay.unsupported.is_none());
        assert_eq!(applied.len(), messages.len() + 1);
        assert_eq!(
            serde_json::to_value(&applied[..4]).unwrap(),
            serde_json::to_value(&messages[..4]).unwrap()
        );
        let mut collision = applied.clone();
        collision[4].content.push_str("collision");
        assert!(plan_legacy_skill_history(&collision, &metadata).is_err());
        let mut wrong_time = applied.clone();
        wrong_time[4].created_at += Duration::seconds(1);
        assert!(plan_legacy_skill_history(&wrong_time, &metadata).is_err());
        applied[2].tool_success = Some(false);
        assert!(
            plan_legacy_skill_history(&applied, &metadata)
                .unwrap()
                .unsupported
                .is_some(),
            "derived ID never bypasses original anchor"
        );
        let mut cloned_metadata = metadata.clone();
        for key in replay.remove_metadata {
            cloned_metadata.remove(&key);
        }
        assert!(plan_legacy_skill_history(&applied, &cloned_metadata)
            .unwrap()
            .message
            .is_none());
    }

    #[test]
    fn legacy_skill_history_pending_degraded_and_lazy_pins_do_not_become_loaded() {
        let (messages, metadata, _) = fixture();
        let mut pending = metadata.clone();
        pending.remove(ACTIVE_WORKFLOW_METADATA_KEY);
        assert!(plan_legacy_skill_history(&messages, &pending)
            .unwrap()
            .unsupported
            .is_some());
        let mut lazy = metadata.clone();
        lazy.remove(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY);
        assert!(plan_legacy_skill_history(&messages, &lazy)
            .unwrap()
            .unsupported
            .is_some());
        let mut degraded = messages.clone();
        let mut value: Value = serde_json::from_str(&degraded[2].content).unwrap();
        value["activation_status"] = json!("degraded");
        degraded[2].content = value.to_string();
        assert!(degraded[2].tool_success.unwrap());
        assert!(plan_legacy_skill_history(&degraded, &metadata)
            .unwrap()
            .unsupported
            .is_some());
        for success in [None, Some(false)] {
            let mut missing = messages.clone();
            missing[2].tool_success = success;
            assert!(plan_legacy_skill_history(&missing, &metadata)
                .unwrap()
                .unsupported
                .is_some());
        }
        let mut only_ids = HashMap::new();
        only_ids.insert(
            LOADED_SKILL_IDS_METADATA_KEY.into(),
            json!(["review"]).to_string(),
        );
        assert!(plan_legacy_skill_history(&messages, &only_ids)
            .unwrap()
            .unsupported
            .is_some());
    }

    #[test]
    fn legacy_skill_history_rejects_populated_contradictions_atomically() {
        let (messages, metadata, _) = fixture();
        for (key,value) in [
            (SKILL_RUNTIME_ACTIVATION_GENERATION_KEY,"10"),
            (SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,"{\"review\":8}"),
            (SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,"[\"foreign\"]"),
            (SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,"[\"review\",\"review\"]"),
            (SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,"foreign-mode"),
            (SELECTED_SKILL_MODE_METADATA_KEY,"foreign-mode"),
            (LAST_LOADED_SKILL_ID_METADATA_KEY,"foreign"),
            (LOADED_SKILL_IDS_METADATA_KEY,"[]"),
            (WORKFLOW_SELECTION_METADATA_KEY,"{\"id\":\"review\",\"source\":\"user\",\"revision\":7,\"args\":{\"target\":\"src/main.rs\"}}"),
            (ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,"{"),
            (SKILL_RUNTIME_SELECTED_CATALOG_KEY,"[]"),
            (WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,"{\"type\":\"workflow.activated\",\"workflow_id\":\"foreign\",\"revision\":7}"),
        ] {
            let mut changed=metadata.clone(); changed.insert(key.into(),value.into()); let before=changed.clone();
            assert!(plan_legacy_skill_history(&messages,&changed).is_err(),"{key}"); assert_eq!(changed,before);
        }
        for field in ["revision", "source", "kind"] {
            let mut changed = messages.clone();
            let mut receipt: Value = serde_json::from_str(&changed[2].content).unwrap();
            receipt[field] = match field {
                "revision" => json!(8),
                "source" => json!("user"),
                _ => json!("orchestration"),
            };
            changed[2].content = receipt.to_string();
            assert!(
                plan_legacy_skill_history(&changed, &metadata).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn legacy_skill_history_reused_ids_ambiguous_episodes_and_unclosed_batches_never_guess() {
        let (messages, metadata, _) = fixture();
        let mut duplicate = messages.clone();
        duplicate.push(messages[2].clone());
        assert!(plan_legacy_skill_history(&duplicate, &metadata)
            .unwrap()
            .unsupported
            .is_some());
        let mut reused = messages.clone();
        reused.push(messages[1].clone());
        assert!(plan_legacy_skill_history(&reused, &metadata).is_err());
        let mut repeated = messages.clone();
        let mut assistant = messages[1].clone();
        assistant.tool_calls = Some(vec![call(
            "later-load",
            "load_skill",
            json!({"skill_id":"review"}),
        )]);
        assistant.id = "later-assistant".into();
        let mut receipt = messages[2].clone();
        receipt.id = "later-receipt".into();
        receipt.tool_call_id = Some("later-load".into());
        repeated.extend([assistant, receipt]);
        assert!(plan_legacy_skill_history(&repeated, &metadata)
            .unwrap()
            .unsupported
            .is_some());
        let mut interrupted = messages.clone();
        interrupted.insert(3, Message::user("interrupt"));
        assert!(plan_legacy_skill_history(&interrupted, &metadata)
            .unwrap()
            .unsupported
            .is_some());
        let mut missing = messages.clone();
        missing.remove(3);
        assert!(plan_legacy_skill_history(&missing, &metadata).is_err());
        let mut foreign = messages.clone();
        foreign.insert(3, Message::tool_result("foreign-id", "foreign result"));
        assert!(plan_legacy_skill_history(&foreign, &metadata).is_err());
        let mut wrong_epoch = messages.clone();
        wrong_epoch[1].created_at = messages[2].created_at + Duration::seconds(1);
        assert!(plan_legacy_skill_history(&wrong_epoch, &metadata)
            .unwrap()
            .unsupported
            .is_some());
        let mut reorder = messages.clone();
        reorder.swap(2, 3);
        let plan = plan_legacy_skill_history(&reorder, &metadata).unwrap();
        assert_eq!(plan.insert_after, Some(3));
    }

    #[test]
    fn legacy_skill_history_keeps_loaded_body_beyond_8000_and_bounds_original_decoder() {
        let (messages, mut metadata, mut durable) = fixture();
        durable
            .snapshot
            .skills
            .get_mut("review")
            .unwrap()
            .definition
            .prompt = "OLD🙂".repeat(3_000);
        sync_durable(&mut metadata, &durable);
        let result = plan_legacy_skill_history(&messages, &metadata)
            .unwrap()
            .message
            .unwrap();
        assert!(result
            .content
            .contains(&durable.snapshot.skills["review"].definition.prompt));
        assert!(!result.content.contains("truncated"));
        metadata.insert(
            ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.into(),
            " ".repeat(MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES + 1),
        );
        assert!(plan_legacy_skill_history(&messages, &metadata)
            .unwrap_err()
            .contains("512 KiB"));
    }

    #[test]
    fn legacy_skill_history_bounds_valid_populated_revision_json_before_native_validation() {
        let (messages, mut metadata, _) = fixture();
        let oversized = format!(
            "{}{}",
            json!({"review":7}),
            " ".repeat(MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES)
        );
        metadata.insert(SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.into(), oversized);
        assert!(
            plan_legacy_skill_history(&messages, &metadata).is_err(),
            "valid oversized JSON must not bypass the historical byte bound"
        );
    }

    #[test]
    fn legacy_skill_history_does_not_mutate_orchestration_or_invent_mode_from_receipt() {
        let (messages, mut metadata, mut durable) = fixture();
        durable.active.kind = WorkflowKind::Orchestration;
        sync_durable(&mut metadata, &durable);
        let before = metadata.clone();
        let plan = plan_legacy_skill_history(&messages, &metadata).unwrap();
        assert!(plan.message.is_none());
        assert!(plan.remove_metadata.is_empty());
        assert_eq!(metadata, before);
        let (messages, mut metadata, durable) = fixture();
        let mut mixed = durable.snapshot.clone();
        let mut other = mixed.skills["review"].clone();
        other.definition.id = "orchestration".into();
        other.catalog_entry.id = "orchestration".into();
        other.catalog_entry.kind = WorkflowKind::Orchestration;
        mixed.skills.insert("orchestration".into(), other);
        metadata.insert(
            SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.into(),
            serde_json::to_string(&mixed).unwrap(),
        );
        metadata.insert(
            SKILL_RUNTIME_SELECTED_CATALOG_KEY.into(),
            serde_json::to_string(
                &mixed
                    .skills
                    .values()
                    .map(|entry| &entry.catalog_entry)
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );
        metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.into(),
            json!({"review":7,"orchestration":7}).to_string(),
        );
        let plan = plan_legacy_skill_history(&messages, &metadata).unwrap();
        assert!(plan.message.is_none());
        assert!(plan.remove_metadata.is_empty());
        let (messages, mut metadata, mut durable) = fixture();
        durable.snapshot.selected_skill_mode = None;
        metadata.insert(
            ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.into(),
            serde_json::to_string(&durable).unwrap(),
        );
        assert!(
            plan_legacy_skill_history(&messages, &metadata).is_err(),
            "receipt cannot establish mode"
        );
        let (messages, mut metadata, mut durable) = fixture();
        durable.active.args = json!({"target":42});
        sync_durable(&mut metadata, &durable);
        assert!(
            plan_legacy_skill_history(&messages, &metadata).is_err(),
            "typed args still validate schema"
        );
        let (messages, mut metadata, mut durable) = fixture();
        durable.snapshot.catalog_revision = 0;
        sync_durable(&mut metadata, &durable);
        metadata.insert(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.into(), "0".into());
        assert!(plan_legacy_skill_history(&messages, &metadata).is_err());
    }
}
