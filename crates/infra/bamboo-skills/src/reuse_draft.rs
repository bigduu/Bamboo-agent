//! Deterministic, review-only Skill drafts derived from repeated successful tool traces.
//!
//! Drafts live beside the configured Skill directory, never inside a discovery
//! root. Observations retain only canonical tool identities and JSON value
//! shapes; argument and result literals are deliberately not persisted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use bamboo_domain::{canonical_tool_name, Message, MessagePhase, Role, Session, SessionKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::sync::Mutex;

use crate::store::parser::render_skill_markdown;
use crate::{SkillDefinition, SkillError, SkillResult};

const REUSE_DRAFT_VERSION: u32 = 1;
const DEFAULT_REPETITION_THRESHOLD: usize = 3;
const DEFAULT_MAX_IDENTICAL_CALLS: usize = 2;
const MIN_TRACE_STEPS: usize = 2;

/// Bounded policy for promoting repeated observations into one review draft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseDraftConfig {
    pub repetition_threshold: usize,
    pub max_identical_calls: usize,
}

impl Default for ReuseDraftConfig {
    fn default() -> Self {
        Self {
            repetition_threshold: DEFAULT_REPETITION_THRESHOLD,
            max_identical_calls: DEFAULT_MAX_IDENTICAL_CALLS,
        }
    }
}

impl ReuseDraftConfig {
    fn normalized(self) -> Self {
        Self {
            repetition_threshold: self.repetition_threshold.max(1),
            max_identical_calls: self.max_identical_calls.max(1),
        }
    }
}

/// JSON field/type projection used for signatures and review fixtures.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NormalizedValueShape {
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array {
        elements: Vec<NormalizedValueShape>,
    },
    Object {
        fields: BTreeMap<String, NormalizedValueShape>,
    },
}

impl NormalizedValueShape {
    fn from_json(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(number) if number.is_i64() || number.is_u64() => Self::Integer,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Array(values) => {
                let mut elements = values.iter().map(Self::from_json).collect::<Vec<_>>();
                elements.sort();
                elements.dedup();
                Self::Array { elements }
            }
            Value::Object(fields) => Self::Object {
                fields: fields
                    .iter()
                    .map(|(name, value)| (name.clone(), Self::from_json(value)))
                    .collect(),
            },
        }
    }

    fn type_label(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Boolean => "boolean".to_string(),
            Self::Integer => "integer".to_string(),
            Self::Number => "number".to_string(),
            Self::String => "string".to_string(),
            Self::Array { elements } if elements.is_empty() => "array".to_string(),
            Self::Array { elements } => format!(
                "array<{}>",
                elements
                    .iter()
                    .map(Self::type_label)
                    .collect::<Vec<_>>()
                    .join("|")
            ),
            Self::Object { fields } if fields.is_empty() => "object".to_string(),
            Self::Object { .. } => "object".to_string(),
        }
    }
}

/// One successful tool call reduced to its reviewable shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedToolStep {
    pub tool_name: String,
    pub arguments: NormalizedValueShape,
    pub output: NormalizedValueShape,
}

/// Ordered shape of one completed user turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedToolTrace {
    pub steps: Vec<NormalizedToolStep>,
}

impl NormalizedToolTrace {
    /// Stable identity based only on canonical tool names, order and argument shapes.
    pub fn signature(&self) -> String {
        #[derive(Serialize)]
        struct SignatureStep<'a> {
            tool_name: &'a str,
            arguments: &'a NormalizedValueShape,
        }

        let signature_steps = self
            .steps
            .iter()
            .map(|step| SignatureStep {
                tool_name: &step.tool_name,
                arguments: &step.arguments,
            })
            .collect::<Vec<_>>();
        let payload = serde_json::to_vec(&signature_steps)
            .expect("normalized trace serialization cannot fail");
        hex::encode(Sha256::digest(payload))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseDraftInput {
    pub name: String,
    pub step: usize,
    pub tool_name: String,
    pub argument_path: String,
    pub value_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseDraftOutput {
    pub name: String,
    pub step: Option<usize>,
    pub source: String,
    pub value_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseDraftValidation {
    pub expected_signature: String,
    pub match_example: NormalizedToolTrace,
    pub nonmatch_example: NormalizedToolTrace,
    pub nonmatch_signature: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReuseDraftRepresentation {
    InstructionSkill,
}

/// Reviewable artifact emitted only when distinct-session support reaches the threshold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseDraftArtifact {
    pub version: u32,
    pub signature: String,
    pub proposed_name: String,
    pub description: String,
    pub representation: ReuseDraftRepresentation,
    pub selection_reason: String,
    pub distinct_session_count: usize,
    pub tool_sequence: Vec<String>,
    pub inputs: Vec<ReuseDraftInput>,
    pub outputs: Vec<ReuseDraftOutput>,
    pub match_example: NormalizedToolTrace,
    pub nonmatch_example: NormalizedToolTrace,
    pub validation: ReuseDraftValidation,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReuseDraftObservation {
    version: u32,
    signature: String,
    session_key: String,
    trace: NormalizedToolTrace,
}

/// Process-local coordinator for the isolated observation and candidate files.
pub(crate) struct ReuseDraftCollector {
    root: PathBuf,
    config: ReuseDraftConfig,
    write_lock: Mutex<()>,
}

impl ReuseDraftCollector {
    pub(crate) fn for_skills_dir(skills_dir: &Path, config: ReuseDraftConfig) -> Self {
        let parent = skills_dir.parent().unwrap_or_else(|| Path::new("."));
        Self {
            root: parent.join("reuse-drafts"),
            config: config.normalized(),
            write_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn observe(
        &self,
        session: &Session,
        message_start: usize,
    ) -> SkillResult<Option<ReuseDraftArtifact>> {
        if session.kind != SessionKind::Root {
            return Ok(None);
        }
        let Some(trace) =
            extract_completed_trace(session, message_start, self.config.max_identical_calls)
        else {
            return Ok(None);
        };
        let signature = trace.signature();
        let session_key = session_key(session);
        let _guard = self.write_lock.lock().await;

        let observation_dir = self.root.join("observations").join(&signature);
        fs::create_dir_all(&observation_dir).await?;
        let observation_path = observation_dir.join(format!("{session_key}.json"));
        if !path_exists(&observation_path).await? {
            let observation = ReuseDraftObservation {
                version: REUSE_DRAFT_VERSION,
                signature: signature.clone(),
                session_key,
                trace: trace.clone(),
            };
            write_json(&observation_path, &observation).await?;
        }

        let distinct_session_count = count_json_files(&observation_dir).await?;
        if distinct_session_count < self.config.repetition_threshold {
            return Ok(None);
        }

        let artifact = build_artifact(&trace, distinct_session_count);
        let candidate_dir = self.root.join("candidates").join(&artifact.proposed_name);
        let draft_path = candidate_dir.join("draft.json");
        if path_exists(&draft_path).await? {
            return Ok(None);
        }

        fs::create_dir_all(&candidate_dir).await?;
        let skill = skill_definition_for_artifact(&artifact);
        let markdown = render_skill_markdown(&skill)?;
        write_json(&draft_path, &artifact).await?;
        fs::write(candidate_dir.join("SKILL.md"), markdown).await?;
        write_json(&candidate_dir.join("validation.json"), &artifact.validation).await?;

        Ok(Some(artifact))
    }
}

fn extract_completed_trace(
    session: &Session,
    message_start: usize,
    max_identical_calls: usize,
) -> Option<NormalizedToolTrace> {
    let turn = session.messages.get(message_start..)?;
    let final_message = turn.last()?;
    if !matches!(final_message.role, Role::Assistant)
        || final_message
            .tool_calls
            .as_ref()
            .is_some_and(|calls| !calls.is_empty())
        || final_message.phase == Some(MessagePhase::Commentary)
        || final_message.content.trim().is_empty()
    {
        return None;
    }

    let trace_messages = turn.get(..turn.len().saturating_sub(1))?;
    let mut calls = Vec::new();
    let mut call_ids = HashSet::new();
    let mut literal_fingerprints = HashMap::<String, usize>::new();
    for message in trace_messages {
        let Some(tool_calls) = message
            .tool_calls
            .as_ref()
            .filter(|_| matches!(message.role, Role::Assistant))
        else {
            continue;
        };
        for call in tool_calls {
            if call.id.is_empty() || !call_ids.insert(call.id.clone()) {
                return None;
            }
            let tool_name = canonical_tool_name(&call.function.name);
            if tool_name.is_empty() {
                return None;
            }
            let arguments = serde_json::from_str::<Value>(&call.function.arguments).ok()?;
            let fingerprint = format!(
                "{tool_name}\0{}",
                serde_json::to_string(&canonicalize_json(&arguments)).ok()?
            );
            let repetitions = literal_fingerprints.entry(fingerprint).or_default();
            *repetitions += 1;
            if *repetitions > max_identical_calls {
                return None;
            }
            calls.push((call.id.as_str(), tool_name, arguments));
        }
    }
    if calls.len() < MIN_TRACE_STEPS {
        return None;
    }

    let mut results = HashMap::<&str, &Message>::new();
    for message in trace_messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
    {
        let call_id = message.tool_call_id.as_deref()?;
        if !call_ids.contains(call_id)
            || message.tool_success != Some(true)
            || results.insert(call_id, message).is_some()
        {
            return None;
        }
    }
    if results.len() != calls.len() {
        return None;
    }

    let steps = calls
        .into_iter()
        .map(|(call_id, tool_name, arguments)| {
            results.get(call_id)?;
            Some(NormalizedToolStep {
                tool_name,
                arguments: NormalizedValueShape::from_json(&arguments),
                // Durable tool results are stored in `Message.content`, which
                // is a string even when it happens to contain encoded JSON.
                output: NormalizedValueShape::String,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(NormalizedToolTrace { steps })
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(fields) => {
            let mut names = fields.keys().collect::<Vec<_>>();
            names.sort();
            Value::Object(
                names
                    .into_iter()
                    .map(|name| (name.clone(), canonicalize_json(&fields[name])))
                    .collect(),
            )
        }
        _ => value.clone(),
    }
}

fn session_key(session: &Session) -> String {
    hex::encode(Sha256::digest(session.id.as_bytes()))
}

fn build_artifact(
    trace: &NormalizedToolTrace,
    distinct_session_count: usize,
) -> ReuseDraftArtifact {
    let signature = trace.signature();
    let tool_sequence = trace
        .steps
        .iter()
        .map(|step| step.tool_name.clone())
        .collect::<Vec<_>>();
    let proposed_name = proposed_name(&tool_sequence, &signature);
    let sequence_words = proposed_name
        .trim_start_matches("reuse-")
        .rsplit_once('-')
        .map_or(proposed_name.as_str(), |(words, _)| words)
        .replace('-', " ");
    let description =
        format!("Reviewable instruction draft for the repeated {sequence_words} tool sequence");
    let inputs = draft_inputs(trace);
    let outputs = draft_outputs(trace);
    let match_example = trace.clone();
    let nonmatch_example = nonmatching_example(trace);
    let validation = ReuseDraftValidation {
        expected_signature: signature.clone(),
        match_example: match_example.clone(),
        nonmatch_signature: nonmatch_example.signature(),
        nonmatch_example: nonmatch_example.clone(),
    };
    let (representation, selection_reason) = select_representation();

    ReuseDraftArtifact {
        version: REUSE_DRAFT_VERSION,
        signature,
        proposed_name,
        description,
        representation,
        selection_reason,
        distinct_session_count,
        tool_sequence,
        inputs,
        outputs,
        match_example,
        nonmatch_example,
        validation,
    }
}

fn select_representation() -> (ReuseDraftRepresentation, String) {
    // The normalized trace intentionally retains no literal commands or
    // arguments, and Bamboo has no provider-neutral CLI for replaying an
    // arbitrary tool name. A shell script could therefore only be a larger,
    // non-functional wrapper. The instruction Skill is the sole valid and
    // smallest reviewable representation for this placeholder-only MVP.
    (
        ReuseDraftRepresentation::InstructionSkill,
        "The placeholder-only trace cannot reconstruct executable commands; the instruction Skill is the only valid and smallest reviewable representation."
            .to_string(),
    )
}

fn proposed_name(tool_sequence: &[String], signature: &str) -> String {
    let mut parts = tool_sequence
        .iter()
        .take(3)
        .map(|name| slug_component(name))
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        parts.push("tools".to_string());
    }
    let mut stem = format!("reuse-{}", parts.join("-"));
    stem.truncate(96);
    while stem.ends_with('-') {
        stem.pop();
    }
    format!("{stem}-{}", &signature[..12])
}

fn slug_component(value: &str) -> String {
    let mut output = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            if separator && !output.is_empty() {
                output.push('-');
            }
            output.push(character.to_ascii_lowercase());
            separator = false;
        } else {
            separator = true;
        }
    }
    output.trim_matches('-').to_string()
}

fn draft_inputs(trace: &NormalizedToolTrace) -> Vec<ReuseDraftInput> {
    let mut inputs = Vec::new();
    for (index, step) in trace.steps.iter().enumerate() {
        collect_inputs(index + 1, &step.tool_name, "", &step.arguments, &mut inputs);
    }
    inputs
}

fn collect_inputs(
    step: usize,
    tool_name: &str,
    path: &str,
    shape: &NormalizedValueShape,
    inputs: &mut Vec<ReuseDraftInput>,
) {
    match shape {
        NormalizedValueShape::Object { fields } if !fields.is_empty() => {
            for (field, value) in fields {
                let next = if path.is_empty() {
                    field.clone()
                } else {
                    format!("{path}.{field}")
                };
                collect_inputs(step, tool_name, &next, value, inputs);
            }
        }
        _ => {
            let argument_path = if path.is_empty() { "$" } else { path };
            let parameter = slug_component(argument_path).replace('-', "_");
            inputs.push(ReuseDraftInput {
                name: format!(
                    "step_{step}_{}",
                    if parameter.is_empty() {
                        "value"
                    } else {
                        &parameter
                    }
                ),
                step,
                tool_name: tool_name.to_string(),
                argument_path: argument_path.to_string(),
                value_type: shape.type_label(),
            });
        }
    }
}

fn draft_outputs(trace: &NormalizedToolTrace) -> Vec<ReuseDraftOutput> {
    let mut outputs = trace
        .steps
        .iter()
        .enumerate()
        .map(|(index, step)| ReuseDraftOutput {
            name: format!("step_{}_result", index + 1),
            step: Some(index + 1),
            source: step.tool_name.clone(),
            value_type: step.output.type_label(),
        })
        .collect::<Vec<_>>();
    outputs.push(ReuseDraftOutput {
        name: "final_answer".to_string(),
        step: None,
        source: "assistant".to_string(),
        value_type: "string".to_string(),
    });
    outputs
}

fn nonmatching_example(trace: &NormalizedToolTrace) -> NormalizedToolTrace {
    let mut example = trace.clone();
    example.steps.swap(0, 1);
    if example.signature() == trace.signature() {
        example.steps[0].tool_name = "nonmatching-tool".to_string();
    }
    example
}

fn skill_definition_for_artifact(artifact: &ReuseDraftArtifact) -> SkillDefinition {
    let mut prompt = String::from(
        "This is a review-only reuse draft. Review and edit it before moving it into a Skill discovery directory.\n\n## Inputs\n",
    );
    for input in &artifact.inputs {
        prompt.push_str(&format!(
            "- `{}` (`{}`): argument `{}` for step {} `{}`.\n",
            input.name, input.value_type, input.argument_path, input.step, input.tool_name
        ));
    }
    prompt.push_str("\n## Procedure\n");
    for (index, tool_name) in artifact.tool_sequence.iter().enumerate() {
        prompt.push_str(&format!(
            "{}. Call `{tool_name}` using the declared step {} inputs.\n",
            index + 1,
            index + 1
        ));
    }
    prompt.push_str("\n## Outputs\n");
    for output in &artifact.outputs {
        prompt.push_str(&format!(
            "- `{}` (`{}`) from `{}`.\n",
            output.name, output.value_type, output.source
        ));
    }
    prompt.push_str(&format!(
        "\n## Validation\nMatching trace signature: `{}`. The sibling `validation.json` contains deterministic matching and non-matching fixtures.\n",
        artifact.signature
    ));

    let mut skill = SkillDefinition::new(
        artifact.proposed_name.clone(),
        artifact.proposed_name.clone(),
        artifact.description.clone(),
        prompt,
    );
    skill.tool_refs = artifact.tool_sequence.clone();
    skill.tool_refs.sort();
    skill.tool_refs.dedup();
    skill.metadata = Some(serde_json::json!({
        "reuse_draft": true,
        "signature": artifact.signature,
        "source_sessions": artifact.distinct_session_count,
    }));
    skill
}

async fn path_exists(path: &Path) -> SkillResult<bool> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn count_json_files(directory: &Path) -> SkillResult<usize> {
    let mut entries = fs::read_dir(directory).await?;
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("json")
        {
            count += 1;
        }
    }
    Ok(count)
}

async fn write_json(path: &Path, value: &impl Serialize) -> SkillResult<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        SkillError::Storage(format!("failed to serialize reuse draft: {error}"))
    })?;
    fs::write(path, bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bamboo_domain::{FunctionCall, Message, Session, ToolCall};
    use serde_json::json;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    use super::*;
    use crate::capability_discovery::{
        CapabilityDiscoveryEligibility, CapabilityDiscoveryIndex, InvocationEligibility,
    };
    use crate::{
        WorkflowCatalogEntry, WorkflowCatalogSnapshot, WorkflowKind, WorkflowSource, WorkflowStatus,
    };

    fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: serde_json::to_string(&arguments).expect("arguments"),
            },
        }
    }

    fn completed_session(id: &str, first_path: &str, limit: u64) -> Session {
        let mut session = Session::new(id, "test-model");
        session.add_message(Message::user("inspect the project"));
        session.add_message(Message::assistant(
            "",
            Some(vec![
                call(
                    "call-read",
                    "default::read",
                    json!({"path": first_path, "options": {"limit": limit}}),
                ),
                call(
                    "call-grep",
                    "Grep",
                    json!({"pattern": "needle", "paths": [first_path]}),
                ),
            ]),
        ));
        session.add_message(Message::tool_result(
            "call-grep",
            json!({"matches": 2}).to_string(),
        ));
        session.add_message(Message::tool_result("call-read", "file contents"));
        session.add_message(Message::assistant("Inspection complete", None));
        session
    }

    fn collector(temp: &TempDir, threshold: usize) -> ReuseDraftCollector {
        ReuseDraftCollector::for_skills_dir(
            &temp.path().join("skills"),
            ReuseDraftConfig {
                repetition_threshold: threshold,
                ..Default::default()
            },
        )
    }

    #[test]
    fn signature_uses_canonical_names_order_and_argument_shapes_only() {
        let first = extract_completed_trace(
            &completed_session("one", "/private/a", 20),
            0,
            DEFAULT_MAX_IDENTICAL_CALLS,
        )
        .expect("trace");
        let second = extract_completed_trace(
            &completed_session("two", "/workspace/b", 900),
            0,
            DEFAULT_MAX_IDENTICAL_CALLS,
        )
        .expect("trace");

        assert_eq!(first.steps[0].tool_name, "Read");
        assert_eq!(first.signature(), second.signature());

        let mut reordered = first.clone();
        reordered.steps.swap(0, 1);
        assert_ne!(first.signature(), reordered.signature());

        let mut changed_type = first.clone();
        if let NormalizedValueShape::Object { fields } = &mut changed_type.steps[0].arguments {
            fields.insert("path".to_string(), NormalizedValueShape::Integer);
        }
        assert_ne!(first.signature(), changed_type.signature());
    }

    #[test]
    fn builtin_aliases_converge_while_mcp_namespaces_remain_distinct() {
        let mut builtin_alias = completed_session("alias", "/a", 1);
        let mut canonical = builtin_alias.clone();
        builtin_alias.messages[1]
            .tool_calls
            .as_mut()
            .expect("calls")[0]
            .function
            .name = "default::applyPatch".to_string();
        canonical.messages[1].tool_calls.as_mut().expect("calls")[0]
            .function
            .name = "Edit".to_string();
        assert_eq!(
            extract_completed_trace(&builtin_alias, 0, 2)
                .expect("alias")
                .signature(),
            extract_completed_trace(&canonical, 0, 2)
                .expect("canonical")
                .signature()
        );

        let mut alpha = completed_session("alpha", "/a", 1);
        let mut beta = alpha.clone();
        alpha.messages[1].tool_calls.as_mut().expect("calls")[0]
            .function
            .name = "mcp__alpha__read_file".to_string();
        beta.messages[1].tool_calls.as_mut().expect("calls")[0]
            .function
            .name = "mcp__beta__read_file".to_string();
        assert_ne!(
            extract_completed_trace(&alpha, 0, 2)
                .expect("alpha")
                .signature(),
            extract_completed_trace(&beta, 0, 2)
                .expect("beta")
                .signature()
        );
    }

    #[test]
    fn array_shapes_ignore_literals_length_and_object_key_order() {
        let left = NormalizedValueShape::from_json(&json!({
            "items": [{"name": "a", "enabled": true}, {"enabled": false, "name": "b"}]
        }));
        let right = NormalizedValueShape::from_json(&json!({
            "items": [{"enabled": true, "name": "different"}]
        }));
        assert_eq!(left, right);
    }

    #[test]
    fn incomplete_failed_unknown_duplicate_and_missing_final_traces_are_rejected() {
        let base = completed_session("base", "/a", 10);

        let mut missing = base.clone();
        missing.messages.remove(2);
        assert!(extract_completed_trace(&missing, 0, 2).is_none());

        let mut failed = base.clone();
        failed.messages[2].tool_success = Some(false);
        assert!(extract_completed_trace(&failed, 0, 2).is_none());

        let mut unknown = base.clone();
        unknown.messages[2].tool_success = None;
        assert!(extract_completed_trace(&unknown, 0, 2).is_none());

        let mut duplicate = base.clone();
        duplicate.messages.insert(3, duplicate.messages[2].clone());
        assert!(extract_completed_trace(&duplicate, 0, 2).is_none());

        let mut no_final = base;
        no_final.messages.pop();
        assert!(extract_completed_trace(&no_final, 0, 2).is_none());

        let mut commentary_final = completed_session("commentary", "/a", 10);
        commentary_final.messages.last_mut().expect("final").phase = Some(MessagePhase::Commentary);
        assert!(extract_completed_trace(&commentary_final, 0, 2).is_none());

        let mut one_step = completed_session("one-step", "/a", 10);
        one_step.messages[1]
            .tool_calls
            .as_mut()
            .expect("calls")
            .pop();
        one_step.messages.remove(2);
        assert!(extract_completed_trace(&one_step, 0, 2).is_none());
    }

    #[test]
    fn retry_heavy_exact_calls_are_rejected() {
        let mut session = Session::new("retry", "test-model");
        session.add_message(Message::user("retry"));
        session.add_message(Message::assistant(
            "",
            Some(
                (0..3)
                    .map(|index| call(&format!("call-{index}"), "Read", json!({"path": "/same"})))
                    .collect(),
            ),
        ));
        for index in 0..3 {
            session.add_message(Message::tool_result(format!("call-{index}"), "same"));
        }
        session.add_message(Message::assistant("done", None));

        assert!(extract_completed_trace(&session, 0, 2).is_none());
    }

    #[test]
    fn message_start_bounds_the_run_without_reanchoring_on_internal_users() {
        let mut session = completed_session("bounded", "/old", 1);
        let message_start = session.messages.len();
        let current = completed_session("current", "/new", 2);
        session.messages.extend(current.messages);
        session.messages.insert(
            message_start + 2,
            Message::user("synthetic stop-hook continuation"),
        );

        let trace = extract_completed_trace(&session, message_start, 2).expect("current run");
        assert_eq!(trace.steps.len(), 2);
        assert_eq!(trace.steps[0].tool_name, "Read");
        assert!(extract_completed_trace(&session, 0, 2).is_none());
    }

    #[tokio::test]
    async fn distinct_sessions_emit_one_isolated_draft_without_literals() {
        let temp = TempDir::new().expect("temp");
        let collector = collector(&temp, 3);

        assert!(collector
            .observe(&completed_session("one", "/secret/one", 1), 0)
            .await
            .expect("first")
            .is_none());
        assert!(collector
            .observe(&completed_session("two", "/secret/two", 2), 0)
            .await
            .expect("second")
            .is_none());
        let artifact = collector
            .observe(&completed_session("three", "/secret/three", 3), 0)
            .await
            .expect("third")
            .expect("threshold draft");

        assert_eq!(artifact.distinct_session_count, 3);
        assert_eq!(
            artifact.representation,
            ReuseDraftRepresentation::InstructionSkill
        );
        assert!(artifact.selection_reason.contains("placeholder-only"));
        assert_eq!(artifact.validation.expected_signature, artifact.signature);
        assert_ne!(artifact.validation.nonmatch_signature, artifact.signature);
        let candidate_dir = temp
            .path()
            .join("reuse-drafts/candidates")
            .join(&artifact.proposed_name);
        for file in ["draft.json", "SKILL.md", "validation.json"] {
            assert!(candidate_dir.join(file).is_file(), "missing {file}");
        }
        assert!(!candidate_dir.join("draft.sh").exists());

        let persisted = WalkDir::new(temp.path().join("reuse-drafts"))
            .into_iter()
            .map(|entry| entry.expect("draft entry"))
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| std::fs::read_to_string(entry.path()).expect("draft file"))
            .collect::<String>();
        assert!(!persisted.contains("/secret/"));
        assert!(!persisted.contains("needle"));
        assert!(!persisted.contains("file contents"));

        assert!(collector
            .observe(&completed_session("four", "/secret/four", 4), 0)
            .await
            .expect("existing draft")
            .is_none());
    }

    #[tokio::test]
    async fn repeated_root_session_is_idempotent() {
        let temp = TempDir::new().expect("temp");
        let collector = collector(&temp, 2);
        let first = completed_session("same", "/one", 1);
        let second = completed_session("same", "/two", 2);

        assert!(collector.observe(&first, 0).await.expect("first").is_none());
        assert!(collector
            .observe(&second, 0)
            .await
            .expect("repeat")
            .is_none());
        let observation_dir = temp.path().join("reuse-drafts/observations").join(
            extract_completed_trace(&first, 0, 2)
                .expect("trace")
                .signature(),
        );
        assert_eq!(count_json_files(&observation_dir).await.expect("count"), 1);
    }

    #[tokio::test]
    async fn child_sessions_and_out_of_range_slices_create_nothing() {
        let temp = TempDir::new().expect("temp");
        let collector = collector(&temp, 1);
        let completed = completed_session("root", "/one", 1);
        let mut child = Session::new_child("child", "root", "test-model", "child");
        child.messages = completed.messages.clone();

        assert!(collector.observe(&child, 0).await.expect("child").is_none());
        assert!(collector
            .observe(&completed, completed.messages.len() + 1)
            .await
            .expect("out of range")
            .is_none());
        assert!(!temp.path().join("reuse-drafts").exists());
    }

    #[tokio::test]
    async fn skill_manager_constructor_applies_the_explicit_threshold() {
        let temp = TempDir::new().expect("temp");
        let skills_dir = temp.path().join("skills");
        let manager = crate::SkillManager::with_config_and_reuse_drafts(
            crate::SkillStoreConfig {
                skills_dir,
                ..Default::default()
            },
            ReuseDraftConfig {
                repetition_threshold: 2,
                ..Default::default()
            },
        );

        assert!(manager
            .observe_completed_tool_trace(&completed_session("one", "/one", 1), 0)
            .await
            .expect("first")
            .is_none());
        let artifact = manager
            .observe_completed_tool_trace(&completed_session("two", "/two", 2), 0)
            .await
            .expect("second")
            .expect("configured threshold");
        assert_eq!(artifact.distinct_session_count, 2);
    }

    #[tokio::test]
    async fn generated_skill_round_trips_stays_outside_catalog_and_matches_unified_ranker() {
        let temp = TempDir::new().expect("temp");
        let skills_dir = temp.path().join("skills");
        let collector = ReuseDraftCollector::for_skills_dir(
            &skills_dir,
            ReuseDraftConfig {
                repetition_threshold: 1,
                ..Default::default()
            },
        );
        let artifact = collector
            .observe(&completed_session("one", "/one", 1), 0)
            .await
            .expect("observe")
            .expect("draft");
        let candidate_dir = temp
            .path()
            .join("reuse-drafts/candidates")
            .join(&artifact.proposed_name);
        let skill_path = candidate_dir.join("SKILL.md");
        let markdown = std::fs::read_to_string(&skill_path).expect("markdown");
        let parsed = crate::store::parser::parse_markdown_skill(&skill_path, &markdown)
            .expect("generated Skill parses");
        assert_eq!(parsed.id, artifact.proposed_name);

        let store = crate::SkillStore::new(crate::SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize store");
        assert!(!store
            .get_all_skills()
            .await
            .iter()
            .any(|skill| skill.id == artifact.proposed_name));

        let entry = WorkflowCatalogEntry {
            id: parsed.id.clone(),
            name: parsed.name.clone(),
            description: parsed.description.clone(),
            kind: WorkflowKind::Instruction,
            source: WorkflowSource::User,
            revision: 1,
            content_digest: String::new(),
            version: "1".to_string(),
            invocation_policy: json!({"explicit": true, "automatic": true}),
            argument_schema: json!({}),
            status: WorkflowStatus::Valid,
            legacy: false,
            migration_status: None,
            last_error: None,
            winner: true,
            shadowed_candidates: Vec::new(),
        };
        let index = CapabilityDiscoveryIndex::from_snapshots(
            std::iter::empty(),
            &WorkflowCatalogSnapshot {
                revision: 1,
                entries: vec![entry],
            },
            &WorkflowCatalogSnapshot::default(),
            &CapabilityDiscoveryEligibility {
                skill_invocation: InvocationEligibility::Automatic,
                ..Default::default()
            },
        );
        assert!(index
            .discover_unambiguous_automatic_skill(&artifact.proposed_name)
            .is_some());
        assert!(index
            .discover_unambiguous_automatic_skill(
                "please reuse the repeated read grep tool sequence",
            )
            .is_some());
        assert!(index.discover_unambiguous_automatic_skill("read").is_none());
        assert!(index
            .discover_unambiguous_automatic_skill("deploy database migration")
            .is_none());
    }

    #[test]
    fn candidate_paths_are_valid_skill_directories() {
        let trace = extract_completed_trace(
            &completed_session("one", "/one", 1),
            0,
            DEFAULT_MAX_IDENTICAL_CALLS,
        )
        .expect("trace");
        let artifact = build_artifact(&trace, 3);
        assert!(crate::store::parser::is_valid_skill_id(
            Path::new(&artifact.proposed_name)
                .file_name()
                .and_then(|name| name.to_str())
                .expect("name")
        ));
    }
}
