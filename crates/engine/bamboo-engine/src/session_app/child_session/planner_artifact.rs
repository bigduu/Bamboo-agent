//! Delegated Plan artifacts are derived from the existing sealed Session commit.
//! No writer tool, new outcome store, queue, or execution approval is involved.
//! Files can be rebuilt from that commit; reads always revalidate its identity.

use super::result_projection::{decode_report_with_limit, plain_child_report_message, unique_json};
use super::{ChildSessionError, ChildSessionPort};
use crate::runtime::execution::{ChildCompletion, ChildCompletionSource};
use bamboo_domain::{ChildContextBinding, Session};
use bamboo_memory::plan_store::{PlanSection, PlanStore};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;

pub const CONTRACT_KEY: &str = "planner.artifact_contract";
pub const CONTRACT_VERSION: &str = "1";
const MAX_REPORT_BYTES: usize = 80 * 1024;
const MAX_PLAN_BYTES: usize = 64 * 1024;
const MAX_SECTIONS: usize = 32;
const MAX_READ_BYTES: usize = 4096;

/// Appended only to an explicitly selected-context Plan assignment. The worker
/// returns data; the trusted runtime publishes it after its normal terminal save.
pub const REPORT_INSTRUCTION: &str = r#"
Return one plain JSON final response, without fences or tool calls, at most
81920 UTF-8 bytes in total. Its exact members are: version:1,
outcome (completed/partial/blocked/failed), summary
(at most 2048 UTF-8 bytes), reported_evidence (at most 16 objects with exact
description/reference/sha256 members; absent reference and sha256 are null),
reported_verification (at most 16 exact check/reported_status/details objects;
status passed/failed/not_run/unknown), proposals, blockers, open_decisions
(each at most 16 strings of 1024 bytes), and plan_markdown (Markdown beginning with a heading,
at most 65536 UTF-8 bytes, at most 32 headings with short titles). Include a
concrete ordered plan and label proposals separately from observed evidence.
Do not Write a file or request a Write grant: the runtime publishes the final
report as a versioned plan artifact. A plan is never execution approval.
"#;

fn invalid(reason: &str) -> ChildSessionError {
    ChildSessionError::InvalidArguments(format!("planner_artifact: {reason}"))
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

struct Artifact {
    revision: String,
    markdown: String,
    sections: Vec<PlanSection>,
    reference: Value,
}

pub fn uses_artifact_contract(child: &Session) -> bool {
    child.subagent_type().as_deref() == Some("planner")
        && child.metadata.get(CONTRACT_KEY).map(String::as_str) == Some(CONTRACT_VERSION)
}

fn artifact(child: &Session, parent: &Session) -> Result<Artifact, ChildSessionError> {
    if !uses_artifact_contract(child)
        || !child
            .agent_runtime_state
            .as_ref()
            .is_some_and(|r| r.read_only)
        || child
            .metadata
            .get("lifecycle")
            .is_some_and(|v| v == "resident")
    {
        return Err(invalid("unsupported_planner"));
    }
    let binding = ChildContextBinding::from_session(child)
        .map_err(|_| invalid("invalid_assignment"))?
        .ok_or_else(|| invalid("missing_assignment"))?;
    let source = ChildCompletionSource::from_committed_session(child)
        .filter(|source| source.status == "completed" && source.matches_parent(child, parent))
        .ok_or_else(|| invalid("stale_or_uncommitted_source"))?;
    let message = child
        .messages
        .iter()
        .find(|m| Some(&m.id) == source.result_message_id.as_ref())
        .filter(|m| plain_child_report_message(m))
        .ok_or_else(|| invalid("report_not_current_final"))?;
    if message.content.len() > MAX_REPORT_BYTES {
        return Err(invalid("report_budget_exceeded"));
    }
    let mut report = unique_json(&message.content).map_err(invalid)?;
    let plan = report
        .as_object_mut()
        .and_then(|o| o.remove("plan_markdown"))
        .and_then(|v| v.as_str().map(str::to_owned))
        .filter(|s| s.starts_with('#') && s.len() <= MAX_PLAN_BYTES)
        .ok_or_else(|| invalid("invalid_plan_markdown"))?;
    // Reuse the exact ChildReport contract, including duplicate detection,
    // unknown-field denial and explicit nulls. Reported facts stay claims.
    let report =
        decode_report_with_limit(&report.to_string(), MAX_REPORT_BYTES).map_err(invalid)?;
    let mut markdown = format!("{plan}\n\n## Reported evidence\n");
    for entry in report["reported_evidence"]
        .as_array()
        .expect("validated report")
    {
        markdown.push_str(&format!("- {entry}\n"));
    }
    markdown.push_str("\n## Open decisions\n");
    for entry in report["open_decisions"]
        .as_array()
        .expect("validated report")
    {
        markdown.push_str(&format!("- {entry}\n"));
    }
    // The complete strict report, not only its preview, is durable and readable.
    markdown.push_str(&format!("\n## Planner report\n{}\n", report));
    let mut sections = PlanStore::index_plan_sections(&child.id, &markdown).sections;
    if sections.len() > MAX_SECTIONS + 3 || sections.iter().any(|s| s.heading.len() > 128) {
        return Err(invalid("section_budget_exceeded"));
    }
    for (index, section) in sections.iter_mut().enumerate() {
        // Repeated headings cannot alias another section.
        section.id = format!("s{index}");
        section.parent_id = None;
        section.anchor_terms.clear();
    }
    if serde_json::to_vec(&sections).map_or(true, |bytes| bytes.len() > 6000) {
        return Err(invalid("section_budget_exceeded"));
    }
    let revision = digest(
        &serde_json::to_vec(&json!({
            "version":1,"parent_created_at":parent.created_at,"source":source,
            "assignment_sha256":binding.assignment_sha256,"report":report,"plan":plan
        }))
        .expect("plan source serializes"),
    );
    let mut summary = report["summary"]
        .as_str()
        .expect("validated summary")
        .to_owned();
    if summary.len() > 512 {
        let mut end = 512;
        while !summary.is_char_boundary(end) {
            end -= 1;
        }
        summary.truncate(end);
    }
    let reference = json!({
        "view":"planner_artifact","version":1,"available":true,
        "child_session_id":child.id,"child_created_at":child.created_at,
        "assignment_sha256":binding.assignment_sha256,"revision":revision,
        "file":format!("plan/delegated/{revision}.md"),
        "summary":summary,"section_count":sections.len(),
        "execution_approved":false,"evidence_is_reported":true,
        "read":{"action":"read","child_session_id":child.id,
            "expected_child_created_at":child.created_at,
            "expected_assignment_sha256":binding.assignment_sha256,"revision":revision}
    });
    Ok(Artifact {
        revision,
        markdown,
        sections,
        reference,
    })
}

/// Publish before the existing completion Inbox transaction, including terminal
/// siblings recovered by the coordinator. Retrying verifies identical bytes.
pub async fn publish_completion(
    home: &Path,
    completion: &ChildCompletion,
    child: &Session,
    parent: &Session,
) -> Result<Option<Value>, ChildSessionError> {
    if !uses_artifact_contract(child) {
        return Ok(None);
    }
    // A trusted watchdog may persist a terminal failure without a report seal.
    // Its already-owned control-plane outcome may release the existing wait;
    // no unsealed text, evidence reference or successful artifact is exposed.
    if completion.source.is_none()
        && matches!(
            completion.status.as_str(),
            "error" | "timeout" | "cancelled" | "skipped"
        )
        && child.kind == bamboo_domain::SessionKind::Child
        && child.id == completion.child_session_id
        && parent.id == completion.parent_session_id
        && child.parent_session_id.as_deref() == Some(parent.id.as_str())
        && child.last_run_status().as_deref() == Some(completion.status.as_str())
    {
        return Ok(Some(
            json!({"view":"planner_artifact","version":1,"available":false,
            "reason":"planner_terminal_failure","terminal_status":completion.status,
            "execution_approved":false,"report_source_available":false}),
        ));
    }
    if completion.source.as_ref().is_none_or(|s| {
        !s.matches_completion(completion)
            || ChildCompletionSource::from_committed_session(child).as_ref() != Some(s)
    }) {
        return Err(invalid("stale_completion"));
    }
    let built = match artifact(child, parent) {
        Ok(built) => built,
        // A failed/malformed plan must unblock its parent with an honest small
        // unavailable result, never a legacy raw-report fallback.
        Err(error) => {
            return Ok(Some(json!({"view":"planner_artifact","version":1,
            "available":false,"reason":error.to_string(),"execution_approved":false})))
        }
    };
    let home = home.to_path_buf();
    let reference = built.reference.clone();
    tokio::task::spawn_blocking(move || materialize(&home, &built))
        .await
        .map_err(|e| ChildSessionError::Execution(format!("plan publisher task: {e}")))?
        .map_err(|e| ChildSessionError::Execution(format!("plan publication failed: {e}")))?;
    Ok(Some(reference))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadArgs {
    pub action: String,
    pub child_session_id: String,
    pub expected_child_created_at: chrono::DateTime<chrono::Utc>,
    pub expected_assignment_sha256: String,
    pub revision: String,
    #[serde(default)]
    pub section_id: Option<String>,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

fn selected(built: &Artifact, child: &Session, args: &ReadArgs) -> Result<(), ChildSessionError> {
    if args.action != "read"
        || child.created_at != args.expected_child_created_at
        || built.reference["assignment_sha256"] != args.expected_assignment_sha256
        || built.revision != args.revision
    {
        return Err(invalid("stale_result_selector"));
    }
    Ok(())
}

/// The caller already owns a same-tree Child port. Recheck both canonical
/// snapshots after file IO; a filesystem reference never supplies authority.
pub async fn read(
    port: &dyn ChildSessionPort,
    home: &Path,
    parent_id: &str,
    args: ReadArgs,
) -> Result<Value, ChildSessionError> {
    let parent = port.load_root_session(parent_id).await?;
    let child = port
        .load_child_for_parent(parent_id, &args.child_session_id)
        .await?;
    let built = artifact(&child, &parent)?;
    selected(&built, &child, &args)?;
    let home = home.to_path_buf();
    let (built, markdown) = tokio::task::spawn_blocking(move || {
        let markdown = materialize(&home, &built)?;
        Ok::<_, std::io::Error>((built, markdown))
    })
    .await
    .map_err(|e| ChildSessionError::Execution(e.to_string()))?
    .map_err(|e| ChildSessionError::Execution(format!("plan read failed: {e}")))?;
    let parent = port.load_root_session(parent_id).await?;
    let child = port
        .load_child_for_parent(parent_id, &args.child_session_id)
        .await?;
    selected(&artifact(&child, &parent)?, &child, &args)?;
    let Some(id) = args.section_id.as_deref() else {
        if args.offset != 0 {
            return Err(invalid("offset_requires_section"));
        }
        let index: Vec<Value> = built
            .sections
            .iter()
            .map(|s| {
                json!({
                    "id":s.id,"heading":s.heading,"line_start":s.line_start,"line_end":s.line_end
                })
            })
            .collect();
        return Ok(
            json!({"version":1,"revision":built.revision,"sections":index,
            "execution_approved":false,"evidence_is_reported":true}),
        );
    };
    let section = built
        .sections
        .iter()
        .find(|s| s.id == id)
        .ok_or_else(|| invalid("unknown_section"))?;
    let text = markdown
        .lines()
        .skip(section.line_start)
        .take(section.line_end - section.line_start + 1)
        .collect::<Vec<_>>()
        .join("\n");
    if args.offset > text.len() || !text.is_char_boundary(args.offset) {
        return Err(invalid("invalid_section_offset"));
    }
    let limit = args
        .max_bytes
        .unwrap_or(MAX_READ_BYTES)
        .clamp(4, MAX_READ_BYTES);
    let mut end = text.len().min(args.offset.saturating_add(limit));
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    loop {
        let value = json!({"version":1,"revision":built.revision,"section_id":id,
            "text":&text[args.offset..end],"byte_start":args.offset,"byte_end":end,
            "next_offset":(end < text.len()).then_some(end),"execution_approved":false,
            "evidence_is_reported":true});
        if serde_json::to_vec(&value).is_ok_and(|bytes| bytes.len() <= 8192) {
            return Ok(value);
        }
        end -= 1;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
    }
}

// A single immutable materialization under the existing PlanStore directory.
// Canonical contents remain in Session; this is neither lifecycle authority nor
// another recovery protocol. All names below the configured home are fixed or
// digest-derived. Directory FDs prevent swapped symlinks from escaping home.
#[cfg(unix)]
fn materialize(home: &Path, artifact: &Artifact) -> std::io::Result<String> {
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::OpenOptionsExt;
    fn component(parent: &File, name: &str) -> std::io::Result<File> {
        let name = CString::new(name).expect("fixed directory component");
        // SAFETY: valid parent FD and NUL-terminated component; no borrowed FD
        // is closed by either syscall. Newly opened FD becomes File-owned.
        let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if created != 0
            && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(std::io::Error::last_os_error());
        }
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        if created == 0 {
            parent.sync_all()?;
        }
        Ok(file)
    }
    fn open(parent: &File, name: &CString, flags: i32) -> std::io::Result<File> {
        // SAFETY: valid directory FD and generated name; owns successful FD.
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600,
            )
        };
        if fd < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }
    let home = std::fs::canonicalize(home)?;
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(home)?;
    let plans = component(&root, "plan")?;
    let directory = component(&plans, "delegated")?;
    let name = CString::new(format!("{}.md", artifact.revision)).expect("digest name");
    match open(&directory, &name, libc::O_RDONLY) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let temp = CString::new(format!(".pending-{}", uuid::Uuid::new_v4())).unwrap();
            let result = (|| {
                let mut file = open(
                    &directory,
                    &temp,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                )?;
                file.write_all(artifact.markdown.as_bytes())?;
                file.sync_all()?;
                // linkat is no-clobber. Another identical completion may win;
                // the final exact-byte verification below is still mandatory.
                let rc = unsafe {
                    libc::linkat(
                        directory.as_raw_fd(),
                        temp.as_ptr(),
                        directory.as_raw_fd(),
                        name.as_ptr(),
                        0,
                    )
                };
                if rc != 0
                    && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
                {
                    return Err(std::io::Error::last_os_error());
                }
                directory.sync_all()
            })();
            // SAFETY: remove only this invocation's generated staging name.
            let removed = unsafe { libc::unlinkat(directory.as_raw_fd(), temp.as_ptr(), 0) };
            result?;
            if removed != 0 {
                return Err(std::io::Error::last_os_error());
            }
            directory.sync_all()?;
        }
        Err(error) => return Err(error),
    }
    let file = open(&directory, &name, libc::O_RDONLY)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() != artifact.markdown.len() as u64 {
        return Err(std::io::Error::other("immutable plan artifact differs"));
    }
    let mut bytes = Vec::new();
    file.take(artifact.markdown.len() as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes != artifact.markdown.as_bytes() {
        return Err(std::io::Error::other("immutable plan artifact differs"));
    }
    String::from_utf8(bytes).map_err(|e| std::io::Error::other(e.to_string()))
}

#[cfg(not(unix))]
fn materialize(_: &Path, _: &Artifact) -> std::io::Result<String> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "versioned planner artifacts require a nofollow directory-FD publisher",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Message;

    fn fixture() -> (Session, Session) {
        let mut parent = Session::new("planner-parent", "model");
        parent.add_message(Message::user("Preserve the original contract"));
        let packet = bamboo_domain::ChildContextPacket {
            version: 1,
            objective: "Plan only".into(),
            constraints: vec![],
            acceptance: vec!["Return evidence and decisions".into()],
            non_goals: vec![],
            necessary_user_instructions: vec![],
            recorded_decisions: vec![],
            source_user_message_ids: vec![parent.messages[0].id.clone()],
            background_message_ids: vec![],
        };
        let mut child = Session::new_child("planner-child", &parent.id, "model", "Planner");
        child.set_subagent_type("planner");
        child
            .metadata
            .insert(CONTRACT_KEY.into(), CONTRACT_VERSION.into());
        child
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .read_only = true;
        let resolved = packet.resolve(&parent, "Plan only").unwrap();
        let binding = ChildContextBinding::new(
            &parent,
            &child.id,
            resolved.required_brief.clone(),
            resolved,
        )
        .unwrap();
        binding.install(&mut child).unwrap();
        child.add_message(binding.assignment_message());
        child.add_message(Message::assistant(json!({
            "version":1,"outcome":"completed","summary":"Plan prepared",
            "plan_markdown":"# Safe migration\n## Design\nOnly observe 用户🪷.\n## Verification\nKeep the tests.",
            "reported_evidence":[{"description":"A reported source","reference":"/never/open/me","sha256":null}],
            "reported_verification":[],"proposals":["Review the plan"],
            "blockers":[],"open_decisions":["Choose the rollout window"]
        }).to_string(), None));
        child.set_last_run_status("completed");
        ChildCompletionSource::prepare(&mut child, "planner-run", &Default::default());
        (parent, child)
    }

    #[test]
    fn planner_artifact_binds_source_revision_and_never_grants_execution() {
        let (parent, child) = fixture();
        let before = serde_json::to_value(&parent).unwrap();
        let built = artifact(&child, &parent).unwrap();
        assert_eq!(built.reference["execution_approved"], false);
        assert!(built.markdown.contains("Choose the rollout window"));
        assert!(built.markdown.contains("/never/open/me"));
        assert_eq!(before, serde_json::to_value(&parent).unwrap());
        let restored: Session =
            serde_json::from_value(serde_json::to_value(&child).unwrap()).unwrap();
        assert_eq!(
            artifact(&restored, &parent).unwrap().revision,
            built.revision
        );
        let mut changed = child.clone();
        changed.messages.last_mut().unwrap().content.push(' ');
        assert!(
            artifact(&changed, &parent).is_err(),
            "old seal cannot attest new content"
        );
        ChildCompletionSource::prepare(&mut changed, "planner-run-v2", &Default::default());
        assert_ne!(
            artifact(&changed, &parent).unwrap().revision,
            built.revision
        );
        let mut wrong_parent = parent.clone();
        wrong_parent.created_at += chrono::Duration::seconds(1);
        assert!(artifact(&child, &wrong_parent).is_err());
        let mut changed_source = parent;
        changed_source.messages[0].content = "Rewritten source".into();
        assert!(artifact(&child, &changed_source).is_err());
    }

    #[test]
    fn planner_artifact_rejects_unsealed_nonreadonly_unknown_and_duplicate_reports() {
        let (parent, child) = fixture();
        let mut changed = child.clone();
        changed.agent_runtime_state.as_mut().unwrap().read_only = false;
        assert!(artifact(&changed, &parent).is_err());
        for content in [
            child.messages.last().unwrap().content.replacen(
                "\"version\":1",
                "\"version\":1,\"version\":1",
                1,
            ),
            child
                .messages
                .last()
                .unwrap()
                .content
                .replacen("{", "{\"approved\":true,", 1),
            "prose cannot publish a plan".into(),
        ] {
            let mut changed = child.clone();
            changed.messages.last_mut().unwrap().content = content;
            ChildCompletionSource::prepare(&mut changed, "malformed-plan", &Default::default());
            assert!(artifact(&changed, &parent).is_err());
        }
    }

    #[test]
    fn planner_artifact_large_report_metadata_is_not_limited_by_resume_budget() {
        let (parent, mut child) = fixture();
        let mut report: Value =
            serde_json::from_str(&child.messages.last().unwrap().content).unwrap();
        report["reported_evidence"] = json!((0..9)
            .map(|index| json!({
                "description":format!("{index}{}", "x".repeat(999)),
                "reference":null,"sha256":null
            }))
            .collect::<Vec<_>>());
        let raw = report.to_string();
        assert!(raw.len() > 8192 && raw.len() < MAX_REPORT_BYTES);
        child.messages.last_mut().unwrap().content = raw;
        ChildCompletionSource::prepare(&mut child, "large-planner-report", &Default::default());
        let built = artifact(&child, &parent)
            .expect("legal report metadata is stored in the artifact, not inlined on resume");
        assert!(built.markdown.len() > 16 * 1024);
        assert!(serde_json::to_vec(&built.reference).unwrap().len() < 4096);
        report.as_object_mut().unwrap().remove("plan_markdown");
        assert_eq!(
            super::super::result_projection::decode_report(&report.to_string()),
            Err("result_budget_exceeded"),
            "compact result delivery must retain its original cap"
        );
    }

    #[cfg(unix)]
    #[test]
    fn planner_artifact_file_is_rebuildable_immutable_and_nofollow() {
        let dir = tempfile::tempdir().unwrap();
        let (parent, child) = fixture();
        let built = artifact(&child, &parent).unwrap();
        assert_eq!(materialize(dir.path(), &built).unwrap(), built.markdown);
        assert_eq!(materialize(dir.path(), &built).unwrap(), built.markdown);
        let path = dir
            .path()
            .join(format!("plan/delegated/{}.md", built.revision));
        std::fs::write(&path, "corrupt").unwrap();
        assert!(materialize(dir.path(), &built).is_err());
        std::fs::remove_file(&path).unwrap();
        assert_eq!(materialize(dir.path(), &built).unwrap(), built.markdown);
        std::fs::remove_file(&path).unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(outside.path(), &path).unwrap();
        assert!(materialize(dir.path(), &built).is_err());
        assert_eq!(std::fs::read(outside.path()).unwrap(), b"");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_artifact_completion_only_publishes_current_sealed_report() {
        let dir = tempfile::tempdir().unwrap();
        let (parent, child) = fixture();
        let mut completion = ChildCompletion {
            parent_session_id: parent.id.clone(),
            child_session_id: child.id.clone(),
            status: "completed".into(),
            error: None,
            completed_at: child.updated_at,
            source: ChildCompletionSource::from_committed_session(&child),
        };
        let result = publish_completion(dir.path(), &completion, &child, &parent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result["available"], true);
        assert!(!result.to_string().contains("Choose the rollout window"));
        completion.source.as_mut().unwrap().activation_run_id = "foreign-run".into();
        assert!(publish_completion(dir.path(), &completion, &child, &parent)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn planner_artifact_synthetic_failure_returns_no_report_or_file_but_unsealed_success_is_rejected(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (parent, child) = fixture();
        for status in ["error", "timeout", "cancelled", "skipped"] {
            let mut child = child.clone();
            child.set_last_run_status(status);
            child.metadata.remove("runtime.child_completion_source_v1");
            child.messages.last_mut().unwrap().content =
                "UNSEALED_REPORT_MUST_NOT_BE_IMPORTED".into();
            let completion = ChildCompletion {
                parent_session_id: parent.id.clone(),
                child_session_id: child.id.clone(),
                status: status.into(),
                error: Some("synthetic failure".into()),
                completed_at: child.updated_at,
                source: None,
            };
            let result = publish_completion(dir.path(), &completion, &child, &parent)
                .await
                .expect("a failed job must unblock its parent without authenticating its report")
                .unwrap();
            assert_eq!(result["available"], false);
            assert_eq!(result["execution_approved"], false);
            assert!(result["read"].is_null());
            assert!(!result.to_string().contains("UNSEALED_REPORT"));
            assert!(!dir.path().join("plan/delegated").exists());
            let mut success = completion;
            success.status = "completed".into();
            child.set_last_run_status("completed");
            assert!(
                publish_completion(dir.path(), &success, &child, &parent)
                    .await
                    .is_err(),
                "a source-less success cannot publish a file or release a successful report"
            );
        }
    }
}
