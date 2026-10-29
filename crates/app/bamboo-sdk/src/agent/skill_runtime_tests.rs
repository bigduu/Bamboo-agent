//! Actual defaults SDK -> Engine -> production resolver -> Reader/provider tests.
//! No fixture resolver, seeded Q, or historical input is used to grant access.

use super::*;
use bamboo_agent_core::tools::{FunctionCall, ToolSchema};
use bamboo_domain::{ImageUrlRef, MessagePart, TokenBudget};
use bamboo_engine::ExecuteRequestBuilder;
use bamboo_llm::retry::UpstreamRetry;
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Default)]
struct Trace {
    requests: Vec<Vec<Message>>,
    schemas: Vec<Vec<String>>,
    packages: BTreeMap<String, String>,
    contents: BTreeMap<(String, String), String>,
    eof: Vec<(String, String)>,
    pending: Option<(String, String)>,
    index: usize,
    reference: bool,
    task_started: bool,
    task_completed: bool,
    failures: Vec<String>,
    main_pages: usize,
}
static NEXT_PROVIDER_CALL: AtomicU64 = AtomicU64::new(0);

struct SkillsProvider {
    wanted: Vec<String>,
    task: PathBuf,
    read_references: bool,
    cap: u32,
    trace: Mutex<Trace>,
}
impl SkillsProvider {
    fn call(trace: &mut Trace, name: &str, args: Value) -> LLMChunk {
        let id = format!(
            "sdk-live-call-{}",
            NEXT_PROVIDER_CALL.fetch_add(1, Ordering::Relaxed)
        );
        trace.pending = Some((id.clone(), name.to_owned()));
        LLMChunk::ToolCalls(vec![ToolCall {
            id,
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.to_owned(),
                arguments: args.to_string(),
            },
        }])
    }
    fn next(&self, messages: &[Message], tools: &[ToolSchema]) -> Vec<LLMChunk> {
        let mut trace = self.trace.lock().unwrap();
        trace.requests.push(messages.to_vec());
        trace.schemas.push(
            tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
        );
        assert!(tools.iter().any(|tool| tool.function.name == "skills_read"));
        assert!(!tools.iter().any(|tool| matches!(
            tool.function.name.as_str(),
            "load_skill" | "read_skill_resource"
        )));
        if let Some((call_id, name)) = trace.pending.take() {
            let message = messages
                .iter()
                .rev()
                .find(|message| message.tool_call_id.as_deref() == Some(call_id.as_str()))
                .unwrap();
            if message.tool_success != Some(true) {
                trace.failures.push(message.content.clone());
                return vec![
                    LLMChunk::Token("Skill unavailable; bounded fallback".into()),
                    LLMChunk::Done,
                ];
            }
            if name == "Read" {
                assert!(message.content.contains("NORMAL_TASK_ACTION"));
                trace.task_completed = true;
                return vec![
                    LLMChunk::Token("Normal task completed after complete Skill EOF".into()),
                    LLMChunk::Done,
                ];
            }
            let page: Value = serde_json::from_str(&message.content)
                .expect("generic compressor must preserve complete page JSON");
            let chat = bamboo_llm::providers::common::openai_compat::messages_to_openai_compat_json(
                messages,
            );
            let actual = chat
                .iter()
                .find(|block| block["tool_call_id"] == call_id)
                .unwrap();
            assert_eq!(actual["content"], message.content);
            let ceiling = if self.cap == 0 {
                RESPONSE_BYTES
            } else {
                self.cap as usize
            };
            assert!(
                serde_json::to_vec(actual).unwrap().len() <= ceiling,
                "actual provider page envelope exceeded current dispatch cap"
            );
            if name == "skills_list" {
                for skill in page["skills"].as_array().unwrap() {
                    if self
                        .wanted
                        .iter()
                        .any(|wanted| skill["name"].as_str() == Some(wanted.as_str()))
                    {
                        trace.packages.insert(
                            skill["name"].as_str().unwrap().to_owned(),
                            skill["package"].as_str().unwrap().to_owned(),
                        );
                    }
                }
                if trace.packages.len() != self.wanted.len() {
                    if let Some(cursor) = page["next_cursor"].as_str() {
                        return vec![
                            Self::call(
                                &mut trace,
                                "skills_list",
                                json!({"cursor":cursor,"limit":20}),
                            ),
                            LLMChunk::Done,
                        ];
                    }
                    trace
                        .failures
                        .push("wanted Skill absent from current catalog".into());
                    return vec![
                        LLMChunk::Token("No matching Skill; ordinary fallback".into()),
                        LLMChunk::Done,
                    ];
                }
            } else {
                let wanted = self.wanted[trace.index].clone();
                let resource = if trace.reference {
                    "references/proof.txt"
                } else {
                    "SKILL.md"
                };
                trace
                    .contents
                    .entry((wanted.clone(), resource.to_owned()))
                    .or_default()
                    .push_str(page["contents"].as_str().unwrap());
                if !trace.reference {
                    trace.main_pages += 1;
                }
                if let Some(cursor) = page["next_cursor"].as_str() {
                    let package = trace.packages[&wanted].clone();
                    return vec![
                        Self::call(
                            &mut trace,
                            "skills_read",
                            json!({"package":package,"resource":resource,"cursor":cursor}),
                        ),
                        LLMChunk::Done,
                    ];
                }
                trace.eof.push((wanted, resource.to_owned()));
                if !trace.reference && self.read_references {
                    trace.reference = true;
                } else {
                    trace.reference = false;
                    trace.index += 1;
                }
            }
        } else {
            return vec![
                LLMChunk::Token("Reading the selected Skills.".into()),
                Self::call(&mut trace, "skills_list", json!({"limit":20})),
                LLMChunk::Done,
            ];
        }
        if trace.index < self.wanted.len() {
            let package = trace.packages[&self.wanted[trace.index]].clone();
            let resource = if trace.reference {
                "references/proof.txt"
            } else {
                "SKILL.md"
            };
            return vec![
                Self::call(
                    &mut trace,
                    "skills_read",
                    json!({"package":package,"resource":resource}),
                ),
                LLMChunk::Done,
            ];
        }
        assert_eq!(
            trace.eof.len(),
            self.wanted.len() * if self.read_references { 2 } else { 1 }
        );
        trace.task_started = true;
        vec![
            Self::call(&mut trace, "Read", json!({"file_path":self.task})),
            LLMChunk::Done,
        ]
    }
}
#[async_trait]
impl LLMProvider for SkillsProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        Ok(Box::pin(futures::stream::iter(
            self.next(messages, tools).into_iter().map(Ok),
        )))
    }
}

#[derive(Default)]
struct PassiveProvider(Mutex<Vec<(Vec<Message>, Vec<String>)>>);
#[async_trait]
impl LLMProvider for PassiveProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.lock().unwrap().push((
            messages.to_vec(),
            tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
        ));
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("ordinary answer".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

fn write_skill(home: &Path, id: &str, automatic: bool) -> String {
    let root = home.join("skills").join(id);
    std::fs::create_dir_all(root.join("references")).unwrap();
    std::fs::create_dir_all(root.join("agents")).unwrap();
    let body = format!("---\nname: {id}\ndescription: Inspect the proof task carefully and then perform the normal action.\n---\nRead references/proof.txt when proof is requested.\n{}\nMAIN_EOF_{id}\n", "quoted \"界\" slash \\ content\n".repeat(450));
    std::fs::write(root.join("SKILL.md"), &body).unwrap();
    std::fs::write(
        root.join("references/proof.txt"),
        format!("REFERENCE_EOF_{id}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("agents/bamboo.yaml"),
        format!("invocation_policy:\n  explicit: true\n  automatic: {automatic}\n"),
    )
    .unwrap();
    body
}
fn write_config(home: &Path) {
    std::fs::write(home.join("config.json"), r#"{"provider":"anthropic","providers":{"anthropic":{"api_key":"fixture","model":"claude-test"}}}"#).unwrap();
}
async fn build(home: &Path, provider: Arc<dyn LLMProvider>) -> Agent {
    write_config(home);
    Agent::builder()
        .provider(provider)
        .model("claude-test")
        .instruction("Complete the requested proof task.")
        .with_defaults_for_data_dir(home.to_owned())
        .await
        .unwrap()
        .build()
        .unwrap()
}
async fn selections(agent: &Agent, ids: &[&str]) -> Vec<WorkflowSelection> {
    let catalog = agent
        .sdk_skills
        .as_ref()
        .unwrap()
        .manager
        .store()
        .skill_catalog_snapshot()
        .await;
    ids.iter()
        .map(|id| {
            let entry = catalog
                .entries
                .iter()
                .find(|entry| entry.id == *id && entry.winner)
                .unwrap();
            WorkflowSelection {
                id: (*id).to_owned(),
                source: entry.source,
                revision: entry.revision,
                args: json!({}),
            }
        })
        .collect()
}
fn budget(session: &mut Session, cap: u32) {
    session.token_budget = Some(TokenBudget {
        max_tool_output_tokens: cap,
        ..TokenBudget::default()
    });
}

#[tokio::test]
async fn sdk_typed_actual_defaults_reads_multiple_main_pages_references_then_task() {
    let home = tempfile::tempdir().unwrap();
    let first = write_skill(home.path(), "sdk-alpha", false);
    let second = write_skill(home.path(), "sdk-beta", false);
    let task = home.path().join("task.txt");
    std::fs::write(&task, "NORMAL_TASK_ACTION").unwrap();
    let provider = Arc::new(SkillsProvider {
        wanted: vec!["sdk-alpha".into(), "sdk-beta".into()],
        task,
        read_references: true,
        cap: 4096,
        trace: Mutex::default(),
    });
    let agent = build(home.path(), provider.clone()).await;
    assert!(
        agent.activation_router().is_none(),
        "approved finite local SDK caller needs no router"
    );
    let mut session = agent.new_session("sdk-live-two").unwrap();
    budget(&mut session, 4096);
    let mut input = SdkSkillInput::new(
        "Perform the proof task",
        selections(&agent, &["sdk-alpha", "sdk-beta"]).await,
    );
    let image = MessagePart::ImageUrl {
        image_url: ImageUrlRef {
            url: "data:image/png;base64,AA==".into(),
            detail: Some("low".into()),
        },
    };
    input.parts = vec![
        MessagePart::Text {
            text: input.content.clone(),
        },
        image.clone(),
        image.clone(),
    ];
    agent.run_with_skills(&mut session, input).await.unwrap();
    let trace = provider.trace.lock().unwrap();
    assert!(trace.failures.is_empty(), "{:?}", trace.failures);
    assert!(trace.main_pages > 2 && trace.task_started && trace.task_completed);
    assert_eq!(
        trace.contents[&("sdk-alpha".into(), "SKILL.md".into())],
        first
    );
    assert_eq!(
        trace.contents[&("sdk-beta".into(), "SKILL.md".into())],
        second
    );
    assert_eq!(
        trace.contents[&("sdk-alpha".into(), "references/proof.txt".into())],
        "REFERENCE_EOF_sdk-alpha\n"
    );
    let first_request = &trace.requests[0];
    assert!(first_request
        .iter()
        .any(|message| message.content.contains("Using Skills")
            && message.content.contains("sdk-alpha")));
    let submitted = session
        .messages
        .iter()
        .find(|message| {
            message.role == Role::User && message.content.starts_with("Perform the proof task")
        })
        .unwrap();
    let user = first_request
        .iter()
        .find(|message| message.id == submitted.id)
        .unwrap();
    assert_eq!(
        serde_json::to_value(user).unwrap(),
        serde_json::to_value(submitted).unwrap()
    );
    assert!(user.content.contains("### Explicit Skill"));
    assert_eq!(
        user.content_parts
            .as_ref()
            .unwrap()
            .iter()
            .filter(|part| matches!(part, MessagePart::ImageUrl { .. }))
            .count(),
        2
    );
    assert!(
        matches!(&user.content_parts.as_ref().unwrap()[0], MessagePart::Text { text } if text == &user.content)
    );
    let id = user.id.clone();
    assert!(trace.requests.iter().all(|request| request
        .iter()
        .filter(|message| message.id == id)
        .count()
        == 1));
    drop(trace);
    let saved = agent
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.id == id)
            .count(),
        1
    );
    assert!(!saved
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
}

#[tokio::test]
async fn sdk_plain_string_has_current_implicit_catalog_without_startup_keyword_and_retains_nonew() {
    for cap in [0, 4096] {
        let home = tempfile::tempdir().unwrap();
        let expected = write_skill(home.path(), "sdk-implicit", true);
        let task = home.path().join("task.txt");
        std::fs::write(&task, "NORMAL_TASK_ACTION").unwrap();
        let provider = Arc::new(SkillsProvider {
            wanted: vec!["sdk-implicit".into()],
            task,
            read_references: false,
            cap,
            trace: Mutex::default(),
        });
        let agent = build(home.path(), provider.clone()).await;
        let mut session = agent
            .new_session(format!("sdk-live-implicit-{cap}"))
            .unwrap();
        budget(&mut session, cap);
        agent
            .run(
                &mut session,
                "Inspect the proof and perform its requested action",
            )
            .await
            .unwrap();
        let trace = provider.trace.lock().unwrap();
        assert!(trace.failures.is_empty(), "{:?}", trace.failures);
        assert!(
            trace.requests.len() > 3 && trace.task_completed,
            "successful NoNew must retain current input across Reader pages/task"
        );
        assert_eq!(
            trace.contents[&("sdk-implicit".into(), "SKILL.md".into())],
            expected
        );
        let user = trace.requests[0]
            .iter()
            .find(|message| {
                message.role == Role::User
                    && message.content == "Inspect the proof and perform its requested action"
            })
            .unwrap();
        assert_eq!(
            user.content,
            "Inspect the proof and perform its requested action"
        );
        assert!(!user.content.contains("Explicit Skill"));
    }
}

#[tokio::test]
async fn sdk_typed_exact_durable_existing_accepts_and_unsaved_or_wrong_revision_rejects_before_append(
) {
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "sdk-existing", false);
    let provider = Arc::new(PassiveProvider::default());
    let agent = build(home.path(), provider.clone()).await;
    let mut session = agent.new_session("sdk-live-existing").unwrap();
    agent
        .run(&mut session, "initial ordinary task")
        .await
        .unwrap();
    session = agent
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    let selection = selections(&agent, &["sdk-existing"]).await;
    agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new("new exact input", selection.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .messages
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        2
    );
    session = agent
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    let before_calls = provider.0.lock().unwrap().len();
    let mut invalid = selection;
    invalid[0].revision += 1;
    let before = serde_json::to_value(&session).unwrap();
    assert!(agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new("stale source revision", invalid)
        )
        .await
        .is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    session
        .metadata
        .insert("unsaved.host.edit".into(), "staged".into());
    let before = serde_json::to_value(&session).unwrap();
    assert!(agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new(
                "unsaved snapshot",
                selections(&agent, &["sdk-existing"]).await
            )
        )
        .await
        .is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(provider.0.lock().unwrap().len(), before_calls);
}

#[tokio::test]
async fn sdk_disabled_and_host_empty_ceiling_reject_actual_typed_factory() {
    for empty_ceiling in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-denied", false);
        let provider = Arc::new(PassiveProvider::default());
        let mut agent = build(home.path(), provider.clone()).await;
        let selection = selections(&agent, &["sdk-denied"]).await;
        if empty_ceiling {
            let mut policy = agent.sdk_skills.as_ref().unwrap().as_ref().clone();
            policy.ceiling = Some(BTreeSet::new());
            agent.sdk_skills = Some(Arc::new(policy));
        } else {
            agent
                .sdk_skills
                .as_ref()
                .unwrap()
                .config
                .write()
                .await
                .skills
                .disabled
                .push("sdk-denied".into());
        }
        let mut session = agent
            .new_session(format!("sdk-denied-{empty_ceiling}"))
            .unwrap();
        assert!(agent
            .run_with_skills(
                &mut session,
                SdkSkillInput::new("explicit selection is not a ceiling", selection)
            )
            .await
            .is_err());
        assert!(session.messages.is_empty() && provider.0.lock().unwrap().is_empty());
    }
}

struct NoCurrentProvider(Mutex<Trace>);
#[async_trait]
impl LLMProvider for NoCurrentProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let mut trace = self.0.lock().unwrap();
        trace.requests.push(messages.to_vec());
        assert!(tools.iter().any(|tool| tool.function.name == "skills_read"));
        assert!(!messages
            .iter()
            .any(|message| message.content.contains("Using Skills")));
        let chunks = if trace.pending.take().is_some() {
            let result = messages
                .iter()
                .rev()
                .find(|message| message.role == Role::Tool)
                .unwrap();
            assert_eq!(result.tool_success, Some(false));
            assert!(
                result.content.contains("accepted current User")
                    || result.content.contains("current input")
            );
            vec![
                LLMChunk::Token("ordinary task continues without current Skill access".into()),
                LLMChunk::Done,
            ]
        } else {
            vec![
                SkillsProvider::call(&mut trace, "skills_list", json!({})),
                LLMChunk::Done,
            ]
        };
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn sdk_run_session_and_restart_cannot_restore_a_historical_input_grant() {
    for resume in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-history", true);
        let provider = Arc::new(NoCurrentProvider(Mutex::default()));
        let agent = build(home.path(), provider.clone()).await;
        let mut session = agent.new_session(format!("sdk-history-{resume}")).unwrap();
        let history = Message::user("historical Skill sdk-history request");
        let history_id = history.id.clone();
        session.add_message(history);
        agent.storage().save_session(&session).await.unwrap();
        session = agent
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        if resume {
            agent.resume(&mut session).await.unwrap();
        } else {
            agent.run_session(&mut session).await.unwrap();
        }
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            1
        );
        assert_eq!(
            session
                .messages
                .iter()
                .find(|message| message.role == Role::User)
                .unwrap()
                .id,
            history_id
        );
        assert_eq!(provider.0.lock().unwrap().requests.len(), 2);
    }
}

#[tokio::test]
async fn sdk_explicit_empty_custom_and_from_runtime_keep_unknown_surface_closed() {
    for custom in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_config(home.path());
        write_skill(home.path(), "sdk-hidden", false);
        let provider = Arc::new(PassiveProvider::default());
        let builder = Agent::builder()
            .provider(provider.clone())
            .model("claude-test")
            .with_defaults_for_data_dir(home.path().to_owned())
            .await
            .unwrap();
        let agent = if custom {
            builder
                .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::new()))
                .build()
                .unwrap()
        } else {
            builder.no_tools().build().unwrap()
        };
        assert!(agent.sdk_skills.is_none());
        let mut session = agent.new_session(format!("sdk-custom-{custom}")).unwrap();
        assert!(agent
            .run_with_skills(
                &mut session,
                SdkSkillInput::new("typed default capability is unavailable", vec![])
            )
            .await
            .is_err());
        assert!(session.messages.is_empty());
        agent
            .run(&mut session, "ordinary task still works")
            .await
            .unwrap();
        assert!(provider
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, schemas)| !schemas
                .iter()
                .any(|name| matches!(name.as_str(), "skills_list" | "skills_read"))));
        let wrapped = Agent::from_runtime(agent.inner.clone());
        assert!(wrapped.sdk_skills.is_none());
    }
}

#[tokio::test]
async fn sdk_real_default_guard_drop_cancel_and_no_context_deny_without_q_or_forged_phase() {
    let home = tempfile::tempdir().unwrap();
    let agent = build(home.path(), Arc::new(PassiveProvider::default())).await;
    let session = agent.new_session("sdk-owned-guard").unwrap();
    let defaults = agent.sdk_skills.as_ref().unwrap();
    let cancel = CancellationToken::new();
    let guard = defaults.begin_without_input(&session, cancel.clone());
    let run = guard.run.clone();
    let tools = guard.tools(agent.inner.default_tools().clone());
    let call = ToolCall {
        id: "fake-context-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "skills_list".into(),
            arguments: "{}".into(),
        },
    };
    assert!(tools.execute(&call).await.is_err());
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&session.id);
    assert!(
        tools.execute_with_context(&call, ctx).await.is_err(),
        "public IDs/context do not mint real Q/current input"
    );
    cancel.cancel();
    assert!(run.check_live().is_err());
    drop(guard);
    assert!(
        !run.live.load(Ordering::Acquire),
        "host and tool Arc clones must not keep dropped future authority alive"
    );
}

// Portable command hooks have a reviewed bundle digest. SDK has no plugin
// crate dependency; this fixture writes the real public installed registry and
// uses Python's SHA-256 to prepare its exact reviewed bytes on Unix test hosts.
// The production HookRunner still verifies the registry/digest before executing.
#[cfg(unix)]
fn install_prompt_hook(home: &Path, deny: bool) -> PathBuf {
    let root = home.join("plugins");
    let bundle = root.join("sdk-prompt-policy");
    std::fs::create_dir_all(&bundle).unwrap();
    let count = home.join("sdk-prompt-hook-count.txt");
    let quoted = format!("'{}'", count.to_string_lossy().replace('\'', "'\\''"));
    let script = if deny {
        format!("#!/bin/sh\nprintf 'hit\\n' >> {quoted}\nprintf 'SDK prompt blocked' >&2\nexit 2\n")
    } else {
        format!("#!/bin/sh\nprintf 'hit\\n' >> {quoted}\nprintf '%s' '{{\"hookSpecificOutput\":{{\"hookEventName\":\"UserPromptSubmit\",\"additionalContext\":\"SDK_HOOK_ACCEPTED\"}}}}'\n")
    };
    std::fs::write(bundle.join("policy.sh"), script).unwrap();
    std::fs::write(
        bundle.join("plugin.json"),
        serde_json::to_vec(&json!({
            "id":"sdk-prompt-policy","name":"SDK prompt policy","version":"0.1.0",
            "provides":{"hooks":[{"config":"hooks.json","scripts":["policy.sh"]}]}
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(bundle.join("hooks.json"), serde_json::to_vec(&json!({"hooks":{
        "UserPromptSubmit":[{"hooks":[{"type":"command","command":"sh \"$PLUGIN_ROOT/policy.sh\"","timeout":2}]}]
    }})).unwrap()).unwrap();
    let digest = std::process::Command::new("python3").arg("-c").arg(r#"
import hashlib, pathlib, stat, struct, sys
root = pathlib.Path(sys.argv[1]); digest = hashlib.sha256()
digest.update(b'sdk-prompt-policy\x000.1.0')
def permissions(path):
    digest.update(struct.pack('<I', stat.S_IMODE(path.lstat().st_mode)))
def visit(directory):
    permissions(directory)
    for path in sorted(directory.iterdir()):
        name = str(path.relative_to(root)).encode()
        if path.is_dir():
            digest.update(b'directory'); digest.update(struct.pack('<Q',len(name))); digest.update(name); visit(path)
        else:
            data=path.read_bytes(); digest.update(b'file'); permissions(path)
            digest.update(struct.pack('<Q',len(name))); digest.update(name); digest.update(struct.pack('<Q',len(data))); digest.update(data)
visit(root)
print(digest.hexdigest())
"#).arg(&bundle).output().expect("portable fixture requires the repository's Python3 test host");
    assert!(
        digest.status.success(),
        "{}",
        String::from_utf8_lossy(&digest.stderr)
    );
    let digest = String::from_utf8(digest.stdout).unwrap().trim().to_owned();
    std::fs::write(root.join("installed.json"), serde_json::to_vec(&json!({"plugins":[{
        "id":"sdk-prompt-policy","version":"0.1.0","source":{"type":"local_dir","path":bundle},
        "plugin_dir":bundle,"installed_at":"2026-10-09T00:00:00Z","status":"installed",
        "registered":{"hooks":[{"plugin_id":"sdk-prompt-policy","version":"0.1.0","config":"hooks.json","digest":digest,"trusted_digest":digest,"enabled":true}]}
    }]})).unwrap()).unwrap();
    count
}

#[cfg(unix)]
#[tokio::test]
async fn sdk_typed_real_portable_hook_once_before_f_and_append_with_same_images() {
    for deny in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-hook", false);
        let count = install_prompt_hook(home.path(), deny);
        let provider = Arc::new(PassiveProvider::default());
        let agent = build(home.path(), provider.clone()).await;
        let mut session = agent
            .new_session(format!("sdk-portable-hook-{deny}"))
            .unwrap();
        let mut input = SdkSkillInput::new(
            "submit exact typed prompt",
            selections(&agent, &["sdk-hook"]).await,
        );
        input.parts = vec![
            MessagePart::Text {
                text: input.content.clone(),
            },
            MessagePart::ImageUrl {
                image_url: ImageUrlRef {
                    url: "data:image/png;base64,AA==".into(),
                    detail: None,
                },
            },
        ];
        let result = agent.run_with_skills(&mut session, input).await;
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "hit\n",
            "UserPromptSubmit must execute once for this real SDK submission"
        );
        if deny {
            assert!(result.is_err() && session.messages.is_empty());
            assert!(provider.0.lock().unwrap().is_empty());
        } else {
            result.unwrap();
            let trace = provider.0.lock().unwrap();
            let submitted = session
                .messages
                .iter()
                .find(|message| {
                    message.role == Role::User
                        && message.content.starts_with("submit exact typed prompt")
                })
                .unwrap();
            let user = trace[0]
                .0
                .iter()
                .find(|message| message.id == submitted.id)
                .unwrap();
            assert_eq!(
                serde_json::to_value(user).unwrap(),
                serde_json::to_value(submitted).unwrap()
            );
            assert!(
                user.content.contains("SDK_HOOK_ACCEPTED")
                    && user.content.contains("### Explicit Skill")
            );
            assert!(
                matches!(&user.content_parts.as_ref().unwrap()[0], MessagePart::Text { text } if text == &user.content)
            );
            assert!(matches!(
                &user.content_parts.as_ref().unwrap()[1],
                MessagePart::ImageUrl { .. }
            ));
            assert_eq!(
                session
                    .messages
                    .iter()
                    .filter(|message| message.id == user.id)
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn supervisor_delegation_first_sdk_denies_selected_skill_before_append_and_hides_metadata() {
    for ultra in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-supervisor", true);
        let provider = Arc::new(PassiveProvider::default());
        let agent = build(home.path(), provider.clone()).await;
        let bootstrap = agent
            .supervisor_sessions()
            .get_or_create_default("claude-test")
            .await
            .unwrap();
        let mut session = agent
            .storage()
            .load_session(&bootstrap.session_id)
            .await
            .unwrap()
            .unwrap();
        session.set_root_orchestration_only(ultra).unwrap();
        let defaults = agent.sdk_skills.as_ref().unwrap();
        defaults.sessions.save(&mut session).await.unwrap();
        // Exercise the restored typed identity, not an SDK-only role flag.
        session = serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
        assert_eq!(
            session.supervisor_incarnation_id(),
            Some(bootstrap.incarnation_id)
        );
        let before = serde_json::to_value(&session).unwrap();
        let cache_before = defaults
            .sessions
            .cache()
            .get(&session.id)
            .map(|entry| serde_json::to_value(&*entry.read()).unwrap());
        let result = agent
            .run_with_skills(
                &mut session,
                SdkSkillInput::new(
                    "explicit implementation Skill must be delegated",
                    selections(&agent, &["sdk-supervisor"]).await,
                ),
            )
            .await;
        assert!(
            result.is_err(),
            "Supervisor Ultra={ultra}: selected Skill reached the provider"
        );
        assert!(result.unwrap_err().to_string().contains("delegat"));
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(
            serde_json::to_value(
                agent
                    .storage()
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            before,
            "denial must precede durable input or runtime publication",
        );
        assert_eq!(
            defaults
                .sessions
                .cache()
                .get(&session.id)
                .map(|entry| serde_json::to_value(&*entry.read()).unwrap()),
            cache_before
        );
        assert!(provider.0.lock().unwrap().is_empty());
        assert!(defaults
            .manager
            .pinned_activation_for_workspace(&session.id, None)
            .await
            .unwrap()
            .is_none());

        // An ordinary typed User remains usable. Automatic Skill metadata is
        // absent even though this defaults-backed host has a matching Skill.
        agent
            .run_with_skills(
                &mut session,
                SdkSkillInput::new("coordinate the authorized work", vec![]),
            )
            .await
            .unwrap();
        let trace = provider.0.lock().unwrap();
        assert!(!trace.is_empty());
        for (messages, schemas) in trace.iter() {
            assert!(!schemas.iter().any(|name| matches!(
                name.as_str(),
                "skills_list" | "skills_read" | "load_skill" | "workflow_run"
            )));
            assert!(!messages
                .iter()
                .any(|message| message.content.contains("Using Skills")
                    || message.content.contains("### Explicit Skill")
                    || message.content.contains("MAIN_EOF_sdk-supervisor")));
        }
    }
}

#[tokio::test]
async fn sdk_defaults_ultra_keeps_exact_registered_authority_intersection_and_denies_skill_input() {
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "sdk-ultra", true);
    let provider = Arc::new(PassiveProvider::default());
    let agent = build(home.path(), provider.clone()).await;
    let mut session = agent.new_session("sdk-ultra-actual").unwrap();
    session.set_root_orchestration_only(true).unwrap();
    let canonical = bamboo_domain::ROOT_ORCHESTRATION_TOOLS
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(canonical.len(), 18);
    assert!(canonical.contains("session_history"));
    let tools = agent.inner.default_tools();
    let history = tools
        .framework_session_tool_for_exact("session_history")
        .expect("defaults must install the actual common history instance");
    assert_eq!(
        history.kind(),
        bamboo_agent_core::tools::FrameworkSessionToolKind::History
    );
    assert!(history.is_current());
    assert!(Arc::ptr_eq(
        &history.instance(),
        &tools.tool_instance_for_exact("session_history").unwrap()
    ));
    let expected = agent
        .inner
        .default_tools()
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .filter(|name| canonical.contains(name))
        .collect::<BTreeSet<_>>();
    assert!(agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new(
                "selected Skill must be denied in Ultra",
                selections(&agent, &["sdk-ultra"]).await
            )
        )
        .await
        .is_err());
    assert!(session.messages.is_empty() && provider.0.lock().unwrap().is_empty());
    agent
        .run(&mut session, "ordinary Ultra orchestration")
        .await
        .unwrap();
    let trace = provider.0.lock().unwrap();
    let actual = trace[0].1.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "Ultra must preserve the exact actually registered subset of the canonical set; no fake SDK-only registrations");
    assert!(!actual.contains("skills_list") && !actual.contains("skills_read"));
    assert!(!trace[0]
        .0
        .iter()
        .any(|message| message.content.contains("Using Skills")));
}

struct RevokingProvider {
    inner: SkillsProvider,
    config: Mutex<Option<Arc<RwLock<Config>>>>,
    source: Option<PathBuf>,
    revoked: AtomicBool,
}
#[async_trait]
impl LLMProvider for RevokingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let main_page = {
            let trace = self.inner.trace.lock().unwrap();
            trace.pending.as_ref().is_some_and(|(id, name)| {
                name == "skills_read"
                    && messages.iter().any(|message| {
                        message.tool_call_id.as_deref() == Some(id.as_str())
                            && message.tool_success == Some(true)
                            && serde_json::from_str::<Value>(&message.content)
                                .ok()
                                .is_some_and(|page| page["next_cursor"].is_string())
                    })
            })
        };
        if main_page && !self.revoked.swap(true, Ordering::AcqRel) {
            if let Some(source) = self.source.as_ref() {
                let mut raw = std::fs::read_to_string(source).unwrap();
                raw.push('\n');
                std::fs::write(source, raw).unwrap();
            } else {
                let config = self.config.lock().unwrap().as_ref().unwrap().clone();
                config
                    .write()
                    .await
                    .skills
                    .disabled
                    .push("sdk-revoked".into());
            }
        }
        Ok(Box::pin(futures::stream::iter(
            self.inner.next(messages, tools).into_iter().map(Ok),
        )))
    }
}

#[tokio::test]
async fn sdk_actual_reader_continuation_rechecks_disabled_and_raw_source_before_task() {
    for raw_source in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-revoked", true);
        let task = home.path().join("task.txt");
        std::fs::write(&task, "NORMAL_TASK_ACTION").unwrap();
        let provider = Arc::new(RevokingProvider {
            inner: SkillsProvider {
                wanted: vec!["sdk-revoked".into()],
                task,
                read_references: false,
                cap: 4096,
                trace: Mutex::default(),
            },
            config: Mutex::new(None),
            source: raw_source.then(|| home.path().join("skills/sdk-revoked/SKILL.md")),
            revoked: AtomicBool::new(false),
        });
        let agent = build(home.path(), provider.clone()).await;
        *provider.config.lock().unwrap() = Some(agent.sdk_skills.as_ref().unwrap().config.clone());
        let mut session = agent
            .new_session(format!("sdk-revoke-page-{raw_source}"))
            .unwrap();
        budget(&mut session, 4096);
        agent
            .run(&mut session, "Inspect the proof task")
            .await
            .unwrap();
        assert!(provider.revoked.load(Ordering::Acquire));
        let trace = provider.inner.trace.lock().unwrap();
        assert_eq!(trace.main_pages, 1);
        assert_eq!(
            trace.failures.len(),
            1,
            "real warm cursor continuation must fail through its paired ToolResult"
        );
        assert!(trace.eof.is_empty() && !trace.task_started && !trace.task_completed);
    }
}

#[tokio::test]
async fn sdk_typed_cancellation_interrupts_actual_session_owner_wait_before_append() {
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "sdk-cancel-owner", false);
    let provider = Arc::new(PassiveProvider::default());
    let agent = build(home.path(), provider.clone()).await;
    let mut session = agent.new_session("sdk-cancel-owner").unwrap();
    let selection = selections(&agent, &["sdk-cancel-owner"]).await;
    let defaults = agent.sdk_skills.as_ref().unwrap();
    let owner = defaults
        .sessions
        .persistence()
        .acquire_lock(&session.id)
        .await;
    let cancel = CancellationToken::new();
    let before = serde_json::to_value(&session).unwrap();
    {
        let run = agent.run_with_skills_and_cancel(
            &mut session,
            SdkSkillInput::new("cancel before accepting this User", selection.clone()),
            cancel.clone(),
        );
        tokio::pin!(run);
        tokio::select! {
            biased;
            result = &mut run => panic!("held real Session owner should keep preparation pending: {result:?}"),
            _ = tokio::task::yield_now() => {},
        }
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut run)
            .await
            .expect("cancellation must wake the actual owner wait");
        assert!(result.is_err());
    }
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert!(provider.0.lock().unwrap().is_empty());
    assert!(agent
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .is_none());
    drop(owner);
    // The canceled preparation abandons the actual DirectLease, so a complete
    // subsequent submission is accepted rather than left Busy by a second map.
    agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new("retry an actual accepted submission", selection),
        )
        .await
        .unwrap();
    assert!(provider.0.lock().unwrap().len() >= 1);
    assert_eq!(
        session
            .messages
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1
    );
}

struct LegacyCheckpointProvider {
    repository: Mutex<Option<SessionRepository>>,
    session_id: String,
    saved_before_provider: Mutex<Vec<Session>>,
    inner: NoCurrentProvider,
}
#[async_trait]
impl LLMProvider for LegacyCheckpointProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        let repository = self.repository.lock().unwrap().as_ref().unwrap().clone();
        let saved = repository
            .storage()
            .load_session(&self.session_id)
            .await
            .unwrap()
            .unwrap();
        self.saved_before_provider.lock().unwrap().push(saved);
        assert!(!tools.iter().any(|tool| matches!(
            tool.function.name.as_str(),
            "load_skill" | "read_skill_resource"
        )));
        assert!(!messages
            .iter()
            .any(|message| message.metadata.as_ref().is_some_and(|metadata| {
                serde_json::to_string(metadata)
                    .unwrap()
                    .contains("workflow_runtime")
            })));
        self.inner.chat_stream(messages, tools, max, model).await
    }
}

async fn real_loaded_instruction_history(
    agent: &Agent,
    mut session: Session,
) -> (Session, String, String) {
    use bamboo_agent_core::tools::Tool;
    use bamboo_skills::runtime_metadata::{
        SKILL_RUNTIME_ACTIVATION_GENERATION_KEY, SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY, SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
        SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    };
    let defaults = agent.sdk_skills.as_ref().unwrap();
    let session_id = session.id.clone();
    session.title = "Preserve the original legacy title".into();
    session
        .metadata
        .insert("unrelated.original.metadata".into(), "preserved".into());
    let original_user = Message::user("the old legacy selection");
    let original_user_id = original_user.id.clone();
    session.add_message(original_user);
    let selection = selections(&agent, &["sdk-loaded-history"]).await.remove(0);
    // Build the original legacy episode with the production pin and loader.
    // These are old host selection metadata, not current SDK authority/Q.
    let descriptor = defaults
        .manager
        .pin_current_activation_for_workspace(&session_id, None, &[selection.id.clone()], None)
        .await
        .unwrap();
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        json!([selection.id]).to_string(),
    );
    session
        .metadata
        .insert(SKILL_RUNTIME_SELECTION_SOURCE_KEY.into(), "explicit".into());
    session.metadata.insert(
        SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.into(),
        descriptor.catalog_revision.to_string(),
    );
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.into(),
        serde_json::to_string(&descriptor.skill_revisions).unwrap(),
    );
    session.metadata.insert(
        bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY.into(),
        serde_json::to_string(&selection).unwrap(),
    );
    let call = ToolCall {
        id: "actual-old-load-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "load_skill".into(),
            arguments: json!({"skill_id":"sdk-loaded-history"}).to_string(),
        },
    };
    session.add_message(Message::assistant("", Some(vec![call.clone()])));
    defaults.sessions.save(&mut session).await.unwrap();
    let loader = bamboo_server_tools::skill_runtime::LoadSkillTool::new(
        defaults.manager.clone(),
        defaults.config.clone(),
        defaults.sessions.clone(),
    )
    .with_project_store(defaults.projects.clone());
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&session_id);
    let outcome = loader
        .invoke(json!({"skill_id":"sdk-loaded-history"}), ctx.to_tool_ctx())
        .await
        .unwrap();
    let ToolOutcome::Completed(receipt) = outcome else {
        panic!("legacy load must complete")
    };
    let receipt_json: Value = serde_json::from_str(&receipt.result).unwrap();
    assert!(receipt.success && receipt_json["activation_status"] == "active");
    session = defaults
        .sessions
        .storage()
        .load_session(&session_id)
        .await
        .unwrap()
        .unwrap();
    let receipt_message = Message::tool_result_with_status(&call.id, receipt.result, true);
    let receipt_id = receipt_message.id.clone();
    session.add_message(receipt_message);
    defaults.sessions.save(&mut session).await.unwrap();
    assert!(session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
    assert!(session
        .metadata
        .contains_key(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
    assert!(
        session
            .metadata
            .contains_key(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY),
        "real loader without an event sender must leave its actual pending outbox"
    );
    (session, original_user_id, receipt_id)
}

#[tokio::test]
async fn sdk_real_loaded_instruction_converts_checkpointed_history_once_and_restart_is_inert() {
    use bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY;
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join("skills/sdk-loaded-history");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("SKILL.md"), "---\nname: sdk-loaded-history\ndescription: Historical loaded instructions.\n---\nOLD_COMPLETE_INSTRUCTION_HISTORY\n").unwrap();
    let session_id = "sdk-loaded-history";
    let provider = Arc::new(LegacyCheckpointProvider {
        repository: Mutex::new(None),
        session_id: session_id.into(),
        saved_before_provider: Mutex::default(),
        inner: NoCurrentProvider(Mutex::default()),
    });
    let agent = build(home.path(), provider.clone()).await;
    let (mut session, original_user_id, receipt_id) =
        real_loaded_instruction_history(&agent, agent.new_session(session_id).unwrap()).await;
    let defaults = agent.sdk_skills.as_ref().unwrap();
    *provider.repository.lock().unwrap() = Some(defaults.sessions.clone());
    let original_messages = session.messages.clone();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(128);
    // This is the real run_session execution boundary, retaining its events
    // solely to prove the old Instruction outbox cannot publish on cutover.
    agent
        .execute_internal(&mut session, event_tx, CancellationToken::new(), None)
        .await
        .unwrap();
    while let Ok(event) = event_rx.try_recv() {
        assert!(
            !matches!(
                event,
                bamboo_agent_core::AgentEvent::WorkflowActivated { .. }
            ),
            "old Instruction outbox must be inert on the SDK host surface"
        );
    }
    assert!(defaults
        .manager
        .pinned_activation_for_workspace(session_id, None)
        .await
        .unwrap()
        .is_none());
    let history = session
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Assistant
                && message.tool_calls.is_none()
                && message.content.contains("### Instructions")
                && message.content.contains("OLD_COMPLETE_INSTRUCTION_HISTORY")
        })
        .collect::<Vec<_>>();
    assert_eq!(history.len(), 1);
    let history_id = history[0].id.clone();
    let history_json = serde_json::to_value(history[0]).unwrap();
    let check_history_boundary = |snapshot: &Session| {
        let mut expected = original_messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        let original_ids = expected.clone();
        expected.push(history_id.clone());
        let actual = snapshot
            .messages
            .iter()
            .filter(|message| original_ids.contains(&message.id) || message.id == history_id)
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "original complete tool batch IDs/order and one ordinary history message must be preserved");
        for original in &original_messages {
            let retained = snapshot
                .messages
                .iter()
                .find(|message| message.id == original.id)
                .unwrap();
            assert_eq!(
                serde_json::to_value(retained).unwrap(),
                serde_json::to_value(original).unwrap()
            );
        }
        let receipt_at = snapshot
            .messages
            .iter()
            .position(|message| message.id == receipt_id)
            .unwrap();
        let history_at = snapshot
            .messages
            .iter()
            .position(|message| message.id == history_id)
            .unwrap();
        assert!(receipt_at < history_at);
        // The existing append-safe checkpoint preserves original durable IDs
        // and appends the newly configured SDK System before the new history.
        // No ordinary User, Assistant or tool result may split this boundary.
        for between in &snapshot.messages[receipt_at + 1..history_at] {
            assert_eq!(
                between.role,
                Role::System,
                "only the actual SDK System can be appended at this boundary"
            );
            assert!(!original_ids.contains(&between.id));
            assert_eq!(between.content, agent.system_prompt.as_deref().unwrap());
        }
    };
    check_history_boundary(&session);
    for saved in provider.saved_before_provider.lock().unwrap().iter() {
        check_history_boundary(saved);
        assert_eq!(
            saved
                .messages
                .iter()
                .filter(|message| message.id == history_id)
                .count(),
            1,
            "ordinary history must checkpoint before the real provider"
        );
        assert!(!saved
            .metadata
            .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
        assert!(!saved
            .metadata
            .contains_key(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY));
        assert!(!saved
            .metadata
            .contains_key(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
    }
    assert_eq!(session.title, "Preserve the original legacy title");
    assert_eq!(
        session
            .metadata
            .get("unrelated.original.metadata")
            .map(String::as_str),
        Some("preserved")
    );
    // Restart through the public SDK on the actual saved checkpoint. No fresh
    // User is created, and the old loaded receipt cannot restore a read grant.
    let saved = agent
        .storage()
        .load_session(session_id)
        .await
        .unwrap()
        .unwrap();
    let restarted_provider = Arc::new(LegacyCheckpointProvider {
        repository: Mutex::new(None),
        session_id: session_id.into(),
        saved_before_provider: Mutex::default(),
        inner: NoCurrentProvider(Mutex::default()),
    });
    let restarted_agent = build(home.path(), restarted_provider.clone()).await;
    *restarted_provider.repository.lock().unwrap() = Some(
        restarted_agent
            .sdk_skills
            .as_ref()
            .unwrap()
            .sessions
            .clone(),
    );
    let mut restarted = saved;
    restarted_agent.run_session(&mut restarted).await.unwrap();
    assert_eq!(
        restarted
            .messages
            .iter()
            .filter(|message| message.id == history_id)
            .count(),
        1
    );
    assert_eq!(
        serde_json::to_value(
            restarted
                .messages
                .iter()
                .find(|message| message.id == history_id)
                .unwrap()
        )
        .unwrap(),
        history_json
    );
    assert_eq!(
        restarted
            .messages
            .iter()
            .filter(|message| message.role == Role::User)
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>(),
        vec![original_user_id.as_str()]
    );
    assert_eq!(provider.inner.0.lock().unwrap().requests.len(), 2);
    assert_eq!(restarted_provider.inner.0.lock().unwrap().requests.len(), 2);
}

#[tokio::test]
async fn supervisor_delegation_first_sdk_legacy_history_rejects_before_conversion_and_restart() {
    for (trusted_supervisor, ultra) in [(true, false), (true, true), (false, true)] {
        for canonical_selection in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let root = home.path().join("skills/sdk-loaded-history");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("SKILL.md"), "---\nname: sdk-loaded-history\ndescription: Historical loaded instructions.\n---\nOLD_COMPLETE_INSTRUCTION_HISTORY\n").unwrap();
            let provider = Arc::new(PassiveProvider::default());
            let agent = build(home.path(), provider.clone()).await;
            let (legacy, _, _) = real_loaded_instruction_history(
                &agent,
                agent.new_session("legacy-history-fixture").unwrap(),
            )
            .await;
            let (mut session, incarnation) = if trusted_supervisor {
                let bootstrap = agent
                    .supervisor_sessions()
                    .get_or_create_default("claude-test")
                    .await
                    .unwrap();
                let session = agent
                    .storage()
                    .load_session(&bootstrap.session_id)
                    .await
                    .unwrap()
                    .unwrap();
                (session, Some(bootstrap.incarnation_id))
            } else {
                (
                    agent.new_session("ordinary-ultra-legacy-history").unwrap(),
                    None,
                )
            };
            session.set_root_orchestration_only(ultra).unwrap();
            // Model an old trusted Supervisor checkpoint with an actually loaded
            // Instruction episode. Current Supervisor tool admission stays on;
            // only the historical fixture bytes come from the production loader.
            session.messages = legacy.messages;
            session.metadata.extend(legacy.metadata);
            if canonical_selection {
                session.set_selected_skill_ids(vec!["sdk-loaded-history".into()]);
            }
            session = serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
            assert_eq!(session.supervisor_incarnation_id(), incarnation);
            assert_eq!(
                session
                    .selected_skill_ids()
                    .is_some_and(|ids| !ids.is_empty()),
                canonical_selection
            );
            let plan = bamboo_engine::session_app::plan_legacy_skill_history(
                &session.messages,
                &session.metadata,
            )
            .unwrap();
            assert!(plan.unsupported.is_none());
            assert!(plan
                .message
                .unwrap()
                .content
                .contains("OLD_COMPLETE_INSTRUCTION_HISTORY"));
            let legacy_metadata = plan
                .remove_metadata
                .iter()
                .map(|key| (key.clone(), session.metadata[key].clone()))
                .collect::<BTreeMap<_, _>>();
            let original_messages = session.messages.clone();
            agent
                .sdk_skills
                .as_ref()
                .unwrap()
                .sessions
                .save(&mut session)
                .await
                .unwrap();

            // Both attempts use fresh defaults hosts and durable reloads. An
            // error must not migrate away the evidence and make the retry pass.
            for restart in 0..2 {
                let resumed_agent = build(home.path(), provider.clone()).await;
                let mut resumed = resumed_agent
                    .storage()
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
                let result = resumed_agent.run_session(&mut resumed).await;
                assert!(result.is_err(), "Supervisor={trusted_supervisor}, Ultra={ultra}, canonical={canonical_selection}, restart={restart}: legacy Skill reached the provider");
                assert!(result.unwrap_err().to_string().contains("delegat"));
                assert!(
                    provider.0.lock().unwrap().is_empty(),
                    "legacy instructions must never reach a provider"
                );
                let durable = resumed_agent
                    .storage()
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
                let defaults = resumed_agent.sdk_skills.as_ref().unwrap();
                let cached = defaults
                    .sessions
                    .cache()
                    .get(&session.id)
                    .map(|entry| entry.read().clone());
                for snapshot in [&resumed, &durable].into_iter().chain(cached.as_ref()) {
                    for (key, value) in &legacy_metadata {
                        assert_eq!(
                            snapshot.metadata.get(key),
                            Some(value),
                            "denial must precede legacy checkpoint mutation: {key}"
                        );
                    }
                    for original in &original_messages {
                        let retained = snapshot
                            .messages
                            .iter()
                            .find(|message| message.id == original.id)
                            .unwrap();
                        assert_eq!(
                            serde_json::to_value(retained).unwrap(),
                            serde_json::to_value(original).unwrap()
                        );
                    }
                    assert!(
                        !snapshot
                            .messages
                            .iter()
                            .any(|message| message.role == Role::Assistant
                                && message.tool_calls.is_none()
                                && message.content.contains("### Instructions")
                                && message.content.contains("OLD_COMPLETE_INSTRUCTION_HISTORY")),
                        "denial must precede ordinary Skill history insertion"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn sdk_typed_registered_router_busy_cancel_and_drop_release_before_canonical_owner_retry() {
    for drop_future in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let main = write_skill(home.path(), "sdk-router-owner", false);
        // Canonical Existing Root retains its real zero-B token policy.
        // Keep the same >512KiB source across the SDK envelope ceiling fix.
        let main = format!("{main}\n{}\n", "a".repeat(512 * 1024 + 4096));
        std::fs::write(home.path().join("skills/sdk-router-owner/SKILL.md"), &main).unwrap();
        write_config(home.path());
        let task = home.path().join("router-retry-task.txt");
        std::fs::write(&task, "NORMAL_TASK_ACTION").unwrap();
        let provider = Arc::new(SkillsProvider {
            wanted: vec!["sdk-router-owner".into()],
            task,
            read_references: false,
            cap: 0,
            trace: Mutex::default(),
        });
        let router = bamboo_engine::SessionActivationRouter::new();
        let agent = Agent::builder()
            .provider(provider.clone())
            .model("claude-test")
            .instruction("Complete the requested proof task.")
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(home.path().to_owned())
            .await
            .unwrap()
            .build()
            .unwrap();
        assert!(agent.sdk_skills.is_some());
        assert!(Arc::ptr_eq(agent.activation_router().unwrap(), &router));
        assert!(agent.session_inbox().is_some() && agent.session_messenger().is_some());
        // No inbox work is admitted in this case: a direct registration does
        // not reserve a successor or need a fixture spawner.
        let target = format!("sdk-router-owner-{drop_future}");
        let mut session = agent.new_session(&target).unwrap();
        budget(&mut session, 4096);
        let defaults = agent.sdk_skills.as_ref().unwrap();
        defaults.sessions.save(&mut session).await.unwrap();
        // Reload the genuine durable Existing input after runtime-only budget state is stripped.
        session = agent
            .storage()
            .load_session(&target)
            .await
            .unwrap()
            .unwrap();
        let selections = selections(&agent, &["sdk-router-owner"]).await;
        let before = serde_json::to_value(&session).unwrap();
        let mut contender = session.clone();
        let owner = defaults.sessions.persistence().acquire_lock(&target).await;
        let cancel = CancellationToken::new();
        {
            let mut run = Box::pin(agent.run_with_skills_and_cancel(
                &mut session,
                SdkSkillInput::new(
                    "registered owner waits before accepting a User",
                    selections.clone(),
                ),
                cancel.clone(),
            ));
            let real_run_id = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut run => panic!("canonical owner must keep preparation pending: {result:?}"),
                        _ = tokio::task::yield_now() => {},
                    }
                    if let Some(run_id) = router.current_run_id(&target).await {
                        break run_id;
                    }
                }
            }).await.expect("actual typed entry must register its real router owner");
            assert!(router.owns_run(&target, &real_run_id).await);
            let busy = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                agent.run_with_skills(
                    &mut contender,
                    SdkSkillInput::new("busy contender cannot append", selections.clone()),
                ),
            )
            .await
            .expect(
                "actual competing registration must fail rather than wait for the canonical owner",
            )
            .expect_err("second exact typed run must collide with the real router owner");
            assert!(busy
                .to_string()
                .contains("session activation owner collision"));
            assert!(busy.to_string().contains(&real_run_id));
            assert!(router.owns_run(&target, &real_run_id).await);
            assert_eq!(serde_json::to_value(&contender).unwrap(), before);
            if drop_future {
                drop(run); // Real SDK future -> DirectLease -> registration Drop.
            } else {
                cancel.cancel();
                let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut run)
                    .await
                    .expect("handled cancellation must finish registered cleanup");
                assert!(result.is_err());
                drop(run);
                assert!(
                    router.current_run_id(&target).await.is_none(),
                    "handled cancellation returns only after its exact registration is released"
                );
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while router.current_run_id(&target).await.is_some() {
                    tokio::task::yield_now().await;
                }
            }).await.expect("dropped future cleanup must release registered Busy while canonical owner is still held");
            assert!(!router.owns_run(&target, &real_run_id).await);
        }
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(
            serde_json::to_value(
                agent
                    .storage()
                    .load_session(&target)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            before
        );
        assert!(provider.trace.lock().unwrap().requests.is_empty());
        drop(owner);
        agent
            .run_with_skills(
                &mut session,
                SdkSkillInput::new("retry after actual registered cleanup", selections),
            )
            .await
            .unwrap();
        assert!(router.current_run_id(&target).await.is_none());
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            1
        );
        assert_eq!(
            session
                .effective_token_budget()
                .unwrap()
                .max_tool_output_tokens,
            0,
            "registered retry must use its actual current zero-B budget"
        );
        let trace = provider.trace.lock().unwrap();
        assert!(trace.failures.is_empty(), "{:?}", trace.failures);
        assert!(trace.main_pages > 1 && trace.task_completed);
        assert_eq!(
            trace.contents[&("sdk-router-owner".into(), "SKILL.md".into())],
            main
        );
    }
}

struct SdkReservationBarrier {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl bamboo_engine::SessionActivationSpawner for SdkReservationBarrier {
    async fn reserve_activation(
        &self,
        _: &str,
        _: u64,
    ) -> Result<bamboo_engine::SessionActivationReserveOutcome, bamboo_domain::SessionActivationError>
    {
        // This controls the public host-reservation adapter's actual await.
        // The real router publishes/releases its own reservation token; this
        // adapter never installs an owner, invents a registration or grants Q.
        self.entered.notify_one();
        self.release.notified().await;
        Err(bamboo_domain::SessionActivationError::Internal(
            "fixture stops the unlaunched reservation".into(),
        ))
    }
}

#[tokio::test]
async fn sdk_typed_cancel_wakes_real_public_router_reservation_wait_without_append() {
    for already_cancelled in [false, true] {
        let home = tempfile::tempdir().unwrap();
        write_skill(home.path(), "sdk-reservation-wait", false);
        write_config(home.path());
        let provider = Arc::new(PassiveProvider::default());
        let router = bamboo_engine::SessionActivationRouter::new();
        let spawner = Arc::new(SdkReservationBarrier {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        router.set_spawner(spawner.clone()).await;
        let agent = Agent::builder()
            .provider(provider.clone())
            .model("claude-test")
            .instruction("Complete the requested proof task.")
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(home.path().to_owned())
            .await
            .unwrap()
            .build()
            .unwrap();
        assert!(agent.sdk_skills.is_some());
        assert!(Arc::ptr_eq(agent.activation_router().unwrap(), &router));
        let target = format!("sdk-reservation-wait-{already_cancelled}");
        let mut session = agent.new_session(&target).unwrap();
        let defaults = agent.sdk_skills.as_ref().unwrap();
        defaults.sessions.save(&mut session).await.unwrap();
        let selected = selections(&agent, &["sdk-reservation-wait"]).await;
        let messenger = agent.session_messenger().unwrap().clone();
        let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
            &target,
            "real durable inbox work awaiting its host reservation",
        );
        let activation = tokio::spawn(async move { messenger.send(envelope).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            spawner.entered.notified(),
        )
        .await
        .expect("real messenger admission must enter the public router reservation adapter");
        assert_eq!(
            agent
                .session_inbox()
                .unwrap()
                .inspect(&target)
                .await
                .unwrap()
                .pending,
            1
        );
        assert!(router.current_run_id(&target).await.is_none());
        let before = serde_json::to_value(&session).unwrap();
        let durable_before = agent
            .storage()
            .load_session(&target)
            .await
            .unwrap()
            .unwrap();
        let cancel = CancellationToken::new();
        if already_cancelled {
            cancel.cancel();
        }
        {
            let mut run = Box::pin(agent.run_with_skills_and_cancel(
                &mut session,
                SdkSkillInput::new(
                    "typed User must not append while registration waits",
                    selected.clone(),
                ),
                cancel.clone(),
            ));
            if !already_cancelled {
                tokio::select! {
                    biased;
                    result = &mut run => panic!("actual router reservation must keep registration pending: {result:?}"),
                    _ = tokio::task::yield_now() => {},
                }
                cancel.cancel();
            }
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut run)
                .await.expect("typed cancellation must interrupt actual register_run reservation_wait.changed without releasing the spawner");
            assert!(result.unwrap_err().to_string().contains("cancelled"));
        }
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(
            serde_json::to_value(
                agent
                    .storage()
                    .load_session(&target)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&durable_before).unwrap()
        );
        assert!(provider.0.lock().unwrap().is_empty());
        assert!(router.current_run_id(&target).await.is_none());
        assert!(
            !activation.is_finished(),
            "typed cancellation must leave the other genuine host reservation alone"
        );
        // A second genuine typed entry proves that canceling the first waiter
        // did not clear the foreign router reservation token. With a wrongly
        // cleared token it would register an owner or finish, instead of
        // remaining pending on the actual public register_run wait.
        let second_cancel = CancellationToken::new();
        {
            let mut second = Box::pin(agent.run_with_skills_and_cancel(
                &mut session,
                SdkSkillInput::new(
                    "independent typed waiter must preserve the foreign reservation",
                    selected.clone(),
                ),
                second_cancel.clone(),
            ));
            let pending =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut second).await;
            assert!(pending.is_err(), "second genuine typed entry must stay pending while the original reservation remains held");
            assert!(router.current_run_id(&target).await.is_none());
            assert!(!activation.is_finished());
            assert!(provider.0.lock().unwrap().is_empty());
            second_cancel.cancel();
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut second)
                .await.expect("independent typed waiter cancellation must wake without taking the foreign token");
            assert!(result.unwrap_err().to_string().contains("cancelled"));
        }
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(
            serde_json::to_value(
                agent
                    .storage()
                    .load_session(&target)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&durable_before).unwrap()
        );
        assert!(router.current_run_id(&target).await.is_none());
        assert!(!activation.is_finished());
        // Release only the original public spawner future. Its intentional
        // fail-closed result rolls back the real router token; durable inbox
        // admission stays present and has not become SDK caller authority.
        spawner.release.notify_one();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), activation)
            .await
            .expect("original reservation must finish rollback")
            .unwrap();
        assert!(result.is_err());
        assert!(router.current_run_id(&target).await.is_none());
        assert_eq!(
            agent
                .session_inbox()
                .unwrap()
                .inspect(&target)
                .await
                .unwrap()
                .pending,
            1
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sdk_typed_account_mismatch_stops_before_portable_hook_or_append() {
    let home = tempfile::tempdir().unwrap();
    let hook_count = install_prompt_hook(home.path(), false);
    let provider = Arc::new(PassiveProvider::default());
    let agent = build(home.path(), provider.clone()).await;
    let mut session = agent.new_session("sdk-typed-unpaired-account").unwrap();
    session.model = "other-model".into();
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new(
        "other-provider",
        "other-model",
    ));
    let before = serde_json::to_value(&session).unwrap();
    let result = agent
        .run_with_skills(
            &mut session,
            SdkSkillInput::new("Synthetic typed account witness", vec![]),
        )
        .await;
    assert!(result.is_err());
    assert!(
        !hook_count.exists(),
        "prompt hook must not run on a mismatched account"
    );
    assert!(provider.0.lock().unwrap().is_empty());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert!(agent
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .is_none());
}

#[derive(Default)]
struct SdkIdentityCapture(Mutex<Vec<String>>);

#[async_trait]
impl LLMProvider for SdkIdentityCapture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.lock().unwrap().push(model.to_string());
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Synthetic completion".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn sdk_wrapped_runtime_requires_client_for_bound_accounts_but_keeps_bare_sessions() {
    let home = tempfile::tempdir().unwrap();
    let main = Arc::new(SdkIdentityCapture::default());
    let scoped = build(home.path(), main.clone()).await;
    let agent = Agent::from_runtime(scoped.inner.clone());

    let mut bare = Session::new("sdk-wrapped-bare", "claude-test");
    bare.add_message(Message::user("Synthetic bare request"));
    agent.run_session(&mut bare).await.unwrap();
    let bare_calls = main.0.lock().unwrap().clone();
    assert!(!bare_calls.is_empty() && bare_calls.iter().all(|model| model == "claude-test"));
    main.0.lock().unwrap().clear();

    let mut bound = Session::new("sdk-wrapped-bound", "model-b");
    bound.model_ref = Some(bamboo_domain::ProviderModelRef::new(
        "synthetic-b",
        "model-b",
    ));
    bound.add_message(Message::user("Synthetic bound request"));
    let before = serde_json::to_value(&bound).unwrap();
    assert!(agent.run_session(&mut bound).await.is_err());
    assert_eq!(serde_json::to_value(&bound).unwrap(), before);
    assert!(main.0.lock().unwrap().is_empty());

    for retry_entry in [false, true] {
        let mut session = Session::new(format!("sdk-wrapped-named-{retry_entry}"), "model-b");
        session.add_message(Message::user("Synthetic named request"));
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request =
            ExecuteRequestBuilder::new("Synthetic named request", tx, CancellationToken::new())
                .model("model-b")
                .provider_name("synthetic-b")
                .build();
        let result = if retry_entry {
            agent
                .execute_with_upstream_retry(&mut session, request, UpstreamRetry::Bounded)
                .await
        } else {
            agent.execute(&mut session, request).await
        };
        drain.abort();
        assert!(result.is_err());
        assert!(main.0.lock().unwrap().is_empty());

        let other = Arc::new(SdkIdentityCapture::default());
        let mut paired = Session::new(format!("sdk-wrapped-paired-{retry_entry}"), "model-b");
        paired.model_ref = Some(bamboo_domain::ProviderModelRef::new(
            "synthetic-b",
            "model-b",
        ));
        paired.add_message(Message::user("Synthetic explicit pair"));
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request =
            ExecuteRequestBuilder::new("Synthetic explicit pair", tx, CancellationToken::new())
                .provider_name("synthetic-b")
                .provider_override(other.clone())
                .build();
        let result = if retry_entry {
            agent
                .execute_with_upstream_retry(&mut paired, request, UpstreamRetry::Bounded)
                .await
        } else {
            agent.execute(&mut paired, request).await
        };
        drain.abort();
        result.unwrap();
        assert!(main.0.lock().unwrap().is_empty());
        let other_calls = other.0.lock().unwrap().clone();
        assert!(!other_calls.is_empty() && other_calls.iter().all(|model| model == "model-b"));
    }
}

// Synthetic end-to-end SDK request capture; no external provider is used.
#[derive(Default)]
struct SdkReasoningCapture(Mutex<Vec<(String, Option<bamboo_domain::ReasoningEffort>)>>);

impl SdkReasoningCapture {
    fn reply(
        &self,
        model: &str,
        effort: Option<bamboo_domain::ReasoningEffort>,
    ) -> Result<LLMStream, LLMError> {
        self.0.lock().unwrap().push((model.to_owned(), effort));
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Synthetic completion".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[async_trait]
impl LLMProvider for SdkReasoningCapture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.reply(model, None)
    }

    async fn chat_stream_with_options(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.reply(model, options.and_then(|value| value.reasoning_effort))
    }
}

fn sdk_reasoning_config() -> bamboo_llm::Config {
    let mut config = bamboo_llm::Config::default();
    config.provider = "openai".into();
    config.default_provider_instance = Some("chat-a".into());
    config.features.provider_model_ref = true;
    *config.memory_mut() = Some(
        serde_json::from_value(json!({
            "auto_dream_enabled": false, "project_prompt_injection": false,
            "relevant_recall": false, "relevant_recall_rerank": false,
            "project_first_dream": false, "ledger_agenda_injection": false,
        }))
        .unwrap(),
    );
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat": {"provider": "chat-a", "model": "main-a", "reasoning_effort": "low"}
        }))
        .unwrap(),
    );
    config.provider_instances.insert(
        "chat-a".into(),
        serde_json::from_value(json!({
            "provider_type": "openai", "enabled": true, "model": "main-a",
            "runtime_models": ["main-a", "cheap"]
        }))
        .unwrap(),
    );
    config
}

#[tokio::test]
async fn sdk_run_session_reasoning_respects_selected_model_and_session() {
    use bamboo_domain::ReasoningEffort::{High, Low, Medium, Xhigh};
    let cases = [
        (
            "session",
            "main-a",
            Some(High),
            Some(Xhigh),
            None,
            "main-a",
            Some(High),
        ),
        (
            "ref",
            "cheap",
            None,
            Some(Xhigh),
            None,
            "cheap",
            Some(Xhigh),
        ),
        ("other-model", "cheap", None, None, None, "cheap", None),
        ("chat-role", "main-a", None, None, None, "main-a", Some(Low)),
        (
            "normalized-ref",
            " main-a ",
            None,
            Some(Xhigh),
            None,
            "main-a",
            Some(Xhigh),
        ),
        (
            "normalized-override",
            " main-a ",
            None,
            Some(Xhigh),
            Some("main-a"),
            "main-a",
            Some(Xhigh),
        ),
        (
            "explicit-model",
            "cheap",
            None,
            Some(Xhigh),
            Some("main-a"),
            "main-a",
            Some(Low),
        ),
    ];
    for (
        label,
        original_model,
        session_effort,
        target_effort,
        model_override,
        expected_model,
        expected_effort,
    ) in cases
    {
        let home = tempfile::tempdir().unwrap();
        let capture = Arc::new(SdkReasoningCapture::default());
        let mut builder = Agent::builder()
            .provider(capture.clone())
            .config(Arc::new(tokio::sync::RwLock::new(sdk_reasoning_config())))
            .tools(std::iter::empty());
        if let Some(model) = model_override {
            builder = builder.model(model);
        }
        let agent = builder
            .with_defaults_for_data_dir(home.path().to_owned())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = Session::new(format!("sdk-reasoning-{label}"), original_model);
        let mut target = bamboo_domain::ProviderModelRef::new("chat-a", original_model);
        target.reasoning_effort = target_effort;
        session.model_ref = Some(target);
        session.reasoning_effort = session_effort;
        session.add_message(Message::user("Synthetic reasoning witness"));
        agent.run_session(&mut session).await.unwrap();
        let calls = capture.0.lock().unwrap().clone();
        assert!(!calls.is_empty(), "{label}: no request");
        assert!(
            calls
                .iter()
                .all(|(model, effort)| model == expected_model && *effort == expected_effort),
            "{label}: {calls:?}"
        );
        if label == "explicit-model" {
            assert_eq!(
                session.model_ref.as_ref().unwrap().reasoning_effort,
                None,
                "{label}"
            );
        } else if label == "normalized-override" {
            assert_eq!(
                session.model_ref.as_ref().unwrap().reasoning_effort,
                Some(Xhigh),
                "{label}"
            );
        }
    }

    // The low-level API deliberately remains a request-owned escape hatch.
    let home = tempfile::tempdir().unwrap();
    let capture = Arc::new(SdkReasoningCapture::default());
    let agent = Agent::builder()
        .provider(capture.clone())
        .config(Arc::new(tokio::sync::RwLock::new(sdk_reasoning_config())))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(home.path().to_owned())
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut session = Session::new("sdk-reasoning-explicit-request", "main-a");
    session.reasoning_effort = Some(High);
    session.add_message(Message::user("Synthetic explicit request"));
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let request =
        ExecuteRequestBuilder::new("Synthetic explicit request", tx, CancellationToken::new())
            .provider_name("chat-a")
            .model("main-a")
            .reasoning_effort(Medium)
            .build();
    agent.execute(&mut session, request).await.unwrap();
    drain.abort();
    let calls = capture.0.lock().unwrap().clone();
    assert!(
        !calls.is_empty()
            && calls
                .iter()
                .all(|(model, effort)| model == "main-a" && *effort == Some(Medium)),
        "{calls:?}"
    );
}

async fn sdk_expect_reasoning(
    agent: &Agent,
    capture: &SdkReasoningCapture,
    label: &str,
    expected: Option<bamboo_domain::ReasoningEffort>,
) {
    sdk_expect_reasoning_for_provider(agent, capture, label, "chat-a", expected).await;
}

async fn sdk_expect_reasoning_for_provider(
    agent: &Agent,
    capture: &SdkReasoningCapture,
    label: &str,
    provider: &str,
    expected: Option<bamboo_domain::ReasoningEffort>,
) {
    capture.0.lock().unwrap().clear();
    let mut session = Session::new(format!("sdk-reasoning-{label}"), "main-a");
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new(provider, "main-a"));
    session.add_message(Message::user("Synthetic config snapshot witness"));
    agent.run_session(&mut session).await.unwrap();
    let calls = capture.0.lock().unwrap().clone();
    assert!(
        !calls.is_empty()
            && calls
                .iter()
                .all(|(model, effort)| model == "main-a" && *effort == expected),
        "{label}: {calls:?}"
    );
}

async fn sdk_expect_source_refusal(agent: &Agent, capture: &SdkReasoningCapture, label: &str) {
    capture.0.lock().unwrap().clear();
    let mut session = Session::new(format!("sdk-reasoning-{label}"), "main-a");
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new("openai", "main-a"));
    session.add_message(Message::user("Synthetic config source refusal"));
    let error = agent
        .run_session(&mut session)
        .await
        .expect_err("changed source must be refused");
    assert!(
        error.to_string().contains("sdk_provider_binding:"),
        "{error}"
    );
    assert!(capture.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sdk_reasoning_follows_current_config_and_builder_order() {
    use bamboo_domain::ReasoningEffort::{High, Low, Max};

    // A matching explicit model has the same Chat default in either builder order.
    for before_defaults in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let capture = Arc::new(SdkReasoningCapture::default());
        let builder = Agent::builder()
            .provider(capture.clone())
            .config(Arc::new(tokio::sync::RwLock::new(sdk_reasoning_config())))
            .tools(std::iter::empty());
        let builder = if before_defaults {
            builder.model("main-a")
        } else {
            builder
        };
        let builder = builder
            .with_defaults_for_data_dir(home.path().to_owned())
            .await
            .unwrap();
        let builder = if before_defaults {
            builder
        } else {
            builder.model("main-a")
        };
        let agent = builder.build().unwrap();
        sdk_expect_reasoning(
            &agent,
            &capture,
            &format!("builder-{before_defaults}"),
            Some(Low),
        )
        .await;
    }

    // Replacing the config after default assembly must not retain the old role effort.
    let home = tempfile::tempdir().unwrap();
    let capture = Arc::new(SdkReasoningCapture::default());
    let mut replacement = sdk_reasoning_config();
    replacement.defaults.as_mut().unwrap().chat.reasoning_effort = Some(High);
    let live = Arc::new(tokio::sync::RwLock::new(replacement));
    let agent = Agent::builder()
        .provider(capture.clone())
        .config(Arc::new(tokio::sync::RwLock::new(sdk_reasoning_config())))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(home.path().to_owned())
        .await
        .unwrap()
        .model("main-a")
        .config(live.clone())
        .build()
        .unwrap();
    sdk_expect_reasoning(&agent, &capture, "replacement", Some(High)).await;

    // Live updates apply only to the same admitted provider/model pair.
    live.write()
        .await
        .defaults
        .as_mut()
        .unwrap()
        .chat
        .reasoning_effort = Some(Low);
    sdk_expect_reasoning(&agent, &capture, "live-effort", Some(Low)).await;
    live.write().await.defaults.as_mut().unwrap().chat.model = "cheap".into();
    sdk_expect_reasoning(&agent, &capture, "live-other-model", None).await;
    {
        let mut config = live.write().await;
        config.provider_instances.insert(
            "chat-b".into(),
            serde_json::from_value(json!({
                "provider_type": "openai", "enabled": true, "model": "main-a",
                "runtime_models": ["main-a"]
            }))
            .unwrap(),
        );
        let role = &mut config.defaults.as_mut().unwrap().chat;
        role.provider = "chat-b".into();
        role.model = "main-a".into();
        role.reasoning_effort = Some(Max);
    }
    sdk_expect_reasoning(&agent, &capture, "live-other-account", None).await;
    {
        let mut config = live.write().await;
        let role = &mut config.defaults.as_mut().unwrap().chat;
        role.provider = " chat-a ".into();
        role.model = "main-a".into();
    }
    sdk_expect_reasoning(&agent, &capture, "invalid-role-provider", None).await;
    {
        let mut config = live.write().await;
        let role = &mut config.defaults.as_mut().unwrap().chat;
        role.provider = "chat-a".into();
        role.model = " main-a ".into();
    }
    sdk_expect_reasoning(&agent, &capture, "invalid-role-model", None).await;
}

#[tokio::test]
async fn sdk_reasoning_legacy_scope_does_not_take_new_instance_alias() {
    use bamboo_domain::ReasoningEffort::{Low, Max};
    let home = tempfile::tempdir().unwrap();
    let capture = Arc::new(SdkReasoningCapture::default());
    let mut config = sdk_reasoning_config();
    config.provider = "openai".into();
    config.provider_instances.clear();
    config.default_provider_instance = None;
    config.defaults.as_mut().unwrap().chat.provider = "openai".into();
    let live = Arc::new(tokio::sync::RwLock::new(config));
    let agent = Agent::builder()
        .provider(capture.clone())
        .model("main-a")
        .config(live.clone())
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(home.path().to_owned())
        .await
        .unwrap()
        .build()
        .unwrap();
    sdk_expect_reasoning_for_provider(&agent, &capture, "legacy-only", "openai", Some(Low)).await;
    live.write().await.provider_instances.insert(
        "chat-b".into(),
        serde_json::from_value(json!({
            "provider_type": "openai", "enabled": true, "model": "main-a",
            "runtime_models": ["main-a"]
        }))
        .unwrap(),
    );
    sdk_expect_reasoning_for_provider(
        &agent,
        &capture,
        "hybrid-legacy-default",
        "openai",
        Some(Low),
    )
    .await;
    {
        let mut config = live.write().await;
        config.default_provider_instance = Some("chat-b".into());
        config.defaults.as_mut().unwrap().chat.reasoning_effort = Some(Max);
    }
    sdk_expect_reasoning_for_provider(&agent, &capture, "hybrid-instance-default", "openai", None)
        .await;
    {
        let mut config = live.write().await;
        config.provider_instances.insert(
            "openai".into(),
            serde_json::from_value(json!({
                "provider_type": "openai", "enabled": true, "model": "main-a",
                "runtime_models": ["main-a"]
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("openai".into());
    }
    sdk_expect_source_refusal(&agent, &capture, "same-id-new-instance").await;
}

#[tokio::test]
async fn sdk_reasoning_instance_scope_does_not_take_same_id_legacy_role() {
    use bamboo_domain::ReasoningEffort::{Low, Max};
    let home = tempfile::tempdir().unwrap();
    let capture = Arc::new(SdkReasoningCapture::default());
    let mut config = sdk_reasoning_config();
    config.provider = "openai".into();
    config.provider_instances.clear();
    config.provider_instances.insert(
        "openai".into(),
        serde_json::from_value(json!({
            "provider_type": "openai", "enabled": true, "model": "main-a",
            "runtime_models": ["main-a"]
        }))
        .unwrap(),
    );
    config.default_provider_instance = Some("openai".into());
    config.defaults.as_mut().unwrap().chat.provider = "openai".into();
    let live = Arc::new(tokio::sync::RwLock::new(config));
    let agent = Agent::builder()
        .provider(capture.clone())
        .model("main-a")
        .config(live.clone())
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(home.path().to_owned())
        .await
        .unwrap()
        .build()
        .unwrap();
    sdk_expect_reasoning_for_provider(&agent, &capture, "instance-only", "openai", Some(Low)).await;
    {
        let mut config = live.write().await;
        config.provider_instances.remove("openai");
        config.default_provider_instance = None;
        config.defaults.as_mut().unwrap().chat.reasoning_effort = Some(Max);
    }
    sdk_expect_source_refusal(&agent, &capture, "same-id-new-legacy").await;
}
