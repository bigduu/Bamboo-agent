use super::*;
use bamboo_agent_core::tools::{Tool, ToolCtx};
use bamboo_agent_core::{Message, Session};
use bamboo_skills::SkillStoreConfig;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::RwLock;

struct Resolver(RwLock<Option<SkillCatalogCaller>>);
#[async_trait]
impl SkillCatalogCallerResolver for Resolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.0
            .read()
            .await
            .clone()
            .ok_or_else(|| ToolError::Execution("unknown actual caller".into()))
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    config: Arc<RwLock<Config>>,
    manager: Arc<SkillManager>,
    repo: bamboo_engine::SessionRepository,
    resolver: Arc<Resolver>,
    tool: SkillsListTool,
    ctx: ToolCtx,
}
impl Fixture {
    async fn new(count: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let skills = directory.path().join("skills");
        for index in 0..count {
            let id = format!("catalog-{index}");
            let root = skills.join(&id);
            std::fs::create_dir_all(&root).unwrap();
            let description =
                serde_json::to_string("界 \"quoted\" \\ escaped\ttext\nline").unwrap();
            std::fs::write(
                root.join("SKILL.md"),
                format!("---\nname: {id}\ndescription: {description}\n---\nPRIVATE BODY {id}"),
            )
            .unwrap();
            if index == 0 {
                std::fs::create_dir_all(root.join("agents")).unwrap();
                std::fs::write(
                    root.join("agents/bamboo.yaml"),
                    "invocation_policy:\n  explicit: true\n  automatic: false\n",
                )
                .unwrap();
            }
        }
        let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: skills,
            ..Default::default()
        }));
        manager.initialize().await.unwrap();
        let storage = Arc::new(bamboo_storage::JsonlStorage::new(
            directory.path().join("sessions"),
        ));
        storage.init().await.unwrap();
        let repo = bamboo_engine::SessionRepository::new(
            bamboo_engine::SessionCache::default(),
            storage.clone(),
            Arc::new(bamboo_storage::LockedSessionStore::new(storage)),
        );
        let mut session = Session::new("catalog-session", "model");
        let input = Message::user("use catalog-0");
        let input_id = input.id.clone();
        session.messages.push(input);
        session
            .metadata
            .insert("selected_skill_ids".into(), "[\"catalog-0\"]".into());
        repo.save(&mut session).await.unwrap();
        let resolver = Arc::new(Resolver(RwLock::new(Some(SkillCatalogCaller {
            caller_id: "known-sdk-caller".into(),
            session_id: session.id.clone(),
            input_id: input_id.clone(),
            ceiling: Some((0..count).map(|index| format!("catalog-{index}")).collect()),
            invocation: Some(SkillCatalogInvocation {
                input_id,
                skills: BTreeSet::from(["catalog-0".into()]),
            }),
            mode: None,
            context_window: Some(100_000),
            metadata_tokens: None,
            response_bytes: 8_000,
        }))));
        let config = Arc::new(RwLock::new(Config::default()));
        let tool = SkillsListTool::new(
            manager.clone(),
            config.clone(),
            repo.clone(),
            resolver.clone(),
        );
        let mut ctx = ToolCtx::none("same-call-id");
        ctx.session_id = Some(session.id.into());
        Self {
            _directory: directory,
            config,
            manager,
            repo,
            resolver,
            tool,
            ctx,
        }
    }
    async fn assert_render_matches_list(&self) {
        let (_, page) = self.page(None, 20).await.unwrap();
        let rendered = self.tool.render_catalog(&self.ctx).await.unwrap();
        let names = page["skills"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rendered.included_count, names.len());
        assert_eq!(
            rendered.omitted_count, 0,
            "budget must fit every eligible entry"
        );
        let rendered_names = rendered
            .text
            .lines()
            .filter_map(|line| {
                // Root-alias table rows are not Skill metadata lines.
                line.strip_prefix("- ")
                    .filter(|rest| !rest.starts_with('`'))
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(name, _)| name)
            })
            .collect::<Vec<_>>();
        assert_eq!(rendered_names, names);
        assert!(!rendered.text.contains("PRIVATE BODY"));
    }

    async fn page(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(bamboo_agent_core::tools::ToolResult, Value), ToolError> {
        let result = self
            .tool
            .invoke(json!({"cursor":cursor,"limit":limit}), self.ctx.clone())
            .await?
            .into_tool_result();
        let page = serde_json::from_str(&result.result).unwrap();
        assert!(!result.result.contains("PRIVATE BODY"));
        Ok((result, page))
    }
}

#[tokio::test]
async fn catalog_list_distinguishes_actual_callers_and_rejects_wide_cursor_replay() {
    let fixture = Fixture::new(3).await;
    let before = fixture.repo.load("catalog-session").await.unwrap();
    fixture.assert_render_matches_list().await;
    let (_, wide) = fixture.page(None, 1).await.unwrap();
    let cursor = wide["next_cursor"].as_str().unwrap();
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = Some(BTreeSet::new());
    fixture.assert_render_matches_list().await;
    assert!(fixture.page(Some(cursor), 1).await.is_err());
    assert!(fixture.page(None, 1).await.unwrap().1["skills"]
        .as_array()
        .unwrap()
        .is_empty());
    {
        let mut state = fixture.resolver.0.write().await;
        let caller = state.as_mut().unwrap();
        caller.caller_id = "another-actual-caller".into();
        caller.ceiling = Some(BTreeSet::from(["catalog-1".into()]));
    }
    assert_eq!(
        fixture.page(None, 1).await.unwrap().1["skills"][0]["package"],
        "catalog-1"
    );
    fixture.assert_render_matches_list().await;
    assert!(fixture.page(Some(cursor), 1).await.is_err());
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None;
    fixture.assert_render_matches_list().await;
    assert!(!fixture.page(None, 20).await.unwrap().1["skills"]
        .as_array()
        .unwrap()
        .is_empty());
    *fixture.resolver.0.write().await = None;
    assert!(fixture.tool.render_catalog(&fixture.ctx).await.is_err());
    assert!(fixture.page(None, 1).await.is_err());
    assert!(fixture.page(Some(cursor), 1).await.is_err());
    let after = fixture.repo.load("catalog-session").await.unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
}

#[tokio::test]
async fn catalog_list_fresh_input_config_and_ultra_revoke_metadata() {
    let fixture = Fixture::new(3).await;
    let (_, page) = fixture.page(None, 1).await.unwrap();
    let cursor = page["next_cursor"].as_str().unwrap();
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    let next = Message::user("new input without a Skill mention");
    fixture.resolver.0.write().await.as_mut().unwrap().input_id = next.id.clone();
    session.messages.push(next);
    fixture.repo.save(&mut session).await.unwrap();
    // An intent for N cannot be attached to accepted input N+1.
    assert!(fixture.page(None, 20).await.is_err());
    assert!(fixture.tool.render_catalog(&fixture.ctx).await.is_err());
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .invocation = None;
    fixture.assert_render_matches_list().await;
    let (_, current) = fixture.page(None, 20).await.unwrap();
    assert!(current["skills"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["package"] != "catalog-0"));
    assert!(fixture.page(Some(cursor), 1).await.is_err());
    fixture.config.write().await.skills.disabled = vec!["catalog-1".into(), "catalog-2".into()];
    fixture.assert_render_matches_list().await;
    assert!(fixture.page(None, 20).await.unwrap().1["skills"]
        .as_array()
        .unwrap()
        .is_empty());
    fixture.config.write().await.skills.disabled.clear();
    session.root_orchestration_only = true;
    session.root_tool_authority_revision += 1;
    fixture.repo.save(&mut session).await.unwrap();
    fixture.assert_render_matches_list().await;
    assert!(fixture.page(None, 20).await.unwrap().1["skills"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(fixture.page(Some(cursor), 1).await.is_err());
}

#[tokio::test]
async fn catalog_list_rejects_malformed_arguments_restrictions_and_unavailable_project() {
    let fixture = Fixture::new(2).await;
    for args in [
        json!({"limit":0}),
        json!({"limit":21}),
        json!({"ceiling":null}),
        json!({"cursor":"x".repeat(2049)}),
    ] {
        assert!(fixture
            .tool
            .invoke(args, fixture.ctx.clone())
            .await
            .is_err());
    }
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling =
        Some(BTreeSet::from(["../outside".into()]));
    assert!(fixture.page(None, 20).await.is_err());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None;
    fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("../invalid-mode".into());
    assert!(fixture.page(None, 20).await.is_err());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    fixture.resolver.0.write().await.as_mut().unwrap().mode = None;
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    session.set_project_id_meta("assigned-but-unavailable");
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    assert!(fixture.page(None, 20).await.is_err());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
}

#[tokio::test]
async fn catalog_list_pages_all_metadata_with_real_provider_cache_envelopes() {
    use bamboo_llm::cache::{CacheTtl, PromptCachePlan};
    use bamboo_llm::providers::{
        anthropic::build_anthropic_request_with_cache,
        common::openai_responses::build_responses_body,
    };
    let mut fixture = Fixture::new(27).await;
    fixture.ctx.tool_call_id = "真实\"call\\id".repeat(8).into();
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 1_500;
    let mut cursor = None::<String>;
    let mut packages = BTreeSet::new();
    let mut pages = 0;
    loop {
        let (result, page) = fixture.page(cursor.as_deref(), 20).await.unwrap();
        let entries = page["skills"].as_array().unwrap();
        assert!(!entries.is_empty() && entries.len() <= 20);
        for entry in entries {
            assert!(packages.insert(entry["package"].as_str().unwrap().to_string()));
        }
        assert!(serde_json::to_vec(&result).unwrap().len() <= 1_500);
        let message = Message::tool_result_with_status(
            fixture.ctx.tool_call_id.as_ref(),
            &result.result,
            true,
        );
        let neighbor = Message::tool_result("neighbor", "unrelated neighbor output");
        let calls = serde_json::from_value(json!([
            {"id":fixture.ctx.tool_call_id,"type":"function","function":{"name":"skills_list","arguments":"{}"}},
            {"id":"neighbor","type":"function","function":{"name":"neighbor","arguments":"{}"}}
        ])).unwrap();
        let messages = [
            Message::assistant("", Some(calls)),
            message.clone(),
            neighbor,
        ];
        for ttl in [CacheTtl::Default, CacheTtl::Extended] {
            let plan = PromptCachePlan {
                breakpoint_message_ids: vec![message.id.clone()],
                ttl,
                ..Default::default()
            };
            let request = build_anthropic_request_with_cache(
                &messages,
                &[],
                "claude-test",
                64,
                false,
                None,
                None,
                Some(&plan),
            );
            let blocks = request["messages"][1]["content"].as_array().unwrap();
            assert_eq!(blocks.len(), 2, "neighbor result shares the user message");
            let block = &blocks[0];
            assert_eq!(block["tool_use_id"], fixture.ctx.tool_call_id.as_ref());
            assert_eq!(block["cache_control"]["type"], "ephemeral");
            assert_eq!(block["cache_control"]["ttl"].as_str(), ttl.anthropic_ttl());
            assert_eq!(
                serde_json::from_str::<Value>(block["content"].as_str().unwrap()).unwrap(),
                page
            );
            assert!(serde_json::to_vec(block).unwrap().len() <= 1_500);
            assert!(blocks[1].get("cache_control").is_none());
        }
        for cached in [false, true] {
            let plan = PromptCachePlan {
                breakpoint_message_ids: vec![message.id.clone()],
                ..Default::default()
            };
            let request = build_responses_body(
                "gpt-6",
                &messages,
                &[],
                None,
                None,
                None,
                None,
                cached.then_some(&plan),
            );
            let block = request["input"]
                .as_array()
                .unwrap()
                .iter()
                .find(|block| {
                    block["type"] == "function_call_output"
                        && block["call_id"] == fixture.ctx.tool_call_id.as_ref()
                })
                .unwrap();
            let output = if cached {
                assert_eq!(
                    block["output"][0]["prompt_cache_breakpoint"]["mode"],
                    "explicit"
                );
                block["output"][0]["text"].as_str().unwrap()
            } else {
                block["output"].as_str().unwrap()
            };
            assert_eq!(serde_json::from_str::<Value>(output).unwrap(), page);
            assert!(serde_json::to_vec(block).unwrap().len() <= 1_500);
        }
        pages += 1;
        let next = page["next_cursor"].as_str().map(str::to_owned);
        if let Some(next) = &next {
            let offset = next.split_once(':').unwrap().1.parse::<usize>().unwrap();
            assert_eq!(offset, packages.len(), "successful nonfinal page advances");
            assert_ne!(cursor.as_ref(), Some(next));
        } else {
            break;
        }
        cursor = next;
        assert!(pages <= 27);
    }
    assert_eq!(packages.len(), 27);
    assert!(pages > 2, "wire budget rather than count alone split pages");
    for budget in [0, 1, 300] {
        fixture
            .resolver
            .0
            .write()
            .await
            .as_mut()
            .unwrap()
            .response_bytes = budget;
        assert!(
            fixture.page(None, 20).await.is_err(),
            "indivisible entry/tiny envelope is an error"
        );
    }
}

#[tokio::test]
async fn catalog_cursor_tracks_current_source_policy_authority() {
    let fixture = Fixture::new(3).await;
    fixture.assert_render_matches_list().await;
    let (_, page) = fixture.page(None, 1).await.unwrap();
    let cursor = page["next_cursor"].as_str().unwrap();
    let root = fixture._directory.path().join("skills/catalog-0");
    let file = root.join("SKILL.md");
    let raw = std::fs::read_to_string(&file).unwrap();
    std::fs::write(&file, format!("{raw}\n ")).unwrap();
    assert!(
        fixture.page(Some(cursor), 1).await.is_err(),
        "raw bytes invalidate cursors even when metadata stays equal"
    );
    let (_, page) = fixture.page(None, 1).await.unwrap();
    let cursor = page["next_cursor"].as_str().unwrap();
    std::fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: false\n  automatic: false\n",
    )
    .unwrap();
    assert!(fixture.page(Some(cursor), 1).await.is_err());
    let (_, page) = fixture.page(None, 20).await.unwrap();
    assert_eq!(page["skills"].as_array().unwrap().len(), 2);
    fixture.assert_render_matches_list().await;
}

#[tokio::test]
async fn catalog_list_uses_one_session_snapshot_across_workspace_aba() {
    let mut fixture = Fixture::new(2).await;
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .invocation = None;
    let workspace_a = fixture._directory.path().join("workspace-a");
    let workspace_b = fixture._directory.path().join("workspace-b");
    std::fs::create_dir_all(&workspace_a).unwrap();
    let foreign = workspace_b.join(".bamboo/skills/catalog-0");
    std::fs::create_dir_all(&foreign).unwrap();
    std::fs::write(
        foreign.join("SKILL.md"),
        "---\nname: catalog-0\ndescription: FOREIGN_WORKSPACE_B\n---\nFOREIGN PRIVATE BODY",
    )
    .unwrap();
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    session.set_workspace_path_meta(workspace_a.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    let barrier = Arc::new((tokio::sync::Notify::new(), tokio::sync::Notify::new()));
    fixture.tool.store_selection_barrier = Some(barrier.clone());

    // The config guard parks the real invocation after its first cached Session
    // snapshot. The second barrier is after actual workspace-store resolution,
    // so the fixture never relies on filesystem worker scheduling or sleeps.
    let config_guard = fixture.config.write().await;
    let invocation = fixture.tool.invoke(json!({}), fixture.ctx.clone());
    tokio::pin!(invocation);
    assert!(futures::poll!(&mut invocation).is_pending());
    session.set_workspace_path_meta(workspace_b.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    drop(config_guard);
    tokio::select! {
        _ = barrier.0.notified() => {},
        result = &mut invocation => panic!("store-selection barrier was not reached: {result:?}"),
    }
    session.set_workspace_path_meta(workspace_a.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    barrier.1.notify_one();
    let result = invocation.await.unwrap().into_tool_result();
    let page: Value = serde_json::from_str(&result.result).unwrap();
    let current = fixture.repo.load("catalog-session").await.unwrap();
    assert_eq!(
        current.workspace_path_meta().as_deref(),
        Some(workspace_a.to_string_lossy().as_ref())
    );
    assert!(
        page["skills"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["package"] != "catalog-0"),
        "workspace B's automatic override cannot appear under workspace A authority: {page}"
    );
    assert!(!result.result.contains("FOREIGN_WORKSPACE_B"));
    assert_eq!(page["skills"].as_array().unwrap().len(), 1);
    assert_eq!(page["skills"][0]["package"], "catalog-1");
    assert!(!result.result.contains("PRIVATE BODY"));
}

#[tokio::test]
async fn selected_source_current_caller_ceiling_and_input_not_old_selection_are_authority() {
    let fixture = Fixture::new(3).await;
    let bytes = fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .unwrap();
    assert!(bytes.snapshot.contents().contains("PRIVATE BODY catalog-0"));
    fixture
        .tool
        .probe_selected_source(&fixture.ctx, &bytes)
        .await
        .unwrap();
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = Some(BTreeSet::new());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_err());
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &bytes)
        .await
        .is_err());
    {
        let mut state = fixture.resolver.0.write().await;
        let caller = state.as_mut().unwrap();
        caller.caller_id = "another-host".into();
        caller.ceiling = Some(BTreeSet::from(["catalog-1".into()]));
    }
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_err());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_ok());
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None;
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    let input = Message::user("new input with no mention");
    fixture.resolver.0.write().await.as_mut().unwrap().input_id = input.id.clone();
    session.messages.push(input);
    fixture.repo.save(&mut session).await.unwrap();
    assert!(
        fixture
            .tool
            .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
            .await
            .is_err(),
        "stale Input N intent"
    );
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .invocation = None;
    assert!(
        fixture
            .tool
            .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
            .await
            .is_err(),
        "stale Session.selected_skill_ids is not intent"
    );
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &bytes)
        .await
        .is_err());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_ok());
    *fixture.resolver.0.write().await = None;
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &bytes)
        .await
        .is_err());
}

#[tokio::test]
async fn selected_source_host_denies_config_ultra_mode_project_and_physical_changes() {
    let fixture = Fixture::new(2).await;
    let held = fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .unwrap();
    let root = fixture._directory.path().join("skills/catalog-0");
    std::fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: false\n  automatic: true\n",
    )
    .unwrap();
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_err());
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &held)
        .await
        .is_err());
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .invocation = None;
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_ok());
    std::fs::write(
        root.join("agents/openai.yaml"),
        "policy:\n  allow_implicit_invocation: false\n",
    )
    .unwrap();
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_err());
    fixture.config.write().await.skills.disabled = vec!["catalog-1".into()];
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    fixture.config.write().await.skills.disabled.clear();
    let permitted = fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .unwrap();
    fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("different".into());
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &permitted)
        .await
        .is_err());
    fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("../bad".into());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    fixture.resolver.0.write().await.as_mut().unwrap().mode = None;
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    session.root_orchestration_only = true;
    session.root_tool_authority_revision += 1;
    fixture.repo.save(&mut session).await.unwrap();
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &permitted)
        .await
        .is_err());
    session.root_orchestration_only = false;
    session.set_project_id_meta("unavailable-project");
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .is_err());
    session.clear_project_id_meta();
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    let physical = fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md")
        .await
        .unwrap();
    fixture
        .tool
        .probe_selected_source(&fixture.ctx, &physical)
        .await
        .unwrap();
    let main = fixture._directory.path().join("skills/catalog-1/SKILL.md");
    std::fs::rename(&main, main.with_extension("previous")).unwrap();
    std::fs::write(&main, physical.snapshot.contents()).unwrap();
    assert!(fixture
        .tool
        .probe_selected_source(&fixture.ctx, &physical)
        .await
        .is_err());
}

struct FinalResolver {
    caller: SkillCatalogCaller,
    calls: std::sync::atomic::AtomicUsize,
    repo: bamboo_engine::SessionRepository,
    unavailable: bool,
}
#[async_trait]
impl SkillCatalogCallerResolver for FinalResolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
            if self.unavailable {
                return Err(ToolError::Execution("caller revoked during read".into()));
            }
            let mut session = self.repo.load("catalog-session").await.unwrap();
            session.root_orchestration_only = true;
            session.root_tool_authority_revision += 1;
            self.repo.save(&mut session).await.unwrap();
        }
        Ok(self.caller.clone())
    }
}

#[tokio::test]
async fn selected_source_final_async_resolver_cannot_hide_new_ultra_or_error() {
    for unavailable in [false, true] {
        let fixture = Fixture::new(1).await;
        let caller = fixture.resolver.0.read().await.clone().unwrap();
        let tool = SkillsListTool::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
            Arc::new(FinalResolver {
                caller,
                calls: Default::default(),
                repo: fixture.repo.clone(),
                unavailable,
            }),
        );
        assert!(
            tool.selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
                .await
                .is_err(),
            "late caller/scope revocation {unavailable}"
        );
    }
}

struct FinalConfigResolver {
    caller: SkillCatalogCaller,
    calls: std::sync::atomic::AtomicUsize,
    config: Arc<RwLock<Config>>,
    held: std::sync::Mutex<Option<tokio::sync::OwnedRwLockWriteGuard<Config>>>,
    entered: tokio::sync::Notify,
}
#[async_trait]
impl SkillCatalogCallerResolver for FinalConfigResolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
            let guard = self.config.clone().write_owned().await;
            *self.held.lock().unwrap() = Some(guard);
            self.entered.notify_one();
        }
        Ok(self.caller.clone())
    }
}

#[tokio::test]
async fn selected_source_and_probe_final_config_wait_cannot_hide_saved_ultra() {
    let mut grants = Vec::new();
    for probe in [false, true] {
        let fixture = Fixture::new(1).await;
        // Warm the actual store and Session cache, and hold real previously selected bytes.
        let selected = fixture
            .tool
            .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
            .await
            .unwrap();
        assert!(selected.snapshot.contents().contains("PRIVATE BODY"));
        let mut session = fixture.repo.load("catalog-session").await.unwrap();
        let resolver = Arc::new(FinalConfigResolver {
            caller: fixture.resolver.0.read().await.clone().unwrap(),
            calls: Default::default(),
            config: fixture.config.clone(),
            held: Default::default(),
            entered: Default::default(),
        });
        let tool = SkillsListTool::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
            resolver.clone(),
        );
        let request = async {
            if probe {
                tool.probe_selected_source(&fixture.ctx, &selected)
                    .await
                    .map(|()| "probe granted".to_string())
            } else {
                tool.selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
                    .await
                    .map(|value| value.snapshot.contents().to_string())
            }
        };
        tokio::pin!(request);
        tokio::select! {
            _ = resolver.entered.notified() => {},
            result = &mut request => panic!("final config wait not reached: {result:?}"),
        }
        assert!(futures::poll!(&mut request).is_pending());
        assert_eq!(resolver.calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        session.root_orchestration_only = true;
        session.root_tool_authority_revision += 1;
        fixture.repo.save(&mut session).await.unwrap();
        drop(resolver.held.lock().unwrap().take().unwrap());
        if let Ok(contents) = request.await {
            grants.push((probe, contents));
        }
    }
    assert!(
        grants.is_empty(),
        "final config wait leaked grants: {grants:?}"
    );
}

#[tokio::test]
async fn selected_source_same_session_store_is_not_relooked_up_during_aba() {
    let mut fixture = Fixture::new(2).await;
    let workspace_a = fixture._directory.path().join("workspace-a");
    let workspace_b = fixture._directory.path().join("workspace-b");
    std::fs::create_dir_all(&workspace_a).unwrap();
    let foreign = workspace_b.join(".bamboo/skills/catalog-1");
    std::fs::create_dir_all(&foreign).unwrap();
    std::fs::write(
        foreign.join("SKILL.md"),
        "---\nname: catalog-1\ndescription: foreign\n---\nFOREIGN BODY",
    )
    .unwrap();
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    session.set_workspace_path_meta(workspace_a.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    let barrier = Arc::new((tokio::sync::Notify::new(), tokio::sync::Notify::new()));
    fixture.tool.store_selection_barrier = Some(barrier.clone());
    let guard = fixture.config.write().await;
    let selection = fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-1", "SKILL.md");
    tokio::pin!(selection);
    assert!(futures::poll!(&mut selection).is_pending());
    session.set_workspace_path_meta(workspace_b.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    drop(guard);
    tokio::select! {
        _ = barrier.0.notified() => {},
        result = &mut selection => panic!("store barrier not reached: {}", result.is_ok()),
    }
    session.set_workspace_path_meta(workspace_a.to_string_lossy());
    session.metadata_version += 1;
    fixture.repo.save(&mut session).await.unwrap();
    barrier.1.notify_one();
    let result = selection.await.unwrap();
    assert!(result
        .snapshot
        .contents()
        .contains("PRIVATE BODY catalog-1"));
    assert!(!result.snapshot.contents().contains("FOREIGN BODY"));
}
