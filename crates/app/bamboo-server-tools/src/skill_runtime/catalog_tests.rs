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
    fn validate_current(
        &self,
        ctx: &ToolCtx,
        expected: &SkillCatalogCaller,
    ) -> Result<(), ToolError> {
        let current = self
            .0
            .try_read()
            .map_err(|_| ToolError::Execution("current actual caller is busy".into()))?;
        validate_fixture_caller(ctx, current.as_ref(), expected)
    }
}

fn validate_fixture_caller(
    ctx: &ToolCtx,
    current: Option<&SkillCatalogCaller>,
    expected: &SkillCatalogCaller,
) -> Result<(), ToolError> {
    let current = current.ok_or_else(|| ToolError::Execution("unknown actual caller".into()))?;
    let snapshot = |caller: &SkillCatalogCaller| {
        serde_json::to_value(caller).map_err(|error| ToolError::Execution(error.to_string()))
    };
    if ctx.session_id() != Some(current.session_id.as_str())
        || snapshot(current)? != snapshot(expected)?
    {
        return Err(ToolError::Execution("current actual caller changed".into()));
    }
    Ok(())
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
            std::fs::create_dir_all(root.join("references")).unwrap();
            std::fs::write(root.join("references/empty.txt"), "").unwrap();
            std::fs::write(
                root.join("references/raw.txt"),
                "界🦀\r\n\0\"\\aux".repeat(200),
            )
            .unwrap();
            std::fs::write(root.join("references/bad.bin"), [b'a', 0xff]).unwrap();
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

#[tokio::test]
async fn skills_read_real_invoke_obeys_escaped_envelope_budget_and_advances() {
    let fixture = Fixture::new(1).await;
    let path = fixture._directory.path().join("skills/catalog-0/SKILL.md");
    let mut raw = std::fs::read_to_string(&path).unwrap();
    raw.push_str(&"界🦀\r\n\"\\\0".repeat(200));
    std::fs::write(&path, raw).unwrap();
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 350;
    let tool = SkillsReadTool::new(SkillsListTool::new(
        fixture.manager.clone(),
        fixture.config.clone(),
        fixture.repo.clone(),
        fixture.resolver.clone(),
    ));
    let result = tool
        .invoke(json!({"package":"catalog-0"}), fixture.ctx.clone())
        .await
        .unwrap()
        .into_tool_result();
    assert!(serde_json::to_vec(&json!({"type":"function_call_output","call_id":fixture.ctx.tool_call_id,"output":result.result})).unwrap().len() <= 350, "read must budget actual nested provider envelopes");
    let page: Value = serde_json::from_str(&result.result).unwrap();
    assert!(!page["contents"].as_str().unwrap().is_empty());
    assert!(
        page["next_cursor"].as_str().is_some(),
        "partial read must advance to true EOF"
    );
}

fn reader(fixture: &Fixture) -> SkillsReadTool {
    SkillsReadTool::new(SkillsListTool::new(
        fixture.manager.clone(),
        fixture.config.clone(),
        fixture.repo.clone(),
        fixture.resolver.clone(),
    ))
}
async fn read_page(
    tool: &SkillsReadTool,
    ctx: &ToolCtx,
    package: &str,
    resource: &str,
    cursor: Option<&str>,
) -> Result<(bamboo_agent_core::tools::ToolResult, Value), ToolError> {
    let result = tool
        .invoke(
            json!({"package":package,"resource":resource,"cursor":cursor}),
            ctx.clone(),
        )
        .await?
        .into_tool_result();
    let page = serde_json::from_str(&result.result).unwrap();
    Ok((result, page))
}
fn assert_read_provider_blocks(
    result: &bamboo_agent_core::tools::ToolResult,
    ctx: &ToolCtx,
    budget: usize,
) {
    use bamboo_llm::cache::{CacheTtl, PromptCachePlan};
    use bamboo_llm::providers::{
        anthropic::build_anthropic_request_with_cache,
        common::openai_responses::build_responses_body,
    };
    assert!(serde_json::to_vec(result).unwrap().len() <= budget);
    let message = Message::tool_result_with_status(ctx.tool_call_id.as_ref(), &result.result, true);
    let calls = serde_json::from_value(json!([
        {"id":ctx.tool_call_id,"type":"function","function":{"name":"skills_read","arguments":"{}"}},
        {"id":"neighbor","type":"function","function":{"name":"neighbor","arguments":"{}"}}
    ])).unwrap();
    let messages = [
        Message::assistant("", Some(calls)),
        message.clone(),
        Message::tool_result("neighbor", "unrelated"),
    ];
    let mut largest = serde_json::to_vec(result).unwrap().len();
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
        assert_eq!(
            blocks.len(),
            2,
            "neighbor results merge into the same user message"
        );
        let block = &blocks[0];
        assert_eq!(block["tool_use_id"], ctx.tool_call_id.as_ref());
        assert_eq!(block["content"], result.result);
        assert_eq!(block["cache_control"]["ttl"].as_str(), ttl.anthropic_ttl());
        assert!(blocks[1].get("cache_control").is_none());
        let cost = serde_json::to_vec(block).unwrap().len();
        assert!(
            cost <= budget,
            "actual block {cost} > budget {budget}; measured={}: {block}",
            super::catalog::page_size(result, &ctx.tool_call_id).unwrap()
        );
        largest = largest.max(cost);
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
                    && block["call_id"] == ctx.tool_call_id.as_ref()
            })
            .unwrap();
        let text = if cached {
            block["output"][0]["text"].as_str().unwrap()
        } else {
            block["output"].as_str().unwrap()
        };
        assert_eq!(text, result.result);
        let cost = serde_json::to_vec(block).unwrap().len();
        assert!(
            cost <= budget,
            "actual block {cost} > budget {budget}; measured={}: {block}",
            super::catalog::page_size(result, &ctx.tool_call_id).unwrap()
        );
        largest = largest.max(cost);
    }
    let chat =
        bamboo_llm::providers::common::openai_compat::messages_to_openai_compat_json(&messages);
    let block = chat
        .iter()
        .find(|v| v["tool_call_id"] == ctx.tool_call_id.as_ref())
        .unwrap();
    assert_eq!(block["content"], result.result);
    let cost = serde_json::to_vec(block).unwrap().len();
    assert!(cost <= budget);
    largest = largest.max(cost);
    use bamboo_llm::protocol::{gemini::GeminiRequest, ToProvider};
    let gemini: GeminiRequest = messages.to_vec().to_provider().unwrap();
    let gemini = serde_json::to_value(gemini).unwrap();
    let block = gemini["contents"][1]["parts"][0].clone();
    assert_eq!(block["functionResponse"]["name"], ctx.tool_call_id.as_ref());
    assert_eq!(
        block["functionResponse"]["response"],
        serde_json::from_str::<Value>(&result.result).unwrap()
    );
    let cost = serde_json::to_vec(&block).unwrap().len();
    assert!(cost <= budget);
    largest = largest.max(cost);
    assert_eq!(
        super::catalog::page_size(result, &ctx.tool_call_id).unwrap(),
        largest,
        "counter must equal actual largest public-converter block"
    );
}

#[tokio::test]
async fn skills_read_all_four_real_provider_envelopes_concatenate_raw_files_to_true_eof() {
    let mut fixture = Fixture::new(1).await;
    fixture.ctx.tool_call_id = "真实\"call\\id".repeat(8).into();
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 1500;
    let tool = reader(&fixture);
    for resource in ["SKILL.md", "references/raw.txt", "references/empty.txt"] {
        let expected = std::fs::read_to_string(
            fixture
                ._directory
                .path()
                .join("skills/catalog-0")
                .join(resource),
        )
        .unwrap();
        let mut cursor = None::<String>;
        let mut joined = String::new();
        let mut pages = 0;
        loop {
            let (result, page) = read_page(
                &tool,
                &fixture.ctx,
                "catalog-0",
                resource,
                cursor.as_deref(),
            )
            .await
            .unwrap();
            assert_read_provider_blocks(&result, &fixture.ctx, 1500);
            assert_eq!(page["package"], "catalog-0");
            assert_eq!(page["resource"], resource);
            let contents = page["contents"].as_str().unwrap();
            joined.push_str(contents);
            pages += 1;
            if let Some(next) = page["next_cursor"].as_str() {
                assert!(!contents.is_empty());
                assert_ne!(cursor.as_deref(), Some(next));
                let offset = next.rsplit(':').next().unwrap().parse::<usize>().unwrap();
                assert_eq!(offset, joined.len());
                assert!(expected.is_char_boundary(offset) && offset < expected.len());
                cursor = Some(next.to_owned());
            } else {
                assert_eq!(
                    joined, expected,
                    "None means real complete EOF including raw CRLF/NUL"
                );
                break;
            }
            assert!(pages <= expected.len() + 1);
        }
        if resource == "references/raw.txt" {
            assert!(pages > 2);
        }
        if expected.is_empty() {
            assert_eq!(pages, 1);
        }
    }
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-0", "references/bad.bin", None)
            .await
            .is_err(),
        "whole-file UTF8 validation precedes any first prefix page"
    );
}

#[tokio::test]
async fn skills_read_tiny_indivisible_empty_eof_handles_and_flat_schema() {
    let fixture = Fixture::new(1).await;
    let tool = reader(&fixture);
    assert_eq!(tool.name(), "skills_read");
    assert_eq!(tool.parameters_schema()["additionalProperties"], false);
    for args in [
        json!({}),
        json!({"package":"catalog-0","scope":"Global"}),
        json!({"package":""}),
        json!({"package":"catalog-0\n"}),
        json!({"package":"a".repeat(2049)}),
        json!({"package":"catalog-0","resource":""}),
        json!({"package":"catalog-0","cursor":""}),
        json!({"package":"catalog-0","resource":"../outside"}),
        json!({"package":"catalog-0","resource":"/ambient/SKILL.md"}),
    ] {
        assert!(tool.invoke(args, fixture.ctx.clone()).await.is_err());
    }
    for budget in [0, 1, 150] {
        fixture
            .resolver
            .0
            .write()
            .await
            .as_mut()
            .unwrap()
            .response_bytes = budget;
        for resource in ["SKILL.md", "references/empty.txt"] {
            assert!(read_page(&tool, &fixture.ctx, "catalog-0", resource, None)
                .await
                .is_err());
        }
    }
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 8_000;
    let (empty, _) = read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/empty.txt",
        None,
    )
    .await
    .unwrap();
    let eof_budget = super::catalog::page_size(&empty, &fixture.ctx.tool_call_id).unwrap();
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = eof_budget;
    assert!(
        read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/empty.txt",
            None
        )
        .await
        .is_ok(),
        "exact empty EOF envelope fits"
    );
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .is_err(),
        "an indivisible first UTF8 character plus continuation cannot fit that envelope"
    );
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 350;
    let (_, page) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
        .await
        .unwrap();
    let cursor = page["next_cursor"].as_str().unwrap();
    let prefix = cursor.rsplit_once(':').unwrap().0;
    for malformed in [
        "not-a-cursor".into(),
        format!("{prefix}:99999999"),
        format!("{prefix}:+1"),
        format!("{prefix}:2"),
        format!("{prefix}:0:1"),
    ] {
        assert!(read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            Some(&malformed)
        )
        .await
        .is_err());
    }
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-0", "SKILL.md", Some(cursor))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn skills_read_warm_cache_rejects_fresh_actual_caller_input_and_host_denies() {
    let fixture = Fixture::new(2).await;
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 350;
    let tool = reader(&fixture);
    let configured = fixture.resolver.0.read().await.clone().unwrap();
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None;
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .is_ok(),
        "None is unrestricted only for a resolved known caller"
    );
    *fixture.resolver.0.write().await = Some(configured);
    let (_, page) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
        .await
        .unwrap();
    let cursor = page["next_cursor"].as_str().unwrap();
    let original = fixture.resolver.0.read().await.clone().unwrap();
    for ceiling in [
        Some(BTreeSet::new()),
        Some(BTreeSet::from(["catalog-1".into()])),
    ] {
        fixture.resolver.0.write().await.as_mut().unwrap().ceiling = ceiling;
        assert!(read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            Some(cursor)
        )
        .await
        .is_err());
    }
    *fixture.resolver.0.write().await = None;
    assert!(read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/raw.txt",
        Some(cursor)
    )
    .await
    .is_err());
    *fixture.resolver.0.write().await = Some(original.clone());
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None;
    assert!(
        read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            Some(cursor)
        )
        .await
        .is_err(),
        "even a wider current known ceiling cannot replay another fingerprint"
    );
    *fixture.resolver.0.write().await = Some(original.clone());
    let mut session = fixture.repo.load("catalog-session").await.unwrap();
    let next = Message::user("Input N+1 with no Skill mention");
    session.messages.push(next.clone());
    fixture.repo.save(&mut session).await.unwrap();
    fixture.resolver.0.write().await.as_mut().unwrap().input_id = next.id.clone();
    assert!(
        read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            Some(cursor)
        )
        .await
        .is_err(),
        "old invocation input is malformed"
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
        read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .is_err(),
        "stale selected_skill_ids cannot authorize manual-only Skill"
    );
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-1", "SKILL.md", None)
            .await
            .is_ok(),
        "automatic Skill needs no selected pin"
    );
    *fixture.resolver.0.write().await = Some(original.clone());
    fixture
        .config
        .write()
        .await
        .skills
        .disabled
        .push("catalog-0".into());
    assert!(read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/raw.txt",
        Some(cursor)
    )
    .await
    .is_err());
    fixture.config.write().await.skills.disabled.clear();
    session.root_orchestration_only = true;
    session.root_tool_authority_revision += 1;
    fixture.repo.save(&mut session).await.unwrap();
    assert!(read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/raw.txt",
        Some(cursor)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn skills_read_cursor_rejects_current_raw_physical_policy_mode_and_project_changes() {
    for change in [
        "raw",
        "leaf",
        "bundle",
        "root",
        "host-deny",
        "openai",
        "malformed",
        "removed",
        "mode",
        "project",
    ] {
        let fixture = Fixture::new(1).await;
        fixture
            .resolver
            .0
            .write()
            .await
            .as_mut()
            .unwrap()
            .response_bytes = 350;
        let tool = reader(&fixture);
        let (_, page) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .unwrap();
        let cursor = page["next_cursor"].as_str().unwrap();
        let root = fixture._directory.path().join("skills");
        let bundle = root.join("catalog-0");
        let file = bundle.join("references/raw.txt");
        match change {
            "raw" => std::fs::write(&file, "changed raw data").unwrap(),
            "leaf" => {
                let bytes = std::fs::read(&file).unwrap();
                std::fs::rename(&file, file.with_extension("old")).unwrap();
                std::fs::write(&file, bytes).unwrap();
            }
            "bundle" | "root" => {
                let path = if change == "bundle" { &bundle } else { &root };
                let renamed = path.with_extension("old");
                std::fs::rename(path, &renamed).unwrap();
                std::fs::create_dir_all(path.join(if change == "bundle" {
                    "references"
                } else {
                    "catalog-0/references"
                }))
                .unwrap();
                let target = if change == "bundle" {
                    path.clone()
                } else {
                    path.join("catalog-0")
                };
                let original = if change == "bundle" {
                    renamed.clone()
                } else {
                    renamed.join("catalog-0")
                };
                std::fs::copy(original.join("SKILL.md"), target.join("SKILL.md")).unwrap();
                std::fs::copy(
                    original.join("references/raw.txt"),
                    target.join("references/raw.txt"),
                )
                .unwrap();
            }
            "host-deny" => std::fs::write(
                bundle.join("agents/bamboo.yaml"),
                "invocation_policy:\n  explicit: false\n  automatic: false\n",
            )
            .unwrap(),
            "openai" => std::fs::write(
                bundle.join("agents/openai.yaml"),
                "policy:\n  allow_implicit_invocation: false\n",
            )
            .unwrap(),
            "malformed" => {
                std::fs::write(bundle.join("agents/bamboo.yaml"), "invocation_policy: [\n").unwrap()
            }
            "removed" => std::fs::remove_file(&file).unwrap(),
            "mode" => {
                fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("review-mode".into())
            }
            "project" => {
                let mut session = fixture.repo.load("catalog-session").await.unwrap();
                session.set_project_id_meta("missing-project");
                session.metadata_version += 1;
                fixture.repo.save(&mut session).await.unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            read_page(
                &tool,
                &fixture.ctx,
                "catalog-0",
                "references/raw.txt",
                Some(cursor)
            )
            .await
            .is_err(),
            "old cursor must reject {change}, including byte-identical physical replacement"
        );
    }
}

struct ReadBarrierResolver {
    caller: SkillCatalogCaller,
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl SkillCatalogCallerResolver for ReadBarrierResolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 6 {
            // Cold read uses four resolutions; warm final probe uses the seventh.
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(self.caller.clone())
    }
}
#[tokio::test]
async fn skills_read_eviction_during_final_probe_never_revives_the_old_cursor() {
    let fixture = Fixture::new(1).await;
    let mut caller = fixture.resolver.0.read().await.clone().unwrap();
    caller.response_bytes = 350;
    let resolver = Arc::new(ReadBarrierResolver {
        caller,
        calls: Default::default(),
        entered: Default::default(),
        release: Default::default(),
    });
    let tool = SkillsReadTool::new(SkillsListTool::new(
        fixture.manager.clone(),
        fixture.config.clone(),
        fixture.repo.clone(),
        resolver.clone(),
    ));
    let (_, first) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
        .await
        .unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let pending = read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/raw.txt",
        Some(cursor),
    );
    tokio::pin!(pending);
    tokio::select! {
        _ = resolver.entered.notified() => {},
        result = &mut pending => panic!("final warm probe barrier was not reached: {result:?}"),
    }
    assert!(futures::poll!(&mut pending).is_pending());
    // A real concurrent cold read evicts the entry while the old page/body borrow lives.
    read_page(&tool, &fixture.ctx, "catalog-0", "SKILL.md", None)
        .await
        .unwrap();
    resolver.release.notify_one();
    assert!(
        pending.await.is_err(),
        "membership validation rejects concurrent eviction"
    );
    let (_, replacement) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
        .await
        .unwrap();
    assert_ne!(replacement["next_cursor"].as_str(), Some(cursor));
    assert!(
        read_page(
            &tool,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            Some(cursor)
        )
        .await
        .is_err(),
        "identical reread must not resurrect an evicted cursor"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn skills_read_native_windows_junction_replacement_rejects_foreign_auxiliary_data() {
    let fixture = Fixture::new(1).await;
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 350;
    let tool = reader(&fixture);
    let (_, first) = read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
        .await
        .unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    // cmd treats forward slashes in mklink paths as option prefixes.
    let bundle = fixture._directory.path().join("skills").join("catalog-0");
    let original = bundle.join("references");
    std::fs::rename(&original, bundle.join("old-references")).unwrap();
    let foreign = fixture._directory.path().join("foreign");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("raw.txt"), "FOREIGN PRIVATE BODY").unwrap();
    let output = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(&original)
        .arg(&foreign)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "real junction creation must succeed: {output:?}"
    );
    assert!(read_page(
        &tool,
        &fixture.ctx,
        "catalog-0",
        "references/raw.txt",
        Some(cursor)
    )
    .await
    .is_err());
    assert!(
        read_page(&tool, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .is_err()
    );
}

// Bamboo-authored output preparation fixtures. Production has no caller-cap
// producer: this test-only resolver composes the explicitly supplied current cap.
struct BudgetResolver {
    caller: Arc<Resolver>,
    cap: RwLock<Option<u32>>,
}
#[async_trait]
impl SkillCatalogCallerResolver for BudgetResolver {
    async fn resolve(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let mut current = self.caller.resolve(ctx).await?;
        current.response_bytes =
            skill_response_byte_budget(current.response_bytes, *self.cap.read().await)?;
        Ok(current)
    }
}
fn budget_reader(fixture: &Fixture, cap: Option<u32>) -> (SkillsReadTool, Arc<BudgetResolver>) {
    let resolver = Arc::new(BudgetResolver {
        caller: fixture.resolver.clone(),
        cap: RwLock::new(cap),
    });
    let reader = SkillsReadTool::new(SkillsListTool::new(
        fixture.manager.clone(),
        fixture.config.clone(),
        fixture.repo.clone(),
        resolver.clone(),
    ));
    (reader, resolver)
}

#[tokio::test]
async fn skill_output_composed_known_caps_bound_all_real_converters_and_reconstruct_eof() {
    let mut fixture = Fixture::new(1).await;
    fixture.ctx.tool_call_id = "call-界\"\\🦀".repeat(16).into();
    let expected = std::fs::read_to_string(
        fixture
            ._directory
            .path()
            .join("skills")
            .join("catalog-0")
            .join("references")
            .join("raw.txt"),
    )
    .unwrap();
    for cap in [0, 1024, 1500] {
        let (reader, _) = budget_reader(&fixture, Some(cap));
        let budget = skill_response_byte_budget(8_000, Some(cap)).unwrap();
        let mut cursor = None;
        let mut joined = String::new();
        let mut seen = BTreeSet::new();
        loop {
            let (result, page) = read_page(
                &reader,
                &fixture.ctx,
                "catalog-0",
                "references/raw.txt",
                cursor.as_deref(),
            )
            .await
            .unwrap();
            assert_read_provider_blocks(&result, &fixture.ctx, budget);
            assert!(result.result.len() <= budget);
            let contents = page["contents"].as_str().unwrap();
            assert!(!contents.is_empty());
            joined.push_str(contents);
            if let Some(next) = page["next_cursor"].as_str() {
                assert!(seen.insert(next.to_string()));
                assert_eq!(
                    next.rsplit(':').next().unwrap().parse::<usize>().unwrap(),
                    joined.len()
                );
                cursor = Some(next.to_string());
            } else {
                break;
            }
            assert!(seen.len() <= expected.len());
        }
        assert_eq!(joined, expected);
        if cap == 1024 {
            assert!(!seen.is_empty());
        }
    }
}

#[tokio::test]
async fn skill_output_unknown_zero_and_tiny_caps_are_real_reader_errors_without_partial_pages() {
    let fixture = Fixture::new(1).await;
    for cap in [None, Some(1), Some(128)] {
        let (reader, _) = budget_reader(&fixture, cap);
        let error = read_page(
            &reader,
            &fixture.ctx,
            "catalog-0",
            "references/raw.txt",
            None,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, ToolError::Execution(_)),
            "actual resolver/Reader failure: {error}"
        );
    }
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .response_bytes = 0;
    let (reader, _) = budget_reader(&fixture, Some(0));
    assert!(read_page(
        &reader,
        &fixture.ctx,
        "catalog-0",
        "references/empty.txt",
        None
    )
    .await
    .is_err());
}

#[tokio::test]
async fn skill_output_budget_composition_preserves_fresh_warm_and_cold_authority_denials() {
    for change in [
        "tighten", "unknown", "ceiling", "disabled", "ultra", "input", "manual", "mode", "raw",
        "policy", "physical",
    ] {
        let fixture = Fixture::new(1).await;
        let (warm, resolver) = budget_reader(&fixture, Some(1024));
        let (_, first) = read_page(&warm, &fixture.ctx, "catalog-0", "references/raw.txt", None)
            .await
            .unwrap();
        let cursor = first["next_cursor"].as_str().unwrap();
        let mut session = fixture.repo.load("catalog-session").await.unwrap();
        let root = fixture._directory.path().join("skills").join("catalog-0");
        match change {
            "tighten" => *resolver.cap.write().await = Some(512),
            "unknown" => *resolver.cap.write().await = None,
            "ceiling" => {
                fixture.resolver.0.write().await.as_mut().unwrap().ceiling = Some(BTreeSet::new())
            }
            "disabled" => fixture
                .config
                .write()
                .await
                .skills
                .disabled
                .push("catalog-0".into()),
            "ultra" => {
                session.root_orchestration_only = true;
                session.root_tool_authority_revision += 1;
                fixture.repo.save(&mut session).await.unwrap();
            }
            "input" => {
                let next = Message::user("new current input with no invocation");
                session.messages.push(next.clone());
                fixture.repo.save(&mut session).await.unwrap();
                let mut current = fixture.resolver.0.write().await;
                current.as_mut().unwrap().input_id = next.id;
                current.as_mut().unwrap().invocation = None;
            }
            "manual" => {
                fixture
                    .resolver
                    .0
                    .write()
                    .await
                    .as_mut()
                    .unwrap()
                    .invocation = None
            }
            "mode" => {
                fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("../invalid".into())
            }
            "raw" => {
                std::fs::write(root.join("references").join("raw.txt"), "foreign raw bytes")
                    .unwrap();
            }
            "policy" => {
                std::fs::write(
                    root.join("agents").join("bamboo.yaml"),
                    "invocation_policy:\n  explicit: false\n  automatic: false\n",
                )
                .unwrap();
                fixture.manager.store().reload().await.unwrap();
            }
            "physical" => {
                std::fs::rename(
                    root.join("references").join("raw.txt"),
                    root.join("references").join("old.txt"),
                )
                .unwrap();
                std::fs::write(
                    root.join("references").join("raw.txt"),
                    "foreign physical file",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            read_page(
                &warm,
                &fixture.ctx,
                "catalog-0",
                "references/raw.txt",
                Some(cursor)
            )
            .await
            .is_err(),
            "warm {change}"
        );
        let cold = SkillsReadTool::new(SkillsListTool::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
            resolver,
        ));
        assert!(
            read_page(
                &cold,
                &fixture.ctx,
                "catalog-0",
                "references/raw.txt",
                Some(cursor)
            )
            .await
            .is_err(),
            "cold old cursor {change}"
        );
        // A newly authorized capture may read current auxiliary bytes; old
        // warm/cold cursors never authorize a replaced snapshot.
        if !matches!(change, "tighten" | "raw" | "physical") {
            assert!(
                read_page(&cold, &fixture.ctx, "catalog-0", "references/raw.txt", None)
                    .await
                    .is_err(),
                "cold fresh source {change}"
            );
        }
    }
}

impl Fixture {
    fn input_factory(&self) -> SkillInputFactory {
        SkillInputFactory::new(
            self.manager.clone(),
            self.config.clone(),
            self.repo.clone(),
            self.resolver.clone(),
        )
    }
    async fn fresh_input(&self, ids: &[&str]) -> (Message, Vec<bamboo_skills::WorkflowSelection>) {
        let user = Message::user("### Explicit Skill client-fragment\nReview this input");
        let mut resolver = self.resolver.0.write().await;
        let caller = resolver.as_mut().unwrap();
        caller.input_id = user.id.clone();
        caller.invocation = Some(SkillCatalogInvocation {
            input_id: user.id.clone(),
            skills: ids.iter().map(|id| (*id).into()).collect(),
        });
        drop(resolver);
        let catalog = self.manager.store().skill_catalog_snapshot().await;
        let selections = ids
            .iter()
            .map(|id| {
                let entry = catalog
                    .entries
                    .iter()
                    .find(|entry| &entry.id == id)
                    .unwrap();
                bamboo_skills::WorkflowSelection {
                    id: (*id).into(),
                    source: entry.source,
                    revision: entry.revision,
                    args: json!({}),
                }
            })
            .collect();
        (user, selections)
    }
}

#[tokio::test]
async fn skill_factory_unappended_user_preserves_message_and_existing_reader_predicate() {
    use bamboo_domain::{ImageUrlRef, MessagePart};
    let fixture = Fixture::new(2).await;
    let (mut user, selections) = fixture.fresh_input(&["catalog-0", "catalog-1"]).await;
    user.content_parts = Some(vec![
        MessagePart::Text {
            text: user.content.clone(),
        },
        MessagePart::ImageUrl {
            image_url: ImageUrlRef {
                url: "https://example.invalid/image.png".into(),
                detail: Some("high".into()),
            },
        },
        MessagePart::Text {
            text: "additional text".into(),
        },
    ]);
    let before = serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap();
    let original = serde_json::to_value(&user).unwrap();
    assert!(
        fixture.tool.render_catalog(&fixture.ctx).await.is_err(),
        "old API requires the actually persisted User"
    );
    let prepared = fixture
        .input_factory()
        .prepare_input(
            &fixture.ctx,
            &user,
            SkillInputSession::Existing,
            &selections,
        )
        .await
        .unwrap();
    assert_eq!(prepared.message.id, user.id);
    assert_eq!(prepared.message.role, user.role);
    assert_eq!(prepared.message.created_at, user.created_at);
    assert!(prepared.message.content.starts_with(&user.content));
    assert!(prepared.message.content.contains("PRIVATE BODY catalog-0"));
    assert!(prepared.message.content.contains("PRIVATE BODY catalog-1"));
    assert!(!prepared.message.content.contains("aux"));
    let parts = prepared.message.content_parts.as_ref().unwrap();
    assert!(matches!(&parts[0], MessagePart::Text { text } if text == &prepared.message.content));
    assert_eq!(&parts[1..], &user.content_parts.as_ref().unwrap()[1..]);
    assert_eq!(serde_json::to_value(&user).unwrap(), original);
    assert_eq!(
        serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap(),
        before
    );
    assert!(fixture
        .manager
        .store()
        .activation_descriptor("catalog-session")
        .await
        .is_none());
    assert!(fixture
        .tool
        .selected_source(&fixture.ctx, "catalog-0", "SKILL.md")
        .await
        .is_err());
}

#[tokio::test]
async fn skill_factory_new_session_is_host_owned_and_ordinary_input_is_a_noop() {
    let fixture = Fixture::new(1).await;
    let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
    let session = Session::new("new-host-session", "model");
    let mut ctx = fixture.ctx.clone();
    ctx.session_id = Some(session.id.clone().into());
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .session_id = session.id.clone();
    let factory = fixture.input_factory();
    let before = serde_json::to_value(&session).unwrap();
    assert!(factory
        .prepare_input(&ctx, &user, SkillInputSession::Existing, &selections)
        .await
        .is_err());
    let prepared = factory
        .prepare_input(&ctx, &user, SkillInputSession::New(&session), &selections)
        .await
        .unwrap();
    assert!(prepared.message.content.contains("PRIVATE BODY"));
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert!(fixture
        .repo
        .storage()
        .load_session(&session.id)
        .await
        .unwrap()
        .is_none());
    assert!(fixture.repo.cache().get(&session.id).is_none());
    fixture
        .resolver
        .0
        .write()
        .await
        .as_mut()
        .unwrap()
        .invocation = None;
    let ordinary = factory
        .prepare_input(&ctx, &user, SkillInputSession::New(&session), &[])
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(ordinary.message).unwrap(),
        serde_json::to_value(&user).unwrap()
    );
    assert!(ordinary.warnings.is_empty());
    *fixture.resolver.0.write().await = None;
    assert!(factory
        .prepare_input(&ctx, &user, SkillInputSession::New(&session), &[])
        .await
        .is_err());
}

#[tokio::test]
async fn skill_factory_enforces_fresh_ceiling_manual_disabled_ultra_and_input_bindings() {
    for change in [
        "none",
        "empty",
        "other",
        "disabled",
        "ultra",
        "unknown",
        "intent",
        "input",
        "mode",
        "collision",
        "new-collision",
    ] {
        let fixture = Fixture::new(2).await;
        let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
        let mut session = fixture.repo.load("catalog-session").await.unwrap();
        match change {
            "none" => fixture.resolver.0.write().await.as_mut().unwrap().ceiling = None,
            "empty" => {
                fixture.resolver.0.write().await.as_mut().unwrap().ceiling = Some(BTreeSet::new())
            }
            "other" => {
                fixture.resolver.0.write().await.as_mut().unwrap().ceiling =
                    Some(BTreeSet::from(["catalog-1".into()]))
            }
            "disabled" => fixture.config.write().await.skills.disabled = vec!["catalog-0".into()],
            "ultra" => {
                session.root_orchestration_only = true;
                session.root_tool_authority_revision += 1;
                fixture.repo.save(&mut session).await.unwrap();
            }
            "unknown" => *fixture.resolver.0.write().await = None,
            "intent" => {
                fixture
                    .resolver
                    .0
                    .write()
                    .await
                    .as_mut()
                    .unwrap()
                    .invocation = None
            }
            "input" => {
                fixture.resolver.0.write().await.as_mut().unwrap().input_id =
                    "historical-input".into()
            }
            "mode" => {
                fixture.resolver.0.write().await.as_mut().unwrap().mode = Some("../escape".into())
            }
            "collision" => {
                session.messages.push(user.clone());
                fixture.repo.save(&mut session).await.unwrap();
            }
            "new-collision" => {}
            _ => unreachable!(),
        }
        let host = if change == "new-collision" {
            SkillInputSession::New(&session)
        } else {
            SkillInputSession::Existing
        };
        let result = fixture
            .input_factory()
            .prepare_input(&fixture.ctx, &user, host, &selections)
            .await;
        assert_eq!(result.is_ok(), change == "none", "{change}");
    }
}

#[tokio::test]
async fn skill_factory_selection_is_atomic_and_schema_arguments_are_bounded() {
    let missing = Fixture::new(1).await;
    let (user, _) = missing.fresh_input(&["catalog-0"]).await;
    assert!(
        missing
            .input_factory()
            .prepare_input(&missing.ctx, &user, SkillInputSession::Existing, &[])
            .await
            .is_err(),
        "a nonempty explicit invocation cannot lose all typed selections"
    );
    let empty = Fixture::new(1).await;
    let (user, selections) = empty.fresh_input(&[]).await;
    assert!(
        empty
            .input_factory()
            .prepare_input(&empty.ctx, &user, SkillInputSession::Existing, &selections)
            .await
            .is_err(),
        "an empty explicit invocation is not ordinary input"
    );
    for change in [
        "duplicate",
        "missing",
        "source",
        "revision",
        "args",
        "schema",
        "policy",
    ] {
        let fixture = Fixture::new(2).await;
        let (user, mut selections) = fixture.fresh_input(&["catalog-0", "catalog-1"]).await;
        match change {
            "duplicate" => selections[1] = selections[0].clone(),
            "missing" => {
                selections[1].id = "missing-skill".into();
                fixture
                    .resolver
                    .0
                    .write()
                    .await
                    .as_mut()
                    .unwrap()
                    .invocation
                    .as_mut()
                    .unwrap()
                    .skills = BTreeSet::from(["catalog-0".into(), "missing-skill".into()]);
            }
            "source" => selections[1].source = bamboo_skills::WorkflowSource::Workspace,
            "revision" => selections[1].revision += 1,
            "args" => selections[1].args = json!({"target":"界".repeat(8192)}),
            "schema" => selections[1].args = json!(true),
            "policy" => {
                std::fs::create_dir_all(fixture._directory.path().join("skills/catalog-1/agents"))
                    .unwrap();
                std::fs::write(
                    fixture
                        ._directory
                        .path()
                        .join("skills/catalog-1/agents/bamboo.yaml"),
                    "invocation_policy:\n  explicit: false\n  automatic: true\n",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let original = serde_json::to_value(&user).unwrap();
        assert!(
            fixture
                .input_factory()
                .prepare_input(
                    &fixture.ctx,
                    &user,
                    SkillInputSession::Existing,
                    &selections
                )
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(serde_json::to_value(&user).unwrap(), original);
    }
}

// Read interception retains the real durable Storage and existing persistence
// coordinator; it creates no fake caller permission or publication snapshot.
struct FactoryStorageGate {
    inner: Arc<dyn bamboo_domain::Storage>,
    loads: std::sync::atomic::AtomicUsize,
    pause_at: usize,
    fail_at: usize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl bamboo_domain::Storage for FactoryStorageGate {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        let call = self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if call == self.pause_at {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if call == self.fail_at {
            return Err(std::io::Error::other(
                "original injected factory storage read failure",
            ));
        }
        self.inner.load_session(id).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
}
fn gated_factory(
    fixture: &Fixture,
    pause_at: usize,
    fail_at: usize,
) -> (SkillInputFactory, Arc<FactoryStorageGate>) {
    let gate = Arc::new(FactoryStorageGate {
        inner: fixture.repo.storage().clone(),
        loads: Default::default(),
        pause_at,
        fail_at,
        entered: Default::default(),
        release: Default::default(),
    });
    let repo = bamboo_engine::SessionRepository::new(
        fixture.repo.cache().clone(),
        gate.clone(),
        fixture.repo.persistence().clone(),
    );
    (
        SkillInputFactory::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            repo,
            fixture.resolver.clone(),
        ),
        gate,
    )
}

#[tokio::test]
async fn skill_factory_first_and_final_storage_errors_cannot_become_new_or_success() {
    for (new, fail_at) in [(false, 1), (false, 2), (true, 1), (true, 2)] {
        let fixture = Fixture::new(1).await;
        let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
        let session = Session::new("new-error-session", "model");
        let mut ctx = fixture.ctx.clone();
        if new {
            ctx.session_id = Some(session.id.clone().into());
            fixture
                .resolver
                .0
                .write()
                .await
                .as_mut()
                .unwrap()
                .session_id = session.id.clone();
        }
        let (factory, gate) = gated_factory(&fixture, 0, fail_at);
        let host = if new {
            SkillInputSession::New(&session)
        } else {
            SkillInputSession::Existing
        };
        let error = factory
            .prepare_input(&ctx, &user, host, &selections)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("original injected factory storage read failure"),
            "{new}/{fail_at}: {error}"
        );
        assert_eq!(
            gate.loads.load(std::sync::atomic::Ordering::SeqCst),
            fail_at
        );
        assert!(fixture
            .repo
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn skill_factory_final_storage_holds_config_owner_and_current_source_guards() {
    for changed in ["main", "auxiliary", "policy", "physical"] {
        let fixture = Fixture::new(1).await;
        let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
        let (factory, gate) = gated_factory(&fixture, 2, 0);
        let pending = factory.prepare_input(
            &fixture.ctx,
            &user,
            SkillInputSession::Existing,
            &selections,
        );
        tokio::pin!(pending);
        tokio::select! { _ = gate.entered.notified() => {}, result = &mut pending => panic!("final storage barrier not reached: {result:?}") }
        assert!(
            fixture.config.try_write().is_err(),
            "configuration remains guarded across final storage await"
        );
        let owner_wait = fixture.repo.persistence().acquire_lock("catalog-session");
        tokio::pin!(owner_wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut owner_wait)
                .await
                .is_err()
        );
        let root = fixture._directory.path().join("skills/catalog-0");
        match changed {
            "main" => {
                let path = root.join("SKILL.md");
                let text = std::fs::read_to_string(&path).unwrap();
                std::fs::write(path, format!("{text}\n ")).unwrap();
            }
            "auxiliary" => {
                std::fs::write(root.join("references/raw.txt"), "changed auxiliary").unwrap()
            }
            "policy" => std::fs::write(
                root.join("agents/bamboo.yaml"),
                "invocation_policy:\n  explicit: false\n  automatic: false\n",
            )
            .unwrap(),
            "physical" => {
                let path = root.join("SKILL.md");
                let text = std::fs::read_to_string(&path).unwrap();
                std::fs::rename(&path, root.join("old-main.txt")).unwrap();
                std::fs::write(path, text).unwrap();
            }
            _ => unreachable!(),
        }
        let manager = fixture.manager.clone();
        let writer = tokio::spawn(async move { manager.store().reload().await });
        gate.release.notify_one();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut pending)
                .await
                .unwrap()
                .is_err(),
            "{changed}"
        );
        drop(pending);
        drop(owner_wait);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(fixture.config.try_write().is_ok());
    }
}

#[tokio::test]
async fn skill_factory_cache_miss_and_changed_reload_do_not_form_an_owner_cycle() {
    let fixture = Fixture::new(1).await;
    let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
    fixture.repo.cache().remove("catalog-session");
    let owner = fixture
        .repo
        .persistence()
        .acquire_lock("catalog-session")
        .await;
    let factory = fixture.input_factory();
    let pending = factory.prepare_input(
        &fixture.ctx,
        &user,
        SkillInputSession::Existing,
        &selections,
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    let path = fixture._directory.path().join("skills/catalog-0/SKILL.md");
    std::fs::write(
        path,
        "---\nname: catalog-0\ndescription: changed\n---\nNEW BODY",
    )
    .unwrap();
    // A real changed reload must complete while the cache-miss reader waits for
    // the existing chat owner. The reader has acquired no publication guard.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.manager.store().reload(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(owner);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut pending)
            .await
            .unwrap()
            .is_err(),
        "stale typed revision must deny after owner release"
    );
}

struct FactoryMutationResolver {
    caller: SkillCatalogCaller,
    calls: std::sync::atomic::AtomicUsize,
    repo: bamboo_engine::SessionRepository,
    config: Arc<RwLock<Config>>,
    change: &'static str,
}
#[async_trait]
impl SkillCatalogCallerResolver for FactoryMutationResolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let mut caller = self.caller.clone();
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            match self.change {
                "caller" => caller.ceiling = Some(BTreeSet::new()),
                "error" => {
                    return Err(ToolError::Execution(
                        "original final resolver failure".into(),
                    ))
                }
                "config" => self.config.write().await.skills.disabled = vec!["catalog-0".into()],
                "new-row" => {
                    let mut row = Session::new(&caller.session_id, "model");
                    self.repo.save(&mut row).await.unwrap();
                }
                "root-aba" => {
                    let mut session = self
                        .repo
                        .try_load(&caller.session_id)
                        .await
                        .unwrap()
                        .unwrap();
                    session.set_root_orchestration_only(true).unwrap();
                    self.repo.save(&mut session).await.unwrap();
                    session.set_root_orchestration_only(false).unwrap();
                    self.repo.save(&mut session).await.unwrap();
                }
                "workspace-aba" => {
                    let mut session = self
                        .repo
                        .try_load(&caller.session_id)
                        .await
                        .unwrap()
                        .unwrap();
                    let original = session.workspace_path_meta().unwrap();
                    session.set_workspace_path_meta("/temporary-foreign-workspace");
                    session.metadata_version += 1;
                    self.repo.save(&mut session).await.unwrap();
                    session.set_workspace_path_meta(original);
                    session.metadata_version += 1;
                    self.repo.save(&mut session).await.unwrap();
                }
                _ => unreachable!(),
            }
        }
        Ok(caller)
    }
    fn validate_current(
        &self,
        ctx: &ToolCtx,
        expected: &SkillCatalogCaller,
    ) -> Result<(), ToolError> {
        validate_fixture_caller(ctx, Some(&self.caller), expected)
    }
}

#[tokio::test]
async fn skill_factory_final_resolver_cannot_hide_config_caller_or_session_aba() {
    for change in [
        "caller",
        "error",
        "config",
        "root-aba",
        "workspace-aba",
        "new-row",
    ] {
        let fixture = Fixture::new(1).await;
        let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
        let mut session = fixture.repo.load("catalog-session").await.unwrap();
        // Official Root mode transitions reject selected legacy Skill metadata.
        session.clear_selected_skill_ids();
        if change == "workspace-aba" {
            session.set_workspace_path_meta(fixture._directory.path().to_string_lossy());
        }
        session.metadata_version += 1;
        fixture.repo.save(&mut session).await.unwrap();
        let mut ctx = fixture.ctx.clone();
        let new = Session::new("appearing-host-session", "model");
        let mut caller = fixture.resolver.0.read().await.clone().unwrap();
        if change == "new-row" {
            ctx.session_id = Some(new.id.clone().into());
            caller.session_id = new.id.clone();
        }
        let resolver = Arc::new(FactoryMutationResolver {
            caller,
            calls: Default::default(),
            repo: fixture.repo.clone(),
            config: fixture.config.clone(),
            change,
        });
        let factory = SkillInputFactory::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
            resolver.clone(),
        );
        let host = if change == "new-row" {
            SkillInputSession::New(&new)
        } else {
            SkillInputSession::Existing
        };
        assert!(
            factory
                .prepare_input(&ctx, &user, host, &selections)
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(resolver.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        if change == "root-aba" {
            let current = fixture.repo.load("catalog-session").await.unwrap();
            assert!(!current.root_orchestration_only_enabled());
            assert_eq!(
                current.root_tool_authority_revision,
                session.root_tool_authority_revision + 2
            );
        }
    }
}

#[tokio::test]
async fn skill_factory_new_and_existing_scopes_use_actual_project_mode_and_workspace() {
    let fixture = Fixture::new(1).await;
    let projects =
        Arc::new(bamboo_projects::ProjectStore::open(fixture._directory.path()).unwrap());
    let project = projects.create("Factory scoped inputs", None).unwrap();
    let workspace = fixture._directory.path().join("workspace");
    let project_root = projects
        .paths()
        .project_home(&project.id)
        .join("skills/catalog-0");
    let mode_root = workspace.join(".bamboo/skills-review/catalog-0");
    for (root, body) in [
        (&project_root, "PROJECT BODY"),
        (&mode_root, "MODE WORKSPACE BODY"),
    ] {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            format!("---\nname: catalog-0\ndescription: scoped summary\n---\n{body}"),
        )
        .unwrap();
    }
    let mut existing = fixture.repo.load("catalog-session").await.unwrap();
    existing.set_project_id_meta(project.id.to_string());
    existing.set_workspace_path_meta(workspace.to_string_lossy());
    existing.metadata_version += 1;
    fixture.repo.save(&mut existing).await.unwrap();
    for new in [false, true] {
        let session = if new {
            let mut session = Session::new("new-scoped-host", "model");
            session.set_project_id_meta(project.id.to_string());
            session.set_workspace_path_meta(workspace.to_string_lossy());
            session
        } else {
            existing.clone()
        };
        for mode in [None, Some("review")] {
            let (user, _) = fixture.fresh_input(&["catalog-0"]).await;
            let mut ctx = fixture.ctx.clone();
            ctx.session_id = Some(session.id.clone().into());
            {
                let mut caller = fixture.resolver.0.write().await;
                let caller = caller.as_mut().unwrap();
                caller.session_id = session.id.clone();
                caller.mode = mode.map(str::to_owned);
            }
            let factory = fixture.input_factory().with_project_store(projects.clone());
            let access = SkillToolAccess::new(
                fixture.manager.clone(),
                fixture.config.clone(),
                fixture.repo.clone(),
            )
            .with_project_store(projects.clone());
            let store = access.skill_store_for_session(&session).await.unwrap();
            let projection = store
                .progressive_catalog_for_mode(
                    mode,
                    &bamboo_skills::progressive::SkillCatalogEligibility {
                        ceiling: None,
                        explicit: BTreeSet::from(["catalog-0".into()]),
                        disabled: BTreeSet::new(),
                        deny_all: false,
                    },
                )
                .await
                .unwrap();
            let entry = projection
                .entries
                .iter()
                .find(|entry| entry.package == "catalog-0")
                .unwrap();
            let selections = [bamboo_skills::WorkflowSelection {
                id: entry.package.clone(),
                source: entry.source,
                revision: entry.revision,
                args: json!({"escaped":"界\"\\\n"}),
            }];
            let host = if new {
                SkillInputSession::New(&session)
            } else {
                SkillInputSession::Existing
            };
            let prepared = factory
                .prepare_input(&ctx, &user, host, &selections)
                .await
                .unwrap();
            assert!(prepared.message.content.contains(if mode.is_some() {
                "MODE WORKSPACE BODY"
            } else {
                "PROJECT BODY"
            }));
            assert!(!prepared.message.content.contains("PRIVATE BODY catalog-0"));
            assert!(prepared.message.content.contains("界\\\"\\\\\\n"));
            let host = if new {
                SkillInputSession::New(&session)
            } else {
                SkillInputSession::Existing
            };
            assert!(
                fixture
                    .input_factory()
                    .prepare_input(&ctx, &user, host, &selections)
                    .await
                    .is_err(),
                "missing Project resolver cannot fall back to global"
            );
        }
    }
}

#[cfg(unix)]
struct FactoryWorkspaceAliasResolver {
    caller: SkillCatalogCaller,
    calls: std::sync::atomic::AtomicUsize,
    alias: std::path::PathBuf,
    replacement: std::path::PathBuf,
}
#[cfg(unix)]
#[async_trait]
impl SkillCatalogCallerResolver for FactoryWorkspaceAliasResolver {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            std::fs::remove_file(&self.alias).unwrap();
            std::os::unix::fs::symlink(&self.replacement, &self.alias).unwrap();
        }
        Ok(self.caller.clone())
    }
    fn validate_current(
        &self,
        ctx: &ToolCtx,
        expected: &SkillCatalogCaller,
    ) -> Result<(), ToolError> {
        validate_fixture_caller(ctx, Some(&self.caller), expected)
    }
}

#[cfg(unix)]
#[tokio::test]
async fn skill_factory_workspace_alias_retarget_cannot_accept_the_cached_old_store() {
    for (new, project) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = Fixture::new(1).await;
        let (user, _) = fixture.fresh_input(&["catalog-0"]).await;
        let workspace_a = fixture._directory.path().join("workspace-a");
        let workspace_b = fixture._directory.path().join("workspace-b");
        let alias = fixture._directory.path().join("workspace-alias");
        for (workspace, body) in [
            (&workspace_a, "WORKSPACE A BODY"),
            (&workspace_b, "WORKSPACE B BODY"),
        ] {
            let root = workspace.join(".bamboo/skills/catalog-0");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(
                root.join("SKILL.md"),
                format!("---\nname: catalog-0\ndescription: scoped summary\n---\n{body}"),
            )
            .unwrap();
        }
        std::os::unix::fs::symlink(&workspace_a, &alias).unwrap();
        let projects =
            Arc::new(bamboo_projects::ProjectStore::open(fixture._directory.path()).unwrap());
        let assigned = projects.create("Alias scoped input", None).unwrap();
        let mut session = if new {
            Session::new("new-workspace-alias-host", "model")
        } else {
            fixture.repo.load("catalog-session").await.unwrap()
        };
        session.set_workspace_path_meta(alias.to_string_lossy());
        if project {
            session.set_project_id_meta(assigned.id.to_string());
        }
        session.metadata_version += 1;
        if !new {
            fixture.repo.save(&mut session).await.unwrap();
        }
        let mut ctx = fixture.ctx.clone();
        ctx.session_id = Some(session.id.clone().into());
        let mut caller = fixture.resolver.0.read().await.clone().unwrap();
        caller.session_id = session.id.clone();
        let access = SkillToolAccess::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
        )
        .with_project_store(projects.clone());
        // Prime the real alias cache with A before the factory's first resolve.
        let store = access.skill_store_for_session(&session).await.unwrap();
        let catalog = store.skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "catalog-0")
            .unwrap();
        assert_eq!(entry.source, bamboo_skills::WorkflowSource::Workspace);
        let selections = [bamboo_skills::WorkflowSelection {
            id: entry.id.clone(),
            source: entry.source,
            revision: entry.revision,
            args: json!({}),
        }];
        let resolver = Arc::new(FactoryWorkspaceAliasResolver {
            caller,
            calls: Default::default(),
            alias: alias.clone(),
            replacement: workspace_b.clone(),
        });
        let factory = SkillInputFactory::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
            resolver.clone(),
        )
        .with_project_store(projects);
        let host = if new {
            SkillInputSession::New(&session)
        } else {
            SkillInputSession::Existing
        };
        let result = factory.prepare_input(&ctx, &user, host, &selections).await;
        assert!(
            result.is_err(),
            "retargeted alias must deny: new={new}, project={project}; {result:?}"
        );
        assert_eq!(resolver.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            std::fs::canonicalize(&alias).unwrap(),
            std::fs::canonicalize(&workspace_b).unwrap()
        );
        if !project {
            let cached = access.skill_store_for_session(&session).await.unwrap();
            assert!(
                Arc::ptr_eq(&store, &cached),
                "legacy alias caching must remain unchanged"
            );
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn skill_factory_canonical_path_cannot_reuse_a_historical_alias_store() {
    for new in [false, true] {
        let fixture = Fixture::new(1).await;
        let (user, _) = fixture.fresh_input(&["catalog-0"]).await;
        let canonical_parent = std::fs::canonicalize(fixture._directory.path()).unwrap();
        let workspace = canonical_parent.join("recreated-workspace");
        let foreign = canonical_parent.join("foreign-workspace");
        let root = foreign.join(".bamboo/skills/catalog-0");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            "---\nname: catalog-0\ndescription: scoped summary\n---\nFOREIGN BODY",
        )
        .unwrap();
        std::os::unix::fs::symlink(&foreign, &workspace).unwrap();
        let mut session = if new {
            Session::new("new-historical-alias-host", "model")
        } else {
            fixture.repo.load("catalog-session").await.unwrap()
        };
        session.set_workspace_path_meta(workspace.to_string_lossy());
        session.metadata_version += 1;
        if !new {
            fixture.repo.save(&mut session).await.unwrap();
        }
        let access = SkillToolAccess::new(
            fixture.manager.clone(),
            fixture.config.clone(),
            fixture.repo.clone(),
        );
        let old = access.skill_store_for_session(&session).await.unwrap();
        let catalog = old.skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "catalog-0")
            .unwrap();
        let selections = [bamboo_skills::WorkflowSelection {
            id: entry.id.clone(),
            source: entry.source,
            revision: entry.revision,
            args: json!({}),
        }];
        std::fs::remove_file(&workspace).unwrap();
        let root = workspace.join(".bamboo/skills/catalog-0");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            "---\nname: catalog-0\ndescription: scoped summary\n---\nACTUAL BODY",
        )
        .unwrap();
        assert!(!std::fs::symlink_metadata(&workspace)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::canonicalize(&workspace).unwrap(),
            std::fs::canonicalize(fixture._directory.path())
                .unwrap()
                .join("recreated-workspace")
        );
        let cached = access.skill_store_for_session(&session).await.unwrap();
        assert!(Arc::ptr_eq(&old, &cached));
        fixture
            .resolver
            .0
            .write()
            .await
            .as_mut()
            .unwrap()
            .session_id = session.id.clone();
        let mut ctx = fixture.ctx.clone();
        ctx.session_id = Some(session.id.clone().into());
        let host = if new {
            SkillInputSession::New(&session)
        } else {
            SkillInputSession::Existing
        };
        let result = fixture
            .input_factory()
            .prepare_input(&ctx, &user, host, &selections)
            .await;
        if let Ok(prepared) = &result {
            assert!(
                prepared.message.content.contains("FOREIGN BODY"),
                "the regression must actually reach the historical foreign store"
            );
        }
        assert!(
            result.is_err(),
            "a canonical key with a historical alias store must deny: new={new}; {result:?}"
        );
    }
}

#[tokio::test]
async fn skill_factory_caller_revocation_during_final_storage_is_denied() {
    let fixture = Fixture::new(1).await;
    let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
    let before = serde_json::to_value(
        fixture
            .repo
            .storage()
            .load_session("catalog-session")
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let (factory, gate) = gated_factory(&fixture, 2, 0);
    let pending = factory.prepare_input(
        &fixture.ctx,
        &user,
        SkillInputSession::Existing,
        &selections,
    );
    tokio::pin!(pending);
    tokio::select! {
        _ = gate.entered.notified() => {},
        result = &mut pending => panic!("final storage barrier not reached: {result:?}"),
    }
    assert!(fixture.config.try_write().is_err());
    // Actual authority source is independent of Config and Session owners.
    fixture.resolver.0.write().await.as_mut().unwrap().ceiling = Some(BTreeSet::new());
    gate.release.notify_one();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut pending)
        .await
        .unwrap();
    let after = serde_json::to_value(
        fixture
            .repo
            .storage()
            .load_session("catalog-session")
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(before, after, "authority revocation did not alter Session");
    assert!(
        result.is_err(),
        "revoked caller must not receive Skill instructions: {result:?}"
    );
}

#[tokio::test]
async fn skill_factory_final_caller_snapshot_denies_changes_unknown_and_busy() {
    for change in [
        "unchanged",
        "identity",
        "session",
        "input",
        "invocation-input",
        "invocation-skills",
        "invocation-none",
        "mode",
        "context-window",
        "metadata-tokens",
        "response-bytes",
        "unknown",
        "busy",
    ] {
        let fixture = Fixture::new(1).await;
        let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
        let before =
            serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap();
        let (factory, gate) = gated_factory(&fixture, 2, 0);
        let pending = factory.prepare_input(
            &fixture.ctx,
            &user,
            SkillInputSession::Existing,
            &selections,
        );
        tokio::pin!(pending);
        tokio::select! {
            _ = gate.entered.notified() => {},
            result = &mut pending => panic!("final storage barrier not reached: {result:?}"),
        }
        assert!(fixture.config.try_write().is_err());
        let mut authority = fixture.resolver.0.write().await;
        if change == "unknown" {
            *authority = None;
        } else {
            let caller = authority.as_mut().unwrap();
            match change {
                "unchanged" | "busy" => {}
                "identity" => caller.caller_id = "another-host".into(),
                "session" => caller.session_id = "another-session".into(),
                "input" => caller.input_id = "another-input".into(),
                "invocation-input" => {
                    caller.invocation.as_mut().unwrap().input_id = "another-input".into()
                }
                "invocation-skills" => caller.invocation.as_mut().unwrap().skills.clear(),
                "invocation-none" => caller.invocation = None,
                "mode" => caller.mode = Some("review".into()),
                "context-window" => caller.context_window = Some(200_000),
                "metadata-tokens" => caller.metadata_tokens = std::num::NonZeroUsize::new(1),
                "response-bytes" => caller.response_bytes += 1,
                _ => unreachable!(),
            }
        }
        let _busy = if change == "busy" {
            Some(authority)
        } else {
            drop(authority);
            None
        };
        gate.release.notify_one();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut pending)
            .await
            .unwrap();
        if change == "unchanged" {
            assert!(result
                .unwrap()
                .message
                .content
                .contains("PRIVATE BODY catalog-0"));
        } else {
            let error = result.unwrap_err().to_string();
            assert!(!error.contains("PRIVATE BODY"), "{change}: {error}");
            assert!(
                error.contains(if change == "unknown" {
                    "unknown actual caller"
                } else if change == "busy" {
                    "current actual caller is busy"
                } else {
                    "current actual caller changed"
                }),
                "{change}: {error}"
            );
        }
        assert_eq!(
            serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap(),
            before,
            "{change}"
        );
        assert!(fixture.config.try_write().is_ok());
    }
}

#[tokio::test]
async fn skill_factory_legacy_resolver_defaults_to_deny_without_changing_list_read() {
    let fixture = Fixture::new(1).await;
    let original = fixture.resolver.0.read().await.clone();
    // Existing BudgetResolver implements only resolve; no synchronous override.
    let (read, resolver) = budget_reader(&fixture, Some(8_000));
    let list = SkillsListTool::new(
        fixture.manager.clone(),
        fixture.config.clone(),
        fixture.repo.clone(),
        resolver.clone(),
    );
    let before = serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap();
    for after_factory in [false, true] {
        if after_factory {
            let (user, selections) = fixture.fresh_input(&["catalog-0"]).await;
            let factory = SkillInputFactory::new(
                fixture.manager.clone(),
                fixture.config.clone(),
                fixture.repo.clone(),
                resolver.clone(),
            );
            let error = factory
                .prepare_input(
                    &fixture.ctx,
                    &user,
                    SkillInputSession::Existing,
                    &selections,
                )
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("Current Skill caller authority validation is unavailable"));
            assert!(!error.contains("PRIVATE BODY"));
            *fixture.resolver.0.write().await = original.clone();
        }
        let page = list
            .invoke(json!({}), fixture.ctx.clone())
            .await
            .unwrap()
            .into_tool_result();
        assert!(page.result.contains("catalog-0"));
        assert!(!page.result.contains("PRIVATE BODY"));
        let (_, page) = read_page(&read, &fixture.ctx, "catalog-0", "SKILL.md", None)
            .await
            .unwrap();
        assert!(page.to_string().contains("PRIVATE BODY catalog-0"));
        assert_eq!(
            serde_json::to_value(fixture.repo.load("catalog-session").await.unwrap()).unwrap(),
            before
        );
    }
}
