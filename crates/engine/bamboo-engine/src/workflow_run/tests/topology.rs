use super::*;

fn nested_map_flow() -> WorkflowRunDefinition {
    let mut flow = definition(
        vec![
            tool_step("plain", "echo", json!({})),
            tool_step(
                "mapped",
                "echo",
                json!({"from":"item","name":"PRIVATE-INNER-NAME"}),
            ),
        ],
        WorkflowPlan::Retry {
            max_attempts: 2,
            delay_ms: 0,
            node: Box::new(WorkflowPlan::Parallel {
                nodes: vec![
                    choice_leaf("plain"),
                    WorkflowPlan::Map {
                        source: ValueRef::Args {
                            pointer: "/items".into(),
                        },
                        item: "PRIVATE-OUTER-NAME".into(),
                        body: Box::new(WorkflowPlan::Map {
                            source: ValueRef::Item {
                                name: "PRIVATE-OUTER-NAME".into(),
                                pointer: String::new(),
                            },
                            item: "PRIVATE-INNER-NAME".into(),
                            body: Box::new(choice_leaf("mapped")),
                        }),
                    },
                ],
            }),
        },
    );
    flow.input_schema = json!({"type":"object","properties":{"items":{"type":"array","items":{"type":"array","items":{"type":"string"}}}},"required":["items"]});
    flow
}

fn mappings(snapshot: &WorkflowRunSnapshot) -> BTreeMap<String, WorkflowStepTopology> {
    snapshot
        .steps
        .iter()
        .map(|(id, state)| {
            (
                id.clone(),
                state.topology.clone().expect("new runtime step is mapped"),
            )
        })
        .collect()
}

struct BlockingTopologyTools(Arc<tokio::sync::Semaphore>);

#[async_trait]
impl ToolExecutor for BlockingTopologyTools {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.add_permits(1);
        std::future::pending().await
    }
    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

#[tokio::test]
async fn nested_map_topology_survives_interruption_recovery_reload_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let flow = nested_map_flow();
    let args = json!({"items":[["PRIVATE-A","PRIVATE-B"],[],["PRIVATE-C"]]});
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let first = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(BlockingTopologyTools(started.clone())),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let owner = first.clone();
    let task = tokio::spawn(async move { owner.run(request(flow, args)).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), started.acquire_many(4))
        .await
        .unwrap()
        .unwrap()
        .forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(first);
    let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
    let run_id = repository.list_run_ids().await.unwrap().pop().unwrap();
    let before = repository.load(&run_id).await.unwrap().unwrap();
    let expected = mappings(&before);
    assert_eq!(expected.len(), 4);
    assert_eq!(expected["plain"].plan_node_path, vec![0, 0]);
    assert!(expected["plain"].map_index_path.is_empty());
    for (id, indices) in [
        ("mapped@root[0][0]", vec![0, 0]),
        ("mapped@root[0][1]", vec![0, 1]),
        ("mapped@root[2][0]", vec![2, 0]),
    ] {
        assert_eq!(expected[id].planned_step_id, "mapped");
        assert_eq!(expected[id].plan_node_path, vec![0, 1, 0, 0]);
        assert_eq!(expected[id].map_index_path, indices);
    }
    let reopened = engine(directory.path(), MockDefinitions::default());
    let recovered = reopened.recover().await.unwrap().pop().unwrap();
    assert_eq!(recovered.status, WorkflowRunStatus::Suspended);
    assert_eq!(mappings(&recovered), expected);
    assert_eq!(
        mappings(&repository.load(&run_id).await.unwrap().unwrap()),
        expected
    );
    let progress = reopened
        .progress(&run_id, before.last_sequence)
        .await
        .unwrap();
    assert_eq!(mappings(&progress.snapshot), expected);
    assert!(progress
        .events
        .iter()
        .all(|event| event.sequence > before.last_sequence));
    let started = reopened
        .restart(&run_id, true, vec!["read".into()])
        .await
        .unwrap();
    let restarted = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = reopened
                .progress(&started.run_id, 0)
                .await
                .unwrap()
                .snapshot;
            if current.status.is_terminal() {
                break current;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(restarted.status, WorkflowRunStatus::Succeeded);
    assert_ne!(restarted.run_id, run_id);
    assert_eq!(mappings(&restarted), expected);
    let cancelled = reopened.cancel(&run_id).await.unwrap();
    assert_eq!(mappings(&cancelled), expected);
}

#[tokio::test]
async fn topology_preserves_unexpanded_map_prefix_without_fabricating_item_zero() {
    let directory = tempfile::tempdir().unwrap();
    let mut flow = nested_map_flow();
    flow.steps = vec![
        tool_step(
            "mapped",
            "echo",
            json!({"from":"item","name":"PRIVATE-INNER-NAME"}),
        ),
        tool_step("other", "echo", json!({})),
    ];
    let WorkflowPlan::Retry { node, .. } = flow.plan else {
        unreachable!()
    };
    let WorkflowPlan::Parallel { mut nodes } = *node else {
        unreachable!()
    };
    let mut outer = nodes.pop().unwrap();
    let WorkflowPlan::Map { body, .. } = &mut outer else {
        unreachable!()
    };
    *body = Box::new(choice_plan(
        ValueRef::Literal {
            value: json!(false),
        },
        *body.clone(),
        choice_leaf("other"),
    ));
    flow.plan = outer;
    let result = engine(directory.path(), MockDefinitions::default())
        .run(request(
            flow,
            json!({"items":[["PRIVATE-A"],["PRIVATE-B"]]}),
        ))
        .await
        .unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    for index in 0..2 {
        let skipped = &result.steps[&format!("mapped@root[{index}]")];
        assert_eq!(skipped.status, WorkflowStepStatus::Skipped);
        assert_eq!(
            skipped.topology.as_ref().unwrap().map_index_path,
            vec![index]
        );
        assert_eq!(
            skipped.topology.as_ref().unwrap().plan_node_path,
            vec![0, 0, 0]
        );
        assert_eq!(
            result.steps[&format!("other@root[{index}]")]
                .topology
                .as_ref()
                .unwrap()
                .plan_node_path,
            vec![0, 1]
        );
    }
}

#[tokio::test]
async fn topology_keeps_opaque_planned_ids_distinct_from_map_runtime_ids() {
    let directory = tempfile::tempdir().unwrap();
    let flow = definition(
        vec![
            tool_step("a", "echo", json!({"from":"item","name":"item"})),
            tool_step("b", "echo", json!({"from":"step","step":"a"})),
            tool_step("a@root[0]", "echo", json!({"static":1})),
            tool_step("a@root[0]@", "echo", json!({"static":2})),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Map {
                    source: ValueRef::Args {
                        pointer: "/items".into(),
                    },
                    item: "item".into(),
                    body: Box::new(WorkflowPlan::Sequence {
                        nodes: vec![choice_leaf("a"), choice_leaf("b")],
                    }),
                },
                choice_leaf("a@root[0]"),
                choice_leaf("a@root[0]@"),
            ],
        },
    );
    let result = engine(directory.path(), MockDefinitions::default())
        .run(request(flow, json!({"items":[42]})))
        .await
        .unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    assert_eq!(result.steps.len(), 4);
    assert_eq!(
        result.steps["a@root[0]@@"]
            .topology
            .as_ref()
            .unwrap()
            .planned_step_id,
        "a"
    );
    assert_eq!(
        result.steps["a@root[0]"]
            .topology
            .as_ref()
            .unwrap()
            .plan_node_path,
        vec![1]
    );
    assert_eq!(
        result.steps["a@root[0]@"]
            .topology
            .as_ref()
            .unwrap()
            .plan_node_path,
        vec![2]
    );
    assert_eq!(result.steps["b@root[0]"].output, Some(json!(42)));
}
