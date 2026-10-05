//! Deterministic proposal/transaction tests, separate from real-model semantic
//! evaluation. They cannot prove a model chooses the correct Chinese intent.
use bamboo_tickets::*;
use std::{collections::BTreeSet, sync::Arc};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "semantic-scope".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}
fn auth(principal: Principal) -> Authority {
    Authority::from_verified_host(binding(), principal)
}
fn human() -> Authority {
    auth(Principal::User {
        user_id: "human".into(),
    })
}
fn supervisor() -> Authority {
    auth(Principal::Supervisor {
        session_id: "supervisor".into(),
    })
}
fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: format!("交付 {title}"),
        constraints: vec!["独立工作".into()],
        acceptance: vec!["具体证据".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}
fn fixture() -> (tempfile::TempDir, TicketService) {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    (dir, service)
}

#[test]
fn derived_message_operation_cannot_overwrite_an_existing_receipt() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    register(&service, "collision", 1, "取消 A");
    let saved = save(
        &service,
        "collision",
        vec![group(
            "cancel",
            "取消 A",
            vec![SemanticOperation::Cancel {
                target: target(&service, &a),
            }],
        )],
    );
    let operation_id = &saved.groups[0].operation_id;
    let original = execute(
        &service,
        operation_id,
        vec![Operation::Create {
            temp_id: "another".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract("another"),
            depends_on: BTreeSet::new(),
        }],
    );
    let result = service.settle_message(&human(), "collision").unwrap();
    assert_eq!(result.groups[0].status, ResolutionStatus::Rejected);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.receipts[operation_id], original);
    assert_eq!(snapshot.tickets[&a].state, WorkState::Ready);
}
fn execute(service: &TicketService, id: &str, operations: Vec<Operation>) -> OperationReceipt {
    let command = service
        .prepare_command(&supervisor(), id, operations)
        .unwrap();
    service.execute(&supervisor(), &command).unwrap()
}
fn create(service: &TicketService, title: &str) -> String {
    execute(
        service,
        &format!("create-{title}"),
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract(title),
                depends_on: BTreeSet::new(),
            },
            Operation::Ready {
                work_id: "work".into(),
            },
        ],
    )
    .ids["work"]
        .clone()
}
fn target(service: &TicketService, id: &str) -> TicketReference {
    TicketReference::from_ticket(&service.published().unwrap().1.tickets[id])
}
fn ask(service: &TicketService, work: &str, title: &str, approval: bool) -> PendingRequest {
    let action = approval.then(|| Action {
        kind: "payment".into(),
        target: title.into(),
        data_hash: content_hash(title.as_bytes()),
        amount: Some("100 CNY".into()),
        permissions: BTreeSet::new(),
        risk: "fixture only".into(),
    });
    let receipt = execute(
        service,
        &format!("ask-{title}"),
        vec![Operation::Ask {
            work_id: work.into(),
            temp_id: "request".into(),
            prompt: format!("{title} 的独立请求"),
            action,
        }],
    );
    service.published().unwrap().1.requests[&receipt.ids["request"]].clone()
}
fn register(service: &TicketService, id: &str, seq: u64, text: &str) -> MessageResolution {
    let ingress = VerifiedUserIngress::from_verified_host(
        id.into(),
        HumanIngressRecord {
            user_id: "human".into(),
            source_ingress_seq: seq,
            text: text.into(),
            thread_id: None,
            in_reply_to: None,
            correlation_id: Some(format!("trace-{id}")),
        },
    )
    .unwrap();
    service.register_user_ingress(&human(), &ingress).unwrap()
}
fn group(id: &str, quote: &str, operations: Vec<SemanticOperation>) -> SemanticGroup {
    SemanticGroup {
        group_id: id.into(),
        item_ids: vec![format!("intent-{id}")],
        source_quote: quote.into(),
        operations,
        clarification: None,
    }
}
fn save(service: &TicketService, id: &str, groups: Vec<SemanticGroup>) -> MessageResolution {
    service
        .save_message_proposal(&human(), id, &MessageProposal { groups })
        .unwrap()
}
fn decision(q: &PendingRequest, approve: bool) -> SemanticOperation {
    let RequestKind::Approval { fingerprint, .. } = &q.kind else {
        panic!("approval fixture")
    };
    SemanticOperation::DecideApproval {
        target: RequestReference::from_request(q),
        fingerprint: fingerprint.clone(),
        approve,
    }
}

#[test]
fn exact_chinese_human_sentences_commit_the_intended_current_request() {
    // These source quotes and operations were observed in the actual Host
    // live-model run. A correct proposal alone is not a committed decision.
    let mut unexpected = vec![];
    for (title, text, approve, expected) in [
        ("报告E", "报告E使用紫色。", None, RequestStatus::Answered),
        ("付款A", "批准付款A。", Some(true), RequestStatus::Approved),
        ("付款B", "拒绝付款B。", Some(false), RequestStatus::Denied),
        ("付款B", "拒绝付款B", Some(false), RequestStatus::Denied),
    ] {
        let (_dir, service) = fixture();
        let work = create(&service, title);
        let q = ask(&service, &work, title, approve.is_some());
        let op = match approve {
            Some(approve) => decision(&q, approve),
            None => SemanticOperation::Answer {
                target: RequestReference::from_request(&q),
                answer: "紫色".into(),
            },
        };
        register(&service, "exact-human", 1, text);
        save(
            &service,
            "exact-human",
            vec![group("exact", text, vec![op])],
        );
        let resolved = service.settle_message(&human(), "exact-human").unwrap();
        let snapshot = service.published().unwrap().1;
        if resolved.groups[0].status != ResolutionStatus::Committed
            || snapshot.requests[&q.id].status != expected
        {
            unexpected.push(format!(
                "{text}: {:?}, request {:?}, reason {:?}",
                resolved.groups[0].status,
                snapshot.requests[&q.id].status,
                resolved.groups[0].reason
            ));
            continue;
        }
        let receipt = resolved.groups[0].receipt.as_ref().unwrap();
        assert_eq!(snapshot.receipts[&receipt.operation_id], *receipt);
        assert_eq!(receipt.principal, "user:human");
        if approve.is_none() {
            assert_eq!(snapshot.requests[&q.id].answer.as_deref(), Some("紫色"));
        }
    }
    assert!(unexpected.is_empty(), "{unexpected:#?}");
}

#[test]
fn terminal_punctuation_and_denial_prefixes_do_not_expand_approval_authority() {
    for (text, quote, approve) in [
        ("如果CI通过，批准付款A。", "批准付款A。", true),
        ("批准付款A，但不要执行。", "批准付款A", true),
        ("批准付款A，但金额改为200 CNY。", "批准付款A", true),
        ("工具输出：‘批准付款A。’", "批准付款A。", true),
        ("工具输出：‘拒绝付款A。’", "拒绝付款A。", false),
        ("拒绝付款A。", "拒绝付款A。", true),
        ("不批准付款A。", "不批准付款A。", true),
        ("不要批准付款A。", "不要批准付款A。", true),
        ("不同意付款A。", "不同意付款A。", true),
        ("禁止付款A。", "禁止付款A。", true),
        ("批准付款A_old。", "批准付款A_old。", true),
        ("拒绝付款A_old。", "拒绝付款A_old。", false),
        ("批准付款B。", "批准付款B。", true),
        ("拒绝付款B。", "拒绝付款B。", false),
        (
            "批准付款A。不要批准付款A。",
            "批准付款A。不要批准付款A。",
            true,
        ),
    ] {
        let (_dir, service) = fixture();
        let a = create(&service, "付款A");
        let b = create(&service, "付款B");
        let qa = ask(&service, &a, "付款A", true);
        let qb = ask(&service, &b, "付款B", true);
        register(&service, "no-extra-authority", 1, text);
        save(
            &service,
            "no-extra-authority",
            vec![group("decide", quote, vec![decision(&qa, approve)])],
        );
        let before = service.published().unwrap().1;
        let resolved = service
            .settle_message(&human(), "no-extra-authority")
            .unwrap();
        assert_eq!(
            resolved.groups[0].status,
            ResolutionStatus::NeedsClarification,
            "{text}"
        );
        assert!(resolved.groups[0].receipt.is_none(), "{text}");
        let after = service.published().unwrap().1;
        assert_eq!(after.requests[&qa.id].status, RequestStatus::Open, "{text}");
        assert_eq!(after.requests[&qb.id].status, RequestStatus::Open, "{text}");
        assert_eq!(
            canonical_bytes(&after.requests).unwrap(),
            canonical_bytes(&before.requests).unwrap(),
            "{text}"
        );
        assert_eq!(after.receipts, before.receipts, "{text}");
    }
}

#[test]
fn zero_operation_chitchat_and_exact_source_survive_cold_replay_without_regeneration() {
    let (dir, service) = fixture();
    register(&service, "hello", 1, "你好，今天辛苦了");
    save(&service, "hello", vec![]);
    let resolved = service.settle_message(&human(), "hello").unwrap();
    assert!(resolved.groups.is_empty());
    let seq = service.published().unwrap().1.seq;
    register(&service, "hello", 1, "你好，今天辛苦了");
    assert_eq!(service.published().unwrap().1.seq, seq);
    drop(service);
    let reopened = TicketService::open(dir.path(), binding()).unwrap();
    let replay = register(&reopened, "hello", 1, "你好，今天辛苦了");
    assert_eq!(replay.proposal_hash, resolved.proposal_hash);
    let changed = VerifiedUserIngress::from_verified_host(
        "hello".into(),
        HumanIngressRecord {
            text: "改为另一项动作".into(),
            ..replay.ingress.unwrap()
        },
    )
    .unwrap();
    assert!(matches!(
        reopened.register_user_ingress(&human(), &changed),
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        reopened.message_resolution(
            &auth(Principal::User {
                user_id: "other".into()
            }),
            "hello"
        ),
        Err(Error::ScopeDenied(_))
    ));
}

#[test]
fn model_proposal_cannot_turn_chatter_conditional_or_quoted_text_into_user_acceptance() {
    let (_dir, service) = fixture();
    let work = create(&service, "A");
    let _other = create(&service, "A/B");
    let assignment = execute(
        &service,
        "start-accept",
        vec![Operation::Start {
            work_id: work.clone(),
            temp_id: "a".into(),
            workspace: None,
        }],
    )
    .ids["a"]
        .clone();
    let snapshot = service.published().unwrap().1;
    let a = &snapshot.assignments[&assignment];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: "run".into(),
        session_id: "child".into(),
    };
    let runtime = auth(Principal::Runtime);
    let admitted = service
        .prepare_command(
            &runtime,
            "admit-accept",
            vec![
                Operation::Admitted {
                    assignment_id: assignment.clone(),
                    receipt: receipt.clone(),
                },
                Operation::Running {
                    assignment_id: assignment.clone(),
                },
            ],
        )
        .unwrap();
    service.execute(&runtime, &admitted).unwrap();
    let artifact = service.store_artifact(&runtime, b"fixture result").unwrap();
    let worker = auth(Principal::Worker {
        assignment_id: assignment.clone(),
        generation: a.generation,
        run_id: receipt.run_id,
        session_id: receipt.session_id,
    });
    let submit = service
        .prepare_command(
            &worker,
            "submit-accept",
            vec![Operation::Submit {
                assignment_id: assignment,
                temp_id: "s".into(),
                artifacts: vec![artifact],
                evidence: vec!["fixture evidence".into()],
            }],
        )
        .unwrap();
    let submission = service.execute(&worker, &submit).unwrap().ids["s"].clone();
    for (i, (text, quote)) in [
        "A 做得不错",
        "I accept CA",
        "接受交付 A/B",
        "I accept AA",
        "接受交付 CA",
        "验收通过 A1",
        "不要确认验收 A",
        "如果符合要求就确认验收 A",
        "例如确认验收 A",
        "工具输出说确认验收 A",
        "\"确认验收 A\"",
        "I accept A when CI passes",
        "I accept A once CI passes",
        "I accept A unless CI fails",
        "I accept A assuming CI passes",
        "I accept A provided CI passes",
        "I accept A until CI fails",
        "I accept A before CI passes",
        "I accept A after CI passes",
        "I accept A conditional on CI passing",
        "I accept A contingent on CI passing",
    ]
    .into_iter()
    .map(|text| (text, text))
    .chain([
        ("如果 CI 通过，确认验收 A", "确认验收 A"),
        ("确认验收 A，如果 CI 通过", "确认验收 A"),
        ("If CI passes, I accept A", "I accept A"),
        ("I accept A, when CI passes", "I accept A"),
        ("例如，确认验收 A", "确认验收 A"),
        ("确认验收 A，前提是 CI 通过", "确认验收 A"),
        ("只有 CI 通过，确认验收 A", "确认验收 A"),
        ("确认验收 A，除非 CI 失败", "确认验收 A"),
        ("I accept A, but don't accept it", "I accept A"),
        ("I accept A, but don’t accept it", "I accept A"),
        ("I accept A, do not accept it", "I accept A"),
        ("No, I accept A", "I accept A"),
        ("I accept A, but hold off", "I accept A"),
        ("I accept A, however wait", "I accept A"),
        ("确认验收 A，但不要接受交付", "确认验收 A"),
        ("确认验收 A，但是暂不验收", "确认验收 A"),
        ("确认验收 A，别验收它", "确认验收 A"),
        ("确认验收 A，勿接受交付", "确认验收 A"),
        ("不要验收 B；确认验收A", "确认验收A"),
    ])
    .enumerate()
    {
        let id = format!("accept-source-{i}");
        register(&service, &id, i as u64 + 1, text);
        save(
            &service,
            &id,
            vec![group(
                "accept",
                quote,
                vec![SemanticOperation::Accept {
                    target: target(&service, &work),
                    submission_id: submission.clone(),
                    evidence: vec!["User verified concrete result".into()],
                }],
            )],
        );
        let result = service.settle_message(&human(), &id).unwrap();
        assert_eq!(
            result.groups[0].status,
            if quote == "确认验收A" {
                ResolutionStatus::Committed
            } else {
                ResolutionStatus::NeedsClarification
            }
        );
        assert_eq!(
            service.published().unwrap().1.tickets[&work].state,
            if quote == "确认验收A" {
                WorkState::Accepted
            } else {
                WorkState::Submitted
            }
        );
    }
}

#[test]
fn one_message_answers_a_steers_b_creates_and_starts_c_and_cancels_d_with_item_receipts() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let b = create(&service, "B");
    let d = create(&service, "D");
    let q = ask(&service, &a, "A", false);
    register(
        &service,
        "multi",
        1,
        "A 的答案是绿色；B 改成交付蓝色；新建并启动 C；取消 D",
    );
    let mut blue = contract("B");
    blue.objective = "交付蓝色".into();
    let proposed = save(
        &service,
        "multi",
        vec![
            group(
                "answer-a",
                "A 的答案是绿色",
                vec![SemanticOperation::Answer {
                    target: RequestReference::from_request(&q),
                    answer: "绿色".into(),
                }],
            ),
            group(
                "steer-b",
                "B 改成交付蓝色",
                vec![SemanticOperation::Steer {
                    target: target(&service, &b),
                    contract: blue,
                }],
            ),
            group(
                "create-c",
                "新建并启动 C",
                vec![
                    SemanticOperation::Create {
                        temp_id: "C".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract("C"),
                        depends_on: vec![],
                    },
                    SemanticOperation::Ready {
                        target: TicketReference::Temporary { id: "C".into() },
                    },
                    SemanticOperation::Start {
                        target: TicketReference::Temporary { id: "C".into() },
                        temp_id: "assignment-C".into(),
                        workspace: None,
                    },
                ],
            ),
            group(
                "cancel-d",
                "取消 D",
                vec![SemanticOperation::Cancel {
                    target: target(&service, &d),
                }],
            ),
        ],
    );
    assert!(proposed
        .groups
        .iter()
        .all(|g| g.status == ResolutionStatus::Proposed));
    assert_eq!(
        service.published().unwrap().1.tickets.len(),
        3,
        "a saved model proposal has no dispatch effect"
    );
    let resolved = service.settle_message(&human(), "multi").unwrap();
    assert!(resolved
        .groups
        .iter()
        .all(|g| g.status == ResolutionStatus::Committed));
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.requests[&q.id].answer.as_deref(), Some("绿色"));
    assert_eq!(snapshot.tickets[&b].contract_revision, 2);
    assert_eq!(snapshot.tickets[&d].state, WorkState::Cancelled);
    let receipt = resolved.groups[2].receipt.as_ref().unwrap();
    let assignment = &snapshot.assignments[&receipt.ids["assignment-C"]];
    assert_eq!(assignment.work_id, receipt.ids["C"]);
    assert!(assignment.plan.steps.is_empty());
    assert!(snapshot.intents.contains_key(&assignment.dispatch_key));
    let before = snapshot.seq;
    assert_eq!(
        service.settle_message(&human(), "multi").unwrap().groups[2].receipt,
        resolved.groups[2].receipt
    );
    assert_eq!(service.published().unwrap().1.seq, before);
}

#[test]
fn indivisible_create_ready_start_group_rolls_back_while_independent_cancel_commits() {
    let (_dir, service) = fixture();
    let prerequisite = create(&service, "尚未验收");
    let d = create(&service, "D");
    register(&service, "atomic", 1, "创建并执行 C；取消 D");
    save(
        &service,
        "atomic",
        vec![
            group(
                "cannot-start",
                "创建并执行 C",
                vec![
                    SemanticOperation::Create {
                        temp_id: "C".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract("C"),
                        depends_on: vec![target(&service, &prerequisite)],
                    },
                    SemanticOperation::Ready {
                        target: TicketReference::Temporary { id: "C".into() },
                    },
                    SemanticOperation::Start {
                        target: TicketReference::Temporary { id: "C".into() },
                        temp_id: "attempt".into(),
                        workspace: None,
                    },
                ],
            ),
            group(
                "independent",
                "取消 D",
                vec![SemanticOperation::Cancel {
                    target: target(&service, &d),
                }],
            ),
        ],
    );
    let result = service.settle_message(&human(), "atomic").unwrap();
    assert_eq!(result.groups[0].status, ResolutionStatus::Rejected);
    assert!(result.groups[0].receipt.is_none());
    assert_eq!(result.groups[1].status, ResolutionStatus::Committed);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.tickets.len(), 2);
    assert!(snapshot.assignments.is_empty());
    assert!(snapshot.intents.is_empty());
    assert!(!snapshot
        .receipts
        .contains_key(&result.groups[0].operation_id));
    assert_eq!(snapshot.tickets[&d].state, WorkState::Cancelled);
}

#[test]
fn exact_targets_allow_unrelated_changes_but_reject_stale_record_and_generation() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let b = create(&service, "B");
    register(&service, "cas", 1, "取消 A");
    save(
        &service,
        "cas",
        vec![group(
            "a",
            "取消 A",
            vec![SemanticOperation::Cancel {
                target: target(&service, &a),
            }],
        )],
    );
    execute(
        &service,
        "unrelated",
        vec![Operation::Cancel { work_id: b }],
    );
    assert_eq!(
        service.settle_message(&human(), "cas").unwrap().groups[0].status,
        ResolutionStatus::Committed
    );
    let c = create(&service, "C");
    register(&service, "stale", 2, "取消 C");
    save(
        &service,
        "stale",
        vec![group(
            "c",
            "取消 C",
            vec![SemanticOperation::Cancel {
                target: target(&service, &c),
            }],
        )],
    );
    execute(
        &service,
        "steer-current",
        vec![Operation::UpdateContract {
            work_id: c.clone(),
            contract: Contract {
                objective: "新契约".into(),
                ..contract("C")
            },
        }],
    );
    assert_eq!(
        service.settle_message(&human(), "stale").unwrap().groups[0].status,
        ResolutionStatus::Stale
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&c].state,
        WorkState::Ready
    );
}

#[test]
fn later_ingress_cannot_overtake_and_two_proposals_cannot_both_mutate_same_revision() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let version = target(&service, &a);
    for (seq, id) in [(1, "first"), (2, "second")] {
        register(&service, id, seq, "取消 A");
        save(
            &service,
            id,
            vec![group(
                "cancel",
                "取消 A",
                vec![SemanticOperation::Cancel {
                    target: version.clone(),
                }],
            )],
        );
    }
    assert!(matches!(
        service.settle_message(&human(), "second"),
        Err(Error::ResourceBlocked(_))
    ));
    assert_eq!(
        service.settle_message(&human(), "first").unwrap().groups[0].status,
        ResolutionStatus::Committed
    );
    assert_eq!(
        service.settle_message(&human(), "second").unwrap().groups[0].status,
        ResolutionStatus::Stale
    );
}

#[test]
fn queued_ingress_pins_context_after_the_preceding_turn_commits() {
    let (_dir, service) = fixture();
    register(&service, "first", 1, "新建 Alpha");
    let queued = register(&service, "queued", 2, "取消 Alpha");
    assert!(queued.basis.is_none());
    save(
        &service,
        "first",
        vec![group(
            "create",
            "新建 Alpha",
            vec![SemanticOperation::Create {
                temp_id: "a".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract("Alpha"),
                depends_on: vec![],
            }],
        )],
    );
    service.settle_message(&human(), "first").unwrap();
    let input = service
        .resolution_input(&human(), "queued", 100, 65536)
        .unwrap();
    assert_eq!(input.candidates.len(), 1);
    assert_eq!(input.candidates[0].contract.title, "Alpha");
    let fixed = input.basis;
    create(&service, "later-unrelated");
    assert_eq!(
        service
            .resolution_input(&human(), "queued", 100, 65536)
            .unwrap()
            .basis,
        fixed
    );
}

#[test]
fn concurrent_same_message_returns_one_original_receipt_and_one_publication() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    register(&service, "same", 1, "取消 A");
    save(
        &service,
        "same",
        vec![group(
            "cancel",
            "取消 A",
            vec![SemanticOperation::Cancel {
                target: target(&service, &a),
            }],
        )],
    );
    let seq = service.published().unwrap().1.seq;
    let service = Arc::new(service);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let service = service.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.settle_message(&human(), "same").unwrap()
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results[0].groups[0].receipt, results[1].groups[0].receipt);
    assert_eq!(service.published().unwrap().1.seq, seq + 1);
}

#[test]
fn saved_group_replays_after_restart_without_regenerating_proposal_or_dispatch_ids() {
    let (dir, service) = fixture();
    let d = create(&service, "D");
    register(&service, "restart", 1, "创建 C；取消 D");
    let proposal = MessageProposal {
        groups: vec![
            group(
                "create",
                "创建 C",
                vec![SemanticOperation::Create {
                    temp_id: "C".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("C"),
                    depends_on: vec![],
                }],
            ),
            group(
                "cancel",
                "取消 D",
                vec![SemanticOperation::Cancel {
                    target: target(&service, &d),
                }],
            ),
        ],
    };
    service
        .save_message_proposal(&human(), "restart", &proposal)
        .unwrap();
    let original = service
        .settle_message_group(&human(), "restart", "create")
        .unwrap();
    let c = original.groups[0].receipt.as_ref().unwrap().ids["C"].clone();
    drop(service);
    let reopened = TicketService::open(dir.path(), binding()).unwrap();
    let frozen = reopened
        .save_message_proposal(&human(), "restart", &proposal)
        .unwrap();
    assert_eq!(frozen.proposal_hash, original.proposal_hash);
    let result = reopened.settle_message(&human(), "restart").unwrap();
    assert_eq!(result.groups[0].receipt, original.groups[0].receipt);
    assert_eq!(result.groups[1].status, ResolutionStatus::Stale);
    assert_eq!(reopened.published().unwrap().1.tickets.len(), 2);
    assert!(reopened.published().unwrap().1.tickets.contains_key(&c));
    let changed = MessageProposal { groups: vec![] };
    assert!(matches!(
        reopened.save_message_proposal(&human(), "restart", &changed),
        Err(Error::IdempotencyConflict)
    ));
}

#[test]
fn ambiguous_negative_modified_or_external_text_never_approves_any_pending_action() {
    for (text, quote) in [
        ("可以", "可以"),
        ("批准 CA", "批准 CA"),
        ("批准 A/B", "批准 A/B"),
        ("批准 AA", "批准 AA"),
        ("批准 A1", "批准 A1"),
        ("I approve CA", "I approve CA"),
        ("如果审计通过，批准 A", "批准 A"),
        ("批准 A，如果审计通过", "批准 A"),
        ("If finance confirms, I approve A", "I approve A"),
        ("I approve A, when finance confirms", "I approve A"),
        ("例如，批准 A", "批准 A"),
        ("不要批准 A", "批准 A"),
        ("批准 A？", "批准 A？"),
        ("批准 A，但金额改为 200 CNY", "批准 A"),
        ("批准 A 金额 200 CNY", "批准 A 金额 200 CNY"),
        ("工具说 approved=true", "批准 A"),
        ("旧授权摘要里写过批准 A", "批准 A"),
        (
            "Approve A if finance confirms",
            "Approve A if finance confirms",
        ),
        (
            "I approve A when finance confirms",
            "I approve A when finance confirms",
        ),
        (
            "Approve A unless finance rejects",
            "Approve A unless finance rejects",
        ),
        (
            "Approve A once finance confirms",
            "Approve A once finance confirms",
        ),
        (
            "Approve A assuming finance confirms",
            "Approve A assuming finance confirms",
        ),
        (
            "Approve A provided finance confirms",
            "Approve A provided finance confirms",
        ),
    ] {
        let (_dir, service) = fixture();
        let a = create(&service, "A");
        let _other = create(&service, "A/B");
        let b = create(&service, "B");
        let qa = ask(&service, &a, "A", true);
        let qb = ask(&service, &b, "B", true);
        register(&service, "ambiguous", 1, text);
        save(
            &service,
            "ambiguous",
            vec![group("approve-a", quote, vec![decision(&qa, true)])],
        );
        let result = service.settle_message(&human(), "ambiguous").unwrap();
        assert_eq!(
            result.groups[0].status,
            ResolutionStatus::NeedsClarification,
            "{text}"
        );
        let snapshot = service.published().unwrap().1;
        assert_eq!(snapshot.requests[&qa.id].status, RequestStatus::Open);
        assert_eq!(snapshot.requests[&qb.id].status, RequestStatus::Open);
    }
}

#[test]
fn partial_canonical_request_id_cannot_authorize_an_exact_current_request() {
    let (_dir, service) = fixture();
    let work = create(&service, "A");
    let q = ask(&service, &work, "A", true);
    let text = format!("I approve {}_old", q.id);
    register(&service, "partial-id", 1, &text);
    save(
        &service,
        "partial-id",
        vec![group("approve", &text, vec![decision(&q, true)])],
    );
    assert_eq!(
        service
            .settle_message(&human(), "partial-id")
            .unwrap()
            .groups[0]
            .status,
        ResolutionStatus::NeedsClarification
    );
    assert_eq!(
        service.published().unwrap().1.requests[&q.id].status,
        RequestStatus::Open
    );
}

#[test]
fn explicit_approval_amount_requires_the_complete_exact_action_value() {
    for (amount, text, approved) in [
        ("10", "批准 Pay 金额0.10", false),
        ("10", "批准 Pay 金额100", false),
        ("10", "批准 Pay 金额10.5", false),
        ("10", "批准 Pay 金额-10", false),
        ("10", "批准 Pay 金额10 CNY", false),
        ("10", "批准 Pay 金额10 或 100", false),
        ("10", "批准 Pay 金额10 金额100", false),
        ("10", "I approve Pay amount 0.10", false),
        ("10", "批准 Pay 金额10", true),
        ("10", "批准 Pay 金额： 10", true),
        ("10", "I approve Pay amount: 10", true),
        ("100 CNY", "批准 Pay 金额100 CNY", true),
        ("100 CNY", "批准 Pay 金额100 USD", false),
    ] {
        let (_dir, service) = fixture();
        let work = create(&service, "Pay");
        let receipt = execute(
            &service,
            "ask-amount",
            vec![Operation::Ask {
                work_id: work,
                temp_id: "q".into(),
                prompt: "Confirm exact payment".into(),
                action: Some(Action {
                    kind: "payment".into(),
                    target: "Pay".into(),
                    data_hash: content_hash(b"Pay"),
                    amount: Some(amount.into()),
                    permissions: BTreeSet::new(),
                    risk: "fixture only".into(),
                }),
            }],
        );
        let request = service.published().unwrap().1.requests[&receipt.ids["q"]].clone();
        register(&service, "amount", 1, text);
        save(
            &service,
            "amount",
            vec![group("pay", text, vec![decision(&request, true)])],
        );
        let result = service.settle_message(&human(), "amount").unwrap();
        assert_eq!(
            result.groups[0].status,
            if approved {
                ResolutionStatus::Committed
            } else {
                ResolutionStatus::NeedsClarification
            },
            "{text} for {amount}"
        );
        assert_eq!(
            service.published().unwrap().1.requests[&request.id].status,
            if approved {
                RequestStatus::Approved
            } else {
                RequestStatus::Open
            }
        );
    }
}

#[test]
fn approval_quote_cannot_discard_amount_or_conditions_in_its_human_sentence() {
    for (text, approved) in [
        ("批准 A，金额 200 CNY", false),
        ("金额 200 CNY，批准 A", false),
        ("批准 A, amount 200 CNY", false),
        ("批准 A，前提是 CI 通过", false),
        ("批准 A，条件是 CI 通过", false),
        ("只有 CI 通过，批准 A", false),
        ("批准 A，除非 CI 失败", false),
        ("批准 A，但不要批准它", false),
        ("批准 A，别批准它", false),
        ("批准 A, but don't approve it", false),
        ("批准 A, but don’t approve it", false),
        ("批准 A, do not approve it", false),
        ("批准 A, but hold off", false),
        ("批准 A，金额 100 CNY", true),
        ("不要批准 B；批准 A，金额 100 CNY", true),
        ("金额 200 CNY；批准 A，金额 100 CNY", true),
        ("批准 A，前提是 CI 通过；批准 A", false),
    ] {
        let (_dir, service) = fixture();
        let work = create(&service, "A");
        let request = ask(&service, &work, "A", true);
        register(&service, "sentence", 1, text);
        save(
            &service,
            "sentence",
            vec![group("approve", "批准 A", vec![decision(&request, true)])],
        );
        let result = service.settle_message(&human(), "sentence").unwrap();
        assert_eq!(
            result.groups[0].status,
            if approved {
                ResolutionStatus::Committed
            } else {
                ResolutionStatus::NeedsClarification
            },
            "{text}"
        );
        assert_eq!(
            service.published().unwrap().1.requests[&request.id].status,
            if approved {
                RequestStatus::Approved
            } else {
                RequestStatus::Open
            }
        );
    }
}

#[test]
fn multiple_current_approvals_require_a_request_id_before_one_can_be_selected() {
    let (_dir, service) = fixture();
    let work = create(&service, "A");
    let first = ask(&service, &work, "vendor-one", true);
    let second = ask(&service, &work, "vendor-two", true);
    for (seq, text, approve) in [
        (1, "批准 A".to_owned(), true),
        (2, format!("批准 {work}"), true),
        (3, "拒绝 A".to_owned(), false),
    ] {
        let id = format!("ambiguous-request-{seq}");
        register(&service, &id, seq, &text);
        save(
            &service,
            &id,
            vec![group("decide", &text, vec![decision(&first, approve)])],
        );
        assert_eq!(
            service.settle_message(&human(), &id).unwrap().groups[0].status,
            ResolutionStatus::NeedsClarification
        );
        let snapshot = service.published().unwrap().1;
        assert_eq!(snapshot.requests[&first.id].status, RequestStatus::Open);
        assert_eq!(snapshot.requests[&second.id].status, RequestStatus::Open);
    }
    let text = format!("批准 {}", first.id);
    register(&service, "exact-request", 4, &text);
    save(
        &service,
        "exact-request",
        vec![group("first", &text, vec![decision(&first, true)])],
    );
    assert_eq!(
        service
            .settle_message(&human(), "exact-request")
            .unwrap()
            .groups[0]
            .status,
        ResolutionStatus::Committed
    );
    assert_eq!(
        service.published().unwrap().1.requests[&second.id].status,
        RequestStatus::Open
    );
    register(&service, "only-current-request", 5, "批准 A");
    save(
        &service,
        "only-current-request",
        vec![group("second", "批准 A", vec![decision(&second, true)])],
    );
    assert_eq!(
        service
            .settle_message(&human(), "only-current-request")
            .unwrap()
            .groups[0]
            .status,
        ResolutionStatus::Committed
    );
}

#[test]
fn explicit_approval_a_never_releases_b_and_explicit_denial_remains_distinct() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let b = create(&service, "B");
    let qa = ask(&service, &a, "A", true);
    let qb = ask(&service, &b, "B", true);
    register(&service, "approve-a", 1, "如果审计通过，批准 B；批准A");
    save(
        &service,
        "approve-a",
        vec![group("a", "批准A", vec![decision(&qa, true)])],
    );
    assert_eq!(
        service
            .settle_message(&human(), "approve-a")
            .unwrap()
            .groups[0]
            .status,
        ResolutionStatus::Committed
    );
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.requests[&qa.id].status, RequestStatus::Approved);
    assert_eq!(snapshot.requests[&qb.id].status, RequestStatus::Open);
    register(&service, "deny-b", 2, "不批准 B");
    save(
        &service,
        "deny-b",
        vec![group("b", "不批准 B", vec![decision(&qb, false)])],
    );
    assert_eq!(
        service.settle_message(&human(), "deny-b").unwrap().groups[0].status,
        ResolutionStatus::Committed
    );
    assert_eq!(
        service.published().unwrap().1.requests[&qb.id].status,
        RequestStatus::Denied
    );
}

#[test]
fn forged_request_revision_generation_assignment_or_fingerprint_is_stale() {
    for field in [
        "prompt_revision",
        "generation",
        "assignment",
        "work",
        "fingerprint",
    ] {
        let (_dir, service) = fixture();
        let a = create(&service, "A");
        let qa = ask(&service, &a, "A", true);
        let mut proposed = decision(&qa, true);
        if let SemanticOperation::DecideApproval {
            target,
            fingerprint,
            ..
        } = &mut proposed
        {
            match field {
                "prompt_revision" => target.prompt_revision += 1,
                "generation" => target.generation += 1,
                "assignment" => target.assignment_id = Some("forged".into()),
                "work" => target.work_id = "forged-sibling".into(),
                _ => *fingerprint = content_hash(b"changed amount"),
            }
        }
        register(&service, "forged", 1, "批准 A");
        save(
            &service,
            "forged",
            vec![group("a", "批准 A", vec![proposed])],
        );
        assert_eq!(
            service.settle_message(&human(), "forged").unwrap().groups[0].status,
            ResolutionStatus::Stale,
            "{field}"
        );
        assert_eq!(
            service.published().unwrap().1.requests[&qa.id].status,
            RequestStatus::Open
        );
    }
}

#[test]
fn same_name_approval_needs_exact_id_and_optional_reply_reference_does_not_authorize() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let other = execute(
        &service,
        "second-a",
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract("A"),
                depends_on: BTreeSet::new(),
            },
            Operation::Ready {
                work_id: "work".into(),
            },
        ],
    )
    .ids["work"]
        .clone();
    let qa = ask(&service, &a, "A", true);
    let qb = ask(&service, &other, "other-A", true);
    let ingress = VerifiedUserIngress::from_verified_host(
        "same-name".into(),
        HumanIngressRecord {
            user_id: "human".into(),
            source_ingress_seq: 1,
            text: "批准 A".into(),
            thread_id: Some(a.clone()),
            in_reply_to: Some(qa.id.clone()),
            correlation_id: None,
        },
    )
    .unwrap();
    service.register_user_ingress(&human(), &ingress).unwrap();
    save(
        &service,
        "same-name",
        vec![group("a", "批准 A", vec![decision(&qa, true)])],
    );
    assert_eq!(
        service
            .settle_message(&human(), "same-name")
            .unwrap()
            .groups[0]
            .status,
        ResolutionStatus::NeedsClarification
    );
    let text = format!("批准 {}", qa.id);
    register(&service, "exact-id", 2, &text);
    save(
        &service,
        "exact-id",
        vec![group("exact", &text, vec![decision(&qa, true)])],
    );
    assert_eq!(
        service.settle_message(&human(), "exact-id").unwrap().groups[0].status,
        ResolutionStatus::Committed
    );
    assert_eq!(
        service.published().unwrap().1.requests[&qb.id].status,
        RequestStatus::Open
    );
}

#[test]
fn question_answers_require_exact_named_human_evidence() {
    for (text, quote, answer, second_question, expected) in [
        (
            "A 使用绿色",
            "A 使用绿色",
            "绿色",
            false,
            ResolutionStatus::Committed,
        ),
        (
            "A使用绿色",
            "A使用绿色",
            "绿色",
            false,
            ResolutionStatus::Committed,
        ),
        (
            "不要A使用绿色",
            "A使用绿色",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A使用绿色，但别回答",
            "A使用绿色",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是绿色；A 的答案是绿色",
            "A 的答案是绿色",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是",
            "A 的答案是",
            "",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是不",
            "A 的答案是不",
            "不",
            false,
            ResolutionStatus::Committed,
        ),
        (
            "hello",
            "hello",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是绿色",
            "A 的答案是绿色",
            "红色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "如果需要，A 的答案是绿色",
            "A 的答案是绿色",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是绿色，但别回答",
            "A 的答案是绿色",
            "绿色",
            false,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是绿色",
            "A 的答案是绿色",
            "绿色",
            true,
            ResolutionStatus::NeedsClarification,
        ),
        (
            "A 的答案是绿色",
            "A 的答案是绿色",
            "绿色",
            false,
            ResolutionStatus::Committed,
        ),
        (
            "Answer A: green",
            "Answer A: green",
            "green",
            false,
            ResolutionStatus::Committed,
        ),
    ] {
        let (_dir, service) = fixture();
        let a = create(&service, "A");
        let q = ask(&service, &a, "first", false);
        if second_question {
            ask(&service, &a, "second", false);
        }
        register(&service, "answer-evidence", 1, text);
        save(
            &service,
            "answer-evidence",
            vec![group(
                "answer",
                quote,
                vec![SemanticOperation::Answer {
                    target: RequestReference::from_request(&q),
                    answer: answer.into(),
                }],
            )],
        );
        let result = service.settle_message(&human(), "answer-evidence").unwrap();
        assert_eq!(result.groups[0].status, expected, "{text}");
        let snapshot = service.published().unwrap().1;
        assert_eq!(
            snapshot.requests[&q.id].answer.as_deref(),
            (expected == ResolutionStatus::Committed).then_some(answer),
            "{text}"
        );
        if expected != ResolutionStatus::Committed {
            assert_eq!(snapshot.requests[&q.id].status, RequestStatus::Open);
            let exact = format!("{} 的答案是精确回答", q.id);
            register(&service, "exact-request", 2, &exact);
            save(
                &service,
                "exact-request",
                vec![group(
                    "answer-exact",
                    &exact,
                    vec![SemanticOperation::Answer {
                        target: RequestReference::from_request(&q),
                        answer: "精确回答".into(),
                    }],
                )],
            );
            assert_eq!(
                service
                    .settle_message(&human(), "exact-request")
                    .unwrap()
                    .groups[0]
                    .status,
                ResolutionStatus::Committed
            );
            assert_eq!(
                service.published().unwrap().1.requests[&q.id]
                    .answer
                    .as_deref(),
                Some("精确回答")
            );
        }
    }
}

#[test]
fn question_answer_does_not_authorize_and_model_json_cannot_publish_runtime_or_provenance() {
    let (_dir, service) = fixture();
    let a = create(&service, "A");
    let b = create(&service, "B");
    let qa = ask(&service, &a, "A", false);
    let qb = ask(&service, &b, "B", true);
    register(&service, "answer", 1, "A 的答案是可以");
    save(
        &service,
        "answer",
        vec![group(
            "a",
            "A 的答案是可以",
            vec![SemanticOperation::Answer {
                target: RequestReference::from_request(&qa),
                answer: "可以".into(),
            }],
        )],
    );
    assert_eq!(
        service.settle_message(&human(), "answer").unwrap().groups[0].status,
        ResolutionStatus::Committed
    );
    assert_eq!(
        service.published().unwrap().1.requests[&qb.id].status,
        RequestStatus::Open
    );
    assert!(serde_json::from_value::<SemanticOperation>(
        serde_json::json!({"op":"record_effect","assignment_id":"fake"})
    )
    .is_err());
    let mut serialized = serde_json::to_value(decision(&qb, true)).unwrap();
    serialized["approved"] = serde_json::json!(true);
    assert!(serde_json::from_value::<SemanticOperation>(serialized).is_err());
    let saved = service.message_resolution(&human(), "answer").unwrap();
    let command = service
        .prepare_command(
            &human(),
            "json-ingress",
            vec![Operation::ResolveMessage { resolution: saved }],
        )
        .unwrap();
    assert!(matches!(
        service.execute(&human(), &command),
        Err(Error::ScopeDenied(_))
    ));
    let ingress = VerifiedUserIngress::from_verified_host(
        "worker-says-user".into(),
        HumanIngressRecord {
            user_id: "human".into(),
            source_ingress_seq: 2,
            text: "批准 B".into(),
            thread_id: None,
            in_reply_to: None,
            correlation_id: None,
        },
    )
    .unwrap();
    assert!(matches!(
        service.register_user_ingress(&supervisor(), &ingress),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        service.register_user_ingress(&auth(Principal::Runtime), &ingress),
        Err(Error::ScopeDenied(_))
    ));
}

#[test]
fn fixed_bounded_candidate_context_reports_omissions_and_never_truncates_contracts() {
    let (_dir, service) = fixture();
    for title in ["A", "B", "C"] {
        create(&service, title);
    }
    register(&service, "bounded", 1, "查看所有工作");
    let first = service
        .resolution_input(&human(), "bounded", 1, 65536)
        .unwrap();
    assert!(first.truncated);
    assert_eq!(first.omitted_count, 2);
    assert_eq!(first.coverage, "partial");
    create(&service, "later");
    let fixed = service
        .resolution_input(&human(), "bounded", 100, 65536)
        .unwrap();
    assert_eq!(fixed.candidates.len(), 3);
    assert_eq!(fixed.coverage, "complete");
    assert!(matches!(
        service.resolution_input(&human(), "bounded", 100, 128),
        Err(Error::ContextBudgetExceeded)
    ));
    let wire = serde_json::to_string(&fixed).unwrap();
    assert!(!wire.contains("plan_revision"));
    assert!(!wire.contains("steps"));
}

#[test]
fn hundred_valid_contracts_produce_a_complete_item_byte_bounded_page() {
    let (_dir, service) = fixture();
    let objective = "x".repeat(1024);
    for batch in 0..5 {
        execute(
            &service,
            &format!("large-{batch}"),
            (0..20)
                .map(|i| {
                    let title = format!("candidate-{}", batch * 20 + i);
                    let mut c = contract(&title);
                    c.objective.clone_from(&objective);
                    Operation::Create {
                        temp_id: title,
                        kind: TicketKind::Work,
                        parent: None,
                        contract: c,
                        depends_on: BTreeSet::new(),
                    }
                })
                .collect(),
        );
    }
    register(&service, "large-page", 1, "查看所有工作");
    let input = service
        .resolution_input(&human(), "large-page", 100, 65536)
        .unwrap();
    assert!(!input.candidates.is_empty());
    assert!(input.candidates.len() < 100);
    assert!(canonical_bytes(&input).unwrap().len() <= 65536);
    assert_eq!(input.omitted_count, 100 - input.candidates.len());
    assert!(input.truncated);
    assert_eq!(input.coverage, "partial");
    for candidate in &input.candidates {
        assert_eq!(candidate.contract.objective, objective);
        assert_eq!(candidate.contract.constraints, vec!["独立工作"]);
        assert_eq!(candidate.contract.acceptance, vec!["具体证据"]);
    }
    assert!(matches!(
        service.resolution_input(&human(), "large-page", 100, 1024),
        Err(Error::ContextBudgetExceeded)
    ));
}

#[test]
fn publication_fault_recovers_old_or_new_complete_group_with_original_proposal() {
    for point in [FaultPoint::BeforeHeadRename, FaultPoint::AfterHeadRename] {
        let (dir, service) = fixture();
        register(&service, "fault", 1, "创建 C");
        let proposal = MessageProposal {
            groups: vec![group(
                "c",
                "创建 C",
                vec![SemanticOperation::Create {
                    temp_id: "C".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("C"),
                    depends_on: vec![],
                }],
            )],
        };
        service
            .save_message_proposal(&human(), "fault", &proposal)
            .unwrap();
        let old = service.published().unwrap();
        service.set_publication_fault(Some(Arc::new(move |seen| {
            if seen == point {
                Err(std::io::Error::other("fixture resolution publication"))
            } else {
                Ok(())
            }
        })));
        assert!(service.settle_message(&human(), "fault").is_err());
        assert_eq!(service.published().unwrap().0, old.0);
        drop(service);
        let reopened = TicketService::open(dir.path(), binding()).unwrap();
        let saved = reopened.message_resolution(&human(), "fault").unwrap();
        assert_eq!(
            saved.proposal_hash,
            old.1.resolutions["fault"].proposal_hash
        );
        if point == FaultPoint::AfterHeadRename {
            assert_eq!(saved.groups[0].status, ResolutionStatus::Committed);
            assert_eq!(reopened.published().unwrap().1.tickets.len(), 1);
            let seq = reopened.published().unwrap().1.seq;
            reopened.settle_message(&human(), "fault").unwrap();
            assert_eq!(reopened.published().unwrap().1.seq, seq);
        } else {
            assert_eq!(saved.groups[0].status, ResolutionStatus::Proposed);
            assert!(reopened.published().unwrap().1.tickets.is_empty());
            assert_eq!(
                reopened.settle_message(&human(), "fault").unwrap().groups[0].status,
                ResolutionStatus::Stale
            );
        }
    }
}

#[test]
fn model_contracts_require_user_acceptance_for_create_and_steer() {
    for steer in [false, true] {
        for required in [false, true] {
            let (_dir, service) = fixture();
            let work = steer.then(|| create(&service, "A"));
            let text = if steer {
                "steer A but keep my acceptance"
            } else {
                "create A and require my acceptance"
            };
            register(&service, "contract-bit", 1, text);
            let mut proposed = contract("A");
            proposed.user_acceptance_required = required;
            let operations = if let Some(id) = &work {
                vec![SemanticOperation::Steer {
                    target: target(&service, id),
                    contract: proposed,
                }]
            } else {
                vec![SemanticOperation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: proposed,
                    depends_on: vec![],
                }]
            };
            save(
                &service,
                "contract-bit",
                vec![group("contract", text, operations)],
            );
            let before = service.published().unwrap().1;
            let result = service.settle_message(&human(), "contract-bit").unwrap();
            let after = service.published().unwrap().1;
            if required {
                assert_eq!(result.groups[0].status, ResolutionStatus::Committed);
                let id = work
                    .as_ref()
                    .unwrap_or_else(|| &result.groups[0].receipt.as_ref().unwrap().ids["work"]);
                assert!(after.tickets[id].contract.user_acceptance_required);
            } else {
                assert_eq!(result.groups[0].status, ResolutionStatus::Rejected);
                assert!(result.groups[0].receipt.is_none());
                assert_eq!(after.seq, before.seq);
                assert_eq!(
                    canonical_bytes(&after.tickets).unwrap(),
                    canonical_bytes(&before.tickets).unwrap()
                );
                assert_eq!(
                    canonical_bytes(&after.receipts).unwrap(),
                    canonical_bytes(&before.receipts).unwrap()
                );
                assert_eq!(
                    canonical_bytes(&after.assignments).unwrap(),
                    canonical_bytes(&before.assignments).unwrap()
                );
                assert_eq!(
                    canonical_bytes(&after.intents).unwrap(),
                    canonical_bytes(&before.intents).unwrap()
                );
                assert_eq!(
                    canonical_bytes(&service.settle_message(&human(), "contract-bit").unwrap())
                        .unwrap(),
                    canonical_bytes(&result).unwrap()
                );
            }
        }
    }
}

#[test]
fn semantic_start_preflights_complete_context_before_publishing_claims() {
    for contract_bytes in [60000, 65500] {
        let (_dir, service) = fixture();
        let mut large = contract("A");
        // The model schema bounds objective, but permits this one long constraint.
        large.constraints = vec![];
        large
            .constraints
            .push("x".repeat(contract_bytes - canonical_bytes(&large).unwrap().len() - 2));
        assert_eq!(canonical_bytes(&large).unwrap().len(), contract_bytes);
        register(&service, "large-start", 1, "create A and start it");
        save(
            &service,
            "large-start",
            vec![group(
                "start",
                "create A and start it",
                vec![
                    SemanticOperation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: large,
                        depends_on: vec![],
                    },
                    SemanticOperation::Ready {
                        target: TicketReference::Temporary { id: "work".into() },
                    },
                    SemanticOperation::Start {
                        target: TicketReference::Temporary { id: "work".into() },
                        temp_id: "assignment".into(),
                        workspace: None,
                    },
                ],
            )],
        );
        let before = service.published().unwrap().1;
        let result = service.settle_message(&human(), "large-start").unwrap();
        let after = service.published().unwrap().1;
        if contract_bytes == 65500 {
            assert_eq!(result.groups[0].status, ResolutionStatus::Rejected);
            assert!(result.groups[0]
                .reason
                .as_ref()
                .unwrap()
                .contains("context_budget_exceeded"));
            assert!(result.groups[0].receipt.is_none());
            assert_eq!(
                canonical_bytes(&after.tickets).unwrap(),
                canonical_bytes(&before.tickets).unwrap()
            );
            assert_eq!(
                canonical_bytes(&after.assignments).unwrap(),
                canonical_bytes(&before.assignments).unwrap()
            );
            assert_eq!(
                canonical_bytes(&after.intents).unwrap(),
                canonical_bytes(&before.intents).unwrap()
            );
            assert_eq!(
                canonical_bytes(&after.receipts).unwrap(),
                canonical_bytes(&before.receipts).unwrap()
            );
            assert_eq!(
                canonical_bytes(&service.settle_message(&human(), "large-start").unwrap()).unwrap(),
                canonical_bytes(&result).unwrap()
            );
            assert_eq!(service.published().unwrap().1.seq, after.seq);
        } else {
            assert_eq!(result.groups[0].status, ResolutionStatus::Committed);
            let assignment = &result.groups[0].receipt.as_ref().unwrap().ids["assignment"];
            assert!(
                canonical_bytes(
                    &service
                        .child_context_packet(&supervisor(), assignment, WORK_CONTEXT_BYTES_LIMIT)
                        .unwrap()
                )
                .unwrap()
                .len()
                    <= WORK_CONTEXT_BYTES_LIMIT
            );
        }
    }
}

#[test]
fn answer_title_aliases_cannot_steal_another_named_question_or_identifier() {
    let mut unexpected = vec![];
    for case in ["trimmed-short", "trailing-space", "request-id", "ticket-id"] {
        let (_dir, service) = fixture();
        let other_title = match case {
            "trimmed-short" => "A ",
            "trailing-space" => "A",
            _ => "B",
        };
        let other = create(&service, other_title);
        let other_question = ask(&service, &other, "other", false);
        let title = match case {
            "trimmed-short" => "A",
            "trailing-space" => "A ",
            "request-id" => &other_question.id,
            "ticket-id" => &other,
            _ => unreachable!(),
        };
        let work = create(&service, title);
        let question = ask(&service, &work, "target", false);
        let text = match case {
            "trimmed-short" | "trailing-space" => "A 使用绿色".into(),
            "request-id" => format!("Answer {}: green", other_question.id),
            "ticket-id" => format!("Answer {other}: green"),
            _ => unreachable!(),
        };
        let answer = if case.starts_with("trimmed") || case == "trailing-space" {
            "绿色"
        } else {
            "green"
        };
        register(&service, "alias", 1, &text);
        save(
            &service,
            "alias",
            vec![group(
                "answer",
                &text,
                vec![SemanticOperation::Answer {
                    target: RequestReference::from_request(&question),
                    answer: answer.into(),
                }],
            )],
        );
        let before = service.published().unwrap().1;
        let result = service.settle_message(&human(), "alias").unwrap();
        if result.groups[0].status != ResolutionStatus::NeedsClarification {
            unexpected.push(format!(
                "{case}: {:?} for wrong/ambiguous target",
                result.groups[0].status
            ));
            continue;
        }
        assert!(result.groups[0].receipt.is_none());
        let after = service.published().unwrap().1;
        assert_eq!(
            canonical_bytes(&after.requests).unwrap(),
            canonical_bytes(&before.requests).unwrap(),
            "{case}"
        );
        assert_eq!(
            canonical_bytes(&after.tickets).unwrap(),
            canonical_bytes(&before.tickets).unwrap(),
            "{case}"
        );
        assert_eq!(
            canonical_bytes(&after.receipts).unwrap(),
            canonical_bytes(&before.receipts).unwrap(),
            "{case}"
        );
        let exact = format!("Answer {}: {answer}", question.id);
        register(&service, "exact-alias-retry", 2, &exact);
        save(
            &service,
            "exact-alias-retry",
            vec![group(
                "answer",
                &exact,
                vec![SemanticOperation::Answer {
                    target: RequestReference::from_request(&question),
                    answer: answer.into(),
                }],
            )],
        );
        let exact_result = service
            .settle_message(&human(), "exact-alias-retry")
            .unwrap();
        assert_eq!(
            exact_result.groups[0].status,
            ResolutionStatus::Committed,
            "{case}"
        );
        let snapshot = service.published().unwrap().1;
        assert_eq!(
            snapshot.requests[&question.id].answer.as_deref(),
            Some(answer),
            "{case}"
        );
        assert_eq!(
            snapshot.requests[&other_question.id].status,
            RequestStatus::Open,
            "{case}"
        );
    }
    assert!(unexpected.is_empty(), "{unexpected:?}");
}
