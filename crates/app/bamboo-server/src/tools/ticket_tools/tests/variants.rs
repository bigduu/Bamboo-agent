use super::*;

#[test]
fn public_work_update_schema_rejects_cross_variant_fields_that_typed_decoders_reject() {
    let validator = jsonschema::validator_for(&schema::parameters("work_update")).unwrap();
    let human = json!({"message_id":"canonical-human","proposal":{"groups":[]}});
    let mutation = json!({"operation_id":"reject-current","expected_seq":21,"expected_epoch":3,
        "operations":[{"op":"reject","work_id":"work","submission_id":"submission","reason":"actual source reads blocked"}]});
    assert!(validator.is_valid(&human));
    assert!(serde_json::from_value::<SemanticMutation>(human.clone()).is_ok());
    assert!(validator.is_valid(&mutation));
    assert!(serde_json::from_value::<Mutation>(mutation.clone()).is_ok());
    // These partial mixed shapes were schema-valid in the actual DeepSeek
    // failure, while the Host's exact typed decoder correctly rejected them.
    for (key, value) in mutation.as_object().unwrap() {
        let mut mixed = human.clone();
        mixed[key] = value.clone();
        assert!(serde_json::from_value::<SemanticMutation>(mixed.clone()).is_err());
        assert!(
            !validator.is_valid(&mixed),
            "schema accepted cross-variant {key}: {mixed}"
        );
    }
    for (key, value) in human.as_object().unwrap() {
        let mut mixed = mutation.clone();
        mixed[key] = value.clone();
        assert!(serde_json::from_value::<Mutation>(mixed.clone()).is_err());
        assert!(
            !validator.is_valid(&mixed),
            "schema accepted cross-variant {key}: {mixed}"
        );
    }
}

#[tokio::test]
async fn supervisor_without_pending_human_can_reject_but_not_accept_user_required_submission() {
    let f = Fixture::new().await;
    let (service, supervisor) = f
        .app
        .authority(Principal::Supervisor {
            session_id: f.session.id.clone(),
        })
        .await
        .unwrap();
    let start = service
        .prepare_command(
            &supervisor,
            "fixture-start",
            vec![
                Operation::Create {
                    temp_id: "w".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    depends_on: Default::default(),
                    contract: Contract {
                        title: "Review handoff".into(),
                        objective: "Read actual sources".into(),
                        constraints: vec![],
                        acceptance: vec!["Source evidence".into()],
                        user_acceptance_required: true,
                        allowed_tools: std::collections::BTreeSet::from(["Task".into()]),
                    },
                },
                Operation::Ready {
                    work_id: "w".into(),
                },
                Operation::Start {
                    work_id: "w".into(),
                    temp_id: "a".into(),
                    workspace: None,
                },
            ],
        )
        .unwrap();
    let started = service.execute(&supervisor, &start).unwrap();
    let work = started.ids["w"].clone();
    let assignment_id = started.ids["a"].clone();
    let snapshot = service.published().unwrap().1;
    let assignment = &snapshot.assignments[&assignment_id];
    let runtime = Authority::from_verified_host(snapshot.binding.clone(), Principal::Runtime);
    let admit = service
        .prepare_command(
            &runtime,
            "fixture-admit",
            vec![
                Operation::Admitted {
                    assignment_id: assignment_id.clone(),
                    receipt: RuntimeReceipt {
                        dispatch_key: assignment.dispatch_key.clone(),
                        spec_hash: snapshot.intents[&assignment.dispatch_key].spec_hash.clone(),
                        run_id: "fixture-run".into(),
                        session_id: "fixture-child".into(),
                    },
                },
                Operation::Running {
                    assignment_id: assignment_id.clone(),
                },
            ],
        )
        .unwrap();
    service.execute(&runtime, &admit).unwrap();
    let worker = Authority::from_verified_host(
        snapshot.binding,
        Principal::Worker {
            assignment_id: assignment_id.clone(),
            generation: 1,
            run_id: "fixture-run".into(),
            session_id: "fixture-child".into(),
        },
    );
    let artifact = service
        .store_artifact(&runtime, b"Actual source reads were blocked")
        .unwrap();
    let submit = service
        .prepare_command(
            &worker,
            "fixture-submit",
            vec![Operation::Submit {
                assignment_id,
                temp_id: "s".into(),
                evidence: vec!["Read tools were denied".into()],
                artifacts: vec![artifact],
            }],
        )
        .unwrap();
    let submitted = service.execute(&worker, &submit).unwrap();
    let submission = submitted.ids["s"].clone();
    assert!(f.app.oldest_unresolved_human().unwrap().is_none());
    let before = service.published().unwrap().1;
    let tool = f.tool("work_update");
    let forged_human = invoke(
        &tool,
        json!({"message_id":submission,"proposal":{"groups":[]}}),
        f.ctx(),
    )
    .await;
    assert_eq!(forged_human.1["status_code"], 403);
    let accept = f.mutation("not-user-accept",json!([{"op":"accept","work_id":work,"submission_id":submission,"evidence":["Supervisor review"]}]));
    assert_eq!(invoke(&tool, accept, f.ctx()).await.1["status_code"], 403);
    assert_eq!(
        canonical_bytes(&service.published().unwrap().1).unwrap(),
        canonical_bytes(&before).unwrap()
    );
    let args = f.mutation("supervisor-reject",json!([{"op":"reject","work_id":work,"submission_id":submission,"reason":"Actual source reads were blocked"}]));
    assert!(jsonschema::validator_for(&tool.parameters_schema())
        .unwrap()
        .is_valid(&args));
    let rejected = invoke(&tool, args.clone(), f.ctx()).await;
    assert!(rejected.0, "{rejected:?}");
    assert_eq!(rejected.1["status"], "committed");
    assert_eq!(
        invoke(&tool, args, f.ctx()).await,
        rejected,
        "exact retry replays its receipt"
    );
    let after = service.published().unwrap().1;
    assert_eq!(after.tickets[&work].state, WorkState::Ready);
    assert!(after.tickets[&work].current_submission.is_none());
    assert_eq!(
        canonical_bytes(&after.submissions).unwrap(),
        canonical_bytes(&before.submissions).unwrap()
    );
    assert_eq!(after.seq, before.seq + 1);
}
