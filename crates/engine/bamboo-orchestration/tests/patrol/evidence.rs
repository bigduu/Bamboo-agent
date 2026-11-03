//! Pure evidence rules over the existing canonical Ticket fixture. These tests
//! neither resolve native memory nor grant Host/runtime authority.
use super::*;
use bamboo_domain::{ProjectManifest, ProjectStatus};
use bamboo_orchestration::patrol::diagnostic_input::prepare_diagnostic_input;
use bamboo_orchestration::patrol::evidence::{
    EvidencePlan, UnmeteredWorkPatrolEvidence, WorkPatrolEvidenceCoverage, WorkPatrolEvidenceLimit,
    WorkPatrolSourceOutcome, MAX_WORK_PATROL_EVIDENCE_BYTES,
};
use bamboo_tickets::{Error, ProjectContextSnapshot, Snapshot};
type Plan = EvidencePlan<String, String>;

// Counterfactual projection controls retain the snapshot's full immutable spec
// and receipt consistency; they are not claimed as persisted lifecycle writes.
fn context(
    snapshot: &Snapshot,
    scope: &Scope,
    edit: impl FnOnce(&mut ProjectContextSnapshot),
) -> Snapshot {
    let mut changed = snapshot.clone();
    let key = changed.assignments[&scope.assignment_id]
        .dispatch_key
        .clone();
    let mut value = changed.project.contexts
        [&changed.project.assignment_contexts[&scope.assignment_id]]
        .clone();
    edit(&mut value);
    let hash = bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&value).unwrap());
    changed.project.contexts.insert(hash.clone(), value);
    changed
        .project
        .assignment_contexts
        .insert(scope.assignment_id.clone(), hash.clone());
    let intent = changed.intents.get_mut(&key).unwrap();
    intent.immutable_spec.project_context_hash = Some(hash);
    intent.spec_hash = bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(&intent.immutable_spec).unwrap(),
    );
    intent.receipt.as_mut().unwrap().spec_hash = intent.spec_hash.clone();
    changed
        .assignments
        .get_mut(&scope.assignment_id)
        .unwrap()
        .runtime
        .as_mut()
        .unwrap()
        .spec_hash = intent.spec_hash.clone();
    changed
}

#[test]
fn evidence_plan_keeps_adopted_context_and_rejects_mismatched_snapshot_or_cursor() {
    let fixture = Fixture::new();
    let (commit, snapshot) = fixture.service().published().unwrap();
    let scope = &fixture.scope;
    let mut candidate = snapshot.clone();
    candidate
        .project
        .work_contexts
        .insert(scope.work_id.clone(), "newer-candidate".into());
    let plan = Plan::prepare(&candidate, scope, &commit, 0).unwrap();
    let evidence = plan.finish().unwrap();
    assert_eq!(
        evidence.adopted_context_hash,
        snapshot.project.assignment_contexts[&scope.assignment_id]
    );
    assert_eq!(
        evidence.coverage,
        WorkPatrolEvidenceCoverage::BoundedSnapshot
    );
    assert_eq!(evidence.through_cursor, snapshot.seq);
    assert_eq!(
        evidence.execution_facts_revision,
        snapshot.execution_facts_revision()
    );
    assert_eq!(evidence.reference_count, 3);
    assert!(!evidence.all_sources_matched);
    assert!(matches!(
        Plan::prepare(&snapshot, scope, &commit, snapshot.seq + 1),
        Err(Error::RevisionConflict)
    ));
    let mut missing = snapshot.clone();
    missing.assignments.clear();
    assert!(Plan::prepare(&missing, scope, &commit, 0).is_err());
    let mut wrong = scope.clone();
    wrong.generation += 1;
    assert!(Plan::prepare(&snapshot, &wrong, &commit, 0).is_err());
    let mut corrupt = snapshot.clone();
    corrupt
        .project
        .assignment_contexts
        .insert(scope.assignment_id.clone(), "foreign".into());
    assert!(matches!(
        Plan::prepare(&corrupt, scope, &commit, 0),
        Err(Error::IdempotencyConflict)
    ));
    let mut corrupt = snapshot.clone();
    let hash = &snapshot.project.assignment_contexts[&scope.assignment_id];
    corrupt
        .project
        .contexts
        .get_mut(hash)
        .unwrap()
        .project_revision += 1;
    assert!(matches!(
        Plan::prepare(&corrupt, scope, &commit, 0),
        Err(Error::IdempotencyConflict)
    ));
}

#[test]
fn evidence_ref_source_and_aggregate_bounds_preserve_partial_outcomes() {
    let fixture = Fixture::new();
    let (commit, snapshot) = fixture.service().published().unwrap();
    let scope = &fixture.scope;
    let extra = context(&snapshot, scope, |value| {
        value.sources = (0..7)
            .map(|i| (format!("opaque:{i}"), "version".into()))
            .collect()
    });
    assert!(matches!(
        Plan::prepare(&extra, scope, &commit, 0),
        Err(Error::ContextBudgetExceeded)
    ));
    let empty = context(&snapshot, scope, |value| value.sources.clear());
    assert!(
        !Plan::prepare(&empty, scope, &commit, 0)
            .unwrap()
            .finish()
            .unwrap()
            .all_sources_matched
    );
    let three = context(&snapshot, scope, |value| {
        value.sources = (0..3)
            .map(|i| (format!("opaque:{i}"), "version".into()))
            .collect()
    });
    let mut plan = Plan::prepare(&three, scope, &commit, 0).unwrap();
    for index in 0..3 {
        plan.record_source(
            index,
            WorkPatrolSourceOutcome::VersionMatched {
                reference: "adapter-owned-ref".into(),
                title: "Bounded body".into(),
                content: "x".repeat(5600),
            },
        )
        .unwrap();
    }
    let evidence = plan.finish().unwrap();
    assert!(!evidence.all_sources_matched);
    assert!(matches!(
        evidence.sources[2].outcome,
        WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::AggregateBytes,
            ..
        }
    ));
    assert!(serde_json::to_vec(&evidence).unwrap().len() <= MAX_WORK_PATROL_EVIDENCE_BYTES);
    let mut plan = Plan::prepare(&snapshot, scope, &commit, 0).unwrap();
    plan.record_source(
        0,
        WorkPatrolSourceOutcome::VersionMatched {
            reference: "opaque".into(),
            title: "large".into(),
            content: "界".repeat(3000),
        },
    )
    .unwrap();
    assert!(matches!(
        plan.finish().unwrap().sources[0].outcome,
        WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::SourceBytes,
            ..
        }
    ));
    plan.record_source(0, WorkPatrolSourceOutcome::Unsupported)
        .unwrap();
    assert!(!plan.finish().unwrap().all_sources_matched);
    assert!(plan
        .record_source(1, WorkPatrolSourceOutcome::Missing)
        .is_err());
    plan.record_source(
        0,
        WorkPatrolSourceOutcome::VersionMatched {
            reference: "opaque".into(),
            title: "exact".into(),
            content: "Exact adapter-projected bytes.".into(),
        },
    )
    .unwrap();
    assert!(plan.finish().unwrap().all_sources_matched);
    assert_eq!(plan.sources()[0].source_key, "spec");
    assert_eq!(plan.sources()[0].expected_version, "v1");
}

#[test]
fn evidence_project_rules_require_active_resources_and_exact_workspace_declaration() {
    let fixture = Fixture::new();
    let (commit, snapshot) = fixture.service().published().unwrap();
    let plan = Plan::prepare(&snapshot, &fixture.scope, &commit, 0).unwrap();
    let context = plan.context();
    let mut manifest = ProjectManifest::new(
        context.project_id.clone(),
        "Fixture",
        None,
        std::time::SystemTime::now().into(),
    );
    manifest.workspace_bindings = vec![context.workspace_binding.clone()];
    plan.validate_project(&manifest).unwrap();
    manifest.revision += 1;
    plan.validate_project(&manifest).unwrap();
    manifest.resource_revision += 1;
    assert!(matches!(
        plan.validate_project(&manifest),
        Err(Error::RevisionConflict)
    ));
    manifest.resource_revision -= 1;
    manifest.workspace_bindings[0].label = Some("changed".into());
    assert!(matches!(
        plan.validate_project(&manifest),
        Err(Error::RevisionConflict)
    ));
    manifest.workspace_bindings[0] = context.workspace_binding.clone();
    manifest.status = ProjectStatus::Archived;
    assert!(matches!(
        plan.validate_project(&manifest),
        Err(Error::ResourceBlocked(_))
    ));
}

#[test]
fn evidence_overflow_marker_keeps_earlier_partial_content_near_the_aggregate_limit() {
    let fixture = Fixture::new();
    let (commit, snapshot) = fixture.service().published().unwrap();
    let three = context(&snapshot, &fixture.scope, |value| {
        value.sources = (0..3)
            .map(|i| (format!("opaque:{i}"), "version".into()))
            .collect();
    });
    let mut plan = Plan::prepare(&three, &fixture.scope, &commit, 0).unwrap();
    let matched = |length| WorkPatrolSourceOutcome::VersionMatched {
        reference: "opaque".to_string(),
        title: "edge".to_string(),
        content: "x".repeat(length),
    };
    plan.record_source(0, matched(7600)).unwrap();
    plan.record_source(1, matched(0)).unwrap();
    let base_bytes = serde_json::to_vec(&plan.finish().unwrap()).unwrap().len();
    // The old budget admitted this body but left too little space to replace
    // the final Unavailable outcome with an aggregate-overflow marker.
    let fill = MAX_WORK_PATROL_EVIDENCE_BYTES - 34 - base_bytes;
    assert!(fill < 8000, "fixture keeps each individual source bounded");
    plan.record_source(1, matched(fill)).unwrap();
    plan.record_source(2, matched(7600)).unwrap();
    let evidence = plan.finish().unwrap();
    assert!(matches!(
        &evidence.sources[0].outcome,
        WorkPatrolSourceOutcome::VersionMatched { content, .. } if content.len() == 7600
    ));
    assert!(evidence.sources.iter().any(|source| matches!(
        source.outcome,
        WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::AggregateBytes,
            ..
        }
    )));
    assert!(!evidence.all_sources_matched);
    assert!(serde_json::to_vec(&evidence).unwrap().len() <= MAX_WORK_PATROL_EVIDENCE_BYTES);
}

type Evidence = UnmeteredWorkPatrolEvidence<String, String>;

fn diagnostic_fixture(selected_source: bool) -> (Evidence, Observation, Model) {
    let fixture = Fixture::new();
    let (commit, snapshot) = fixture.service().published().unwrap();
    let since = snapshot.seq - 1;
    let through = fixture
        .observation()
        .observation(&fixture.scope, since)
        .unwrap();
    let snapshot = if selected_source {
        snapshot
    } else {
        context(&snapshot, &fixture.scope, |value| value.sources.clear())
    };
    let mut plan = Plan::prepare(&snapshot, &fixture.scope, &commit, since).unwrap();
    if selected_source {
        plan.record_source(
            0,
            WorkPatrolSourceOutcome::VersionMatched {
                reference: "opaque native versioned ref".into(),
                title: "Adopted context".into(),
                content: "Exact source bytes for metering.".into(),
            },
        )
        .unwrap();
    }
    let model = Model {
        provider: "fixture-provider".into(),
        model: "fixture-model".into(),
        max_input_tokens: 1000,
        max_output_tokens: 128,
        max_evidence_bytes: MAX_WORK_PATROL_EVIDENCE_BYTES,
        max_evidence_refs: 8,
    };
    (plan.finish().unwrap(), through, model)
}

#[test]
fn diagnostic_input_keeps_the_exact_metered_envelope_and_fixed_refs() {
    for selected_source in [false, true] {
        let (evidence, through, mut model) = diagnostic_fixture(selected_source);
        let bytes = bamboo_tickets::canonical_bytes(&evidence).unwrap();
        let count = if selected_source { 3 } else { 2 };
        model.max_evidence_bytes = bytes.len() + count * 64;
        model.max_evidence_refs = count;
        let mut metered = None;
        let input = prepare_diagnostic_input(
            &evidence,
            &evidence.source_commit,
            evidence.since_cursor,
            &through,
            &model,
            |selected, summary, refs| {
                assert_eq!(selected, &model);
                assert_eq!(summary.as_bytes(), bytes.as_slice());
                assert_eq!(refs.len(), count);
                assert_eq!(
                    refs.iter().map(|r| &r.id).collect::<BTreeSet<_>>().len(),
                    count
                );
                assert!(refs
                    .iter()
                    .all(|r| r.id.len() == 64 && r.revision == through.source_revision));
                metered = Some((summary.to_owned(), refs.to_vec()));
                Ok(model.max_input_tokens)
            },
        )
        .unwrap();
        assert_eq!(input.scope, through.scope);
        assert_eq!(input.source_revision, through.source_revision);
        assert_eq!(input.since_cursor, evidence.since_cursor);
        assert_eq!(input.through_cursor, through.cursor);
        assert_eq!(input.input_tokens, model.max_input_tokens);
        assert_eq!((input.summary, input.references), metered.unwrap());
    }
}

fn unexpected_meter(_: &Model, _: &str, _: &[EvidenceRef]) -> Result<u32, PortError> {
    panic!("invalid evidence must never invoke the meter")
}

#[test]
fn diagnostic_input_rejects_stale_or_foreign_snapshot_before_metering() {
    let (evidence, through, model) = diagnostic_fixture(true);
    let prepare = |commit: &str, since, through: &Observation| {
        prepare_diagnostic_input(&evidence, commit, since, through, &model, unexpected_meter)
    };
    assert_eq!(
        prepare("different-commit", evidence.since_cursor, &through),
        Err(PortError::Conflict)
    );
    assert_eq!(
        prepare(&evidence.source_commit, evidence.since_cursor + 1, &through),
        Err(PortError::Conflict)
    );
    let mut foreign = through.clone();
    foreign.scope.generation += 1;
    assert_eq!(
        prepare(&evidence.source_commit, evidence.since_cursor, &foreign),
        Err(PortError::Conflict)
    );
    let mut newer = through.clone();
    newer.source_revision += 1;
    assert_eq!(
        prepare(&evidence.source_commit, evidence.since_cursor, &newer),
        Err(PortError::Conflict)
    );
    newer = through;
    newer.cursor += 1;
    assert_eq!(
        prepare(&evidence.source_commit, evidence.since_cursor, &newer),
        Err(PortError::Conflict)
    );
}

#[test]
fn diagnostic_input_rejects_partial_inconsistent_or_overbudget_evidence_before_metering() {
    let (evidence, through, model) = diagnostic_fixture(true);
    let prepare = |evidence: &Evidence, model: &Model| {
        prepare_diagnostic_input(
            evidence,
            &evidence.source_commit,
            evidence.since_cursor,
            &through,
            model,
            unexpected_meter,
        )
    };
    for outcome in [
        WorkPatrolSourceOutcome::Missing,
        WorkPatrolSourceOutcome::VersionMismatch {
            observed_version: "v2".into(),
        },
    ] {
        let mut partial = evidence.clone();
        partial.sources[0].outcome = outcome;
        partial.all_sources_matched = false;
        assert_eq!(prepare(&partial, &model), Err(PortError::Denied));
    }
    let mut rejected = evidence.clone();
    rejected.all_sources_matched = false;
    assert_eq!(prepare(&rejected, &model), Err(PortError::Denied));
    rejected = evidence.clone();
    rejected.reference_count -= 1;
    assert_eq!(prepare(&rejected, &model), Err(PortError::Denied));
    rejected = evidence.clone();
    rejected.sources.push(evidence.sources[0].clone());
    rejected.reference_count += 1;
    assert_eq!(prepare(&rejected, &model), Err(PortError::Denied));
    rejected = evidence.clone();
    rejected.ticket.contract.objective = "界".repeat(MAX_WORK_PATROL_EVIDENCE_BYTES / 3 + 1);
    assert_eq!(prepare(&rejected, &model), Err(PortError::Denied));
    for invalid_model in [
        Model {
            max_evidence_bytes: 1,
            ..model.clone()
        },
        Model {
            max_evidence_refs: 2,
            ..model.clone()
        },
        Model {
            max_input_tokens: 0,
            ..model.clone()
        },
    ] {
        assert_eq!(prepare(&evidence, &invalid_model), Err(PortError::Denied));
    }
}

#[test]
fn diagnostic_input_rejects_meter_failure_zero_and_over_cap() {
    let (evidence, through, model) = diagnostic_fixture(true);
    for (metered, expected) in [
        (Err(PortError::Unavailable), PortError::Unavailable),
        (Ok(0), PortError::Denied),
        (Ok(model.max_input_tokens + 1), PortError::Denied),
    ] {
        let result = prepare_diagnostic_input(
            &evidence,
            &evidence.source_commit,
            evidence.since_cursor,
            &through,
            &model,
            |_, _, _| metered,
        );
        assert_eq!(result, Err(expected));
    }
}
