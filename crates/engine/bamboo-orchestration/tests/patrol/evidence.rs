//! Pure evidence rules over the existing canonical Ticket fixture. These tests
//! neither resolve native memory nor grant Host/runtime authority.
use super::*;
use bamboo_domain::{ProjectManifest, ProjectStatus};
use bamboo_orchestration::patrol::evidence::{
    EvidencePlan, WorkPatrolEvidenceCoverage, WorkPatrolEvidenceLimit, WorkPatrolSourceOutcome,
    MAX_WORK_PATROL_EVIDENCE_BYTES,
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
