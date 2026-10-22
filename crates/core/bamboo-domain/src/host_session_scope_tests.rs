//! Pure finite-scope data fixtures; no registry, filesystem or live grants.

use super::*;
use crate::{
    conversation_reset_snapshot_sha256, prepare_conversation_reset, ConversationGenerationError,
    ConversationResetPreparation, ConversationResetReadSafety, ConversationResetRequest, Message,
    DEFAULT_SUPERVISOR_SESSION_ID,
};

fn root(id: &str) -> Session {
    let mut session = Session::new(id, "synthetic-model");
    session.created_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    session.set_project_id_meta("explicit-project");
    session
}

fn observe(session: &Session) -> HostScopeSessionObservation {
    HostScopeSessionObservation::capture(session, Vec::new()).unwrap()
}

fn workspace() -> String {
    // Construct an absolute platform-local path without creating any files.
    std::env::temp_dir()
        .join("synthetic-host-scope-workspace")
        .to_string_lossy()
        .into_owned()
}

fn create_scope(operation_id: Uuid) -> HostSessionCreateScope {
    HostSessionCreateScope {
        operation_id,
        project_id: "explicit-project".parse().unwrap(),
        workspace_path: workspace(),
        created_target_rights: BTreeSet::new(),
    }
}

fn binding() -> HostSessionScopeBinding {
    let caller = root("scope-caller");
    let target = root("authorized-target");
    HostSessionScopeBinding {
        schema_version: HOST_SESSION_SCOPE_VERSION,
        grant_id: Uuid::from_u128(1),
        revision: 1,
        caller: observe(&caller),
        targets: BTreeMap::from([(
            target.id.clone(),
            HostSessionTargetScope {
                observation: observe(&target),
                rights: BTreeSet::from([HostSessionRight::ListMetadata]),
            },
        )]),
        creates: BTreeMap::from([(Uuid::from_u128(2), create_scope(Uuid::from_u128(2)))]),
    }
}

#[test]
fn stable_scope_identity_supports_root_child_and_supervisor_without_inheritance() {
    for version in [None, Some(1), Some(2)] {
        let mut parent = root("parent-root");
        parent.session_version = version;
        let mut child = Session::new_child_of("child", &parent, "model", "bounded task");
        child.created_at = parent.created_at + chrono::Duration::seconds(1);
        child.session_version = version;
        child.set_project_id_meta("explicit-project");
        let mut supervisor = root(DEFAULT_SUPERVISOR_SESSION_ID);
        supervisor.session_version = version;
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: Uuid::from_u128(3),
        };
        let ancestors = vec![ActorAncestorObservation {
            actor_id: parent.id.clone(),
            session_created_at: parent.created_at,
            metadata_version: parent.metadata_version,
        }];
        for (session, ancestors) in [
            (&parent, Vec::new()),
            (&child, ancestors),
            (&supervisor, Vec::new()),
        ] {
            let observation = HostScopeSessionObservation::capture(session, ancestors).unwrap();
            assert!(observation
                .identity
                .matches_caller(&SessionCallerReference::from(session)));
            observation.check_session(session).unwrap();
            let decoded: HostScopeSessionObservation =
                serde_json::from_value(serde_json::to_value(&observation).unwrap()).unwrap();
            assert_eq!(decoded, observation);
            let mut changed = session.clone();
            changed.add_message(Message::user("same identity, another ordinary message"));
            decoded.check_session(&changed).unwrap();
        }
        let parent_identity = observe(&parent).identity;
        assert!(!parent_identity.matches_caller(&SessionCallerReference::from(&child)));
    }
}

#[test]
fn finite_targets_and_each_right_are_exact_and_creation_implies_no_target_right() {
    let mut scope = binding();
    scope.validate().unwrap();
    scope
        .target("authorized-target", HostSessionRight::ListMetadata)
        .unwrap();
    for right in [
        HostSessionRight::InspectBoundedProgress,
        HostSessionRight::SendPeer,
    ] {
        assert_eq!(
            scope.target("authorized-target", right),
            Err(HostSessionScopeError::Denied)
        );
    }
    assert_eq!(
        scope.target("ungranted-target", HostSessionRight::ListMetadata),
        Err(HostSessionScopeError::Denied)
    );
    let other = root("send-only-target");
    scope.targets.insert(
        other.id.clone(),
        HostSessionTargetScope {
            observation: observe(&other),
            rights: BTreeSet::from([HostSessionRight::SendPeer]),
        },
    );
    scope.validate().unwrap();
    scope.target(&other.id, HostSessionRight::SendPeer).unwrap();
    assert_eq!(
        scope.target("authorized-target", HostSessionRight::SendPeer),
        Err(HostSessionScopeError::Denied)
    );
    assert_eq!(
        scope.target(&other.id, HostSessionRight::ListMetadata),
        Err(HostSessionScopeError::Denied)
    );
    scope
        .targets
        .get_mut("authorized-target")
        .unwrap()
        .rights
        .insert(HostSessionRight::InspectBoundedProgress);
    scope
        .target(
            "authorized-target",
            HostSessionRight::InspectBoundedProgress,
        )
        .unwrap();
    assert!(scope
        .creates
        .values()
        .all(|create| create.created_target_rights.is_empty()));
    assert_eq!(
        scope.target("new-created-root", HostSessionRight::SendPeer),
        Err(HostSessionScopeError::Denied)
    );
    let mut raw = serde_json::to_value(create_scope(Uuid::from_u128(2))).unwrap();
    raw.as_object_mut().unwrap().remove("created_target_rights");
    let decoded: HostSessionCreateScope = serde_json::from_value(raw).unwrap();
    assert!(decoded.created_target_rights.is_empty());
}

#[test]
fn empty_malformed_or_over_capacity_scope_is_rejected() {
    let good = binding();
    let mut invalid = good.clone();
    invalid.schema_version = HOST_SESSION_SCOPE_VERSION + 1;
    assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    let mut invalid = good.clone();
    invalid.grant_id = Uuid::nil();
    assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    invalid = good.clone();
    invalid.revision = 0;
    assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    invalid = good.clone();
    invalid.targets.clear();
    invalid.creates.clear();
    assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));

    let mut bounded = good;
    bounded.targets = (0..MAX_HOST_SESSION_TARGETS)
        .map(|index| {
            let target = root(&format!("target-{index}"));
            (
                target.id.clone(),
                HostSessionTargetScope {
                    observation: observe(&target),
                    rights: BTreeSet::from([HostSessionRight::SendPeer]),
                },
            )
        })
        .collect();
    bounded.creates = (1..=MAX_HOST_SESSION_CREATES)
        .map(|index| {
            let id = Uuid::from_u128(index as u128);
            (id, create_scope(id))
        })
        .collect();
    bounded.validate().unwrap();
    let extra = root("over-capacity-target");
    bounded.targets.insert(
        extra.id.clone(),
        HostSessionTargetScope {
            observation: observe(&extra),
            rights: BTreeSet::from([HostSessionRight::SendPeer]),
        },
    );
    assert_eq!(bounded.validate(), Err(HostSessionScopeError::Invalid));
    bounded.targets.remove(&extra.id);
    let id = Uuid::from_u128(MAX_HOST_SESSION_CREATES as u128 + 1);
    bounded.creates.insert(id, create_scope(id));
    assert_eq!(bounded.validate(), Err(HostSessionScopeError::Invalid));
}

#[test]
fn malformed_ancestry_and_misbound_or_empty_target_data_are_rejected() {
    let parent = root("parent");
    let mut child = Session::new_child_of("child", &parent, "model", "task");
    child.created_at = parent.created_at + chrono::Duration::seconds(1);
    child.set_project_id_meta("explicit-project");
    let observation = HostScopeSessionObservation::capture(
        &child,
        vec![ActorAncestorObservation {
            actor_id: parent.id.clone(),
            session_created_at: parent.created_at,
            metadata_version: parent.metadata_version,
        }],
    )
    .unwrap();
    for change in 0..5 {
        let mut invalid = observation.clone();
        match change {
            0 => invalid.ancestors.clear(),
            1 => invalid.ancestors[0].actor_id = child.id.clone(),
            2 => invalid.ancestors[0].session_created_at += chrono::Duration::days(1),
            3 => invalid.identity.parent_created_at = None,
            _ => invalid.identity.root_session_id = "unobserved-root".into(),
        }
        assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    }
    let mut too_deep = observation.clone();
    too_deep.identity.spawn_depth = MAX_HOST_SESSION_ANCESTORS as u32 + 1;
    too_deep.ancestors = vec![too_deep.ancestors[0].clone(); MAX_HOST_SESSION_ANCESTORS + 1];
    assert_eq!(too_deep.validate(), Err(HostSessionScopeError::Invalid));
    for change in 0..4 {
        let mut invalid = binding();
        let target = invalid.targets.get_mut("authorized-target").unwrap();
        match change {
            0 => target.observation.identity.session_id = "another-id".into(),
            1 => target.rights.clear(),
            2 => target.observation.project_id = None,
            _ => {
                invalid.targets.clear();
                invalid.targets.insert(
                    invalid.caller.identity.session_id.clone(),
                    HostSessionTargetScope {
                        observation: invalid.caller.clone(),
                        rights: BTreeSet::from([HostSessionRight::SendPeer]),
                    },
                );
            }
        }
        assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    }
}

#[test]
fn stable_scope_checks_still_reject_recreated_or_changed_authority_and_project() {
    let source = root("scope-caller");
    let observation = observe(&source);
    for change in 0..6 {
        let mut changed = source.clone();
        match change {
            0 => changed.created_at += chrono::Duration::seconds(1),
            1 => changed.root_tool_authority_revision += 1,
            2 => changed.workspace = Some(workspace()),
            3 => changed.set_project_id_meta("other-project"),
            4 => changed.metadata_version += 1,
            _ => {
                changed.authority_identity = SessionAuthorityIdentity::Supervisor {
                    incarnation_id: Uuid::from_u128(6),
                }
            }
        }
        assert_eq!(
            observation.check_session(&changed),
            Err(HostSessionScopeError::Stale)
        );
    }
}

#[test]
fn creation_requires_exact_operation_project_workspace_and_valid_explicit_data() {
    let scope = binding();
    let id = Uuid::from_u128(2);
    let project: ProjectId = "explicit-project".parse().unwrap();
    scope.create(id, &project, &workspace()).unwrap();
    assert_eq!(
        scope.create(Uuid::from_u128(99), &project, &workspace()),
        Err(HostSessionScopeError::Denied)
    );
    assert_eq!(
        scope.create(id, &"other-project".parse().unwrap(), &workspace()),
        Err(HostSessionScopeError::Denied)
    );
    assert_eq!(
        scope.create(id, &project, "/other-workspace"),
        Err(HostSessionScopeError::Denied)
    );
    for change in 0..5 {
        let mut invalid = scope.clone();
        let create = invalid.creates.get_mut(&id).unwrap();
        match change {
            0 => create.operation_id = Uuid::from_u128(99),
            1 => create.operation_id = Uuid::nil(),
            2 => create.workspace_path = "relative/workspace".into(),
            3 => create.workspace_path.push(' '),
            _ => {
                create.workspace_path = std::env::temp_dir()
                    .join("..")
                    .join("outside")
                    .to_string_lossy()
                    .into_owned()
            }
        }
        assert_eq!(invalid.validate(), Err(HostSessionScopeError::Invalid));
    }
}

#[test]
fn serialized_binding_and_another_issuer_cannot_forge_an_installation_token() {
    let issuer = HostSessionScopeIssuer::default();
    let same_issuer = issuer.clone();
    let original = issuer.issue(binding()).unwrap();
    assert!(same_issuer.recognizes(&original));
    assert!(original.same_installation(&original.clone()));
    let copied: HostSessionScopeBinding =
        serde_json::from_value(serde_json::to_value(original.binding()).unwrap()).unwrap();
    let other_issuer = HostSessionScopeIssuer::default();
    let forged = other_issuer.issue(copied.clone()).unwrap();
    assert!(!issuer.recognizes(&forged));
    assert!(!original.same_installation(&forged));
    let reissued = issuer.issue(copied).unwrap();
    assert!(issuer.recognizes(&reissued));
    assert!(!original.same_installation(&reissued));
    // Issued data is still not installed authority. Only the canonical private
    // registry can install one token and retain its publication fence.
}

fn reset_candidate(source: &Session) -> Session {
    let state = source.conversation_reset.clone().unwrap_or_default();
    let request = ConversationResetRequest {
        session_id: source.id.clone(),
        operation_id: format!("{}:{}", state.conversation_revision, Uuid::new_v4()),
        expected_birth_token: source.root_mode_birth_token(),
        expected_conversation_revision: state.conversation_revision,
        expected_snapshot_sha256: conversation_reset_snapshot_sha256(source).unwrap(),
    };
    let ConversationResetPreparation::Prepared(prepared) = prepare_conversation_reset(
        source,
        &state,
        &request,
        ConversationResetReadSafety::IdleObserved,
        source.updated_at + chrono::Duration::seconds(1),
    )
    .unwrap() else {
        panic!("expected pure reset preparation")
    };
    let mut candidate = prepared.proposed_session;
    candidate.conversation_reset = Some(prepared.proposed_state);
    candidate
}

#[test]
fn valid_conversation_reset_preserves_scope_while_old_operation_generation_is_stale() {
    let mut caller = root("scope-caller");
    caller.add_message(Message::system("retain exactly this system message"));
    caller.add_message(Message::user("old synthetic history"));
    let scope = observe(&caller);
    let first = reset_candidate(&caller);
    let second = reset_candidate(&first);
    for fresh in [&first, &second] {
        scope.check_session(fresh).unwrap();
        assert!(scope
            .identity
            .matches_caller(&SessionCallerReference::from(fresh)));
        let raw =
            serde_json::to_value(HostScopeSessionObservation::capture(fresh, Vec::new()).unwrap())
                .unwrap();
        assert!(raw.get("conversation_reset").is_none());
        assert!(raw["identity"].get("conversation_reset").is_none());
        assert_eq!(
            caller.check_conversation_generation(fresh),
            Err(ConversationGenerationError::StaleGeneration)
        );
    }
    assert_eq!(
        first.check_conversation_generation(&second),
        Err(ConversationGenerationError::StaleGeneration)
    );
    let target_scope = binding().targets.remove("authorized-target").unwrap();
    target_scope
        .observation
        .check_session(&reset_candidate(&root("authorized-target")))
        .unwrap();

    let mut corrupt = first;
    corrupt.conversation_reset.as_mut().unwrap().schema_version += 1;
    assert_eq!(
        scope.check_session(&corrupt),
        Err(HostSessionScopeError::Stale)
    );
    assert_eq!(
        HostScopeSessionObservation::capture(&corrupt, Vec::new()),
        Err(HostSessionScopeError::Invalid)
    );
    // This demonstrates the two data contracts. ToolCtx/replay/final writer
    // generation propagation is a separate mandatory integration, not provided
    // by a persistent scope observation or by this pure fixture.
}
