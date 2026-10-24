//! Stable identity for one Host-authorized creation, never an authority token.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{SessionAuthorityIdentity, SessionCallerReference, DEFAULT_SUPERVISOR_SESSION_ID};

/// Pure first-input address, never a creation or target grant. Scope changes
/// cannot allocate a second message for the same reserved Root operation.
pub fn host_root_first_input_identity(
    caller: &SessionCallerReference,
    operation_id: Uuid,
    reserved_root: &crate::Session,
) -> Result<(crate::SessionMessageId, Option<String>), &'static str> {
    let (_, reserved_id) = host_session_create_identity(caller, operation_id)?;
    if reserved_root.id != reserved_id {
        return Err("host_session_create_reservation_changed");
    }
    let id = match &caller.authority_identity {
        SessionAuthorityIdentity::Supervisor { incarnation_id } => {
            let reference = crate::SupervisorReference {
                session_id: caller.session_id.clone(),
                incarnation_id: *incarnation_id,
            };
            return Ok((
                crate::supervisor_followup_message_id(
                    &reference,
                    &reserved_id,
                    &operation_id.to_string(),
                ),
                None,
            ));
        }
        SessionAuthorityIdentity::Ordinary => crate::SessionMessageId::stable(
            "host-root-first-input:v1",
            &serde_json::json!({
                "caller_session_id": caller.session_id,
                "caller_created_at": caller.created_at,
                "target_session_id": reserved_id,
                "target_created_at": reserved_root.created_at,
                "operation_id": operation_id,
            }),
        ),
    };
    let thread = Some(id.to_string());
    Ok((id, thread))
}

/// Creation identity survives scope replacement and conversation reset. Only
/// the caller's immutable lifetime and the operation UUID select a reservation.
/// A current exact creation permit is still required at every final write.
pub fn host_session_create_identity(
    caller: &SessionCallerReference,
    operation_id: Uuid,
) -> Result<(String, String), &'static str> {
    if operation_id.is_nil()
        || caller.session_id.is_empty()
        || caller.session_id.trim() != caller.session_id
        || caller.session_id.contains(['/', '\\'])
        || caller.session_id.contains("..")
    {
        return Err("host_session_create_identity_invalid");
    }
    let mut hash = Sha256::new();
    if let SessionAuthorityIdentity::Supervisor { incarnation_id } = &caller.authority_identity {
        if caller.session_id != DEFAULT_SUPERVISOR_SESSION_ID || incarnation_id.is_nil() {
            return Err("host_session_create_identity_invalid");
        }
        // Preserve the existing operation address when an installed Host scope
        // replaces the legacy Supervisor route. A retry must not reserve a
        // second Root just because its current authorization route changed.
        hash.update(format!(
            "supervised-root:v1\0{}\0{}\0{}",
            caller.session_id, incarnation_id, operation_id
        ));
    } else {
        let source = serde_json::to_vec(&(&caller.session_id, caller.created_at, operation_id))
            .map_err(|_| "host_session_create_identity_invalid")?;
        hash.update(b"host-root:v1\0");
        hash.update(source);
    }
    let digest = format!("{:x}", hash.finalize());
    let target = Uuid::parse_str(&digest[..32])
        .map_err(|_| "host_session_create_identity_invalid")?
        .to_string();
    Ok((digest, target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Session;

    #[test]
    fn creation_key_binds_birth_and_operation_without_rotating_execution_fields() {
        let caller = Session::new("caller", "model");
        let original = SessionCallerReference::from(&caller);
        let operation = Uuid::new_v4();
        let identity = host_session_create_identity(&original, operation).unwrap();
        let mut changed = original.clone();
        changed.root_tool_authority_revision += 1;
        changed.workspace = Some("/another-workspace".into());
        assert_eq!(
            host_session_create_identity(&changed, operation).unwrap(),
            identity
        );
        changed.created_at += chrono::Duration::seconds(1);
        assert_ne!(
            host_session_create_identity(&changed, operation).unwrap(),
            identity
        );
        assert_ne!(
            host_session_create_identity(&original, Uuid::new_v4()).unwrap(),
            identity
        );
        assert!(host_session_create_identity(&original, Uuid::nil()).is_err());
        let mut invalid = original;
        invalid.session_id = "../caller".into();
        assert!(host_session_create_identity(&invalid, operation).is_err());
    }

    #[test]
    fn supervisor_scope_switch_keeps_the_existing_operation_address() {
        let mut session = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let incarnation = Uuid::new_v4();
        session.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: incarnation,
        };
        let caller = SessionCallerReference::from(&session);
        let operation = Uuid::new_v4();
        let existing = format!(
            "{:x}",
            Sha256::digest(format!(
                "supervised-root:v1\0{}\0{}\0{}",
                caller.session_id, incarnation, operation
            ))
        );
        let (digest, target) = host_session_create_identity(&caller, operation).unwrap();
        assert_eq!(digest, existing);
        assert_eq!(
            target,
            Uuid::parse_str(&existing[..32]).unwrap().to_string()
        );
        let mut changed = caller.clone();
        changed.root_tool_authority_revision += 1;
        changed.workspace = Some("/current-workspace".into());
        assert_eq!(
            host_session_create_identity(&changed, operation).unwrap().0,
            existing
        );
        changed.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: Uuid::new_v4(),
        };
        assert_ne!(
            host_session_create_identity(&changed, operation).unwrap().0,
            existing
        );
        changed.session_id = "not-the-canonical-supervisor".into();
        assert!(host_session_create_identity(&changed, operation).is_err());
    }

    #[test]
    fn first_input_address_is_birth_bound_and_keeps_supervisor_legacy_wire() {
        let source = Session::new("caller", "model");
        let caller = SessionCallerReference::from(&source);
        let operation = Uuid::new_v4();
        let (_, id) = host_session_create_identity(&caller, operation).unwrap();
        let mut target = Session::new(id, "model");
        let identity = host_root_first_input_identity(&caller, operation, &target).unwrap();
        assert_eq!(identity.1.as_deref(), Some(identity.0.to_string().as_str()));
        let mut changed_caller = caller.clone();
        changed_caller.root_tool_authority_revision += 1;
        assert_eq!(
            host_root_first_input_identity(&changed_caller, operation, &target).unwrap(),
            identity
        );
        target.created_at += chrono::Duration::seconds(1);
        assert_ne!(
            host_root_first_input_identity(&caller, operation, &target).unwrap(),
            identity
        );
        target.id = "unrelated-root".into();
        assert!(host_root_first_input_identity(&caller, operation, &target).is_err());

        let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let incarnation_id = Uuid::new_v4();
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor { incarnation_id };
        let caller = SessionCallerReference::from(&supervisor);
        let (_, id) = host_session_create_identity(&caller, operation).unwrap();
        let target = Session::new(id, "model");
        let (message_id, thread) =
            host_root_first_input_identity(&caller, operation, &target).unwrap();
        assert!(thread.is_none());
        assert_eq!(
            message_id,
            crate::supervisor_followup_message_id(
                &crate::SupervisorReference {
                    session_id: supervisor.id,
                    incarnation_id,
                },
                &target.id,
                &operation.to_string()
            )
        );
    }
}
