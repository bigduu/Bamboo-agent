//! Stable identity for one Host-authorized creation, never an authority token.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::SessionCallerReference;

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
    let source = serde_json::to_vec(&(&caller.session_id, caller.created_at, operation_id))
        .map_err(|_| "host_session_create_identity_invalid")?;
    let mut hash = Sha256::new();
    hash.update(b"host-root:v1\0");
    hash.update(source);
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
}
