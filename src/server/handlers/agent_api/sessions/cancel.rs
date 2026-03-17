use std::collections::HashMap;

use actix_web::{web, HttpResponse};
use uuid::Uuid;

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::super::types::CancelRequest;

fn resolve_claude_session_id(
    session_id: &str,
    aliases: Option<&HashMap<String, String>>,
) -> Option<String> {
    if Uuid::parse_str(session_id).is_ok() {
        return Some(session_id.to_string());
    }

    aliases.and_then(|mapping| mapping.get(session_id).cloned())
}

/// Cancels a running Claude Code execution.
pub async fn cancel_claude_execution(
    state: web::Data<AppState>,
    req: web::Json<CancelRequest>,
) -> Result<HttpResponse, AppError> {
    let session_id = req.session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(AppError::BadRequest("session_id is required".to_string()));
    }

    {
        let runners = state.claude_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            runner.cancel_token.cancel();
        }
    }

    let claude_session_id = if Uuid::parse_str(&session_id).is_ok() {
        Some(session_id.clone())
    } else {
        let aliases = state.claude_session_aliases.read().await;
        resolve_claude_session_id(&session_id, Some(&aliases))
    };

    let run_id = if let Some(ref claude_session_id) = claude_session_id {
        state
            .process_registry
            .get_claude_session_by_id(claude_session_id)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
            .map(|info| info.run_id)
    } else {
        None
    };

    if let Some(run_id) = run_id {
        let _ = state
            .process_registry
            .kill_process(run_id)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Cancellation request sent",
            "session_id": session_id,
            "claude_session_id": claude_session_id,
            "run_id": run_id
        })))
    } else {
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Session not found or not running",
            "session_id": session_id,
            "claude_session_id": claude_session_id
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::resolve_claude_session_id;

    #[test]
    fn resolve_claude_session_id_keeps_uuid_input() {
        let session_id = Uuid::new_v4().to_string();
        let resolved = resolve_claude_session_id(&session_id, None);
        assert_eq!(resolved, Some(session_id));
    }

    #[test]
    fn resolve_claude_session_id_uses_alias_mapping_for_non_uuid() {
        let mut aliases = HashMap::new();
        aliases.insert("friendly-name".to_string(), Uuid::new_v4().to_string());

        let resolved = resolve_claude_session_id("friendly-name", Some(&aliases));
        assert_eq!(resolved, aliases.get("friendly-name").cloned());
    }

    #[test]
    fn resolve_claude_session_id_returns_none_for_unknown_alias() {
        let aliases = HashMap::new();
        let resolved = resolve_claude_session_id("missing-alias", Some(&aliases));
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_claude_session_id_returns_none_for_non_uuid_without_aliases() {
        let resolved = resolve_claude_session_id("some-name", None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_claude_session_id_handles_empty_string() {
        // Empty string is not a valid UUID
        let resolved = resolve_claude_session_id("", None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_claude_session_id_handles_whitespace_only() {
        // Whitespace-only string is not a valid UUID
        let resolved = resolve_claude_session_id("   ", None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_claude_session_id_prefers_uuid_over_alias() {
        let mut aliases = HashMap::new();
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        aliases.insert(uuid_string.clone(), "different-uuid".to_string());

        // Should return the UUID itself, not look it up in aliases
        let resolved = resolve_claude_session_id(&uuid_string, Some(&aliases));
        assert_eq!(resolved, Some(uuid_string));
    }

    #[test]
    fn resolve_claude_session_id_handles_uuid_with_different_formats() {
        let uuid = Uuid::new_v4();

        // Standard format with hyphens
        let standard = uuid.to_string();
        let resolved = resolve_claude_session_id(&standard, None);
        assert_eq!(resolved, Some(standard));

        // Simple format without hyphens
        let simple = uuid.simple().to_string();
        let resolved_simple = resolve_claude_session_id(&simple, None);
        assert_eq!(resolved_simple, Some(simple));
    }

    #[test]
    fn resolve_claude_session_id_handles_urn_format() {
        let uuid = Uuid::new_v4();
        let urn = format!("urn:uuid:{}", uuid);
        // URN format is actually parsed by Uuid::parse_str
        let resolved = resolve_claude_session_id(&urn, None);
        assert!(resolved.is_some());
        // The resolved value should be the URN string itself
        assert_eq!(resolved, Some(urn));
    }

    #[test]
    fn resolve_claude_session_id_handles_braced_format() {
        let uuid = Uuid::new_v4();
        let braced = format!("{{{}}}", uuid);
        // Braced format is parsed by Uuid::parse_str
        let resolved = resolve_claude_session_id(&braced, None);
        assert_eq!(resolved, Some(braced));
    }

    #[test]
    fn resolve_claude_session_id_handles_alias_with_special_characters() {
        let mut aliases = HashMap::new();
        let target_uuid = Uuid::new_v4().to_string();
        aliases.insert("my-session-123_test".to_string(), target_uuid.clone());

        let resolved = resolve_claude_session_id("my-session-123_test", Some(&aliases));
        assert_eq!(resolved, Some(target_uuid));
    }

    #[test]
    fn resolve_claude_session_id_handles_alias_with_unicode() {
        let mut aliases = HashMap::new();
        let target_uuid = Uuid::new_v4().to_string();
        aliases.insert("会话-🎯".to_string(), target_uuid.clone());

        let resolved = resolve_claude_session_id("会话-🎯", Some(&aliases));
        assert_eq!(resolved, Some(target_uuid));
    }

    #[test]
    fn resolve_claude_session_id_empty_aliases_map() {
        let aliases = HashMap::new();
        let resolved = resolve_claude_session_id("any-name", Some(&aliases));
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_claude_session_id_case_sensitive_alias() {
        let mut aliases = HashMap::new();
        let target_uuid = Uuid::new_v4().to_string();
        aliases.insert("MySession".to_string(), target_uuid.clone());

        // Exact case should match
        let resolved = resolve_claude_session_id("MySession", Some(&aliases));
        assert_eq!(resolved, Some(target_uuid.clone()));

        // Different case should not match
        let resolved_lower = resolve_claude_session_id("mysession", Some(&aliases));
        assert_eq!(resolved_lower, None);
    }

    #[test]
    fn resolve_claude_session_id_multiple_aliases() {
        let mut aliases = HashMap::new();
        let uuid1 = Uuid::new_v4().to_string();
        let uuid2 = Uuid::new_v4().to_string();
        let uuid3 = Uuid::new_v4().to_string();

        aliases.insert("session-1".to_string(), uuid1.clone());
        aliases.insert("session-2".to_string(), uuid2.clone());
        aliases.insert("session-3".to_string(), uuid3.clone());

        assert_eq!(
            resolve_claude_session_id("session-1", Some(&aliases)),
            Some(uuid1)
        );
        assert_eq!(
            resolve_claude_session_id("session-2", Some(&aliases)),
            Some(uuid2)
        );
        assert_eq!(
            resolve_claude_session_id("session-3", Some(&aliases)),
            Some(uuid3)
        );
    }

    #[test]
    fn resolve_claude_session_id_alias_with_empty_value() {
        let mut aliases = HashMap::new();
        aliases.insert("empty-alias".to_string(), String::new());

        let resolved = resolve_claude_session_id("empty-alias", Some(&aliases));
        assert_eq!(resolved, Some(String::new()));
    }

    #[test]
    fn resolve_claude_session_id_nil_uuid() {
        let nil_uuid = "00000000-0000-0000-0000-000000000000";
        let resolved = resolve_claude_session_id(nil_uuid, None);
        assert_eq!(resolved, Some(nil_uuid.to_string()));
    }

    #[test]
    fn resolve_claude_session_id_max_uuid() {
        let max_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        let resolved = resolve_claude_session_id(max_uuid, None);
        assert_eq!(resolved, Some(max_uuid.to_string()));
    }

    #[test]
    fn resolve_claude_session_id_invalid_uuid_string() {
        // Various invalid UUID formats
        assert_eq!(resolve_claude_session_id("not-a-uuid", None), None);
        assert_eq!(resolve_claude_session_id("12345", None), None);
        assert_eq!(
            resolve_claude_session_id("12345678-1234-1234-1234-12345678901", None),
            None
        ); // Too long
        assert_eq!(
            resolve_claude_session_id("12345678-1234-1234-1234-1234567890", None),
            None
        ); // Wrong characters
    }

    #[test]
    fn resolve_claude_session_id_alias_value_can_be_any_string() {
        let mut aliases = HashMap::new();
        // Alias values don't have to be UUIDs
        aliases.insert("alias1".to_string(), "custom-value".to_string());
        aliases.insert("alias2".to_string(), "another-value".to_string());

        assert_eq!(
            resolve_claude_session_id("alias1", Some(&aliases)),
            Some("custom-value".to_string())
        );
        assert_eq!(
            resolve_claude_session_id("alias2", Some(&aliases)),
            Some("another-value".to_string())
        );
    }
}
