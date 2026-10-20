use super::*;

#[test]
fn authentication_and_access_denial_have_distinct_guidance() {
    let auth = anthropic_auth_error(reqwest::StatusCode::UNAUTHORIZED, "invalid API key".into())
        .to_string();
    assert!(auth.contains("HTTP 401") && auth.contains("Authentication was rejected"));
    let denied =
        anthropic_auth_error(reqwest::StatusCode::FORBIDDEN, "forbidden".into()).to_string();
    assert!(denied.contains("HTTP 403") && denied.contains("Access denied"));
    assert!(!denied.contains("API key"));
}
