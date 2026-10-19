//! Existing owner credentials and Web request protection; no enrollment or proof issuer.
use crate::{
    app_state::AppState,
    handlers::settings::{self, OwnerCredentialStatus},
};
use actix_web::{http::header, HttpRequest};

pub(super) async fn credential_status(
    state: &AppState,
    request: &HttpRequest,
) -> OwnerCredentialStatus {
    if settings::request_is_run_scoped(request) {
        return OwnerCredentialStatus::Required;
    }
    // A native Rust test host may inject its authenticated principal. No HTTP
    // header, JSON claim or production code can construct this extension.
    #[cfg(test)]
    {
        use actix_web::HttpMessage;
        if let Some(principal) = request
            .extensions()
            .get::<super::project_scope_web_tests::TestOwner>()
        {
            use super::project_scope_web_tests::TestOwner;
            return match principal {
                TestOwner::Authenticated => OwnerCredentialStatus::Authenticated,
                TestOwner::Required => OwnerCredentialStatus::Required,
                TestOwner::Unconfigured => OwnerCredentialStatus::Unconfigured,
                TestOwner::Unavailable => OwnerCredentialStatus::Unavailable,
            };
        }
    }
    let exact = match state
        .read_exact_credential_section(bamboo_config::SectionId::AccessControl)
        .await
    {
        Ok(exact) => exact,
        Err(_) => return OwnerCredentialStatus::Unavailable,
    };
    if exact.section.status == bamboo_config::SectionStatus::Missing && exact.section.revision == 0
    {
        return OwnerCredentialStatus::Unconfigured;
    }
    if exact.section.status != bamboo_config::SectionStatus::Healthy {
        return OwnerCredentialStatus::Unavailable;
    }
    settings::owner_credential_status(request, &exact.config)
}

fn http_origin(raw: &str) -> Option<String> {
    let url = url::Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.host().is_none()
    {
        return None;
    }
    Some(url.origin().ascii_serialization())
}

/// JSON PUT + authenticated credentials + exact Origin protect the mutation.
/// SameSite and the general local-development CORS policy alone are insufficient.
pub(super) fn same_origin_json(request: &HttpRequest) -> bool {
    if !request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return false;
    }
    let mut origins = request.headers().get_all(header::ORIGIN);
    let Some(origin) = origins
        .next()
        .and_then(|v| v.to_str().ok())
        .and_then(http_origin)
    else {
        return false;
    };
    if origins.next().is_some() {
        return false;
    }
    let mut hosts = request.headers().get_all(header::HOST);
    let Some(host) = hosts.next().and_then(|v| v.to_str().ok()) else {
        return false;
    };
    if hosts.next().is_some() {
        return false;
    }
    let scheme = request.connection_info().scheme().to_owned();
    let expected = http_origin(&format!("{scheme}://{host}"));
    expected.as_ref() == Some(&origin)
        || crate::config::is_explicitly_allowed_browser_origin(&origin)
}
