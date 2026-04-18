use std::net::IpAddr;

use actix_web::{
    body::{EitherBody, MessageBody},
    cookie::{time::Duration as CookieDuration, Cookie, SameSite},
    dev::{ServiceRequest, ServiceResponse},
    http::header,
    middleware::Next,
    web, HttpRequest, HttpResponse, ResponseError,
};
use chrono::{SecondsFormat, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use bamboo_infrastructure_config::{AccessControlConfig, Config};
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

#[derive(Serialize)]
pub struct AccessStatusResponse {
    pub password_enabled: bool,
    pub local_bypass: bool,
    pub requires_password: bool,
}

#[derive(Debug, Deserialize)]
pub struct VerifyPasswordRequest {
    pub password: String,
}

#[derive(Serialize)]
pub struct VerifyPasswordResponse {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePasswordRequest {
    #[serde(default)]
    pub current_password: String,
    #[serde(default)]
    pub new_password: String,
}

#[derive(Serialize)]
pub struct UpdatePasswordResponse {
    pub success: bool,
    pub password_enabled: bool,
}

const ACCESS_VERIFIED_COOKIE_NAME: &str = "bamboo_access_verified";
const ACCESS_VERIFIED_COOKIE_MAX_AGE_SECS: i64 = 60 * 60 * 12;
const ACCESS_VERIFIED_COOKIE_VERSION: &str = "v1";

fn normalize_ip(ip: &str) -> &str {
    let ip = ip.trim();
    ip.strip_prefix("::ffff:").unwrap_or(ip)
}

fn split_host_and_port(value: &str) -> &str {
    let candidate = value.trim();
    if candidate.is_empty() {
        return candidate;
    }

    let without_brackets = candidate
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .unwrap_or(candidate);

    if without_brackets.parse::<IpAddr>().is_ok() {
        return without_brackets;
    }

    without_brackets
        .split(':')
        .next()
        .unwrap_or(without_brackets)
        .trim()
}

fn is_local_host(host: &str) -> bool {
    let normalized = split_host_and_port(host)
        .trim()
        .trim_end_matches('.')
        .to_lowercase();
    if normalized.is_empty() {
        return false;
    }

    if normalized == "localhost" || normalized.ends_with(".local") {
        return true;
    }

    let normalized = normalize_ip(&normalized);
    match normalized.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => {
            v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified()
        }
        Ok(IpAddr::V6(v6)) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_unspecified()
        }
        Err(_) => false,
    }
}

fn request_host_candidates(req: &HttpRequest) -> Vec<String> {
    let mut candidates = Vec::new();

    for header_name in [
        header::HOST,
        header::HeaderName::from_static("x-forwarded-host"),
        header::HeaderName::from_static("x-original-host"),
    ] {
        if let Some(value) = req
            .headers()
            .get(&header_name)
            .and_then(|v| v.to_str().ok())
        {
            for part in value.split(',') {
                let host = part.trim();
                if !host.is_empty() {
                    candidates.push(host.to_string());
                }
            }
        }
    }

    if let Some(uri_host) = req.uri().host() {
        let host = uri_host.trim();
        if !host.is_empty() {
            candidates.push(host.to_string());
        }
    }

    candidates
}

fn is_local_request(req: &HttpRequest) -> bool {
    let host_candidates = request_host_candidates(req);
    if !host_candidates.is_empty() {
        return host_candidates.iter().all(|host| is_local_host(host));
    }

    if let Some(peer) = req.peer_addr() {
        return is_local_host(&peer.ip().to_string());
    }

    let conn = req.connection_info();
    for candidate in [conn.realip_remote_addr(), conn.peer_addr()]
        .into_iter()
        .flatten()
    {
        if is_local_host(candidate) {
            return true;
        }
    }

    false
}

fn compute_password_hash(password: &str, salt_hex: &str) -> Option<String> {
    let salt = hex::decode(salt_hex).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(&salt);
    hasher.update(password.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

fn verify_password(config: &Config, password: &str) -> bool {
    let Some(access) = config.access_control.as_ref() else {
        return false;
    };
    if !access.password_enabled {
        return false;
    }

    let (Some(hash), Some(salt)) = (
        access.password_hash.as_deref(),
        access.password_salt.as_deref(),
    ) else {
        return false;
    };

    compute_password_hash(password, salt)
        .map(|computed| computed == hash)
        .unwrap_or(false)
}

fn access_verification_cookie_value(config: &Config) -> Option<String> {
    let access = config.access_control.as_ref()?;
    if !access.password_enabled {
        return None;
    }

    let hash = access.password_hash.as_deref()?.trim();
    let salt = access.password_salt.as_deref()?.trim();
    if hash.is_empty() || salt.is_empty() {
        return None;
    }

    let mut hasher = Sha256::new();
    hasher.update(ACCESS_VERIFIED_COOKIE_VERSION.as_bytes());
    hasher.update(b":");
    hasher.update(hash.as_bytes());
    hasher.update(b":");
    hasher.update(salt.as_bytes());
    Some(format!(
        "{}:{}",
        ACCESS_VERIFIED_COOKIE_VERSION,
        hex::encode(hasher.finalize())
    ))
}

fn request_has_verified_access_cookie(req: &HttpRequest, config: &Config) -> bool {
    let expected = match access_verification_cookie_value(config) {
        Some(value) => value,
        None => return false,
    };

    req.cookie(ACCESS_VERIFIED_COOKIE_NAME)
        .map(|cookie| cookie.value() == expected)
        .unwrap_or(false)
}

fn build_access_verified_cookie(config: &Config, secure: bool) -> Option<Cookie<'static>> {
    let value = access_verification_cookie_value(config)?;
    Some(
        Cookie::build(ACCESS_VERIFIED_COOKIE_NAME, value)
            .path("/")
            .http_only(true)
            .same_site(SameSite::Lax)
            .secure(secure)
            .max_age(CookieDuration::seconds(ACCESS_VERIFIED_COOKIE_MAX_AGE_SECS))
            .finish(),
    )
}

fn is_public_access_route(path: &str) -> bool {
    matches!(
        path,
        "/api/v1/health" | "/v1/bamboo/access/status" | "/v1/bamboo/access/verify"
    )
}

pub async fn enforce_access_password_middleware<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<EitherBody<B>>, actix_web::Error> {
    let path = req.path().to_string();
    if is_public_access_route(&path) {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_left_body);
    }

    let app_state = match req.app_data::<web::Data<AppState>>() {
        Some(state) => state.clone(),
        None => {
            return next
                .call(req)
                .await
                .map(ServiceResponse::map_into_left_body)
        }
    };

    let config = app_state.config.read().await.clone();
    let access_status = build_access_status(&config, req.request());
    if !access_status.requires_password
        || request_has_verified_access_cookie(req.request(), &config)
    {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_left_body);
    }

    let response = AppError::Unauthorized("access password verification required".to_string())
        .error_response()
        .map_into_right_body();
    Ok(req.into_response(response))
}

fn build_access_status(config: &Config, req: &HttpRequest) -> AccessStatusResponse {
    let password_enabled = config
        .access_control
        .as_ref()
        .map(|access| {
            access.password_enabled
                && access
                    .password_hash
                    .as_deref()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false)
                && access
                    .password_salt
                    .as_deref()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false)
        })
        .unwrap_or(false);
    let local_bypass = is_local_request(req);

    AccessStatusResponse {
        password_enabled,
        local_bypass,
        requires_password: password_enabled && !local_bypass,
    }
}

pub async fn get_access_status(
    req: HttpRequest,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    Ok(HttpResponse::Ok().json(build_access_status(&config, &req)))
}

pub async fn verify_access_password(
    req: HttpRequest,
    payload: web::Json<VerifyPasswordRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let password = payload.password.trim();
    if password.is_empty() {
        return Err(AppError::BadRequest("password is required".to_string()));
    }

    let config = app_state.config.read().await.clone();
    if !verify_password(&config, password) {
        return Err(AppError::Unauthorized("invalid password".to_string()));
    }

    let secure = req.connection_info().scheme().eq_ignore_ascii_case("https");
    let cookie = build_access_verified_cookie(&config, secure)
        .ok_or_else(|| AppError::Unauthorized("access password is not enabled".to_string()))?;

    Ok(HttpResponse::Ok()
        .cookie(cookie)
        .json(VerifyPasswordResponse { success: true }))
}

pub async fn update_access_password(
    req: HttpRequest,
    app_state: web::Data<AppState>,
    payload: web::Json<UpdatePasswordRequest>,
) -> Result<HttpResponse, AppError> {
    let local_bypass = is_local_request(&req);
    let new_password = payload.new_password.trim();

    if new_password.is_empty() {
        return Err(AppError::BadRequest("new_password is required".to_string()));
    }

    let current_config = app_state.config.read().await.clone();
    let password_already_enabled = current_config
        .access_control
        .as_ref()
        .map(|access| access.password_enabled)
        .unwrap_or(false);

    if password_already_enabled && !local_bypass {
        let current_password = payload.current_password.trim();
        if current_password.is_empty() {
            return Err(AppError::Unauthorized(
                "current_password is required".to_string(),
            ));
        }
        if !verify_password(&current_config, current_password) {
            return Err(AppError::Unauthorized(
                "invalid current password".to_string(),
            ));
        }
    }

    let mut salt_bytes = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut salt_bytes);
    let salt_hex = hex::encode(salt_bytes);
    let password_hash = compute_password_hash(new_password, &salt_hex).ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!("failed to compute password hash"))
    })?;
    let updated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    app_state
        .update_config(
            move |config| {
                config.access_control = Some(AccessControlConfig {
                    password_enabled: true,
                    password_hash: Some(password_hash.clone()),
                    password_salt: Some(salt_hex.clone()),
                    updated_at: Some(updated_at.clone()),
                });
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(UpdatePasswordResponse {
        success: true,
        password_enabled: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn loopback_request_is_local() {
        let req = TestRequest::default()
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .insert_header((header::HOST, "localhost:9562"))
            .to_http_request();
        assert!(is_local_request(&req));
    }

    #[test]
    fn private_lan_host_is_local() {
        let req = TestRequest::default()
            .insert_header((header::HOST, "192.168.0.10:9562"))
            .to_http_request();
        assert!(is_local_request(&req));
    }

    #[test]
    fn remote_host_is_not_local_even_when_peer_is_loopback() {
        let req = TestRequest::default()
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_http_request();
        assert!(!is_local_request(&req));
    }

    #[test]
    fn password_hash_roundtrip_verifies() {
        let salt_hex = hex::encode([1_u8; 16]);
        let hash = compute_password_hash("secret", &salt_hex).unwrap();
        let config = Config {
            access_control: Some(AccessControlConfig {
                password_enabled: true,
                password_hash: Some(hash),
                password_salt: Some(salt_hex),
                updated_at: None,
            }),
            ..Config::default()
        };

        assert!(verify_password(&config, "secret"));
        assert!(!verify_password(&config, "wrong"));
    }
}
