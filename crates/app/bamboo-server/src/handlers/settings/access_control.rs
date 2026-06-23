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

use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use bamboo_config::{Config, DeviceCredential};

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

// ── v2-P2 per-device tokens (#181) ──────────────────────────────────────────
//
// A device token reuses the SAME hash construction as the access password
// (`compute_password_hash` = SHA-256(salt || secret)); no new crypto dependency.
// Plaintext tokens are returned to the client ONCE at pairing and are NEVER
// stored or logged — only the hash is persisted.

/// Device-token prefix. `bd1_` + 32 hex chars (16 random bytes).
const DEVICE_TOKEN_PREFIX: &str = "bd1_";
/// Device-id prefix. `bamboo_` + 12 hex chars (6 random bytes).
const DEVICE_ID_PREFIX: &str = "bamboo_";
/// HTTP header carrying the device id companion for a `Authorization: Bearer`
/// device token (the token alone can't locate its per-device salt).
const DEVICE_ID_HEADER: &str = "x-device-id";

/// Constant-time comparison over two byte slices. Returns `false` immediately on
/// a length mismatch (lengths are not secret here — both are fixed-width hex
/// digests), then folds every byte so the loop time does not depend on where the
/// first differing byte is. Used for the device-token hash compare as
/// defense-in-depth for the new credential path (the password path predates this
/// and keeps `==`).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Generate `len` random bytes as a lowercase hex string.
fn random_hex(len: usize) -> String {
    let mut bytes = vec![0_u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Issue a fresh device credential for `label`.
///
/// Returns the [`DeviceCredential`] to persist (hash only) and the plaintext
/// `device_token` to return to the client ONCE. A fresh 16-byte salt is generated
/// per device; `token_hash = SHA-256(salt || token)`.
pub(crate) fn issue_device_token(label: &str) -> (DeviceCredential, String) {
    let device_id = format!("{DEVICE_ID_PREFIX}{}", random_hex(6));
    let token = format!("{DEVICE_TOKEN_PREFIX}{}", random_hex(16));
    let salt_hex = random_hex(16);
    // compute_password_hash only returns None on a non-hex salt; ours is always
    // valid hex, so the hash is infallible here. Fail loudly rather than persist
    // an empty (dead) token_hash if that invariant is ever broken.
    let token_hash =
        compute_password_hash(&token, &salt_hex).expect("device salt is always valid hex");
    let created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let credential = DeviceCredential {
        device_id,
        label: label.to_string(),
        token_hash,
        token_salt: salt_hex,
        created_at,
        last_used_at: None,
        revoked: false,
    };
    (credential, token)
}

/// Verify a presented `(device_id, token)` pair against the stored devices.
///
/// Returns `false` if access control is unset, the device is unknown or revoked,
/// or the hash does not match. The hash comparison is constant-time.
pub(crate) fn verify_device_token(config: &Config, device_id: &str, token: &str) -> bool {
    let Some(access) = config.access_control.as_ref() else {
        return false;
    };
    // device_id is a public, non-secret companion id; a plain `==` lookup here is
    // intentional. Only the token hash compare below must be constant-time.
    let Some(device) = access.devices.iter().find(|d| d.device_id == device_id) else {
        return false;
    };
    if device.revoked {
        return false;
    }
    let Some(computed) = compute_password_hash(token, &device.token_salt) else {
        return false;
    };
    constant_time_eq(computed.as_bytes(), device.token_hash.as_bytes())
}

/// Whether the config has at least one non-revoked device. When true (even with
/// no root password) the middleware must require a credential for non-local
/// requests.
fn has_active_devices(config: &Config) -> bool {
    config
        .access_control
        .as_ref()
        .map(|access| access.devices.iter().any(|d| !d.revoked))
        .unwrap_or(false)
}

/// Extract a presented device token from a request.
///
/// Scheme (documented for clients): the token rides in
/// `Authorization: Bearer bd1_<...>` and its companion device id in
/// `X-Device-Id: bamboo_<...>` (the token alone cannot locate its per-device
/// salt). Returns `(device_id, token)` when both are present and the
/// Authorization value carries a `bd1_`-prefixed bearer token.
fn presented_device_token(req: &HttpRequest) -> Option<(String, String)> {
    let auth = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))?
        .trim();
    if !token.starts_with(DEVICE_TOKEN_PREFIX) {
        return None;
    }
    let device_id = req
        .headers()
        .get(DEVICE_ID_HEADER)?
        .to_str()
        .ok()?
        .trim()
        .to_string();
    if device_id.is_empty() {
        return None;
    }
    Some((device_id, token.to_string()))
}

/// Whether the request carries a valid device-token credential.
fn request_has_valid_device_token(req: &HttpRequest, config: &Config) -> bool {
    match presented_device_token(req) {
        Some((device_id, token)) => verify_device_token(config, &device_id, &token),
        None => false,
    }
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
        "/api/v1/health"
            | "/v1/bamboo/access/status"
            | "/v1/bamboo/access/verify"
            // v2-P2 (#181): a brand-new device has no credential yet, so the
            // pairing endpoint must be reachable unauthenticated. It self-gates
            // by requiring the owner root password in its body. `/v2/stream`
            // stays GATED.
            | "/v2/pair"
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
    // Auth is required when a credential mechanism is configured (a root password
    // OR at least one active device) AND the request is not a local bypass. An
    // instance with NO devices + NO password behaves EXACTLY as before — zero
    // regression. When required, accept EITHER a verified password cookie OR a
    // valid per-device token (#181).
    if !access_status.requires_password
        || request_has_verified_access_cookie(req.request(), &config)
        || request_has_valid_device_token(req.request(), &config)
    {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_left_body);
    }

    let response = AppError::Unauthorized("access credential verification required".to_string())
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
    // v2-P2 (#181): once any device is paired, public access requires a
    // credential even if the root password itself is unset — the device tokens
    // become the gating mechanism. No devices + no password ⇒ unchanged behavior.
    let credential_required = password_enabled || has_active_devices(config);

    AccessStatusResponse {
        password_enabled,
        local_bypass,
        requires_password: credential_required && !local_bypass,
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
                // Mutate in place so an existing `access_control` keeps its paired
                // `devices` across a root-password change. Replacing the whole
                // struct with `devices: vec![]` would silently wipe every device
                // token on every password update (#181).
                let access = config.access_control.get_or_insert_with(Default::default);
                access.password_enabled = true;
                access.password_hash = Some(password_hash.clone());
                access.password_salt = Some(salt_hex.clone());
                access.updated_at = Some(updated_at.clone());
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

// ── v2-P2 pairing (#181) ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PairDeviceRequest {
    /// Owner root password — authorizes first-device pairing.
    #[serde(default)]
    pub root_password: String,
    /// Human-readable device label, e.g. "iPhone 15".
    #[serde(default)]
    pub label: String,
}

#[derive(Serialize)]
pub struct PairDeviceResponse {
    pub device_id: String,
    /// Plaintext token — returned ONCE; the server stores only its hash.
    pub device_token: String,
    pub expires_hint: &'static str,
}

/// `POST /v2/pair` — first-device pairing via the owner root password.
///
/// Slice 1 implements ONLY the root-password path. Pairing CODES
/// (`POST /v2/pair/code` + code-based `/v2/pair`) are deferred to slice 2.
///
/// The endpoint is on the public whitelist (a new device has no credential), so
/// it self-gates: it requires the owner root password. If no root password is
/// configured, pairing is refused with a clear instruction to set one first —
/// the root credential is required to authorize issuing device tokens.
pub async fn pair_device(
    payload: web::Json<PairDeviceRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let label = payload.label.trim();
    if label.is_empty() {
        return Err(AppError::BadRequest("label is required".to_string()));
    }

    let config = app_state.config.read().await.clone();

    let password_enabled = config
        .access_control
        .as_ref()
        .map(|access| access.password_enabled)
        .unwrap_or(false);
    if !password_enabled {
        return Err(AppError::BadRequest(
            "set an access password first: the owner root password is required to authorize device pairing".to_string(),
        ));
    }

    let root_password = payload.root_password.trim();
    if root_password.is_empty() || !verify_password(&config, root_password) {
        return Err(AppError::Unauthorized("invalid root password".to_string()));
    }

    let (credential, token) = issue_device_token(label);
    let device_id = credential.device_id.clone();

    app_state
        .update_config(
            move |config| {
                // Preserve every existing field + already-paired devices: append,
                // never replace.
                let access = config.access_control.get_or_insert_with(Default::default);
                access.devices.push(credential.clone());
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    // NOTE: `token` is the plaintext credential — it is returned to the client
    // here ONCE and is never logged.
    Ok(HttpResponse::Ok().json(PairDeviceResponse {
        device_id,
        device_token: token,
        expires_hint: "rotate-on-demand",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;
    use bamboo_config::AccessControlConfig;

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
                devices: Vec::new(),
            }),
            ..Config::default()
        };

        assert!(verify_password(&config, "secret"));
        assert!(!verify_password(&config, "wrong"));
    }

    // ── v2-P2 device token primitives + gate (#181) ────────────────────────

    fn config_with_password() -> Config {
        let salt_hex = hex::encode([1_u8; 16]);
        let hash = compute_password_hash("secret", &salt_hex).unwrap();
        Config {
            access_control: Some(AccessControlConfig {
                password_enabled: true,
                password_hash: Some(hash),
                password_salt: Some(salt_hex),
                updated_at: None,
                devices: Vec::new(),
            }),
            ..Config::default()
        }
    }

    #[test]
    fn constant_time_eq_matches_and_rejects() {
        assert!(constant_time_eq(b"abcd", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abce"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn issued_token_has_expected_format_and_verifies() {
        let (cred, token) = issue_device_token("iPhone 15");
        assert!(token.starts_with("bd1_"));
        assert_eq!(token.len(), "bd1_".len() + 32);
        assert!(cred.device_id.starts_with("bamboo_"));
        assert_eq!(cred.device_id.len(), "bamboo_".len() + 12);
        assert_eq!(cred.label, "iPhone 15");
        assert!(!cred.revoked);
        // The plaintext token must NOT be stored anywhere on the credential.
        assert_ne!(cred.token_hash, token);

        let mut config = config_with_password();
        config
            .access_control
            .as_mut()
            .unwrap()
            .devices
            .push(cred.clone());

        assert!(verify_device_token(&config, &cred.device_id, &token));
        assert!(!verify_device_token(&config, &cred.device_id, "bd1_wrong"));
        assert!(!verify_device_token(&config, "bamboo_unknown", &token));
    }

    #[test]
    fn revoked_token_is_rejected() {
        let (mut cred, token) = issue_device_token("iPad");
        cred.revoked = true;
        let mut config = config_with_password();
        let device_id = cred.device_id.clone();
        config.access_control.as_mut().unwrap().devices.push(cred);
        assert!(!verify_device_token(&config, &device_id, &token));
    }

    #[test]
    fn has_active_devices_ignores_revoked() {
        let mut config = config_with_password();
        assert!(!has_active_devices(&config));
        let (mut cred, _t) = issue_device_token("d");
        cred.revoked = true;
        config
            .access_control
            .as_mut()
            .unwrap()
            .devices
            .push(cred.clone());
        assert!(!has_active_devices(&config));
        let (cred2, _t2) = issue_device_token("d2");
        config.access_control.as_mut().unwrap().devices.push(cred2);
        assert!(has_active_devices(&config));
    }

    fn remote_req() -> HttpRequest {
        TestRequest::default()
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_http_request()
    }

    fn local_req() -> HttpRequest {
        TestRequest::default()
            .insert_header((header::HOST, "localhost:9562"))
            .to_http_request()
    }

    #[test]
    fn no_devices_no_password_does_not_require_credential() {
        // Zero-regression baseline: an instance with neither password nor devices
        // never requires a credential, even for a remote request.
        let config = Config::default();
        assert!(!build_access_status(&config, &remote_req()).requires_password);
    }

    #[test]
    fn password_only_gate_matches_prior_behavior() {
        let config = config_with_password();
        assert!(build_access_status(&config, &remote_req()).requires_password);
        assert!(!build_access_status(&config, &local_req()).requires_password);
    }

    #[test]
    fn device_presence_requires_credential_even_without_password() {
        // A device paired but no root password still gates remote access.
        let (cred, _t) = issue_device_token("d");
        let config = Config {
            access_control: Some(AccessControlConfig {
                password_enabled: false,
                password_hash: None,
                password_salt: None,
                updated_at: None,
                devices: vec![cred],
            }),
            ..Config::default()
        };
        assert!(build_access_status(&config, &remote_req()).requires_password);
        // Local still bypasses.
        assert!(!build_access_status(&config, &local_req()).requires_password);
    }

    #[test]
    fn valid_device_token_on_request_authenticates() {
        let (cred, token) = issue_device_token("d");
        let device_id = cred.device_id.clone();
        let mut config = config_with_password();
        config.access_control.as_mut().unwrap().devices.push(cred);

        let req = TestRequest::default()
            .insert_header((header::HOST, "bamboo.example.com"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
            .insert_header((DEVICE_ID_HEADER, device_id))
            .to_http_request();
        assert!(request_has_valid_device_token(&req, &config));

        // Wrong token rejected.
        let bad = TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer bd1_deadbeef"))
            .insert_header((DEVICE_ID_HEADER, "bamboo_unknown"))
            .to_http_request();
        assert!(!request_has_valid_device_token(&bad, &config));

        // Missing device-id header → not a credential.
        let no_id = TestRequest::default()
            .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
            .to_http_request();
        assert!(!request_has_valid_device_token(&no_id, &config));
    }
}
