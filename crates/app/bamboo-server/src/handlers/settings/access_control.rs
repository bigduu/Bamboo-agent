use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use actix_web::{
    body::{EitherBody, MessageBody},
    cookie::{time::Duration as CookieDuration, Cookie, SameSite},
    dev::{ServiceRequest, ServiceResponse},
    http::header,
    middleware::Next,
    web, HttpRequest, HttpResponse, ResponseError,
};
use chrono::{SecondsFormat, Utc};
use rand::{Rng, RngCore};
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
            // by requiring the owner root password in its body.
            | "/v2/pair"
            // v2-P2 (#189): the WS upgrade opens unauthenticated, but the ws_v2
            // handler then ENFORCES auth before serving ANY channel — it is
            // pre-authorized when the upgrade itself carries a credential
            // (local bypass / verified password cookie / device-token header),
            // OR it must present a VERIFIED `hello` device token before any
            // subscribe/stop is honored, and an unauthenticated socket is closed
            // on a short deadline. Browsers cannot set headers on a WS upgrade,
            // so this open-upgrade + hello-carrier path is the ONLY way a
            // browser device-token client can authenticate over WS.
            // `/v2/pair/code` + `/v2/devices*` STAY gated (not listed here).
            | "/v2/stream"
    )
}

/// The single source of truth for the access allow-decision, shared by
/// `enforce_access_password_middleware` (every gated route) and the ws_v2
/// handler (`/v2/stream` pre-auth). A request is authorized when no credential
/// is required (no password + no devices, or a local bypass), OR it carries a
/// verified password cookie, OR it carries a valid per-device token header.
///
/// This MUST stay a pure extraction of the middleware's prior allow expression:
/// changing it changes the gate for every route at once.
pub(crate) fn request_is_authorized(req: &HttpRequest, config: &Config) -> bool {
    !build_access_status(config, req).requires_password
        || request_has_verified_access_cookie(req, config)
        || request_has_valid_device_token(req, config)
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
    // Auth is required when a credential mechanism is configured (a root password
    // OR at least one active device) AND the request is not a local bypass. An
    // instance with NO devices + NO password behaves EXACTLY as before — zero
    // regression. When required, accept EITHER a verified password cookie OR a
    // valid per-device token (#181). The allow-decision is centralized in
    // `request_is_authorized` so the ws_v2 handler enforces the SAME rule (#189).
    if request_is_authorized(req.request(), &config) {
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
    /// Owner root password — authorizes first-device pairing (slice 1 path).
    #[serde(default)]
    pub root_password: String,
    /// One-time 6-digit pairing code — authorizes subsequent-device pairing
    /// (slice 2 path). Requested by an already-authenticated device via
    /// `POST /v2/pair/code`.
    #[serde(default)]
    pub code: String,
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

/// `POST /v2/pair` — redeem a credential to pair a NEW device. Two paths:
///
/// - **code** (slice 2): a one-time 6-digit pairing code requested by an already
///   authenticated device via `POST /v2/pair/code`. This is the public route a
///   brand-new device with no credential uses, so it carries a brute-force guard
///   (see [`PairingCodeGuard`]).
/// - **root_password** (slice 1): the owner root password directly authorizes
///   first-device pairing. Unchanged byte-for-byte from slice 1.
///
/// The endpoint is on the public whitelist (a new device has no credential), so
/// it self-gates on one of the two credentials above.
pub async fn pair_device(
    payload: web::Json<PairDeviceRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let label = payload.label.trim();
    if label.is_empty() {
        return Err(AppError::BadRequest("label is required".to_string()));
    }

    let code = payload.code.trim();
    let root_password = payload.root_password.trim();

    // Dispatch: a non-empty code takes the slice-2 code-redemption path; else a
    // non-empty root password takes the unchanged slice-1 path; else 400.
    if !code.is_empty() {
        return pair_device_with_code(&app_state, code, label).await;
    }
    if !root_password.is_empty() {
        return pair_device_with_root_password(&app_state, root_password, label).await;
    }

    Err(AppError::BadRequest(
        "provide either a root_password or a one-time pairing code".to_string(),
    ))
}

/// Slice-1 root-password pairing path. Behavior is identical to slice 1.
async fn pair_device_with_root_password(
    app_state: &AppState,
    root_password: &str,
    label: &str,
) -> Result<HttpResponse, AppError> {
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

    if !verify_password(&config, root_password) {
        return Err(AppError::Unauthorized("invalid root password".to_string()));
    }

    persist_new_device(app_state, label).await
}

/// Slice-2 code-redemption pairing path. The code must EXIST and be UNEXPIRED in
/// the ephemeral store and is consumed ONE-TIME (atomically removed on a
/// successful match) so it cannot be reused. Guarded against brute force.
async fn pair_device_with_code(
    app_state: &AppState,
    code: &str,
    label: &str,
) -> Result<HttpResponse, AppError> {
    // Brute-force gate FIRST: if we are in a cooldown, reject before touching the
    // store so an attacker can't probe code validity during the cooldown.
    if app_state.pairing_code_guard.in_cooldown() {
        return Err(AppError::Unauthorized(
            "too many failed pairing attempts — try again later".to_string(),
        ));
    }

    // One-time consume: `remove` is atomic in DashMap, so two concurrent redeems
    // of the SAME code race on the single removal — exactly one wins the `Some`,
    // the other gets `None` and is treated as an invalid code. After taking the
    // entry we still check expiry (a stale-but-present entry must not pair).
    let consumed = app_state.pairing_codes.remove(code);
    let valid = match consumed {
        Some((_k, entry)) => !entry.is_expired(),
        None => false,
    };

    if !valid {
        // Record the failure; trip the cooldown after the threshold and
        // proactively invalidate outstanding codes so a near-miss attacker can't
        // keep probing the rest of the (small) code space.
        if app_state.pairing_code_guard.record_failure() {
            app_state.pairing_codes.clear();
        }
        return Err(AppError::Unauthorized(
            "invalid or expired pairing code".to_string(),
        ));
    }

    // Success resets the failure counter.
    app_state.pairing_code_guard.record_success();
    persist_new_device(app_state, label).await
}

/// Issue a fresh device credential for `label`, append it to the persisted
/// devices (preserving every existing field + device), and return the plaintext
/// token ONCE.
async fn persist_new_device(app_state: &AppState, label: &str) -> Result<HttpResponse, AppError> {
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

// ── v2-P2 pairing codes + brute-force guard (#181, slice 2) ──────────────────

/// Default code lifetime (~2 minutes).
const PAIRING_CODE_TTL: Duration = Duration::from_secs(120);
/// Failed code-redemption attempts within the window before the cooldown trips.
const PAIRING_FAILURE_THRESHOLD: u32 = 10;
/// How long the code-redemption path stays locked once the threshold is hit.
const PAIRING_COOLDOWN: Duration = Duration::from_secs(60);

/// An in-memory one-time pairing code entry. Holds only an `Instant` expiry —
/// the code itself is the DashMap key. PROCESS-EPHEMERAL: never persisted.
#[derive(Debug, Clone)]
pub struct PairingCodeEntry {
    expires_at: Instant,
}

impl PairingCodeEntry {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            expires_at: Instant::now() + ttl,
        }
    }

    /// Whether this code has passed its TTL. Pure predicate over `Instant` —
    /// directly unit-testable by constructing an already-elapsed expiry.
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// Per-process brute-force guard for the public code-redemption path.
///
/// Design (flagged for review): a 6-digit numeric code is only ~1M wide, and
/// `POST /v2/pair { code }` is public, so without a guard it is brute-forceable
/// within a code's 120s TTL. The guard is a simple bounded failed-attempt
/// counter with a cooldown:
///
/// - Each failed code redemption increments a counter.
/// - After [`PAIRING_FAILURE_THRESHOLD`] (10) failures, a [`PAIRING_COOLDOWN`]
///   (60s) lockout trips: all further code redemptions are rejected for the
///   duration, AND the caller proactively clears outstanding codes (so a
///   near-miss attacker can't resume probing the small space). The counter
///   resets when the cooldown elapses, or on any successful redemption.
///
/// This is per-PROCESS, not per-IP (the public route sits behind no reverse
/// proxy that reliably carries client IPs in this deployment), so it is a global
/// rate cap on the code path. The root-password path is untouched (its own
/// throttling is tracked separately in #190). Trade-off: a global cooldown means
/// a determined attacker can also deny a legitimate device's pairing for 60s by
/// burning failures — acceptable for a short, operator-initiated pairing window.
#[derive(Debug, Default)]
pub struct PairingCodeGuard {
    inner: Mutex<PairingGuardState>,
}

#[derive(Debug, Default)]
struct PairingGuardState {
    failures: u32,
    /// When set and still in the future, the code path is locked.
    cooldown_until: Option<Instant>,
}

impl PairingCodeGuard {
    /// Whether the code-redemption path is currently locked out. Clears an
    /// elapsed cooldown (and its failure count) as a side effect.
    pub fn in_cooldown(&self) -> bool {
        let mut state = self.inner.lock().expect("pairing guard mutex poisoned");
        match state.cooldown_until {
            Some(until) if Instant::now() < until => true,
            Some(_) => {
                // Cooldown elapsed → reset.
                state.cooldown_until = None;
                state.failures = 0;
                false
            }
            None => false,
        }
    }

    /// Record a failed redemption. Returns `true` IFF this failure tripped the
    /// cooldown (so the caller can invalidate outstanding codes).
    pub fn record_failure(&self) -> bool {
        let mut state = self.inner.lock().expect("pairing guard mutex poisoned");
        state.failures = state.failures.saturating_add(1);
        if state.failures >= PAIRING_FAILURE_THRESHOLD {
            state.cooldown_until = Some(Instant::now() + PAIRING_COOLDOWN);
            true
        } else {
            false
        }
    }

    /// Reset the guard after a successful redemption.
    pub fn record_success(&self) {
        let mut state = self.inner.lock().expect("pairing guard mutex poisoned");
        state.failures = 0;
        state.cooldown_until = None;
    }
}

/// Generate a fresh 6-digit numeric code, e.g. "842913". Leading zeros are kept.
///
/// Uses `gen_range` (uniform rejection sampling) rather than `% 1_000_000` to
/// avoid the modulo bias that would make a handful of low codes very slightly
/// more probable. `thread_rng` is a CSPRNG, so codes are unpredictable.
fn generate_pairing_code() -> String {
    let n = rand::thread_rng().gen_range(0..1_000_000);
    format!("{n:06}")
}

/// Drop every expired entry from the ephemeral code store (opportunistic GC).
fn purge_expired_codes(codes: &dashmap::DashMap<String, PairingCodeEntry>) {
    codes.retain(|_code, entry| !entry.is_expired());
}

#[derive(Serialize)]
pub struct PairingCodeResponse {
    pub code: String,
    /// TTL in whole seconds.
    pub ttl: u64,
}

/// `POST /v2/pair/code` — an ALREADY-AUTHENTICATED device/owner requests a
/// one-time pairing code for a new device.
///
/// GATED: this route sits behind `enforce_access_password_middleware` (NOT on
/// the public whitelist), so only a local_bypass desktop, a valid device token,
/// or the verified password cookie can reach it. The generated code is the
/// short-lived credential the brand-new device then redeems at `/v2/pair`.
pub async fn create_pairing_code(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    // Opportunistic GC so the store can't grow unbounded with stale codes.
    purge_expired_codes(&app_state.pairing_codes);

    let code = generate_pairing_code();
    let entry = PairingCodeEntry::new(PAIRING_CODE_TTL);
    // Overwrite on the astronomically-rare collision — the latest request wins.
    app_state.pairing_codes.insert(code.clone(), entry);

    Ok(HttpResponse::Ok().json(PairingCodeResponse {
        code,
        ttl: PAIRING_CODE_TTL.as_secs(),
    }))
}

// ── v2-P2 device management (#181, slice 2) ──────────────────────────────────

/// Summary DTO for `GET /v2/devices`. CRITICAL: this MUST NOT carry
/// `token_hash`/`token_salt` — a credential leak here would let any reader of
/// the device list mint a matching token. Only non-secret metadata is exposed.
#[derive(Serialize)]
pub struct DeviceSummary {
    pub device_id: String,
    pub label: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked: bool,
}

impl DeviceSummary {
    fn from_credential(d: &DeviceCredential) -> Self {
        Self {
            device_id: d.device_id.clone(),
            label: d.label.clone(),
            created_at: d.created_at.clone(),
            last_used_at: d.last_used_at.clone(),
            revoked: d.revoked,
        }
    }
}

/// `GET /v2/devices` — list paired devices (GATED). Returns the summary DTO with
/// NO secret material.
pub async fn list_devices(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let devices: Vec<DeviceSummary> = config
        .access_control
        .as_ref()
        .map(|access| {
            access
                .devices
                .iter()
                .map(DeviceSummary::from_credential)
                .collect()
        })
        .unwrap_or_default();
    Ok(HttpResponse::Ok().json(devices))
}

/// `DELETE /v2/devices/{device_id}` — revoke a device (GATED).
///
/// Sets `revoked = true` (the audit row is KEPT, not removed) and persists.
/// Revocation is instant: `verify_device_token` already rejects revoked devices
/// and `has_active_devices` recomputes, so the revoked token stops working on
/// the very next request. Returns 404 if the device id is unknown.
pub async fn revoke_device(
    path: web::Path<String>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let device_id = path.into_inner();

    // Existence check up front so an unknown id is a clean 404 without a
    // (no-op) persist.
    {
        let config = app_state.config.read().await;
        let exists = config
            .access_control
            .as_ref()
            .map(|access| access.devices.iter().any(|d| d.device_id == device_id))
            .unwrap_or(false);
        if !exists {
            return Err(AppError::NotFound(format!("unknown device {device_id}")));
        }
    }

    let target = device_id.clone();
    app_state
        .update_config(
            move |config| {
                if let Some(access) = config.access_control.as_mut() {
                    if let Some(device) = access.devices.iter_mut().find(|d| d.device_id == target)
                    {
                        device.revoked = true;
                    }
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "device_id": device_id, "revoked": true })))
}

/// `POST /v2/devices/{device_id}/rotate` — issue a NEW token for the SAME device
/// (GATED).
///
/// Keeps `device_id`/`label`/`created_at`, resets `revoked = false`, and
/// replaces `token_hash`/`token_salt` with a fresh pair. The OLD token stops
/// verifying immediately (its salt is gone). Returns the new plaintext token
/// ONCE. Returns 404 if the device id is unknown.
pub async fn rotate_device(
    path: web::Path<String>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let device_id = path.into_inner();

    // Existence check up front so an unknown id is a clean 404 without persisting
    // a no-op config snapshot.
    {
        let config = app_state.config.read().await;
        let exists = config
            .access_control
            .as_ref()
            .map(|access| access.devices.iter().any(|d| d.device_id == device_id))
            .unwrap_or(false);
        if !exists {
            return Err(AppError::NotFound(format!("unknown device {device_id}")));
        }
    }

    // Generate a brand-new credential, then graft its secret material onto the
    // existing device row (reusing `issue_device_token` for the fresh salt+hash).
    let (fresh, token) = issue_device_token("");

    let target = device_id.clone();
    app_state
        .update_config(
            move |config| {
                if let Some(access) = config.access_control.as_mut() {
                    if let Some(device) = access.devices.iter_mut().find(|d| d.device_id == target)
                    {
                        device.token_hash = fresh.token_hash.clone();
                        device.token_salt = fresh.token_salt.clone();
                        device.revoked = false;
                        device.last_used_at = None;
                    }
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    // NOTE: `token` is the plaintext credential — returned ONCE, never logged.
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

    // ── v2-P2 shared allow-decision: request_is_authorized (#189) ──────────
    //
    // This is the SINGLE source of truth the middleware and the ws_v2 handler
    // both call. These tests pin its truth table so the open `/v2/stream`
    // upgrade enforces exactly what the middleware enforces everywhere else.

    #[test]
    fn request_is_authorized_local_is_always_allowed() {
        // A local request bypasses regardless of configured credentials.
        let config = config_with_password();
        assert!(request_is_authorized(&local_req(), &config));
    }

    #[test]
    fn request_is_authorized_remote_with_devices_and_no_creds_is_denied() {
        // Remote + a credential mechanism configured + no presented credential.
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
        assert!(!request_is_authorized(&remote_req(), &config));
    }

    #[test]
    fn request_is_authorized_remote_with_password_and_no_creds_is_denied() {
        let config = config_with_password();
        assert!(!request_is_authorized(&remote_req(), &config));
    }

    #[test]
    fn request_is_authorized_remote_with_valid_cookie_is_allowed() {
        let config = config_with_password();
        let cookie_value =
            access_verification_cookie_value(&config).expect("password config yields a cookie");
        let req = TestRequest::default()
            .insert_header((header::HOST, "bamboo.example.com"))
            .cookie(Cookie::new(ACCESS_VERIFIED_COOKIE_NAME, cookie_value))
            .to_http_request();
        assert!(request_is_authorized(&req, &config));
    }

    #[test]
    fn request_is_authorized_remote_with_valid_device_token_header_is_allowed() {
        let (cred, token) = issue_device_token("d");
        let device_id = cred.device_id.clone();
        let mut config = config_with_password();
        config.access_control.as_mut().unwrap().devices.push(cred);

        let req = TestRequest::default()
            .insert_header((header::HOST, "bamboo.example.com"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
            .insert_header((DEVICE_ID_HEADER, device_id))
            .to_http_request();
        assert!(request_is_authorized(&req, &config));
    }

    #[test]
    fn request_is_authorized_no_password_no_devices_is_open() {
        // Zero-regression baseline: an instance with neither credential mechanism
        // never requires auth, so even a remote request is authorized.
        let config = Config::default();
        assert!(request_is_authorized(&remote_req(), &config));
    }

    #[test]
    fn stream_is_public_but_sibling_routes_are_not() {
        // #189: the upgrade is whitelisted; the gated siblings are NOT.
        assert!(is_public_access_route("/v2/stream"));
        assert!(is_public_access_route("/v2/pair"));
        assert!(!is_public_access_route("/v2/pair/code"));
        assert!(!is_public_access_route("/v2/devices"));
        assert!(!is_public_access_route("/v2/devices/bamboo_x"));
    }

    // ── v2-P2 pairing codes + brute-force guard (#181, slice 2) ────────────

    #[test]
    fn generated_pairing_code_is_six_digits() {
        for _ in 0..1000 {
            let code = generate_pairing_code();
            assert_eq!(code.len(), 6, "code {code:?} must be 6 chars");
            assert!(
                code.chars().all(|c| c.is_ascii_digit()),
                "code {code:?} must be all digits"
            );
        }
    }

    #[test]
    fn pairing_code_expiry_predicate() {
        // A fresh code with a positive TTL is not expired.
        let fresh = PairingCodeEntry::new(Duration::from_secs(120));
        assert!(!fresh.is_expired());

        // A zero-TTL code is immediately expired (expires_at == now).
        let zero = PairingCodeEntry::new(Duration::from_secs(0));
        assert!(zero.is_expired());

        // An entry whose expiry is in the past is expired.
        let past = PairingCodeEntry {
            expires_at: Instant::now() - Duration::from_secs(1),
        };
        assert!(past.is_expired());
    }

    #[test]
    fn purge_expired_codes_drops_only_expired() {
        let codes: dashmap::DashMap<String, PairingCodeEntry> = dashmap::DashMap::new();
        codes.insert(
            "live".into(),
            PairingCodeEntry::new(Duration::from_secs(120)),
        );
        codes.insert(
            "dead".into(),
            PairingCodeEntry {
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );
        purge_expired_codes(&codes);
        assert!(codes.contains_key("live"));
        assert!(!codes.contains_key("dead"));
    }

    #[test]
    fn guard_trips_cooldown_after_threshold() {
        let guard = PairingCodeGuard::default();
        assert!(!guard.in_cooldown());
        // The first THRESHOLD-1 failures do not trip the cooldown.
        for _ in 0..(PAIRING_FAILURE_THRESHOLD - 1) {
            assert!(!guard.record_failure());
            assert!(!guard.in_cooldown());
        }
        // The THRESHOLD-th failure trips it.
        assert!(guard.record_failure());
        assert!(guard.in_cooldown());
    }

    #[test]
    fn guard_success_resets_failures() {
        let guard = PairingCodeGuard::default();
        for _ in 0..(PAIRING_FAILURE_THRESHOLD - 1) {
            guard.record_failure();
        }
        guard.record_success();
        // After a reset, the counter starts over — one more failure does NOT trip.
        assert!(!guard.record_failure());
        assert!(!guard.in_cooldown());
    }

    #[test]
    fn guard_clears_elapsed_cooldown() {
        let guard = PairingCodeGuard::default();
        // Force a cooldown that has already elapsed.
        {
            let mut state = guard.inner.lock().unwrap();
            state.failures = PAIRING_FAILURE_THRESHOLD;
            state.cooldown_until = Some(Instant::now() - Duration::from_secs(1));
        }
        // in_cooldown observes the elapsed deadline and resets.
        assert!(!guard.in_cooldown());
        assert!(!guard.record_failure(), "counter was reset to 0");
    }

    #[test]
    fn device_summary_excludes_secret_material() {
        // Serialized JSON of the GET /v2/devices DTO MUST NOT carry the token
        // hash or salt. Assert on the serialized keys/values directly.
        let (cred, _t) = issue_device_token("iPhone");
        let summary = DeviceSummary::from_credential(&cred);
        let json = serde_json::to_value(&summary).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            !obj.contains_key("token_hash"),
            "must not expose token_hash"
        );
        assert!(
            !obj.contains_key("token_salt"),
            "must not expose token_salt"
        );
        // And the actual secret VALUES must not leak under any key.
        let serialized = serde_json::to_string(&summary).unwrap();
        assert!(!serialized.contains(&cred.token_hash));
        assert!(!serialized.contains(&cred.token_salt));
        // Expected non-secret fields ARE present.
        assert!(obj.contains_key("device_id"));
        assert!(obj.contains_key("label"));
        assert!(obj.contains_key("created_at"));
        assert!(obj.contains_key("revoked"));
    }
}
