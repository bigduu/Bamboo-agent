use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bamboo_domain::poison::PoisonRecover;

use actix_web::{
    body::{EitherBody, MessageBody},
    cookie::{time::Duration as CookieDuration, Cookie, SameSite},
    dev::{ServiceRequest, ServiceResponse},
    http::header,
    middleware::Next,
    web, HttpMessage, HttpRequest, HttpResponse, ResponseError,
};
use chrono::{SecondsFormat, Utc};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{app_state::AppState, error::AppError};
use bamboo_config::{Config, DeviceCredential};

fn credential_revision(app_state: &AppState) -> Result<u64, AppError> {
    bamboo_config::CredentialStore::open(&app_state.app_data_dir)
        .revision()
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "access-control credential revision unavailable: {error}"
            ))
        })
}

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
    // The real TCP peer is the source of truth for the local-bypass decision. A
    // client-controlled `Host` / `X-Forwarded-Host` header MUST NOT upgrade a
    // known-REMOTE peer to "local" — that was an auth bypass (#199): a request
    // from the public internet carrying `Host: localhost` would be treated as
    // local and skip the access password entirely.
    //
    // We deliberately trust ONLY the actual socket peer here, NOT
    // `X-Forwarded-For` / realip (also client-controlled, and there is no
    // trusted-proxy mode configured — bamboo terminates TLS itself per the v2
    // design, so the socket peer IS the client).
    let peer_local: Option<bool> = req
        .peer_addr()
        .map(|peer| is_local_host(&peer.ip().to_string()));

    let host_candidates = request_host_candidates(req);
    if !host_candidates.is_empty() {
        let host_local = host_candidates.iter().all(|host| is_local_host(host));
        // Local only when the Host says local AND the peer is not known-remote.
        //
        // A peer of `None` falls back to the Host signal (so loopback/LAN dev +
        // unit tests without socket info still resolve local). This is NOT a
        // remote-reachable bypass: actix populates `peer_addr()` for every
        // accepted TCP/TLS socket, so a real internet client always yields
        // `Some(_)`. `None` occurs only for unit `TestRequest`s and non-`net`
        // transports (UDS / in-memory) — none of which a remote attacker can
        // drive — so a spoofed `Host: localhost` from `None` cannot originate
        // off-box.
        //
        // DEPLOYMENT CAVEAT: because we trust the socket peer (not `realip` /
        // `X-Forwarded-For`), a reverse proxy on the SAME host (proxy→bamboo over
        // loopback) makes every forwarded request's peer `127.0.0.1`. A normal
        // proxy forwards the client's real `Host` (a public name → `host_local`
        // false → still requires auth), but a proxy that rewrites `Host` to a
        // local value would make all proxied clients local-bypass. The v2 design
        // is "no proxy — bamboo terminates TLS itself", so this is acceptable;
        // a trusted-proxy mode (trust `X-Forwarded-For`) would be a separate opt-in.
        return host_local && peer_local != Some(false);
    }

    // No Host header: decide purely from the real socket peer.
    if let Some(local) = peer_local {
        return local;
    }
    let conn = req.connection_info();
    conn.peer_addr().map(is_local_host).unwrap_or(false)
}

/// Best-effort client-IP key for per-IP throttling (#190).
///
/// Mirrors the precedence `is_local_request` uses to read the client address:
/// `peer_addr` first (the real TCP peer — the hardest to spoof when there is no
/// reverse proxy), then `connection_info().realip_remote_addr()` (an
/// `X-Forwarded-For`-derived address, used when a trusted proxy fronts the app),
/// then the raw `connection_info().peer_addr()`. The address is normalized
/// (stripping an `::ffff:` v4-mapped prefix) so the same client maps to one key
/// regardless of representation. Returns `None` when no address can be
/// determined, in which case the caller falls back to a single shared key so the
/// path is still rate-capped rather than unguarded.
///
/// CAVEAT: behind a proxy that does NOT set a trusted forwarded header, every
/// request shares the proxy's peer IP and would collapse onto one key (global
/// cap). And per-IP keying is inherently defeatable by an attacker who can rotate
/// source IPs — this raises the cost of a brute force, it does not make it
/// impossible. This is the documented trade-off of per-IP throttling.
fn client_ip_key(req: &HttpRequest) -> Option<String> {
    if let Some(peer) = req.peer_addr() {
        return Some(normalize_ip(&peer.ip().to_string()).to_string());
    }

    let conn = req.connection_info();
    for candidate in [conn.realip_remote_addr(), conn.peer_addr()]
        .into_iter()
        .flatten()
    {
        let normalized = normalize_ip(candidate).trim();
        if !normalized.is_empty() {
            return Some(normalized.to_string());
        }
    }

    None
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
    rand::rng().fill(&mut bytes);
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
        token_credential_ref: None,
        token_configured: false,
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
fn presented_bearer_token(req: &HttpRequest) -> Option<&str> {
    let auth = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    Some(
        auth.strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "))?
            .trim(),
    )
}

fn presented_device_token(req: &HttpRequest) -> Option<(String, String)> {
    let token = presented_bearer_token(req)?;
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

/// Public route suffixes reachable under BOTH of bamboo's native-API version
/// prefixes (`/api/v1` canonical, `/v1` legacy alias — see
/// `routes::bamboo_v1_routes` / #251 finding 1). Kept as one list so a route
/// that's public under one prefix can't silently end up gated under its alias
/// — the exact drift `is_public_access_route` used to be exposed to when each
/// prefix's public paths were hand-listed separately (#251 finding 7).
const PUBLIC_VERSIONED_SUFFIXES: &[&str] =
    &["/health", "/bamboo/access/status", "/bamboo/access/verify"];

fn is_public_access_route(path: &str) -> bool {
    for prefix in ["/api/v1", "/v1"] {
        if let Some(suffix) = path.strip_prefix(prefix) {
            if PUBLIC_VERSIONED_SUFFIXES.contains(&suffix) {
                return true;
            }
        }
    }

    matches!(
        path,
        // Unversioned liveness/readiness probes for load balancers / k8s —
        // must be reachable without a credential. #251 (finding 6).
        "/healthz"
            | "/readyz"
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

    // A `bcx1_` bearer opts into the narrower Codex run-token contract. It is
    // validated even on loopback (where ordinary requests retain the existing
    // desktop bypass), is accepted only for the OpenAI Responses surface, and
    // never falls back to cookie/device auth if expired or revoked.
    if let Some(token) = presented_bearer_token(req.request()) {
        if token.starts_with(crate::codex_run_tokens::CODEX_RUN_TOKEN_PREFIX) {
            let scoped_path = matches!(path.as_str(), "/openai/v1/responses" | "/openai/v1/models");
            if scoped_path {
                if let Some(context) = app_state.codex_run_tokens.verify(token) {
                    req.extensions_mut().insert(context);
                    return next
                        .call(req)
                        .await
                        .map(ServiceResponse::map_into_left_body);
                }
            }
            let response = AppError::Unauthorized(
                "invalid, expired, or out-of-scope Codex run credential".to_string(),
            )
            .error_response()
            .map_into_right_body();
            return Ok(req.into_response(response));
        }
    }

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

    // #190: per-IP brute-force throttle. A local/desktop request is exempt
    // (`root_throttle_key` returns None) so the desktop never locks itself out.
    // If the key is in cooldown, reject with 429 + Retry-After BEFORE comparing.
    let throttle_key = root_throttle_key(&req);
    if let Some(key) = throttle_key.as_deref() {
        if let RootGuardDecision::Cooldown { retry_after_secs } =
            app_state.root_password_guard.check(key)
        {
            return Ok(too_many_requests_response(retry_after_secs));
        }
    }

    let config = app_state.config.read().await.clone();
    if !verify_password(&config, password) {
        if let Some(key) = throttle_key.as_deref() {
            app_state.root_password_guard.record_failure(key);
        }
        return Err(AppError::Unauthorized("invalid password".to_string()));
    }

    // Correct password resets this key's counter.
    if let Some(key) = throttle_key.as_deref() {
        app_state.root_password_guard.record_success(key);
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
    rand::rng().fill(&mut salt_bytes);
    let salt_hex = hex::encode(salt_bytes);
    let password_hash = compute_password_hash(new_password, &salt_hex).ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!("failed to compute password hash"))
    })?;
    let updated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let expected_revision = credential_revision(&app_state)?;
    app_state
        .update_access_control_credentials(
            expected_revision,
            true,
            BTreeSet::new(),
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
    req: HttpRequest,
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
        return pair_device_with_root_password(&req, &app_state, root_password, label).await;
    }

    Err(AppError::BadRequest(
        "provide either a root_password or a one-time pairing code".to_string(),
    ))
}

/// Slice-1 root-password pairing path. Behavior is identical to slice 1, plus the
/// #190 per-IP brute-force throttle in front of the password compare.
async fn pair_device_with_root_password(
    req: &HttpRequest,
    app_state: &AppState,
    root_password: &str,
    label: &str,
) -> Result<HttpResponse, AppError> {
    // #190: per-IP brute-force throttle. Loopback/desktop is exempt
    // (`root_throttle_key` returns None). If in cooldown, reject with 429 +
    // Retry-After BEFORE comparing the password.
    let throttle_key = root_throttle_key(req);
    if let Some(key) = throttle_key.as_deref() {
        if let RootGuardDecision::Cooldown { retry_after_secs } =
            app_state.root_password_guard.check(key)
        {
            return Ok(too_many_requests_response(retry_after_secs));
        }
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

    if !verify_password(&config, root_password) {
        if let Some(key) = throttle_key.as_deref() {
            app_state.root_password_guard.record_failure(key);
        }
        return Err(AppError::Unauthorized("invalid root password".to_string()));
    }

    // Correct password resets this key's counter.
    if let Some(key) = throttle_key.as_deref() {
        app_state.root_password_guard.record_success(key);
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

    let expected_revision = credential_revision(app_state)?;
    app_state
        .update_access_control_credentials(
            expected_revision,
            false,
            BTreeSet::from([device_id.clone()]),
            move |config| {
                // Preserve every existing field + already-paired devices: append,
                // never replace.
                let access = config.access_control.get_or_insert_with(Default::default);
                access.devices.push(credential.clone());
                Ok(())
            },
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
        let mut state = self.inner.lock().recover_poison();
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
        let mut state = self.inner.lock().recover_poison();
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
        let mut state = self.inner.lock().recover_poison();
        state.failures = 0;
        state.cooldown_until = None;
    }
}

// ── #190: per-IP root-password brute-force guard ────────────────────────────
//
// Both public root-password-checking endpoints — `POST /v1/bamboo/access/verify`
// (`verify_access_password`) and `POST /v2/pair` on its root-password path
// (`pair_device_with_root_password`) — accept the owner root password with no
// rate limiting. `verify_password` is constant-time (no timing leak), but
// nothing caps the request RATE, so an attacker can brute-force the password.
//
// `RootPasswordGuard` is a per-client-IP failed-attempt counter with a cooldown.
// It mirrors the SHAPE of `PairingCodeGuard` (failure threshold → cooldown,
// self-healing decay, success resets) but is keyed per IP via a `DashMap` so one
// attacker cannot lock out every other client. A loopback/desktop request is
// exempted by the caller (see `is_local_request`) so the desktop can never lock
// itself out.

/// Consecutive failed root-password attempts from one key before the cooldown
/// trips. Lower than the code path's threshold (10) — a root password is the
/// high-value secret and there is no legitimate reason to fail it 5 times.
const ROOT_PASSWORD_FAILURE_THRESHOLD: u32 = 5;
/// How long a key stays locked once the threshold is hit.
const ROOT_PASSWORD_COOLDOWN: Duration = Duration::from_secs(60);
/// Cap on tracked IP keys. Per-IP keying means an attacker rotating source IPs
/// could otherwise grow the map unbounded (slow memory DoS). When a NEW key
/// would exceed this, we first sweep keys not in an active cooldown (abandoned
/// partial-failures + elapsed cooldowns), which are inert anyway — so memory is
/// bounded to roughly the set of IPs actively in a 60s lockout.
const ROOT_PASSWORD_MAX_KEYS: usize = 10_000;

/// Per-key attempt state for the root-password guard.
#[derive(Debug, Default, Clone)]
struct RootAttemptState {
    failures: u32,
    /// When set and still in the future, this key is locked.
    cooldown_until: Option<Instant>,
}

/// Per-client-IP brute-force guard for the root-password endpoints (#190).
///
/// Keyed by a best-effort client-IP string (see `client_ip_key`) so a single
/// attacker only locks out their own key, not every client. Each key:
///
/// - increments a failure counter on a wrong password;
/// - after [`ROOT_PASSWORD_FAILURE_THRESHOLD`] (5) consecutive failures, trips a
///   [`ROOT_PASSWORD_COOLDOWN`] (60s) lockout — further attempts from that key
///   are rejected with HTTP 429 BEFORE the password is even compared;
/// - resets on any successful password check;
/// - self-heals: once the cooldown elapses the key's state is cleared, so a key
///   that simply made a few mistakes recovers automatically.
///
/// Loopback exemption is the CALLER's responsibility (it never calls into the
/// guard for a local request) so the desktop can never lock itself out.
///
/// This is per-PROCESS state and is NOT persisted — a restart clears all
/// counters by design. The code-redemption path keeps its own `PairingCodeGuard`
/// (a separate, global guard); this is strictly the root-password paths.
#[derive(Debug, Default)]
pub struct RootPasswordGuard {
    inner: dashmap::DashMap<String, RootAttemptState>,
}

/// Outcome of consulting the guard for a key.
pub enum RootGuardDecision {
    /// Not locked — proceed to compare the password.
    Allow,
    /// Locked — reject with 429 and this many whole seconds in `Retry-After`.
    Cooldown { retry_after_secs: u64 },
}

impl RootPasswordGuard {
    /// Check whether `key` is currently locked. Clears an elapsed cooldown (and
    /// its failure count) as a side effect so a recovered key returns `Allow`.
    pub fn check(&self, key: &str) -> RootGuardDecision {
        let now = Instant::now();
        if let Some(mut entry) = self.inner.get_mut(key) {
            if let Some(until) = entry.cooldown_until {
                if now < until {
                    let retry_after_secs = (until - now).as_secs().max(1);
                    return RootGuardDecision::Cooldown { retry_after_secs };
                }
                // Cooldown elapsed → reset this key.
                entry.failures = 0;
                entry.cooldown_until = None;
            }
        }
        RootGuardDecision::Allow
    }

    /// Record a failed root-password attempt for `key`. Trips the cooldown once
    /// the threshold is reached.
    pub fn record_failure(&self, key: &str) {
        let now = Instant::now();
        // Bound memory: before adding a NEW key past the cap, drop every key not
        // in an active cooldown (those are inert — an elapsed cooldown or an
        // abandoned sub-threshold failure count contributes nothing to gating).
        if !self.inner.contains_key(key) && self.inner.len() >= ROOT_PASSWORD_MAX_KEYS {
            self.inner
                .retain(|_, st| matches!(st.cooldown_until, Some(until) if now < until));
        }
        let mut entry = self.inner.entry(key.to_string()).or_default();
        // A still-live cooldown shouldn't be reachable here (the caller checks
        // first), but if it is, leave it; otherwise count the failure.
        if matches!(entry.cooldown_until, Some(until) if now < until) {
            return;
        }
        // If a previous cooldown elapsed, this is a fresh window.
        if entry.cooldown_until.is_some() {
            entry.failures = 0;
            entry.cooldown_until = None;
        }
        entry.failures = entry.failures.saturating_add(1);
        if entry.failures >= ROOT_PASSWORD_FAILURE_THRESHOLD {
            entry.cooldown_until = Some(now + ROOT_PASSWORD_COOLDOWN);
        }
    }

    /// Reset `key` after a successful root-password check.
    pub fn record_success(&self, key: &str) {
        self.inner.remove(key);
    }
}

/// Resolve the throttle key for a request, honoring the loopback exemption.
///
/// Returns `None` for a local/loopback request (desktop is NEVER throttled), or
/// `Some(key)` for a remote request — the per-IP key when an address is
/// available, else a single shared `"unknown"` key so the path still has a
/// global rate cap rather than being silently unguarded.
fn root_throttle_key(req: &HttpRequest) -> Option<String> {
    if is_local_request(req) {
        return None;
    }
    Some(client_ip_key(req).unwrap_or_else(|| "unknown".to_string()))
}

/// Build the 429 response for a tripped root-password cooldown, with a
/// `Retry-After` header (whole seconds). The body carries no secret material.
fn too_many_requests_response(retry_after_secs: u64) -> HttpResponse {
    HttpResponse::TooManyRequests()
        .insert_header((header::RETRY_AFTER, retry_after_secs.to_string()))
        .json(serde_json::json!({
            "error": crate::error::error_value(
                "too many failed password attempts — try again later"
            )
        }))
}

/// Generate a fresh 6-digit numeric code, e.g. "842913". Leading zeros are kept.
///
/// Uses `gen_range` (uniform rejection sampling) rather than `% 1_000_000` to
/// avoid the modulo bias that would make a handful of low codes very slightly
/// more probable. `thread_rng` is a CSPRNG, so codes are unpredictable.
fn generate_pairing_code() -> String {
    let n = rand::rng().random_range(0..1_000_000);
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
    let expected_revision = credential_revision(&app_state)?;
    app_state
        .update_access_control_credentials(
            expected_revision,
            false,
            BTreeSet::new(),
            move |config| {
                if let Some(access) = config.access_control.as_mut() {
                    if let Some(device) = access.devices.iter_mut().find(|d| d.device_id == target)
                    {
                        device.revoked = true;
                    }
                }
                Ok(())
            },
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
    let expected_revision = credential_revision(&app_state)?;
    app_state
        .update_access_control_credentials(
            expected_revision,
            false,
            BTreeSet::from([device_id.clone()]),
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
    use actix_web::{
        body::to_bytes,
        http::StatusCode,
        middleware::from_fn,
        test::{self, TestRequest},
        App,
    };
    use bamboo_config::AccessControlConfig;
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;

    macro_rules! test_config {
        (@assign $config:ident, providers, $value:expr) => { *$config.providers_mut() = $value; };
        (@assign $config:ident, memory, $value:expr) => { *$config.memory_mut() = $value; };
        (@assign $config:ident, subagents, $value:expr) => { *$config.subagents_mut() = $value; };
        (@assign $config:ident, $field:ident, $value:expr) => { $config.$field = $value; };
        ($($field:ident: $value:expr),* $(,)?) => {{
            let mut config = Config::default();
            $(test_config!(@assign config, $field, $value);)*
            config
        }};
    }

    #[actix_web::test]
    async fn codex_run_token_is_path_and_session_scoped_and_revocation_beats_loopback_bypass() {
        async fn probe(req: HttpRequest) -> HttpResponse {
            let session_id = req
                .extensions()
                .get::<crate::codex_run_tokens::CodexRunAuthContext>()
                .map(|context| context.session_id.clone())
                .unwrap_or_else(|| "missing-context".to_string());
            HttpResponse::Ok().body(session_id)
        }

        let data_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(data_dir.path().to_path_buf()).await.unwrap();
        let tokens = state.codex_run_tokens.clone();
        let issued = tokens.issue("codex-child-570").unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .wrap(from_fn(enforce_access_password_middleware))
                .route("/openai/v1/responses", web::post().to(probe))
                .route("/openai/v1/chat/completions", web::post().to(probe)),
        )
        .await;

        let valid = TestRequest::post()
            .uri("/openai/v1/responses")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header((header::HOST, "localhost:9562"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", issued.token)))
            .to_request();
        let valid = test::call_service(&app, valid).await;
        assert_eq!(valid.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(valid.into_body()).await.unwrap(),
            "codex-child-570".as_bytes()
        );

        let out_of_scope = TestRequest::post()
            .uri("/openai/v1/chat/completions")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header((header::HOST, "localhost:9562"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", issued.token)))
            .to_request();
        assert_eq!(
            test::call_service(&app, out_of_scope).await.status(),
            StatusCode::UNAUTHORIZED
        );

        tokens.revoke(&issued.token_id);
        let revoked_on_loopback = TestRequest::post()
            .uri("/openai/v1/responses")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header((header::HOST, "localhost:9562"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", issued.token)))
            .to_request();
        assert_eq!(
            test::call_service(&app, revoked_on_loopback).await.status(),
            StatusCode::UNAUTHORIZED,
            "revoked bcx1_ credentials must never fall through to loopback bypass"
        );
    }

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
    fn spoofed_local_host_from_remote_peer_is_not_local() {
        // #199: a request from a PUBLIC peer carrying `Host: localhost` (or any
        // local-looking Host / X-Forwarded-Host) must NOT be treated as local —
        // otherwise a remote attacker bypasses the access password entirely.
        for spoof in ["localhost:9562", "127.0.0.1", "192.168.0.1"] {
            let req = TestRequest::default()
                .peer_addr("203.0.113.5:40000".parse().unwrap()) // public peer
                .insert_header((header::HOST, spoof))
                .to_http_request();
            assert!(
                !is_local_request(&req),
                "remote peer + spoofed Host '{spoof}' must not be local"
            );
            // Same via X-Forwarded-Host.
            let req2 = TestRequest::default()
                .peer_addr("203.0.113.5:40000".parse().unwrap())
                .insert_header(("x-forwarded-host", spoof))
                .to_http_request();
            assert!(
                !is_local_request(&req2),
                "remote peer + spoofed X-Forwarded-Host '{spoof}' must not be local"
            );
        }
    }

    #[test]
    fn loopback_peer_with_no_host_is_local() {
        let req = TestRequest::default()
            .peer_addr("127.0.0.1:5000".parse().unwrap())
            .to_http_request();
        assert!(is_local_request(&req));
    }

    #[test]
    fn password_hash_roundtrip_verifies() {
        let salt_hex = hex::encode([1_u8; 16]);
        let hash = compute_password_hash("secret", &salt_hex).unwrap();
        let config = test_config! {
            access_control: Some(AccessControlConfig {
                password_enabled: true,
                password_hash: Some(hash),
                password_salt: Some(salt_hex),
                password_credential_ref: None,
                password_configured: false,
                updated_at: None,
                devices: Vec::new(),
            }),
        };

        assert!(verify_password(&config, "secret"));
        assert!(!verify_password(&config, "wrong"));
    }

    // ── v2-P2 device token primitives + gate (#181) ────────────────────────

    fn config_with_password() -> Config {
        let salt_hex = hex::encode([1_u8; 16]);
        let hash = compute_password_hash("secret", &salt_hex).unwrap();
        test_config! {
            access_control: Some(AccessControlConfig {
                password_enabled: true,
                password_hash: Some(hash),
                password_salt: Some(salt_hex),
                password_credential_ref: None,
                password_configured: false,
                updated_at: None,
                devices: Vec::new(),
            }),
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
        let config = test_config! {
            access_control: Some(AccessControlConfig {
                password_enabled: false,
                password_hash: None,
                password_salt: None,
                password_credential_ref: None,
                password_configured: false,
                updated_at: None,
                devices: vec![cred],
            }),
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
        let config = test_config! {
            access_control: Some(AccessControlConfig {
                password_enabled: false,
                password_hash: None,
                password_salt: None,
                password_credential_ref: None,
                password_configured: false,
                updated_at: None,
                devices: vec![cred],
            }),
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

    #[test]
    fn health_probes_are_public() {
        // #251 (finding 6): unversioned liveness/readiness probes must be
        // reachable by a load balancer without a credential.
        assert!(is_public_access_route("/healthz"));
        assert!(is_public_access_route("/readyz"));
        assert!(is_public_access_route("/api/v1/health"));
    }

    #[test]
    fn public_access_status_routes_are_public_under_both_version_prefixes() {
        // #251 (finding 1 + 7): `/v1/bamboo/access/*` is aliased at the new
        // canonical `/api/v1/bamboo/access/*` prefix — a route that's public
        // under one prefix must stay public under its alias, or a legitimate
        // pre-auth client (checking whether a password is required at all)
        // would get locked out simply for calling the canonical path.
        for prefix in ["/v1", "/api/v1"] {
            assert!(
                is_public_access_route(&format!("{prefix}/bamboo/access/status")),
                "{prefix}/bamboo/access/status must be public"
            );
            assert!(
                is_public_access_route(&format!("{prefix}/bamboo/access/verify")),
                "{prefix}/bamboo/access/verify must be public"
            );
            // The sibling password-UPDATE route must stay gated under both
            // prefixes — only status/verify are pre-auth.
            assert!(
                !is_public_access_route(&format!("{prefix}/bamboo/access/password")),
                "{prefix}/bamboo/access/password must stay gated"
            );
        }
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

    // ── #190: per-IP root-password brute-force guard ───────────────────────

    #[test]
    fn root_guard_trips_cooldown_after_threshold_per_key() {
        let guard = RootPasswordGuard::default();
        let key = "203.0.113.7";
        // The first THRESHOLD-1 failures do not trip the cooldown.
        for _ in 0..(ROOT_PASSWORD_FAILURE_THRESHOLD - 1) {
            guard.record_failure(key);
            assert!(matches!(guard.check(key), RootGuardDecision::Allow));
        }
        // The THRESHOLD-th failure trips it.
        guard.record_failure(key);
        match guard.check(key) {
            RootGuardDecision::Cooldown { retry_after_secs } => {
                assert!(retry_after_secs >= 1);
                assert!(retry_after_secs <= ROOT_PASSWORD_COOLDOWN.as_secs());
            }
            RootGuardDecision::Allow => panic!("key must be in cooldown after threshold"),
        }
    }

    #[test]
    fn root_guard_keys_are_independent() {
        // Per-IP isolation: tripping one key must NOT lock out a different key.
        let guard = RootPasswordGuard::default();
        for _ in 0..ROOT_PASSWORD_FAILURE_THRESHOLD {
            guard.record_failure("198.51.100.1");
        }
        assert!(matches!(
            guard.check("198.51.100.1"),
            RootGuardDecision::Cooldown { .. }
        ));
        // A different IP is untouched.
        assert!(matches!(
            guard.check("198.51.100.2"),
            RootGuardDecision::Allow
        ));
    }

    #[test]
    fn root_guard_success_resets_key() {
        let guard = RootPasswordGuard::default();
        let key = "203.0.113.9";
        for _ in 0..(ROOT_PASSWORD_FAILURE_THRESHOLD - 1) {
            guard.record_failure(key);
        }
        guard.record_success(key);
        // After a reset, the counter starts over — one more failure does NOT trip.
        guard.record_failure(key);
        assert!(matches!(guard.check(key), RootGuardDecision::Allow));
    }

    #[test]
    fn root_guard_clears_elapsed_cooldown() {
        let guard = RootPasswordGuard::default();
        let key = "203.0.113.10";
        // Force a cooldown that has already elapsed.
        guard.inner.insert(
            key.to_string(),
            RootAttemptState {
                failures: ROOT_PASSWORD_FAILURE_THRESHOLD,
                cooldown_until: Some(Instant::now() - Duration::from_secs(1)),
            },
        );
        // check() observes the elapsed deadline, resets, and allows.
        assert!(matches!(guard.check(key), RootGuardDecision::Allow));
        // The counter was reset to 0 — one fresh failure does not re-trip.
        guard.record_failure(key);
        assert!(matches!(guard.check(key), RootGuardDecision::Allow));
    }

    #[test]
    fn root_guard_evicts_inert_keys_past_the_cap() {
        let guard = RootPasswordGuard::default();
        // Fill to the cap with single-failure (inert, no cooldown) keys, plus a
        // few extra to trip the sweep. The map must NOT grow unbounded.
        for i in 0..(ROOT_PASSWORD_MAX_KEYS + 50) {
            guard.record_failure(&format!("10.0.{}.{}", i / 256, i % 256));
        }
        assert!(
            guard.inner.len() <= ROOT_PASSWORD_MAX_KEYS,
            "inert keys must be swept so the map stays bounded (was {})",
            guard.inner.len()
        );
        // An actively cooling-down key survives a sweep.
        let hot = "203.0.113.200";
        for _ in 0..ROOT_PASSWORD_FAILURE_THRESHOLD {
            guard.record_failure(hot);
        }
        for i in 0..(ROOT_PASSWORD_MAX_KEYS + 50) {
            guard.record_failure(&format!("172.16.{}.{}", i / 256, i % 256));
        }
        assert!(
            matches!(guard.check(hot), RootGuardDecision::Cooldown { .. }),
            "a key in active cooldown must survive eviction sweeps"
        );
    }

    #[test]
    fn root_throttle_key_exempts_loopback_and_keys_remote() {
        // Loopback/desktop is exempt → no key → never throttled.
        assert!(root_throttle_key(&local_req()).is_none());

        // A remote request with a peer addr yields that IP as the key.
        let remote = TestRequest::default()
            .peer_addr("203.0.113.5:443".parse().unwrap())
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_http_request();
        assert_eq!(root_throttle_key(&remote).as_deref(), Some("203.0.113.5"));
    }

    #[test]
    fn client_ip_key_strips_v4_mapped_prefix() {
        let req = TestRequest::default()
            .peer_addr("[::ffff:203.0.113.5]:443".parse().unwrap())
            .to_http_request();
        assert_eq!(client_ip_key(&req).as_deref(), Some("203.0.113.5"));
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
