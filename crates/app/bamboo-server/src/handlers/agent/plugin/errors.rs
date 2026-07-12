//! HTTP status mapping for `bamboo_plugin::PluginError`.
//!
//! `PluginError` (defined in the `bamboo-plugin` infra crate) and
//! `actix_web::ResponseError` are both foreign to this crate — an
//! `impl ResponseError for PluginError` here would violate Rust's orphan
//! rule (neither the trait nor the type is local), short of adding an
//! `actix-web` dependency to the `infra`-layer `bamboo-plugin` crate, which
//! would be a much heavier coupling than one mapping fn. So this is a plain
//! function, called at every plugin handler's error path — the same shape
//! `handlers/agent/mcp` and `handlers/agent/prompt_presets` already use for
//! their ad hoc `HttpResponse::X().json(json!({"error": ...}))` responses
//! (a flat `{ "error": "<message>" }` body, NOT the nested
//! `AppError`/`JsonErrorWrapper` shape `crate::error` uses elsewhere).
//!
//! # Status map (frozen — shared with the parallel CLI agent's expectations)
//!
//! | `PluginError` variant         | HTTP status |
//! |-------------------------------|-------------|
//! | `Conflict`                    | 409 |
//! | `AlreadyInstalled`            | 409 |
//! | `UnsupportedPlatform`         | 422 |
//! | `NotFound`                    | 404 |
//! | `InvalidManifest`             | 400 |
//! | `ArtifactVerificationFailed`  | 400 |
//! | `BundleVerificationFailed`    | 400 |
//! | `ChecksumRequired`            | 400 |
//! | `UntrustedHost`               | 403 |
//! | `UnsignedOrUntrustedSignature`| 403 |
//! | `RedirectRefused`             | 403 |
//! | `Registration` / `Io` / `Json` / `NotImplemented` | 500 |
//!
//! `UntrustedHost`/`UnsignedOrUntrustedSignature`/`RedirectRefused` are
//! mapped to 403 rather than 400 (unlike the checksum/manifest variants
//! above): all three are source-TRUST/authorization refusals — "you may not
//! install from here / from this publisher / by following a redirect off the
//! approved host without an explicit opt-out" — not a malformed request, and
//! not a server bug (a redirect refusal must never look like a 500), which
//! 403 fits better than either 400 or 500.
//!
//! `Io`/`Json` are bucketed with `Registration` under 500 rather than 400
//! even though they can originate from a caller-supplied plugin bundle
//! (a malformed archive, a truncated download): the same two variants are
//! also raised for bamboo's OWN store files (`installed.json`,
//! `prompt-presets.json`) via `#[from]`, so — mirroring how the contract
//! already buckets `Io` as an "unexpected" failure rather than a client
//! error — there is no way to tell the two apart from the variant alone, and
//! defaulting to 500 never hides a genuine validation problem (which always
//! surfaces as `InvalidManifest`/`ArtifactVerificationFailed` instead).
//! `NotImplemented` should be unreachable through this HTTP surface (the
//! installer-core agent's `ServerPluginInstaller` implements every step) but
//! is mapped defensively rather than left to panic.
//!
//! # 500 bodies are sanitized — never `error.to_string()` verbatim
//!
//! `PluginError::Io`'s `Display` (via `thiserror`'s `#[from] std::io::Error`)
//! can embed a raw local filesystem path (e.g. a permission error naming
//! `/home/alice/.bamboo/plugins/...`), and `Json`'s can embed byte offsets
//! into a caller-supplied file. Neither is actionable for an HTTP caller and
//! both are a minor local-path/detail disclosure into a response body, so —
//! unlike the actionable variants below, whose messages ARE returned as-is —
//! every variant mapped to 500 gets a fixed, generic body here. The real
//! detail is still fully preserved, just server-side: logged via `tracing`
//! before the generic response is built.

use actix_web::HttpResponse;
use bamboo_plugin::PluginError;

/// Fixed body for every variant that maps to a 500 (see the module docs on
/// why these are never `error.to_string()` verbatim).
const GENERIC_INTERNAL_ERROR_MESSAGE: &str = "internal error during plugin operation";

pub fn plugin_error_response(error: &PluginError) -> HttpResponse {
    // Actionable, safe variants: their `Display` text never contains more
    // than what the caller already supplied (a plugin id/manifest field), so
    // it's returned to the client as-is.
    let actionable_status = match error {
        PluginError::Conflict { .. } => Some(actix_web::http::StatusCode::CONFLICT),
        PluginError::AlreadyInstalled(_) => Some(actix_web::http::StatusCode::CONFLICT),
        PluginError::UnsupportedPlatform { .. } => {
            Some(actix_web::http::StatusCode::UNPROCESSABLE_ENTITY)
        }
        PluginError::NotFound(_) => Some(actix_web::http::StatusCode::NOT_FOUND),
        PluginError::InvalidManifest(_) => Some(actix_web::http::StatusCode::BAD_REQUEST),
        PluginError::ArtifactVerificationFailed(_) => {
            Some(actix_web::http::StatusCode::BAD_REQUEST)
        }
        PluginError::BundleVerificationFailed(_) => Some(actix_web::http::StatusCode::BAD_REQUEST),
        PluginError::ChecksumRequired(_) => Some(actix_web::http::StatusCode::BAD_REQUEST),
        PluginError::UntrustedHost(_) => Some(actix_web::http::StatusCode::FORBIDDEN),
        PluginError::UnsignedOrUntrustedSignature(_) => {
            Some(actix_web::http::StatusCode::FORBIDDEN)
        }
        PluginError::RedirectRefused(_) => Some(actix_web::http::StatusCode::FORBIDDEN),
        PluginError::Registration(_)
        | PluginError::NotImplemented(_)
        | PluginError::Io(_)
        | PluginError::Json(_) => None,
    };

    match actionable_status {
        Some(status) => {
            let body = serde_json::json!({ "error": error.to_string() });
            HttpResponse::build(status).json(body)
        }
        None => {
            // Unexpected/internal failures — the detail (which, for Io/Json,
            // can contain a local filesystem path) is logged server-side
            // only; the HTTP body is a fixed, generic message.
            tracing::error!(%error, "plugin operation failed with an internal error");
            let body = serde_json::json!({ "error": GENERIC_INTERNAL_ERROR_MESSAGE });
            HttpResponse::InternalServerError().json(body)
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use bamboo_plugin::PluginError;

    use super::plugin_error_response;

    #[test]
    fn maps_every_variant_to_the_documented_status() {
        let cases: Vec<(PluginError, StatusCode)> = vec![
            (
                PluginError::Conflict {
                    kind: "mcp server",
                    name: "shared-tool".to_string(),
                    plugin_id: "demo".to_string(),
                },
                StatusCode::CONFLICT,
            ),
            (
                PluginError::AlreadyInstalled("demo".to_string()),
                StatusCode::CONFLICT,
            ),
            (
                PluginError::UnsupportedPlatform {
                    plugin_id: "demo".to_string(),
                    platform: "linux".to_string(),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                PluginError::NotFound("demo".to_string()),
                StatusCode::NOT_FOUND,
            ),
            (
                PluginError::InvalidManifest("bad id".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (
                PluginError::ArtifactVerificationFailed("sha256 mismatch".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (
                PluginError::BundleVerificationFailed("sha256 mismatch".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (
                PluginError::ChecksumRequired("refusing to install without a checksum".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (
                PluginError::UntrustedHost("host not in trusted_hosts".to_string()),
                StatusCode::FORBIDDEN,
            ),
            (
                PluginError::UnsignedOrUntrustedSignature("bundle is unsigned".to_string()),
                StatusCode::FORBIDDEN,
            ),
            (
                PluginError::RedirectRefused("refused to follow a redirect".to_string()),
                StatusCode::FORBIDDEN,
            ),
            (
                PluginError::Registration("config write failed".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                PluginError::NotImplemented("todo".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                PluginError::Io(std::io::Error::other("boom")),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];

        for (error, expected) in cases {
            let response = plugin_error_response(&error);
            assert_eq!(response.status(), expected, "{error}");
        }
    }

    #[actix_web::test]
    async fn error_body_is_a_flat_error_string() {
        let error = PluginError::NotFound("demo".to_string());
        let response = plugin_error_response(&error);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(
            json,
            serde_json::json!({ "error": "plugin not found: demo" })
        );
    }

    /// The four variants mapped to 500 (`Registration`/`NotImplemented`/`Io`/
    /// `Json`) must NEVER leak their raw `Display` text — in particular an
    /// `Io` error's message, which can embed a local filesystem path — into
    /// the response body. Every one of them gets the same fixed, generic
    /// message; the real detail only goes to `tracing`.
    #[actix_web::test]
    async fn internal_error_variants_get_a_generic_sanitized_body() {
        let sensitive_path = "/home/alice/.bamboo/plugins/some-plugin/secret-detail";
        let cases: Vec<PluginError> = vec![
            PluginError::Registration(format!("failed touching {sensitive_path}")),
            PluginError::NotImplemented(format!("not done yet: {sensitive_path}")),
            PluginError::Io(std::io::Error::other(format!(
                "permission denied: {sensitive_path}"
            ))),
            PluginError::Json(
                serde_json::from_str::<serde_json::Value>("{ not valid json")
                    .expect_err("deliberately malformed"),
            ),
        ];

        for error in cases {
            let response = plugin_error_response(&error);
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

            let bytes = actix_web::body::to_bytes(response.into_body())
                .await
                .expect("read body");
            let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
            assert_eq!(
                json,
                serde_json::json!({ "error": "internal error during plugin operation" }),
                "500 body must be the fixed generic message, not {error}'s own Display text"
            );

            let body_text = String::from_utf8(bytes.to_vec()).unwrap();
            assert!(
                !body_text.contains("/home/alice"),
                "500 body must never contain a local filesystem path fragment"
            );
        }
    }
}
