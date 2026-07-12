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
//! | `Registration` / `Io` / `Json` / `NotImplemented` | 500 |
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

use actix_web::HttpResponse;
use bamboo_plugin::PluginError;

pub fn plugin_error_response(error: &PluginError) -> HttpResponse {
    let body = serde_json::json!({ "error": error.to_string() });
    match error {
        PluginError::Conflict { .. } => HttpResponse::Conflict().json(body),
        PluginError::AlreadyInstalled(_) => HttpResponse::Conflict().json(body),
        PluginError::UnsupportedPlatform { .. } => HttpResponse::UnprocessableEntity().json(body),
        PluginError::NotFound(_) => HttpResponse::NotFound().json(body),
        PluginError::InvalidManifest(_) => HttpResponse::BadRequest().json(body),
        PluginError::ArtifactVerificationFailed(_) => HttpResponse::BadRequest().json(body),
        PluginError::Registration(_)
        | PluginError::NotImplemented(_)
        | PluginError::Io(_)
        | PluginError::Json(_) => HttpResponse::InternalServerError().json(body),
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
}
