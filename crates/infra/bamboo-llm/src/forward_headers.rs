//! End-to-end headers for one compatibility API call.

use std::{cell::RefCell, collections::HashSet, future::Future};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONNECTION};

struct ForwardHeaders {
    request: HeaderMap,
    response: RefCell<HeaderMap>,
}

tokio::task_local! {
    static FORWARD_HEADERS: ForwardHeaders;
}

pub async fn scope<T>(request: HeaderMap, future: impl Future<Output = T>) -> (T, HeaderMap) {
    FORWARD_HEADERS
        .scope(
            ForwardHeaders {
                request: end_to_end_headers(&request),
                response: RefCell::default(),
            },
            async {
                let result = future.await;
                let response = FORWARD_HEADERS.with(|headers| headers.response.take());
                (result, response)
            },
        )
        .await
}

pub(crate) fn has_request_headers() -> bool {
    FORWARD_HEADERS
        .try_with(|headers| !headers.request.is_empty())
        .unwrap_or(false)
}

pub(crate) fn apply_request(headers: &mut HeaderMap) {
    let _ = FORWARD_HEADERS.try_with(|forward| {
        forward.response.borrow_mut().clear();
        for name in forward.request.keys() {
            let values = forward.request.get_all(name);
            if !headers.contains_key(name) {
                for value in values {
                    headers.append(name.clone(), value.clone());
                }
            } else if name == "anthropic-beta" {
                let mut seen = HashSet::new();
                let betas = headers
                    .get_all(name)
                    .iter()
                    .chain(values.iter())
                    .filter_map(|value| value.to_str().ok())
                    .flat_map(|value| value.split(',').map(str::trim))
                    .filter(|beta| !beta.is_empty() && seen.insert(*beta))
                    .collect::<Vec<_>>();
                if let Ok(value) = HeaderValue::from_str(&betas.join(",")) {
                    headers.insert(name.clone(), value);
                }
            }
        }
    });
}

pub(crate) fn capture_response(headers: &HeaderMap) {
    let _ = FORWARD_HEADERS.try_with(|forward| {
        *forward.response.borrow_mut() = end_to_end_headers(headers);
    });
}

fn end_to_end_headers(headers: &HeaderMap) -> HeaderMap {
    let connection_options: HashSet<_> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    let mut retained = HeaderMap::new();
    for (name, value) in headers {
        if !connection_options.contains(name) && is_forwardable(name.as_str()) {
            retained.append(name.clone(), value.clone());
        }
    }
    retained
}

fn is_forwardable(mut name: &str) -> bool {
    // Some gateways prefix upstream fields when embedding them in their reply.
    while let Some(inner) = name.strip_prefix("llm_provider-") {
        name = inner;
    }

    // These fields belong to the HTTP hop, serialized entity, or local API.
    if matches!(
        name,
        "host"
            | "connection"
            | "proxy-connection"
            | "keep-alive"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "via"
            | "forwarded"
            | "expect"
            | "max-forwards"
            | "accept"
            | "accept-encoding"
            | "accept-language"
            | "user-agent"
            | "server"
            | "origin"
            | "referer"
            | "location"
            | "link"
            | "refresh"
            | "date"
            | "age"
            | "cache-control"
            | "pragma"
            | "expires"
            | "vary"
            | "etag"
            | "last-modified"
            | "range"
            | "accept-ranges"
            | "strict-transport-security"
            | "permissions-policy"
            | "referrer-policy"
            | "clear-site-data"
            | "alt-svc"
            | "nel"
            | "report-to"
            | "reporting-endpoints"
            | "www-authenticate"
            | "cookie2"
            | "set-cookie2"
            | "x-real-ip"
            | "x-method-override"
            | "x-sendfile"
            | "x-frame-options"
            | "x-content-type-options"
            | "x-xss-protection"
            | "idempotency-key"
            | "openai-organization"
            | "openai-project"
            | "x-goog-user-project"
            | "content-type"
            | "content-length"
            | "content-encoding"
            | "content-language"
            | "content-location"
            | "content-disposition"
            | "content-range"
            | "content-digest"
            | "content-md5"
            | "content-security-policy"
            | "content-security-policy-report-only"
            | "content-transfer-encoding"
            | "if-match"
            | "if-none-match"
            | "if-modified-since"
            | "if-unmodified-since"
            | "if-range"
    ) || [
        "access-control-",
        "sec-",
        "cross-origin-",
        "bamboo-",
        "x-bamboo-",
        "x-forwarded-",
        "x-original-",
        "x-rewrite-",
        "x-http-method-",
        "x-accel-",
        "x-envoy-",
        "x-middleware-",
    ]
    .iter()
    .any(|prefix| name.starts_with(prefix))
    {
        return false;
    }

    // API-key spellings and authentication aliases can appear in custom namespaces.
    !name.contains("api-key")
        && !name.contains("api_key")
        && !name.ends_with("-access-key")
        && !name.ends_with("-subscription-key")
        && !name.split(['-', '_']).any(|part| {
            matches!(
                part,
                "auth"
                    | "authorization"
                    | "authenticate"
                    | "authentication"
                    | "apikey"
                    | "password"
                    | "cookie"
                    | "credential"
                    | "credentials"
                    | "secret"
                    | "token"
                    | "signature"
            )
        })
}
