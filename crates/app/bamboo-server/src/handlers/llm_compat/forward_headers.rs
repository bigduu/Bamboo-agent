//! Bridge the host HTTP headers to the shared provider transport.

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::{header, Method},
    middleware::Next,
    Error,
};

struct ForwardedResponseHeaders(Vec<header::HeaderName>);

pub(crate) async fn forward<B: MessageBody + 'static>(
    request: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    if request.method() != Method::POST {
        return next.call(request).await;
    }
    let mut incoming = reqwest::header::HeaderMap::new();
    for (name, value) in request.headers() {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            incoming.append(name, value);
        }
    }

    let (response, upstream) =
        bamboo_llm::forward_headers::scope(incoming, next.call(request)).await;
    let mut response = response?;
    let mut exposed = Vec::new();
    for name in upstream.keys() {
        let Ok(name) = header::HeaderName::try_from(name.as_str()) else {
            continue;
        };
        if response.headers().contains_key(&name) {
            continue;
        }
        for value in upstream.get_all(name.as_str()) {
            if let Ok(value) = header::HeaderValue::from_bytes(value.as_bytes()) {
                response.headers_mut().append(name.clone(), value);
            }
        }
        exposed.push(name);
    }
    if !exposed.is_empty() {
        response
            .response_mut()
            .extensions_mut()
            .insert(ForwardedResponseHeaders(exposed));
    }
    Ok(response)
}

/// Our CORS layer owns the static exposure list. Extend it on the way out only
/// after that layer has admitted the origin.
pub(crate) async fn expose<B: MessageBody + 'static>(
    request: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let mut response = next.call(request).await?;
    let forwarded = response
        .response_mut()
        .extensions_mut()
        .remove::<ForwardedResponseHeaders>();
    if let Some(ForwardedResponseHeaders(names)) = forwarded {
        if response
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        {
            let names = names
                .iter()
                .map(header::HeaderName::as_str)
                .collect::<Vec<_>>();
            if let Ok(value) = header::HeaderValue::from_str(&names.join(", ")) {
                response
                    .headers_mut()
                    .append(header::ACCESS_CONTROL_EXPOSE_HEADERS, value);
            }
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests;
