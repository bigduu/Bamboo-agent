use std::net::IpAddr;

use actix_web::{
    http::{header, uri::Authority},
    web, HttpRequest, HttpResponse,
};
use serde::Deserialize;
use uuid::Uuid;

use super::{user, AppState, Error, TicketHttpError};
use crate::app_state::ticket_application::codex_handoff::WorkSelector;

pub fn configure(routes: &mut web::ServiceConfig) {
    routes
        .route("/codex-handoff", web::post().to(prepare))
        .route("/codex-handoff/claim", web::post().to(claim));
}

fn loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.to_canonical().is_loopback())
}

pub(super) fn require_local(req: &HttpRequest) -> Result<(), TicketHttpError> {
    let local_host = req
        .headers()
        .get(header::HOST)
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.parse::<Authority>().ok())
        .is_some_and(|value| loopback_host(value.host()));
    let local_origin = req.headers().get(header::ORIGIN).is_none_or(|header| {
        header
            .to_str()
            .ok()
            .and_then(|raw| url::Url::parse(raw).ok())
            .is_some_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some_and(loopback_host)
            })
    });
    if !local_host
        || !local_origin
        || crate::server::tailscale::is_ingress(req)
        || !req
            .peer_addr()
            .is_some_and(|peer| peer.ip().to_canonical().is_loopback())
    {
        return Err(
            Error::ScopeDenied("Codex Work handoffs require a local connection".into()).into(),
        );
    }
    Ok(())
}

async fn prepare(
    state: web::Data<AppState>,
    req: HttpRequest,
    selector: web::Json<WorkSelector>,
) -> Result<HttpResponse, TicketHttpError> {
    require_local(&req)?;
    let principal = user(&state, &req).await?;
    let prepared = state
        .tickets
        .prepare_codex_handoff(principal, selector.into_inner())
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .json(prepared))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    offer_id: Uuid,
}

async fn claim(
    state: web::Data<AppState>,
    req: HttpRequest,
    claim: web::Json<Claim>,
) -> Result<HttpResponse, TicketHttpError> {
    require_local(&req)?;
    let principal = user(&state, &req).await?;
    let prompt = state
        .tickets
        .claim_codex_handoff(principal, claim.offer_id)
        .await?;
    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .content_type("text/plain; charset=utf-8")
        .body(prompt))
}
