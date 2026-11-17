//! Tailnet-only access to the existing Bamboo and Lotus HTTP application.

mod cli;
mod status;

use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    pin::Pin,
    sync::RwLock,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use actix_web::{
    body::{BodySize, EitherBody, MessageBody},
    dev::{ServiceRequest, ServiceResponse},
    http::header,
    middleware::Next,
    web, HttpRequest, HttpResponse,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::{
    sync::{CancellationToken, WaitForCancellationFutureOwned},
    task::AbortOnDropHandle,
};

use crate::{handlers::settings::has_usable_access_password, AppError, AppState};
use cli::{Cli, ServeWorker};
use status::{ClientStatus, PortUse, Share, State, Status, HTTPS_PORTS};

pub use cli::helper_entry;

const HTTPS_SETUP_URL: &str = "https://login.tailscale.com/admin/dns";
const APPROVAL_URL: &str = "https://login.tailscale.com/admin/machines";
const RETRY_INTERVAL: Duration = Duration::from_secs(15);
const STARTUP_DEADLINE: Duration = Duration::from_secs(10);

pub(super) struct TailscaleAccess {
    _owner: File,
    ingress: SocketAddr,
    preference_file: PathBuf,
    runtime: Mutex<Runtime>,
    active: RwLock<Option<(Share, CancellationToken)>>,
}

struct Runtime {
    desired: Option<Share>,
    worker: Option<(ServeWorker, Instant)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Connect,
    Enable,
    Disable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionRequest {
    action: Action,
}

impl TailscaleAccess {
    pub(super) fn supported() -> bool {
        cli::helper_available()
    }

    pub(super) async fn bind(home: &Path) -> std::io::Result<(TcpListener, web::Data<Self>)> {
        // Only one Bamboo process may manage a saved share in this data dir.
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(home.join("tailscale-access.lock"))?;
        owner.try_lock().map_err(std::io::Error::other)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let ingress = listener.local_addr()?;
        let preference_file = home.join("tailscale-access.json");
        let desired = tokio::fs::read(&preference_file)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Share>(&bytes).ok())
            .filter(Share::valid);
        let access = web::Data::new(Self {
            _owner: owner,
            ingress,
            preference_file,
            runtime: Mutex::new(Runtime {
                desired,
                worker: None,
            }),
            active: RwLock::new(None),
        });
        Ok((listener, access))
    }

    pub(super) fn reconcile(
        access: web::Data<Self>,
        app: web::Data<AppState>,
    ) -> AbortOnDropHandle<()> {
        AbortOnDropHandle::new(tokio::spawn(async move {
            let mut delay = Duration::ZERO;
            loop {
                tokio::time::sleep(delay).await;
                delay = RETRY_INTERVAL;
                let mut runtime = access.runtime.lock().await;
                if runtime.desired.is_some()
                    && access
                        .refresh(&mut runtime, &app, None, true)
                        .await
                        .is_ok_and(|status| status.state == State::Starting)
                {
                    delay = Duration::from_secs(1);
                }
            }
        }))
    }

    pub(super) async fn shutdown(&self) {
        self.stop(&mut *self.runtime.lock().await).await;
    }

    fn publish(&self, share: Option<Share>) {
        if let Ok(mut active) = self.active.write() {
            if active.as_ref().map(|(share, _)| share) == share.as_ref() {
                return;
            }
            if let Some((_, token)) = active.take() {
                token.cancel();
            }
            *active = share.map(|share| (share, CancellationToken::new()));
        }
    }

    async fn stop(&self, runtime: &mut Runtime) {
        self.publish(None);
        if let Some((worker, _)) = runtime.worker.take() {
            worker.stop().await;
        }
    }

    async fn save(&self, share: &Share) -> std::io::Result<()> {
        let path = self.preference_file.clone();
        let bytes = serde_json::to_vec(share)?;
        tokio::task::spawn_blocking(move || {
            let mut file = tempfile::NamedTempFile::new_in(path.parent().expect("data directory"))?;
            file.write_all(&bytes)?;
            file.as_file().sync_all()?;
            file.persist(path)?;
            Ok(())
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn refresh(
        &self,
        runtime: &mut Runtime,
        app: &AppState,
        action: Option<Action>,
        reconcile: bool,
    ) -> Result<Status, AppError> {
        if action == Some(Action::Disable) {
            // Keep the current share visible if its saved preference could
            // not be cleared. Otherwise a restart could silently restore it.
            match tokio::fs::remove_file(&self.preference_file).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(AppError::StorageError(error)),
            }
            runtime.desired = None;
            self.stop(runtime).await;
        }

        let mut status = Status::new(runtime.desired.is_some());
        let Some(cli) = Cli::find() else {
            status.state = State::NotInstalled;
            self.stop(runtime).await;
            return Ok(status);
        };
        let mut client = ClientStatus::read(&cli).await;
        if action == Some(Action::Connect) && client.can_connect() {
            let auth_url = ClientStatus::connect(&cli).await;
            client = ClientStatus::read(&cli).await;
            if client.can_connect() && client.auth_url().is_none() {
                match auth_url {
                    Ok(url) => client.set_auth_url(url),
                    Err(()) => {
                        self.stop(runtime).await;
                        return Ok(status);
                    }
                }
            }
        }
        status.state = client.state();
        status.action_url = match status.state {
            State::LoginRequired => client.auth_url().map(str::to_owned),
            State::DeviceApprovalRequired => Some(APPROVAL_URL.to_owned()),
            _ => None,
        };
        if status.state != State::Ready {
            self.stop(runtime).await;
            return Ok(status);
        }

        let Some(node) = client.node() else {
            status.state = State::Unavailable;
            self.stop(runtime).await;
            return Ok(status);
        };
        status.tailnet = client.tailnet().map(str::to_owned);
        if runtime
            .desired
            .as_ref()
            .is_some_and(|share| !share.belongs_to(node))
        {
            status.state = State::TailnetChanged;
            self.stop(runtime).await;
            return Ok(status);
        }
        let password_ready = has_usable_access_password(&*app.config.read().await);
        if !password_ready {
            status.state = State::PasswordRequired;
            self.stop(runtime).await;
            return Ok(status);
        }
        if !client.https_ready(node) {
            status.state = State::HttpsRequired;
            status.action_url = Some(HTTPS_SETUP_URL.to_owned());
            self.stop(runtime).await;
            return Ok(status);
        }

        if runtime.desired.is_none() && action != Some(Action::Enable) {
            return Ok(status);
        }
        let Some(config) = status::ServeConfig::read(&cli).await else {
            status.state = State::Unavailable;
            self.stop(runtime).await;
            return Ok(status);
        };
        if let Some((worker, _)) = &mut runtime.worker {
            if !worker.running() {
                self.stop(runtime).await;
                if !reconcile && action != Some(Action::Enable) {
                    status.state = State::ServeFailed;
                    return Ok(status);
                }
            }
        }
        let target = format!("http://{}", self.ingress);
        let share = match runtime.desired.clone() {
            Some(share) => share,
            None => {
                let Some(port) = HTTPS_PORTS.into_iter().find(|port| {
                    config.port_use(&Share::new(node, *port), &target) == PortUse::Free
                }) else {
                    status.state = State::PortInUse;
                    return Ok(status);
                };
                let share = Share::new(node, port);
                self.save(&share).await.map_err(AppError::StorageError)?;
                runtime.desired = Some(share.clone());
                status.enabled = true;
                share
            }
        };
        match config.port_use(&share, &target) {
            PortUse::Owned if runtime.worker.is_some() => {
                self.publish(Some(share.clone()));
                status.state = State::Active;
                status.url = Some(format!("{}/", share.origin()));
            }
            PortUse::Free => {
                self.publish(None);
                if let Some((_, started)) = &runtime.worker {
                    if started.elapsed() < STARTUP_DEADLINE {
                        status.state = State::Starting;
                        return Ok(status);
                    }
                    self.stop(runtime).await;
                    status.state = State::ServeFailed;
                } else if reconcile || action == Some(Action::Enable) {
                    status.state = match cli.serve(share.port, self.ingress) {
                        Ok(worker) => {
                            runtime.worker = Some((worker, Instant::now()));
                            State::Starting
                        }
                        Err(_) => State::ServeFailed,
                    };
                } else {
                    status.state = State::ServeFailed;
                }
            }
            _ => {
                self.stop(runtime).await;
                status.state = State::PortInUse;
            }
        }
        Ok(status)
    }

    fn accept(&self, req: &HttpRequest) -> Option<CancellationToken> {
        let active = self.active.read().ok()?;
        let (share, token) = active.as_ref()?;
        let origin = share.origin();
        let authority = origin.trim_start_matches("https://");
        let forwarded_host = header_value(req, "x-forwarded-host");
        // Serve replaces the identity/forwarding headers. Funnel traffic and
        // tagged clients without a user identity cannot pass this listener.
        let allowed = req.peer_addr().is_some_and(|peer| peer.ip().is_loopback())
            && !req.headers().contains_key("tailscale-funnel-request")
            && header_value(req, "tailscale-user-login")
                .is_some_and(|user| !user.trim().is_empty())
            && header_value(req, "x-forwarded-proto") == Some("https")
            && forwarded_host == header_value(req, "host")
            && forwarded_host.is_some_and(|host| {
                host.eq_ignore_ascii_case(authority)
                    || (share.port == 443 && host.eq_ignore_ascii_case(&format!("{authority}:443")))
            })
            && origin_matches(req, &origin);
        allowed.then(|| token.clone())
    }
}

pub(crate) fn is_ingress(req: &HttpRequest) -> bool {
    req.app_data::<web::Data<TailscaleAccess>>()
        .is_some_and(|access| req.app_config().local_addr() == access.ingress)
}

pub(crate) fn client_ip(req: &HttpRequest) -> Option<std::net::IpAddr> {
    if !is_ingress(req) {
        return None;
    }
    // The dedicated listener admits Serve's HTTPS requests. Serve overwrites
    // this header with one Tailnet peer address.
    header_value(req, "x-forwarded-for")?.parse().ok()
}

pub(super) async fn restrict_ingress<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> actix_web::Result<ServiceResponse<EitherBody<B>>> {
    if !is_ingress(req.request()) {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_left_body);
    }
    let Some(token) = req
        .app_data::<web::Data<TailscaleAccess>>()
        .and_then(|access| access.accept(req.request()))
    else {
        return Ok(req
            .into_response(HttpResponse::Forbidden().finish())
            .map_into_right_body());
    };
    let app = req
        .app_data::<web::Data<AppState>>()
        .expect("shared HTTP application state");
    if !has_usable_access_password(&*app.config.read().await) {
        return Ok(req
            .into_response(HttpResponse::ServiceUnavailable().finish())
            .map_into_right_body());
    }
    let response = tokio::select! {
        biased;
        _ = token.cancelled() => return Err(actix_web::error::ErrorForbidden("Tailscale access closed")),
        response = next.call(req) => response?,
    };
    Ok(response
        .map_body(|_, body| TailnetBody {
            body: Box::pin(body),
            cancelled: Box::pin(token.cancelled_owned()),
        })
        .map_into_boxed_body()
        .map_into_right_body())
}

// Closing the share also ends active SSE and WebSocket response bodies.
struct TailnetBody<B> {
    body: Pin<Box<B>>,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<B: MessageBody> MessageBody for TailnetBody<B> {
    type Error = Box<dyn std::error::Error>;

    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<web::Bytes, Self::Error>>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(
                std::io::Error::other("Tailscale access closed").into()
            )));
        }
        self.body
            .as_mut()
            .poll_next(cx)
            .map(|item| item.map(|result| result.map_err(Into::into)))
    }
}

fn header_value<'a>(req: &'a HttpRequest, name: &str) -> Option<&'a str> {
    req.headers().get(name)?.to_str().ok()
}

fn origin_matches(req: &HttpRequest, expected: &str) -> bool {
    !req.headers().contains_key(header::ORIGIN) || header_value(req, "origin") == Some(expected)
}

fn locally_managed(req: &HttpRequest) -> bool {
    if is_ingress(req) || !req.peer_addr().is_some_and(|peer| peer.ip().is_loopback()) {
        return false;
    }
    let Some(host) = header_value(req, "host") else {
        return false;
    };
    let Ok(authority) = host.parse::<actix_web::http::uri::Authority>() else {
        return false;
    };
    let hostname = authority.host().trim_matches(['[', ']']);
    let loopback = hostname.eq_ignore_ascii_case("localhost")
        || hostname
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    loopback
        && authority.port_u16().unwrap_or(80) == req.app_config().local_addr().port()
        && ![
            "forwarded",
            "x-forwarded-for",
            "x-forwarded-host",
            "x-forwarded-proto",
            "x-original-host",
            "tailscale-user-login",
        ]
        .into_iter()
        .any(|name| req.headers().contains_key(name))
        && origin_matches(req, &format!("http://{host}"))
}

pub(crate) fn routes() -> impl actix_web::dev::HttpServiceFactory {
    web::resource("/bamboo/tailscale")
        .route(web::get().to(get))
        .route(web::post().to(update))
}

async fn get(req: HttpRequest, app: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    handle(req, app, None).await
}

async fn update(
    req: HttpRequest,
    app: web::Data<AppState>,
    body: web::Json<ActionRequest>,
) -> Result<HttpResponse, AppError> {
    handle(req, app, Some(body.action)).await
}

async fn handle(
    req: HttpRequest,
    app: web::Data<AppState>,
    action: Option<Action>,
) -> Result<HttpResponse, AppError> {
    let access = req
        .app_data::<web::Data<TailscaleAccess>>()
        .filter(|_| locally_managed(&req))
        .cloned()
        .ok_or_else(|| AppError::NotFound("local Tailscale settings".into()))?;
    let update = async move {
        let mut runtime = access.runtime.lock().await;
        access.refresh(&mut runtime, &app, action, false).await
    };
    let status = if action.is_some() {
        // The preference write and corresponding in-memory update share one
        // owner even if the browser disconnects before receiving the reply.
        tokio::spawn(update)
            .await
            .map_err(|error| AppError::InternalError(error.into()))??
    } else {
        update.await?
    };
    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .json(status))
}

#[cfg(test)]
mod tests;
