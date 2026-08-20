//! Shared HTTP/1.1 server construction for Bamboo's Actix application face.
//!
//! Actix Web 4 couples each high-level Rustls feature to its HTTP/2 feature.
//! That HTTP/2 feature still resolves `h2 0.3`, which is affected by
//! RUSTSEC-2026-0258. Bamboo's client transport contract is HTTPS plus WSS
//! (HTTP/1.1 Upgrade), so every inbound listener is constructed explicitly as
//! HTTP/1.1 here. Outbound Reqwest clients retain HTTP/2 independently.
//!
//! Keeping plaintext and Rustls construction in this one module is a security
//! invariant: a TLS listener must never advertise `h2` and then hand the stream
//! to an H1-only dispatcher.

use std::{
    fmt, io,
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use actix_http::{body::MessageBody, HttpService, Request, Response};
use actix_server::{Server, ServerBuilder};
use actix_service::{map_config, IntoServiceFactory, Service, ServiceFactory, ServiceFactoryExt};
use actix_web::{dev::AppConfig, Error};
use rustls::ServerConfig;

const HTTP11_ALPN: &[u8] = b"http/1.1";

/// Actix keeps `AppConfig`'s ordinary constructor crate-private and exposes
/// this semver-exempt constructor for external server harnesses. Isolate that
/// dependency seam here so every app factory receives the real listener host,
/// local address, and secure bit; a default config would incorrectly report
/// HTTPS requests as `http`.
fn app_config(secure: bool, host: String, addr: SocketAddr) -> AppConfig {
    AppConfig::__priv_test_new(secure, host, addr)
}

/// Build and start an HTTP/1.1-only Actix server over the supplied listeners.
///
/// `tls = None` serves plaintext HTTP/1.1. A Rustls config serves HTTPS/WSS and
/// must advertise only `http/1.1`; [`crate::server::tls::build_rustls_config`]
/// establishes that invariant before this function is called.
pub(super) fn build_h1_server<F, I, S, B>(
    factory: F,
    listeners: Vec<TcpListener>,
    workers: usize,
    tls: Option<ServerConfig>,
) -> io::Result<Server>
where
    F: Fn() -> I + Send + Clone + 'static,
    I: IntoServiceFactory<S, Request>,
    S: ServiceFactory<Request, Config = AppConfig> + 'static,
    S::Error: Into<Error> + 'static,
    S::InitError: fmt::Debug,
    S::Response: Into<Response<B>> + 'static,
    S::Service: 'static,
    <S::Service as Service<Request>>::Future: 'static,
    B: MessageBody + 'static,
{
    if let Some(config) = &tls {
        if config.alpn_protocols.len() != 1 || config.alpn_protocols[0].as_slice() != HTTP11_ALPN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Bamboo's inbound TLS config must advertise only ALPN http/1.1",
            ));
        }
    }

    if listeners.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "at least one HTTP listener is required",
        ));
    }

    let mut builder = ServerBuilder::default().workers(workers);

    for (index, listener) in listeners.into_iter().enumerate() {
        let addr = listener.local_addr()?;
        let name = format!("bamboo-http1-{index}-{addr}");
        let host = addr.to_string();
        let app_factory = factory.clone();

        builder = match &tls {
            Some(tls_config) => {
                let tls_config = tls_config.clone();
                builder.listen(name, listener, move || {
                    let app_config = app_config(true, host.clone(), addr);
                    let app = app_factory()
                        .into_factory()
                        .map_err(|err| err.into().error_response());

                    HttpService::build()
                        .secure()
                        .local_addr(addr)
                        .client_disconnect_timeout(Duration::from_secs(1))
                        .h1(map_config(app, move |_| app_config.clone()))
                        .rustls_0_23(tls_config.clone())
                })?
            }
            None => builder.listen(name, listener, move || {
                let app_config = app_config(false, host.clone(), addr);
                let app = app_factory()
                    .into_factory()
                    .map_err(|err| err.into().error_response());

                HttpService::build()
                    .local_addr(addr)
                    .client_disconnect_timeout(Duration::from_secs(1))
                    .h1(map_config(app, move |_| app_config.clone()))
                    .tcp()
            })?,
        };
    }

    Ok(builder.run())
}
