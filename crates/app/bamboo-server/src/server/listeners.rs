use std::net::{TcpListener, ToSocketAddrs};

use socket2::{Domain, Protocol, Socket, Type};
use tracing::warn;

const HTTP_LISTEN_BACKLOG: i32 = 1024;

pub(super) const DEFAULT_WORKER_COUNT: usize = 10;

pub(super) fn resolve_worker_count() -> usize {
    // Keep server-level worker configuration non-breaking by sourcing from env.
    // The `bamboo` binary maps config/CLI into this env var before starting.
    const ENV_KEY: &str = "BAMBOO_WORKERS";

    match std::env::var(ENV_KEY) {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                warn!(
                    "Invalid {} value '{}'; using default worker count {}",
                    ENV_KEY, raw, DEFAULT_WORKER_COUNT
                );
                DEFAULT_WORKER_COUNT
            }
        },
        Err(_) => DEFAULT_WORKER_COUNT,
    }
}

fn try_make_listener(addr: &str) -> std::io::Result<TcpListener> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Resolve and bind every address for an `HttpServer::bind((host, port))`
/// compatible call. `WebService` historically used that API, which keeps any
/// successful IPv4/IPv6 listener even if another resolved address fails.
pub(super) fn build_resolved_listeners(bind: &str, port: u16) -> Result<Vec<TcpListener>, String> {
    let addresses = (bind, port)
        .to_socket_addrs()
        .map_err(|e| format!("Failed to resolve bind address {bind}:{port}: {e}"))?;
    let mut listeners = Vec::new();
    let mut last_error = None;

    for address in addresses {
        match make_http_listener(address) {
            Ok(listener) => {
                listeners.push(listener);
            }
            Err(error) => last_error = Some((address, error)),
        }
    }

    if !listeners.is_empty() {
        Ok(listeners)
    } else if let Some((address, error)) = last_error {
        Err(format!("Failed to bind listener {address}: {error}"))
    } else {
        Err(format!(
            "Bind address {bind}:{port} resolved to no socket addresses"
        ))
    }
}

/// Match the socket options used by Actix Web's high-level `HttpServer::bind`.
/// This keeps rapid Unix restarts and Windows IPv6 dual-stack behavior stable
/// after the HTTP/1.1-only server moved to pre-bound listeners.
fn make_http_listener(address: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;

    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;

    #[cfg(windows)]
    if address.is_ipv6() {
        if let Err(error) = socket.set_only_v6(false) {
            warn!(%address, %error, "failed to enable IPv4 on the IPv6 HTTP listener");
        }
    }

    socket.bind(&address.into())?;
    socket.listen(HTTP_LISTEN_BACKLOG)?;
    let listener = TcpListener::from(socket);
    listener.set_nonblocking(true)?;
    Ok(listener)
}

fn is_addr_in_use(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::AddrInUse
}

pub(super) fn build_desktop_listeners(port: u16) -> Result<Vec<TcpListener>, String> {
    let v4_addr = format!("127.0.0.1:{port}");
    let v4 = try_make_listener(&v4_addr)
        .map_err(|e| format!("Failed to bind listener {v4_addr}: {e}"))?;

    let v6_addr = format!("[::1]:{port}");
    let v6 = match try_make_listener(&v6_addr) {
        Ok(l) => Some(l),
        Err(e) => {
            warn!("IPv6 loopback bind skipped ({v6_addr}): {e}");
            None
        }
    };

    let mut listeners = vec![v4];
    if let Some(v6) = v6 {
        listeners.push(v6);
    }

    Ok(listeners)
}

pub(super) fn build_bind_listeners(bind: &str, port: u16) -> Result<Vec<TcpListener>, String> {
    // Build listeners first so IPv6 is best-effort (never fatal when unsupported).
    // - 127.0.0.1/localhost: add ::1 if available
    // - 0.0.0.0: add [::] if available (and tolerate IPv4 AddrInUse when IPv6 binds dual-stack)
    let mut listeners: Vec<TcpListener> = Vec::new();
    let mut has_ipv6_any = false;

    if bind == "0.0.0.0" {
        let v6_addr = format!("[::]:{port}");
        match try_make_listener(&v6_addr) {
            Ok(l) => {
                has_ipv6_any = true;
                listeners.push(l);
            }
            Err(e) => warn!("IPv6 any bind skipped ({v6_addr}): {e}"),
        }

        // IPv4 is still preferred for compatibility; tolerate AddrInUse when IPv6 bound dual-stack.
        let v4_addr = format!("0.0.0.0:{port}");
        match try_make_listener(&v4_addr) {
            Ok(l) => listeners.push(l),
            Err(e) => {
                if has_ipv6_any && is_addr_in_use(&e) {
                    warn!("IPv4 any bind skipped (already covered by IPv6 dual-stack?) ({v4_addr}): {e}");
                } else {
                    return Err(format!("Failed to bind listener {v4_addr}: {e}"));
                }
            }
        }
    } else if bind == "127.0.0.1" || bind == "localhost" {
        let v4_addr = format!("127.0.0.1:{port}");
        listeners.push(
            try_make_listener(&v4_addr)
                .map_err(|e| format!("Failed to bind listener {v4_addr}: {e}"))?,
        );

        let v6_addr = format!("[::1]:{port}");
        match try_make_listener(&v6_addr) {
            Ok(l) => listeners.push(l),
            Err(e) => warn!("IPv6 loopback bind skipped ({v6_addr}): {e}"),
        }
    } else if bind.contains(':') {
        // Treat as IPv6 literal.
        let addr = if bind.starts_with('[') {
            format!("{bind}:{port}")
        } else {
            format!("[{bind}]:{port}")
        };
        listeners.push(
            try_make_listener(&addr).map_err(|e| format!("Failed to bind listener {addr}: {e}"))?,
        );
    } else {
        // IPv4 literal or hostname.
        let addr = format!("{bind}:{port}");
        listeners.push(
            try_make_listener(&addr).map_err(|e| format!("Failed to bind listener {addr}: {e}"))?,
        );
    }

    Ok(listeners)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_listener_preserves_exact_ipv4_bind() {
        let listeners =
            build_resolved_listeners("127.0.0.1", 0).expect("bind an ephemeral IPv4 listener");
        assert_eq!(listeners.len(), 1);
        let address = listeners[0].local_addr().expect("listener address");
        assert!(address.is_ipv4());
        assert!(address.ip().is_loopback());
        assert_ne!(address.port(), 0);
    }

    #[test]
    fn resolved_localhost_listeners_remain_loopback() {
        let listeners =
            build_resolved_listeners("localhost", 0).expect("bind resolved localhost listeners");
        assert!(!listeners.is_empty());
        assert!(listeners.iter().all(|listener| listener
            .local_addr()
            .expect("listener address")
            .ip()
            .is_loopback()));
    }

    #[test]
    fn production_loopback_listeners_are_all_loopback() {
        let listeners = build_bind_listeners("127.0.0.1", 0).expect("build loopback listeners");
        assert!(!listeners.is_empty());
        assert!(listeners.iter().all(|listener| listener
            .local_addr()
            .expect("listener address")
            .ip()
            .is_loopback()));
        assert!(listeners
            .iter()
            .any(|listener| listener.local_addr().expect("listener address").is_ipv4()));
    }
}
