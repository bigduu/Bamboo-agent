use std::net::TcpListener;

use log::warn;

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
