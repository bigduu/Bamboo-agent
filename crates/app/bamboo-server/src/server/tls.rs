//! TLS termination support for the actix-web HTTP face (v2-P1, #181).
//!
//! bamboo terminates TLS itself via rustls — no reverse proxy. Certificates are
//! manual PEM files configured under `server.tls` (see
//! `docs/design/api-v2-transport.md` §3). When `server.tls` is absent the server
//! keeps its plaintext HTTP/1.1 path unchanged; this module is only consulted
//! when TLS is explicitly configured.
//!
//! **Fail-fast invariant:** if `server.tls` is set but the cert/key files are
//! missing or unparseable, [`build_rustls_config`] returns an `Err` and the
//! caller must refuse to start. It never silently downgrades to plaintext.
//!
//! **Crypto provider:** rustls 0.23 needs an active [`CryptoProvider`]. Rather
//! than depend on a process-default provider being installed (a common footgun
//! that panics at handshake time), we build the config against an explicit
//! `ring` provider via [`ServerConfig::builder_with_provider`]. `ring` is the
//! provider already resolved in the workspace lockfile.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;

use bamboo_config::TlsConfig;

/// Build a rustls [`ServerConfig`] from manual PEM cert/key files.
///
/// Returns a descriptive `Err(String)` (never panics, never falls back to
/// plaintext) when:
/// - the cert or key file is missing / unreadable,
/// - the cert file contains no certificates,
/// - the key file contains no usable private key (PKCS#8, PKCS#1/RSA, or SEC1),
/// - rustls rejects the cert/key pair.
pub(super) fn build_rustls_config(tls: &TlsConfig) -> Result<ServerConfig, String> {
    let cert_path = tls.cert_file.display();
    let key_path = tls.key_file.display();

    // --- certificate chain ---
    let cert_file = File::open(&tls.cert_file)
        .map_err(|e| format!("TLS: failed to open cert_file '{cert_path}': {e}"))?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("TLS: failed to parse certificates from '{cert_path}': {e}"))?;
    if certs.is_empty() {
        return Err(format!(
            "TLS: no certificates found in cert_file '{cert_path}' (expected PEM CERTIFICATE blocks)"
        ));
    }

    // --- private key (PKCS#8, then PKCS#1/RSA, then SEC1/EC) ---
    let key = load_private_key(&tls.key_file)
        .map_err(|e| format!("TLS: failed to load key_file '{key_path}': {e}"))?;

    // Build against an explicit `ring` provider so we never rely on a process
    // default being installed (avoids the rustls 0.23 no-provider panic).
    let provider = Arc::new(ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: rustls protocol version setup failed: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            format!("TLS: rustls rejected the cert/key pair (cert '{cert_path}', key '{key_path}'): {e}")
        })?;

    // Bamboo's inbound Actix face is intentionally HTTP/1.1-only (#849).
    // Advertising `h2` here would be worse than a clean rejection: the stream
    // would reach an H1 dispatcher after ALPN had promised HTTP/2. Keep this
    // explicit even though rustls's empty default would also fall back to H1,
    // so clients and regression tests can verify the transport contract.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(config)
}

/// Load the first usable private key from a PEM file, trying PKCS#8, then
/// PKCS#1/RSA, then SEC1/EC. Returns a clear error if none is present.
fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>, String> {
    // rustls_pemfile::private_key understands all three PEM key kinds and
    // returns the first one found, so a single pass covers PKCS#8 / RSA / SEC1.
    let file = File::open(path).map_err(|e| format!("open: {e}"))?;
    match rustls_pemfile::private_key(&mut BufReader::new(file)) {
        Ok(Some(key)) => Ok(key),
        Ok(None) => {
            Err("no private key found (expected a PKCS#8, RSA, or SEC1 PEM block)".to_string())
        }
        Err(e) => Err(format!("parse: {e}")),
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// Generate a throwaway X.509 v3 self-signed cert + PKCS#8 key via openssl.
    ///
    /// `Ok(None)` is reserved for an absent openssl executable. A present
    /// executable that fails must fail the test instead of silently skipping
    /// the rustls success path.
    pub(in crate::server) fn gen_self_signed(
        dir: &Path,
    ) -> Result<Option<(PathBuf, PathBuf)>, String> {
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        let output = Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-keyout"])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .args([
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=localhost",
                // An X.509 extension forces v3 output. macOS LibreSSL otherwise
                // emits a v1 certificate that pinned rustls-webpki rejects.
                "-addext",
                "basicConstraints=critical,CA:FALSE",
            ])
            .output();
        let output = match output {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("failed to launch openssl: {error}")),
        };
        if !output.status.success() {
            return Err(format!(
                "openssl failed to generate the TLS fixture ({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(Some((cert, key)))
    }

    pub(in crate::server) fn assert_x509_v3(cert: &Path) {
        let output = Command::new("openssl")
            .args(["x509", "-in"])
            .arg(cert)
            .args(["-noout", "-text"])
            .output()
            .expect("openssl that generated the fixture should remain available");
        assert!(
            output.status.success(),
            "openssl failed to inspect generated TLS fixture ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        let certificate_text = String::from_utf8_lossy(&output.stdout);
        assert!(
            certificate_text
                .lines()
                .any(|line| line.trim() == "Version: 3 (0x2)"),
            "expected generated TLS fixture to be X.509 v3:\n{certificate_text}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{assert_x509_v3, gen_self_signed};
    use super::*;
    use std::path::PathBuf;

    #[cfg(target_os = "macos")]
    #[inline(never)]
    fn capture_test_backtrace() -> std::backtrace::Backtrace {
        std::backtrace::Backtrace::force_capture()
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_test_link_keeps_dwarf_backtraces() {
        // The macOS platform fixture links this crate's monolithic lib-test,
        // whose build disables only compact-unwind synthesis. Exercise a real
        // stack walk so the CI guard also proves that the retained __eh_frame
        // records remain usable for test debugging.
        let backtrace = capture_test_backtrace();
        assert_eq!(
            backtrace.status(),
            std::backtrace::BacktraceStatus::Captured
        );
        assert!(
            backtrace
                .to_string()
                .lines()
                .any(|line| !line.trim().is_empty()),
            "captured backtrace should contain at least one frame"
        );
    }

    #[test]
    fn build_rustls_config_errors_on_missing_cert_file() {
        let tls = TlsConfig {
            cert_file: PathBuf::from("/nonexistent/bamboo-tls/cert.pem"),
            key_file: PathBuf::from("/nonexistent/bamboo-tls/key.pem"),
        };
        let err = build_rustls_config(&tls).expect_err("missing cert must fail");
        assert!(
            err.contains("cert_file"),
            "error should name cert_file: {err}"
        );
        assert!(
            err.contains("/nonexistent/bamboo-tls/cert.pem"),
            "error should include the path: {err}"
        );
    }

    #[test]
    fn build_rustls_config_errors_on_empty_cert_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "not a pem certificate\n").unwrap();
        std::fs::write(&key, "not a pem key\n").unwrap();
        let tls = TlsConfig {
            cert_file: cert,
            key_file: key,
        };
        let err = build_rustls_config(&tls).expect_err("garbage cert must fail");
        assert!(
            err.contains("no certificates found"),
            "error should explain empty cert: {err}"
        );
    }

    #[test]
    fn build_rustls_config_errors_on_missing_key_in_keyfile() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A valid-looking cert file but a key file with no key block.
        let Some((cert, _key)) =
            gen_self_signed(dir.path()).expect("present openssl should generate the TLS fixture")
        else {
            eprintln!("skipping: openssl unavailable");
            return;
        };
        let bad_key = dir.path().join("bad_key.pem");
        std::fs::write(
            &bad_key,
            "-----BEGIN GARBAGE-----\nzz\n-----END GARBAGE-----\n",
        )
        .unwrap();
        let tls = TlsConfig {
            cert_file: cert,
            key_file: bad_key,
        };
        let err = build_rustls_config(&tls).expect_err("no key must fail");
        assert!(
            err.contains("key_file"),
            "error should name key_file: {err}"
        );
    }

    #[test]
    fn build_rustls_config_succeeds_on_valid_self_signed_cert() {
        // Smoke test the full success path (parse + crypto provider + accept the
        // cert/key pair) when openssl is available to mint a throwaway cert.
        // Skips gracefully on environments without openssl.
        let dir = tempfile::tempdir().expect("tempdir");
        let Some((cert, key)) =
            gen_self_signed(dir.path()).expect("present openssl should generate the TLS fixture")
        else {
            eprintln!("skipping build_rustls_config success test: openssl unavailable");
            return;
        };
        assert_x509_v3(&cert);
        let tls = TlsConfig {
            cert_file: cert,
            key_file: key,
        };
        let cfg = build_rustls_config(&tls).expect("valid self-signed cert should build a config");
        // A built config implies the ring provider was usable and the pair was accepted.
        assert!(
            !cfg.crypto_provider().cipher_suites.is_empty(),
            "expected a non-empty cipher suite set from the ring provider"
        );
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"http/1.1".to_vec()],
            "the inbound TLS face must never advertise vulnerable HTTP/2 (#849)"
        );
    }
}
