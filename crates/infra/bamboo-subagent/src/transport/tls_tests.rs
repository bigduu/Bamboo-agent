//! Regression coverage for the PEM-loaded server and pinned-root client.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::{build_server_config, client_config_trusting_cert};

fn openssl(command: &mut Command) {
    let output = command
        .output()
        .expect("openssl fixture command should run");
    assert!(
        output.status.success(),
        "openssl fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture(dir: &Path, ec: bool) -> Option<(PathBuf, PathBuf)> {
    match Command::new("openssl").arg("version").output() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("cannot launch openssl: {error}"),
        Ok(output) => assert!(output.status.success(), "openssl version should succeed"),
    }
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    let mut req = Command::new("openssl");
    req.args(["req", "-new", "-x509"]);
    if ec {
        openssl(
            Command::new("openssl")
                .args([
                    "ecparam",
                    "-name",
                    "prime256v1",
                    "-genkey",
                    "-noout",
                    "-out",
                ])
                .arg(&key),
        );
        req.arg("-key").arg(&key);
    } else {
        req.args(["-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&key);
    }
    openssl(req.arg("-out").arg(&cert).args([
        "-days",
        "1",
        "-subj",
        "/CN=localhost",
        "-addext",
        "subjectAltName=DNS:localhost",
        "-addext",
        "basicConstraints=critical,CA:FALSE",
    ]));
    Some((cert, key))
}

async fn handshake(
    server: rustls::ServerConfig,
    client: rustls::ClientConfig,
) -> Vec<CertificateDer<'static>> {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let connector = TlsConnector::from(Arc::new(client));
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let (client, server) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            connector.connect(ServerName::try_from("localhost").unwrap(), client_io),
            acceptor.accept(server_io)
        )
    })
    .await
    .expect("TLS handshake should not hang");
    let client = client.expect("client accepts the loaded chain");
    let server = server.expect("server accepts a client without a certificate");
    assert!(server.get_ref().1.peer_certificates().is_none());
    client
        .get_ref()
        .1
        .peer_certificates()
        .expect("server chain")
        .to_vec()
}

#[tokio::test]
async fn pem_key_formats_build_configs_and_complete_tls_handshakes() {
    let rsa = tempfile::tempdir().unwrap();
    let Some((rsa_cert, rsa_key)) = fixture(rsa.path(), false) else {
        eprintln!("skipping: openssl unavailable");
        return;
    };
    let pkcs8 = rsa.path().join("pkcs8.pem");
    openssl(
        Command::new("openssl")
            .args(["pkcs8", "-topk8", "-nocrypt", "-in"])
            .arg(&rsa_key)
            .arg("-out")
            .arg(&pkcs8),
    );
    let pkcs1 = rsa.path().join("pkcs1.pem");
    let version = Command::new("openssl").arg("version").output().unwrap();
    let version = String::from_utf8_lossy(&version.stdout);
    let mut convert = Command::new("openssl");
    convert.arg("rsa");
    // OpenSSL 3+ defaults even `rsa` output to PKCS#8. LibreSSL and
    // OpenSSL 1.x write the traditional PKCS#1 form without this option.
    if version
        .strip_prefix("OpenSSL ")
        .and_then(|v| v.split('.').next())
        .and_then(|v| v.parse::<u32>().ok())
        .is_some_and(|v| v >= 3)
    {
        convert.arg("-traditional");
    }
    openssl(convert.arg("-in").arg(&rsa_key).arg("-out").arg(&pkcs1));
    let ec = tempfile::tempdir().unwrap();
    let (ec_cert, sec1) = fixture(ec.path(), true).expect("openssl is available");
    for (cert, key, label) in [
        (&rsa_cert, &pkcs8, "PRIVATE KEY"),
        (&rsa_cert, &pkcs1, "RSA PRIVATE KEY"),
        (&ec_cert, &sec1, "EC PRIVATE KEY"),
    ] {
        assert!(fs::read_to_string(key)
            .unwrap()
            .contains(&format!("-----BEGIN {label}-----")));
        let server =
            build_server_config(cert, key).expect("supported key should build server config");
        let client = client_config_trusting_cert(cert).expect("pinned roots should load");
        handshake(server, client).await;
    }
}

#[tokio::test]
async fn loaded_certificate_chain_and_first_private_key_keep_file_order() {
    let first = tempfile::tempdir().unwrap();
    let Some((first_cert, first_key)) = fixture(first.path(), false) else {
        eprintln!("skipping: openssl unavailable");
        return;
    };
    let second = tempfile::tempdir().unwrap();
    let (second_cert, second_key) = fixture(second.path(), true).expect("openssl is available");
    let chain = first.path().join("chain.pem");
    fs::write(
        &chain,
        [
            fs::read(&first_cert).unwrap(),
            fs::read(&second_cert).unwrap(),
        ]
        .concat(),
    )
    .unwrap();
    let expected = CertificateDer::pem_file_iter(&chain)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let keys = first.path().join("keys.pem");
    // Certificates in the key file are skipped; key format does not change
    // first-match selection. The second key does not match the leaf.
    fs::write(
        &keys,
        [
            fs::read(&second_cert).unwrap(),
            fs::read(&first_key).unwrap(),
            fs::read(&second_key).unwrap(),
        ]
        .concat(),
    )
    .unwrap();
    let server = build_server_config(&chain, &keys).expect("first key matches leaf");
    let client = client_config_trusting_cert(&chain).expect("all pinned roots should load");
    assert_eq!(handshake(server, client).await, expected);
    fs::write(
        &keys,
        [
            fs::read(&second_key).unwrap(),
            fs::read(&first_key).unwrap(),
        ]
        .concat(),
    )
    .unwrap();
    assert!(build_server_config(&chain, &keys)
        .unwrap_err()
        .contains("rustls rejected cert/key"));
    // A valid first certificate must not hide a malformed later block.
    fs::write(
        &chain,
        [
            fs::read(&first_cert).unwrap(),
            b"-----BEGIN CERTIFICATE-----\n!invalid!\n-----END CERTIFICATE-----\n".to_vec(),
        ]
        .concat(),
    )
    .unwrap();
    assert!(build_server_config(&chain, &first_key)
        .unwrap_err()
        .contains("parse cert_file"));
    assert!(client_config_trusting_cert(&chain)
        .unwrap_err()
        .contains("parse cert_file"));
}

#[test]
fn server_and_pinned_client_reject_empty_or_malformed_certificates() {
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("unused-key.pem");
    for (pem, expected_error) in [
        ("", "no certificates"),
        ("not a PEM certificate", "no certificates"),
        (
            "-----BEGIN CERTIFICATE-----\n!invalid!\n-----END CERTIFICATE-----\n",
            "parse cert_file",
        ),
        ("-----BEGIN CERTIFICATE-----\nAQID\n", "parse cert_file"),
    ] {
        fs::write(&cert, pem).unwrap();
        assert!(build_server_config(&cert, &key)
            .unwrap_err()
            .contains(expected_error));
        assert!(client_config_trusting_cert(&cert)
            .unwrap_err()
            .contains(expected_error));
    }
}

#[test]
fn server_rejects_empty_or_malformed_private_keys() {
    let dir = tempfile::tempdir().unwrap();
    let Some((cert, _)) = fixture(dir.path(), false) else {
        eprintln!("skipping: openssl unavailable");
        return;
    };
    let key = dir.path().join("bad-key.pem");
    for (pem, expected_error) in [
        ("", "no private key"),
        ("not a PEM key", "no private key"),
        (
            "-----BEGIN PRIVATE KEY-----\n!invalid!\n-----END PRIVATE KEY-----\n",
            "parse key_file",
        ),
        ("-----BEGIN EC PRIVATE KEY-----\nAQID\n", "parse key_file"),
    ] {
        fs::write(&key, pem).unwrap();
        assert!(build_server_config(&cert, &key)
            .unwrap_err()
            .contains(expected_error));
    }
}
