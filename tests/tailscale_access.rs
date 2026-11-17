#![cfg(unix)]

//! Runs the real Bamboo entry point and access middleware against a local
//! Tailscale CLI fixture. The fixture models daemon state and Serve ownership.

use std::{
    fs,
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use reqwest::{header, Client, RequestBuilder, StatusCode};
use serde_json::{json, Value};
use tokio::{process::Child, time};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};

const HOST: &str = "bamboo.fixture.ts.net:8443";
const ORIGIN: &str = "https://bamboo.fixture.ts.net:8443";
const PASSWORD: &str = "synthetic-access-password";
const CLI: &str = r#"#!/bin/sh
case "$*" in
  "status --json --peers=false")
    cat "$HOME/client-status.json" ;;
  "up --json --timeout=3s")
    printf '%s\n' '{"BackendState":"NeedsLogin","AuthURL":"https://login.tailscale.com/a/test-fixture"}'
    exit 1 ;;
  "serve status --json")
    base='"TCP":{"443":{"HTTPS":true}}'
    if read -r pid port target < "$HOME/registration" && kill -0 "$pid" 2>/dev/null; then
      printf '{%s,"Foreground":{"fixture":{"TCP":{"%s":{"HTTPS":true}},"Web":{"bamboo.fixture.ts.net:%s":{"Handlers":{"/":{"Proxy":"%s"}}}}}}}\n' "$base" "$port" "$port" "$target"
    else
      printf '{%s}\n' "$base"
    fi ;;
  "serve --bg=false --https="*)
    printf '%s %s %s\n' "$$" "${3#--https=}" "$4" > "$HOME/registration"
    exec /bin/sleep 300 ;;
  *) exit 2 ;;
esac
"#;

fn connect_client(fixture: &Path, node: &str, https: bool) {
    let domains = if https {
        vec!["bamboo.fixture.ts.net"]
    } else {
        vec![]
    };
    fs::write(
        fixture.join("client-status.json"),
        json!({
            "BackendState": "Running",
            "Self": {"ID": node, "DNSName": "bamboo.fixture.ts.net."},
            "CurrentTailnet": {"MagicDNSSuffix": "fixture.ts.net", "MagicDNSEnabled": true},
            "CertDomains": domains
        })
        .to_string(),
    )
    .unwrap();
}

async fn start(fixture: &Path, client: &Client) -> (Child, String) {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut bamboo = tokio::process::Command::new(env!("CARGO_BIN_EXE_bamboo"));
    let log = fixture.join("bamboo-startup.log");
    let output = fs::File::create(&log).unwrap();
    let mut child = bamboo
        .env_clear()
        .env("HOME", fixture)
        .env("PATH", format!("{}:/usr/bin:/bin", fixture.display()))
        .env("BAMBOO_JIANDU_DATA_DIR", fixture.join("jiandu"))
        .args([
            "serve",
            "--bind",
            "127.0.0.1",
            "--workers",
            "1",
            "--port",
            &port.to_string(),
        ])
        .arg("--data-dir")
        .arg(fixture.join("data"))
        .arg("--static-dir")
        .arg(fixture.join("web"))
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let ready = time::timeout(Duration::from_secs(60), async {
        loop {
            if child.try_wait().unwrap().is_some() {
                return false;
            }
            if client
                .get(format!("{base}/api/v1/health"))
                .send()
                .await
                .is_ok_and(|res| res.status().is_success())
            {
                return true;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert_eq!(
        ready.ok(),
        Some(true),
        "local Bamboo HTTP readiness: {:?}",
        fs::read_to_string(log)
    );
    (child, base)
}

async fn json_response(request: RequestBuilder) -> Value {
    let response = request.send().await.unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();
    assert!(status.is_success(), "{status} {body}");
    body
}

async fn access_status(client: &Client, base: &str) -> Value {
    json_response(client.get(format!("{base}/api/v1/bamboo/tailscale"))).await
}

async fn set_access(client: &Client, base: &str, action: &str) -> Value {
    json_response(
        client
            .post(format!("{base}/api/v1/bamboo/tailscale"))
            .json(&json!({"action":action})),
    )
    .await
}

async fn wait_for_share(client: &Client, base: &str) {
    time::timeout(Duration::from_secs(10), async {
        loop {
            let status = access_status(client, base).await;
            if status["state"] == "active" {
                assert_eq!(status["url"], format!("{ORIGIN}/"));
                break;
            }
            assert!(status["enabled"].as_bool().unwrap(), "{status}");
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("verified foreground registration");
}

fn registration(fixture: &Path) -> (String, String) {
    let text = fs::read_to_string(fixture.join("registration")).unwrap();
    let parts = text.split_whitespace().collect::<Vec<_>>();
    assert_eq!(parts[1], "8443", "the existing port 443 is reserved");
    (parts[0].into(), parts[2].into())
}

async fn wait_for_worker_exit(pid: &str) {
    time::timeout(Duration::from_secs(8), async {
        while Command::new("/bin/kill")
            .args(["-0", pid])
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
        {
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("foreground Tailscale CLI exits when its owner closes");
}

fn proxied(request: RequestBuilder) -> RequestBuilder {
    request
        .header(header::HOST, HOST)
        .header("x-forwarded-host", HOST)
        .header("x-forwarded-proto", "https")
        .header("tailscale-user-login", "tester@example.net")
}

#[tokio::test]
async fn managed_tailscale_access_survives_restart_and_gates_the_full_http_app() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = temp.path();
    fs::create_dir(fixture.join("web")).unwrap();
    fs::write(
        fixture.join("web/index.html"),
        "Lotus through the same Bamboo server",
    )
    .unwrap();
    fs::write(fixture.join("tailscale"), CLI).unwrap();
    fs::set_permissions(fixture.join("tailscale"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        fixture.join("client-status.json"),
        r#"{"BackendState":"NeedsLogin"}"#,
    )
    .unwrap();
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let (mut bamboo, local) = start(fixture, &client).await;
    assert_eq!(
        access_status(&client, &local).await["state"],
        "login_required"
    );
    assert_eq!(
        set_access(&client, &local, "connect").await["action_url"],
        "https://login.tailscale.com/a/test-fixture"
    );
    connect_client(fixture, "device-1", false);
    assert_eq!(
        set_access(&client, &local, "enable").await["state"],
        "password_required"
    );
    let password_path = "/api/v1/bamboo/access/password";
    let revision = json_response(client.get(format!("{local}/api/v1/bamboo/access/status"))).await
        ["revision"]
        .clone();
    let password = json_response(
        client
            .post(format!("{local}{password_path}"))
            .json(&json!({"expected_revision": revision, "new_password": PASSWORD})),
    )
    .await;
    assert_eq!(password["password_enabled"], true);
    assert_eq!(
        set_access(&client, &local, "enable").await["state"],
        "https_required"
    );
    connect_client(fixture, "device-1", true);

    let denied = client
        .post(format!("{local}/api/v1/bamboo/tailscale"))
        .header(header::ORIGIN, "https://unrelated.example")
        .json(&json!({"action":"enable"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        set_access(&client, &local, "enable").await["state"],
        "starting"
    );
    wait_for_share(&client, &local).await;
    let (first_pid, ingress) = registration(fixture);

    let preference = fixture.join("data/tailscale-access.json");
    let saved_preference = preference.with_extension("saved");
    fs::rename(&preference, &saved_preference).unwrap();
    fs::create_dir(&preference).unwrap();
    let failed_disable = client
        .post(format!("{local}/api/v1/bamboo/tailscale"))
        .json(&json!({"action": "disable"}))
        .send()
        .await
        .unwrap();
    assert_eq!(failed_disable.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(access_status(&client, &local).await["state"], "active");
    fs::remove_dir(&preference).unwrap();
    fs::rename(&saved_preference, &preference).unwrap();

    assert_eq!(
        client.get(&ingress).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        proxied(client.get(&ingress))
            .header("tailscale-funnel-request", "?1")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        proxied(client.get(&ingress))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "Lotus through the same Bamboo server"
    );
    let projects = format!("{ingress}/api/v1/projects");
    assert_eq!(
        proxied(client.get(&projects))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let bootstrap = json_response(proxied(client.get(format!("{ingress}/api/v1/bootstrap")))).await;
    assert_eq!(bootstrap["auth"]["request_state"], "unauthenticated");
    let verify = |ip: &str, password: &str| {
        proxied(client.post(format!("{ingress}/api/v1/bamboo/access/verify")))
            .header(header::ORIGIN, ORIGIN)
            .header("x-forwarded-for", ip)
            .json(&json!({"password": password}))
            .send()
    };
    for _ in 0..5 {
        assert_eq!(
            verify("100.64.0.2", "incorrect").await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        verify("100.64.0.2", PASSWORD).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let verify = verify("100.64.0.3", PASSWORD).await.unwrap();
    assert_eq!(verify.status(), StatusCode::OK);
    let cookie = verify.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(cookie.to_ascii_lowercase().contains("; secure"));
    let credential = cookie.split(';').next().unwrap();
    assert_eq!(
        proxied(client.get(&projects))
            .header(header::COOKIE, credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        proxied(client.get(format!("{ingress}/api/v1/bamboo/tailscale")))
            .header(header::COOKIE, credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let ws_url = ingress.replacen("http:", "ws:", 1) + "/v2/stream";
    let mut request = ws_url.into_client_request().unwrap();
    for (name, value) in [
        ("host", HOST),
        ("x-forwarded-host", HOST),
        ("x-forwarded-proto", "https"),
        ("tailscale-user-login", "tester@example.net"),
        ("origin", ORIGIN),
        ("cookie", credential),
        ("sec-websocket-protocol", "bamboo.v2"),
    ] {
        request.headers_mut().insert(name, value.parse().unwrap());
    }
    let (mut ws, _) = connect_async(request).await.unwrap();
    ws.send(Message::Text(r#"{"type":"hello"}"#.into()))
        .await
        .unwrap();
    let frame = time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap()["type"],
        "welcome"
    );
    connect_client(fixture, "different-device", true);
    assert_eq!(
        access_status(&client, &local).await["state"],
        "tailnet_changed"
    );
    time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(frame)) = ws.next().await {
            if frame.is_close() {
                break;
            }
        }
    })
    .await
    .expect("an existing WebSocket closes when the share is revoked");
    wait_for_worker_exit(&first_pid).await;
    connect_client(fixture, "device-1", true);
    set_access(&client, &local, "enable").await;
    wait_for_share(&client, &local).await;
    let (pid_before_restart, _) = registration(fixture);
    bamboo.kill().await.unwrap();
    wait_for_worker_exit(&pid_before_restart).await;

    let (_restarted, local) = start(fixture, &client).await;
    time::timeout(Duration::from_secs(12), async {
        loop {
            let (pid, ingress) = registration(fixture);
            if pid != pid_before_restart
                && proxied(client.get(ingress))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("saved share is served after restart without opening settings");
    wait_for_share(&client, &local).await;
    let (restarted_pid, ingress) = registration(fixture);
    let revision = json_response(client.get(format!("{local}/api/v1/bamboo/access/status"))).await
        ["revision"]
        .clone();
    json_response(
        client
            .post(format!("{local}{password_path}"))
            .json(&json!({"expected_revision":revision,"action":"clear"})),
    )
    .await;
    assert!(!proxied(client.get(format!("{ingress}/api/v1/projects")))
        .header(header::COOKIE, credential)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    assert_eq!(
        set_access(&client, &local, "disable").await["enabled"],
        false
    );
    wait_for_worker_exit(&restarted_pid).await;
    assert!(!fixture.join("data/tailscale-access.json").exists());
}
