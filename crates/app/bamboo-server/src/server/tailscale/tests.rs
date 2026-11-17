use actix_web::test::TestRequest;
use serde_json::json;
use tempfile::tempdir;

use super::{status::ServeConfig, *};

const HOST: &str = "bamboo.example.ts.net";
const TARGET: &str = "http://127.0.0.1:54321";

fn share() -> Share {
    serde_json::from_value(json!({"node_id":"node1","dns_name":HOST,"port":8443})).unwrap()
}

#[test]
fn only_the_exact_private_foreground_route_can_be_adopted() {
    let rule = json!({
        "TCP": {"8443": {"HTTPS": true}},
        "Web": {"bamboo.example.ts.net:8443": {"Handlers": {"/": {"Proxy": TARGET}}}}
    });
    let check = |value| {
        serde_json::from_value::<ServeConfig>(value)
            .unwrap()
            .port_use(&share(), TARGET)
    };
    assert_eq!(check(json!({})), PortUse::Free);
    assert_eq!(check(rule.clone()), PortUse::Busy);
    assert_eq!(
        check(json!({"Foreground": {"one": rule.clone()}})),
        PortUse::Owned
    );
    assert_eq!(
        check(json!({"Foreground": {"one": rule.clone(), "two": rule.clone()}})),
        PortUse::Busy
    );
    assert_eq!(
        check(json!({
            "AllowFunnel": {"bamboo.example.ts.net:8443": true},
            "Foreground": {"one": rule.clone()}
        })),
        PortUse::Busy
    );
    let wrong_target = check(json!({"Foreground": {"one": {
        "TCP": {"8443": {"HTTPS": true}},
        "Web": {"bamboo.example.ts.net:8443": {"Handlers": {"/": {"Proxy": "http://127.0.0.1:9562"}}}}
    }}}));
    assert_eq!(wrong_target, PortUse::Busy);
}

#[test]
fn login_urls_must_belong_to_the_official_tailscale_login_flow() {
    for url in [
        "https://login.tailscale.com.evil/a/123",
        "https://login.tailscale.com@evil.test/a/123",
        "http://login.tailscale.com/a/123",
        "https://login.tailscale.com/admin/dns",
    ] {
        let status: ClientStatus =
            serde_json::from_value(json!({"BackendState":"NeedsLogin","AuthURL":url})).unwrap();
        assert_eq!(status.auth_url(), None);
    }
    let url = "https://login.tailscale.com/a/123";
    let status: ClientStatus =
        serde_json::from_value(json!({"BackendState":"NeedsLogin","AuthURL":url})).unwrap();
    assert_eq!(status.auth_url(), Some(url));
}

#[actix_web::test]
async fn ingress_requires_private_serve_identity_and_the_pinned_https_origin() {
    let home = tempdir().unwrap();
    let (_, access) = TailscaleAccess::bind(home.path()).await.unwrap();
    access.publish(Some(share()));
    let proxied = || {
        TestRequest::default()
            .peer_addr("127.0.0.1:22222".parse().unwrap())
            .insert_header(("host", "bamboo.example.ts.net:8443"))
            .insert_header(("x-forwarded-host", "bamboo.example.ts.net:8443"))
            .insert_header(("x-forwarded-proto", "https"))
    };
    let private = || proxied().insert_header(("tailscale-user-login", "owner@example.net"));
    assert!(access.accept(&private().to_http_request()).is_some());
    assert!(access.accept(&proxied().to_http_request()).is_none());
    assert!(access
        .accept(
            &private()
                .insert_header(("tailscale-funnel-request", "?1"))
                .to_http_request()
        )
        .is_none());
    assert!(access
        .accept(
            &private()
                .insert_header(("origin", "https://other.example.org"))
                .to_http_request()
        )
        .is_none());
    assert!(access
        .accept(
            &private()
                .insert_header(("x-forwarded-host", "other.example.ts.net:8443"))
                .to_http_request()
        )
        .is_none());
    access.publish(None);
    assert!(access.accept(&private().to_http_request()).is_none());
}
