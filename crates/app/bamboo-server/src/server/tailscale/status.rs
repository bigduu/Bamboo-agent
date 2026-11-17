use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::cli::Cli;

pub(super) const HTTPS_PORTS: [u16; 3] = [443, 8443, 10000];

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum State {
    NotInstalled,
    Unavailable,
    Disconnected,
    LoginRequired,
    DeviceApprovalRequired,
    PasswordRequired,
    HttpsRequired,
    Ready,
    Starting,
    Active,
    PortInUse,
    TailnetChanged,
    ServeFailed,
}

#[derive(Serialize)]
pub(super) struct Status {
    pub state: State,
    pub enabled: bool,
    pub tailnet: Option<String>,
    pub url: Option<String>,
    pub action_url: Option<String>,
}

impl Status {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            state: State::Unavailable,
            enabled,
            tailnet: None,
            url: None,
            action_url: None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub(super) struct ClientStatus {
    backend_state: String,
    #[serde(rename = "AuthURL")]
    auth_url: String,
    #[serde(rename = "Self")]
    node: Option<Node>,
    current_tailnet: Option<Tailnet>,
    cert_domains: Option<Vec<String>>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub(super) struct Node {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "DNSName")]
    dns_name: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Tailnet {
    #[serde(rename = "MagicDNSSuffix")]
    suffix: String,
    #[serde(rename = "MagicDNSEnabled")]
    magic_dns: bool,
}

fn hostname(value: &str) -> &str {
    value.trim_end_matches('.')
}

fn is_tailscale_domain(value: &str) -> bool {
    value.to_ascii_lowercase().ends_with(".ts.net")
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn is_login_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some("login.tailscale.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.path().starts_with("/a/")
    })
}

impl ClientStatus {
    pub(super) async fn read(cli: &Cli) -> Self {
        cli.output(&["status", "--json", "--peers=false"])
            .await
            .ok()
            .and_then(|output| serde_json::from_slice(&output.stdout).ok())
            .unwrap_or_default()
    }

    pub(super) async fn connect(cli: &Cli) -> Result<Option<String>, ()> {
        let output = cli
            .output(&["up", "--json", "--timeout=3s"])
            .await
            .map_err(|_| ())?;
        // `up` can emit an authorization URL and later a terminal state as
        // separate JSON documents. A timeout still leaves a usable login link.
        let auth_url = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<Self>()
            .filter_map(Result::ok)
            .find_map(|status| is_login_url(&status.auth_url).then_some(status.auth_url));
        if output.status.success() || auth_url.is_some() {
            Ok(auth_url)
        } else {
            Err(())
        }
    }

    pub(super) fn auth_url(&self) -> Option<&str> {
        is_login_url(&self.auth_url).then_some(self.auth_url.as_str())
    }

    pub(super) fn set_auth_url(&mut self, url: Option<String>) {
        self.auth_url = url.unwrap_or_default();
    }

    pub(super) fn state(&self) -> State {
        match self.backend_state.as_str() {
            "Running" => State::Ready,
            "Stopped" => State::Disconnected,
            "NeedsLogin" | "NoState" => State::LoginRequired,
            "NeedsMachineAuth" => State::DeviceApprovalRequired,
            _ => State::Unavailable,
        }
    }

    pub(super) fn can_connect(&self) -> bool {
        matches!(self.state(), State::LoginRequired | State::Disconnected)
    }

    pub(super) fn node(&self) -> Option<&Node> {
        self.node
            .as_ref()
            .filter(|node| !node.id.is_empty() && is_tailscale_domain(hostname(&node.dns_name)))
    }

    pub(super) fn tailnet(&self) -> Option<&str> {
        self.current_tailnet
            .as_ref()
            .map(|tailnet| hostname(&tailnet.suffix))
            .filter(|name| is_tailscale_domain(name))
    }

    pub(super) fn https_ready(&self, node: &Node) -> bool {
        self.current_tailnet
            .as_ref()
            .is_some_and(|tailnet| tailnet.magic_dns)
            && self.tailnet().is_some_and(|tailnet| {
                hostname(&node.dns_name)
                    .split_once('.')
                    .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(tailnet))
            })
            && self.cert_domains.as_ref().is_some_and(|domains| {
                domains
                    .iter()
                    .any(|domain| hostname(domain).eq_ignore_ascii_case(hostname(&node.dns_name)))
            })
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Share {
    node_id: String,
    dns_name: String,
    pub port: u16,
}

impl Share {
    pub(super) fn new(node: &Node, port: u16) -> Self {
        Self {
            node_id: node.id.clone(),
            dns_name: hostname(&node.dns_name).to_ascii_lowercase(),
            port,
        }
    }

    pub(super) fn valid(&self) -> bool {
        !self.node_id.is_empty()
            && is_tailscale_domain(&self.dns_name)
            && HTTPS_PORTS.contains(&self.port)
    }

    pub(super) fn belongs_to(&self, node: &Node) -> bool {
        self.node_id == node.id && self.dns_name.eq_ignore_ascii_case(hostname(&node.dns_name))
    }

    fn host_port(&self) -> String {
        format!("{}:{}", self.dns_name, self.port)
    }

    pub(super) fn origin(&self) -> String {
        if self.port == 443 {
            format!("https://{}", self.dns_name)
        } else {
            format!("https://{}", self.host_port())
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub(super) struct ServeConfig {
    #[serde(rename = "TCP")]
    tcp: BTreeMap<u16, PortHandler>,
    web: BTreeMap<String, Website>,
    allow_funnel: BTreeMap<String, bool>,
    foreground: BTreeMap<String, Self>,
}

#[derive(Deserialize)]
struct PortHandler {
    #[serde(rename = "HTTPS", default)]
    https: bool,
}

#[derive(Deserialize)]
struct Website {
    #[serde(rename = "Handlers")]
    handlers: BTreeMap<String, Handler>,
}

#[derive(Deserialize)]
struct Handler {
    #[serde(rename = "Proxy")]
    proxy: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum PortUse {
    Free,
    Owned,
    Busy,
}

impl ServeConfig {
    pub(super) async fn read(cli: &Cli) -> Option<Self> {
        let output = cli.output(&["serve", "status", "--json"]).await.ok()?;
        if !output.status.success() {
            return None;
        }
        // The CLI emits a null config when Serve has never been configured.
        Some(
            serde_json::from_slice::<Option<Self>>(&output.stdout)
                .ok()?
                .unwrap_or_default(),
        )
    }

    fn uses_port(&self, port: u16) -> bool {
        let has_port = |authority: &str| {
            authority
                .rsplit_once(':')
                .and_then(|(_, value)| value.parse::<u16>().ok())
                == Some(port)
        };
        self.tcp.contains_key(&port)
            || self.web.keys().any(|authority| has_port(authority))
            || self
                .allow_funnel
                .iter()
                .any(|(authority, enabled)| *enabled && has_port(authority))
    }

    pub(super) fn port_use(&self, share: &Share, backend: &str) -> PortUse {
        if self.uses_port(share.port) {
            return PortUse::Busy;
        }
        let mut use_of_port = PortUse::Free;
        let host_port = share.host_port();
        for session in self
            .foreground
            .values()
            .filter(|session| session.uses_port(share.port))
        {
            let matches_owned = session
                .tcp
                .get(&share.port)
                .is_some_and(|handler| handler.https)
                && session.web.get(&host_port).is_some_and(|site| {
                    site.handlers.len() == 1
                        && site
                            .handlers
                            .get("/")
                            .and_then(|handler| handler.proxy.as_deref())
                            == Some(backend)
                })
                && !session.allow_funnel.values().any(|enabled| *enabled);
            if use_of_port != PortUse::Free || !matches_owned {
                return PortUse::Busy;
            }
            use_of_port = PortUse::Owned;
        }
        use_of_port
    }
}
