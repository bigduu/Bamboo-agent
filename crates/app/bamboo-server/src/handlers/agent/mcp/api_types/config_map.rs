use super::response::{HeaderConfigApi, McpServerConfigApi, TransportConfigApi};

fn mask() -> String {
    "****...****".to_string()
}

pub(super) fn to_api_config(server: &bamboo_mcp::McpServerConfig) -> McpServerConfigApi {
    let transport = match &server.transport {
        bamboo_mcp::TransportConfig::Stdio(stdio) => {
            // Never return plaintext; only return keys so users can see which env vars exist.
            let mut keys: Vec<String> = stdio.env_encrypted.keys().cloned().collect();
            keys.extend(stdio.env.keys().cloned());
            keys.sort();
            keys.dedup();

            let env = keys.into_iter().map(|key| (key, mask())).collect();

            TransportConfigApi::Stdio {
                command: stdio.command.clone(),
                args: stdio.args.clone(),
                cwd: stdio.cwd.clone(),
                env,
                startup_timeout_ms: stdio.startup_timeout_ms,
            }
        }
        bamboo_mcp::TransportConfig::Sse(sse) => TransportConfigApi::Sse {
            url: sse.url.clone(),
            headers: sse
                .headers
                .iter()
                .map(|header| HeaderConfigApi {
                    name: header.name.clone(),
                    value: mask(),
                })
                .collect(),
            connect_timeout_ms: sse.connect_timeout_ms,
        },
        bamboo_mcp::TransportConfig::StreamableHttp(sh) => TransportConfigApi::StreamableHttp {
            url: sh.url.clone(),
            headers: sh
                .headers
                .iter()
                .map(|header| HeaderConfigApi {
                    name: header.name.clone(),
                    value: mask(),
                })
                .collect(),
            connect_timeout_ms: sh.connect_timeout_ms,
        },
    };

    McpServerConfigApi {
        id: server.id.clone(),
        name: server.name.clone(),
        enabled: server.enabled,
        transport,
        request_timeout_ms: server.request_timeout_ms,
        healthcheck_interval_ms: server.healthcheck_interval_ms,
        reconnect: server.reconnect.clone(),
        allowed_tools: server.allowed_tools.clone(),
        denied_tools: server.denied_tools.clone(),
    }
}
