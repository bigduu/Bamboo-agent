use serde::Deserialize;
use std::collections::HashMap;

use bamboo_llm::Config;

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalAgentProfile {
    pub agent_id: String,
    pub protocol: ExternalAgentProtocol,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_card_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub permission_profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill: Option<String>,
    #[serde(default)]
    pub allow_non_streaming_fallback: bool,
    /// Actor protocol only: path to the worker binary to spawn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_bin: Option<String>,
    /// Actor protocol only: fixed arguments passed to the worker binary
    /// (e.g. `["subagent-worker"]` when `worker_bin` is the main `bamboo`
    /// binary). Per-child data never rides here — it goes in the spec.
    #[serde(default)]
    pub worker_args: Vec<String>,
    /// Actor protocol only: directory the worker self-registers into
    /// (Tier-1 file fabric). Defaults to a per-user temp dir when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fabric_dir: Option<String>,
    /// Actor protocol only: which engine the worker runs.
    /// `"bamboo_runtime"` (default) for the real agent loop, `"echo"` for a
    /// dependency-free smoke run through the whole chain, `"claude_code"` to
    /// drive the official Claude Code CLI (see the `claude_code_*` fields
    /// below).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    /// `executor = "claude_code"` only: override the `claude` executable.
    /// `None` runs `claude` resolved from `PATH`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_binary: Option<String>,
    /// `executor = "claude_code"` only: `--model` override. `None` omits the
    /// flag (CLI default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_model: Option<String>,
    /// `executor = "claude_code"` only: `--permission-mode` override. `None`
    /// still passes an EXPLICIT `default` to the CLI (issue #443 — the
    /// headless stream-json default is `auto`, which self-approves every
    /// tool and never asks); it does not mean "omit the flag".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_permission_mode: Option<String>,
    /// `executor = "claude_code"` only: `true` lets the child inherit the
    /// invoking user's `~/.claude` MCP servers/skills/settings. `false`/unset
    /// (the default) isolates it (`--strict-mcp-config` +
    /// `--setting-sources project`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_inherit_user_config: Option<bool>,
    /// `executor = "claude_code"` only: extra env var NAMES forwarded
    /// verbatim from this process's env to the child, on top of the fixed
    /// HOME/PATH/SHELL/TERM/LANG/LC_*/TMPDIR/USER/LOGNAME allowlist.
    /// Forwarding `ANTHROPIC_API_KEY` here is an explicit opt-in that flips
    /// billing from the CLI's own subscription auth to the API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_forward_env: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub enum ExternalAgentProtocol {
    #[serde(rename = "a2a_jsonrpc")]
    A2aJsonRpc,
    /// Independent actor process speaking the `bamboo-subagent` WebSocket protocol.
    #[serde(rename = "actor", alias = "subprocess")]
    Actor,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubagentRouting {
    pub runtime: String, // "external" or "bamboo"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

/// Parse external agent profiles from `Config.extra["externalAgents"]`.
pub fn parse_external_agents(config: &Config) -> HashMap<String, ExternalAgentProfile> {
    let Some(value) = config.extra.get("externalAgents") else {
        return HashMap::new();
    };

    match serde_json::from_value(value.clone()) {
        Ok(agents) => agents,
        Err(error) => {
            tracing::error!(
                "Invalid externalAgents config; external agent routing disabled: {}",
                error
            );
            HashMap::new()
        }
    }
}

/// Parse subagent routing table from `Config.extra["subagentRouting"]`.
pub fn parse_subagent_routing(config: &Config) -> HashMap<String, SubagentRouting> {
    let Some(value) = config.extra.get("subagentRouting") else {
        return HashMap::new();
    };

    match serde_json::from_value(value.clone()) {
        Ok(routing) => routing,
        Err(error) => {
            tracing::error!(
                "Invalid subagentRouting config; external subagent routing disabled: {}",
                error
            );
            HashMap::new()
        }
    }
}

/// Synthetic agent id for the built-in local actor worker (friendly
/// `subagents.runtime = "actor"` path; no `externalAgents` entry needed).
pub const LOCAL_ACTOR_AGENT_ID: &str = "local-actor";

/// Resolve runtime metadata for a subagent_type based on config routing.
///
/// Sub-agents always run as actors (the in-process runtime was removed), so the
/// default for every type is the built-in **local actor** worker. The only
/// exception is the legacy expert `subagentRouting[type]` table, which can still
/// pin a specific role to an external agent (e.g. a remote `a2a_jsonrpc` service
/// or a custom actor profile). A legacy `runtime: "bamboo"` entry — which used
/// to mean in-process — now falls through to the local-actor default.
pub fn resolve_runtime_metadata(config: &Config, subagent_type: &str) -> HashMap<String, String> {
    let local_actor_metadata = || {
        HashMap::from([
            ("runtime.kind".to_string(), "external".to_string()),
            ("external.protocol".to_string(), "actor".to_string()),
            (
                "external.agent_id".to_string(),
                LOCAL_ACTOR_AGENT_ID.to_string(),
            ),
        ])
    };

    let routing = parse_subagent_routing(config);
    let agents = parse_external_agents(config);

    // Legacy expert routing: pin a specific role to an external agent.
    if let Some(route) = routing.get(subagent_type) {
        if route.runtime == "external" {
            let mut metadata = HashMap::new();
            metadata.insert("runtime.kind".to_string(), "external".to_string());

            if let Some(agent_id) = &route.agent_id {
                metadata.insert("external.agent_id".to_string(), agent_id.clone());

                if let Some(profile) = agents.get(agent_id) {
                    metadata.insert(
                        "external.protocol".to_string(),
                        match profile.protocol {
                            ExternalAgentProtocol::A2aJsonRpc => "a2a_jsonrpc".to_string(),
                            ExternalAgentProtocol::Actor => "actor".to_string(),
                        },
                    );
                    metadata.insert(
                        "external.permission_profile".to_string(),
                        profile.permission_profile.clone(),
                    );
                    if let Some(url) = &profile.agent_card_url {
                        metadata.insert("external.agent_card_url".to_string(), url.clone());
                    }
                }
            }

            return metadata;
        }
        // `runtime: "bamboo"` (or anything non-external): fall through to the
        // local-actor default below.
    }

    // Default: the built-in local actor worker.
    local_actor_metadata()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_external_agents_from_config_extra() {
        let mut config = Config::default();
        config.extra.insert(
            "externalAgents".to_string(),
            serde_json::json!({
                "remote_impl": {
                    "agent_id": "remote_impl",
                    "protocol": "a2a_jsonrpc",
                    "agent_card_url": "https://example.com/agent-card.json",
                    "auth_ref": "REMOTE_IMPL_TOKEN",
                    "permission_profile": "remote_limited"
                }
            }),
        );

        let agents = parse_external_agents(&config);
        assert_eq!(agents.len(), 1);
        let profile = agents.get("remote_impl").unwrap();
        assert_eq!(profile.agent_id, "remote_impl");
        assert!(matches!(
            profile.protocol,
            ExternalAgentProtocol::A2aJsonRpc
        ));
        assert_eq!(
            profile.agent_card_url,
            Some("https://example.com/agent-card.json".to_string())
        );
        assert_eq!(profile.auth_ref, Some("REMOTE_IMPL_TOKEN".to_string()));
    }

    #[test]
    fn parse_subagent_routing_from_config_extra() {
        let mut config = Config::default();
        config.extra.insert(
            "subagentRouting".to_string(),
            serde_json::json!({
                "impl": { "runtime": "external", "agent_id": "remote_impl" },
                "plan": { "runtime": "bamboo" }
            }),
        );

        let routing = parse_subagent_routing(&config);
        assert_eq!(routing.len(), 2);
        assert_eq!(routing.get("impl").unwrap().runtime, "external");
        assert_eq!(
            routing.get("impl").unwrap().agent_id,
            Some("remote_impl".to_string())
        );
        assert_eq!(routing.get("plan").unwrap().runtime, "bamboo");
    }

    #[test]
    fn resolve_runtime_metadata_routes_impl_to_external() {
        let mut config = Config::default();
        config.extra.insert(
            "externalAgents".to_string(),
            serde_json::json!({
                "remote_impl": {
                    "agent_id": "remote_impl",
                    "protocol": "a2a_jsonrpc",
                    "permission_profile": "remote_limited"
                }
            }),
        );
        config.extra.insert(
            "subagentRouting".to_string(),
            serde_json::json!({
                "impl": { "runtime": "external", "agent_id": "remote_impl" }
            }),
        );

        let metadata = resolve_runtime_metadata(&config, "impl");
        assert_eq!(metadata.get("runtime.kind"), Some(&"external".to_string()));
        assert_eq!(
            metadata.get("external.protocol"),
            Some(&"a2a_jsonrpc".to_string())
        );
        assert_eq!(
            metadata.get("external.agent_id"),
            Some(&"remote_impl".to_string())
        );
    }

    #[test]
    fn unknown_type_defaults_to_local_actor() {
        // Sub-agents always run as actors: a type with no expert routing
        // resolves to the built-in local actor worker.
        let mut config = Config::default();
        config.subagents = Default::default();
        let metadata = resolve_runtime_metadata(&config, "unknown");
        assert_eq!(metadata.get("runtime.kind"), Some(&"external".to_string()));
        assert_eq!(
            metadata.get("external.protocol"),
            Some(&"actor".to_string())
        );
        assert_eq!(
            metadata.get("external.agent_id"),
            Some(&LOCAL_ACTOR_AGENT_ID.to_string())
        );
    }

    #[test]
    fn every_type_defaults_to_local_actor() {
        let config = Config::default();
        let metadata = resolve_runtime_metadata(&config, "researcher");
        assert_eq!(metadata.get("runtime.kind"), Some(&"external".to_string()));
        assert_eq!(
            metadata.get("external.protocol"),
            Some(&"actor".to_string())
        );
        assert_eq!(
            metadata.get("external.agent_id"),
            Some(&LOCAL_ACTOR_AGENT_ID.to_string())
        );
    }

    #[test]
    fn legacy_bamboo_routing_falls_through_to_local_actor() {
        // `runtime: "bamboo"` used to mean in-process; with in-process removed
        // it falls through to the local-actor default.
        let mut config = Config::default();
        config.extra.insert(
            "subagentRouting".to_string(),
            serde_json::json!({
                "plan": { "runtime": "bamboo" }
            }),
        );

        let metadata = resolve_runtime_metadata(&config, "plan");
        assert_eq!(
            metadata.get("external.agent_id"),
            Some(&LOCAL_ACTOR_AGENT_ID.to_string())
        );
    }

    #[test]
    fn legacy_external_routing_selects_remote_agent() {
        let mut config = Config::default();
        config.extra.insert(
            "externalAgents".to_string(),
            serde_json::json!({
                "remote_impl": {
                    "agent_id": "remote_impl",
                    "protocol": "a2a_jsonrpc",
                    "permission_profile": "remote_limited"
                }
            }),
        );
        config.extra.insert(
            "subagentRouting".to_string(),
            serde_json::json!({
                "impl": { "runtime": "external", "agent_id": "remote_impl" }
            }),
        );

        // explicit legacy per-type external routing still selects the remote agent
        let metadata = resolve_runtime_metadata(&config, "impl");
        assert_eq!(
            metadata.get("external.agent_id"),
            Some(&"remote_impl".to_string())
        );
    }
}
