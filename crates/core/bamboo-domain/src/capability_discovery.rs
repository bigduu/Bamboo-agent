//! Provider-neutral capability discovery contracts.
//!
//! Provider adapters lower these logical values to their native discovery
//! protocol. The contract deliberately contains metadata references only: no
//! tool parameter schemas, Skill instructions/resources, Workflow definitions,
//! filesystem paths, or credentials belong here.

use serde::{Deserialize, Serialize};

/// Logical name of Bamboo's single capability discovery gateway.
pub const DISCOVER_CAPABILITY_NAME: &str = "discover";

/// Maximum number of Unicode scalar values accepted in a discovery query.
pub const MAX_DISCOVERY_QUERY_CHARS: usize = 256;

/// Maximum number of results returned by one discovery query.
pub const MAX_DISCOVERY_RESULTS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    Tool,
    Skill,
    Workflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySource {
    Builtin,
    Server,
    Mcp,
    Custom,
    Project,
    Workspace,
    User,
    Plugin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    Available,
    Valid,
}

/// Public, bounded projection of a catalog entry's invocation policy.
///
/// Catalog metadata is extensible JSON, but discovery results are part of the
/// model-visible boundary. Only the two policy flags Bamboo currently enforces
/// may cross that boundary; arbitrary bundle keys, nested schemas, paths, or
/// credentials must remain in the host-owned catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInvocationPolicy {
    pub explicit: bool,
    pub automatic: bool,
}

/// Provider-neutral target that a provider adapter may load after discovery.
///
/// Skill and Workflow targets carry the exact catalog identity observed by the
/// search. Calling a gateway is a later operation; discovery itself does not
/// load, execute, authorize, or persist anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CapabilityInvocationTarget {
    Tool {
        name: String,
    },
    Skill {
        name: String,
        skill_id: String,
        source: CapabilitySource,
        revision: u64,
    },
    Workflow {
        name: String,
        workflow_id: String,
        source: CapabilitySource,
        revision: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityMatch {
    #[serde(rename = "ref")]
    pub capability_ref: String,
    pub kind: CapabilityKind,
    pub display_name: String,
    pub summary: String,
    pub source: CapabilitySource,
    pub revision: Option<u64>,
    pub status: CapabilityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_policy: Option<CapabilityInvocationPolicy>,
    pub invocation_target: CapabilityInvocationTarget,
}

/// Bounded logical discovery request. Validation is performed by the discovery
/// service so every provider adapter shares the same limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverCapabilitiesRequest {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<CapabilityKind>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverCapabilitiesResult {
    pub query: String,
    pub matches: Vec<CapabilityMatch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_rejects_fields_outside_the_bounded_contract() {
        let error = serde_json::from_value::<DiscoverCapabilitiesRequest>(json!({
            "query": "git changes",
            "action": "list"
        }))
        .expect_err("management actions are not part of discover");
        assert!(error.to_string().contains("unknown field `action`"));

        serde_json::from_value::<DiscoverCapabilitiesRequest>(json!({
            "query": "git changes",
            "kinds": ["unknown"]
        }))
        .expect_err("unknown capability kinds must fail closed");
    }

    #[test]
    fn result_uses_provider_neutral_reference_and_target_fields() {
        let result = DiscoverCapabilitiesResult {
            query: "git changes".to_string(),
            matches: vec![CapabilityMatch {
                capability_ref: "tool:GitInspect".to_string(),
                kind: CapabilityKind::Tool,
                display_name: "GitInspect".to_string(),
                summary: "Inspect repository status, history, and diffs".to_string(),
                source: CapabilitySource::Builtin,
                revision: None,
                status: CapabilityStatus::Available,
                invocation_policy: None,
                invocation_target: CapabilityInvocationTarget::Tool {
                    name: "GitInspect".to_string(),
                },
            }],
        };
        let value = serde_json::to_value(result).expect("serialize discovery result");

        assert_eq!(value["matches"][0]["ref"], "tool:GitInspect");
        assert_eq!(value["matches"][0]["invocation_target"]["kind"], "tool");
        assert!(value["matches"][0].get("invocation_policy").is_none());
    }
}
