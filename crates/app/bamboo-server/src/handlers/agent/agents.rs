//! remote-actor P2a (#181): the `/v1/agents` control-plane registry.
//!
//! An in-memory, cross-host agent registry that lets workers on OTHER machines
//! register (and heartbeat) and lets parents resolve/list/withdraw them over
//! HTTP. It is the network counterpart of the local-file
//! `bamboo_subagent::FileFabric` (`crates/infra/bamboo-subagent/src/discovery.rs`):
//! same [`AgentRecord`] + `lease_expires_at` semantics, but reachable across the
//! network instead of a shared filesystem. The client side is
//! `bamboo_subagent::RegistryFabric`.
//!
//! AUTH: these routes are GATED by `enforce_access_password_middleware` (NOT on
//! the public whitelist). Registration is a control-plane write — a forged
//! record could poison routing (remote-actor-plan §5 "活性伪造:注册需持
//! token"), so a credential (device token / verified cookie / local bypass) is
//! required exactly like every other `/api/v1` route.
//!
//! GC: lazy-filter-on-read. Every GET drops records whose lease has expired
//! (relative to `now`) before returning, and opportunistically prunes them from
//! the map. There is NO background sweep task in P2a — a registered-but-silent
//! worker simply ages out the moment a reader observes it past its lease. This is
//! sufficient for P2a (documented trade-off): the map can hold expired entries
//! between reads, but they are never SERVED, and any read prunes them.

use actix_web::{web, HttpResponse};
use bamboo_subagent::proto::AgentRecord;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::error::AppError;
use crate::AppState;

/// Default lease lifetime when a registration omits `lease_ttl_secs`.
///
/// RENEW CADENCE (worker contract, #202): a registry-published worker (a
/// `bamboo_subagent::RegistryFabric` heartbeat loop) MUST re-`publish()` well
/// WITHIN this TTL — each upsert refreshes `lease_expires_at = now + ttl`, and a
/// record whose lease expires is pruned (lazily on read, eagerly on the next
/// write) and STOPS being a scheduling candidate. Renew at roughly `TTL/2` to
/// `TTL/3` (here ~10-15s) so a single slow/late heartbeat — or a transient
/// network blip on the renew call — does not expire a worker that is actually
/// alive. This mirrors the file-fabric serve loop's "renew < TTL" discipline.
/// The Schedulable scheduler now tolerates a stale-but-leased worker by failing
/// over to the next live candidate, but that is a SECOND line of defense; the
/// renew cadence is the first.
const DEFAULT_LEASE_TTL_SECS: i64 = 30;
/// Upper bound on a requested lease TTL — a worker cannot pin itself live for an
/// unbounded time; it must keep heartbeating. Caps a misbehaving/forged TTL.
const MAX_LEASE_TTL_SECS: i64 = 3600;

/// In-memory control-plane registry: `agent_id -> AgentRecord`.
///
/// Mirrors how `pairing_codes` is held on [`AppState`] — a `DashMap` initialized
/// in the builder. PROCESS-EPHEMERAL: never persisted; a restart drops all
/// registrations (workers re-register on their next heartbeat, exactly like the
/// file fabric being wiped). Lease expiry is enforced lazily on read.
#[derive(Default)]
pub struct AgentRegistry {
    agents: dashmap::DashMap<String, AgentRecord>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Upsert a record by `agent_id`. The caller has already stamped
    /// `lease_expires_at`. Returns the stored record.
    ///
    /// Also prunes expired rows on the WRITE path so the map stays bounded to
    /// live leases even if nothing ever calls `list` (lazy-GC-on-read alone
    /// would let an attacker register many short-lived ids — that nobody
    /// resolves — and grow memory; registration is the write path, so pruning
    /// here caps growth at the rate of registrations).
    fn upsert(&self, rec: AgentRecord) -> AgentRecord {
        let now = Utc::now();
        self.agents
            .retain(|id, r| id == &rec.agent_id || r.lease_expires_at > now);
        self.agents.insert(rec.agent_id.clone(), rec.clone());
        rec
    }

    /// Resolve one live record as of `now`, pruning it if expired.
    fn resolve_as_of(&self, agent_id: &str, now: DateTime<Utc>) -> Option<AgentRecord> {
        match self.agents.get(agent_id).map(|r| r.clone()) {
            Some(rec) if rec.lease_expires_at > now => Some(rec),
            Some(_) => {
                // Expired — prune lazily so the map self-cleans on read.
                self.agents.remove(agent_id);
                None
            }
            None => None,
        }
    }

    /// All live records as of `now`, optionally filtered by `role`. Expired
    /// records are dropped from the map as a side effect (lazy gc).
    fn list_as_of(&self, now: DateTime<Utc>, role: Option<&str>) -> Vec<AgentRecord> {
        // Prune expired first so the map doesn't accumulate dead rows.
        self.agents.retain(|_id, rec| rec.lease_expires_at > now);
        let mut out: Vec<AgentRecord> = self
            .agents
            .iter()
            .filter(|r| role.is_none_or(|want| r.role == want))
            .map(|r| r.clone())
            .collect();
        out.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        out
    }

    /// Remove a record. Idempotent (a missing id is a no-op).
    fn remove(&self, agent_id: &str) {
        self.agents.remove(agent_id);
    }

    /// Insert a record verbatim (lease as-is) — for tests that need to seed an
    /// already-expired or pre-stamped record without going through the HTTP
    /// upsert path (which always re-stamps the lease to `now + ttl`).
    pub fn insert_for_test(&self, rec: AgentRecord) {
        self.agents.insert(rec.agent_id.clone(), rec);
    }
}

/// Registration / heartbeat body for `POST /v1/agents`.
///
/// Accepts EITHER a full record (all of `agent_id`/`pid`/`started_at`, with the
/// client's own `lease_expires_at` IGNORED in favor of a server-stamped one) OR
/// the lighter `{role, labels, endpoint, version, lease_ttl_secs}` shape. The
/// server is the lease authority: it always re-derives `lease_expires_at = now +
/// ttl` so a client cannot forge an over-long lease.
#[derive(Debug, Deserialize)]
pub struct RegisterAgentRequest {
    /// Stable id; an upsert key. Defaults to a fresh UUID when omitted (a brand
    /// new worker that lets the registry name it).
    #[serde(default)]
    pub agent_id: Option<String>,
    pub role: String,
    #[serde(default)]
    pub labels: Vec<String>,
    pub endpoint: String,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub version: String,
    /// Lease TTL in seconds; clamped to `[1, MAX_LEASE_TTL_SECS]`. Omitted →
    /// `DEFAULT_LEASE_TTL_SECS`.
    #[serde(default)]
    pub lease_ttl_secs: Option<i64>,
    /// Original creation time, preserved across heartbeats when the client sends
    /// it; otherwise stamped to `now` on first registration.
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
}

/// Query for `GET /v1/agents`.
#[derive(Debug, Deserialize)]
pub struct ListAgentsQuery {
    #[serde(default)]
    pub role: Option<String>,
}

/// `POST /v1/agents` — register or heartbeat (GATED).
///
/// Upserts by `agent_id` and stamps `lease_expires_at = now + ttl`. Re-posting
/// the same `agent_id` is the renewal mechanism (identical to the file fabric
/// re-writing its record). Returns the stored [`AgentRecord`].
pub async fn register_agent(
    app_state: web::Data<AppState>,
    body: web::Json<RegisterAgentRequest>,
) -> Result<HttpResponse, AppError> {
    let req = body.into_inner();
    let now = Utc::now();

    let ttl_secs = req
        .lease_ttl_secs
        .unwrap_or(DEFAULT_LEASE_TTL_SECS)
        .clamp(1, MAX_LEASE_TTL_SECS);

    let rec = AgentRecord {
        agent_id: req
            .agent_id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        role: req.role,
        labels: req.labels,
        endpoint: req.endpoint,
        pid: req.pid,
        version: req.version,
        // Preserve the worker's original start time across heartbeats; default to
        // now on first registration.
        started_at: req.started_at.unwrap_or(now),
        // The server is the lease authority — always re-derive it.
        lease_expires_at: now + Duration::seconds(ttl_secs),
    };

    let stored = app_state.agent_registry.upsert(rec);
    Ok(HttpResponse::Ok().json(stored))
}

/// `GET /v1/agents` — list live records, optionally `?role=<r>` filtered (GATED).
/// Expired-lease records are not returned (lazy gc on read).
pub async fn list_agents(
    app_state: web::Data<AppState>,
    query: web::Query<ListAgentsQuery>,
) -> Result<HttpResponse, AppError> {
    let recs = app_state
        .agent_registry
        .list_as_of(Utc::now(), query.role.as_deref());
    Ok(HttpResponse::Ok().json(recs))
}

/// `GET /v1/agents/{agent_id}` — resolve one (GATED). 404 if absent or expired.
pub async fn get_agent(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let agent_id = path.into_inner();
    match app_state
        .agent_registry
        .resolve_as_of(&agent_id, Utc::now())
    {
        Some(rec) => Ok(HttpResponse::Ok().json(rec)),
        None => Err(AppError::NotFound(format!("agent {agent_id}"))),
    }
}

/// `DELETE /v1/agents/{agent_id}` — withdraw (GATED). Idempotent: a missing id
/// still returns 200 (matching the file fabric's clean-shutdown semantics).
pub async fn withdraw_agent(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let agent_id = path.into_inner();
    app_state.agent_registry.remove(&agent_id);
    Ok(HttpResponse::Ok().json(serde_json::json!({ "agent_id": agent_id, "withdrawn": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, role: &str, expires: DateTime<Utc>) -> AgentRecord {
        AgentRecord {
            agent_id: id.into(),
            role: role.into(),
            labels: vec![],
            endpoint: "ws://127.0.0.1:1".into(),
            pid: 1,
            version: "0".into(),
            started_at: Utc::now(),
            lease_expires_at: expires,
        }
    }

    #[test]
    fn upsert_resolve_list_withdraw() {
        let reg = AgentRegistry::new();
        let now = Utc::now();
        reg.upsert(rec("a", "service", now + Duration::seconds(30)));
        reg.upsert(rec("b", "gpu", now + Duration::seconds(30)));

        assert!(reg.resolve_as_of("a", now).is_some());
        assert_eq!(reg.list_as_of(now, None).len(), 2);
        // role filter
        let gpu = reg.list_as_of(now, Some("gpu"));
        assert_eq!(gpu.len(), 1);
        assert_eq!(gpu[0].agent_id, "b");

        reg.remove("a");
        assert!(reg.resolve_as_of("a", now).is_none());
        assert_eq!(reg.list_as_of(now, None).len(), 1);
    }

    #[test]
    fn expired_lease_is_not_returned_and_is_pruned() {
        let reg = AgentRegistry::new();
        let now = Utc::now();
        reg.upsert(rec("fresh", "service", now + Duration::seconds(30)));
        reg.upsert(rec("stale", "service", now - Duration::seconds(1)));

        // resolve: stale is None, fresh is Some.
        assert!(reg.resolve_as_of("stale", now).is_none());
        assert!(reg.resolve_as_of("fresh", now).is_some());

        // list drops the stale one (and prunes it from the map).
        let live = reg.list_as_of(now, None);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].agent_id, "fresh");
        // the stale row was pruned by the list-as-of retain
        assert!(!reg.agents.contains_key("stale"));
    }

    #[test]
    fn reregister_refreshes_lease() {
        let reg = AgentRegistry::new();
        let now = Utc::now();
        reg.upsert(rec("a", "service", now + Duration::seconds(1)));
        let before = reg.resolve_as_of("a", now).unwrap().lease_expires_at;
        // re-register with a later lease
        reg.upsert(rec("a", "service", now + Duration::seconds(60)));
        let after = reg.resolve_as_of("a", now).unwrap().lease_expires_at;
        assert!(after > before, "re-register must extend the lease");
        // still a single row (upsert, not append)
        assert_eq!(reg.list_as_of(now, None).len(), 1);
    }

    #[test]
    fn upsert_prunes_expired_rows_on_the_write_path() {
        let reg = AgentRegistry::new();
        let now = Utc::now();
        // Seed an already-expired row directly (bypass the upsert prune).
        reg.insert_for_test(rec("ghost", "service", now - Duration::seconds(1)));
        reg.upsert(rec("live", "service", now + Duration::seconds(30)));
        // A registration (write) of a DIFFERENT id evicts the expired ghost even
        // though nothing ever called list/resolve — the map stays bounded.
        reg.upsert(rec("trigger", "service", now + Duration::seconds(30)));
        assert!(
            !reg.agents.contains_key("ghost"),
            "an expired row must be pruned on the write path"
        );
        // Live rows (including the one that triggered the prune) are retained.
        assert!(reg.agents.contains_key("live"));
        assert!(reg.agents.contains_key("trigger"));
    }
}
