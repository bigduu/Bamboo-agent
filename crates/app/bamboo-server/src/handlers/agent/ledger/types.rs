use serde::Deserialize;

/// Query string for `GET /api/v1/ledger/records`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ListRecordsQuery {
    /// `global` | `project` | `all` (default `all`). Forgiving: `project` (or
    /// `all`) without a `project_key` degrades to the global scope instead of
    /// erroring.
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub project_key: Option<String>,
    /// Comma-separated status tokens (e.g. `open,in_progress`).
    #[serde(default)]
    pub status: Option<String>,
    /// Comma-separated kind tokens (e.g. `todo,reminder`).
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub include_terminal: Option<bool>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Query string for `PATCH`/`DELETE /api/v1/ledger/records/{id}`: the record
/// is looked up in the global scope first, then — when `project_key` is given —
/// in that project's scope.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LocateRecordQuery {
    #[serde(default)]
    pub project_key: Option<String>,
}

/// Query string for `GET /api/v1/ledger/agenda`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AgendaQuery {
    #[serde(default)]
    pub project_key: Option<String>,
    /// Days ahead to include (default 7, clamped to 1..=31).
    #[serde(default)]
    pub horizon_days: Option<i64>,
}

/// Body of `POST /api/v1/ledger/records` — upsert semantics mirroring the
/// `ledger` tool's `upsert` action: no `id` (or an unknown one) creates a
/// record (`title` required); an existing `id` partially updates it (absent
/// fields keep their current values). Times accept RFC3339 or `YYYY-MM-DD`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct UpsertRecordRequest {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    /// `todo` | `event` | `reminder` | `habit` | custom kind (default `todo`).
    #[serde(default)]
    pub kind: Option<String>,
    /// `low` | `medium` | `high` | `critical`.
    #[serde(default)]
    pub priority: Option<String>,
    /// `global` (default) | `project` (requires `project_key`).
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub project_key: Option<String>,
    /// Free markdown notes stored as the record document's body.
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub due_at: Option<String>,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub ends_at: Option<String>,
    #[serde(default)]
    pub remind_at: Option<Vec<String>>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub parent_id: Option<String>,
}

/// Body of `PATCH /api/v1/ledger/records/{id}` — the same optional fields as
/// the upsert body (minus `id`/`scope`, which identify the record) plus a
/// `status` (+ optional `reason`) that is applied through the store's
/// transition path so history and the audit log record it.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PatchRecordRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub due_at: Option<String>,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub ends_at: Option<String>,
    #[serde(default)]
    pub remind_at: Option<Vec<String>>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub parent_id: Option<String>,
    /// `open` | `in_progress` | `blocked` | `done` | `cancelled` | `expired`.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}
