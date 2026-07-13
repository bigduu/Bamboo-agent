//! Ledger HTTP endpoints — the `/api/v1/ledger` surface over the
//! prospective-record ledger (todos, events, reminders, habits).
//!
//! Thin REST layer over [`bamboo_memory::ledger_store::LedgerStore`], mirroring
//! the `ledger` agent tool's argument conventions (see
//! `bamboo-server-tools/src/ledger`) so a UI and the agent read/write the same
//! store the same way. Unlike the tool, this layer never touches the schedule
//! bridge: reminder/recurrence -> schedule syncing is a tool-layer concern
//! (see the note in `handlers::upsert_record_core`).

mod handlers;
mod types;

pub use handlers::{agenda, delete_record, list_records, patch_record, upsert_record};
pub use types::{
    AgendaQuery, ListRecordsQuery, LocateRecordQuery, PatchRecordRequest, UpsertRecordRequest,
};

#[cfg(test)]
mod tests;
