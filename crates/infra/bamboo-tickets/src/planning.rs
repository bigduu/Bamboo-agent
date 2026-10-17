//! Owner planning metadata over the existing Ticket snapshot and audit receipts.
//! This never changes execution identity, claims, contracts, or acceptance.

use bamboo_domain::TaskPriority;
use serde::{Deserialize, Deserializer};

use crate::{Authority, Error, Result, Snapshot};

pub(crate) fn is_default_priority(priority: &TaskPriority) -> bool {
    *priority == TaskPriority::Medium
}

pub(crate) fn required_nullable_deadline<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

fn validate_deadline(deadline: Option<&str>) -> Result<()> {
    if let Some(value) = deadline {
        // RFC 3339 -00:00 means an unknown local offset, not an explicit timezone.
        if value.len() > 64
            || !value.is_ascii()
            || !matches!(value.as_bytes().get(10), Some(b'T' | b't'))
            || value.ends_with("-00:00")
            || chrono::DateTime::parse_from_rfc3339(value).is_err()
        {
            return Err(Error::InvalidTransition(
                "expected completion requires RFC 3339 with an explicit known offset".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn set(
    snapshot: &mut Snapshot,
    authority: &Authority,
    ticket_id: &str,
    expected_record_revision: u64,
    priority: &TaskPriority,
    expected_completion_at: Option<&str>,
) -> Result<()> {
    // A model estimate cannot overwrite the owner's desired deadline.
    authority.user()?;
    validate_deadline(expected_completion_at)?;
    let ticket = snapshot
        .tickets
        .get_mut(ticket_id)
        .ok_or_else(|| Error::InvalidTransition("ticket not found".into()))?;
    if ticket.record_revision != expected_record_revision {
        return Err(Error::RevisionConflict);
    }
    if ticket.archived {
        return Err(Error::InvalidTransition(
            "unarchive the Ticket before editing planning metadata".into(),
        ));
    }
    ticket.priority = priority.clone();
    // Preserve the explicit owner-supplied offset; do not infer a local timezone.
    ticket.expected_completion_at = expected_completion_at.map(str::to_owned);
    ticket.record_revision += 1;
    ticket.updated_seq = snapshot.seq;
    // Older writers must reject even a clearing command's new audit payload.
    snapshot.schema = snapshot.schema.max(7);
    Ok(())
}

pub(crate) fn validate(snapshot: &Snapshot) -> Result<()> {
    for ticket in snapshot.tickets.values() {
        if snapshot.schema < 7
            && (!is_default_priority(&ticket.priority) || ticket.expected_completion_at.is_some())
        {
            return Err(Error::InvalidTransition(
                "Ticket planning metadata requires schema 7".into(),
            ));
        }
        validate_deadline(ticket.expected_completion_at.as_deref())?;
    }
    Ok(())
}
