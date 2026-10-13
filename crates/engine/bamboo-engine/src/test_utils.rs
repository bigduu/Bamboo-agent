//! Feature-gated support for cross-crate runtime regressions.
//! These helpers reuse the canonical terminal seal rather than duplicating it.

use crate::execution::ChildCompletionSource;
use bamboo_domain::Session;
use std::collections::HashSet;

/// Stage the production seal in a caller-owned terminal snapshot before saving.
pub fn prepare_child_completion_source(
    session: &mut Session,
    activation_run_id: &str,
    prior_message_ids: &HashSet<String>,
) {
    ChildCompletionSource::prepare(session, activation_run_id, prior_message_ids);
}

/// Validate the production seal on the caller's successfully committed snapshot.
pub fn committed_child_completion_source(session: &Session) -> Option<ChildCompletionSource> {
    ChildCompletionSource::from_committed_session(session)
}
