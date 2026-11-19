//! Request-scoped reuse of paid, nondeterministic Legacy Vision descriptions.
//!
//! A final-request checkpoint can be bookkeeping only: it commits the reconciled
//! model-context ledger without changing anything the provider is asked. The
//! checkpoint/reprepare loop then re-derives the prepared context, and that
//! derivation applies the Legacy Vision fallback again. Because a description
//! request is both paid and nondeterministic, its fresh text feeds the
//! model-context ledger — so every rebuild would report another change and the
//! loop could exhaust its reprepare budget without ever converging, while billing
//! the same retained image repeatedly.
//!
//! This store is deliberately narrow:
//!
//! - **Request-scoped.** It lives on one round's configuration, is cleared when
//!   the round starts, and is never persisted.
//! - **Source-keyed.** Callers clear it as soon as provider-relevant source state
//!   changes, so a changed input, a changed authority decision or a changed
//!   attachment never reuses a mismatched description.
//! - **Not a cache.** It holds only descriptions already produced for the same
//!   image under the same target within one request, and adds no durable image
//!   store, transcript entry or cross-round reuse.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

/// One retained image's description within a request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct VisionDescriptionKey {
    /// Provider instance and model the description was requested from.
    pub vision_model: String,
    /// The exact image source the description belongs to.
    pub image_url: String,
    pub detail: Option<String>,
    /// The request-specific focus, when the caller supplied one.
    pub focus: Option<String>,
}

/// Descriptions already produced for this request, keyed by source identity.
#[derive(Debug, Default)]
pub(crate) struct VisionDescriptionReuse {
    entries: Mutex<HashMap<VisionDescriptionKey, String>>,
}

impl VisionDescriptionReuse {
    /// A described image is only a request-scoped optimization; a poisoned lock
    /// must not fail an otherwise valid round.
    fn entries(&self) -> MutexGuard<'_, HashMap<VisionDescriptionKey, String>> {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Drop every retained description. Callers must do this whenever
    /// provider-relevant source state changes so a stale description can never be
    /// applied to a different request.
    pub(crate) fn clear(&self) {
        self.entries().clear();
    }

    /// Return a description already prepared for exactly this source.
    pub(crate) fn get(&self, key: &VisionDescriptionKey) -> Option<String> {
        self.entries().get(key).cloned()
    }

    /// Retain a description produced for exactly this source.
    pub(crate) fn insert(&self, key: VisionDescriptionKey, description: String) {
        self.entries().insert(key, description);
    }
}
