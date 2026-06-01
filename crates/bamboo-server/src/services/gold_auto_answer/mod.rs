//! Server-side re-export shim for the engine Gold auto-answer service.
//!
//! The service now lives in `bamboo_engine::gold_auto_answer`. These re-exports
//! preserve the historical `crate::services::gold_auto_answer::…` paths used in
//! the server crate.

pub use bamboo_engine::gold_auto_answer::{
    maybe_auto_answer_pending_question, GoldAutoAnswerOutcome,
};

#[cfg(test)]
mod tests;
