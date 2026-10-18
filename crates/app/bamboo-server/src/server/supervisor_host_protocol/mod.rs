//! Proposed private Supervisor scope protocol, with no production admission.
//!
//! This pure component deliberately has no CLI startup flag, stdin task,
//! AppState access, verifier installation or CAS call. UnadmittedChannel may
//! negotiate transport but always refuses an apply. The independently verified
//! native Host admission source is a separate unresolved security boundary;
//! neither a parent PID nor private pipe supplies owner consent.

mod state;
mod wire;
