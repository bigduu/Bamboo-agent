//! Focused execution of the real Host checkpoint source and its private
//! publication-fault tests. Cargo also compiles the production server library;
//! unrelated server unit tests are outside this target's acceptance scope.

#[path = "../src/app_state/ticket_application/patrol_adapter/checkpoint_store.rs"]
pub mod checkpoint_store;
