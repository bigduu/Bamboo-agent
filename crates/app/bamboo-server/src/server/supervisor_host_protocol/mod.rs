//! Private, opt-in per-intent Bamboo-owned native confirmation channel.
//! Transport never grants consent. HTTP owner verification stays unchanged.
mod adapter;
mod canonical;
mod confirmation;
mod native_macos;
mod provider;
mod state;
mod transport;
mod wire;
pub use provider::native_scope_confirmation_helper_entry;
pub(super) use transport::NativeChannel;

#[cfg(test)]
mod canonical_tests;
