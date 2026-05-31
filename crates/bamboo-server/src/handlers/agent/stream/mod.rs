//! Account-scoped change-feed SSE endpoint.

mod handler;
mod response;

pub use handler::handler;

#[cfg(test)]
mod tests;
