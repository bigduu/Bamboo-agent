//! Server-Sent Events (SSE) handler for real-time agent event streaming.

mod handler;
mod stream;
mod terminal;

pub use handler::handler;

#[cfg(test)]
mod tests;
