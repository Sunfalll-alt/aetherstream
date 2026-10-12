//! # AetherStream Server Layer
//!
//! High-performance async TCP networking, connection multiplexing, and broker coordination.

#![warn(missing_docs)]

pub mod error;
pub mod handler;
pub mod server;
pub mod state;

pub use error::ServerError;
pub use handler::handle_request;
pub use server::{AetherServer, ServerConfig};
pub use state::BrokerState;
