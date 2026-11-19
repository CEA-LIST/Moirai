//! A single-owner replica loop with replaceable transports and a small HTTP API.
//!
//! See the crate README and the `vanilla_set` / `commit_set` examples for setup.
pub mod config;
mod http_api;
pub mod node;
pub mod tcp;
pub mod transport;
mod wire;

pub use config::{Config, PeerConfig};
pub use node::{Application, CoreReplica, Node, NodeHandle, TickHook, UpdateResult};
pub use tcp::TcpTransport;
pub use transport::{Packet, Transport};
