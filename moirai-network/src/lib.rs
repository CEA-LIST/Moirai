//! Moirai Network Module
//!
//! This module provides the network infrastructure for distributed Moirai CRDT replicas.
//! It enables replicas to communicate over TCP using JSON serialization and provides
//! an HTTP API for external clients.
//!
//! # Architecture
//!
//! The network module is designed with transport abstraction:
//! - `transport.rs` - Transport-agnostic trait for CRDT replication
//! - `tcp_transport.rs` - TCP implementation for Docker/server environments
//! - `http_api.rs` - HTTP adapter built on generic node channels
//! - `discovery.rs` - optional bootnode peer discovery; off unless configured
//! - `state_transfer.rs` - handing a compacted log to a replica that joins late
//!
//! Future transports (WebRTC, WebSocket) can implement the same trait.

// --- Generic layer (always available) ---
pub mod discovery;
pub mod generic;
pub mod http_api;
pub mod query;
pub mod state_transfer;
pub mod tcp_transport;
pub mod transport;

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<V> = rustc_hash::FxHashSet<V>;
