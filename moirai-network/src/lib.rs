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
//! - `direct_transport.rs` - one TCP stream per peer; the transport for peers
//!   that can be dialled. Formerly `tcp_transport::TcpTransport`
//! - `composite.rs` - what a node is built with: routes each peer to the
//!   provider that can reach it, and realizes the same trait
//! - `http_api.rs` - HTTP adapter built on generic node channels
//! - `discovery.rs` - optional bootnode peer discovery; off unless configured
//! - `dashboard.rs` - optional outbound reporting to a monitoring dashboard; off unless configured
//! - `workload.rs` - the operation shapes the rig drives, and a seeded generator over them
//! - `state_transfer.rs` - handing a compacted log to a replica that joins late
//! - `relay_transport.rs` - one multiplexed session to a relay, for peers that
//!   cannot be dialled at all
//! - `tcp_transport.rs` - deprecated shim for the old name of `DirectTransport`
//!
//! Future transports (WebRTC, WebSocket) can implement the same trait.

// --- Generic layer (always available) ---
pub mod composite;
pub mod dashboard;
pub mod direct_transport;
pub mod discovery;
pub mod generic;
pub mod http_api;
pub mod query;
pub mod relay_transport;
pub mod state_transfer;
pub mod tcp_transport;
pub mod transport;
pub mod workload;

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<V> = rustc_hash::FxHashSet<V>;
