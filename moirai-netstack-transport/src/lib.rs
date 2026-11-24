//! Transports for [`moirai_netstack`]: the network layer's seven transports
//! from 601c887's `moirai-network`, without an operation type.
//!
//! The frame is [`moirai_netstack::TransportMessage`] itself, so one
//! `CrdtTransport` carries wire v1 and stays object safe:
//!
//! - `transport.rs` - the trait and its errors
//! - `direct_transport.rs` - one TCP stream per peer; the transport for peers
//!   that can be dialled
//! - `relay_transport.rs` - one multiplexed session to a relay, for peers that
//!   cannot be dialled at all
//! - `composite.rs` - routes each peer to the provider that can reach it, and
//!   realizes the same trait
//! - `zenoh_transport.rs` - one Zenoh session per node (feature `zenoh`, on by
//!   default)
//! - `any_transport.rs` - TCP or Zenoh, chosen by `MOIRAI_TRANSPORT`
//! - `transport_config.rs` - `MOIRAI_TRANSPORT` and the Zenoh session's
//!   configuration
//! - `tcp_transport.rs` - deprecated shim for the old name of
//!   `DirectTransport`
//! - `zenoh_patch.rs` - the start-up check that the linked zenoh is the
//!   vendored, patched copy

pub mod any_transport;
pub mod composite;
pub mod direct_transport;
pub mod relay_transport;
pub mod tcp_transport;
pub mod transport;
pub mod transport_config;
#[cfg(feature = "zenoh")]
pub mod zenoh_patch;
#[cfg(feature = "zenoh")]
pub mod zenoh_transport;

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<V> = rustc_hash::FxHashSet<V>;
