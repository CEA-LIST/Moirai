//! The in-tree Zenoh router (NW.14 note, vault `spec/Zenoh transport,
//! peer-to-peer and router.md`, section 6.2).
//!
//! A router introduces peer-mode `model_node`s to each other: each node is
//! told only the router (`ZENOH_ROUTER`), the router gossips every peer it
//! holds to the others, one hop, and the peers then link and exchange data
//! directly, so they keep converging if the router stops. Client-mode nodes
//! (`ZENOH_MODE=client`) link to the router alone, and their data goes
//! through it.
//!
//! The router never answers a multicast scout: by Zenoh's defaults a router
//! scouts and answers on 224.0.0.224:7446, and a Ledgera peer on its compiled
//! configuration autoconnects to any router that answers there, which would
//! join the two networks (vault `spec/Zenoh in Ledgera and Detune, as
//! deployed.md`, section 8). So [`config`] turns multicast scouting off
//! explicitly, since Zenoh reads an unset key as on.

use std::fmt;

/// The port the router listens on when `LISTEN_PORT` is unset: Zenoh's own
/// default router port.
pub const DEFAULT_PORT: u16 = 7447;

/// How long, in microseconds, a blocking put the router forwards may wait for
/// a free batch before Zenoh drops it and closes the link. The nodes' own
/// value (`moirai_network::transport_config::WAIT_BEFORE_CLOSE_US`), so a
/// state transfer the router carries to a client is not cut at Zenoh's 5 s
/// where the node that sent it would wait 30 s.
pub const WAIT_BEFORE_CLOSE_US: u64 = 30_000_000;

/// A configuration value Zenoh refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// The configuration the router opens its one session from: router mode,
/// listening on `tcp/0.0.0.0:<listen_port>` (0: a port the kernel chooses),
/// multicast scouting off, gossip on and one hop, so it introduces the peers
/// connected to it to each other and to nobody else.
pub fn config(listen_port: u16) -> Result<zenoh::Config, ConfigError> {
    let mut config = zenoh::Config::default();
    let listen = format!("[\"tcp/0.0.0.0:{listen_port}\"]");
    let wait = WAIT_BEFORE_CLOSE_US.to_string();
    for (key, value) in [
        ("mode", "\"router\""),
        ("listen/endpoints", listen.as_str()),
        ("connect/endpoints", "[]"),
        ("scouting/multicast/enabled", "false"),
        ("scouting/gossip/enabled", "true"),
        ("scouting/gossip/multihop", "false"),
        (
            "transport/link/tx/queue/congestion_control/block/wait_before_close",
            wait.as_str(),
        ),
    ] {
        config.insert_json5(key, value).map_err(|e| {
            ConfigError(format!(
                "the Zenoh configuration refused {key} = {value}: {e}"
            ))
        })?;
    }
    Ok(config)
}
