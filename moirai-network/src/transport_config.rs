//! Which transport a node runs on, and how its Zenoh session is configured.
//!
//! Read from the variables a node already takes, plus `MOIRAI_TRANSPORT` and
//! a few `ZENOH_*` ones (NW.14 note, vault `spec/Zenoh transport, peer-to-peer
//! and router.md`, sections 4 and 6):
//!
//! | Variable | Meaning |
//! |---|---|
//! | `MOIRAI_TRANSPORT` | `tcp` or `zenoh`; unset is [`DEFAULT_TRANSPORT`] |
//! | `SESSION_ID` | the `<session>` key chunk under Zenoh; default `default` |
//! | `ZENOH_ROUTER` | peer mode, introduced by this router: gossip on, one hop |
//! | `ZENOH_MODE` | `client`, everything through `ZENOH_ROUTER`; refused without it |
//! | `ZENOH_SCOUTING` | `multicast` to opt in, on Moirai's own port |
//! | `ZENOH_CONFIG` | a Zenoh configuration file, used as is |
//! | `BOOTNODE_URL` | refused under Zenoh, which finds its peers without one |
//!
//! Everything is read through a lookup function, so a caller (a test) can
//! pass variables without touching the process environment; a node reads the
//! environment through [`TransportConfig::from_env`].

use std::fmt;

/// The transports a node can run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// [`crate::composite::CompositeTransport`]: a TCP stream per peer, relay
    /// fallback.
    Tcp,
    /// `crate::zenoh_transport::ZenohTransport`: one Zenoh session.
    Zenoh,
}

/// The transport a node runs on when `MOIRAI_TRANSPORT` is unset. TCP until
/// each integration branch flips it after its merge (note section 8).
pub const DEFAULT_TRANSPORT: TransportKind = TransportKind::Tcp;

/// The multicast group and port a node scouts on with
/// `ZENOH_SCOUTING=multicast`. Zenoh's group, not Zenoh's port: 7446 is the
/// one Ledgera's stock configuration scouts on, and a Moirai node must never
/// answer a Ledgera scout (note 6.1).
pub const MOIRAI_SCOUTING_ADDRESS: &str = "224.0.0.224:7450";

/// How long, in microseconds, a blocking put may wait for a free batch
/// before Zenoh drops it and closes the link
/// (`transport/link/tx/queue/congestion_control/block/wait_before_close`).
///
/// Every put of `crate::zenoh_transport::ZenohTransport` blocks, and Zenoh
/// gives a message one deadline from its first wait, never moved, so this
/// bounds the time to queue a whole state transfer. The largest one
/// `model_node` sends at this cut was measured at 1,217,035 bytes (vault
/// `spec/items/NW.16a.md`, section 7a); 30 s carries it on a link of about
/// 0.32 Mbit/s, where Zenoh's default of 5 s needs about 1.95 Mbit/s.
pub const WAIT_BEFORE_CLOSE_US: u64 = 30_000_000;

/// A variable that was refused, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportConfigError(String);

impl fmt::Display for TransportConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TransportConfigError {}

fn refuse<T>(why: impl Into<String>) -> Result<T, TransportConfigError> {
    Err(TransportConfigError(why.into()))
}

/// What the variables say about the transport.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    kind: TransportKind,
    session_id: String,
    router: Option<String>,
    client: bool,
    multicast: bool,
    config_file: Option<String>,
}

impl TransportConfig {
    /// Read every variable through `lookup`, never the process environment.
    /// An empty value reads as unset, as a compose file's `VAR=` does.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, TransportConfigError> {
        let var = |name: &str| lookup(name).filter(|v| !v.is_empty());
        let kind = match var("MOIRAI_TRANSPORT").as_deref() {
            None => DEFAULT_TRANSPORT,
            Some("tcp") => TransportKind::Tcp,
            Some("zenoh") => TransportKind::Zenoh,
            Some(other) => {
                return refuse(format!(
                    "MOIRAI_TRANSPORT=`{other}` is neither `tcp` nor `zenoh`"
                ))
            }
        };
        let mut config = Self {
            kind,
            session_id: var("SESSION_ID").unwrap_or_else(|| "default".to_string()),
            router: None,
            client: false,
            multicast: false,
            config_file: None,
        };
        if kind == TransportKind::Tcp {
            // The Zenoh variables mean nothing to a TCP node.
            return Ok(config);
        }

        if var("BOOTNODE_URL").is_some() {
            return refuse(
                "BOOTNODE_URL is set, but a node under Zenoh finds its peers without a \
                 bootnode; unset it, or run with MOIRAI_TRANSPORT=tcp",
            );
        }
        config.router = var("ZENOH_ROUTER");
        config.client = match var("ZENOH_MODE").as_deref() {
            None | Some("peer") => false,
            Some("client") if config.router.is_some() => true,
            Some("client") => {
                return refuse(
                    "ZENOH_MODE=client sends everything through ZENOH_ROUTER, which is unset",
                )
            }
            Some(other) => {
                return refuse(format!(
                    "ZENOH_MODE=`{other}` is neither `peer` nor `client`"
                ))
            }
        };
        config.multicast = match var("ZENOH_SCOUTING").as_deref() {
            None => false,
            Some("multicast") => true,
            Some(other) => {
                return refuse(format!(
                    "ZENOH_SCOUTING=`{other}`: `multicast` is the only scouting a node opts into"
                ))
            }
        };
        config.config_file = var("ZENOH_CONFIG");
        if config.config_file.is_some() {
            for name in ["ZENOH_ROUTER", "ZENOH_MODE", "ZENOH_SCOUTING"] {
                if var(name).is_some() {
                    return refuse(format!(
                        "ZENOH_CONFIG is used as is, so {name}, which would change it, is refused \
                         beside it"
                    ));
                }
            }
        }
        Ok(config)
    }

    /// [`from_lookup`] over the process environment.
    ///
    /// [`from_lookup`]: TransportConfig::from_lookup
    pub fn from_env() -> Result<Self, TransportConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn kind(&self) -> TransportKind {
        self.kind
    }

    /// The `<session>` chunk of every key under Zenoh: `SESSION_ID`, default
    /// `default`.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The Zenoh configuration of a node listening on `listen_port` whose
    /// `PEERS` are `peer_addresses` (`id → host:port`).
    ///
    /// - `ZENOH_CONFIG`: the file, as is; neither the port nor the peers are
    ///   added.
    /// - `ZENOH_MODE=client`: client mode, connecting to `ZENOH_ROUTER` only,
    ///   listening on nothing.
    /// - Otherwise peer mode, listening on `tcp/0.0.0.0:<listen_port>` and
    ///   connecting to `ZENOH_ROUTER`, if any, and every peer. Gossip is on,
    ///   one hop, only with a router, so the router can introduce the peers.
    ///
    /// Multicast scouting is off unless `ZENOH_SCOUTING=multicast`, which
    /// puts it on [`MOIRAI_SCOUTING_ADDRESS`]. A blocking put waits at most
    /// [`WAIT_BEFORE_CLOSE_US`] for a free batch.
    #[cfg(feature = "zenoh")]
    pub fn zenoh_config(
        &self,
        listen_port: u16,
        peer_addresses: &crate::HashMap<crate::transport::PeerId, String>,
    ) -> Result<zenoh::Config, TransportConfigError> {
        if let Some(path) = &self.config_file {
            return zenoh::Config::from_file(path).or_else(|e| {
                refuse(format!(
                    "ZENOH_CONFIG `{path}` is not a Zenoh configuration: {e}"
                ))
            });
        }

        let mut connect: Vec<String> = self.router.iter().cloned().collect();
        let listen: Vec<String>;
        let mode;
        if self.client {
            mode = "client";
            listen = Vec::new();
        } else {
            mode = "peer";
            listen = vec![format!("tcp/0.0.0.0:{listen_port}")];
            let mut peers: Vec<String> = peer_addresses
                .values()
                .map(|addr| format!("tcp/{addr}"))
                .collect();
            peers.sort();
            connect.extend(peers);
        }

        let mut config = zenoh::Config::default();
        let mut set = |key: &str, value: String| {
            config.insert_json5(key, &value).or_else(|e| {
                refuse(format!(
                    "the Zenoh configuration refused {key} = {value}: {e}"
                ))
            })
        };
        set("mode", json(&mode))?;
        set("listen/endpoints", json(&listen))?;
        set("connect/endpoints", json(&connect))?;
        set("scouting/multicast/enabled", json(&self.multicast))?;
        if self.multicast {
            set("scouting/multicast/address", json(&MOIRAI_SCOUTING_ADDRESS))?;
        }
        let gossip = self.router.is_some();
        set("scouting/gossip/enabled", json(&gossip))?;
        if gossip {
            set("scouting/gossip/multihop", json(&false))?;
        }
        set(
            "transport/link/tx/queue/congestion_control/block/wait_before_close",
            json(&WAIT_BEFORE_CLOSE_US),
        )?;
        Ok(config)
    }
}

/// A configuration value as the JSON `insert_json5` takes.
#[cfg(feature = "zenoh")]
fn json<T: serde::Serialize + ?Sized>(value: &T) -> String {
    serde_json::to_string(value).expect("a string, a list of strings or a bool serializes")
}
