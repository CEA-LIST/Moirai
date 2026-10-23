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
//!
//! Regions (NW.16c's Layout R): given `ROUTER_REGIONS`, the router opens
//! [`config_with_regions`], one Zenoh subregion per name and a last catch-all
//! for a node of any other region or of none. Peers of one region are
//! introduced to each other and link directly; the router never introduces
//! peers of two regions, and forwards between regions. A node claims its
//! region with `ZENOH_REGION`, which Zenoh takes on trust. Without the list
//! the router keeps Zenoh's `auto` preset, [`config`].

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

/// The variable the router reads its region list from.
pub const ROUTER_REGIONS: &str = "ROUTER_REGIONS";

/// The most bytes a Zenoh region name may hold.
pub const MAX_REGION_NAME_BYTES: usize = 32;

/// The region names of a comma-separated `ROUTER_REGIONS`, in order, each as
/// written. Refused: an empty name (the variable set but empty included), a
/// name over [`MAX_REGION_NAME_BYTES`] bytes, a name given twice.
pub fn regions(raw: &str) -> Result<Vec<String>, ConfigError> {
    let mut names: Vec<String> = Vec::new();
    for name in raw.split(',') {
        if name.is_empty() {
            return Err(ConfigError(format!(
                "{ROUTER_REGIONS}=`{raw}` holds an empty region name"
            )));
        }
        if name.len() > MAX_REGION_NAME_BYTES {
            return Err(ConfigError(format!(
                "{ROUTER_REGIONS}=`{raw}`: the region name `{name}` is {} bytes, over Zenoh's \
                 {MAX_REGION_NAME_BYTES}",
                name.len()
            )));
        }
        if names.iter().any(|known| known == name) {
            return Err(ConfigError(format!(
                "{ROUTER_REGIONS}=`{raw}` names the region `{name}` twice"
            )));
        }
        names.push(name.to_string());
    }
    Ok(names)
}

/// [`config`] plus `gateway.south`: one subregion per name of `regions`, in
/// order, holding the nodes that claim that `region_name`, then a last
/// catch-all `{}` holding every other node (another region, none, a client),
/// which Zenoh would otherwise refuse. `regions` are names [`regions`] read.
pub fn config_with_regions(
    listen_port: u16,
    regions: &[String],
) -> Result<zenoh::Config, ConfigError> {
    let mut config = config(listen_port)?;
    let mut south: Vec<String> = regions
        .iter()
        .map(|name| {
            format!(
                "{{\"filters\":[{{\"region_names\":[{}]}}]}}",
                json_string(name)
            )
        })
        .collect();
    south.push("{}".to_string());
    // Zenoh's key paths stop at `gateway`, so it is set whole.
    let gateway = format!("{{\"south\":[{}]}}", south.join(","));
    config.insert_json5("gateway", &gateway).map_err(|e| {
        ConfigError(format!(
            "the Zenoh configuration refused gateway = {gateway}: {e}"
        ))
    })?;
    Ok(config)
}

/// `names` as a JSON array of strings.
pub fn json_strings(names: &[String]) -> String {
    let items: Vec<String> = names.iter().map(|name| json_string(name)).collect();
    format!("[{}]", items.join(","))
}

/// `text` as a JSON string literal.
fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
