//! NW.16a, C1 at L1 (vault `spec/items/NW.16a.md`): every Zenoh
//! configuration Moirai builds sets `wait_before_close` explicitly, to the
//! value the spec registers from its measurement of the largest state
//! transfer `model_node` sends at this cut.
//!
//! Under `CongestionControl::Block`, Zenoh 1.10.1 gives a message one
//! deadline, `wait_before_close` after the first time any of its fragments
//! waits for a free batch, and never moves it (zenoh-transport
//! `src/common/pipeline.rs`, `Deadline::new(wait_before_close, None)` and
//! `WaitTime::advance`'s `None` arm); past it the message is dropped and the
//! link is closed. Zenoh's default is 5 s (zenoh-config `src/defaults.rs`,
//! `wait_before_close: 5000000`, in microseconds). The key is read back from
//! the configuration `TransportConfig::zenoh_config` builds, with
//! `Config::get_json`. Unset, this key reads Zenoh's default, `5000000`
//! (read at d28b6d7), so the registered value differs from it and a read
//! of it shows the key was set.
//!
//! `ZENOH_CONFIG` is not covered: that file is used as is (NW.15, Z6), and
//! what it says about `wait_before_close` is the operator's.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16a_wait_before_close_l1
//! ```

use moirai_network::transport_config::TransportConfig;
use moirai_network::HashMap;

/// The key in Zenoh 1.10.1's configuration (zenoh-config `src/lib.rs`,
/// `transport.link.tx.queue.congestion_control.block.wait_before_close`).
const KEY: &str = "transport/link/tx/queue/congestion_control/block/wait_before_close";

/// Registered in the spec, section C1: microseconds.
const REGISTERED_US: i64 = 30_000_000;

/// Zenoh 1.10.1's own default, for the messages only.
const ZENOH_DEFAULT_US: i64 = 5_000_000;

fn built(vars: &[(&str, &str)]) -> zenoh::Config {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let config = TransportConfig::from_lookup(|name| vars.get(name).cloned())
        .unwrap_or_else(|e| panic!("set-up: the variables {vars:?} are refused: {e}"));
    let mut peers: HashMap<String, String> = HashMap::default();
    peers.insert("b".to_string(), "127.0.0.1:47001".to_string());
    config
        .zenoh_config(0, &peers)
        .unwrap_or_else(|e| panic!("set-up: no configuration for {vars:?}: {e}"))
}

#[test]
fn c1_every_configuration_moirai_builds_sets_wait_before_close() {
    let router = ("ZENOH_ROUTER", "tcp/127.0.0.1:47447");
    let cases: [(&str, Vec<(&str, &str)>); 4] = [
        ("peer mode", vec![("MOIRAI_TRANSPORT", "zenoh")]),
        (
            "peer mode with multicast scouting",
            vec![
                ("MOIRAI_TRANSPORT", "zenoh"),
                ("ZENOH_SCOUTING", "multicast"),
            ],
        ),
        (
            "peer mode introduced by a router",
            vec![("MOIRAI_TRANSPORT", "zenoh"), router],
        ),
        (
            "client mode",
            vec![
                ("MOIRAI_TRANSPORT", "zenoh"),
                router,
                ("ZENOH_MODE", "client"),
            ],
        ),
    ];
    for (mode, vars) in cases {
        let config = built(&vars);
        let read = config
            .get_json(KEY)
            .unwrap_or_else(|e| panic!("set-up: {KEY} is not a key of Zenoh 1.10.1: {e}"));
        assert_eq!(
            read,
            REGISTERED_US.to_string(),
            "C1, {mode}: the configuration Moirai builds reads {KEY} = {read}; NW.16a registers \
             {REGISTERED_US} µs, set explicitly (unset, the key reads Zenoh's default, \
             {ZENOH_DEFAULT_US} µs)"
        );
    }
}
