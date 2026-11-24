//! NW.19, P1's L1 checks (vault `spec/items/NW.19.md`, section 1, P1): Zenoh
//! is the transport a node runs on when `MOIRAI_TRANSPORT` is unset, TCP is
//! still chosen by `MOIRAI_TRANSPORT=tcp`, and a build without
//! moirai-network's `zenoh` feature refuses the default at start with a
//! message that names the remedy.
//!
//! The default build (feature `zenoh`) runs the first group, which reads
//! variables through `TransportConfig::from_lookup` and never the process
//! environment. The lite build, without features, runs the second group as
//! well:
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --test nw19_default_l1
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --no-default-features --test nw19_default_l1
//! ```
//!
//! The lite group builds the transport from the process environment through
//! `AnyTransport::from_env`, as a node's constructor does, so its tests take
//! one lock and set the variables they name, one test at a time.
//!
//! NW.31b: ported from 601c887 (`moirai-network/tests/nw19_default_l1.rs`) to
//! `moirai-netstack-transport` by name: no op type, and a text frame is an
//! `Event` whose event is a JSON string where 601c887 used `Raw`.

use moirai_netstack_transport::transport_config::{
    DEFAULT_TRANSPORT, TransportConfig, TransportKind,
};

/// The kind `from_lookup` reads from exactly `vars`.
fn kind(vars: &[(&str, &str)]) -> Result<TransportKind, String> {
    TransportConfig::from_lookup(|name| {
        vars.iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| (*v).to_string())
    })
    .map(|c| c.kind())
    .map_err(|e| e.to_string())
}

#[test]
fn nw19_l1_default_transport_is_zenoh() {
    assert_eq!(
        DEFAULT_TRANSPORT,
        TransportKind::Zenoh,
        "NW19 L1 DEFAULT: the default transport is not Zenoh"
    );
}

#[test]
fn nw19_l1_unset_reads_zenoh_and_an_explicit_choice_is_kept() {
    assert_eq!(
        kind(&[]),
        Ok(TransportKind::Zenoh),
        "NW19 L1 UNSET: MOIRAI_TRANSPORT unset"
    );
    assert_eq!(
        kind(&[("MOIRAI_TRANSPORT", "")]),
        Ok(TransportKind::Zenoh),
        "NW19 L1 EMPTY: an empty MOIRAI_TRANSPORT reads as unset"
    );
    assert_eq!(
        kind(&[("MOIRAI_TRANSPORT", "tcp")]),
        Ok(TransportKind::Tcp),
        "NW19 L1 TCP: MOIRAI_TRANSPORT=tcp"
    );
    assert_eq!(
        kind(&[("MOIRAI_TRANSPORT", "zenoh")]),
        Ok(TransportKind::Zenoh),
        "NW19 L1 ZENOH: MOIRAI_TRANSPORT=zenoh"
    );
    assert!(
        kind(&[("MOIRAI_TRANSPORT", "udp")]).is_err(),
        "NW19 L1 UNKNOWN: an unknown transport is accepted"
    );
}

/// A node with `BOOTNODE_URL` and nothing else set is on Zenoh now, which has
/// no bootnode, so it is refused, naming the way back to TCP; with TCP named
/// the bootnode is taken as before.
#[test]
fn nw19_l1_a_bootnode_with_the_transport_unset_is_refused_naming_tcp() {
    let refused = kind(&[("BOOTNODE_URL", "http://127.0.0.1:9/")]);
    match &refused {
        Err(why) => assert!(
            why.contains("MOIRAI_TRANSPORT=tcp"),
            "NW19 L1 BOOTNODE-UNSET: refused, but the message names no MOIRAI_TRANSPORT=tcp: {why}"
        ),
        Ok(k) => panic!(
            "NW19 L1 BOOTNODE-UNSET: BOOTNODE_URL with MOIRAI_TRANSPORT unset is accepted as {k:?}"
        ),
    }
    assert_eq!(
        kind(&[
            ("MOIRAI_TRANSPORT", "tcp"),
            ("BOOTNODE_URL", "http://127.0.0.1:9/")
        ]),
        Ok(TransportKind::Tcp),
        "NW19 L1 BOOTNODE-TCP: a bootnode under an explicit TCP"
    );
}

/// The lite build: no `zenoh` feature.
#[cfg(not(feature = "zenoh"))]
mod lite {
    use std::sync::Mutex;

    use moirai_netstack_transport::HashMap;
    use moirai_netstack_transport::any_transport::AnyTransport;

    /// The variables these tests set are process-wide.
    static ENV: Mutex<()> = Mutex::new(());

    const CLEARED: [&str; 7] = [
        "MOIRAI_TRANSPORT",
        "BOOTNODE_URL",
        "ZENOH_ROUTER",
        "ZENOH_MODE",
        "ZENOH_CONFIG",
        "ZENOH_SCOUTING",
        "ZENOH_REGION",
    ];

    /// The transport `AnyTransport::from_env` builds with exactly `set` in
    /// the environment (every transport variable else removed), on a port
    /// the kernel chooses, with no peer.
    fn build(set: &[(&str, &str)]) -> Result<AnyTransport, String> {
        let _held = ENV.lock().unwrap_or_else(|p| p.into_inner());
        // The ENV lock above serialises every change to these variables.
        #[allow(unused_unsafe)]
        unsafe {
            for var in CLEARED {
                std::env::remove_var(var);
            }
            for (k, v) in set {
                std::env::set_var(k, v);
            }
        }
        let built = AnyTransport::from_env("nw19-lite".into(), 0, HashMap::default())
            .map_err(|e| e.to_string());
        #[allow(unused_unsafe)]
        unsafe {
            for (k, _) in set {
                std::env::remove_var(k);
            }
        }
        built
    }

    fn names_the_feature(why: &str) -> bool {
        let lower = why.to_lowercase();
        lower.contains("zenoh") && lower.contains("feature")
    }

    #[test]
    fn nw19_lite_unset_refuses_naming_the_feature_and_tcp() {
        // Without the feature the enum has one arm, TCP.
        match build(&[]) {
            Ok(_) => panic!(
                "NW19 LITE UNSET: a build without the zenoh feature ran TCP with MOIRAI_TRANSPORT unset"
            ),
            Err(why) => {
                assert!(
                    names_the_feature(&why),
                    "NW19 LITE UNSET: the refusal names no zenoh feature: {why}"
                );
                assert!(
                    why.contains("MOIRAI_TRANSPORT=tcp"),
                    "NW19 LITE UNSET: the refusal names no MOIRAI_TRANSPORT=tcp: {why}"
                );
                assert!(
                    !why.contains("MOIRAI_TRANSPORT=zenoh"),
                    "NW19 LITE UNSET: the refusal says MOIRAI_TRANSPORT=zenoh, which nobody set: {why}"
                );
            }
        }
    }

    #[test]
    fn nw19_lite_explicit_zenoh_refuses_naming_the_feature() {
        match build(&[("MOIRAI_TRANSPORT", "zenoh")]) {
            Ok(_) => panic!(
                "NW19 LITE ZENOH: a build without the zenoh feature accepted MOIRAI_TRANSPORT=zenoh"
            ),
            Err(why) => assert!(
                names_the_feature(&why),
                "NW19 LITE ZENOH: the refusal names no zenoh feature: {why}"
            ),
        }
    }

    #[test]
    fn nw19_lite_explicit_tcp_builds_tcp() {
        match build(&[("MOIRAI_TRANSPORT", "tcp")]) {
            Ok(t) => assert_eq!(
                t.session_port(),
                None,
                "NW19 LITE TCP: a TCP transport reports a Zenoh session port"
            ),
            Err(why) => {
                panic!("NW19 LITE TCP: MOIRAI_TRANSPORT=tcp refused in the lite build: {why}")
            }
        }
    }
}
