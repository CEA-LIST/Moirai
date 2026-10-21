//! NW.15, Z2 and Z3 over today's `CompositeTransport`: the TCP twin of
//! `nw15_nodes_zenoh.rs` (vault `spec/items/NW.15.md`). Same scenarios, same
//! hand-computed states, in `nw15_common/nodes.rs`.
//!
//! Green at d996427, before any Zenoh code exists: it shows that every state
//! the scenarios expect is what a node does today on TCP, so a red in the
//! Zenoh twin is the Zenoh backend's, never the scenario's. After NW.15 it
//! must stay green: TCP is still the default and unchanged.
//!
//! Every node listens on `127.0.0.1:0`; all listeners of a batch are bound
//! before any node starts, and each node dials the peers it knows once
//! (`GenericNode::connect`), as `model_node` does.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_nodes_tcp -- --test-threads=1
//! ```

#[path = "nw15_common/nodes.rs"]
mod nodes;

use std::collections::BTreeMap;
use std::net::TcpListener;

use moirai_network::composite::CompositeTransport;
use moirai_network::transport::PeerId;
use moirai_network::HashMap;
use nodes::{Net, Op};

#[derive(Default)]
struct TcpNet {
    addresses: BTreeMap<String, String>,
}

impl Net for TcpNet {
    type T = CompositeTransport<Op>;

    fn open(&mut self, ids: &[&str], known: &[&[&str]]) -> Vec<Self::T> {
        let listeners: Vec<TcpListener> = ids
            .iter()
            .map(|me| {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
                self.addresses.insert(
                    me.to_string(),
                    listener.local_addr().expect("local addr").to_string(),
                );
                listener
            })
            .collect();
        ids.iter()
            .zip(known)
            .zip(listeners)
            .map(|((me, known), listener)| {
                let peers: HashMap<PeerId, String> = known
                    .iter()
                    .map(|peer| (peer.to_string(), self.addresses[*peer].clone()))
                    .collect();
                CompositeTransport::with_listener(me.to_string(), listener, peers)
                    .expect("CompositeTransport::with_listener")
            })
            .collect()
    }
}

#[test]
fn z2_three_nodes_converge_partition_heal_converge() {
    nodes::z2_three_nodes_converge_partition_heal_converge(&mut TcpNet::default());
}

#[test]
fn z3_a_late_joiner_adopts_by_state_transfer_and_converges() {
    nodes::z3_a_late_joiner_adopts_by_state_transfer_and_converges(&mut TcpNet::default());
}
