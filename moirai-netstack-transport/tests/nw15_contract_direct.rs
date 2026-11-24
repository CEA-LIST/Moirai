//! NW.15, Z1 and Z4, the `DirectTransport` half of the contract suite (vault
//! `spec/items/NW.15.md`). The scenarios and every expectation are in
//! `nw15_common/contract.rs`; `nw15_contract_zenoh.rs` runs the same ones
//! against `ZenohTransport`.
//!
//! This half passes at d996427, before any Zenoh code exists: it shows that
//! the suite's expectations are today's TCP behaviour and that the Zenoh
//! half's red comes from the Zenoh half, never from the suite.
//!
//! Each pair of transports gets exactly one TCP stream (the lower id dials),
//! every transport listens on `127.0.0.1:0` and knows every other id.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --test nw15_contract_direct -- --test-threads=1
//! ```
//!
//! NW.31b: ported from 601c887 (`moirai-network/tests/nw15_contract_direct.rs`) to
//! `moirai-netstack-transport` by name: no op type, and a text frame is an
//! `Event` whose event is a JSON string where 601c887 used `Raw`.

#[path = "nw15_common/contract.rs"]
mod contract;

use std::net::TcpListener;

use contract::Rig;
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::PeerId;

struct DirectRig;

impl Rig for DirectRig {
    type T = DirectTransport;

    fn mesh(&mut self, ids: &[&str]) -> Vec<Self::T> {
        let listeners: Vec<TcpListener> = ids
            .iter()
            .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0"))
            .collect();
        let addresses: Vec<String> = listeners
            .iter()
            .map(|l| l.local_addr().expect("local addr").to_string())
            .collect();
        let mut transports: Vec<Self::T> = listeners
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let peers: HashMap<PeerId, String> = ids
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(j, other)| (other.to_string(), addresses[j].clone()))
                    .collect();
                DirectTransport::with_listener(ids[i].to_string(), listener, peers)
                    .expect("DirectTransport::with_listener")
            })
            .collect();
        for i in 0..ids.len() {
            for other in &ids[i + 1..] {
                transports[i]
                    .connect_to(&other.to_string())
                    .expect("dial a peer on loopback");
            }
        }
        contract::await_mesh(&mut transports, ids);
        transports
    }
}

#[test]
fn z1_send_reaches_the_named_peer_once() {
    contract::z1_send_reaches_the_named_peer_once(&mut DirectRig);
}

#[test]
fn z1_broadcast_reaches_every_peer_once_and_not_its_sender() {
    contract::z1_broadcast_reaches_every_peer_once_and_not_its_sender(&mut DirectRig);
}

#[test]
fn z1_one_sided_pause_holds_both_directions_at_the_pauser() {
    contract::z1_one_sided_pause_holds_both_directions_at_the_pauser(&mut DirectRig);
}

#[test]
fn z1_pause_all_and_resume_all_keep_the_buffers() {
    contract::z1_pause_all_and_resume_all_keep_the_buffers(&mut DirectRig);
}

#[test]
fn z1_unknown_peers_are_peer_not_found() {
    contract::z1_unknown_peers_are_peer_not_found(&mut DirectRig);
}

#[test]
fn z1_a_receiver_that_does_not_read_loses_nothing() {
    contract::z1_a_receiver_that_does_not_read_loses_nothing(&mut DirectRig);
}

#[test]
fn z4_a_128_mib_message_arrives_whole() {
    contract::z4_a_128_mib_message_arrives_whole(&mut DirectRig);
}
