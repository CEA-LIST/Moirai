//! NW.15, Z1 and Z4, the `ZenohTransport` half of the contract suite (vault
//! `spec/items/NW.15.md`). Same scenarios and expectations as
//! `nw15_contract_direct.rs`, which passes at d996427; they are in
//! `nw15_common/contract.rs`.
//!
//! One Zenoh session per transport, in one process, peer mode on
//! `tcp/127.0.0.1:0`, multicast scouting and gossip off; the n-th transport
//! connects to the n-1 before it, so the sessions form a clique (note 6.1).
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_contract_zenoh -- --test-threads=1
//! ```

#[path = "nw15_common/contract.rs"]
mod contract;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use contract::Rig;
use moirai_network::transport::PeerId;
use moirai_network::zenoh_transport::ZenohTransport;

struct ZenohRig;

impl Rig for ZenohRig {
    type T = ZenohTransport<String>;

    fn mesh(&mut self, ids: &[&str]) -> Vec<Self::T> {
        let session = zenoh_rig::fresh_session("c");
        let mut transports = Vec::new();
        let mut locators: Vec<String> = Vec::new();
        for me in ids {
            let known: Vec<PeerId> = ids
                .iter()
                .filter(|other| *other != me)
                .map(|other| other.to_string())
                .collect();
            let transport = ZenohTransport::<String>::open(
                me.to_string(),
                &session,
                known,
                zenoh_rig::loopback_peer(&locators),
            )
            .expect("ZenohTransport::open");
            locators.push(zenoh_rig::tcp_locator(transport.locators()));
            transports.push(transport);
        }
        contract::await_mesh(&mut transports, ids);
        transports
    }
}

#[test]
fn z1_send_reaches_the_named_peer_once() {
    contract::z1_send_reaches_the_named_peer_once(&mut ZenohRig);
}

#[test]
fn z1_broadcast_reaches_every_peer_once_and_not_its_sender() {
    contract::z1_broadcast_reaches_every_peer_once_and_not_its_sender(&mut ZenohRig);
}

#[test]
fn z1_one_sided_pause_holds_both_directions_at_the_pauser() {
    contract::z1_one_sided_pause_holds_both_directions_at_the_pauser(&mut ZenohRig);
}

#[test]
fn z1_pause_all_and_resume_all_keep_the_buffers() {
    contract::z1_pause_all_and_resume_all_keep_the_buffers(&mut ZenohRig);
}

#[test]
fn z1_unknown_peers_are_peer_not_found() {
    contract::z1_unknown_peers_are_peer_not_found(&mut ZenohRig);
}

#[test]
fn z1_a_receiver_that_does_not_read_loses_nothing() {
    contract::z1_a_receiver_that_does_not_read_loses_nothing(&mut ZenohRig);
}

#[test]
fn z4_a_128_mib_message_arrives_whole() {
    contract::z4_a_128_mib_message_arrives_whole(&mut ZenohRig);
}
