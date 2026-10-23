//! NW.15, Z2, Z3 and Z5: real `GenericNode`s over `AnyTransport::Zenoh` (vault
//! `spec/items/NW.15.md`). Z2 and Z3 are the scenarios of
//! `nw15_common/nodes.rs`, which `nw15_nodes_tcp.rs` runs green over TCP at
//! d996427; Z5 is Zenoh's own: a closed session reads `Disconnected` and its
//! successor is synced.
//!
//! One Zenoh session per node, in one process, peer mode on
//! `tcp/127.0.0.1:0`, multicast scouting and gossip off; each node connects
//! to the sessions of the peers it knows that are already open.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_nodes_zenoh -- --test-threads=1
//! ```

#[path = "nw15_common/nodes.rs"]
mod nodes;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::collections::BTreeMap;

use moirai_network::any_transport::AnyTransport;
use moirai_network::transport::PeerId;
use moirai_network::zenoh_transport::ZenohTransport;
use moirai_protocol::log_id::LogId;
use nodes::{add, assert_state, await_state, await_status, set, start_node, union, Net, Op};

struct ZenohNet {
    session: String,
    locators: BTreeMap<String, String>,
}

impl ZenohNet {
    fn new() -> Self {
        Self {
            session: zenoh_rig::fresh_session("n"),
            locators: BTreeMap::new(),
        }
    }

    fn open_one(&mut self, me: &str, known: &[&str]) -> ZenohTransport<Op> {
        let connect: Vec<String> = known
            .iter()
            .filter_map(|peer| self.locators.get(*peer).cloned())
            .collect();
        let peers: Vec<PeerId> = known.iter().map(|peer| peer.to_string()).collect();
        let transport = ZenohTransport::<Op>::open(
            me.to_string(),
            &self.session,
            peers,
            zenoh_rig::loopback_peer(&connect),
        )
        .expect("ZenohTransport::open");
        self.locators
            .insert(me.to_string(), zenoh_rig::tcp_locator(transport.locators()));
        transport
    }
}

impl Net for ZenohNet {
    type T = AnyTransport<Op>;

    fn open(&mut self, ids: &[&str], known: &[&[&str]]) -> Vec<Self::T> {
        ids.iter()
            .zip(known)
            .map(|(me, known)| AnyTransport::Zenoh(self.open_one(me, known)))
            .collect()
    }
}

#[test]
fn z2_three_nodes_converge_partition_heal_converge() {
    nodes::z2_three_nodes_converge_partition_heal_converge(&mut ZenohNet::new());
}

#[test]
fn z3_a_late_joiner_adopts_by_state_transfer_and_converges() {
    nodes::z3_a_late_joiner_adopts_by_state_transfer_and_converges(&mut ZenohNet::new());
}

/// Z5: b's session is closed under a running node: a reads b `Disconnected`
/// and keeps writing; b writes too, alone. A successor session with b's id is
/// put under b's node: a reads b `Connected` again, and the two sync by the
/// `Hello` each side yields, with no pause or resume: a gets b's write and b
/// gets a's. Hand-computed: {x0} before, {x0, x1} at a and {x0, y1} at b
/// while closed, {x0, x1, y1} at both after.
#[test]
fn z5_a_closed_session_reads_disconnected_and_its_successor_is_synced() {
    let mut net = ZenohNet::new();
    let log = LogId::generate();
    let pair = net.open(&["a", "b"], &[&["b"], &["a"]]);
    let mut started = pair
        .into_iter()
        .zip(["a", "b"])
        .map(|(t, me)| start_node(me, &["a", "b"], t, &log, false));
    let a = started.next().unwrap();
    let b = started.next().unwrap();
    await_status(&[(&a, "b"), (&b, "a")], "Connected", "Z5 start");
    add(&a, "x0");
    let before = set(&["x0"]);
    await_state(&a, &before, "Z5, a before the close");
    await_state(&b, &before, "Z5, b before the close");

    b.tap.close();
    await_status(
        &[(&a, "b")],
        "Disconnected",
        "Z5, b at a once b's session is closed",
    );
    add(&a, "x1");
    add(&b, "y1");
    await_state(&a, &union(&before, &["x1"]), "Z5, a while b is closed");
    await_state(&b, &union(&before, &["y1"]), "Z5, b while closed");
    std::thread::sleep(nodes::HOLD);
    assert_state(&a, &union(&before, &["x1"]), "Z5, a a second later");
    assert_state(&b, &union(&before, &["y1"]), "Z5, b a second later");

    b.tap
        .replace(AnyTransport::Zenoh(net.open_one("b", &["a"])));
    await_status(
        &[(&a, "b")],
        "Connected",
        "Z5, b at a once its successor is up",
    );
    let after = set(&["x0", "x1", "y1"]);
    await_state(&a, &after, "Z5, a after b's successor");
    await_state(&b, &after, "Z5, b after its successor");

    a.tap.close();
    b.tap.close();
}
