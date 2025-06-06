#![cfg(feature = "crdt")]
use po_crdt::{
    crdt::{mv_register::MVRegister, test_util::n_members},
    protocol::event_graph::EventGraph,
};

fn main() {
    // Obviously, increasing the number of members will increase the
    // memory usage (one LTM per member, which is in Theta(n^2)).
    // Time complexity increase when we are not alone because no op is stabilized
    let mut tcsbs = n_members::<EventGraph<MVRegister<i32>>>(2);

    let max = 40_000;

    for x in 0..max {
        tcsbs[0].tc_bcast(MVRegister::Write(x));
    }
}
