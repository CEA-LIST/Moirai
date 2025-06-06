#![cfg(feature = "crdt")]
use po_crdt::{
    crdt::{mv_register::MVRegister, test_util::n_members},
    protocol::event_graph::EventGraph,
};

fn main() {
    let mut tcsbs = n_members::<EventGraph<MVRegister<i32>>>(32);

    let max = 20_000;

    for x in 0..max {
        tcsbs[0].tc_bcast(MVRegister::Write(x));
    }
}
