#![cfg(feature = "crdt")]

use po_crdt::crdt::{aw_set::AWSet, test_util::quadruplet_graph};

fn main() {
    let (tcsb_a, tcsb_b, tcsb_c, tcsb_d) = quadruplet_graph::<AWSet<u32>>();

    let mut tcsb_arr = [tcsb_a, tcsb_b, tcsb_c, tcsb_d];

    let max = 50_000;

    for x in 0..max {
        for i in 0..tcsb_arr.len() {
            let op = if x >= max / 2 {
                AWSet::Remove(max - x)
            } else {
                AWSet::Add(x)
            };
            let event = tcsb_arr[i].tc_bcast(op);
            for j in 0..tcsb_arr.len() {
                if i != j {
                    tcsb_arr[j].try_deliver(event.clone());
                }
            }
        }
    }

    env_logger::init();

    for (i, tcsb) in tcsb_arr.iter().enumerate() {
        log::info!("TCSB {} stable ops: {}", i, tcsb.state.stable.len());
        log::info!(
            "TCSB {} unstable ops: {}",
            i,
            tcsb.state.unstable.node_count()
        );
        log::info!("Matrix Clock: {}", tcsb.ltm);
    }
}
