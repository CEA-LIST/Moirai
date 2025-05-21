use po_crdt::crdt::{rw_set::RWSet, test_util::twins_graph};

fn main() {
    let (tcsb_a, tcsb_b) = twins_graph::<RWSet<usize>>();

    let mut tcsb_arr = [tcsb_a, tcsb_b];

    for x in 0..100_000 {
        for i in 0..tcsb_arr.len() {
            let op = RWSet::Add(x);
            tcsb_arr[i].tc_bcast(op);
        }
    }

    env_logger::init();

    for (i, tcsb) in tcsb_arr.iter().enumerate() {
        log::info!(
            "TCSB {} -> unstable ops: {}",
            i,
            tcsb.state.unstable.node_count()
        );
        log::info!(
            "TCSB {} : unstable node capacity: {} - unstable edge capacity: {}",
            i,
            tcsb.state.unstable.capacity().0,
            tcsb.state.unstable.capacity().1,
        );
    }
}
