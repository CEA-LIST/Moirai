use po_crdt::crdt::{aw_set::AWSet, test_util::quadruplet_graph};

fn main() {
    patate();

    patate();
}

fn patate() {
    let (tcsb_a, tcsb_b, tcsb_c, tcsb_d) = quadruplet_graph::<AWSet<u32>>();

    let mut tcsb_arr = [tcsb_a, tcsb_b, tcsb_c, tcsb_d];

    let max = 10_000;

    for x in 0..max {
        for i in 0..tcsb_arr.len() {
            let op = AWSet::Add(x);
            let event = tcsb_arr[i].tc_bcast(op);
            for j in 0..tcsb_arr.len() {
                if i != j {
                    tcsb_arr[j].try_deliver(event.clone());
                }
            }
        }
    }
}
