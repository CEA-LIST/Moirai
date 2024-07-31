pub mod aw_map;
pub mod aw_set;
pub mod counter;
pub mod duet;
pub mod graph;
pub mod mv_register;

pub mod test_util {
    use crate::{
        clocks::matrix_clock::MatrixClock,
        protocol::{pure_crdt::PureCRDT, tcsb::Tcsb},
    };
    use std::fmt::Debug;

    pub type Twins<O> = (Tcsb<O>, Tcsb<O>);
    pub type Triplets<O> = (Tcsb<O>, Tcsb<O>, Tcsb<O>);

    pub fn twins<O: PureCRDT + Clone + Debug>() -> Twins<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");

        tcsb_a.ltm = MatrixClock::new(&["a", "b"]);
        tcsb_b.ltm = MatrixClock::new(&["a", "b"]);

        (tcsb_a, tcsb_b)
    }

    pub fn triplets<O: PureCRDT + Clone + Debug>() -> Triplets<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");
        let mut tcsb_c = Tcsb::<O>::new("c");

        tcsb_a.ltm = MatrixClock::new(&["a", "b", "c"]);
        tcsb_b.ltm = MatrixClock::new(&["a", "b", "c"]);
        tcsb_c.ltm = MatrixClock::new(&["a", "b", "c"]);

        (tcsb_a, tcsb_b, tcsb_c)
    }
}
