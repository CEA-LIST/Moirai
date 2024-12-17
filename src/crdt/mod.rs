pub mod aw_set;
pub mod counter;
pub mod graph;
pub mod mv_register;
pub mod rw_set;

pub mod test_util {
    use colored::Colorize;
    use log::debug;

    use crate::protocol::{pure_crdt::PureCRDT, tcsb::Tcsb};
    use std::fmt::Debug;

    pub type Twins<O> = (Tcsb<O>, Tcsb<O>);
    pub type Triplet<O> = (Tcsb<O>, Tcsb<O>, Tcsb<O>);
    pub type Quadruplet<O> = (Tcsb<O>, Tcsb<O>, Tcsb<O>, Tcsb<O>);

    pub fn twins<O: PureCRDT + Clone + Debug>() -> Twins<O> {
        #[cfg(feature = "utils")]
        let mut tcsb_a = Tcsb::new_with_trace("a");
        #[cfg(feature = "utils")]
        let mut tcsb_b = Tcsb::new_with_trace("b");
        #[cfg(not(feature = "utils"))]
        let mut tcsb_a = Tcsb::new("a");
        #[cfg(not(feature = "utils"))]
        let mut tcsb_b = Tcsb::new("b");

        tcsb_a.install_view(vec!["a", "b"]);
        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b"]);

        // --> Causal stability <--
        tcsb_b.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b"]);
        assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b"]);

        let left = "<<<".bold().yellow();
        let right = ">>>".bold().yellow();
        debug!(
            "{left} {} and {} are in the same group! {right}",
            tcsb_a.id.blue(),
            tcsb_b.id.blue()
        );
        (tcsb_a, tcsb_b)
    }

    pub fn triplet<O: PureCRDT + Clone + Debug>() -> Triplet<O> {
        let (mut tcsb_a, mut tcsb_b) = twins::<O>();
        let mut tcsb_c = Tcsb::<O>::new("c");

        tcsb_a.install_view(vec!["a", "b", "c"]);
        tcsb_b.install_view(vec!["a", "b", "c"]);

        // --> Causal stability <--
        tcsb_c.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);

        let left = "<<<".bold().yellow();
        let right = ">>>".bold().yellow();
        debug!(
            "{left} {}, {}, and {} are in the same group! {right}",
            tcsb_a.id.blue(),
            tcsb_b.id.blue(),
            tcsb_c.id.blue()
        );
        (tcsb_a, tcsb_b, tcsb_c)
    }

    pub fn quadruplet<O: PureCRDT + Clone + Debug>() -> Quadruplet<O> {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<O>();

        let mut tcsb_d = Tcsb::<O>::new("d");

        tcsb_a.install_view(vec!["a", "b", "c", "d"]);
        tcsb_b.install_view(vec!["a", "b", "c", "d"]);
        tcsb_c.install_view(vec!["a", "b", "c", "d"]);

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
        assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
        assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);

        tcsb_d.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);

        let left = "<<<".bold().yellow();
        let right = ">>>".bold().yellow();
        debug!(
            "{left} {}, {}, {}, and {} are in the same group! {right}",
            tcsb_a.id.blue(),
            tcsb_b.id.blue(),
            tcsb_c.id.blue(),
            tcsb_d.id.blue()
        );

        (tcsb_a, tcsb_b, tcsb_c, tcsb_d)
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{
        counter::Counter,
        test_util::{quadruplet, triplet, twins},
    };

    #[test_log::test]
    fn test_twins() {
        let _ = twins::<Counter<i32>>();
    }

    #[test_log::test]
    fn test_triplet() {
        let _ = triplet::<Counter<i32>>();
    }

    #[test_log::test]
    fn test_quadruplet() {
        let _ = quadruplet::<Counter<i32>>();
    }
}
