pub mod aw_map;
pub mod aw_set;
pub mod counter;
pub mod duet;
pub mod graph;
pub mod mv_register;
pub mod rw_set;

pub mod test_util {
    use colored::Colorize;
    use log::debug;

    use crate::protocol::{log::Log, po_log::POLog, pure_crdt::PureCRDT, tcsb::Tcsb};

    pub type Twins<L> = (Tcsb<L>, Tcsb<L>);
    pub type Triplet<L> = (Tcsb<L>, Tcsb<L>, Tcsb<L>);
    pub type Quadruplet<L> = (Tcsb<L>, Tcsb<L>, Tcsb<L>, Tcsb<L>);

    pub fn twins_po<O: PureCRDT>() -> Twins<POLog<O>> {
        twins()
    }

    pub fn triplet_po<O: PureCRDT>() -> Triplet<POLog<O>> {
        triplet()
    }

    pub fn quadruplet_po<O: PureCRDT>() -> Quadruplet<POLog<O>> {
        quadruplet()
    }

    pub fn twins<L: Log>() -> Twins<L> {
        #[cfg(feature = "utils")]
        let mut tcsb_a = Tcsb::new_with_trace("a");
        #[cfg(feature = "utils")]
        let mut tcsb_b = Tcsb::new_with_trace("b");
        #[cfg(not(feature = "utils"))]
        let mut tcsb_a = Tcsb::<L>::new("a");
        #[cfg(not(feature = "utils"))]
        let mut tcsb_b = Tcsb::new("b");

        tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
        tcsb_a.start_installing_view();
        tcsb_a.mark_view_installed();
        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b"]);

        // --> Causal stability <--
        tcsb_b.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b"]);
        assert_eq!(tcsb_a.view_id(), tcsb_b.view_id());
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

    pub fn triplet<L: Log>() -> Triplet<L> {
        let (mut tcsb_a, mut tcsb_b) = twins::<L>();
        let mut tcsb_c = Tcsb::<L>::new("c");

        tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        tcsb_a.start_installing_view();
        tcsb_a.mark_view_installed();

        tcsb_b.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        tcsb_b.start_installing_view();
        tcsb_b.mark_view_installed();

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

    pub fn quadruplet<L: Log>() -> Quadruplet<L> {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<L>();

        let mut tcsb_d = Tcsb::<L>::new("d");

        tcsb_a.add_pending_view(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ]);
        tcsb_a.start_installing_view();
        tcsb_a.mark_view_installed();

        tcsb_b.add_pending_view(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ]);
        tcsb_b.start_installing_view();
        tcsb_b.mark_view_installed();

        tcsb_c.add_pending_view(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ]);
        tcsb_c.start_installing_view();
        tcsb_c.mark_view_installed();

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
    use crate::{
        crdt::{
            counter::Counter,
            test_util::{quadruplet, triplet, twins},
        },
        protocol::po_log::POLog,
    };

    #[test_log::test]
    fn test_twins() {
        let _ = twins::<POLog<Counter<i32>>>();
    }

    #[test_log::test]
    fn test_triplet() {
        let _ = triplet::<POLog<Counter<i32>>>();
    }

    #[test_log::test]
    fn test_quadruplet() {
        let _ = quadruplet::<POLog<Counter<i32>>>();
    }
}
