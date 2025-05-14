pub mod aw_graph;
pub mod aw_map;
pub mod aw_set;
pub mod counter;
pub mod duet;
pub mod mv_register;
pub mod resettable_counter;
pub mod rw_set;

pub mod test_util {
    use colored::Colorize;
    use log::debug;

    use crate::protocol::{event_graph::EventGraph, log::Log, pure_crdt::PureCRDT, tcsb::Tcsb};

    pub type Twins<L> = (Tcsb<L>, Tcsb<L>);
    pub type Triplet<L> = (Tcsb<L>, Tcsb<L>, Tcsb<L>);
    pub type Quadruplet<L> = (Tcsb<L>, Tcsb<L>, Tcsb<L>, Tcsb<L>);

    pub fn twins_graph<O: PureCRDT>() -> Twins<EventGraph<O>> {
        twins()
    }

    pub fn triplet_graph<O: PureCRDT>() -> Triplet<EventGraph<O>> {
        triplet()
    }

    pub fn quadruplet_graph<O: PureCRDT>() -> Quadruplet<EventGraph<O>> {
        quadruplet()
    }

    pub fn n_members<L: Log>(n: usize) -> Vec<Tcsb<L>> {
        assert!(n > 1, "The number of members must be greater than 1");
        assert!(
            n <= 26,
            "The number of members must be less than or equal to 26"
        );
        let mut tcsbs = Vec::new();
        let alphabet = "abcdefghijklmnopqrstuvwxyz";
        let alphabet = alphabet.chars().collect::<Vec<char>>();
        for i in alphabet.iter().take(n) {
            #[cfg(feature = "utils")]
            let tcsb = Tcsb::new_with_trace(&i.to_string());
            #[cfg(not(feature = "utils"))]
            let tcsb = Tcsb::<L>::new(&i.to_string());
            tcsbs.push(tcsb);
        }
        let view_content = tcsbs
            .iter()
            .map(|tcsb| tcsb.id.clone())
            .collect::<Vec<String>>();
        for tcsb in tcsbs.iter_mut() {
            tcsb.add_pending_view(view_content.clone());
            tcsb.start_installing_view();
            tcsb.mark_view_installed();
        }
        for i in 0..n {
            assert_eq!(tcsbs[i].ltm.members(), &view_content);
            if i == n - 1 {
                break;
            }
            assert_eq!(tcsbs[i].view_id(), tcsbs[i + 1].view_id());
            assert_eq!(tcsbs[i].ltm, tcsbs[i + 1].ltm);
        }
        tcsbs
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
        assert_eq!(tcsb_a.ltm.members(), &vec!["a", "b"]);

        // --> Causal stability <--
        tcsb_b.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_a.ltm.members(), &vec!["a", "b"]);
        assert_eq!(tcsb_a.view_id(), tcsb_b.view_id());
        assert_eq!(tcsb_b.ltm.members(), &vec!["a", "b"]);

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

        assert_eq!(tcsb_a.ltm.members(), &vec!["a", "b", "c"]);
        assert_eq!(tcsb_b.ltm.members(), &vec!["a", "b", "c"]);
        assert_eq!(tcsb_c.ltm.members(), &vec!["a", "b", "c"]);

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

        assert_eq!(tcsb_a.ltm.members(), &vec!["a", "b", "c", "d"]);
        assert_eq!(tcsb_b.ltm.members(), &vec!["a", "b", "c", "d"]);
        assert_eq!(tcsb_c.ltm.members(), &vec!["a", "b", "c", "d"]);

        tcsb_d.state_transfer(&mut tcsb_a);

        assert_eq!(tcsb_d.ltm.members(), &vec!["a", "b", "c", "d"]);

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
            test_util::{n_members, quadruplet, triplet, twins},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn test_twins() {
        let _ = twins::<EventGraph<Counter<i32>>>();
    }

    #[test_log::test]
    fn test_triplet() {
        let _ = triplet::<EventGraph<Counter<i32>>>();
    }

    #[test_log::test]
    fn test_quadruplet() {
        let _ = quadruplet::<EventGraph<Counter<i32>>>();
    }

    #[test_log::test]
    fn two_members() {
        let _ = n_members::<EventGraph<Counter<i32>>>(8);
    }
}
