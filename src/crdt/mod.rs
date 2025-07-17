pub mod counter;
pub mod flag;
pub mod graph;
pub mod map;
pub mod model;
pub mod object;
pub mod register;
pub mod set;

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
        let mut tcsbs = Vec::new();
        let alphabet = "abcdefghijklmnopqrstuvwxyz".chars().collect::<Vec<char>>();
        let alpha_len = alphabet.len();

        // Determine the minimum number of chars needed for unique ids
        let mut chars_needed = 1;
        let mut max_ids = alpha_len;
        while n > max_ids {
            chars_needed += 1;
            max_ids *= alpha_len;
        }

        for idx in 0..n {
            // Generate id with the required number of chars
            let mut id_chars = Vec::with_capacity(chars_needed);
            let mut rem = idx;
            for _ in 0..chars_needed {
                id_chars.push(alphabet[rem % alpha_len]);
                rem /= alpha_len;
            }
            id_chars.reverse();
            let id: String = id_chars.into_iter().collect();

            let tcsb = Tcsb::<L>::new(&id);
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
            assert_eq!(tcsbs[i].ltm.clock(), tcsbs[i + 1].ltm.clock());
        }
        tcsbs
    }

    pub fn twins<L: Log + Clone>() -> Twins<L> {
        let mut tcsb_a = Tcsb::<L>::new("a");
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

    pub fn triplet<L: Log + Clone>() -> Triplet<L> {
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

    pub fn quadruplet<L: Log + Clone>() -> Quadruplet<L> {
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
