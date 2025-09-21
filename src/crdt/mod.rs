pub mod counter;
pub mod flag;
pub mod graph;
pub mod json;
pub mod list;
pub mod map;
pub mod model;
pub mod register;
pub mod set;

pub mod test_util {
    use std::{collections::HashMap, fmt::Debug};

    use tracing_subscriber::fmt;

    use crate::{
        protocol::{
            broadcast::tcsb::{IsTcsb, Tcsb},
            crdt::pure_crdt::PureCRDT,
            membership::{view::View, Membership},
            replica::{IsReplica, Replica},
            state::{log::IsLog, po_log::VecLog},
        },
        utils::mut_owner::MutOwner,
    };

    pub type Twins<O, L> = (Replica<L, Tcsb<O>>, Replica<L, Tcsb<O>>);
    pub type Triplet<O, L> = (
        Replica<L, Tcsb<O>>,
        Replica<L, Tcsb<O>>,
        Replica<L, Tcsb<O>>,
    );
    pub type Quadruplet<O, L> = (
        Replica<L, Tcsb<O>>,
        Replica<L, Tcsb<O>>,
        Replica<L, Tcsb<O>>,
        Replica<L, Tcsb<O>>,
    );

    pub fn membership_2() -> Membership {
        let mut mapping = HashMap::new();
        let mut view_a = View::new(&"a".to_string());
        view_a.add(&"b".to_string());
        let mut view_b = View::new(&"b".to_string());
        view_b.add(&"a".to_string());
        mapping.insert("a".to_string(), MutOwner::new(view_a));
        mapping.insert("b".to_string(), MutOwner::new(view_b));
        Membership::build(mapping)
    }

    pub fn membership_3() -> Membership {
        let mut mapping = HashMap::new();
        let mut view_a = View::new(&"a".to_string());
        view_a.add(&"b".to_string());
        view_a.add(&"c".to_string());
        let mut view_b = View::new(&"b".to_string());
        view_b.add(&"a".to_string());
        view_b.add(&"c".to_string());
        let mut view_c = View::new(&"c".to_string());
        view_c.add(&"a".to_string());
        view_c.add(&"b".to_string());
        mapping.insert("a".to_string(), MutOwner::new(view_a));
        mapping.insert("b".to_string(), MutOwner::new(view_b));
        mapping.insert("c".to_string(), MutOwner::new(view_c));
        Membership::build(mapping)
    }

    fn membership_n(n: u8) -> Membership {
        assert!(n > 1 && n <= 26, "n must be between 2 and 26");
        let alphabet = "abcdefghijklmnopqrstuvwxyz".chars().collect::<Vec<char>>();
        let mut mapping = HashMap::new();
        for i in 0..n {
            let id = alphabet[i as usize].to_string();
            let mut view = View::new(&id);
            for j in 0..n {
                if i != j {
                    view.add(&alphabet[j as usize].to_string());
                }
            }
            mapping.insert(id, MutOwner::new(view));
        }
        Membership::build(mapping)
    }

    pub fn bootstrap_n<L, T>(n: u8) -> Vec<Replica<L, T>>
    where
        L: IsLog,
        T: IsTcsb<L::Op> + Debug,
    {
        let membership = membership_n(n);
        let mut replicas = Vec::new();
        for i in 0..n {
            let id = (b'a' + i) as char;
            let replica = Replica::<L, T>::bootstrap(id.to_string(), membership.clone());
            replicas.push(replica);
        }
        replicas
    }

    pub fn twins<O>() -> Twins<O, VecLog<O>>
    where
        O: PureCRDT + Clone,
    {
        init_tracing();

        let membership = membership_2();

        let replica_a =
            Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), membership.clone());
        let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), membership);
        (replica_a, replica_b)
    }

    pub fn twins_log<L>() -> Twins<L::Op, L>
    where
        L: IsLog,
    {
        init_tracing();

        let membership = membership_2();

        let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), membership.clone());
        let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), membership);
        (replica_a, replica_b)
    }

    pub fn triplet<O: PureCRDT + Clone>() -> Triplet<O, VecLog<O>> {
        init_tracing();

        let membership = membership_3();

        let replica_a =
            Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), membership.clone());
        let replica_b =
            Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), membership.clone());
        let replica_c = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("c".to_string(), membership);
        (replica_a, replica_b, replica_c)
    }

    pub fn triplet_log<L>() -> Triplet<L::Op, L>
    where
        L: IsLog,
    {
        init_tracing();

        let membership = membership_3();

        let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), membership.clone());
        let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), membership.clone());
        let replica_c = Replica::<L, Tcsb<L::Op>>::bootstrap("c".to_string(), membership);
        (replica_a, replica_b, replica_c)
    }

    pub fn init_tracing() {
        let _ = fmt()
            .with_writer(std::io::stderr)
            .event_format(
                fmt::format()
                    .with_ansi(true)
                    .with_level(true)
                    .with_target(false)
                    .compact(), // nicer layout
            )
            .try_init();
    }

    //     pub fn twins_graph<O: PureCRDT>() -> Twins<EventGraph<O>> {
    //         twins()
    //     }

    //     pub fn triplet_graph<O: PureCRDT>() -> Triplet<EventGraph<O>> {
    //         triplet()
    //     }

    //     pub fn quadruplet_graph<O: PureCRDT>() -> Quadruplet<EventGraph<O>> {
    //         quadruplet()
    //     }

    //     pub fn n_members<L: Log>(n: usize) -> Vec<Tcsb<L>> {
    //         let mut tcsbs = Vec::new();
    //         let alphabet = "abcdefghijklmnopqrstuvwxyz".chars().collect::<Vec<char>>();
    //         let alpha_len = alphabet.len();

    //         // Determine the minimum number of chars needed for unique ids
    //         let mut chars_needed = 1;
    //         let mut max_ids = alpha_len;
    //         while n > max_ids {
    //             chars_needed += 1;
    //             max_ids *= alpha_len;
    //         }

    //         for idx in 0..n {
    //             // Generate id with the required number of chars
    //             let mut id_chars = Vec::with_capacity(chars_needed);
    //             let mut rem = idx;
    //             for _ in 0..chars_needed {
    //                 id_chars.push(alphabet[rem % alpha_len]);
    //                 rem /= alpha_len;
    //             }
    //             id_chars.reverse();
    //             let id: String = id_chars.into_iter().collect();

    //             let tcsb = Tcsb::<L>::new(&id);
    //             tcsbs.push(tcsb);
    //         }

    //         let view_content = tcsbs
    //             .iter()
    //             .map(|tcsb| tcsb.id.clone())
    //             .collect::<Vec<String>>();
    //         for tcsb in tcsbs.iter_mut() {
    //             tcsb.add_pending_view(view_content.clone());
    //             tcsb.start_installing_view();
    //             tcsb.mark_view_installed();
    //         }
    //         for i in 0..n {
    //             assert_eq!(tcsbs[i].ltm.members(), &view_content);
    //             if i == n - 1 {
    //                 break;
    //             }
    //             assert_eq!(tcsbs[i].view_id(), tcsbs[i + 1].view_id());
    //             assert_eq!(tcsbs[i].ltm.clock(), tcsbs[i + 1].ltm.clock());
    //         }
    //         tcsbs
    //     }
}
