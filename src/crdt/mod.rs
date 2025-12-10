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
    use std::fmt::Debug;

    use tracing_subscriber::fmt;

    use crate::protocol::{
        broadcast::tcsb::{IsTcsb, Tcsb},
        crdt::pure_crdt::PureCRDT,
        replica::{IsReplica, Replica},
        state::{log::IsLog, po_log::VecLog},
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

    pub fn bootstrap_n<L, T>(n: u8) -> Vec<Replica<L, T>>
    where
        L: IsLog,
        T: IsTcsb<L::Op> + Debug,
    {
        let mut replicas = Vec::new();
        for i in 0..n {
            let id = i.to_string();
            let replica = Replica::<L, T>::bootstrap(
                id,
                &(0..n)
                    .map(|j| j.to_string())
                    .collect::<Vec<_>>()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            );
            replicas.push(replica);
        }
        replicas
    }

    pub fn twins<O>() -> Twins<O, VecLog<O>>
    where
        O: PureCRDT + Clone,
    {
        init_tracing();

        let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), &["a", "b"]);
        let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), &["a", "b"]);
        (replica_a, replica_b)
    }

    pub fn twins_log<L>() -> Twins<L::Op, L>
    where
        L: IsLog,
    {
        init_tracing();

        let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), &["a", "b"]);
        let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), &["a", "b"]);
        (replica_a, replica_b)
    }

    pub fn triplet<O: PureCRDT + Clone>() -> Triplet<O, VecLog<O>> {
        init_tracing();

        let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), &["a", "b", "c"]);
        let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), &["a", "b", "c"]);
        let replica_c = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("c".to_string(), &["a", "b", "c"]);
        (replica_a, replica_b, replica_c)
    }

    pub fn triplet_log<L>() -> Triplet<L::Op, L>
    where
        L: IsLog,
    {
        init_tracing();

        let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), &["a", "b", "c"]);
        let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), &["a", "b", "c"]);
        let replica_c = Replica::<L, Tcsb<L::Op>>::bootstrap("c".to_string(), &["a", "b", "c"]);
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
}
