use moirai_protocol::{
    broadcast::tcsb::Tcsb,
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

pub fn twins<O>() -> Twins<O, VecLog<O>>
where
    O: PureCRDT + Clone,
{
    let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), &["a", "b"]);
    let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), &["a", "b"]);
    (replica_a, replica_b)
}

pub fn twins_log<L>() -> Twins<L::Op, L>
where
    L: IsLog,
{
    let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), &["a", "b"]);
    let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), &["a", "b"]);
    (replica_a, replica_b)
}

pub fn triplet<O: PureCRDT + Clone>() -> Triplet<O, VecLog<O>> {
    let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("a".to_string(), &["a", "b", "c"]);
    let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("b".to_string(), &["a", "b", "c"]);
    let replica_c = Replica::<VecLog<O>, Tcsb<O>>::bootstrap("c".to_string(), &["a", "b", "c"]);
    (replica_a, replica_b, replica_c)
}

pub fn triplet_log<L>() -> Triplet<L::Op, L>
where
    L: IsLog,
{
    let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap("a".to_string(), &["a", "b", "c"]);
    let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap("b".to_string(), &["a", "b", "c"]);
    let replica_c = Replica::<L, Tcsb<L::Op>>::bootstrap("c".to_string(), &["a", "b", "c"]);
    (replica_a, replica_b, replica_c)
}
