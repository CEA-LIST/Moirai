use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    data_type::replicated::ReplicatedDataType,
    replica::Replica,
    state::{log::IsLog, po_log::VecLog},
};

pub type Twins<Update, L> = (Replica<L, Tcsb<Update>>, Replica<L, Tcsb<Update>>);
pub type Triplet<Update, L> = (
    Replica<L, Tcsb<Update>>,
    Replica<L, Tcsb<Update>>,
    Replica<L, Tcsb<Update>>,
);
pub type Quadruplet<Update, L> = (
    Replica<L, Tcsb<Update>>,
    Replica<L, Tcsb<Update>>,
    Replica<L, Tcsb<Update>>,
    Replica<L, Tcsb<Update>>,
);

pub fn twins<R>() -> Twins<R::Update, VecLog<R>>
where
    R: ReplicatedDataType,
    VecLog<R>: IsLog<Update = R::Update>,
{
    twins_log::<VecLog<R>>()
}

pub fn twins_log<L>() -> Twins<L::Update, L>
where
    L: IsLog,
{
    let replica_a = Replica::<L, Tcsb<L::Update>>::bootstrap("a".into(), &["a", "b"]);
    let replica_b = Replica::<L, Tcsb<L::Update>>::bootstrap("b".into(), &["a", "b"]);
    (replica_a, replica_b)
}

pub fn triplet<R>() -> Triplet<R::Update, VecLog<R>>
where
    R: ReplicatedDataType,
    VecLog<R>: IsLog<Update = R::Update>,
{
    triplet_log::<VecLog<R>>()
}

pub fn triplet_log<L>() -> Triplet<L::Update, L>
where
    L: IsLog,
{
    let replica_a = Replica::<L, Tcsb<L::Update>>::bootstrap("a".into(), &["a", "b", "c"]);
    let replica_b = Replica::<L, Tcsb<L::Update>>::bootstrap("b".into(), &["a", "b", "c"]);
    let replica_c = Replica::<L, Tcsb<L::Update>>::bootstrap("c".into(), &["a", "b", "c"]);
    (replica_a, replica_b, replica_c)
}
