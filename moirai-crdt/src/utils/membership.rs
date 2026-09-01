use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::pure_crdt::PureCRDT,
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::{log::IsLog, po_log::VecLog},
    utils::intern_str::InternalizeOp,
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

// Every helper mints ONE log id and hands it to every replica it builds.
// Letting each replica `bootstrap` — and therefore mint — separately would
// give the twins two different logs, and a replica refuses events from a log
// it does not host, so every exchange below would silently deliver nothing.

pub fn twins<O>() -> Twins<O, VecLog<O>>
where
    O: PureCRDT + Clone + InternalizeOp,
{
    let log_id = LogId::generate();
    let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap_with_log_id(
        "a".to_string(),
        &["a", "b"],
        log_id.clone(),
    );
    let replica_b =
        Replica::<VecLog<O>, Tcsb<O>>::bootstrap_with_log_id("b".to_string(), &["a", "b"], log_id);
    (replica_a, replica_b)
}

pub fn twins_log<L>() -> Twins<L::Op, L>
where
    L: IsLog,
    L::Op: InternalizeOp,
{
    let log_id = LogId::generate();
    let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap_with_log_id(
        "a".to_string(),
        &["a", "b"],
        log_id.clone(),
    );
    let replica_b =
        Replica::<L, Tcsb<L::Op>>::bootstrap_with_log_id("b".to_string(), &["a", "b"], log_id);
    (replica_a, replica_b)
}

pub fn triplet<O: PureCRDT + Clone + InternalizeOp>() -> Triplet<O, VecLog<O>> {
    let log_id = LogId::generate();
    let replica_a = Replica::<VecLog<O>, Tcsb<O>>::bootstrap_with_log_id(
        "a".to_string(),
        &["a", "b", "c"],
        log_id.clone(),
    );
    let replica_b = Replica::<VecLog<O>, Tcsb<O>>::bootstrap_with_log_id(
        "b".to_string(),
        &["a", "b", "c"],
        log_id.clone(),
    );
    let replica_c = Replica::<VecLog<O>, Tcsb<O>>::bootstrap_with_log_id(
        "c".to_string(),
        &["a", "b", "c"],
        log_id,
    );
    (replica_a, replica_b, replica_c)
}

pub fn triplet_log<L>() -> Triplet<L::Op, L>
where
    L: IsLog,
    L::Op: InternalizeOp,
{
    let log_id = LogId::generate();
    let replica_a = Replica::<L, Tcsb<L::Op>>::bootstrap_with_log_id(
        "a".to_string(),
        &["a", "b", "c"],
        log_id.clone(),
    );
    let replica_b = Replica::<L, Tcsb<L::Op>>::bootstrap_with_log_id(
        "b".to_string(),
        &["a", "b", "c"],
        log_id.clone(),
    );
    let replica_c =
        Replica::<L, Tcsb<L::Op>>::bootstrap_with_log_id("c".to_string(), &["a", "b", "c"], log_id);
    (replica_a, replica_b, replica_c)
}
