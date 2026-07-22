use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
};

use crate::{
    broadcast::{
        internalizer::{InternalizeOp, Interner},
        tcsb::Tcsb,
    },
    commitment::{
        commit_op::CommitOp, mixed_consistency_replica::MixedConsistencyReplica, oracle::Omega,
    },
    crdt::{
        query::{Contains, QueryOperation, Read},
        sequential::{ExecuteQuery, SequentialDataType},
    },
    replica::{IsReplica, ReplicaId},
    utils::hashmap::HashSet,
};

#[derive(Clone, Debug)]
enum SetUpdate<V> {
    Add(V),
    Remove(V),
}

impl<V> InternalizeOp for SetUpdate<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Set<V: Eq + PartialEq + Hash>(HashSet<V>);

impl<V> Set<V>
where
    V: Eq + PartialEq + Hash + Clone + Debug + Default,
{
    fn from(values: &[V]) -> Self {
        Set(HashSet::from_iter(values.iter().cloned()))
    }
}

impl<V> SequentialDataType for Set<V>
where
    V: Eq + PartialEq + Hash + Clone + Debug + Default,
{
    type Update = SetUpdate<V>;
    type Value = Set<V>;
    type Rejection = Infallible;

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn apply(&mut self, update: &Self::Update) {
        match update {
            SetUpdate::Add(value) => {
                self.0.insert(value.clone());
            }
            SetUpdate::Remove(value) => {
                self.0.remove(value);
            }
        }
    }
}

impl<V> Display for Set<V>
where
    V: Eq + PartialEq + Hash + Clone + Debug + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<V> ExecuteQuery<Read<Set<V>>> for Set<V>
where
    V: Eq + PartialEq + Hash + Clone + Debug + Default,
{
    fn execute_query(&self, _q: Read<Set<V>>) -> <Read<Set<V>> as QueryOperation>::Response {
        Set(self.0.clone())
    }
}

impl<V> ExecuteQuery<Contains<V>> for Set<V>
where
    V: Eq + PartialEq + Hash + Clone + Debug + Default,
{
    fn execute_query(&self, q: Contains<V>) -> <Contains<V> as QueryOperation>::Response {
        self.0.contains(&q.0)
    }
}

type SetReplica<'a> =
    MixedConsistencyReplica<Set<&'a str>, Omega, Tcsb<CommitOp<SetUpdate<&'a str>>>>;

fn create_3_replicas<'a>() -> (SetReplica<'a>, SetReplica<'a>, SetReplica<'a>) {
    let members: [&ReplicaId; 3] = ["A", "B", "C"];

    (
        SetReplica::bootstrap("A".into(), &members, Omega::with_leader("A")),
        SetReplica::bootstrap("B".into(), &members, Omega::with_leader("A")),
        SetReplica::bootstrap("C".into(), &members, Omega::with_leader("A")),
    )
}

#[test]
fn simulation() {
    let (mut r0, mut r1, _r2) = create_3_replicas();

    r0.update(SetUpdate::Add("a")).unwrap();
    r0.update(SetUpdate::Add("b")).unwrap();
    r0.update(SetUpdate::Remove("a")).unwrap();
    r0.update(SetUpdate::Add("c")).unwrap();

    r1.oracle_mut().set_leader("B");

    r1.update(SetUpdate::Add("d")).unwrap();
    r1.update(SetUpdate::Add("e")).unwrap();
    r1.update(SetUpdate::Remove("d")).unwrap();
    r1.update(SetUpdate::Remove("a")).unwrap();

    let since = r0.since();
    let messages = r1.pull(since);
    r0.receive_batch(messages);

    assert!(r0.query(Contains("e")));

    let since = r1.since();
    let messages = r0.pull(since);
    r1.receive_batch(messages);

    assert_eq!(r0.query(Read::new()), r1.query(Read::new()));

    let expected = Set::from(&["b", "c", "e"]);
    assert_eq!(r0.query(Read::new()), expected);
    assert_eq!(r1.query(Read::new()), expected);
}

#[test]
fn internalization() {
    let (mut r0, mut r1, mut r2) = create_3_replicas();

    let event = r0.send(SetUpdate::Add("a")).unwrap();
    r1.receive(event);

    let since = r2.since();
    let messages = r1.pull(since);
    r2.receive_batch(messages);

    assert_eq!(r2.query(Read::new()), Set::from(&["a"]));
}
