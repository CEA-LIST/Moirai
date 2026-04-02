use std::{
    fmt::{Debug, Display, Formatter},
    hash::Hash,
};

#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    metrics::{FuzzMetrics, StructureMetrics},
    op_generator::OpGeneratorNested,
    value_generator::ValueGenerator,
};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{Get, QueryOperation, Read},
    },
    event::Event,
    replica::ReplicaIdx,
    state::{
        log::IsLog,
        sink::{DefaultSinkExpansion, IsLogSink, ObjectPath, Sink, SinkCollector},
    },
    utils::{boxer::Boxer, intern_str::Interner, translate_ids::TranslateIds},
};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::HashMap;

#[derive(Clone, Debug)]
pub enum UWMap<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L>
where
    K: Clone + Eq + Hash,
{
    children: HashMap<K, L>,
}

impl<K, L> TranslateIds for UWMap<K, L>
where
    K: Clone,
    L: TranslateIds,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        match self {
            UWMap::Update(k, v) => UWMap::Update(k.clone(), v.translate_ids(from, interner)),
            UWMap::Remove(k) => UWMap::Remove(k.clone()),
            UWMap::Clear => UWMap::Clear,
        }
    }
}

impl<K: Clone + Debug + Eq + Hash, L> Default for UWMapLog<K, L> {
    fn default() -> Self {
        Self {
            children: Default::default(),
        }
    }
}

impl<K, L> UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn children(&self) -> &HashMap<K, L> {
        &self.children
    }

    pub fn get_child(&self, key: &K) -> Option<&L> {
        self.children.get(key)
    }
}

impl<K, L> IsLog for UWMapLog<K, L>
where
    L: IsLog,
    K: Clone + Debug + Hash + Eq,
{
    type Value = HashMap<K, L::Value>;
    type Op = UWMap<K, L::Op>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            UWMap::Update(k, v) => {
                let child_op = Event::unfold(event, v);
                self.children.entry(k.clone()).or_default().effect(child_op);
                if self.children.get(&k).unwrap().is_default() {
                    self.children.remove(&k);
                }
            }
            UWMap::Remove(k) => {
                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), true);
                    if child.is_default() {
                        self.children.remove(&k);
                    }
                }
            }
            UWMap::Clear => {
                self.children.retain(|_, child| {
                    child.redundant_by_parent(event.version(), true);
                    !child.is_default() // keep only non-default children
                });
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.children.is_empty()
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        match op {
            UWMap::Update(k, v) => self
                .children
                .get(k)
                .map_or_else(|| true, |child| child.is_enabled(v)),
            UWMap::Remove(_) | UWMap::Clear => true,
        }
    }
}

impl<K, L> IsLogSink for UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLogSink,
{
    fn effect_with_sink(
        &mut self,
        event: Event<Self::Op>,
        path: ObjectPath,
        sink: &mut SinkCollector,
    ) {
        match event.op().clone() {
            UWMap::Update(k, v) => {
                let path = path.map_entry(format!("{:?}", k));
                if self.children.contains_key(&k) {
                    sink.collect(Sink::update(path.clone()));
                } else {
                    sink.collect(Sink::create(path.clone()));
                }

                let child_op = Event::unfold(event, v);
                self.children
                    .entry(k.clone())
                    .or_default()
                    .effect_with_sink(child_op, path, sink);

                if self.children.get(&k).unwrap().is_default() {
                    self.children.remove(&k);
                }
            }
            UWMap::Remove(k) => {
                let path = path.map_entry(format!("{:?}", k));
                sink.collect(Sink::delete(path));

                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), true);
                    if child.is_default() {
                        self.children.remove(&k);
                    }
                }
            }
            UWMap::Clear => {
                sink.collect(Sink::delete(path));
                self.children.retain(|_, child| {
                    child.redundant_by_parent(event.version(), true);
                    !child.is_default() // keep only non-default children
                });
            }
        }
    }
}

impl<K, L> DefaultSinkExpansion for UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLogSink,
{
}

impl<K, L> EvalNested<Read<<Self as IsLog>::Value>> for UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Default + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        let mut map = HashMap::default();
        for (k, v) in &self.children {
            let val = v.execute_query(Read::new());
            if val != <L as IsLog>::Value::default() {
                map.insert(k.clone(), val);
            }
        }
        map
    }
}

impl<'a, K, Q, L> EvalNested<Get<'a, K, Q>> for UWMapLog<K, L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
    K: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(&self, q: Get<K, Q>) -> <Get<'a, K, Q> as QueryOperation>::Response {
        if let Some(child) = self.children.get(&q.key) {
            Some(child.execute_query(q.nested_query))
        } else {
            None
        }
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> FuzzMetrics for UWMapLog<K, L>
where
    K: Clone + Debug + Hash + Eq,
    L: FuzzMetrics,
{
    fn structure_metrics(&self) -> StructureMetrics {
        StructureMetrics::object(self.children.values().map(FuzzMetrics::structure_metrics))
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> OpGeneratorNested for UWMapLog<K, L>
where
    L: OpGeneratorNested,
    K: Clone + Debug + Hash + Eq + PartialEq + ValueGenerator,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        use moirai_fuzz::value_generator::ValueGenerator;
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Update,
            Remove,
            Clear,
        }
        let dist = WeightedIndex::new([5, 2, 1]).unwrap();

        let choice = &[Choice::Update, Choice::Remove, Choice::Clear][dist.sample(rng)];
        let key = K::generate(rng, &<K as ValueGenerator>::Config::default());
        match choice {
            Choice::Update => {
                let child_op = if let Some(child) = self.children.get(&key) {
                    child.generate(rng)
                } else {
                    L::new().generate(rng)
                };
                UWMap::Update(key, child_op)
            }
            Choice::Remove => UWMap::Remove(key),
            Choice::Clear => UWMap::Clear,
        }
    }
}

impl<K, L> Display for UWMapLog<K, L>
where
    K: Display + Clone + PartialEq + Eq + Hash,
    L: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (k, v) in &self.children {
            write!(f, "{} => {}", k, v)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_macros::record;
    use moirai_protocol::{
        crdt::query::{Contains, Get, Read},
        replica::IsReplica,
        state::{event_graph::EventGraph, po_log::VecLog},
    };

    use crate::{
        HashMap,
        counter::resettable_counter::Counter,
        // flag::ew_flag::EWFlag,
        list::{
            eg_walker::List,
            nested_list::{NestedList, NestedListLog},
        },
        map::uw_map::{UWMap, UWMapLog},
        set::aw_set::AWSet,
        utils::membership::{triplet_log, twins_log},
    };

    record!(Duet {
        first: VecLog<Counter<i32>>,
        second: VecLog<Counter<i32>>,
    });

    #[test]
    fn nested_query() {
        let (mut replica_a, mut _replica_b) = twins_log::<UWMapLog<String, VecLog<AWSet<i32>>>>();

        let _ = replica_a
            .send(UWMap::Update("a".to_string(), AWSet::Add(10)))
            .unwrap();
        assert_eq!(
            Some(true),
            replica_a.query(Get::new(&"a".to_string(), Contains::<i32>(10)))
        );
    }

    #[test]
    fn simple_uw_map() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<String, VecLog<Counter<i32>>>>();

        let event = replica_a
            .send(UWMap::Update("a".to_string(), Counter::Dec(5)))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update("b".to_string(), Counter::Inc(5)))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update("a".to_string(), Counter::Inc(15)))
            .unwrap();
        replica_b.receive(event);

        let mut map = HashMap::default();
        map.insert(String::from("a"), 10);
        map.insert(String::from("b"), 5);
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_uw_map() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<String, VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(UWMap::Remove("a".to_string())).unwrap();
        let event_b = replica_b
            .send(UWMap::Update("a".to_string(), Counter::Inc(10)))
            .unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut map = HashMap::default();
        map.insert(String::from("a"), 10);
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
        assert_eq!(
            Some(10),
            replica_a.query(Get::new(&"a".to_string(), Read::new()))
        );
    }

    #[test]
    fn uw_map_duet_counter() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<String, DuetLog>>();

        let event = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                Duet::First(Counter::Inc(15)),
            ))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                Duet::First(Counter::Inc(10)),
            ))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update(
                "b".to_string(),
                Duet::Second(Counter::Dec(7)),
            ))
            .unwrap();
        replica_b.receive(event);

        let mut map = HashMap::default();
        map.insert(
            String::from("a"),
            DuetValue {
                first: 25,
                second: 0,
            },
        );
        map.insert(
            String::from("b"),
            DuetValue {
                first: 5,
                second: -7,
            },
        );
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
    }

    #[test]
    fn uw_map_concurrent_duet_counter() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<String, DuetLog>>();

        let event = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                Duet::First(Counter::Inc(15)),
            ))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))))
            .unwrap();
        replica_b.receive(event);

        let mut map = HashMap::default();
        map.insert(
            String::from("a"),
            DuetValue {
                first: 15,
                second: 0,
            },
        );
        map.insert(
            String::from("b"),
            DuetValue {
                first: 5,
                second: 0,
            },
        );
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));

        let event_a = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                Duet::First(Counter::Inc(10)),
            ))
            .unwrap();
        let event_b = replica_b.send(UWMap::Remove("a".to_string())).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let mut map = HashMap::default();
        map.insert(
            String::from("a"),
            DuetValue {
                first: 10,
                second: 0,
            },
        );
        map.insert(
            String::from("b"),
            DuetValue {
                first: 5,
                second: 0,
            },
        );
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));

        let event = replica_a
            .send(UWMap::Update(
                "b".to_string(),
                Duet::Second(Counter::Dec(7)),
            ))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a.send(UWMap::Remove("b".to_string())).unwrap();
        replica_b.receive(event);

        let mut map = HashMap::default();
        map.insert(
            String::from("a"),
            DuetValue {
                first: 10,
                second: 0,
            },
        );
        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
    }

    #[test]
    fn uw_map_deeply_nested() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<
            UWMapLog<String, UWMapLog<i32, UWMapLog<String, VecLog<Counter<i32>>>>>,
        >();

        let event_a_1 = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(2))),
            ))
            .unwrap();

        let event_a_2 = replica_a
            .send(UWMap::Update(
                "b".to_string(),
                UWMap::Update(2, UWMap::Update("f".to_string(), Counter::Dec(20))),
            ))
            .unwrap();

        let event_a_3 = replica_a
            .send(UWMap::Update(
                "a".to_string(),
                UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
            ))
            .unwrap();

        let event_b_1 = replica_b
            .send(UWMap::Update(
                "a".to_string(),
                UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
            ))
            .unwrap();

        let event_b_2 = replica_b
            .send(UWMap::Update(
                "a".to_string(),
                UWMap::Update(2, UWMap::Remove("f".to_string())),
            ))
            .unwrap();

        let event_c_1 = replica_c
            .send(UWMap::Update(
                "a".to_string(),
                UWMap::Update(1, UWMap::Remove("z".to_string())),
            ))
            .unwrap();

        replica_a.receive(event_b_1.clone());
        replica_a.receive(event_b_2.clone());
        replica_a.receive(event_c_1.clone());

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_a_2.clone());
        replica_b.receive(event_a_3.clone());

        replica_c.receive(event_b_2.clone());
        replica_c.receive(event_a_3.clone());
        replica_c.receive(event_b_1.clone());
        replica_c.receive(event_a_2.clone());
        replica_c.receive(event_a_1.clone());

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
        assert_eq!(replica_c.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn uw_map_nested_list_duet() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWMapLog<String, NestedListLog<DuetLog>>>();

        let event_a_1 = replica_a
            .send(UWMap::Update(
                "scores".to_string(),
                NestedList::insert(0, Duet::First(Counter::Inc(10))),
            ))
            .unwrap();
        replica_b.receive(event_a_1.clone());
        replica_c.receive(event_a_1.clone());

        let event_a_2 = replica_a
            .send(UWMap::Update(
                "scores".to_string(),
                NestedList::update(0, Duet::Second(Counter::Inc(3))),
            ))
            .unwrap();
        let event_b_1 = replica_b
            .send(UWMap::Update(
                "scores".to_string(),
                NestedList::insert(1, Duet::Second(Counter::Dec(4))),
            ))
            .unwrap();
        let event_c_1 = replica_c.send(UWMap::Remove("scores".to_string())).unwrap();

        replica_a.receive(event_b_1.clone());
        replica_a.receive(event_c_1.clone());

        replica_b.receive(event_a_2.clone());
        replica_b.receive(event_c_1.clone());

        replica_c.receive(event_b_1.clone());
        replica_c.receive(event_a_2.clone());

        let mut map = HashMap::default();
        map.insert(
            "scores".to_string(),
            vec![DuetValue {
                first: 0,
                second: -4,
            }],
        );

        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
        assert_eq!(map, replica_c.query(Read::new()));
    }

    #[test]
    fn map_nested_eg_walker() {
        let (mut replica_a, mut replica_b) =
            twins_log::<UWMapLog<String, EventGraph<List<char>>>>();

        let event_a = replica_a
            .send(UWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(UWMap::Remove("doc".to_string())).unwrap();
        replica_a.receive(event_b);

        let result = HashMap::default();
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn map_nested_eg_walker_2() {
        let (mut replica_a, _) = twins_log::<UWMapLog<String, EventGraph<List<char>>>>();

        let _ = replica_a
            .send(UWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let _ = replica_a.send(UWMap::Remove("patate".to_string())).unwrap();
        let _ = replica_a.send(UWMap::Clear).unwrap();

        assert_eq!(replica_a.query(Read::new()), HashMap::default());
    }

    #[test]
    fn map_nested_eg_walker_3() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWMapLog<String, EventGraph<List<char>>>>();

        let event_a = replica_a.send(UWMap::Clear).unwrap();
        let event_b = replica_b
            .send(UWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_c = replica_c.send(UWMap::Clear).unwrap();

        replica_c.receive(event_a.clone());
        replica_c.receive(event_b.clone());

        replica_a.receive(event_b);
        replica_a.receive(event_c.clone());

        replica_b.receive(event_a);
        replica_b.receive(event_c);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
        assert_eq!(replica_c.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn map_nested_eg_walker_4() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWMapLog<String, EventGraph<List<char>>>>();

        let event_a_1 = replica_a
            .send(UWMap::Update(
                "vfq".to_string(),
                List::Insert {
                    content: 'q',
                    pos: 0,
                },
            ))
            .unwrap();

        let event_a_2 = replica_a
            .send(UWMap::Update(
                "tdx".to_string(),
                List::Insert {
                    content: 'W',
                    pos: 0,
                },
            ))
            .unwrap();

        replica_c.receive(event_a_1.clone());
        replica_c.receive(event_a_2.clone());
        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);

        let event_c_1 = replica_c.send(UWMap::Remove("qeq".to_string())).unwrap();
        let event_c_2 = replica_c.send(UWMap::Clear).unwrap();

        replica_a.receive(event_c_1.clone());
        replica_a.receive(event_c_2.clone());
        replica_b.receive(event_c_1);
        replica_b.receive(event_c_2);

        let result = HashMap::default();

        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_c.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_uw_map() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        type UWMapNested = UWMapLog<String, EventGraph<List<char>>>;

        let run = RunConfig::new(0.4, 8, 1_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<UWMapNested>::new("uw_map", runs, true, |a, b| a == b, false);

        fuzzer::<UWMapNested>(config);
    }
}

impl<K, O> Boxer<UWMap<K, O>> for UWMap<K, Box<O>> {
    fn boxer(self) -> UWMap<K, O> {
        match self {
            UWMap::Update(k, v) => UWMap::Update(k, *v),
            UWMap::Remove(k) => UWMap::Remove(k),
            UWMap::Clear => UWMap::Clear,
        }
    }
}

impl<K, O> Boxer<UWMap<K, Box<O>>> for UWMap<K, O> {
    fn boxer(self) -> UWMap<K, Box<O>> {
        match self {
            UWMap::Update(k, v) => UWMap::Update(k, Box::new(v)),
            UWMap::Remove(k) => UWMap::Remove(k),
            UWMap::Clear => UWMap::Clear,
        }
    }
}
