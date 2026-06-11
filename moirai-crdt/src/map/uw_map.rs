use std::{
    fmt::{Debug, Display, Formatter},
    hash::Hash,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGeneratorNested, value_generator::ValueGenerator};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::{BorrowedRead, EvalNested},
        query::{Get, QueryOperation, Read},
    },
    event::Event,
    state::{cache::CacheCell, effect_context::EffectContext, log::IsLog},
    utils::{
        boxer::Boxer,
        intern_str::{InternalizeOp, Interner},
    },
};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::HashMap;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum UWMap<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L>
where
    K: Clone + Eq + Hash,
    L: IsLog,
{
    children: HashMap<K, L>,
    read_cache: CacheCell<HashMap<K, L::Value>>,
}

impl<K, L> Default for UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    fn default() -> Self {
        Self {
            children: Default::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<K, L> UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
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

impl<K, O> InternalizeOp for UWMap<K, O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            UWMap::Update(k, v) => UWMap::Update(k, v.internalize(interner)),
            UWMap::Remove(k) => UWMap::Remove(k),
            UWMap::Clear => UWMap::Clear,
        }
    }
}

impl<K, L> IsLog for UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    type Value = HashMap<K, L::Value>;
    type Op = UWMap<K, L::Op>;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            UWMap::Update(k, v) => {
                let owns_path = ctx.is_owned();
                let existed = self.children.contains_key(&k);
                let child_op = Event::unfold(event, v);

                if owns_path {
                    ctx.with_map_entry(
                        || format!("{:?}", k),
                        |ctx| {
                            if existed {
                                ctx.update();
                            } else {
                                ctx.create();
                            }

                            self.children
                                .entry(k.clone())
                                .or_default()
                                .effect(child_op, ctx);
                        },
                    );
                } else {
                    ctx.with_owned(|ctx| {
                        self.children
                            .entry(k.clone())
                            .or_default()
                            .effect(child_op, ctx);
                    });
                }

                self.refresh_cached_key(&k);
            }
            UWMap::Remove(k) => {
                if ctx.is_owned() {
                    ctx.with_map_entry(|| format!("{:?}", k), |ctx| ctx.delete());
                }

                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), true);
                }
                self.refresh_cached_key(&k);
            }
            UWMap::Clear => {
                self.read_cache.invalidate();
                if ctx.is_owned() {
                    ctx.delete();
                }
                for child in self.children.values_mut() {
                    child.redundant_by_parent(event.version(), true);
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        self.read_cache.invalidate();
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.children.is_empty()
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        match op {
            UWMap::Update(k, v) => self
                .children
                .get(k)
                .map_or_else(|| Ok(()), |child| child.is_enabled(v)),
            UWMap::Remove(_) | UWMap::Clear => Ok(()),
        }
    }
}

impl<K, L> EvalNested<Read<<Self as IsLog>::Value>> for UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        BorrowedRead::read_ref(self).clone()
    }
}

impl<K, L> BorrowedRead for UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn read_ref(&self) -> &Self::Value {
        self.read_cache.get_or_compute(|| self.read_uncached())
    }
}

impl<K, L> UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn read_uncached(&self) -> <Self as IsLog>::Value {
        let mut map = HashMap::default();
        for (k, v) in &self.children {
            let val = v.execute_query(Read::new());
            if val != <L as IsLog>::Value::default() {
                map.insert(k.clone(), val);
            }
        }
        map
    }

    fn refresh_cached_key(&mut self, key: &K) {
        if self.read_cache.get().is_none() {
            return;
        }

        let value = self.children.get(key).and_then(|child| {
            let value = child.execute_query(Read::new());
            (value != <L as IsLog>::Value::default()).then_some(value)
        });

        if let Some(cached) = self.read_cache.get_mut() {
            match value {
                Some(value) => {
                    cached.insert(key.clone(), value);
                }
                None => {
                    cached.remove(key);
                }
            }
        }
    }
}

impl<'a, K, Q, L> EvalNested<Get<'a, K, Q>> for UWMapLog<K, L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q> + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn execute_query(&self, q: Get<K, Q>) -> <Get<'a, K, Q> as QueryOperation>::Response {
        if let Some(child) = self.children.get(q.key) {
            Some(child.execute_query(q.nested_query))
        } else {
            None
        }
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> OpGeneratorNested for UWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>> + OpGeneratorNested,
    K: Clone + Debug + Hash + Eq + PartialEq + ValueGenerator,
    <L as IsLog>::Value: Clone + Default + PartialEq,
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
    L: IsLog + Display,
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
        state::{graph_log::GraphLog, po_log::VecLog},
    };

    use crate::{
        HashMap,
        counter::resettable_counter::Counter,
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
            vec![
                DuetValue {
                    first: 0,
                    second: 3,
                },
                DuetValue {
                    first: 0,
                    second: -4,
                },
            ],
        );

        assert_eq!(map, replica_a.query(Read::new()));
        assert_eq!(map, replica_b.query(Read::new()));
        assert_eq!(map, replica_c.query(Read::new()));
    }

    #[test]
    fn map_nested_eg_walker() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<String, GraphLog<List<char>>>>();

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
        let (mut replica_a, _) = twins_log::<UWMapLog<String, GraphLog<List<char>>>>();

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
            triplet_log::<UWMapLog<String, GraphLog<List<char>>>>();

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

        let mut result = HashMap::default();
        result.insert("doc".to_string(), vec!['A']);

        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_c.query(Read::new()), result);
    }

    #[test]
    fn map_nested_eg_walker_4() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWMapLog<String, GraphLog<List<char>>>>();

        let event_a_1 = replica_a
            .send(UWMap::Update(
                "foo".to_string(),
                List::Insert {
                    content: 'a',
                    pos: 0,
                },
            ))
            .unwrap();

        let event_a_2 = replica_a
            .send(UWMap::Update(
                "bar".to_string(),
                List::Insert {
                    content: 'b',
                    pos: 0,
                },
            ))
            .unwrap();

        replica_c.receive(event_a_1.clone());
        replica_c.receive(event_a_2.clone());
        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);

        let event_c_1 = replica_c.send(UWMap::Remove("alice".to_string())).unwrap();
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

        type UWMapNested = UWMapLog<String, GraphLog<List<char>>>;

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
