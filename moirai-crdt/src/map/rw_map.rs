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
        query::{Contains, Get, QueryOperation, Read},
    },
    event::Event,
    state::{cache::CacheCell, effect_context::EffectContext, log::IsLog, po_log::VecLog},
    utils::{
        boxer::Boxer,
        intern_str::{InternalizeOp, Interner},
    },
};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::{HashMap, set::rw_set::RWSet};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum RWMap<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

#[derive(Clone, Debug)]
pub struct RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    set: VecLog<RWSet<K>>,
    children: HashMap<K, L>,
    read_cache: CacheCell<HashMap<K, L::Value>>,
}

impl<K, L> Default for RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    fn default() -> Self {
        Self {
            set: Default::default(),
            children: Default::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<K, L> RWMapLog<K, L>
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

    pub fn set(&self) -> &VecLog<RWSet<K>> {
        &self.set
    }

    pub fn get_child(&self, key: &K) -> Option<&L> {
        self.children.get(key)
    }
}

impl<K, L> IsLog for RWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    type Value = HashMap<K, L::Value>;
    type Op = RWMap<K, L::Op>;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            RWMap::Update(k, v) => {
                // Query the RWSet
                let was_live = self.is_key_live(&k);
                let set_op = Event::unfold(event.clone(), RWSet::Add(k.clone()));

                // Effect of the RWSet will determine whether the key is live or not, which in turn determines how we effect the child log
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

                // If the child op is not redundant
                if self.is_key_live(&k) {
                    let child_op = Event::unfold(event, v);

                    if ctx.is_owned() {
                        ctx.with_map_entry(
                            || format!("{:?}", k),
                            |ctx| {
                                if was_live {
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
                }

                self.refresh_cached_key(&k);
            }
            RWMap::Remove(k) => {
                let set_op = Event::unfold(event.clone(), RWSet::Remove(k.clone()));
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

                if ctx.is_owned() {
                    ctx.with_map_entry(|| format!("{:?}", k), |ctx| ctx.delete());
                }

                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), false);
                }
                self.refresh_cached_key(&k);
            }
            RWMap::Clear => {
                self.read_cache.invalidate();
                let set_op = Event::unfold(event.clone(), RWSet::Clear);
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

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
        self.set.stabilize(version);
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        self.set.redundant_by_parent(version, conservative);
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.set.is_default() && self.children.values().all(IsLog::is_default)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        match op {
            RWMap::Update(k, v) => {
                if self.is_key_live(k) {
                    self.children
                        .get(k)
                        .map_or_else(|| L::default().is_enabled(v), |child| child.is_enabled(v))
                } else {
                    L::default().is_enabled(v)
                }
            }
            RWMap::Remove(_) | RWMap::Clear => Ok(()),
        }
    }
}

impl<K, L> EvalNested<Read<<Self as IsLog>::Value>> for RWMapLog<K, L>
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

impl<K, L> BorrowedRead for RWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn read_ref(&self) -> &Self::Value {
        self.read_cache.get_or_compute(|| self.read_uncached())
    }
}

impl<K, L> RWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn read_uncached(&self) -> <Self as IsLog>::Value {
        let mut map = HashMap::default();
        for (k, v) in &self.children {
            if self.is_key_live(k) {
                let val = v.execute_query(Read::new());
                if val != <L as IsLog>::Value::default() {
                    map.insert(k.clone(), val);
                }
            }
        }
        map
    }

    fn refresh_cached_key(&mut self, key: &K) {
        if self.read_cache.get().is_none() {
            return;
        }

        let value = if self.is_key_live(key) {
            self.children.get(key).and_then(|child| {
                let value = child.execute_query(Read::new());
                (value != <L as IsLog>::Value::default()).then_some(value)
            })
        } else {
            None
        };

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

    fn is_key_live(&self, key: &K) -> bool {
        self.set.execute_query(Contains(key.clone()))
    }
}

impl<'a, K, Q, L> EvalNested<Get<'a, K, Q>> for RWMapLog<K, L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q> + EvalNested<Read<<L as IsLog>::Value>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    <L as IsLog>::Value: Clone + Default + PartialEq,
{
    fn execute_query(&self, q: Get<K, Q>) -> <Get<'a, K, Q> as QueryOperation>::Response {
        if !self.is_key_live(q.key) {
            return None;
        }

        if let Some(child) = self.children.get(q.key) {
            Some(child.execute_query(q.nested_query))
        } else {
            None
        }
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> OpGeneratorNested for RWMapLog<K, L>
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
                RWMap::Update(key, child_op)
            }
            Choice::Remove => RWMap::Remove(key),
            Choice::Clear => RWMap::Clear,
        }
    }
}

impl<K, L> Display for RWMapLog<K, L>
where
    K: Display + Debug + Clone + PartialEq + Eq + Hash,
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
    use moirai_protocol::{
        crdt::query::{Get, Read},
        replica::IsReplica,
        state::{graph_log::GraphLog, po_log::VecLog},
    };

    use crate::{
        HashMap,
        counter::resettable_counter::Counter,
        list::eg_walker::List,
        map::rw_map::{RWMap, RWMapLog},
        utils::membership::{triplet_log, twins_log},
    };

    type CounterMap = RWMapLog<String, VecLog<Counter<i32>>>;
    type ListMap = RWMapLog<String, GraphLog<List<char>>>;

    #[test]
    fn concurrent_remove_wins_over_update() {
        let (mut replica_a, mut replica_b) = twins_log::<CounterMap>();
        let key = "a".to_string();

        let remove = replica_a.send(RWMap::Remove(key.clone())).unwrap();
        let update = replica_b
            .send(RWMap::Update(key.clone(), Counter::Inc(10)))
            .unwrap();

        replica_a.receive(update);
        replica_b.receive(remove);

        let expected: HashMap<String, i32> = HashMap::default();
        assert_eq!(replica_a.query(Read::new()), expected);
        assert_eq!(replica_b.query(Read::new()), expected);
        assert_eq!(replica_a.query(Get::new(&key, Read::new())), None);
        assert_eq!(replica_b.query(Get::new(&key, Read::new())), None);
    }

    #[test]
    fn update_after_remove_recreates_key() {
        let (mut replica_a, mut replica_b) = twins_log::<CounterMap>();
        let key = "a".to_string();

        let remove = replica_a.send(RWMap::Remove(key.clone())).unwrap();
        replica_b.receive(remove);

        let update = replica_b
            .send(RWMap::Update(key.clone(), Counter::Inc(7)))
            .unwrap();
        replica_a.receive(update);

        let mut expected = HashMap::default();
        expected.insert(key.clone(), 7);

        assert_eq!(replica_a.query(Read::new()), expected);
        assert_eq!(replica_b.query(Read::new()), expected);
        assert_eq!(replica_a.query(Get::new(&key, Read::new())), Some(7));
        assert_eq!(replica_b.query(Get::new(&key, Read::new())), Some(7));
    }

    #[test]
    fn map_nested_eg_walker() {
        let (mut replica_a, mut replica_b) = twins_log::<ListMap>();

        let event_a = replica_a
            .send(RWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(RWMap::Remove("doc".to_string())).unwrap();
        replica_a.receive(event_b);

        let result = HashMap::default();
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn map_nested_eg_walker_2() {
        let (mut replica_a, _) = twins_log::<ListMap>();

        let _ = replica_a
            .send(RWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let _ = replica_a.send(RWMap::Remove("patate".to_string())).unwrap();
        let _ = replica_a.send(RWMap::Clear).unwrap();

        assert_eq!(replica_a.query(Read::new()), HashMap::default());
    }

    /// Execution trace:
    /// replica_a: RWMap::Clear@v1
    /// replica_b: RWMap::Update("doc", List::Insert { content: 'A', pos: 0 })@v2
    /// replica_c: RWMap::Clear@v3
    /// replica_c receives v1, v2
    /// replica_a receives v2, v3
    /// replica_b receives v1, v3
    /// v1 || v2, v1 || v3, v2 || v3
    #[test]
    fn map_nested_eg_walker_3() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<ListMap>();

        let event_a = replica_a.send(RWMap::Clear).unwrap();
        let event_b = replica_b
            .send(RWMap::Update(
                "doc".to_string(),
                List::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_c = replica_c.send(RWMap::Clear).unwrap();

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
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<ListMap>();

        let event_a_1 = replica_a
            .send(RWMap::Update(
                "foo".to_string(),
                List::Insert {
                    content: 'a',
                    pos: 0,
                },
            ))
            .unwrap();

        let event_a_2 = replica_a
            .send(RWMap::Update(
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

        let event_c_1 = replica_c.send(RWMap::Remove("alice".to_string())).unwrap();
        let event_c_2 = replica_c.send(RWMap::Clear).unwrap();

        replica_a.receive(event_c_1.clone());
        replica_a.receive(event_c_2.clone());
        replica_b.receive(event_c_1);
        replica_b.receive(event_c_2);

        let result = HashMap::default();

        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_c.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    /// Execution trace:
    /// digraph {
    ///     0 [ label="[Remove('ym')@(0:1)]"]
    ///     1 [ label="[Clear@(0:2)]"]
    ///     2 [ label="[Remove('hwxt')@(0:3)]"]
    ///     3 [ label="[Update('ym', Insert { content: 'r', pos: 0 })@(1:1)]"]
    ///     4 [ label="[Update('mza', Insert { content: 's', pos: 0 })@(0:4)]"]
    ///     5 [ label="[Update('xz', Insert { content: 'K', pos: 0 })@(0:5)]"]
    ///     6 [ label="[Remove('hcl')@(0:6)]"]
    ///     7 [ label="[Update('snif', Insert { content: 'R', pos: 0 })@(1:2)]"]
    ///     0 -> 1 [ ]  1 -> 2 [ ]  2 -> 4 [ ]  4 -> 5 [ ]  5 -> 6 [ ]  3 -> 7 [ ]
    /// }
    #[test]
    fn map_nested_eg_walker_5() {
        let (mut replica_a, mut replica_b) = twins_log::<ListMap>();

        let event_a_1 = replica_a.send(RWMap::Remove("ym".to_string())).unwrap();
        let event_a_2 = replica_a.send(RWMap::Clear).unwrap();
        let event_a_3 = replica_a.send(RWMap::Remove("hwxt".to_string())).unwrap();
        let event_a_4 = replica_a
            .send(RWMap::Update(
                "mza".to_string(),
                List::Insert {
                    content: 's',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_a_5 = replica_a
            .send(RWMap::Update(
                "xz".to_string(),
                List::Insert {
                    content: 'K',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_a_6 = replica_a.send(RWMap::Remove("hcl".to_string())).unwrap();

        let event_b_1 = replica_b
            .send(RWMap::Update(
                "ym".to_string(),
                List::Insert {
                    content: 'r',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_b_2 = replica_b
            .send(RWMap::Update(
                "snif".to_string(),
                List::Insert {
                    content: 'R',
                    pos: 0,
                },
            ))
            .unwrap();

        replica_a.receive(event_b_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);
        replica_b.receive(event_a_4);
        replica_b.receive(event_a_5);
        replica_b.receive(event_a_6);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_map() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run = RunConfig::new(0.6, 2, 8, None, None, true, false);
        let runs = vec![run.clone(); 10_000];

        let config = FuzzerConfig::<ListMap>::new("rw_map", runs, true, |a, b| a == b, false);

        fuzzer::<ListMap>(config);
    }
}

impl<K, O> Boxer<RWMap<K, O>> for RWMap<K, Box<O>> {
    fn boxer(self) -> RWMap<K, O> {
        match self {
            RWMap::Update(k, v) => RWMap::Update(k, *v),
            RWMap::Remove(k) => RWMap::Remove(k),
            RWMap::Clear => RWMap::Clear,
        }
    }
}

impl<K, O> Boxer<RWMap<K, Box<O>>> for RWMap<K, O> {
    fn boxer(self) -> RWMap<K, Box<O>> {
        match self {
            RWMap::Update(k, v) => RWMap::Update(k, Box::new(v)),
            RWMap::Remove(k) => RWMap::Remove(k),
            RWMap::Clear => RWMap::Clear,
        }
    }
}

impl<K, O> InternalizeOp for RWMap<K, O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            RWMap::Update(k, v) => RWMap::Update(k, v.internalize(interner)),
            RWMap::Remove(k) => RWMap::Remove(k),
            RWMap::Clear => RWMap::Clear,
        }
    }
}
