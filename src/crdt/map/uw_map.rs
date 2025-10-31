use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::{fuzz::config::OpConfig, protocol::state::log::IsLogFuzz};
use crate::{
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::EvalNested,
            query::{Get, QueryOperation, Read},
        },
        event::Event,
        state::log::IsLog,
    },
    HashMap,
};

#[derive(Clone, Debug)]
pub enum UWMap<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

impl<K, O> UWMap<K, Box<O>> {
    pub fn boxed(op: UWMap<K, O>) -> UWMap<K, Box<O>> {
        match op {
            UWMap::Update(k, v) => UWMap::Update(k, Box::new(v)),
            UWMap::Remove(k) => UWMap::Remove(k),
            UWMap::Clear => UWMap::Clear,
        }
    }

    pub fn unboxed(self) -> UWMap<K, O> {
        match self {
            UWMap::Update(k, v) => UWMap::Update(k, *v),
            UWMap::Remove(k) => UWMap::Remove(k),
            UWMap::Clear => UWMap::Clear,
        }
    }
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
    pub(crate) children: HashMap<K, L>,
}

impl<K: Clone + Debug + Eq + Hash, L> Default for UWMapLog<K, L> {
    fn default() -> Self {
        Self {
            children: Default::default(),
        }
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
            }
            UWMap::Remove(k) => {
                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), true);
                }
            }
            UWMap::Clear => {
                for child in self.children.values_mut() {
                    child.redundant_by_parent(event.version(), true);
                }
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

impl<K, Q, L> EvalNested<Get<K, Q>> for UWMapLog<K, L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
    K: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(&self, q: Get<K, Q>) -> <Get<K, Q> as QueryOperation>::Response {
        if let Some(child) = self.children.get(&q.key) {
            Some(child.execute_query(q.nested_query))
        } else {
            None
        }
    }
}

#[cfg(feature = "fuzz")]
impl<L> IsLogFuzz for UWMapLog<String, L>
where
    L: IsLogFuzz,
{
    fn generate_op(&self, rng: &mut impl RngCore, config: &OpConfig) -> Self::Op {
        let choice =
            rand::seq::IteratorRandom::choose(["Update", "Remove", "Clear"].iter(), rng).unwrap();
        match choice.as_ref() {
            "Update" => {
                let key = format!("{}", rng.next_u64() % (config.max_elements as u64));
                let child_op = if let Some(child) = self.children.get(&key) {
                    child.generate_op(rng, config)
                } else {
                    L::new().generate_op(rng, config)
                };
                UWMap::Update(key, child_op)
            }
            "Remove" => {
                let key = format!("{}", rng.next_u64() % (config.max_elements as u64));
                UWMap::Remove(key)
            }
            "Clear" => UWMap::Clear,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            map::uw_map::{UWMap, UWMapLog},
            set::aw_set::AWSet,
            test_util::{triplet_log, twins_log},
        },
        protocol::{
            crdt::query::{Contains, Get, Read},
            event::tagged_op::TaggedOp,
            replica::IsReplica,
            state::po_log::{POLog, VecLog},
        },
        record, HashMap,
    };

    record!(Duet {
        first: POLog::<Counter<i32>, Vec<TaggedOp<Counter<i32>>>>,
        second: POLog::<Counter<i32>, Vec<TaggedOp<Counter<i32>>>>,
    });

    #[test]
    fn nested_query() {
        let (mut replica_a, mut _replica_b) = twins_log::<UWMapLog<String, VecLog<AWSet<i32>>>>();

        let _ = replica_a
            .send(UWMap::Update("a".to_string(), AWSet::Add(10)))
            .unwrap();
        assert_eq!(
            Some(true),
            replica_a.query(Get::new("a".to_string(), Contains::<i32>(10)))
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
            replica_a.query(Get::new("a".to_string(), Read::new()))
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

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_uw_map() {
        // init_tracing();

        use crate::{
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        type UWMapNested = UWMapLog<String, UWMapLog<String, VecLog<Counter<i32>>>>;

        let run = RunConfig::new(0.4, 8, 100_000, None, None);
        let runs = vec![run.clone(); 1];

        let op_config = OpConfig {
            max_elements: 10_000,
        };

        let config =
            FuzzerConfig::<UWMapNested>::new("uw_map", runs, op_config, true, |a, b| a == b, None);

        fuzzer::<UWMapNested>(config);
    }
}
