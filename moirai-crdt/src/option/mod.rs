#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::{FuzzMetrics, StructureMetrics};
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    replica::ReplicaIdx,
    state::{
        log::IsLog,
        sink::{IsLogSink, ObjectPath, Sink, SinkCollector},
    },
    utils::{intern_str::Interner, translate_ids::TranslateIds},
};
#[cfg(feature = "fuzz")]
use rand::RngExt;

#[derive(Clone, Debug)]
pub enum Optional<O> {
    Set(O),
    Unset,
}

impl<O> TranslateIds for Optional<O>
where
    O: TranslateIds,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        match self {
            Optional::Set(o) => Optional::Set(o.translate_ids(from, interner)),
            Optional::Unset => Optional::Unset,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OptionLog<L> {
    child: Option<L>,
}

impl<L> Default for OptionLog<L> {
    fn default() -> Self {
        Self { child: None }
    }
}

impl<L> OptionLog<L> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn child(&self) -> Option<&L> {
        self.child.as_ref()
    }
}

impl<L> IsLog for OptionLog<L>
where
    L: IsLog,
{
    type Value = Option<L::Value>;
    type Op = Optional<L::Op>;

    fn new() -> Self {
        Self::default()
    }

    fn is_enabled(&self, _op: &Self::Op) -> bool {
        true
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            Optional::Set(o) => {
                let child_op = Event::unfold(event, o);
                self.child.get_or_insert_with(L::default).effect(child_op);
                self.child = self.child.take().filter(|c| !c.is_default());
            }
            Optional::Unset => {
                if let Some(child) = self.child.as_mut() {
                    child.redundant_by_parent(event.version(), true);
                    if child.is_default() {
                        self.child = None;
                    }
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        if let Some(ref mut child) = self.child {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        if let Some(ref mut child) = self.child {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        match self.child {
            Some(ref child) => child.is_default(),
            None => true,
        }
    }
}

impl<L> IsLogSink for OptionLog<L>
where
    L: IsLogSink,
{
    fn effect_with_sink(
        &mut self,
        event: Event<Self::Op>,
        path: ObjectPath,
        sink: &mut SinkCollector,
    ) {
        match event.op().clone() {
            Optional::Set(o) => {
                if self.child.is_some() {
                    sink.collect(Sink::update(path.clone()));
                } else {
                    sink.collect(Sink::create(path.clone()));
                }
                let child_op = Event::unfold(event, o);
                self.child.get_or_insert_with(L::default).effect(child_op);
                self.child = self.child.take().filter(|c| !c.is_default());
            }
            Optional::Unset => {
                sink.collect(Sink::delete(path.clone()));
                if let Some(child) = self.child.as_mut() {
                    child.redundant_by_parent(event.version(), true);
                    if child.is_default() {
                        self.child = None;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "fuzz")]
impl<L> FuzzMetrics for OptionLog<L>
where
    L: FuzzMetrics,
{
    fn structure_metrics(&self) -> StructureMetrics {
        self.child
            .as_ref()
            .map(FuzzMetrics::structure_metrics)
            .unwrap_or_else(StructureMetrics::empty)
    }
}

#[cfg(feature = "fuzz")]
impl<L> OpGeneratorNested for OptionLog<L>
where
    L: OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl rand::Rng) -> Self::Op {
        match &self.child {
            Some(child) => {
                if rng.random_bool(1.0 / 5.0) {
                    Optional::Unset
                } else {
                    Optional::Set(child.generate(rng))
                }
            }
            None => Optional::Set(<L as OpGeneratorNested>::generate(&L::default(), rng)),
        }
    }
}

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for OptionLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: Default + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        match self.child {
            Some(ref child) => Some(child.execute_query(Read::new())),
            None => Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog};

    use crate::{
        counter::resettable_counter::Counter,
        option::{OptionLog, Optional},
        utils::membership::twins_log,
    };

    #[test]
    fn simple_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event = replica_a.send(Optional::Set(Counter::Inc(2))).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Optional::Unset).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Optional::Set(Counter::Inc(5))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some(5));
        assert_eq!(replica_b.query(Read::new()), Some(5));
    }

    #[test]
    fn concurrent_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(Optional::Set(Counter::Inc(2))).unwrap();
        let event_b = replica_b.send(Optional::Set(Counter::Inc(5))).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), Some(7));
        assert_eq!(replica_b.query(Read::new()), Some(7));
    }

    #[test]
    fn concurrent_unset_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(Optional::Set(Counter::Inc(10))).unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a.send(Optional::Set(Counter::Inc(2))).unwrap();
        let event_b = replica_b.send(Optional::Unset).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), Some(2));
        assert_eq!(replica_b.query(Read::new()), Some(2));
    }
}
