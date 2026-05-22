use std::convert::Infallible;

#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::RngExt;

#[derive(Clone, Debug)]
pub enum Optional<O> {
    Set(O),
    Unset,
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

impl<O> InternalizeOp for Optional<O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            Optional::Set(o) => Optional::Set(o.internalize(interner)),
            Optional::Unset => Optional::Unset,
        }
    }
}

impl<L> IsLog for OptionLog<L>
where
    L: IsLog,
{
    type Value = Option<L::Value>;
    type Op = Optional<L::Op>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            Optional::Set(o) => {
                if self.child.is_some() {
                    ctx.update();
                } else {
                    ctx.create();
                }
                let child_op = Event::unfold(event, o);
                ctx.with_delegated(|ctx| {
                    self.child
                        .get_or_insert_with(L::default)
                        .effect(child_op, ctx);
                });
                self.child = self.child.take().filter(|c| !c.is_default());
            }
            Optional::Unset => {
                if ctx.is_owned() {
                    ctx.delete();
                }
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
