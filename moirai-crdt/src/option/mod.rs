use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::log::IsLog,
};

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
