use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::protocol::tcsb::RedundantRelation;
use crate::protocol::utils::prune_redundant_events;
use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT};

#[derive(Clone, Debug)]
pub enum Duet<F, S>
where
    F: PureCRDT,
    S: PureCRDT,
{
    First(F),
    Second(S),
}

impl<F, S> PureCRDT for Duet<F, S>
where
    F: PureCRDT + Debug,
    S: PureCRDT + Debug,
{
    type Value = (F::Value, S::Value);

    fn r(_event: &Event<Self>, _state: &POLog<Self>) -> bool {
        false
    }

    fn r_zero(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn r_one(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn effect(event: &Event<Self>, state: &POLog<Self>) -> (bool, Vec<usize>, Vec<Metadata>) {
        let (keep, prune_fn): (bool, RedundantRelation<Self>) = if Self::r(event, state) {
            (false, Self::r_zero)
        } else {
            (true, Self::r_one)
        };

        let (remove_stable_by_index, remove_unstable_by_key) =
            prune_redundant_events(event, state, prune_fn);

        let (f_log, s_log) = state.iter().fold(
            (POLog::new(), POLog::new()),
            |(mut f_log, mut s_log), op| {
                match op.as_ref() {
                    Duet::First(fo) => f_log.new_stable(Arc::new(fo.clone())),
                    Duet::Second(so) => s_log.new_stable(Arc::new(so.clone())),
                }
                (f_log, s_log)
            },
        );

        let (nested_keep, stable, unstable) = match &event.op {
            Duet::First(fo) => F::effect(&Event::new(fo.clone(), event.metadata.clone()), &f_log),
            Duet::Second(so) => S::effect(&Event::new(so.clone(), event.metadata.clone()), &s_log),
        };

        (
            nested_keep && keep,
            [remove_stable_by_index, stable].concat(),
            [remove_unstable_by_key, unstable].concat(),
        )
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, path: &Path) -> Self::Value {
        let mut f_log: POLog<F> = POLog::new();
        let mut s_log: POLog<S> = POLog::new();
        for op in state.iter() {
            match op.as_ref() {
                Duet::First(fo) => f_log.new_stable(Arc::new(fo.clone())),
                Duet::Second(so) => s_log.new_stable(Arc::new(so.clone())),
            }
        }
        (
            F::eval(&f_log, &path.join("first")),
            S::eval(&s_log, &path.join("second")),
        )
    }

    fn to_path(op: &Self) -> PathBuf {
        match op {
            Duet::First(fo) => PathBuf::from("first").join(F::to_path(fo)),
            Duet::Second(so) => PathBuf::from("second").join(S::to_path(so)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{counter::Counter, duet::Duet, test_util::twins};

    #[test_log::test]
    fn simple_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Duet<Counter<i32>, Counter<i32>>>();

        let event = tcsb_a.tc_bcast_op(Duet::First(Counter::Dec(5)));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(Duet::First(Counter::Inc(15)));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(Duet::Second(Counter::Inc(5)));
        tcsb_b.tc_deliver_op(event);

        let result = (10, 5);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
