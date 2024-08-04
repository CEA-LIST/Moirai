use std::fmt::Debug;
use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

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

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, path: &Path) -> Self::Value {
        let mut f_log: POLog<F> = POLog::new();
        let mut s_log: POLog<S> = POLog::new();
        for op in state.iter() {
            match op.as_ref() {
                Duet::First(fo) => f_log.new_stable(Rc::new(fo.clone())),
                Duet::Second(so) => s_log.new_stable(Rc::new(so.clone())),
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

        let event = tcsb_a.tc_bcast(Duet::First(Counter::Dec(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(Duet::First(Counter::Inc(15)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(Duet::Second(Counter::Inc(5)));
        tcsb_b.tc_deliver(event);

        let result = (10, 5);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
