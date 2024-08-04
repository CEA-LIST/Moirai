use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::pure_crdt::PureCRDT;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

#[derive(Clone, Debug)]
pub enum MembershipSet<V> {
    Add(V),
    Remove(V),
}

impl<V> PureCRDT for MembershipSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r(event: &Event<Self>, state: &POLog<Self>) -> bool {
        match &event.op {
            MembershipSet::Remove(i) => {
                let mut flag: bool = true;
                for o in state.iter() {
                    match o.as_ref() {
                        MembershipSet::Add(j) => {
                            if j == i {
                                flag = false;
                            }
                        }
                        MembershipSet::Remove(j) => {
                            if j == i {
                                return true;
                            }
                        }
                    }
                }
                flag
            }
            MembershipSet::Add(i) => {
                return state.iter().any(|o| match o.as_ref() {
                    MembershipSet::Add(j) => j == i,
                    _ => false,
                });
            }
        }
    }

    fn r_zero(_: &Event<Self>, _: &Event<Self>) -> bool {
        false
    }

    fn r_one(_: &Event<Self>, _: &Event<Self>) -> bool {
        false
    }

    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>) {
        if let Some(op) = state.unstable.get(metadata) {
            if let MembershipSet::Remove(_) = op.as_ref() {
                state.unstable.remove(metadata);
            }
        }
    }

    fn eval(state: &POLog<Self>, _: &Path) -> Self::Value {
        let mut set = Self::Value::new();
        for o in state.iter() {
            if let MembershipSet::Add(v) = o.as_ref() {
                set.insert(v.clone());
            }
        }
        set
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::crdt::{membership_set::MembershipSet, test_util::twins};

//     #[test_log::test]
//     fn simple_membership_set() {}
// }
