use crate::{
    crdt::counter::resettable_counter::Counter,
    protocol::{event::tagged_op::TaggedOp, state::po_log::POLog},
};

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<V> = rustc_hash::FxHashSet<V>;

pub fn set_from_slice<T: Eq + std::hash::Hash + Clone>(slice: &[T]) -> HashSet<T> {
    slice.iter().cloned().collect()
}

#[cfg(feature = "crdt")]
pub mod crdt;
#[cfg(feature = "fuzz")]
pub mod fuzz;
pub mod macros;
pub mod protocol;
pub mod utils;

record!(Duet {
    first: POLog::<Counter<i32>, Vec<TaggedOp<Counter<i32>>>>,
    second: POLog::<Counter<i32>, Vec<TaggedOp<Counter<i32>>>>,
});
