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
