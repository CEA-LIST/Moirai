use crate::FxHashSet;

pub mod bootstrap;

pub fn set_from_slice<T: Eq + std::hash::Hash + Clone>(slice: &[T]) -> FxHashSet<T> {
    slice.iter().cloned().collect()
}
