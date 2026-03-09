use crate::HashSet;

pub mod membership;

pub fn set_from_slice<T: Eq + std::hash::Hash + Clone>(slice: &[T]) -> HashSet<T> {
    slice.iter().cloned().collect()
}
