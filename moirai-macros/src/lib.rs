pub mod record;
pub mod union;
pub use moirai_protocol;
pub use paste;
pub mod typed_graph;

pub type FxHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type FxHashSet<V> = rustc_hash::FxHashSet<V>;
