pub mod record;
pub mod union;
// pub use heck;
pub use moirai_protocol;
pub use paste;
pub mod typed_graph;

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<V> = rustc_hash::FxHashSet<V>;
