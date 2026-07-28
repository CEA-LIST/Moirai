pub mod broadcast;
pub mod clock;
pub mod crdt;
pub mod event;
pub mod replica;
pub mod state;
pub mod utils;

type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type HashSet<V> = rustc_hash::FxHashSet<V>;
