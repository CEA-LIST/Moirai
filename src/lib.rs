pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;

#[cfg(feature = "crdt")]
pub mod crdt;
pub mod fuzz;
pub mod macros;
pub mod protocol;
pub mod utils;
