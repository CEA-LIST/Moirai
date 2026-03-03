pub mod bag;
pub mod counter;
pub mod flag;
pub mod graph;
pub mod json;
pub mod list;
pub mod map;
pub mod model;
pub mod option;
pub mod policy;
pub mod register;
pub mod set;
pub mod utils;

type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type HashSet<V> = rustc_hash::FxHashSet<V>;
