pub mod config;
mod display;
pub mod fuzzer;
mod metrics;
pub mod op_generator;
mod runner;
mod serialize;
mod utils;
pub mod value_generator;

type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
