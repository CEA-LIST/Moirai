pub mod config;
mod display;
pub mod fuzzer;
mod metrics;
mod runner;
mod serialize;
mod utils;
pub mod value_generator;

type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
