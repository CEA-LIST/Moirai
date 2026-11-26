//! Run with cargo run --manifest-path moirai-network-node/Cargo.toml --example benchmark.
mod commit_set_benchmark;
//mod commit_btree_set_benchmark;
mod control;
mod measurement;
mod workload;

fn main() -> std::io::Result<()> {
    commit_set_benchmark::main()
    // Uncomment (and comment the above line) to run btree_set benchmark
    //commit_btree_set_benchmark::main()
}
