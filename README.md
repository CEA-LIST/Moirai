# A Rust Implementation of Pure Operation-Based CRDTs

## Monitoring

### Benchmarking

```sh
cargo bench
hyperfine 'cargo test --release'
```

### Flamegraph Profiling

```sh
cargo install flamegraph
cargo flamegraph --root --dev --unit-test po-crdt -- test_graph
cargo flamegraph --test eval_nested
```

### Logging

```sh
RUST_LOG=debug cargo test -- --nocapture
RUST_LOG=debug cargo test <name> -- --nocapture
```

## Work notes

- After flushing, reset the clocks.

## Helpers

```sh
cargo +nightly fmt -- --unstable-features --config imports_granularity=Crate,group_imports=StdExternalCrate
```
