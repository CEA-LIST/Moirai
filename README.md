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

- Use Dotted Version Vectors/Interval Tree Clock to track the history of events.
- Test with [Maelstrom](https://github.com/jepsen-io/maelstrom).
- Use a DAG rather than a BTreeMap for the log...
- After flushing, reset the clocks.
