# A Rust Implementation of Pure Operation-Based CRDTs

## Benchmarking & Profiling

### Benchmarking

```sh
cargo bench
hyperfine 'cargo test --release'
```

### Flamegraph Profiling

```sh
cargo install flamegraph
cargo flamegraph --root --dev --unit-test po-crdt --  test_graph
```

## Work notes

- PO-CRDTs do not provide history, since stable events can be obsoleted by a new
  event at any time.

## Todo list

- [ ] Use Dotted Version Vectors/Inteval Tree Clock to track the history of
      events.
- [ ] Use a better data structure for the PO-Log of events. Something supporting
      the inherent DAG properties of the log.
