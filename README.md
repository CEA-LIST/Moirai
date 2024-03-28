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
cargo flamegraph --root --dev --unit-test po-crdt --  test_graph
```

### Logging

```sh
RUST_LOG=debug cargo test -- --nocapture
```

## Work notes

- PO-CRDTs do not provide history, since stable events can be obsoleted by a new
  event at any time.
- Receiving two events with the same timestamp is an error in the protocol.

## Todo list

- [ ] Use Dotted Version Vectors/Inteval Tree Clock to track the history of
      events.
- [ ] Use a better data structure for the PO-Log of events. Something supporting
      the inherent DAG properties of the log.
- [x] Remove LVV?
