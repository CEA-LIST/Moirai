# Benchmarking, Testing, Profiling, and Performance Analysis

This document provides instructions for benchmarking, profiling, and analyzing the performance of the Moirai CRDT framework. It includes commands for benchmarking, flamegraph profiling, and memory usage analysis.

## Monitoring

### Instruments

```sh
cargo instruments -t time --release --open

cargo instruments -t Allocations --example <name> --release --open
```

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
cargo flamegraph --root --release --example <name>
cargo flamegraph --root --release --unit-test -- crdt::graph::aw_multidigraph::tests::op_weaver_multidigraph
```

### Memory used

```sh
cargo build --example <name> --release

/usr/bin/time -l ./target/release/examples/<name> 2>&1 | awk '
/real/ { real_time = $1 }
/user/ { user_time = $1 }
/sys/ { sys_time = $1 }
/maximum resident set size/ { max_mem = $1 }
/peak memory footprint/ { peak_mem = $1 }
/instructions retired/ { instructions = $1 }
/cycles elapsed/ { cycles = $1 }
END {
    printf "CPU Time:\n"
    printf "  Real: %s sec\n", real_time
    printf "  User: %s sec\n", user_time
    printf "  Sys:  %s sec\n", sys_time
    printf "\nMemory Usage:\n"

    # Scale max_mem
    if (max_mem > 1024*1024*1024)
        printf "  Max Resident Set Size: %.2f GB\n", max_mem / (1024*1024*1024)
    else if (max_mem > 1024*1024)
        printf "  Max Resident Set Size: %.2f MB\n", max_mem / (1024*1024)
    else
        printf "  Max Resident Set Size: %.2f KB\n", max_mem / 1024

    # Scale peak_mem
    if (peak_mem > 1024*1024*1024)
        printf "  Peak Memory Footprint: %.2f GB\n", peak_mem / (1024*1024*1024)
    else if (peak_mem > 1024*1024)
        printf "  Peak Memory Footprint: %.2f MB\n", peak_mem / (1024*1024)
    else
        printf "  Peak Memory Footprint: %.2f KB\n", peak_mem / 1024

    printf "\nPerformance:\n"
    printf "  Instructions Retired: %.2f MInst\n", instructions / 1e6
    printf "  Cycles Elapsed: %.2f MCycles\n", cycles / 1e6
}'
```

### Unit-Tests

```sh
# List all tests
cargo test -- --list

RUST_LOG=debug cargo test -- --nocapture
RUST_LOG=debug cargo test <name> -- --nocapture
```

## Helpers

```sh
cargo +nightly fmt -- --unstable-features --config imports_granularity=Crate,group_imports=StdExternalCrate
```
