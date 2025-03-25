# A Rust Implementation of Pure Operation-Based CRDTs

## Monitoring

### Instruments

```sh
cargo instruments -t time --release --open
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
```

### Memory used

```sh
cargo build example --release

/usr/bin/time -l /target/release/example/<name> 2>&1 | awk '
/real/ { real_time = $1 }
/user/ { user_time = $1 }
/sys/ { sys_time = $1 }
/maximum resident set size/ { max_mem = $1 }
/peak memory footprint/ { peak_mem = $1 }
/instructions retired/ { instructions = $1 }
/cycles elapsed/ { cycles = $1 }
END {
    # Function to scale memory
    function scale_mem(bytes) {
        if (bytes > 1024*1024*1024) return sprintf("%.2f GB", bytes / (1024*1024*1024));
        else if (bytes > 1024*1024) return sprintf("%.2f MB", bytes / (1024*1024));
        else return sprintf("%.2f KB", bytes / 1024);
    }

    printf "CPU Time:\n"
    printf "  Real: %s sec\n", real_time
    printf "  User: %s sec\n", user_time
    printf "  Sys:  %s sec\n", sys_time
    printf "\nMemory Usage:\n"
    printf "  Max Resident Set Size: %s\n", scale_mem(max_mem)
    printf "  Peak Memory Footprint: %s\n", scale_mem(peak_mem)
    printf "\nPerformance:\n"
    printf "  Instructions Retired: %.2f MInst\n", instructions / 1e6
    printf "  Cycles Elapsed: %.2f MCycles\n", cycles / 1e6
}'
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
