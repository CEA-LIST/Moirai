# Benchmarks

This folder contains benchmark results obtained by running the fuzzer tool
(see the [Fuzzer documentation](/src/fuzz/README.md)) on selected CRDT
implementations.

The purpose of these benchmarks is to evaluate the performance impact of
specific design choices in Pure Operation-based CRDTs.

## Benchmark Results Overview

All benchmarks were executed on a **MacBook Air M2 (2022)**.

Benchmarks are grouped by data type. Each benchmark run produces a JSON
report containing raw timing measurements and metadata. Because the project
is still under active development, benchmark reports may:

- Follow slightly different naming conventions,
- Be generated at different stages of implementation maturity, and
- Contain different fields depending on the benchmark version.

The most recent naming convention is: `year-month-day-hour-minute-second_branch_commit-hash.json`

## PaPoC ’26 Submission Experiments

See [BENCHMARKS.md](/bench-results/BENCHMARKS.md).