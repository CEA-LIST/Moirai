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
- follow slightly different naming conventions,
- be generated at different stages of implementation maturity, and
- contain different fields depending on the benchmark version.

The most recent naming convention is: `year-month-day-hour-minute-second_branch_commit-hash.json`


## PaPoC ’26 Submission Experiments

The PaPoC ’26 submission reports results from two independent experiments:

1. **Enable-Wins Flag Set vs. Add-Wins Set**
2. **Incremental vs. full recomputation of the Last Stable Vector (LSV)**

The raw data corresponding to these experiments are stored in this folder and
linked below.

---

## Enable-Wins Flag Set vs. Add-Wins Set

The **Add-Wins Set (AW-Set)** and the **Enable-Wins Flag Set (EWFlag-Set)** are
two Conflict-Free Replicated Data Type (CRDT) implementations of a set, sharing
the same concurrent specification: informally, an element is present in the
set if there exists an `add(e)` operation that is not causally followed by a
`remove(e)` operation.

### Motivation

In the Pure Op framework, the canonical AW-Set implementation requires scanning
the entire Partially-Ordered Log (PO-Log) to detect causally redundant updates.
However, for a given element, an `add` or `remove` operation can make at most
one prior update redundant. Scanning unrelated updates therefore induces
unnecessary overhead.

The EWFlag-Set addresses this issue by exploiting **nested Pure CRDTs**.
It is composed of:
- an **Update-Wins Map**, whose keys are set elements, and
- **Enable-Wins Flags** as values.

Each flag encodes whether the corresponding element is present. Because the
flags preserve add-wins semantics, the concurrent behavior is equivalent to
the AW-Set. Crucially, causal redundancy detection is restricted to the flag
associated with the affected key, rather than the full PO-Log. As a result,
the cost of the `effect()` phase becomes independent of the total number of
updates in the set.

### Experimental Conditions

Both data types were evaluated under identical conditions:

- **Number of replicas:** 16  
- **Network:** fully connected, asynchronous, bidirectional, reliable channels  
- **Replica churn:** 0.7 (each time a replica is selected, it has a 70% chance
  of switching between online and offline states)
- **Causal stability:** disabled, in order to observe the impact of an
  ever-growing PO-Log
- **Element type:** `usize`
- **Element range:** `[0, 1_000_000]`
- **Operations:**
  - `Add(elem)` with probability 62.5%
  - `Remove(elem)` with probability 25%
  - `Clear` with probability 12.5%

Operations are generated without inspecting the current state (e.g., `Remove`
may target an element that is not present), which is consistent with the
standard Set CRDT specification.

Each replica has an equal probability of being selected to issue an update.
Offline replicas continue to generate updates but do not exchange messages.
When selected and online, a replica:
1. pulls missing updates from all other online replicas, and
2. broadcasts its newly issued update to all online replicas.

At the end of each execution, all replicas are brought back online and exchange
any missing updates to ensure convergence.

### Measurements

For each run, we record the **total time spent in the `effect()` function** on
each replica. The reported value is the average across all replicas.

Note that runs for the AW-Set and EWFlag-Set do not share identical execution
traces (update values, delivery order, or concurrency patterns). Empirically,
we observed that these variations do not affect the relative performance:
runs exhibit comparable levels of concurrency.

### Raw Data

- AW-Set:  
  [/bench-results/aw_set/2026-01-30-11-18-48_master_f35232df.json](/bench-results/aw_set/2026-01-30-11-18-48_master_f35232df.json)

- EWFlag-Set:  
  [/bench-results/ew_flag_set/2026-01-30-11-25-32_master_f35232df.json](/bench-results/ew_flag_set/2026-01-30-11-25-32_master_f35232df.json)

---

## Last Stable Vector (LSV) Computation

The **Last Stable Vector (LSV)** is derived from a matrix clock by taking the
minimum value of each column. A naïve approach recomputes the LSV from scratch
after each delivery. An optimized approach computes it **incrementally** by
reusing the previously computed LSV and performing early termination when a
new minimum is impossible.

### Experimental Conditions

This experiment compares:
- `column_wise_min()` (full recomputation), and
- `column_wise_min_incremental()` (incremental computation)

Both functions are implemented on the `MatrixClock` structure.

We consider a system of `n` processes collectively issuing **1,000,000 updates**
on an AW-Set. Upon each update delivery, both LSV computation methods are
executed, and their execution times are measured.

For each run:
1. each replica records the total time spent computing the LSV using each
   method, and
2. the reported value is the average across all replicas.

### Raw Data

The raw benchmark results for this experiment are available at:

[/bench-results/stability_computation/](/bench-results/stability_computation/)
