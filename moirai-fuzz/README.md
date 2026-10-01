# Moirai fuzzer: seeded distributed-system simulator

This crate generates seeded, stochastic executions of a replication protocol.

> Note: It is a property-testing/simulation driver, not a coverage-guided byte-input or general-purpose network fuzzer. Its main oracle checks agreement on a user-supplied query after final convergence.

## Algorithm

Initialize `n` replicas (indexed from 0 to `n`) and a seeded random-number generator. All replicas begin online. Unless a reachability matrix is supplied, the network is fully connected. Repeat until `u` updates have been issued:

1. Select one replica `r` uniformly at random. Advance only `r`'s online/offline state: an online replica disconnects with probability `p_disconnect`, and an offline replica reconnects with probability `p_reconnect`.
2. If `r` is online, perform one complete anti-entropy pull from each eligible online peer, in ascending replica-index order. Each returned batch of missed event messages is received synchronously, including empty batches.
3. If an Ω commitment oracle is used, an offline `r` chooses itself as leader; an online `r` chooses the lowest-index reachable online replica, including itself.
4. Generate an enabled command using the data type's generator, then `send` and deliver it locally to `r`.
5. If `r` is online, the new message is received by eligible online peers in ascending replica-index order.

Optionally, perform a final merge in which every replica pulls pending events from every other replica, regardless of online status or configured reachability. Then, evaluate the same query at each replica. A user-supplied comparison function checks whether results agree.

### Execution-model limitations

- Generators are expected to produce enabled commands. Deliberately invalid commands and rejections are not part of the workload.
- There is no coverage feedback or systematic schedule exploration. A passing set of seeds is not a proof of correctness.
- There is no explicit packet duplication, corruption, random message reordering, or configurable message-delay queue. Churn and the communication mask create "causal lag", but online communication itself is immediate and ordered.
- A selected online replica pulls from all eligible peers before each command. This catch-up policy and fixed peer order favor particular causal histories; they are not an unbiased enumeration of possible concurrent executions.
- Churn probabilities are conditional on selection of that replica, not on global simulation steps or elapsed time. Unselected replicas retain their online/offline state. No physical network or client think time is simulated.
- Final merge overrides churn and the communication mask. Successful convergence therefore demonstrates reconciliation in quiescient periods.

## Metrics

### Time

The fuzzer measures the following time metrics:

- **Response Time (RT):** The time spent in `send()`, `receive()`, and `receive_batch()`. This includes preparing an update and verifying it is valid (which can require a query against the state). A received update may not be delivered in that call, or may trigger the delivery of many updates.
- **Effect Time (ET):** The time spent in `effect()` for each delivered update.
- **Query Time (QT):** The time spent evaluating the configured query.

Times use `Duration` internally and integer nanoseconds in JSON (`*_ns`). The terminal selects a readable unit independently.

### Space

- **Log Size (LS):** Inclusive allocation footprint in bytes of the root log and each observed nested log. It is recorded after each `effect()` and `stabilize()` call.
- **Replica Size (RS):** Size in bytes of the replica ID, log, and TCSB. The observation variables are excluded.

### Other

- **Stability Progress (SP):** Keep track of a replica's stability progresses.

## Derived Metrics

- Inter-Replica Concurrency Ratio (ICR)
- Average concurrency per update over the whole execution
- Average number of events between stability progress points (per replica, including the initial frontier)
- Stability progress count (per replica)
- Average effect time (per replica and across all replicas)
- P99 effect time (per replica)
- Total time spent in effect() (per replica, with exhaustive sampling)
- Average response time (per replica and across all replicas)
- P99 response time (per replica)
- Total time spent in send(), receive(), and receive_batch() (per replica, with exhaustive sampling)
- Client-call (interaction) throughput: the number of `send`, `receive`, and `receive_batch` calls divided by their total response time (per replica, with exhaustive sampling). An empty batch is still one call. This is not wall-clock run throughput or delivered-event throughput.
- Average query time (per replica)
- P99 query time (per replica)
- Replica-size snapshot timeline per communication call
- Inclusive log-size snapshot timelines per effect counter, with stability progress marked
- Final log size (per replica), with tree structure of the nested logs.
- Sampled peak replica size
- Sampled peak root-log size, retaining the nested footprint tree

> Note: the following sections are written using AI-assisted drafting.

## Meaning of asymmetric reachability

`reachability[r][peer]` currently enables an **interaction initiated by `r`**:
both pulling missing updates _from_ `peer` and pushing `r`'s new update _to_
`peer`. It is not a directed packet-delivery matrix: a pull also needs a reply
in the opposite direction. Both endpoints must be online during normal execution.

For example, with `reachability[A][B] = true` and
`reachability[B][A] = false`:

| Selected replica | Anti-entropy                        | Immediate delivery of the new update |
| ---------------- | ----------------------------------- | ------------------------------------ |
| A                | A pulls from B (updates flow B → A) | A → B                                |
| B                | B does not pull from A              | No B → A push                        |

Use symmetric matrices to model communication partitions. Asymmetric masks
model asymmetric _interaction initiation_, not realistic one-way network links.
This section documents the existing behavior; it does not change scheduling.

## Sampling and phases

Each sample is tagged `execution`, `final_convergence`, or `validation` (the final convergence queries). Derived statistics include all phases (callers can use raw samples to analyze phases separately). Log-size samples use effect IDs, with distinct `effect`/`stability` boundaries. A stability snapshot reuses the preceding effect's ID and sampling decision.

Per-replica statistics include sample counts. Run means have two explicit interpretations: `avg_response_time`/`avg_effect_time` pool all recorded samples, whereas `avg_replica_response_time`/`avg_replica_effect_time` weight each replica with at least one sample equally. Replicas with zero samples do not contribute to either mean. Durations are rounded down to whole nanoseconds.

Whole-replica sizing walks the underlying replica ID, TCSB, and unwrapped log with one shared `DeepSizeOf` context, counting shared `Rc`/`Arc` allocations once. Observer layouts, sampling configuration, RNGs, and recorder buffers are excluded. Sizes remain allocation-footprint estimates, not process RSS or allocator bookkeeping measurements.

### Container footprint display

Terminal footprint trees show at most **five individual children per node**, choosing the largest inclusive footprints and displaying them in descending size order. Equal sizes retain their original tree order. Remaining siblings are summarized in one branch, for example `└── … 2 341 more children — Σ 1.200 MB`. The same limit applies recursively to every displayed node, including repeated child types in maps, lists, and other containers. Sorting affects presentation only, not the captured tree.

Summary sizes sum only the omitted direct children's inclusive footprints: their descendants are already included and are not added again. These sums are **not** deduplicated allocation totals; shared allocations can overlap, so do not add summaries to the root size. Root and individual node sizes remain the original inclusive measurements. Rendering is also limited to 40 tree lines and eight child levels; when either limit is reached, omitted children still receive a count-and-size summary. The complete trees in reports and JSON are unchanged.

This reduces terminal output only, not the cost of capturing footprint trees.

## Concurrency calculation

For each issued event, the non-origin components of its version vector count its causal predecessors from other replicas. Summing these counts gives the number of comparable cross-origin event pairs, with each pair counted once. If replica `r` issued `n_r` events, the total number of cross-origin pairs is `sum_{r < s}(n_r * n_s)`. ICR is the fraction of these pairs that are incomparable, or zero when there are no such pairs.

Counts use `u128` and are accumulated while events are appended. For `N` events and `R` replicas, ICR counting takes `O(N * R)` time and `O(R)` additional memory. Every send must be included once, starting at sequence one for each origin, with causal dependencies appended first.

`avg_concurrent_updates` is `2 * concurrent_pairs / N`: each concurrent pair contributes one partner to each of its two updates. This counts concurrency across the complete execution. An empty execution has no average. Both concurrency metrics are computed when execution-graph capture is enabled and are independent of replica metric sampling.

DOT export first performs transitive reduction: redundant edges are removed without changing events or causal reachability. Reduction uses per-origin causal clocks, taking `O(N * R^2)` time and `O(N * R)` temporary memory. A progress bar is shown for graphs with at least 1,000 updates when display is enabled. Repeated exports reuse the reduced graph unless another update was appended.
