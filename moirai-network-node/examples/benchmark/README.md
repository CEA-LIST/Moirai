# Replica-only commitment benchmark

The `benchmark` Cargo example runs the existing Moirai TCP node, TCSB, and
BTree-backed LWW set. All three protocols use the same workload and
instrumentation.

| Option | Protocol |
|---|---|
| `timestamp` | Timestamp-stability based on Lamport clocks and all-member progress. |
| `commit` | Event-oracle majority commitment. |
| `creek` | Existing Creek commitment with an independent Paxos worker and TCP connections. |

Each replica has one Rust producer and a bounded in-process queue. The
producer offers a seeded random mix of `Add`, `Remove`, and `Contains`
operations over uniformly sampled integer keys. `query_percent` is the
probability of `Contains`, and `add_percent` is the conditional probability
of adding among updates. Contains queries evaluate the local optimistic
view. Updates enter TCSB and the selected commit protocol. Idle replicated
no-ops keep the protocol progressing when updates are infrequent.

The rate is the expected offered operations per second per replica. The
generator samples independent exponential interarrival delays with mean
`1 / rate_per_replica` and schedules them against a monotonic clock until
`duration_s` elapses. Arrival schedules are seeded per replica. The number
scheduled in a run varies around `duration_s * rate_per_replica`. Arrivals
more than 1 ms late are counted as missed, while arrivals presented to a
full queue are dropped. The completion record reports both counters.
The replica drains the local queue until it is empty or 2 ms have
elapsed. With a backlog, it immediately services TCP, commitment and
sampling before resuming the workload. When caught up, it uses the
existing 2 ms polling sleep.

## Run one replica on Linux

From the Moirai root, create a replica configuration.

```sh
cat >/tmp/moirai-bench-replica.json <<'JSON'
{
  "replica": {
    "replica_id": 0,
    "listen_port": 19001,
    "http_port": 8081,
    "noop_interval_ms": 150
  },
  "protocol": "commit",
  "workload": {
    "rate_per_replica": 100,
    "duration_s": 5,
    "query_percent": 20,
    "add_percent": 50,
    "key_space": 128,
    "queue_capacity": 1024,
    "seed": 1
  },
  "sample_interval_ms": 1000,
  "network_interface": "lo"
}
JSON

cargo run --release --locked --manifest-path moirai-network-node/Cargo.toml \
  --example benchmark -- /tmp/moirai-bench-replica.json
```

From another terminal, POST `/api/benchmark/start` on port 8081. Poll
`GET /api/benchmark/status` until it returns phase `drained`. Keep the
replica alive for the desired grace period and POST `/api/benchmark/seal`.
Phase `sealed` means the final record is available. Stop the example with
Ctrl+C. For a Kubernetes run, use the [benchmark launcher](../../../kubernetes-launcher/benchmark-replica/README.md).

## Replica records

| Record | Contents |
|---|---|
| `benchmark_start` | Unix timestamp in milliseconds when the replica starts its local workload. |
| `benchmark_sample` | Elapsed monotonic nanoseconds, cumulative processed and committed counters, new latency samples, log bytes, CPU microseconds, RX bytes and TX bytes. A sample is also emitted at queue drain and at seal. |
| `benchmark_workload_done` | Scheduled, enqueued, missed and queue-full slot counts plus processed assignments by operation class. |
| `benchmark_uncommitted` | Batches of `[dequeue_elapsed_ns, origin_sequence]` for local updates still pending at seal, along with the seal time. |
| `benchmark_sealed` | Final sample was emitted and the benchmark loop will stop processing more work. |

A commit-latency tuple is `[commit_elapsed_ns, latency_ns, origin_sequence]`.
Latency starts immediately after the replica dequeues a local update. It ends
when the commit protocol first publishes a frontier that covers the update.
The protocol observer records each frontier inside the commit path, including
when one network batch triggers several commits. Both timestamps use the same
monotonic clock. Queue wait, local queries, and idle no-ops are outside this
latency distribution. The separate pending-at-seal records provide lower bounds on the latency
of updates that have not committed at their origin by the final sample.

The cumulative commit counter counts every newly covered workload update
from any origin once at each observer from its local start. The local commit counter counts just
its own originated updates. Use one observer's all-origin counter for cluster
throughput. CPU is process-wide user plus system CPU time and includes the
generator, instrumentation, and admin thread. Network counters measure all
traffic on the configured interface. Log memory uses Moirai's deep-size
estimate for the set log and commitment state. Benchmark observer heap
allocations and TCP/TCSB buffers are outside that estimate. Creek's network and CPU
counters include its consensus worker. Log memory counts the replica-owned
Creek state and excludes the independently owned Paxos worker journal.

## Files

| File | Purpose |
|---|---|
| `main.rs` | Cargo's auto-discovered example entry point. |
| `commit_set_benchmark.rs` | Chooses the protocol and runs the replica, queue, idle no-ops and periodic sampling. |
| `workload.rs` | Configures random workload generation, pacing, and queue admission. |
| `measurement.rs` | Observes commit frontiers and reads CPU and network counters on Linux. |
| `control.rs` | Provides health, status, start, seal, and optional crash control over HTTP. |
