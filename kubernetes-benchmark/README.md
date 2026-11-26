# Kubernetes commitment benchmark suite

Extract the overlay into the Moirai root beside the existing
`moirai-network-node` and `kubernetes-launcher` directories. Use the
[benchmark launcher setup](../kubernetes-launcher/benchmark-replica/README.md)
to build and distribute the replica image. The suite is an independent
Cargo crate.

## Generate and run a sweep

Edit `sweep_sample.json` or copy it to a new JSON file with your settings.

```sh
python3 kubernetes-benchmark/generate_configs.py kubernetes-benchmark/sweep_sample.json
```
```sh
cargo run --locked --manifest-path kubernetes-benchmark/Cargo.toml -- \
  kubernetes-benchmark/configs/scaling kubernetes-benchmark/results/scaling
```

| Sweep field | Meaning |
|---|---|
| `output_folder` | Required new directory for generated configs, relative to the sweep JSON unless absolute. |
| `base` | Optional experiment template, relative to the sweep JSON unless absolute. Defaults to the launcher's sample. |
| `replicas`, `protocols` | Replica counts and protocol names to sweep. Omitted values come from the base. |
| `total_rates` or `rates_per_replica` | Offered operations/s. Every total rate must divide evenly by every replica count. The default is the base rate per replica. |
| `repetitions` | Number of runs per combination, with successive workload seeds. Defaults to one. |
| `duration_s` | Duration of the local workload in seconds, before queue drain and grace. |
| `with_fault`, `failure_time_s` | Whether replica 0 crashes and when, in seconds after workload start. If enabled, the time must be positive and smaller than `duration_s`. A false flag disables the crash even if a time is provided. If the flag is omitted, a configured non-null time enables the crash. |

The base supplies unmodified launch settings such as grace time, image,
and operation mix. The runner discovers JSON files recursively, runs
them in sorted order, and saves a result directory per trial plus
`suite.tsv`. After a regular trial failure it continues with later
trials and returns nonzero when any trial failed.

## View or export results

```sh
python3 -m pip install -r kubernetes-benchmark/requirements.txt
```
```sh
python3 kubernetes-benchmark/benchmark_results_viewer.py kubernetes-benchmark/results/scaling
```
``` sh
python3 kubernetes-benchmark/benchmark_throughput_viewer.py kubernetes-benchmark/results/scaling
```

Open [the results viewer](http://127.0.0.1:8010) or
[the throughput viewer](http://127.0.0.1:8011) while its server runs.
Select compatible workload settings, then set the offered-rate basis
and the offered rate or replica count. Timeline controls further fix both
the rate and the replica count. Choose **Protocol averages across all
matching runs** for one curve per protocol, or choose a run to inspect its
replicas. Set the trailing window, sample step, start and end times. Choose
line or grouped bar comparisons for network and CPU summaries. Plot titles
and series colors and labels are editable. Apply changes with **Update plots
and reload runs**. Click a graph to download its PNG. Both viewers can
also write standalone HTML with `--export report.html`. Use
`--selection '…'` with the query string from the viewer URL to retain
the current controls in the export.

| Viewer | Graphs |
|---|---|
| `benchmark_results_viewer.py` | Network MiB and CPU seconds per replica versus replica count at a fixed offered rate. |
| `benchmark_throughput_viewer.py` | Network MiB and CPU seconds per replica versus offered operations per second at a fixed replica count. Achieved commit throughput versus p50, p95 and p99 latency. |
| Both viewers | Commit latency, committed throughput, log memory and windowed CPU usage over time. |

## Measurement windows

- **Resource summaries** use each surviving replica's cumulative RX + TX bytes
  and CPU time from its local start through queue drain. Replica values are
  averaged within each run and runs receive equal weight at each plotted point.
  RX + TX counts each inter-replica transmission at both endpoints in cluster
  totals. The graphs show per-replica averages.
- **Achieved commit throughput** divides the lowest surviving replica's
  all-origin committed-update count at its drain sample by its elapsed seconds.
  It counts each replicated update once for the system and includes the effects
  of startup and any queue backlog. Time graphs use commit-count deltas over
  actual sample intervals on each replica. A count is an update covered by the
  frontier, even if the set already had the requested value or did not have it.
- **Commit latency** uses origin-replica timings through the seal horizon,
  including updates committed during the grace period. The throughput-versus-
  latency graph pairs the active-window throughput with these completed
  latencies, preserving drain-period samples. Percentiles are computed within
  a run and averaged with equal weight across runs with samples. Results before
  a deliberate crash can contribute origin latency until that replica's last
  sample. The inventory shows local-commit coverage for sealed replicas and
  dropped generator slots for drained replicas.
- **Timelines** align to each replica's start. Latency is p95 over
  observations in the trailing window, grouped by commit time. Throughput
  and CPU usage use deltas of cumulative counters divided by elapsed wall
  time in the window. The CPU unit is CPU seconds per wall second, so
  1.0 represents one fully utilized core. The viewer interpolates counters
  and log memory between samples. Each time point averages available replicas
  within a run and then gives each available run equal weight. Empty latency
  windows and times beyond a replica's last sample are gaps. Resource and
  throughput timelines cover the grace period wherever samples exist.
- **Updates pending at seal** can be included in v3 results. They contribute
  dequeue-to-seal latency lower bounds at seal time. A dashed local rate
  counts those updates by dequeue time. Committed throughput continues to
  count only commits, including on the sweep's x-axis. Queue drops have no
  dequeue timestamp, and the final commit status of updates from a crashed
  origin is unavailable in its log.

Two runs are compared at the same point only when their benchmark version,
duration, operation mix, key space, queue size, replication timers, sampling
and fault settings match. Protocol and image identify the plotted series.
Older configs and results using `lamport` or `omega` are read as
`timestamp` and `commit` respectively. Seeds and run names
can differ across repetitions. Incomplete runs are listed with the reason
they were skipped.

## Files

| File | Purpose |
|---|---|
| `Cargo.toml`, `Cargo.lock` | Manifest and lockfile for the independent suite runner. |
| `src/main.rs` | Discovers and runs trials and writes the suite status table. |
| `generate_configs.py` | Expands a JSON sweep into separate launcher configs. |
| `sweep_sample.json` | Editable sweep with rates, duration, repetitions, and crash settings. |
| `benchmark_results.py` | Discovers and checks run files and computes metrics and percentiles. |
| `benchmark_plots.py` | Builds replica, throughput and timeline figures. |
| `benchmark_viewer.py` | Shares browser controls, image downloads and HTML export between viewers. |
| `benchmark_results_viewer.py` | Opens the replica-count and timeline viewer. |
| `benchmark_throughput_viewer.py` | Opens the load sweep viewer. |
| `requirements.txt` | Plotting dependency for both viewers. |

Use the plot settings to assign each protocol a numeric bar position (lower values appear first). Each time graph has optional y-axis minimum and maximum fields. Blank bounds use automatic scaling. The benchmark image tag is `:dev`; rebuild and reload that tag after changing replica code.
