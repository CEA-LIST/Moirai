# Kubernetes experiment bundles and result viewer

Use `generate_simulation_files.ipynb` to generate experiment configurations and
`launch_experiment.ipynb` to run them. Build/load the referenced images first,
following [the launcher instructions](../kubernetes-launcher/README.md).

## Open the interactive viewer

From the workspace root, install the plotting dependency if needed and start the
viewer:

```sh
python3 -m pip install -r kubernetes-experiments/requirements.txt
python3 kubernetes-experiments/results_viewer.py
```

Open **http://127.0.0.1:8000**. Stop the server with Ctrl+C. The default results
folder is next to the script, independent of the current working directory.
The viewer uses Python's standard HTTP server and Matplotlib; it needs no
JavaScript packages, CDN, or internet connection.

To scan a specific folder or choose another port:

```sh
python3 kubernetes-experiments/results_viewer.py \
  kubernetes-experiments/results/good-case/optimistic --port 8001
```

The controls let you:

- Select an experiment folder (for example, `good-case/optimistic`).
- Plot a particular **run ID**, taken from the results folder, or the
  **equal-weight average of all identified runs** in that folder.
- Filter by client count, choose line or grouped-bar replica comparisons, and
  choose a replica count for time plots (or show all counts).
- Set the trailing window, time step, elapsed start/end, and maximum latency.
- Include failed requests if desired. They are excluded by default.
- Reload results after experiments finish, and download any graph as PNG.

Changing controls and clicking **Update plots / reload results** rebuilds the
page. Summary comparisons use all selected replica counts; the replica-count
selector only limits the time plots. The experiment inventory lists the runs,
request counts, and failures included in the selection. Data notes identify
partial results or skipped malformed files.

## Graphs and units

All graphs use the new Kubernetes records, with no legacy-data mode:

| Graph | Measurement |
|---|---|
| Network usage vs replicas | Total TX + RX delta across comparable replicas, MiB |
| CPU usage vs replicas | Mean cumulative CPU time delta per comparable replica, seconds |
| Latency over time | Trailing-window mean latency for each stream, ms |
| Throughput over time | Trailing-window completions/second: optimistic, consistent, and total |
| Operation-log memory over time | Mean timestamped log memory per recorded replica, MiB |

Replica images identify the compared implementations. Metadata comes from
`experiment.json`, not parsing configuration values out of filenames. Different
workloads at the same replica count are plotted separately. In particular,
client counts, pacing, operation counts, images, request modes, and fault settings
must match before runs are averaged. Different case/mode folders remain separate.

Within each run, client operations are pooled. Network traffic is summed across
available comparable replicas; CPU is averaged across those replicas. Runs then receive equal weight, regardless of how many
requests or replicas were recorded. Missing runs are not filled in; empty latency
windows and missing metrics are gaps. Partial client/resource records are noted.
Counter deltas marked incomparable by the launcher are excluded.
Network totals include client traffic and count inter-replica traffic at both
endpoints (sender TX and receiver RX). If replica measurements are missing or
incomparable, the network total covers only the available comparable replicas.

Time windows are **(t − window, t]**, based on receive time. Throughput divides
completion count by the full window, including startup. The maximum-latency
filter excludes requests strictly above its threshold from both latency and
throughput; requests at the threshold remain included. Failed requests are
matched using the `failed_requests` timestamp pairs.

As in the notebook, runs align to their **first unfiltered send** and stop
contributing request metrics at their **last send**. Replies after that point
are outside the plotted range. Filtering and cropping never reset the origin.
Disabled modes have zero throughput; a missing enabled stream is unknown.

The new launcher records **operation-log memory**, not total container/process
memory. These plots are therefore the closest available counterparts to the
notebook's memory plots, with explicit labels. Real Unix-millisecond sample
timestamps are aligned to the first client send; per-replica values are linearly
interpolated without extrapolation. Memory-only runs use their first memory
sample as the origin. Network counters include all pod-interface
traffic, including replica-to-replica traffic.

## Result discovery

The viewer recursively finds `*-measurements.jsonl` files with a sibling
`experiment.json`, as produced by the bundle notebook:

```text
results/good-case/optimistic/
  <run-id>/
    <simulation-name>/
      experiment.json
      <simulation-name>-measurements.jsonl
      launch.log
```

It reads `optimistic`/`committed` client records, `replica_resource_usage`, and
`replica_log_memory_usage`. Configurations are checked with the launcher's own
validator. It reads results without modifying configurations, measurements, or
cluster resources. Old Podman JSON result files are not loaded.

## Export or use from Python

Write a portable HTML report with embedded PNG graphs, averaging discovered runs
in the first experiment folder and showing time graphs for its smallest replica
count (pass a specific case/mode folder to choose it):

```sh
python3 kubernetes-experiments/results_viewer.py \
  kubernetes-experiments/results/good-case/optimistic \
  --export kubernetes-experiments/report.html
```

The selected page can also be saved directly from the browser. Graphs are
embedded, so saved HTML reports need no running Python server.

For a notebook or Python session started inside `kubernetes-experiments`:

```python
from results import load_results, build_figures

experiments, notes = load_results("results/good-case/optimistic")
print(*notes, sep="\n")

# Average all discovered runs. To choose one run instead:
# experiments = [e for e in experiments if e.run_id == "<run-id>"]
figures = build_figures(
    experiments,
    style="curve",          # or "bar"
    timeline_replicas=4,     # None shows all replica counts
    window=2.0,
    step=0.25,
    start=0.0,
    end=None,
    max_latency=None,
    include_failed=False,
)
for index, (title, figure) in enumerate(figures):
    figure.savefig(f"plot-{index}.png", dpi=200)
    display(figure)  # In Jupyter; omit in a plain Python script.
```

`load_experiment`, `summary_metrics`, `time_series`, and `average_series` are also
available in `results.py` for numerical analysis without the web interface.
`build_figures` accepts one experiment folder at a time.

Run the numerical and viewer checks with:

```sh
python3 -m unittest discover -s kubernetes-experiments -p 'test_results.py' -v
```
