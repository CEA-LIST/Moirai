"""Read Kubernetes bundle artifacts and build comparable experiment plots."""

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
import runpy
from statistics import fmean

from matplotlib.figure import Figure

VALIDATE_CONFIG = runpy.run_path(str(
    Path(__file__).resolve().parent.parent / "kubernetes-launcher/render-experiment.py"
))["validate_config"]
MODES = ("optimistic", "consistent")


def number(value):
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value) and value >= 0)


def mean(values):
    values = [v for v in values if v is not None and math.isfinite(v)]
    return fmean(values) if values else math.nan


@dataclass
class Experiment:
    source: Path
    config: dict
    run_id: str
    bundle: str
    attempts: dict = field(default_factory=lambda: {mode: [] for mode in MODES})
    successes: dict = field(default_factory=lambda: {mode: [] for mode in MODES})
    recorded_modes: set = field(default_factory=set)
    memory: dict = field(default_factory=dict)  # replica -> [(timestamp_ns, bytes)]
    replicas: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    @property
    def label(self):
        return self.config["replica_image"]

    @property
    def identity(self):
        # Paths and display names do not define a workload; all other fields do.
        return tuple(sorted((k, v) for k, v in self.config.items()
                            if k not in ("simulation_name", "output_path", "results_file")))

    @property
    def bounds(self):
        pairs = [pair for mode in MODES for pair in self.attempts[mode]]
        if pairs:
            # Match the notebook: first send to last send, before filtering.
            return min(s for s, _ in pairs), max(s for s, _ in pairs)
        timestamps = [t for samples in self.memory.values() for t, _ in samples]
        return (min(timestamps), max(timestamps)) if timestamps else (None, None)

    def pairs(self, mode, include_failed=False, max_latency=None):
        pairs = self.attempts[mode] if include_failed else self.successes[mode]
        return [(s, r) for s, r in pairs
                if max_latency is None or r - s <= max_latency * 1e9]


def load_experiment(source):
    """Read one measurements JSONL and its sibling effective experiment.json."""
    source = Path(source).resolve()
    config_path = source.with_name("experiment.json")
    try:
        config = VALIDATE_CONFIG(config_path)
    except SystemExit as error:
        raise ValueError(str(error)) from error
    config.pop("results_file")
    experiment = Experiment(source, config, source.parent.parent.name,
                            str(source.parent.parent.parent))
    seen_clients, seen_replicas = set(), set()
    resource_seen = False
    with source.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be an object")
                kind = record.get("type")
                if "mode" in record:
                    mode = {"optimistic": "optimistic", "committed": "consistent"}.get(record["mode"])
                    if mode is None:
                        raise ValueError(f"unknown client mode: {record['mode']}")
                    client = record["client_id"]
                    if isinstance(client, bool) or not isinstance(client, int) or not 1 <= client <= config["n_clients"]:
                        raise ValueError("client_id is outside the configured range")
                    if not config[f"use_{mode}_requests"]:
                        raise ValueError(f"client stream {mode} is disabled in the configuration")
                    identity = client, mode
                    if identity in seen_clients:
                        raise ValueError(f"duplicate client stream {identity}")
                    seen_clients.add(identity)
                    experiment.recorded_modes.add(mode)
                    pairs = record["latencies"]
                    if not isinstance(pairs, list) or any(
                        not isinstance(p, list) or len(p) != 2
                        or not all(number(v) for v in p) or p[1] < p[0] for p in pairs
                    ):
                        raise ValueError("latencies must contain nonnegative [send_ns, receive_ns] pairs")
                    failures = record.get("failed_requests", [])
                    if not isinstance(failures, list):
                        raise ValueError("failed_requests must be a list")
                    failed_pairs = set()
                    for failure in failures:
                        s, r = failure["start_ns"], failure["end_ns"]
                        if not number(s) or not number(r) or r < s:
                            raise ValueError("invalid failed request timestamps")
                        failed_pairs.add((s, r))
                    if not failed_pairs.issubset({tuple(p) for p in pairs}):
                        raise ValueError("failed request has no matching latency pair")
                    experiment.attempts[mode].extend(tuple(p) for p in pairs)
                    experiment.successes[mode].extend(tuple(p) for p in pairs if tuple(p) not in failed_pairs)
                elif kind == "replica_resource_usage":
                    if resource_seen:
                        raise ValueError("duplicate resource-usage record")
                    resource_seen = True
                    replicas = record["replicas"]
                    if not isinstance(replicas, list) or len(replicas) > config["n_replicas"]:
                        raise ValueError("invalid replica resource list")
                    ordinals = set()
                    for replica in replicas:
                        ordinal = replica["ordinal"]
                        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or not 0 <= ordinal < config["n_replicas"] or ordinal in ordinals:
                            raise ValueError("duplicate or out-of-range replica ordinal")
                        ordinals.add(ordinal)
                        for metric in ("rx_bytes", "tx_bytes", "cpu_time_us"):
                            value = replica[metric]["delta"]
                            if value is not None and not number(value):
                                raise ValueError(f"invalid {metric} delta")
                    experiment.replicas = replicas
                elif kind == "replica_log_memory_usage":
                    replica = record["replica_id"]
                    if isinstance(replica, bool) or not isinstance(replica, int) or replica in seen_replicas:
                        raise ValueError("duplicate or invalid memory replica_id")
                    seen_replicas.add(replica)
                    samples = record["samples"]
                    if not isinstance(samples, list) or any(
                        not isinstance(p, list) or len(p) != 2 or not all(number(v) for v in p)
                        for p in samples
                    ):
                        raise ValueError("memory samples must contain [bytes, timestamp_ms] pairs")
                    # Multiple samples at the same millisecond keep the last value.
                    experiment.memory[replica] = sorted({t * 1_000_000: b for b, t in samples}.items())
                else:
                    raise ValueError(f"unknown measurement type: {kind!r}")
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError(f"line {line_number}: {error}") from error
    if len(experiment.memory) > config["n_replicas"]:
        raise ValueError("more memory records than configured replicas")
    expected_streams = config["n_clients"] * sum(config[f"use_{m}_requests"] for m in MODES)
    if len(seen_clients) < expected_streams:
        experiment.notes.append(f"Only {len(seen_clients)}/{expected_streams} client streams recorded")
    comparable = sum(r.get("comparable") is True for r in experiment.replicas)
    if comparable < config["n_replicas"]:
        experiment.notes.append(f"Comparable resource counters for {comparable}/{config['n_replicas']} replicas")
    if len(experiment.memory) < config["n_replicas"]:
        experiment.notes.append(f"Memory records for {len(experiment.memory)}/{config['n_replicas']} replicas")
    return experiment


def load_results(directory):
    """Recursively discover runs. Return (experiments, file-specific warnings)."""
    directory = Path(directory).expanduser().resolve()
    if not directory.is_dir():
        raise ValueError(f"Results folder does not exist: {directory}")
    experiments, warnings, seen = [], [], set()
    for source in sorted(directory.rglob("*-measurements.jsonl")):
        try:
            experiment = load_experiment(source)
            identity = experiment.bundle, experiment.run_id, experiment.identity
            if identity in seen:
                raise ValueError("duplicate workload in the same run")
            seen.add(identity)
            experiments.append(experiment)
            warnings.extend(f"{source.relative_to(directory)}: {note}" for note in experiment.notes)
        except (OSError, ValueError) as error:
            warnings.append(f"{source.relative_to(directory)}: skipped: {error}")
    return experiments, warnings


def summary_metrics(experiment, *, include_failed=False, max_latency=None):
    """Sum network traffic, average CPU across replicas, and pool client operations."""
    replicas = [r for r in experiment.replicas if r.get("comparable") is True]
    network = [r["rx_bytes"]["delta"] + r["tx_bytes"]["delta"] for r in replicas
               if r["rx_bytes"]["delta"] is not None and r["tx_bytes"]["delta"] is not None]
    cpu = [r["cpu_time_us"]["delta"] for r in replicas]
    peaks = [max(b for _, b in samples) for samples in experiment.memory.values() if samples]
    metrics = {"network_mib": sum(network) / 2**20 if network else math.nan,
               "cpu_seconds": mean(cpu) / 1e6, "peak_memory_mib": mean(peaks) / 2**20}
    for mode in MODES:
        pairs = experiment.pairs(mode, include_failed, max_latency)
        metrics[f"{mode}_latency_ms"] = mean([(r - s) / 1e6 for s, r in pairs])
    return metrics


def interpolate(samples, timestamp):
    """Linearly interpolate real memory timestamps without extrapolation."""
    if not samples or timestamp < samples[0][0] or timestamp > samples[-1][0]:
        return math.nan
    index = bisect_right(samples, (timestamp, math.inf)) - 1
    t, value = samples[index]
    if t == timestamp or index == len(samples) - 1:
        return value
    next_t, next_value = samples[index + 1]
    return value + (next_value - value) * ((timestamp - t) / (next_t - t))


def time_series(experiment, times, *, window=2.0, include_failed=False, max_latency=None):
    """Trailing (t-window, t] windows, aligned to the unfiltered first send."""
    if not number(window) or window <= 0:
        raise ValueError("window must be positive")
    if max_latency is not None and not number(max_latency):
        raise ValueError("max_latency must be nonnegative or None")
    if any(not number(t) for t in times):
        raise ValueError("times must be nonnegative")
    start, end = experiment.bounds
    output = {}
    duration = (end - start) / 1e9 if start is not None else -1
    active = [0 <= t <= duration for t in times]
    for mode in MODES:
        events = sorted(((r - start) / 1e9, (r - s) / 1e6)
                        for s, r in experiment.pairs(mode, include_failed, max_latency)) if start is not None else []
        received = [t for t, _ in events]
        prefix = [0.0]
        for _, latency in events:
            prefix.append(prefix[-1] + latency)
        latencies, rates = [], []
        enabled = experiment.config[f"use_{mode}_requests"]
        # Missing client records are unknown, rather than a measured zero.
        observed = any(experiment.attempts[m] for m in MODES)
        for t, in_range in zip(times, active):
            left, right = bisect_right(received, t - window), bisect_right(received, t)
            count = right - left
            latencies.append((prefix[right] - prefix[left]) / count if in_range and count else math.nan)
            rates.append(count / window if in_range and observed and enabled and mode in experiment.recorded_modes else
                         0.0 if in_range and observed and not enabled else math.nan)
        output[f"{mode}_latency_ms"] = latencies
        output[f"{mode}_throughput"] = rates
    output["total_throughput"] = [a + b for a, b in zip(output["optimistic_throughput"], output["consistent_throughput"])]
    output["memory_mib"] = [mean([interpolate(samples, start + t * 1e9)
                                  for samples in experiment.memory.values()]) / 2**20
                            if start is not None else math.nan for t in times]
    return output


def average_series(experiments, times, **options):
    """Give each available run equal weight at each point; retain empty gaps."""
    series = [time_series(e, times, **options) for e in experiments]
    return {key: [mean(values) for values in zip(*(s[key] for s in series))]
            for key in series[0]} if series else {}


def build_figures(experiments, *, style="curve", timeline_replicas=None,
                  window=2.0, step=0.25, start=0.0, end=None,
                  max_latency=None, include_failed=False):
    """Return [(title, matplotlib Figure)], for one bundle and selected run(s).

    Different workloads at the same replica count are kept in separate series.
    Timeline panels only average experiments with identical configuration fields.
    """
    if style not in ("curve", "bar"):
        raise ValueError("style must be curve or bar")
    for name, value in (("window", window), ("step", step)):
        if not number(value) or value <= 0:
            raise ValueError(f"{name} must be positive")
    if not number(start) or (end is not None and (not number(end) or end <= start)):
        raise ValueError("time range requires 0 <= start < end")
    if max_latency is not None and not number(max_latency):
        raise ValueError("max_latency must be nonnegative or None")
    if len({e.bundle for e in experiments}) > 1:
        raise ValueError("Select one bundle folder before plotting")
    if not experiments:
        return []
    # The main comparison is image vs replica count. Split conflicting workloads
    # at the same x into separate series instead of averaging incompatible runs.
    by_point = defaultdict(set)
    for e in experiments:
        by_point[e.label, e.config["n_replicas"]].add(e.identity)
    ambiguous = any(len(identities) > 1 for identities in by_point.values())
    groups = defaultdict(list)
    for e in experiments:
        label = e.label
        if ambiguous:
            c = e.config
            label += f" | {c['n_clients']}c {c['frequency']}Hz {c['n_operations']}ops"
            label += f" | {'fault@' + str(c['failure_time']) if c['with_fault'] else 'no fault'}"
            label += f" | {'+'.join(m for m in MODES if c[f'use_{m}_requests'])} | {c['client_image']}"
        groups[label, e.config["n_replicas"]].append(e)
    metric_groups = {key: [summary_metrics(e, include_failed=include_failed, max_latency=max_latency)
                           for e in runs] for key, runs in groups.items()}
    labels = sorted({label for label, _ in groups})
    counts = sorted({count for _, count in groups})
    figures = []
    summary_specs = (
        ("Network usage", "Total TX + RX across replicas (MiB)", [("network_mib", "")]),
        ("CPU usage", "Mean CPU time per replica (s)", [("cpu_seconds", "")]),
    )
    def figure(title, xlabel, ylabel):
        fig = Figure(figsize=(9, 4.5), layout="constrained")
        ax = fig.subplots()
        ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
        ax.grid(True, axis="y", alpha=0.25)
        return fig, ax

    for title, ylabel, specs in summary_specs:
        fig, ax = figure(title, "Number of replicas", ylabel)
        nseries = len(labels) * len(specs)
        width = 0.8 / nseries
        plotted = False
        for i, label in enumerate(labels):
            for j, (metric, mode) in enumerate(specs):
                xs = sorted(count for l, count in groups if l == label)
                ys = [mean([r[metric] for r in metric_groups[label, x]]) for x in xs]
                if not any(math.isfinite(y) for y in ys):
                    continue
                plotted = True
                caption = f"{label}: {mode}" if mode else label
                if style == "curve":
                    ax.plot(xs, ys, marker="o", color=f"C{i % 10}",
                            linestyle="--" if mode == "consistent" else "-", label=caption)
                else:
                    positions = [counts.index(x) + (i * len(specs) + j - (nseries - 1) / 2) * width for x in xs]
                    ax.bar(positions, ys, width=width, color=f"C{i % 10}",
                           hatch="//" if mode == "consistent" else None, label=caption)
        ax.set_xticks(counts if style == "curve" else range(len(counts)), labels=counts)
        if plotted:
            ax.legend(fontsize=8)
        else:
            ax.text(0.5, 0.5, "No measurements available", transform=ax.transAxes, ha="center")
        ax.set_ylim(bottom=0)
        figures.append((title, fig))

    timelines = defaultdict(lambda: defaultdict(list))
    for e in experiments:
        if timeline_replicas is None or e.config["n_replicas"] == timeline_replicas:
            # Keep all config fields except replica image for image comparisons.
            key = tuple((k, v) for k, v in e.identity if k != "replica_image")
            timelines[key][e.label].append(e)
    for key, image_runs in sorted(timelines.items()):
        c = dict(key)
        runs = [e for group in image_runs.values() for e in group]
        durations = [(e.bounds[1] - e.bounds[0]) / 1e9 for e in runs if e.bounds[0] is not None]
        last = min(max(durations, default=0), end) if end is not None else max(durations, default=0)
        if last < start:
            continue
        if (last - start) / step > 20000:
            raise ValueError("Too many time points; increase step or shorten the time range")
        times = [start + i * step for i in range(math.floor((last - start) / step) + 1)]
        if times[-1] < last:
            times.append(last)
        curves = {label: average_series(group, times, window=window,
                                        include_failed=include_failed, max_latency=max_latency)
                  for label, group in image_runs.items()}
        caption = f"{c['n_replicas']} replicas · {c['n_clients']} clients · {c['frequency']} pacing/client · {c['n_operations']} ops/stream"
        for title, ylabel, specs in (
            ("Latency over time", "Mean request latency (ms)", [(f"{m}_latency_ms", m, s) for m, s in zip(MODES, ("-", "--"))]),
            ("Throughput over time", "Completed requests / second", [("total_throughput", "total", "-"), ("optimistic_throughput", "optimistic", "--"), ("consistent_throughput", "consistent", ":")]),
            ("Operation-log memory over time", "Mean log memory per replica (MiB)", [("memory_mib", "", "-")]),
        ):
            fig, ax = figure(f"{title}\n{caption}", "Seconds since first send", ylabel)
            plotted = False
            for i, (label, curve) in enumerate(sorted(curves.items())):
                for metric, mode, linestyle in specs:
                    values = curve[metric]
                    if any(math.isfinite(y) for y in values):
                        ax.plot(times, values, color=f"C{i % 10}", linestyle=linestyle,
                                label=f"{label}: {mode}" if mode else label)
                        plotted = True
            if last > start:
                ax.set_xlim(start, last)
            if plotted:
                ax.legend(fontsize=8)
            else:
                ax.text(0.5, 0.5, "No measurements available", transform=ax.transAxes, ha="center")
            ax.set_ylim(bottom=0)
            figures.append((f"{title} — {caption}", fig))
    return figures
