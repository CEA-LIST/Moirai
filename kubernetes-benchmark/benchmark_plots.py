"""Plot workload summaries and individual or averaged timelines."""

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from math import ceil, isfinite, nan
from statistics import mean

from matplotlib.figure import Figure

from benchmark_results import MIB, NS, achieved_latencies_ms, percentiles, summary


TITLES = {
    "network": "Network usage per replica",
    "cpu": "CPU time per replica",
    "sweep": "Commit throughput versus commit latency",
    "sweep_mean": "Commit throughput versus mean commit latency",
    "latency": "Commit latency over time",
    "throughput": "Commit throughput over time",
    "memory": "Log memory over time",
    "cpu_time": "CPU usage over time",
}

PALETTE = ("#1f77b4", "#d95f02", "#2ca02c", "#9467bd", "#8c564b", "#e377c2",
           "#7f7f7f", "#bcbd22", "#17becf", "#d62728")


# Protocol identity stays recognizable when colors or legend order change.
PROTOCOL_PATTERNS = {
    "commit": ("o", "///"),
    "creek": ("s", "xxx"),
    "timestamp": ("x", "..."),
}
FALLBACK_PATTERNS = (("^", "+++"), ("D", "ooo"), ("v", "---"), ("P", "***"))


def protocol_pattern(protocol, index=0):
    return PROTOCOL_PATTERNS.get(protocol, FALLBACK_PATTERNS[index % len(FALLBACK_PATTERNS)])


@dataclass
class PlotOptions:
    style: str = "line"
    window: float = 1.0
    step: float = 0.5
    start: float = 0.0
    end: float | None = None
    measurement_start: float | None = None
    measurement_end: float | None = None
    include_uncommitted: bool = False
    titles: dict = field(default_factory=dict)
    colors: dict = field(default_factory=dict)
    labels: dict = field(default_factory=dict)
    order: dict = field(default_factory=dict)
    ylimits: dict = field(default_factory=dict)

    def color(self, key, index):
        return self.colors.get(key, PALETTE[index % len(PALETTE)])

    def label(self, key, default):
        return self.labels.get(key, default)


def chart(options, key, xlabel, ylabel):
    figure = Figure(figsize=(8, 4), layout="constrained")
    axis = figure.subplots()
    axis.set(title=options.titles.get(key, TITLES[key]), xlabel=xlabel, ylabel=ylabel)
    axis.grid(alpha=0.2)
    figure.benchmark_data = {"schema_version": 1, "graph": key, "series": [], "annotations": []}
    if key in ("sweep", "sweep_mean"):
        figure.benchmark_data["measurement_interval"] = {
            "start_s": options.measurement_start, "end_s": options.measurement_end,
            "latency_selection": "commit time in (start, end]",
            "throughput_method": "interpolated cumulative counter delta / interval seconds",
            "enabled": options.measurement_start is not None or options.measurement_end is not None,
            "blank_end": "reference replica drain time",
        }
    return figure, axis


def record_series(axis, x, y, *, protocol, label, kind="line", **metadata):
    """Keep semantic x values (including bar categories) alongside plot data."""
    axis.figure.benchmark_data["series"].append({
        "protocol": protocol, "label": label, "kind": kind, **metadata,
        "points": [{"x": float(a) if isfinite(a) else None,
                    "y": float(b) if isfinite(b) else None} for a, b in zip(x, y)],
    })


def legend(axis):
    if axis.get_legend_handles_labels()[0]:
        axis.legend(fontsize="small")


def comparison_plots(runs, basis, sweep, options):
    if not runs:
        return []
    if sweep:
        xlabel = "Offered operations/s" if basis == "total" else "Offered operations/s per replica"
    else:
        xlabel = "Configured replica count"
    network, net_ax = chart(options, "network", xlabel, "Mean RX + TX (MiB)")
    cpu, cpu_ax = chart(options, "cpu", xlabel, "Mean CPU time (s)")
    grouped = defaultdict(lambda: defaultdict(list))
    summaries = {}
    for run in runs:
        x = run.load(basis) if sweep else run.replicas
        grouped[run.implementation][x].append(run)
        summaries[run.name] = summary(run, options.measurement_start, options.measurement_end)
    axis_values = sorted({x for points in grouped.values() for x in points})
    positions = {value: index for index, value in enumerate(axis_values)}
    width = 0.8 / len(grouped)
    if sweep:
        ylabel = "Latency lower bound (ms)" if options.include_uncommitted else "Commit latency (ms)"
        latency, latency_ax = chart(options, "sweep", "Achieved committed updates/s", ylabel)
    protocol_indices = {name: index for index, name in
                        enumerate(sorted({protocol for protocol, _ in grouped}))}
    for index, ((protocol, image), points) in enumerate(sorted(grouped.items(), key=lambda item: (options.order.get(item[0][0], 0), item[0]))):
        xs = sorted(points)
        label = options.label(protocol, protocol)
        if sum(key[0] == protocol for key in grouped) > 1:
            label = f"{label} / {image}"
        color = options.color(protocol, protocol_indices[protocol])
        marker, hatch = protocol_pattern(protocol, protocol_indices[protocol])
        for metric, axis in (("network", net_ax), ("cpu", cpu_ax)):
            values = [mean(summaries[run.name][metric] for run in points[x]) for x in xs]
            if options.style == "bar":
                xs_bar = [positions[x] - 0.4 + (index + 0.5) * width for x in xs]
                axis.bar(xs_bar, values, width, color=color, label=label,
                         hatch=hatch, edgecolor="black", linewidth=0.6)
                axis.set_xticks(range(len(axis_values)), axis_values)
            else:
                axis.plot(xs, values, marker=marker, color=color, label=label)
            record_series(axis, xs, values, protocol=protocol, label=label,
                          kind=options.style, image=image, metric=metric)
        if sweep:
            rate_cuts = {}
            for x in xs:
                available = [(summaries[run.name]["throughput"], cuts) for run in points[x]
                             if isfinite(summaries[run.name]["throughput"])
                             and (cuts := percentiles(achieved_latencies_ms(
                                 run, options.include_uncommitted, options.measurement_start, options.measurement_end)))]
                if available:
                    rate_cuts[x] = available
            for p, linestyle in ((50, "-"), (95, "--"), (99, ":")):
                if rate_cuts:
                    achieved = [mean(rate for rate, _ in data) for data in rate_cuts.values()]
                    values = [mean(cuts[p] for _, cuts in data) for data in rate_cuts.values()]
                    latency_ax.plot(achieved, values, linestyle, marker=marker, color=color,
                                    label=f"{label} p{p}")
                    record_series(latency_ax, achieved, values, protocol=protocol,
                                  label=f"{label} p{p}", image=image, percentile=p,
                                  include_uncommitted=options.include_uncommitted)
    legend(net_ax)
    legend(cpu_ax)
    figures = [network, cpu]
    if sweep:
        legend(latency_ax)
        if not latency_ax.lines:
            latency_ax.text(0.5, 0.5, "No latency observations", transform=latency_ax.transAxes,
                            ha="center")
        figures.append(latency)
        figures.append(mean_commit_latency_plot(runs, basis, options))
    return figures


def mean_commit_latency_plot(runs, basis, options):
    """Pool all committed-operation latencies per protocol and offered load.

    Pending operations are excluded even when lower bounds are enabled for the
    other plots. Throughput is averaged across runs contributing commit samples.
    """
    figure, axis = chart(options, "sweep_mean", "Achieved committed updates/s",
                         "Mean commit latency (ms; all committed operations)")
    grouped = defaultdict(lambda: defaultdict(list))
    for run in runs:
        grouped[run.config["protocol"]][run.load(basis)].append(run)
    indices = {protocol: index for index, protocol in enumerate(sorted(grouped))}
    for protocol in sorted(grouped, key=lambda key: (options.order.get(key, 0), key)):
        points = []
        for load, matching in sorted(grouped[protocol].items()):
            rates, delays = [], []
            for run in matching:
                samples = achieved_latencies_ms(run, False, options.measurement_start, options.measurement_end)
                rate = summary(run, options.measurement_start, options.measurement_end)["throughput"]
                if samples and isfinite(rate):
                    rates.append(rate)
                    delays.extend(samples)
            if delays:
                points.append((mean(rates), mean(delays)))
        if points:
            achieved, latencies = zip(*points)
            axis.plot(achieved, latencies, marker=protocol_pattern(protocol, indices[protocol])[0],
                      color=options.color(protocol, indices[protocol]),
                      label=options.label(protocol, protocol))
            record_series(axis, achieved, latencies, protocol=protocol,
                          label=options.label(protocol, protocol), aggregation="mean of all committed operations",
                          include_uncommitted=False)
    legend(axis)
    if not axis.lines:
        axis.text(0.5, 0.5, "No committed latency observations",
                  transform=axis.transAxes, ha="center")
    return figure


def interpolate(xs, ys, x):
    if x < xs[0] or x > xs[-1]:
        return nan
    index = bisect_right(xs, x) - 1
    if x == xs[index]:
        return ys[index]
    span = xs[index + 1] - xs[index]
    return ys[index] + (x - xs[index]) / span * (ys[index + 1] - ys[index])


def replica_timeline(samples, pending, times, options):
    elapsed = [row["elapsed_ns"] / NS for row in samples]
    if elapsed[0] > 0:
        counter_times = [0.0, *elapsed]
        prefix = [0]
    else:
        counter_times = elapsed
        prefix = []
    commits = prefix + [row["committed_updates"] for row in samples]
    cpu = prefix + [row["resources"]["cpu_time_us"] / 1_000_000 for row in samples]
    memory = [row["log_memory_bytes"] / MIB for row in samples]
    observations = [(stamp / NS, delay / 1_000_000) for row in samples
                    for stamp, delay, _ in row["commit_latency_samples"]]
    failed = []
    if options.include_uncommitted and pending is not None:
        seal, updates = pending
        observations.extend((seal / NS, (seal - started) / 1_000_000) for started, _ in updates)
        failed = sorted(started / NS for started, _ in updates)
    observations.sort()
    stamps = [stamp for stamp, _ in observations]
    curves = {key: [] for key in ("latency", "throughput", "memory", "cpu_time", "uncommitted")}
    for t in times:
        begin = max(0.0, t - options.window)
        span = t - begin
        in_window = observations[bisect_right(stamps, begin):bisect_right(stamps, t)] if t <= elapsed[-1] else []
        latency = percentiles([delay for _, delay in in_window]).get(95, nan)
        curves["latency"].append(latency)
        curves["memory"].append(interpolate(elapsed, memory, t))
        for key, data in (("throughput", commits), ("cpu_time", cpu)):
            delta = interpolate(counter_times, data, t) - interpolate(counter_times, data, begin)
            curves[key].append(delta / span if span > 0 else nan)
        missed = bisect_right(failed, t) - bisect_right(failed, begin)
        known = pending is not None and t <= pending[0] / NS
        curves["uncommitted"].append(missed / span if options.include_uncommitted and known and span > 0 else nan)
    return curves


def mean_curves(curves):
    return {key: [mean(available) if (available := [value for value in points if isfinite(value)]) else nan
                  for points in zip(*(curve[key] for curve in curves))]
            for key in curves[0]}


def timelines(runs, individual, options):
    metrics = {
        "latency": "p95 latency lower bound (ms)" if options.include_uncommitted else "p95 commit latency (ms)",
        "throughput": "Updates/s" if options.include_uncommitted else "Observed committed updates/s",
        "memory": "Log memory (MiB)",
        "cpu_time": "CPU seconds per wall second",
    }
    charts = {key: chart(options, key, "Seconds since local workload start", unit)
              for key, unit in metrics.items()}
    epochs = [rows[-1]["elapsed_ns"] / NS for run in runs for rows in run.samples.values() if rows]
    observed_end = max(epochs, default=0)
    end = min(options.end, observed_end) if options.end is not None else observed_end
    step = max(options.step, (end - options.start) / 9999)
    times = []
    if end >= options.start:
        times = [options.start + i * step for i in range(ceil((end - options.start) / step))
                 if options.start + i * step < end]
        times = sorted(set(times + [end] + [epoch for epoch in epochs if options.start <= epoch <= end]))
    grouped = defaultdict(list)
    for run in runs:
        replicas = {
            replica: replica_timeline(rows, run.uncommitted.get(replica), times, options)
            for replica, rows in run.samples.items() if rows
        }
        if individual:
            for replica, curves in replicas.items():
                grouped[f"Replica {replica}"].append(curves)
        elif replicas:
            # Same weight per contributing replica, then same weight per run.
            grouped[run.config["protocol"]].append(mean_curves(list(replicas.values())))
    for index, (key, group) in enumerate(sorted(grouped.items())):
        curves = mean_curves(group)
        label = options.label(key, key)
        color = options.color(key, index)
        for metric, (_, axis) in charts.items():
            axis.plot(times, curves[metric], color=color, label=label,
                      marker=("." if metric == "latency" else None) if individual else protocol_pattern(key, index)[0],
                      markevery=1 if individual else max(1, len(times) // 20))
            record_series(axis, times, curves[metric],
                          protocol=runs[0].config["protocol"] if individual else key,
                          label=label, replica=key.removeprefix("Replica ") if individual else None,
                          metric=metric, include_uncommitted=options.include_uncommitted)
        if any(value > 0 for value in curves["uncommitted"]):
            charts["throughput"][1].plot(times, curves["uncommitted"], color=color,
                                        linestyle="--", label=f"{label} pending at seal (local)",
                                        marker=None if individual else protocol_pattern(key, index)[0],
                                        markevery=max(1, len(times) // 20))
            record_series(charts["throughput"][1], times, curves["uncommitted"],
                          protocol=runs[0].config["protocol"] if individual else key,
                          label=f"{label} pending at seal (local)", metric="uncommitted",
                          replica=key.removeprefix("Replica ") if individual else None)
    fault = runs[0].config["fault_after_s"]
    for metric, (_, axis) in charts.items():
        if metric in options.ylimits:
            axis.set_ylim(*options.ylimits[metric])
        if fault is not None:
            axis.axvline(fault, color="black", linestyle=":", label="Scheduled crash")
            axis.figure.benchmark_data["annotations"].append(
                {"kind": "vertical_line", "x": fault, "label": "Scheduled crash"})
        if options.end is not None:
            axis.set_xlim(options.start, options.end)
        elif observed_end > options.start:
            axis.set_xlim(options.start, observed_end)
        legend(axis)
    return [figure for figure, _ in charts.values()]
