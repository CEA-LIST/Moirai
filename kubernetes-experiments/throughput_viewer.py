#!/usr/bin/env python3
"""Fixed-replica network and CPU comparisons against configured throughput."""

import argparse
import base64
from collections import defaultdict
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
import math
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from matplotlib.colors import is_color_like
from matplotlib.figure import Figure

from results import IMAGE_PALETTE, MODES, load_results, mean, summary_metrics
from results_viewer import DEFAULT_RESULTS, STYLE, input_number, select, text_input


def measured_throughput(experiment):
    """Successful completions / observed request duration, across both modes.

    Failed attempts extend the observation period but do not count as completions.
    Missing enabled streams leave the throughput unknown.
    """
    if any(experiment.config[f"use_{mode}_requests"] and mode not in experiment.recorded_modes
           for mode in MODES):
        return math.nan
    attempts = [p for mode in MODES for p in experiment.attempts[mode]]
    if not attempts:
        return math.nan
    duration = (max(r for _, r in attempts) - min(s for s, _ in attempts)) / 1e9
    return sum(len(experiment.successes[m]) for m in MODES) / duration if duration > 0 else math.nan


def configured_throughput(experiment):
    """Nominal total request rate, matching the configuration generator."""
    c = experiment.config
    return c["frequency"] * c["n_clients"] * sum(c[f"use_{mode}_requests"] for mode in MODES)


def build_figures(experiments, *, replicas, style="curve", image_colors=None,
                  image_labels=None, image_order=None, titles=None):
    """One graph per resource, with one series per image over configured load."""
    if style not in ("curve", "bar"):
        raise ValueError("style must be curve or bar")
    if len({e.bundle for e in experiments}) > 1:
        raise ValueError("Select one experiment folder")
    selected = [e for e in experiments if e.config["n_replicas"] == replicas]
    images = sorted({e.label for e in selected})
    colors = {image: IMAGE_PALETTE[i % len(IMAGE_PALETTE)] for i, image in enumerate(images)}
    colors.update(image_colors or {})
    if any(not is_color_like(c) for c in colors.values()):
        raise ValueError("Invalid image color")
    labels, titles = image_labels or {}, titles or {}
    order = {image: i for i, image in enumerate(image_order or images)}
    if not selected:
        return []
    points = defaultdict(list)
    for e in selected:
        points[e.label, configured_throughput(e)].append(e)
    series = defaultdict(list)
    for (image, throughput), runs in points.items():
        metrics = [summary_metrics(e) for e in runs]
        for metric in ("network_mib", "cpu_seconds"):
            series[image, metric].append((throughput, mean([m[metric] for m in metrics])))
    images.sort(key=lambda image: (order.get(image, len(order)), image))
    xs = sorted({x for _, x in points})
    spacing = min((b - a for a, b in zip(xs, xs[1:])), default=max(xs, default=1) * 0.1 or 1)
    width = 0.8 * spacing / len(images)
    figures = []
    for metric, title, ylabel in (
        ("network_mib", "System-wide network usage", "Total TX + RX across replicas (MiB)"),
        ("cpu_seconds", "Per-replica CPU usage", "Mean CPU time per replica (s)"),
    ):
        fig = Figure(figsize=(9, 4.5), layout="constrained")
        ax = fig.subplots()
        ax.set(title=titles.get(title, f"{title}\n{replicas} replicas"),
               xlabel="Configured throughput (requests / second)", ylabel=ylabel)
        for i, image in enumerate(images):
            data = sorted(series[image, metric])
            if not any(math.isfinite(y) for _, y in data):
                continue
            x, y = zip(*data)
            if style == "curve":
                ax.plot(x, y, marker="o", label=labels.get(image, image), color=colors[image])
            else:
                positions = [v + (i - (len(images) - 1) / 2) * width for v in x]
                ax.bar(positions, y, width=width, label=labels.get(image, image), color=colors[image])
        ax.set_xticks(xs)
        ax.grid(True, axis="y", alpha=0.25)
        ax.set_ylim(bottom=0)
        if ax.has_data():
            ax.legend(fontsize=8)
        else:
            ax.text(0.5, 0.5, "No comparable measurements available", transform=ax.transAxes, ha="center")
        figures.append((title, fig))
    return figures


def render_page(directory, query=None, *, static=False):
    query = query or {}
    value = lambda key, default="": query.get(key, [default])[-1]
    experiments, warnings = load_results(directory)
    bundles = sorted({e.bundle for e in experiments})
    bundle = value("bundle", bundles[0] if bundles else "")
    experiments = [e for e in experiments if e.bundle == bundle]
    run_ids = sorted({e.run_id for e in experiments})
    run = value("run", "average")
    if run not in run_ids:
        run = "average"
    counts = sorted({e.config["n_replicas"] for e in experiments})
    replicas = value("replicas", str(counts[0]) if counts else "1")
    if replicas not in {str(c) for c in counts}:
        replicas = str(counts[0]) if counts else "1"
    controls = select("bundle", "Experiment folder", [(b, b) for b in bundles], bundle)
    controls += select("run", "Run", [("average", "Average of identified runs"), *((r, r) for r in run_ids)], run)
    controls += select("replicas", "Fixed replica count", [(c, c) for c in counts], replicas)
    controls += select("style", "Comparison style", [("curve", "Lines"), ("bar", "Grouped bars")], value("style", "curve"))
    try:
        images = sorted({e.label for e in experiments})
        colors, labels, ranks = {}, {}, {}
        for i, image in enumerate(images):
            colors[image] = value("color:" + image, IMAGE_PALETTE[i % len(IMAGE_PALETTE)])
            labels[image] = value("label:" + image, image)
            ranks[image] = int(value("order:" + image, str(i + 1)))
            controls += text_input("color:" + image, image + " — color", colors[image], "color")
            controls += text_input("label:" + image, image + " — label", labels[image])
            controls += input_number("order:" + image, image + " — bar / legend order (lower first)", str(ranks[image]), 1)
        selected = [e for e in experiments if run == "average" or e.run_id == run]
        figures = build_figures(selected, replicas=int(replicas), style=value("style", "curve"),
                                image_colors=colors, image_labels=labels,
                                image_order=sorted(images, key=lambda image: (ranks[image], image)),
                                titles={k[6:]: v[-1] for k, v in query.items() if k.startswith("title:")})
        charts = []
        for i, (key, fig) in enumerate(figures):
            title = fig.axes[0].get_title()
            controls += text_input("title:" + key, "Graph title: " + key, value("title:" + key, title.replace("\n", " — ")))
            buffer = BytesIO()
            fig.savefig(buffer, format="png", dpi=120)
            fig.clear()
            uri = "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")
            charts.append(f'<article><h2>{escape(title)}</h2><img alt="{escape(title, quote=True)}" src="{uri}">'
                          f'<a download="throughput-plot-{i + 1}.png" href="{uri}">Download PNG</a></article>')
        controls += '<button type="submit">Update plots / reload results</button>'
        form = f'<form method="get">{controls}</form>' if not static else ''
        plots = '<div class="grid">' + ''.join(charts) + '</div>' if charts else '<p class="notice">No experiments match this selection.</p>'
        content = form + plots
    except (ValueError, OverflowError) as error:
        content = f'<p class="notice error">{escape(str(error))}</p><a href="/">Reset controls</a>'
    notes = ''.join(f'<li>{escape(w)}</li>' for w in warnings)
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            f'<title>Moirai throughput comparisons</title><style>{STYLE}</style></head><body>'
            '<h1>Moirai throughput comparisons</h1>' + content +
            '<details><summary>How metrics are calculated</summary>'
            '<p>Configured throughput is frequency × client count × number of enabled request modes, '
            'matching the experiment generator. It is nominal request pacing, not measured completions.</p>'
            '<p>Network usage is cumulative TX + RX across measured comparable replicas, in MiB, including client traffic; '
            'inter-replica traffic counts at both endpoints. CPU is cumulative CPU time averaged across measured comparable replicas, '
            'in seconds. Partial resource totals cover only measured replicas.</p>'
            '<p>All selected experiments at the fixed replica count appear in these two graphs. '
            'At each configured throughput, available resource measurements are averaged with equal weight '
            'across runs and configurations for each image. '
            'Bars are offset around configured throughput in image order; lines use exact configured coordinates.</p></details>'
            + (f'<details><summary>Data notes</summary><ul>{notes}</ul></details>' if notes else '') + '</body></html>')


def make_handler(directory):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path != "/":
                self.send_error(404)
                return
            try:
                body = render_page(directory, parse_qs(url.query, keep_blank_values=True)).encode("utf-8")
            except (OSError, ValueError) as error:
                self.send_error(400, str(error))
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="?", type=Path, default=DEFAULT_RESULTS,
                        help="Results folder to scan recursively (default: this script's results/)")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--export", type=Path, metavar="REPORT.html",
                        help="Write a self-contained HTML report instead of serving")
    args = parser.parse_args()
    if not args.results.expanduser().is_dir():
        parser.error(f"Results folder does not exist: {args.results}")
    if args.export:
        args.export.write_text(render_page(args.results, static=True), encoding="utf-8")
        print(f"Report saved: {args.export.resolve()}")
        return
    with HTTPServer(("127.0.0.1", args.port), make_handler(args.results)) as server:
        print(f"Open http://127.0.0.1:{server.server_port} (Ctrl+C to stop)", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
