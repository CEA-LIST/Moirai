"""Serve the shared browser UI and export self-contained plot reports."""

import argparse
from base64 import b64encode
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
import json
from math import isfinite
import re
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from benchmark_plots import PlotOptions, TITLES, comparison_plots, timelines
from benchmark_results import load_runs, measurement_interval, summary


def select(name, label, choices, current):
    options = "".join(
        f'<option value="{escape(str(value))}"{" selected" if str(value) == str(current) else ""}>'
        f"{escape(str(text))}</option>" for value, text in choices
    )
    return f'<label>{escape(label)} <select name="{name}">{options}</select></label>'


def figure_html(figure):
    axis = figure.axes[0]
    payload = {
        **figure.benchmark_data,
        "title": axis.get_title(),
        "x_label": axis.get_xlabel(), "y_label": axis.get_ylabel(),
        "x_limits": list(axis.get_xlim()), "y_limits": list(axis.get_ylim()),
    }
    encoded_json = b64encode(json.dumps(payload, allow_nan=False, indent=2).encode("utf-8")).decode("ascii")
    filename = "benchmark-" + payload["graph"]
    image = BytesIO()
    figure.savefig(image, format="png", dpi=125)
    figure.clear()
    data = b64encode(image.getvalue()).decode("ascii")
    return (f'<article><a download="{filename}.png" href="data:image/png;base64,{data}">'
            f'<img alt="{escape(payload["title"], quote=True)}" src="data:image/png;base64,{data}"></a>'
            f'<a download="{filename}.json" href="data:application/json;base64,{encoded_json}">'
            'Download graph data (JSON)</a></article>')


def describe_conditions(conditions):
    config = json.loads(conditions)
    settings = config["replica_settings"]
    fault = "healthy" if config["fault_after_s"] is None else f'crash at {config["fault_after_s"]} s'
    return (
        f'Benchmark v{config["benchmark_version"]}, {config["duration_s"]} s, {config["query_percent"]}% queries, {config["add_percent"]}% adds, '
        f'{config["key_space"]} keys, queue {config["queue_capacity"]}, {fault}, '
        f'sample {config["sample_interval_ms"]} ms, grace {config["grace_s"]} s, '
        f'interface {config["network_interface"]}, pull {settings["pull_interval_ms"]} ms / '
        f'{settings["pull_peer_count"]} peers, no-op {settings["noop_interval_ms"]} ms, '
        f'oracle timeout {settings["event_timeout_ms"]} ms'
    )


def input_control(name, label, value, kind="text", limits=""):
    return (f'<label>{escape(label)} <input type="{kind}" name="{escape(name)}" '
            f'value="{escape(str(value))}" {limits}></label>')


def number(value, default, minimum=0):
    try:
        result = float(value)
        if isfinite(result) and result >= minimum:
            return result
    except (TypeError, ValueError):
        pass
    return default


def preset_order(preset):
    config = json.loads(preset)
    return config["fault_after_s"] is not None, -config["benchmark_version"], preset


def page(root, query, sweep, interactive=True):
    runs, notes = load_runs(root)
    title = "Commit throughput sweep" if sweep else "Commit benchmark results"
    get = lambda key, default: query.get(key, [default])[0]
    controls = []
    figures = []
    inventory = []
    detail = "<p>No completed benchmark runs found.</p>"
    if runs:
        conditions = sorted({run.conditions for run in runs}, key=preset_order)
        preset = get("conditions", conditions[0])
        if preset not in conditions:
            preset = conditions[0]
        group = [run for run in runs if run.conditions == preset]
        basis = get("basis", "total")
        if basis not in ("total", "per_replica"):
            basis = "total"
        controls.append(select("conditions", "Workload settings",
                               [(p, describe_conditions(p)) for p in conditions], preset))
        controls.append(select("basis", "Offered rate basis",
                               [("total", "All replicas"), ("per_replica", "Per replica")], basis))

        def selectable(key, label, eligible):
            chosen = get(key, str(eligible[0]))
            result = next((value for value in eligible if str(value) == chosen), eligible[0])
            controls.append(select(key, label, [(v, v) for v in eligible], result))
            return result

        if sweep:
            label = "Replica count"
            fixed = selectable("fixed", label, sorted({run.replicas for run in group}))
            matching = [run for run in group if run.replicas == fixed]
            rate = selectable("timeline_rate", "Timeline offered operations/s",
                              sorted({run.load(basis) for run in matching}))
            count = fixed
        else:
            label = "Offered operations/s"
            fixed = selectable("fixed", label, sorted({run.load(basis) for run in group}))
            matching = [run for run in group if run.load(basis) == fixed]
            count = selectable("timeline_replicas", "Timeline replica count",
                               sorted({run.replicas for run in matching}))
            rate = fixed

        compatible = [run for run in matching if run.replicas == count and run.load(basis) == rate]
        selected = get("timeline", "")
        individual = next((run for run in compatible if run.name == selected), None)
        controls.append(select("timeline", "Timeline view",
                               [("", "Protocol averages across all matching runs")]
                               + [(run.name, run.name) for run in compatible], selected if individual else ""))
        options = PlotOptions(
            style="bar" if get("style", "line") == "bar" else "line",
            window=number(get("window", "1"), 1.0, 0.01),
            step=number(get("step", "0.5"), 0.5, 0.01),
            start=number(get("start", "0"), 0.0),
            end=number(get("end", ""), None),
            include_uncommitted=get("include_uncommitted", "") == "yes",
        )
        if sweep:
            raw_start, raw_end = get("measurement_start", "").strip(), get("measurement_end", "").strip()
            try:
                options.measurement_start = float(raw_start) if raw_start else None
                options.measurement_end = float(raw_end) if raw_end else None
                if any(value is not None and (not isfinite(value) or value < 0)
                       for value in (options.measurement_start, options.measurement_end)):
                    raise ValueError
            except ValueError:
                # Invalid input must not silently restore full-run statistics.
                options.measurement_start = options.measurement_end = 0.0
                notes.append("Achieved-metric bounds must be finite nonnegative seconds; no achieved points are shown.")
            controls.extend((
                input_control("measurement_start", "Achieved metrics start (s), blank = 0", raw_start,
                              "number", 'min="0" step="any"'),
                input_control("measurement_end", "Achieved metrics end (s), blank = drain", raw_end,
                              "number", 'min="0" step="any"'),
            ))
            if raw_start or raw_end:
                notes.append("Achieved throughput and both latency comparison graphs use the selected interval. "
                             "Throughput uses interpolated commit-counter deltas; latencies are selected by commit time in (start, end]. "
                             "A blank end uses each run's reference-replica drain time. Timelines retain their own controls.")
                for run in matching:
                    if measurement_interval(run, options.measurement_start, options.measurement_end) is None:
                        notes.append(f"{run.name}: no achieved metrics for this interval; require start < end "
                                     "and coverage through end in the reference replica's log.")
        if options.end is not None and options.end <= options.start:
            options.end = None
            notes.append("The timeline end must be after its start.")
        controls.append(select("style", "Resource comparison style",
                               [("line", "Lines"), ("bar", "Grouped bars")], options.style))
        controls.extend(input_control(key, name, value, "number", 'min="0" step="any"')
                        for key, name, value in (
                            ("window", "Trailing window (s)", options.window),
                            ("step", "Sample step (s)", options.step),
                            ("start", "Timeline start (s)", options.start),
                            ("end", "Timeline end (s)", "" if options.end is None else options.end),
                        ))
        controls.append(select("include_uncommitted", "Updates still pending at seal",
                               [("", "Exclude"), ("yes", "Include")],
                               "yes" if options.include_uncommitted else ""))
        if options.include_uncommitted:
            if any(run.version < 3 for run in matching):
                notes.append("These older logs lack per-operation records for updates pending at seal.")
            else:
                notes.append("Pending updates contribute latency lower bounds. Their local dequeue rate is drawn separately from commit throughput.")
        settings_controls = []
        for key, fallback in TITLES.items():
            if key in ("sweep", "sweep_mean") and not sweep:
                continue
            name = f"title_{key}"
            value = get(name, "")[:160].strip()
            if value:
                options.titles[key] = value
            settings_controls.append(input_control(name, f"{fallback} title", value))
            if key in ("latency", "throughput", "memory", "cpu_time"):
                lower = number(get(f"ymin_{key}", ""), None, float("-inf"))
                upper = number(get(f"ymax_{key}", ""), None, float("-inf"))
                if lower is not None and upper is not None and lower >= upper:
                    notes.append(f"{fallback} y-axis minimum must be smaller than its maximum.")
                else:
                    options.ylimits[key] = (lower, upper)
                for prefix, caption, bound in (("ymin", "minimum", lower), ("ymax", "maximum", upper)):
                    settings_controls.append(input_control(
                        f"{prefix}_{key}", f"{fallback} y-axis {caption}",
                        "" if bound is None else bound, "number", 'step="any"'))
        named_protocols = sorted({run.config["protocol"] for run in matching})
        named_series = named_protocols.copy()
        if individual:
            named_series.extend(f"Replica {key}" for key in sorted(individual.samples))
        for key in named_series:
            if key in named_protocols:
                order = number(get(f"order_{key}", "0"), 0, float("-inf"))
                options.order[key] = order
                settings_controls.append(input_control(
                    f"order_{key}", f"{key} bar position (lower first)", order, "number", 'step="any"'))
            label_name, color_name = f"label_{key}", f"color_{key}"
            label_value, color_value = get(label_name, "")[:80].strip(), get(color_name, "")
            if label_value:
                options.labels[key] = label_value
            if re.fullmatch(r"#[0-9a-fA-F]{6}", color_value):
                options.colors[key] = color_value
            settings_controls.extend((
                input_control(label_name, f"{key} label", label_value),
                input_control(color_name, f"{key} color (hex)", options.colors.get(key, "")),
            ))
        controls.append("<details><summary>Plot titles, colors, labels, bar order and y-axis limits</summary><div>"
                        + "".join(settings_controls) + "</div></details>")

        figures.extend(comparison_plots(matching, basis, sweep, options))
        figures.extend(timelines([individual] if individual else compatible, bool(individual), options))
        for run in matching:
            last_samples = [rows[-1] for rows in run.samples.values() if rows and rows[-1]["final"]]
            eligible = sum(row["processed_updates"] for row in last_samples)
            measured = sum(row["committed_local_updates"] for row in last_samples)
            dropped = sum(done["queue_full"] + done["missed_slots"] for done in run.workload_done.values())
            achieved = summary(run, options.measurement_start, options.measurement_end)["throughput"]
            achieved_text = f"{achieved:.2f}" if isfinite(achieved) else "Unavailable"
            inventory.append(
                f"<tr><td>{escape(run.name)}</td><td>{escape(run.config['protocol'])}</td>"
                f"<td>{run.replicas}</td><td>{run.load(basis)}</td>"
                f"<td>{achieved_text}</td><td>{measured}/{eligible}</td><td>{dropped}</td></tr>"
            )
        timeline_name = escape(individual.name) if individual else f"Protocol averages across {len(compatible)} runs"
        detail = (f"<p>{escape(describe_conditions(preset))} · {escape(label)} {fixed}</p>"
                  f"<p>Timelines · {timeline_name} · {count} replicas · {rate} offered ops/s</p>")

    form = ('<form action="/" method="get">' + "".join(controls)
            + '<button>Update plots and reload runs</button></form>') if interactive else ""
    table = ""
    if inventory:
        table = ("<h2>Runs</h2><table><tr><th>Run</th><th>Protocol</th><th>Replicas</th>"
                 "<th>Offered ops/s</th><th>Achieved updates/s</th>"
                 "<th>Local commits / processed updates, sealed replicas</th>"
                 "<th>Dropped workload slots, drained replicas</th></tr>"
                 + "".join(inventory) + "</table>")
    notes_html = "".join(f"<li>{escape(note)}</li>" for note in notes)
    plots = "".join(figure_html(figure) for figure in figures)
    return f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
body{{margin:2rem auto;max-width:1120px;padding:0 1rem;font:16px system-ui;color:#17263a}}
form,details div{{display:flex;flex-wrap:wrap;gap:.75rem;align-items:end}}
label{{display:grid;gap:.25rem;max-width:100%}} select{{min-width:5rem;max-width:100%}}
details{{flex-basis:100%}} details div{{margin:.5rem 0}}
img{{width:100%;border:1px solid #ddd}} main{{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:1rem}}
td,th{{padding:.4rem;text-align:left;border-bottom:1px solid #ddd}} table{{display:block;overflow:auto}}
</style>
<h1>{title}</h1><p>{escape(str(root))}</p>{form}{detail}<main>{plots}</main>{table}<ul>{notes_html}</ul></html>"""


def run_viewer(sweep):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="?", type=Path, default=Path(__file__).with_name("results"))
    parser.add_argument("--port", type=int, default=8011 if sweep else 8010)
    parser.add_argument("--export", type=Path)
    parser.add_argument("--selection", default="", help="Optional query string copied from the viewer URL")
    args = parser.parse_args()
    root = args.results.resolve()
    if args.export:
        html = page(root, parse_qs(args.selection), sweep, interactive=False)
        args.export.write_text(html, encoding="utf-8")
        return

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path != "/":
                self.send_error(404)
                return
            html = page(root, parse_qs(url.query), sweep).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

    with HTTPServer(("127.0.0.1", args.port), Handler) as server:
        print(f"Open http://127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
