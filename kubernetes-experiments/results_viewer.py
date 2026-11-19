#!/usr/bin/env python3
"""Local interactive Matplotlib viewer for Kubernetes experiment bundles."""

import argparse
import base64
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from results import build_figures, load_results

DEFAULT_RESULTS = Path(__file__).resolve().parent / "results"

STYLE = """
:root { color-scheme: light; font: 15px system-ui, sans-serif; color: #172d3e; background: #f3f6f8; }
body { margin: 0 auto; max-width: 1450px; padding: 24px; }
h1 { margin-bottom: 6px; } h2 { margin-top: 0; font-size: 18px; }
p { line-height: 1.55; } .muted { color: #526675; }
form, article, details, .notice { background: white; border: 1px solid #dce4e9; border-radius: 10px; padding: 20px; margin: 20px 0; }
form { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 16px; align-items: end; }
label { display: flex; flex-direction: column; gap: 7px; font-weight: 600; }
input, select, button { font: inherit; padding: 9px; border: 1px solid #b8c6d0; border-radius: 5px; min-width: 0; }
button { color: white; background: #215b79; border: 0; cursor: pointer; }
.check { flex-direction: row; align-items: center; font-weight: 400; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 550px), 1fr)); gap: 20px; }
article { margin: 0; padding: 14px; } img { display: block; width: 100%; height: auto; }
a { color: #215b79; } table { border-collapse: collapse; width: 100%; font-size: 13px; }
td, th { padding: 9px; border-bottom: 1px solid #dce4e9; text-align: left; overflow-wrap: anywhere; }
.scroll { overflow-x: auto; } summary { cursor: pointer; font-weight: 600; }
.error { border-color: #d79e6b; } ul { line-height: 1.7; }
"""


def select(name, title, options, selected):
    choices = "".join(
        f'<option value="{escape(str(value), quote=True)}"'
        f'{" selected" if str(value) == selected else ""}>{escape(str(label))}</option>'
        for value, label in options
    )
    return f'<label>{escape(title)}<select name="{name}">{choices}</select></label>'


def input_number(name, title, value, minimum=0):
    return (f'<label>{escape(title)}<input type="number" name="{name}" min="{minimum}" '
            f'step="any" value="{escape(value, quote=True)}"></label>')


def render_page(directory, query=None, *, static=False):
    """Build a self-contained page. query uses the same options as the web form."""
    query = query or {}
    value = lambda key, default="": query.get(key, [default])[-1]
    experiments, warnings = load_results(directory)
    bundles = sorted({e.bundle for e in experiments})
    bundle = value("bundle", bundles[0] if bundles else "")
    experiments = [e for e in experiments if e.bundle == bundle]
    run_ids = sorted({e.run_id for e in experiments})
    run = value("run", "average")
    if run != "average" and run not in run_ids:
        run = "average"  # A folder change may invalidate the previous run selection.
    clients = value("clients", "all")
    timeline = value("replicas", "")
    style = value("style", "curve")
    try:
        directory = Path(directory).resolve()
        def bundle_label(path):
            try:
                return str(Path(path).relative_to(directory)) or directory.name
            except ValueError:
                return str(Path(path))
        client_counts = sorted({e.config["n_clients"] for e in experiments})
        replica_counts = sorted({e.config["n_replicas"] for e in experiments})
        if not timeline or (timeline != "all" and timeline not in {str(r) for r in replica_counts}):
            timeline = str(replica_counts[0]) if replica_counts else "all"
        controls = select("bundle", "Experiment folder", [(b, bundle_label(b)) for b in bundles], bundle)
        controls += select("run", "Run", [("average", "Average of all identified runs"), *((r, r) for r in run_ids)], run)
        controls += select("clients", "Client count", [("all", "All client counts"), *((c, c) for c in client_counts)], clients)
        controls += select("replicas", "Time graphs: replicas", [("all", "All replica counts"), *((r, r) for r in replica_counts)], timeline)
        controls += select("style", "Replica comparison style", [("curve", "Lines"), ("bar", "Grouped bars")], style)
        controls += input_number("window", "Trailing window (s)", value("window", "2"), minimum=0.001)
        controls += input_number("step", "Time step (s)", value("step", "0.25"), minimum=0.001)
        controls += input_number("start", "Start time (s)", value("start", "0"))
        controls += input_number("end", "End time (s), blank = last send", value("end"))
        controls += input_number("max_latency", "Max latency (s), blank = no limit", value("max_latency"))
        controls += ('<label class="check"><input type="checkbox" name="include_failed" value="1"'
                     + (' checked' if value("include_failed") == "1" else '') + '>Include failed requests</label>')
        controls += '<button type="submit">Update plots / reload results</button>'
        form = f'<form method="get">{controls}</form>' if not static else ""
        selected = [e for e in experiments if (run == "average" or e.run_id == run)
                    and (clients == "all" or str(e.config["n_clients"]) == clients)]
        options = dict(
            style=style, timeline_replicas=None if timeline == "all" else int(timeline),
            window=float(value("window", "2")), step=float(value("step", "0.25")),
            start=float(value("start", "0")), end=float(value("end")) if value("end") else None,
            max_latency=float(value("max_latency")) if value("max_latency") else None,
            include_failed=value("include_failed") == "1",
        )
        try:
            figures = build_figures(selected, **options)
            charts = []
            for index, (title, figure) in enumerate(figures):
                buffer = BytesIO()
                figure.savefig(buffer, format="png", dpi=120)
                encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
                figure.clear()
                uri = f"data:image/png;base64,{encoded}"
                charts.append(
                    f'<article><h2>{escape(title)}</h2><img alt="{escape(title, quote=True)}" src="{uri}">'
                    f'<a download="experiment-plot-{index + 1}.png" href="{uri}">Download PNG</a></article>'
                )
            plots = '<div class="grid">' + "".join(charts) + '</div>'
            if not selected:
                plots = '<p class="notice">No experiments match this selection. Choose a folder/run with measurements.</p>'
        except ValueError as error:
            plots = f'<p class="notice error">{escape(str(error))}</p>'
        included_ids = sorted({e.run_id for e in selected})
        description = (f"{len(selected)} experiments · {len(included_ids)} runs · "
                       + ("equal-weight average" if run == "average" else "single run"))
        rows = []
        for e in selected:
            attempts = sum(len(p) for p in e.attempts.values())
            successes = sum(len(p) for p in e.successes.values())
            c = e.config
            cells = [e.run_id, c["simulation_name"], c["n_replicas"], c["n_clients"],
                     c["frequency"], c["n_operations"], attempts, attempts - successes]
            rows.append('<tr>' + ''.join(f'<td>{escape(str(cell))}</td>' for cell in cells) + '</tr>')
        settings = (f"Window: {options['window']:g} s · step: {options['step']:g} s · "
                    f"time: {options['start']:g}–{options['end'] if options['end'] is not None else 'last send'} s · "
                    f"max latency: {options['max_latency'] if options['max_latency'] is not None else 'unlimited'} · "
                    f"failed requests: {'included' if options['include_failed'] else 'excluded'}")
        inventory = ('<details><summary>Included experiments and runs</summary><div class="scroll"><table>'
                     '<thead><tr><th>Run ID</th><th>Experiment</th><th>Replicas</th><th>Clients</th>'
                     '<th>Pacing/client</th><th>Ops/stream</th><th>Requests</th><th>Failed</th></tr></thead>'
                     '<tbody>' + ''.join(rows) + '</tbody></table></div></details>')
        warnings_html = ('<details class="error"><summary>Data notes (' + str(len(warnings)) + ')</summary><ul>'
                         + ''.join(f'<li>{escape(w)}</li>' for w in warnings) + '</ul></details>') if warnings else ''
        content = f'<p class="muted">{escape(description)}</p><p class="muted">{escape(settings)}</p>{form}{inventory}{warnings_html}{plots}'
    except (ValueError, OverflowError) as error:
        content = f'<p class="notice error">{escape(str(error))}</p><p><a href="/">Reset controls</a></p>'
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            f'<title>Moirai experiment results</title><style>{STYLE}</style></head><body>'
            '<h1>Moirai experiment results</h1>'
            f'<p class="muted">Results: {escape(str(directory))}</p>' + content + '''
<details><summary>How metrics are calculated</summary>
<p>Each run contributes equally to averages. Network traffic is summed across
recorded, comparable replicas; CPU is averaged across those replicas.
Client latencies pool operations within each run.
Missing measurements remain gaps; failed requests are excluded unless selected.
A maximum latency limit applies to both latency and throughput.</p>
<p>Time axes align runs to their first unfiltered send. Trailing windows are
(t − window, t], and throughput is the completion count divided by the full
window, including startup. Runs stop contributing request metrics after their
last send. Disabled modes have zero throughput. No requests are inferred from
missing client records.</p>
<p>Memory uses timestamped operation-log bytes, not total container memory.
Per-replica samples are linearly interpolated with no extrapolation and then
averaged. Network totals include TX + RX on the pod interfaces, including client
traffic. Inter-replica packets count at both endpoints. Missing or incomparable
replica counters are excluded, so partial totals cover only measured replicas.
CPU is cumulative CPU time, in seconds.</p>
<p>Replica comparisons include all selected replica counts; the replica selector
only limits time graphs. Distinct workloads are never averaged together.
Partial runs remain visible with data notes; missing runs are not imputed.</p>
</details></body></html>''')


def make_handler(directory):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path != "/":
                self.send_error(404)
                return
            try:
                body = render_page(directory, parse_qs(url.query)).encode("utf-8")
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
    parser.add_argument("--port", type=int, default=8000)
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
