"""Numerical and viewer integration checks; no Kubernetes cluster required."""

import json
import math
from pathlib import Path
import tempfile
import unittest

from results import (average_series, build_figures, load_experiment, load_results,
                     summary_metrics, time_series)
from results_viewer import render_page

EPOCH = 1_790_000_000_000_000_000
MIB = 2**20


def pair(send, receive, epoch=EPOCH):
    return [epoch + int(send * 1e9), epoch + int(receive * 1e9)]


class ResultsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)

    def write(self, run="run-a", *, latencies=None, failed=None, config=None, records=None):
        cfg = dict(simulation_name="commit-awset-2r-1c", n_replicas=2, n_clients=1,
                   n_operations=10, frequency=10, with_fault=False, failure_time=2,
                   use_optimistic_requests=True, use_consistent_requests=False,
                   replica_image="commit:dev", client_image="client:dev", output_path=".")
        cfg.update(config or {})
        folder = self.root / "good-case/optimistic" / run / cfg["simulation_name"]
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "experiment.json").write_text(json.dumps(cfg))
        if latencies is None:
            latencies = [pair(0, 0.1), pair(1, 1.2), pair(2, 2.3)]
        if records is None:
            records = [{"client_id": 1, "mode": "optimistic", "latencies": latencies,
                        "failed_requests": failed or []},
                       {"type": "replica_resource_usage", "replicas": [
                           {"ordinal": 0, "comparable": True, "rx_bytes": {"delta": MIB},
                            "tx_bytes": {"delta": 3 * MIB}, "cpu_time_us": {"delta": 2_000_000}},
                           {"ordinal": 1, "comparable": True, "rx_bytes": {"delta": 2 * MIB},
                            "tx_bytes": {"delta": 4 * MIB}, "cpu_time_us": {"delta": 4_000_000}},
                       ]}]
            for replica in (1, 2):
                records.append({"type": "replica_log_memory_usage", "replica_id": replica,
                                "samples": [[replica * MIB, (EPOCH - 1_000_000_000) // 1_000_000],
                                            [replica * 3 * MIB, (EPOCH + 3_000_000_000) // 1_000_000]]})
        source = folder / f"{cfg['simulation_name']}-measurements.jsonl"
        source.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return source

    def test_config_and_run_discovery(self):
        self.write()
        self.write("run-b")
        (self.root / "unrelated.json").write_text("{}")
        experiments, warnings = load_results(self.root)
        self.assertEqual({e.run_id for e in experiments}, {"run-a", "run-b"})
        self.assertEqual(warnings, [])
        self.assertEqual(experiments[0].bounds, (EPOCH, EPOCH + 2_000_000_000))

    def test_summary_units_and_recorded_peaks(self):
        e = load_experiment(self.write())
        metrics = summary_metrics(e)
        self.assertEqual(metrics["network_mib"], 10)
        self.assertEqual(metrics["cpu_seconds"], 3)
        self.assertEqual(metrics["peak_memory_mib"], 4.5)
        self.assertAlmostEqual(metrics["optimistic_latency_ms"], 200)
        self.assertTrue(math.isnan(metrics["consistent_latency_ms"]))

    def test_window_boundaries_and_filter_preserve_origin(self):
        # At t=2 with window=1, receive=1 is excluded; receive=2 is included.
        pairs = [pair(0, 1), pair(0, 2), pair(2, 2.5)]
        e = load_experiment(self.write(latencies=pairs))
        series = time_series(e, [0, 1, 2, 3], window=1)
        self.assertEqual(series["optimistic_throughput"][:3], [0, 1, 1])
        self.assertTrue(math.isnan(series["optimistic_throughput"][3]))
        filtered = time_series(e, [1, 2], window=1, max_latency=1)
        self.assertEqual(filtered["optimistic_throughput"], [1, 0])
        self.assertEqual(e.bounds[0], EPOCH)
        self.assertEqual(summary_metrics(e, max_latency=1)["optimistic_latency_ms"], 750)

    def test_failures_excluded_or_explicitly_included(self):
        failed_pair = pair(1, 1.2)
        e = load_experiment(self.write(failed=[dict(start_ns=failed_pair[0], end_ns=failed_pair[1])]))
        excluded = time_series(e, [1.5], window=1)
        included = time_series(e, [1.5], window=1, include_failed=True)
        self.assertEqual(excluded["total_throughput"], [0])
        self.assertEqual(included["total_throughput"], [1])

    def test_memory_real_timestamps_and_no_extrapolation(self):
        e = load_experiment(self.write())
        result = time_series(e, [0, 1, 4], window=1)
        # At first send, samples at -1 and +3 seconds interpolate to 1.5x MiB.
        self.assertEqual(result["memory_mib"][:2], [2.25, 3])
        self.assertTrue(math.isnan(result["memory_mib"][2]))

    def test_equal_run_weights_not_operation_weights(self):
        a = load_experiment(self.write(latencies=[pair(0, 0.1), pair(2, 2.1)]))
        b = load_experiment(self.write("run-b", latencies=[pair(0, 0.9)] * 4 + [pair(2, 2.9)]))
        series = average_series([a, b], [1], window=1)
        self.assertAlmostEqual(series["optimistic_latency_ms"][0], 500)
        self.assertEqual(series["total_throughput"][0], 2.5)
        # Runs align to their own epoch, rather than time of day.
        shift = 60_000_000_000
        b.attempts["optimistic"] = [(s + shift, r + shift) for s, r in b.attempts["optimistic"]]
        b.successes["optimistic"] = list(b.attempts["optimistic"])
        shifted = average_series([a, b], [1], window=1)
        self.assertEqual(shifted["total_throughput"], series["total_throughput"])
        # A shorter run stops contributing, rather than diluting the average.
        a.attempts["optimistic"] = [tuple(pair(0, 0.1))]
        a.successes["optimistic"] = list(a.attempts["optimistic"])
        self.assertEqual(average_series([a, b], [1], window=1)["total_throughput"], [4])

    def test_missing_and_incomparable_resource_deltas(self):
        e = load_experiment(self.write())
        e.replicas[1]["comparable"] = False
        e.replicas[1]["cpu_time_us"]["delta"] = None
        self.assertEqual(summary_metrics(e)["network_mib"], 4)
        self.assertEqual(summary_metrics(e)["cpu_seconds"], 2)
        e.replicas = []
        self.assertTrue(math.isnan(summary_metrics(e)["network_mib"]))

    def test_missing_stream_is_not_zero_and_memory_only_works(self):
        source = self.write(config={"use_consistent_requests": True})
        e = load_experiment(source)
        self.assertTrue(e.notes)
        self.assertTrue(math.isnan(time_series(e, [1], window=1)["consistent_throughput"][0]))
        memory_only = self.write("memory-only", records=[{
            "type": "replica_log_memory_usage", "replica_id": 1,
            "samples": [[MIB, EPOCH // 1_000_000], [MIB * 2, (EPOCH + 1_000_000_000) // 1_000_000]],
        }])
        e = load_experiment(memory_only)
        series = time_series(e, [0.5])
        self.assertEqual(series["memory_mib"], [1.5])
        self.assertTrue(math.isnan(series["total_throughput"][0]))

    def test_malformed_files_and_duplicate_streams_are_reported(self):
        good = self.write()
        bad = self.write("bad")
        bad.write_text('{"latencies":\n')
        duplicate = self.write("duplicate")
        lines = duplicate.read_text().splitlines()
        duplicate.write_text("\n".join(lines + [lines[0]]))
        experiments, warnings = load_results(self.root)
        self.assertEqual([e.source for e in experiments], [good])
        self.assertEqual(len(warnings), 2)
        self.assertTrue(any('duplicate client stream' in w for w in warnings))

    def test_figures_bars_and_workload_separation(self):
        a = load_experiment(self.write())
        b = load_experiment(self.write("run-b", config={"frequency": 20}))
        figures = build_figures([a, b], style="bar", window=1, step=1)
        # Two summaries, plus three time plots per distinct workload.
        self.assertEqual(len(figures), 8)
        self.assertEqual([title for title, _ in figures[:2]], ["Network usage", "CPU usage"])
        self.assertEqual(len(figures[0][1].axes[0].patches), 2)
        self.assertEqual(figures[0][1].axes[0].get_ylabel(), "Total TX + RX across replicas (MiB)")
        self.assertEqual([bar.get_height() for bar in figures[0][1].axes[0].patches], [10, 10])
        self.assertGreater(figures[0][1].axes[0].get_ylim()[1], 5)
        self.assertGreater(figures[2][1].axes[0].get_ylim()[1], 100)
        for _, fig in figures:
            fig.clear()
        with self.assertRaises(ValueError):
            build_figures([a], step=0)
        with self.assertRaises(ValueError):
            build_figures([a], step=1e-9)

    def test_web_selection_and_embedded_export(self):
        self.write()
        self.write("run-b")
        html = render_page(self.root, {"run": ["run-a"], "step": ["1"]})
        self.assertIn("1 experiments · 1 runs · single run", html)
        self.assertIn("data:image/png;base64,", html)
        self.assertIn("Download PNG", html)
        self.assertEqual(html.count('<img '), 5)
        self.assertNotIn('Peak operation-log memory', html)
        self.assertNotIn('<h2>Client latency</h2>', html)
        averaged = render_page(self.root, {"step": ["1"]}, static=True)
        self.assertIn("2 experiments · 2 runs · equal-weight average", averaged)
        self.assertNotIn('<form', averaged)
        self.assertNotIn('<script', averaged)
        self.assertIn("window must be positive", render_page(self.root, {"window": ["0"]}))
        self.assertIn("No experiments match", render_page(self.root, {"clients": ["99"]}))


if __name__ == "__main__":
    unittest.main()
