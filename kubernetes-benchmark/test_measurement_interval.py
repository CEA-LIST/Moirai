"""Regression checks for selecting steady commitment periods."""

from copy import deepcopy
import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch

from benchmark_results import NS, Run, achieved_latencies_ms, achieved_throughput, summary
from benchmark_plots import PlotOptions, comparison_plots
from benchmark_viewer import page


def sample(t, count, delays, *, drained=False, final=False):
    return dict(elapsed_ns=int(t * NS), committed_updates=count,
                committed_local_updates=count, processed_updates=30,
                resources=dict(rx_bytes=1024, tx_bytes=2048, cpu_time_us=1000000),
                commit_latency_samples=[[int(stamp * NS), int(ms * 1000000), i]
                                        for i, (stamp, ms) in enumerate(delays)],
                drained=drained, final=final, log_memory_bytes=1024)


def run():
    config = dict(protocol='commit', replica_image='test', n_replicas=1,
                  workload=dict(rate_per_replica=10, duration_s=5, query_percent=0,
                                add_percent=50, key_space=10, queue_capacity=100),
                  replica_settings=dict(pull_interval_ms=100, pull_peer_count=1,
                                        noop_interval_ms=100, event_timeout_ms=1000),
                  sample_interval_ms=1000, network_interface='eth0', grace_s=1, fault_after_s=None)
    rows = [sample(1, 2, [(1, 100)]), sample(3, 22, [(2, 10), (3, 20)]),
            sample(5, 24, [(5, 900)], drained=True), sample(6, 30, [(6, 500)], final=True)]
    return Run('example', config, {0: rows}, {0: dict(queue_full=0, missed_slots=0)},
               version=3, uncommitted={0: (6 * NS, [(4 * NS, 1)])})


class IntervalTests(unittest.TestCase):
    def test_default_preserves_drain_rate_and_seal_latencies(self):
        r = run()
        self.assertEqual(achieved_throughput(r), 24 / 5)
        self.assertEqual(achieved_latencies_ms(r), [100, 10, 20, 900, 500])

    def test_boundaries_and_interpolation(self):
        r = run()
        self.assertEqual(achieved_throughput(r, 1, 3), 10)
        self.assertEqual(achieved_latencies_ms(r, False, 1, 3), [10, 20])
        self.assertEqual(achieved_throughput(r, 2, 4), 5.5)
        self.assertEqual(achieved_latencies_ms(r, False, 2, 4), [20])
        self.assertEqual(achieved_throughput(r, 0, 0.5), 2)
        self.assertEqual(achieved_throughput(r, 3, None), 1)
        self.assertEqual(achieved_throughput(r, None, 3), 22 / 3)
        self.assertEqual(achieved_throughput(r, 5, 6), 6)
        r.samples[1] = deepcopy(r.samples[0])
        self.assertEqual(achieved_throughput(r, 1, 3), 10)  # Never sum observers.

    def test_invalid_and_unobserved_intervals_have_no_points(self):
        r = run()
        for start, end in [(3, 3), (4, 2), (-1, 3), (0, 7), (7, None), (math.nan, 3)]:
            self.assertTrue(math.isnan(achieved_throughput(r, start, end)))
            self.assertEqual(achieved_latencies_ms(r, False, start, end), [])
        figures = comparison_plots([r], 'total', True, PlotOptions(measurement_start=7))
        self.assertFalse(figures[2].axes[0].lines)
        self.assertFalse(figures[3].axes[0].lines)

    def test_pending_at_seal_only_in_interval_containing_seal(self):
        r = run()
        self.assertEqual(achieved_latencies_ms(r, True, 1, 3), [10, 20])
        self.assertEqual(achieved_latencies_ms(r, True, 5, 6), [500, 2000])

    def test_both_latency_graphs_and_exports_use_interval(self):
        r = run()
        options = PlotOptions(measurement_start=1, measurement_end=3)
        figures = comparison_plots([r], 'total', True, options)
        for fig in figures[2:]:
            for series in fig.benchmark_data['series']:
                self.assertEqual(series['points'][0]['x'], 10)
            self.assertEqual(fig.benchmark_data['measurement_interval']['end_s'], 3)
            json.dumps(fig.benchmark_data, allow_nan=False)
        self.assertEqual(figures[2].axes[0].lines[0].get_ydata()[0], 15)
        self.assertEqual(figures[3].axes[0].lines[0].get_ydata()[0], 15)
        self.assertEqual(summary(r)['network'], summary(r, 1, 3)['network'])
        self.assertEqual(summary(r)['cpu'], summary(r, 1, 3)['cpu'])

    @patch('benchmark_viewer.figure_html', return_value='<graph>')
    def test_controls_inventory_and_invalid_selection(self, _):
        with patch('benchmark_viewer.load_runs', return_value=([run()], [])):
            html = page(Path('.'), {'measurement_start': ['1'], 'measurement_end': ['3']}, True)
            self.assertIn('name="measurement_start"', html)
            self.assertIn('<td>10.00</td>', html)
            self.assertEqual(html.count('<graph>'), 8)
            invalid = page(Path('.'), {'measurement_start': ['4'], 'measurement_end': ['2']}, True)
            self.assertIn('Unavailable', invalid)
            self.assertIn('require start &lt; end', invalid)
            invalid = page(Path('.'), {'measurement_start': ['nan']}, True)
            self.assertIn('Unavailable', invalid)
            other = page(Path('.'), {}, False)
            self.assertNotIn('name="measurement_start"', other)


if __name__ == '__main__':
    unittest.main()
