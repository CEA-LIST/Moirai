"""Measured-throughput comparisons and standalone interface checks."""

import math
import unittest
from matplotlib.colors import to_hex

import test_results as fixtures
from results import load_experiment
from throughput_viewer import build_figures, configured_throughput, measured_throughput, render_page


class ThroughputTest(unittest.TestCase):
    setUp = fixtures.ResultsTest.setUp
    write = fixtures.ResultsTest.write

    def test_throughput_uses_completions_and_observed_duration(self):
        e = load_experiment(self.write())
        self.assertAlmostEqual(measured_throughput(e), 3 / 2.3)
        e.attempts['optimistic'].append(tuple(fixtures.pair(3, 4)))
        self.assertEqual(measured_throughput(e), 3 / 4)
        e.config['use_consistent_requests'] = True
        self.assertTrue(math.isnan(measured_throughput(e)))
        e.config['use_consistent_requests'] = False
        e.attempts['optimistic'] = [tuple(fixtures.pair(0, 0))]
        self.assertTrue(math.isnan(measured_throughput(e)))

    def test_fixed_replicas_average_and_styling(self):
        a = load_experiment(self.write())
        b = load_experiment(self.write('run-b', latencies=[fixtures.pair(0, 1)]))
        other = load_experiment(self.write('other', config={'n_replicas': 4}))
        creek = load_experiment(self.write('creek', config={'replica_image': 'creek:dev'}))
        figures = build_figures([a, b, other, creek], replicas=2,
                                image_colors={'commit:dev': '#123456', 'creek:dev': '#abcdef'},
                                image_labels={'commit:dev': 'Commit', 'creek:dev': 'Creek'},
                                image_order=['creek:dev', 'commit:dev'])
        self.assertEqual(len(figures), 2)
        ax = figures[0][1].axes[0]
        self.assertEqual([line.get_label() for line in ax.lines], ['Creek', 'Commit'])
        self.assertEqual(to_hex(ax.lines[1].get_color()), '#123456')
        self.assertAlmostEqual(ax.lines[1].get_xdata()[0], 10)
        self.assertEqual(ax.lines[1].get_ydata()[0], 10)
        self.assertEqual(figures[1][1].axes[0].lines[1].get_ydata()[0], 3)
        bars = build_figures([a, creek], replicas=2, style='bar', image_order=['creek:dev', 'commit:dev'],
                             titles={figures[0][0]: 'Custom network'})
        ax = bars[0][1].axes[0]
        self.assertEqual(ax.get_title(), 'Custom network')
        self.assertLess(ax.patches[0].get_x(), ax.patches[1].get_x())
        self.assertEqual(build_figures([a], replicas=8), [])
        for _, fig in figures + bars:
            fig.clear()

    def test_all_configured_loads_share_two_graphs(self):
        a = load_experiment(self.write())
        b = load_experiment(self.write('higher-load', config={'frequency': 20, 'n_clients': 2, 'n_operations': 40}))
        c = load_experiment(self.write('fault', config={'with_fault': True}))
        figures = build_figures([a, b, c], replicas=2)
        self.assertEqual(len(figures), 2)
        self.assertEqual(len(figures[0][1].axes[0].lines[0].get_xdata()), 2)
        self.assertEqual(list(figures[0][1].axes[0].lines[0].get_xdata()), [10, 40])
        self.assertEqual(configured_throughput(b), 40)
        b.config['use_consistent_requests'] = True
        self.assertEqual(configured_throughput(b), 80)
        a.recorded_modes.clear()
        resource_only = build_figures([a], replicas=2)
        self.assertEqual(list(resource_only[0][1].axes[0].lines[0].get_xdata()), [10])
        for _, fig in figures + resource_only:
            fig.clear()

    def test_interface_has_only_resource_plots_and_no_time_options(self):
        self.write()
        html = render_page(self.root, {'label:commit:dev': ['<Commit>'], 'step': ['invalid']})
        self.assertEqual(html.count('<img '), 2)
        self.assertIn('Fixed replica count', html)
        self.assertIn('value="&lt;Commit&gt;"', html)
        self.assertIn('name="order:commit:dev"', html)
        self.assertIn('name="color:commit:dev"', html)
        self.assertIn('name="title:', html)
        for name in ('window', 'step', 'start', 'end', 'max_latency'):
            self.assertNotIn(f'name="{name}"', html)
        self.assertNotIn('over time', html)
        self.assertNotIn('<form', render_page(self.root, static=True))
        self.assertIn('No experiments match', render_page(self.root, {'bundle': ['missing']}))


if __name__ == '__main__':
    unittest.main()
