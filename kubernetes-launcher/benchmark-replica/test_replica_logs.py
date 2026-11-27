"""Regression checks for overlap recovery and missing measurements."""

import tempfile
from pathlib import Path
import unittest

from replica_logs import join_records, records, validate


def complete_log():
    return [
        {"type": "benchmark_start", "benchmark_version": 3, "replica_id": 0},
        {"type": "benchmark_sample", "replica_id": 0, "elapsed_ns": 100,
         "committed_local_updates": 1, "commit_latency_samples": [[100, 50, 1]],
         "processed_updates": 2, "processed_queries": 0, "drained": True, "final": False},
        {"type": "benchmark_workload_done", "replica_id": 0,
         "scheduled": 2, "enqueued": 2, "queue_full": 0, "missed_slots": 0},
        {"type": "benchmark_sample", "replica_id": 0, "elapsed_ns": 200,
         "committed_local_updates": 1, "commit_latency_samples": [],
         "processed_updates": 2, "processed_queries": 0, "drained": False, "final": True},
        {"type": "benchmark_uncommitted", "replica_id": 0,
         "seal_elapsed_ns": 200, "updates": [[90, 2]]},
        {"type": "benchmark_sealed", "replica_id": 0},
    ]


class RecoveryTests(unittest.TestCase):
    def test_empty_and_interrupted_followers_recover_from_full_snapshot(self):
        events = complete_log()
        for live in ([], events[:2], events[:4]):
            recovered = join_records(live, events)
            self.assertEqual(recovered, events)
            validate(recovered, 0, False)

    def test_rotated_snapshot_joins_without_duplicate_samples(self):
        events = complete_log()
        recovered = join_records(events[:4], events[2:])
        self.assertEqual(recovered, events)
        validate(recovered, 0, False)

    def test_disjoint_rotated_logs_are_rejected(self):
        events = complete_log()
        with self.assertRaisesRegex(ValueError, "no overlap"):
            join_records(events[:2], events[3:])

    def test_seal_marker_alone_does_not_hide_missing_latency(self):
        events = complete_log()
        del events[1]["commit_latency_samples"][:]
        with self.assertRaisesRegex(ValueError, "missing commit latency"):
            validate(events, 0, False)

    def test_pending_measurements_are_required(self):
        events = complete_log()
        del events[-2]
        with self.assertRaisesRegex(ValueError, "missing uncommitted"):
            validate(events, 0, False)

    def test_truncated_live_tail_is_discarded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.log"
            path.write_text('{"type":"benchmark_start"}\n{"type":"benchmark_sa', encoding="utf-8")
            self.assertEqual(records(path, allow_partial_tail=True), [{"type": "benchmark_start"}])
            with self.assertRaises(ValueError):
                records(path)

    def test_crashed_replica_accepts_observed_prefix_without_seal(self):
        validate(complete_log()[:2], 0, True)


if __name__ == "__main__":
    unittest.main()
