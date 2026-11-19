#!/usr/bin/env python3
"""Group replica log-memory events into one compact JSONL record per replica."""

import json
import sys


replicas = {}
for line in sys.stdin:
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if not isinstance(record, dict) or record.get("type") != "log_memory_usage":
        continue

    replica_id = record.get("replica_id")
    replica_name = record.get("replica_name")
    memory_bytes = record.get("log_memory_bytes")
    timestamp = record.get("timestamp_unix_ms")
    if not isinstance(replica_id, int) or isinstance(replica_id, bool):
        continue
    if not isinstance(replica_name, str):
        continue
    if not isinstance(memory_bytes, int) or isinstance(memory_bytes, bool):
        continue
    if not isinstance(timestamp, int) or isinstance(timestamp, bool):
        continue

    replica = replicas.setdefault(replica_id, {
        "type": "replica_log_memory_usage",
        "replica_id": replica_id,
        "replica_name": replica_name,
        "samples": [],
    })
    replica["samples"].append([memory_bytes, timestamp])

for replica_id in sorted(replicas):
    replica = replicas[replica_id]
    replica["samples"].sort(key=lambda sample: sample[1])
    print(json.dumps(replica, separators=(",", ":")))
