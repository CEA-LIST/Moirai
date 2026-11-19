# Moirai Kubernetes launcher

This folder runs a Moirai replica system and a batch of clients on Kubernetes.
The replicas communicate over TCP. Client pods send requests through one
cluster-internal HTTP Service, so they are independent of individual replicas.
The launcher waits for replicas to become ready, runs the client Job, collects
its measurements, and removes the experiment resources.

## Files

| File or folder | Purpose |
|---|---|
| `Dockerfile` | Builds the Moirai replica image from the complete Rust workspace. |
| `multireplica/` | Kustomize resources for the StatefulSet, Services, and per-pod configuration. It can run one or several replicas. |
| `clients/Dockerfile` | Builds the Python client image. |
| `clients/job.yaml` | Configures the indexed Job and client workload. |
| `clients/launcher_client.py` | Sends randomized Add/Remove operations and prints measurement records. |
| `experiment_sample.json` | Example JSON configuration for one experiment. Copy and edit this for each run. |
| `render-experiment.py` | Validates the JSON configuration and renders temporary Kustomize overlays for the run. |
| `measure-replicas.py` | Reads each replica pod's network-interface counters and cgroup CPU usage before and after the client Job. |
| `collect-replica-memory.py` | Groups operation-log memory records by replica for the combined results file. |
| `launch.sh` | Reads an experiment JSON file, starts replicas, waits for readiness, runs clients, collects results, and cleans up. |
| `collect-results.sh` | Combines client results, replica resource measurements, and operation-log memory records into one JSON Lines file. |
| `submit-request.sh` | Sends one manual request to a port-forwarded replica. |

## Prerequisites

Use the root of the complete Moirai workspace for the commands below. It must
contain the workspace `Cargo.toml` and the sibling `moirai-protocol` and
`moirai-crdt` crates required by the replica crate. These examples use Docker
and the existing kind cluster. The account running `launch.sh` also needs
permission to use `kubectl exec` on replica pods. CPU sampling reads cgroup v2
`cpu.stat` from inside each replica container.

## Build and load the images

Build the replica image from the Moirai workspace root, then load it into kind.
The Dockerfile defaults to `vanilla_set` (Lamport commitment). Set
`--build-arg REPLICA_EXAMPLE=commit_set` for majority commitment using the core
`EventOracle`. Both examples use TCP and the current `moirai-protocol` API.
The node crate builds with its own manifest and lockfile; it does not need to be
added to the parent Rust workspace.

```sh
docker build \
  -f kubernetes-launcher/Dockerfile \
  -t moirai-replica:dev \
  .
kind load docker-image moirai-replica:dev
```

Build and load the client image:

```sh
docker build \
  -f kubernetes-launcher/clients/Dockerfile \
  -t moirai-client:dev \
  kubernetes-launcher/clients
kind load docker-image moirai-client:dev
```

For a remote cluster, push both images to a registry its nodes can access and
set those image references in the experiment JSON. The replica configuration
init containers also use `python:3.12-slim`, which the cluster must be able to
pull unless that image is available locally.

## Configure the experiment

Copy `experiment_sample.json` to a new JSON file, then set the values for the
run. The sample config starts four replicas and four clients.

| Field | Meaning |
|---|---|
| `simulation_name` | Lowercase Kubernetes-safe run name used in the result filename. |
| `n_replicas` | Number of replica pods. Set to `1` for a single-replica run. |
| `n_clients` | Number of indexed client processes in the Job. |
| `n_operations` | Operations requested per client stream. |
| `frequency` | Client pacing parameter; it controls randomized inter-request delays and is not a precise requests-per-second rate. |
| `with_fault` | Whether to automatically fail the first replica during the client run. |
| `failure_time` | Integer number of seconds after client Job creation at which to inject the failure. Used only when `with_fault` is `true`. |
| `use_optimistic_requests` | Whether each client runs the optimistic request stream. |
| `use_consistent_requests` | Whether each client runs the wait-for-commit stream. At least one request mode must be enabled. |
| `replica_image` | Image reference for the replica container. |
| `client_image` | Image reference for the client container. |
| `output_path` | Directory for the combined results file. Relative paths are resolved from the JSON file's directory. |

The launcher validates the file before creating Kubernetes resources. It
generates temporary Kustomize overlays that set the StatefulSet replica count
and `REPLICA_COUNT` together, and configure the client Job's parallelism,
operation count, pacing, request modes, and images. The source manifests remain
unchanged. The results file is named
`<simulation_name>-measurements.jsonl` inside `output_path`.

When both request modes are enabled, optimistic and wait-for-commit streams run
together, and the committed stream stops when the optimistic one finishes.

Each replica receives a unique numeric ID from its StatefulSet ordinal and a
peer list containing the other replicas. The headless Service gives the pods
stable DNS names. An init container waits for peer DNS records because the
replica transport resolves peer addresses when it starts. Port 19001 is
declared as TCP in the Service, StatefulSet, and replica image. The replica
binary itself must also use TCP; Kubernetes port declarations do not change
the transport used by the application.

`multireplica/replica-template.json` configures periodic pulls and idle no-ops.
`noop_interval_ms` defaults to `1000`: after no client update for that duration,
each replica emits a replicated no-op, repeating while idle. Set it to `null` to
disable. `event_timeout_ms` defaults to `10000` and configures the core oracle in
`commit_set`; choose a timeout comfortably longer than the no-op interval and
expected communication delays. The renderer also forwards optional
`request_timeout_ms`, `memory_usage_enabled`, and `memory_usage_interval_ms`.
It also forwards `http_optimistic_limit` and `http_consistent_limit` (positive
integers, each defaulting to 64). These independently bound concurrent HTTP
workers: stalled consistent requests do not consume optimistic/read slots.
Increasing the limits increases OS thread and connection requirements; it does
not increase the replica loop's processing capacity.
The strong-update timeout defaults to 25 seconds, below the clients' 30-second
curl timeout. A failed commitment wait returns HTTP 504 so the existing clients
record it as a failure; the applied update remains in the set.

The examples implement the same sequential set operations (`Add`, `Remove`,
`Clear`), with Lamport ordering for concurrent operations. `vanilla_set` waits
for every configured member to advance commitment; `commit_set` can advance with
a surviving majority. State reads include tentative updates and are local
reads, not linearizable read barriers. Restarting replicas still requires
history that may already have been pruned; neither example provides state
transfer or persistent recovery. See the [node README](../moirai-network-node/README.md)
for the runtime API and complete configuration.

## Run the experiment

From the Moirai workspace root, pass the experiment JSON file to the launcher:

```sh
./kubernetes-launcher/launch.sh ./kubernetes-launcher/experiment_sample.json
```

The script refuses to start if a client Job or multi-replica resources from a
previous run still exist. It applies the replica resources, waits for the
StatefulSet rollout, then creates the client Job and monitors it for up to six
minutes. It collects results and removes the Job, StatefulSet, Services,
generated ConfigMap, and temporary overlays on exit. If the Job times out, it
attempts to collect logs from any clients that succeeded before cleanup.

When `with_fault` is `true`, `launch.sh` triggers a failure `failure_time`
seconds after creating the client Job, provided the Job is still running. It
takes a resource snapshot and archives the first replica's logs, then changes
the StatefulSet ordinal range to start at `1` and reduces its replica count by
one. This removes
`moirai-multi-replica-0` from the desired range, so the StatefulSet does not
recreate it during the experiment. Replicas with larger ordinals remain in
place. This uses the StatefulSet start ordinal field, which is stable in
Kubernetes v1.31 and later. If the Job finishes before `failure_time`, no fault
is injected. For a one-replica experiment, the StatefulSet is reduced to zero
replicas after the fault.

The client Job is indexed: each completion runs one client process with a
distinct client ID. Check progress while the script is running from another
terminal:

```sh
kubectl get job moirai-client-load
kubectl get pods -l batch.kubernetes.io/job-name=moirai-client-load
```

To allow longer client workloads, increase `job_timeout_seconds` in
`launch.sh`.

## Results

The launcher writes one JSON Lines file at
`<output_path>/<simulation_name>-measurements.jsonl`. Client lines contain a
client ID, request mode, latency timestamps in nanoseconds, and any
failed-request details. A separate `replica_resource_usage` line contains the
per-replica resource deltas. To collect client results manually while the
completed Job pods still exist, run:

```sh
./kubernetes-launcher/collect-results.sh
```

You can pass a different output file as the collector's first argument. The
collector reads stored pod logs, so it works after client containers have
exited. Collect before deleting the Job, because deleting it also removes its
pods and logs.

The launcher also takes a per-replica snapshot after the StatefulSet is ready
and immediately before creating the client Job, then takes another snapshot
after the Job finishes and before cleanup. The JSON Lines file includes a
`replica_resource_usage` record with each replica's `rx_bytes`, `tx_bytes`, and
CPU usage at both snapshots and their differences. Network counters are read
from the pod's `eth0`, so they count all traffic on that interface, including
replica-to-replica traffic; they are not limited to traffic to the public
Internet. CPU usage comes from cgroup v2 `cpu.stat` (`usage_usec`) and is
reported as `cpu_time_us`, cumulative CPU time in microseconds, not hardware
CPU cycles. If Kubernetes restarts or replaces a replica between snapshots,
the measurements are retained but the delta is `null` because the counters no
longer cover one continuous container lifetime. For the automatically failed
replica, the launcher uses its pre-failure snapshot as the end sample, so its
deltas cover the interval from the initial snapshot until failure. The record's
`end_sample` is `pre_failure` for that replica; the timestamp is recorded in
`failure_sampled_at_utc`.

Replica stdout is also collected before cleanup. The collector groups JSON
records whose `type` is `log_memory_usage` by replica and writes one
`replica_log_memory_usage` line per replica. Each line contains a `samples`
list of `[log_memory_bytes, timestamp_unix_ms]` pairs, for example:

```json
{"type":"replica_log_memory_usage","replica_id":1,"replica_name":"a","samples":[[225,1793971065721],[225,1793971066722]]}
```

The recorded request interval is client-observed wall time around a `curl`
process; it includes process startup, connection setup, Service routing, and
the HTTP response. Kubernetes Services route connections to ready replicas
but do not guarantee equal distribution. The client starts a separate `curl`
process per operation, so operations normally use separate connections.

## Manual requests and crash experiments

For a manual operation, port-forward directly to one replica so the request
and state query reach the same process:

```sh
kubectl port-forward pod/moirai-multi-replica-0 8081:8081
```

In another terminal:

```sh
./kubernetes-launcher/submit-request.sh \
  http://127.0.0.1:8081 \
  '{"Add":"apple"}'
```

The load clients are separate Job pods. To manually simulate a temporary
replica restart while the client Job is running, delete a replica pod from
another terminal:

```sh
kubectl delete pod moirai-multi-replica-1
```

The StatefulSet recreates that replica. For a non-recovering automatic failure,
use `with_fault` instead; the launcher excludes ordinal 0 from the desired
StatefulSet range. Clients continue using the HTTP Service, and requests during
the failure transition may fail and are recorded. The replica state is in
memory, so restarting a pod loses that pod's local state.

If you stop the launcher before it cleans up, collect results first if needed,
then remove any remaining resources:

```sh
kubectl delete job moirai-client-load --ignore-not-found
kubectl delete -k kubernetes-launcher/multireplica --ignore-not-found
```
