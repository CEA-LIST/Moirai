# Kubernetes replica benchmark launcher

Run these commands from the Moirai root. You need Python 3.10+, kubectl, a
reachable Kubernetes cluster, and access to build and distribute an image.
The launcher uses the current kubectl context. It needs permissions to create
and delete its temporary namespace, create, read and delete Pods, create
ConfigMaps and a Service, and use Pod logs and port forwarding.

```sh
docker build -f kubernetes-launcher/Dockerfile \
  --build-arg REPLICA_EXAMPLE=benchmark -t moirai-replica-benchmark:dev .
```
```sh
# For kind, make the image available to every worker before launching.
kind load docker-image moirai-replica-benchmark:dev
```
```sh
bash kubernetes-launcher/benchmark-replica/benchmark_launch.sh \
  kubernetes-launcher/benchmark-replica/experiment_sample.json
```

For a remote cluster, publish the image to a registry reachable by the
workers and put its tag or digest in `replica_image`.

The launcher creates a fresh namespace with a headless Service, one Pod
and one ConfigMap per replica. The replicas use stable Pod DNS names and
retry DNS during startup. Once every replica has connected to all peers,
the launcher sends start requests concurrently over loopback kubectl port
forwards. It polls status until all surviving replicas have processed
their local queue, then allows `grace_s` for replication and commitment
(10 seconds in the sample). It requests a final measurement, waits for the replica admin acknowledgments,
and validates the captured logs before requesting Pod deletion. A missing or
partial stream is recovered through bounded, sequential `kubectl logs`
requests. The collector joins overlapping output without duplicating samples.
A gap left by container log rotation causes recovery to fail explicitly. The Pod manifest
allows Kubernetes one second of termination grace after deletion. The
launcher submits namespace cleanup asynchronously. Start requests
can arrive at slightly different times. Every replica records its own
monotonic origin and a Unix start timestamp.

Set `fault_after_s` to an integer to crash replica 0 that many seconds
after start fan-out. Its replica process exits and the Pod is configured
with `restartPolicy: Never`. The launcher verifies the exit and waits
for the surviving replicas. It retains the crashed replica's records
through its last periodic sample. The configured commitment quorum or
membership still applies. A run can finish with pending updates, such as
after an timestamp-stability participant fails.

The experiment JSON controls the replica count, protocol, image, TCP
pull and idle timing, event-oracle timeout, workload composition and
rate, sample interval, measured network interface, start and drain
deadlines, grace period and optional crash time. Change the interface
if your cluster uses a Pod interface other than `eth0`. Interface
byte counters include background traffic on that interface, including
readiness probes and protocol traffic during the measured interval.
The headless Service publishes TCP port 19001 for causal replication and,
in Creek runs, TCP port 19002 for consensus. Creek derives the consensus
port by adding one to each replica's causal port. The admin server
has no authentication and is intended for disposable benchmark Pods on
a trusted cluster.

Pass `--output DIRECTORY` for a specific new result directory. The
default is under `kubernetes-benchmark/results`. Every result directory
contains the experiment snapshot, rendered manifest, run status, and
`replicas/<id>.log` files. The console and `run.json` report separate
durations for commitment grace, seal acknowledgments, final log delivery,
optional crash verification, deletion requests and tunnel cleanup. Final
log delivery includes recovery and validation. The namespace name in `run.json` identifies the resources
for checking asynchronous cleanup. The launcher follows logs during the run
to retain measurements across
container log rotations. Raw streams and stderr are saved as
`replicas/<id>.stream.log` and `replicas/<id>.stderr.log`. Recovered snapshots
are retained as `replicas/<id>.snapshot.log`. Only validated output becomes
`replicas/<id>.log`. The per-replica recovery result and follower exit code
are in `log_collection.json`.

On failure the launcher returns nonzero and retains the namespace and Pods
for diagnosis or manual log retrieval. The printed namespace can be removed
with `kubectl delete namespace NAME` after recovery. Incomplete runs are
skipped by the viewers. See [the replica benchmark](../../moirai-network-node/examples/benchmark/README.md)
for the sample schema and [the experiment suite](../../kubernetes-benchmark/README.md)
for generation and plotting.

| File | Purpose |
|---|---|
| `benchmark_launch.sh` | Shell entry point for one experiment. |
| `launch_benchmark.py` | Coordinates Kubernetes startup, control calls, crash injection, log collection and cleanup. |
| `benchmark_config.py` | Validates settings and renders Pod, Service, ConfigMap and replica JSON configuration. |
| `replica_logs.py` | Captures stream diagnostics, recovers snapshots with overlap checks, and validates records before deletion. |
| `test_replica_logs.py` | Regression checks for interrupted streams, rotated snapshots, and incomplete measurements. |
| `experiment_sample.json` | A four-replica example with a mixed local workload and ten seconds of drain grace. |

Run the log-recovery checks from the project root with
`python3 -m unittest discover -s kubernetes-launcher/benchmark-replica -p 'test_replica_logs.py'`.
