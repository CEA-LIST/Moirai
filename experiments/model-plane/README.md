# Model-plane measurements

M-E1 to M-E7 of the model plane's validation plan, one directory each, in the
shape the other harnesses under `experiments/` use: a `run.sh`, a `manifest.txt`
the run writes, and the raw numbers beside them. The thresholds were fixed in
the plan before implementation; each harness prints its verdict and exits
non-zero when one is crossed, after writing its CSV.

| dir | what | how |
| --- | --- | --- |
| `m-e1` | `log_id` bytes per frame, and the median `Event` frame of the `r1` seeded workload | a `#[test]` in `moirai-protocol`, then three containers, the dashboard's seeded driver and a probe reading frames off the wire |
| `m-e2` | dispatch cost per frame (hit and miss), deep size per hosted log, one member table, idle RSS | `examples/model_plane_cost.rs` in process at N in {1, 4, 16, 64}; the rig at the same N for RSS |
| `m-e3` | no head-of-line blocking: model A's convergence while model B is under load, 16 models | five containers, the load from the host at the rig's rate |
| `m-e4` | per-log state transfer bytes and time, fresh joiner, donor at N in {1, 16} | a donor container, a fresh joiner container per run, and a probe |
| `m-e5` | apply-to-file latency and store size against model size | `arachne/clients/model-editor/bench` |
| `m-e6` | the mutation run | `cargo-mutants` scoped by `.cargo/mutants.toml`, plus hand-written mutants applied and reverted by script |
| `m-e7` | per-operation cost of the structural conformance check, and the receive path with the check on against off | `examples/conformance_cost.rs` of the Arachne-generated crate in process at N in {1, 4, 16, 64}, both descriptors, 100,000 seeded operations |

`common.sh` holds what they share: the manifest fields, the container helpers
and the checks that a run starts from nothing and leaves nothing. `wire.py`
is the replication protocol from the host, the probe pattern of
`experiments/p2-transfer-size/measure.py`.
