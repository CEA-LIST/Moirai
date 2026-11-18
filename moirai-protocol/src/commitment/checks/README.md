# Running the commitment model checks

Run commands below from the repository root with Python 3.10 or newer. No Python
packages need installing. Assertions implement the checks, so do not use `-O` or
`PYTHONOPTIMIZE`; the script rejects optimized execution.

Quick check:

```sh
python3 moirai-protocol/src/commitment/checks/model_check.py --seeds 2 --steps 20
```

Standard validation, with a machine-readable report:

```sh
python3 moirai-protocol/src/commitment/checks/model_check.py \
  --seeds 40 --steps 110 \
  --output moirai-protocol/src/commitment/checks/validation_results.json
```

A successful run prints `PASS` summaries and exits with status zero. An assertion
failure exits nonzero with a traceback; randomized failures include the membership
size, seed, and step bound. The report is written only after all checks pass.
An existing report is not removed on failure, so always check the process status.

## Options and replay

| Option | Default | Meaning |
| --- | --- | --- |
| `--seeds` | `40` | Positive number of seeded executions per membership size. |
| `--steps` | `90` | Nonnegative random-action bound before the crash/progress phase. |
| `--seed-start` | `0` | First seed in the contiguous range of executions. |
| `--replicas` | `3 5` | One or more positive fixed-membership sizes, up to 65,536 stable IDs. Small sizes are practical; exploration grows quickly. |
| `--output` | none | JSON report path; its parent directory must exist. |

For example, replay seed 17 with five replicas and 110 random actions:

```sh
python3 moirai-protocol/src/commitment/checks/model_check.py \
  --replicas 5 --seed-start 17 --seeds 1 --steps 110
```

Every run also performs three deterministic regressions: a delayed concurrent
candidate after predecessor metadata was pruned; sender predecessor selection
before the outgoing vertex; and protocol/child-log lifecycle behavior, including
inert external stabilization and retaining a checkpoint through prune-then-set.

## What is modeled

The implementation model follows the current Rust ownership and call boundaries:

- `VoteLog` stores votes, support summaries, immutable checkpoint predecessors,
  and the last committed checkpoint. Insertion does not itself commit operations.
- `OmegaCommit.ab_prepare` wraps a prepared child operation in `SupportOp`.
  `ab_handler` inserts delivered vote metadata; `ab_ready_to_commit`, `ab_prune`,
  and `ab_set_last_committed` expose protocol steps separately.
- `CommitmentLog.prepare` delegates to the child and then the protocol.
  `effect` delivers to both, snapshots ready checkpoints, stabilizes the child,
  prunes protocol metadata, and records each checkpoint, in that order.
- `SequenceLog` models the sequence example's deterministic sorted batches and
  optimistic tail. Application commitment uses the full version; metadata pruning
  uses the separate saturating retention boundary.

Each randomized execution compares pruned and unpruned implementations with a
separately calculated full-DAG reference after every delivery. Checks include
prepared predecessor metadata, committed sequence equality, optimistic order,
pairwise prefix agreement, and causal ordering. Oracle outputs vary during the
random phase. Finally, a minority stops issuing events; a live majority adopts
leader 0, exchanges events, and must commit every event issued before that phase.
For one or two replicas there is no minority crash to simulate.

The JSON report records bounds, membership sizes, execution counts, deliveries,
checkpoint applications, suppressed stale candidates, and targeted check names.
It reports Python-model results only; `executes_rust` is false.

## Rust validation and limits

Run actual implementation regressions separately:

```sh
cargo test -p moirai-protocol --lib commitment::examples --all-features --offline
cargo test -p moirai-protocol --lib --all-features --offline
```

Use the repository's Rust toolchain. Omit `--offline` when fetching dependencies
is necessary. Default-feature tests also run with:

```sh
cargo test -p moirai-protocol --lib commitment::examples --offline
```

The model uses canonical integer stable IDs and vector clocks. It abstracts away
local `Resolver` index permutations, Serde, memory layout, TCSB implementation,
CRDT-specific enablement, and the Rust borrow/type system. Rust regressions cover
local-index translation and payload serialization. The model exercises the
implemented protocol methods used for commitment, not the currently unimplemented
`ab_last_committed` accessor.

The full-DAG reference follows the intentional pre-vertex predecessor variant in
[CORRECTNESS.md](../CORRECTNESS.md), not the paper's unmodified recursive rule.
This is bounded randomized model testing with targeted regressions, not an
exhaustive state-space search or a machine-checked proof. Passing it cannot prove
that arbitrary Rust child logs use a compatible ordering/stabilization policy.
