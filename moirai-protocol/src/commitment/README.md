# Commitment

`CommitmentLog<L, A>` wraps a child `IsLog` with an
`AtomicBroadcastProtocol<L>`. The Omega implementation is
`commit_protocols::omega::omega_commit::OmegaCommit<O>`; its replicated update is
`SupportOp<L::Update>`.

## Building replicas

The example builders configure fixed membership, stable wire IDs, Omega, and
TCSB together:

```rust
use moirai_protocol::{
    commitment::examples::{
        tools::builders::commit_replicas,
        commit_set::{CommitSet, SetUpdate},
    },
    crdt::query::Contains,
    replica::IsReplica,
    state::po_log::VecLog,
};

let mut replicas = commit_replicas::<VecLog<CommitSet<u8>>>(3);
let message = replicas[0].send(SetUpdate::Add(7)).unwrap();
replicas[1].receive(message.clone());
replicas[2].receive(message);
assert!(replicas[1].query(&Contains(7)));
```

`commit_replicas::<L>(n)` uses default child logs, names replicas `replica-0`
through `replica-(n-1)`, and initially chooses stable member 0 as leader.
`CommitReplica::new(own_id, members, leader)` accepts explicit configuration;
`CommitReplica::from_parts(child, oracle, members, own_id)` accepts an existing
child log. `members` is a `Vec<(StableReplicaId, ReplicaId)>` with unique IDs and
the same mapping at every replica. The builder rejects a missing own ID or leader.
`into_inner()` returns the underlying `Replica`.

For direct log construction, use
`CommitmentLog::new(child, OmegaCommit::from_parts(oracle, members, own_id))`, then
`Replica::bootstrap_with_state` with the same membership. Default Omega state is
unconfigured and must be configured before preparing or handling events.

For Lamport commitment, use `LamportCommit::new(member_count)` or
`examples::tools::builders::lamport_commit_replicas::<L>(n)`. The count includes
every configured TCSB member, including this replica. Lamport commitment waits
for every member; Omega commitment can advance with a majority.

## Fixed membership and clock metadata

Commitment does not store or query a `Resolver`, update identity views, or remap
existing state. The full membership and each process's local index order must
remain fixed for the lifetime of a configured protocol. Deliver events through
TCSB so their clock indices already refer to that process's local member order.

`OmegaCommit::from_parts` builds its stable-to-local ID table once. It uses the
same ordering as `Replica::bootstrap_with_state`: the local member is first,
followed by the other members in the supplied order. Pass the same member order
to both constructors, or use the replica builders above. Different processes
can still have different local indices. Stable payload IDs and the existing
lexicographic name tie-break remain identical at every process; name ranks are
cached at construction instead of being resolved during checkpoint comparisons.
Membership and the local identity have no mutation setters.

Lamport tracking uses a fixed vector of latest timestamps and one FIFO timestamp
queue per member. Unseen members keep timestamp zero and block commitment.
EventOracle uses a fixed local-index-to-stable-ID vector and one latest activity
time per member. To configure its timeout, use
`EventOracle::with_timeout(fallback_id, std::time::Duration::from_millis(ms))`;
OmegaCommit configures the oracle's member table automatically, including when
replacing the oracle. Reconfiguring an EventOracle with a different index order
is rejected.

Versions and commitment frontiers retain their original, full fixed-membership
clock metadata for TCSB, child-log queries, and serialization. Commitment carries
that metadata through clock/frontier clones and conversions without using it
for membership decisions or identity translation. TCSB's implementation and
event payload formats are unchanged.

Resolver-related API changes:

- `AtomicBroadcastProtocol::ab_ready_to_commit()` takes no arguments; the
  `get_resolver` and `update_resolver` hooks are removed.
- `LamportCommit::new(member_count)` replaces its membership/tracker constructor
  and mutable membership accessors. `LamportTracker::new(member_count)` fixes its
  vector size; `get_stable_frontier()` returns `None` before the first event.
- `EventOracle::with_timeout` replaces the resolver/checkpoint constructor.
  `IsOracle::configure_members` receives stable IDs in fixed local index order.
- `VersionedVote::new` receives the origin's lexicographic name rank as its last
  argument. Its empty, unconfigured `Default` placeholder is removed; actual
  votes always carry a version and an explicit fixed origin rank.

## Protocol and child-log responsibilities

The child log prepares the operation; `ab_prepare` adds protocol metadata.
`child_op` extracts the child operation for enablement and delivery. After child
delivery, `ab_handler` updates vote support. The wrapper collects
`ab_ready_to_commit` versions before mutating the protocol. For each version it
stabilizes the child using the full checkpoint frontier, calls `ab_prune`, then
`ab_set_last_committed`.

The child must preserve uncommitted operations and apply each committed batch in
a deterministic causal order. The example `CommitSet` and the sequence fixture
in `examples/tests.rs` sort batches and optimistic tails with `Sorted<LwwPolicy>`.
Ordinary TCSB causal stability does not advance commitment: the wrapper's
`stabilize` method is inert.

Payload votes use `StableReplicaId(u16)` and predecessor references use
`StableEventId` (stable origin plus `u64` sequence). These are translated to local
`ReplicaIdx` values after delivery. Local index zero can name different replicas
at different processes. Membership is fixed; state transfer and reconfiguration
are outside these examples. Omega is an externally supplied leader choice, not
a failure detector implemented by the protocol. Progress requires a live majority,
eventual agreement on a correct leader, continuing events, and eventual delivery.

## Model checking and tests

The Python model checks the incremental protocol against an independent full-DAG
reference. It is bounded seeded exploration, not exhaustive verification, and
does not execute Rust. See [checks/README.md](checks/README.md) for commands,
replaying failures, options, and limitations; see
[CORRECTNESS.md](CORRECTNESS.md) for invariants and the pre-vertex predecessor rule.

From the repository root:

```sh
python3 moirai-protocol/src/commitment/checks/model_check.py --seeds 40 --steps 110
cargo test -p moirai-protocol --lib commitment::examples --all-features --offline
```

The Rust tests exercise actual local-ID translation, sequence commitment,
pruning, crash progress, set convergence, memory accounting, and serialization.
Omit `--offline` if dependencies have not been fetched. The standalone model uses
only Python's standard library (Python 3.10 or newer); it needs no Cargo feature.
