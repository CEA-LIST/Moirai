# Creek-inspired external commitment

This implements `AtomicBroadcastProtocol<L>` as `Creek<O>`, using Paxos
to order IDs of operations already causally delivered by TCSB.
It is an adaptation of the commitment ideas in Wojciechowski, Kokociński and
Kobus, *Creek: A single-order mixed-consistency replication scheme*, Theoretical
Computer Science 1075 (2026), 115927,
[doi:10.1016/j.tcs.2026.115927](https://doi.org/10.1016/j.tcs.2026.115927).
The supplied paper is the reference, particularly sections 4, 5 and 7.

## Structure and relation to the paper

| Code | Responsibility | Creek correspondence |
| --- | --- | --- |
| `Creek::ab_handler` | Observe causally delivered events; retain their versions and maintain maximal uncommitted heads | The received/unordered sets in Algorithm 3; dependency tracking in Algorithm 1 |
| `Creek::candidate` | Select the oldest head by causal timestamp, with an ID tie-break | Choosing input for indirect consensus (Algorithm 3, lines 15–17) |
| `Paxos::tick` | The Omega-selected leader drives one consensus slot at a time | Paxos realization of the paper's indirect-consensus abstraction |
| `Prepare` / `Promise` | Establish a ballot and recover the highest accepted value from a majority | Paxos phase 1; preserves a potentially chosen value across leader changes |
| `Accept` / `Accepted` | Accept an ID only after local causal delivery; decide after distinct majority votes | Paxos phase 2 plus the availability/dependency test of Algorithm 3, lines 9–10 |
| `Decide`, `Sync`, `Decisions` | Disseminate the chosen sequence and repair missing decisions | Reliable delivery of the agreed ID sequence |
| `Creek::poll` | Drain chosen IDs, wait for local causal delivery, and construct cumulative cuts | Conditional ordered delivery, Algorithm 3, lines 22–24 |
| `CommitmentLog::commit_ready` | Stabilize the chosen vertex and its previously uncommitted causal past | Algorithm 1's `commit`, lines 53–56 |
| `worker::run` / `drive` | Run Paxos independently; synchronize and stabilize checkpoints on the replica owner | Runtime integration, including when the causal log is idle |

The paper disseminates complete operations with reliable broadcast and uses CAB
to order identifiers subject to a dependency predicate. Its Algorithm 3 orders
sets of identifiers using an abstract indirect-consensus primitive; it is not
itself detailed Paxos pseudocode. Here the existing TCSB plus periodic pulls
provides causal delivery and dissemination, making the predicate simply
“TCSB has delivered this root vertex.” Causal delivery implies delivery of its
whole causal past. A replica cannot cast an acceptance vote before that predicate
holds, and cannot apply a learned decision before it holds locally.

## Deliberate differences

- Every operation is eligible, including weak updates and no-ops. The leader does
  not wait for a strong client request before proposing a checkpoint.
- Each consensus instance chooses one maximal uncommitted vertex ID, rather than
  a set of strong-operation IDs. A maximal head covers its causal ancestors.
  Older heads are preferred to prevent a continually growing branch from
  starving another branch.
- This is basic, sequential Paxos: each slot runs both phases. It does not use
  Multi-Paxos phase-1 amortization, pipelining, batching of proposals, or a separate
  leader-election protocol. Membership and numeric identities are fixed.
- There is no multiversioning, rollback machinery, session context, or new
  consistency API. The existing child log supplies deterministic execution and
  stabilization. The example uses the core `CommitSet<String>`.
- Paxos choices may name concurrent vertices. Each delivered checkpoint joins
  the previous committed cut with the chosen vertex's version. The child log
  appends that checkpoint's newly committed operations, preserving the agreed
  prefix rather than re-sorting previously committed operations.

## Ownership and parallel execution

The normal `Node::run` loop and the consensus `worker::run` loop execute on
**separate OS threads**, with independent TCP transports. The worker receives,
votes, retries, repairs decisions, and advances Paxos without calling the node
loop. Causal traffic and Paxos traffic use different connections and queues.
A stalled causal connection cannot put Paxos frames behind its bytes in TCP.

The original separation between Creek and Paxos is restored. `Creek<O>` keeps
its oracle, operation payloads, versions, frontier heads, delivered-sequence map,
applied-slot cursor, and pending commitment cuts on the replica owner thread.
The worker exclusively owns its `Paxos` instance: promises, accepted values,
ballots, votes, outgoing messages, and the retained decision journal.

The two loops synchronize through a small `Arc<Mutex<Shared>>` handoff:

- The owner publishes a coherent ID-only snapshot of leader, applied slot,
  frontier candidate, and delivered sequence numbers from `Creek::poll`.
- The worker copies that snapshot, releases the mutex, and performs all Paxos
  processing and network work on its private state.
- Newly chosen `(slot, vertex ID)` pairs are appended to the handoff queue. The
  owner takes that queue and releases the mutex before building commitment cuts.

Paxos execution, journal repair, network I/O, encoding/decoding, vector-clock
work, and child-log stabilization never run under the handoff lock. The mutex
protects the transfer itself; copying a delivery snapshot still takes work
proportional to membership. This is the original snapshot/decision-transfer
model rather than a mutex covering the whole consensus state.

`drive` is a short callback passed to `Node::run`: it calls `Creek::poll` to
receive decisions and publish input, then `CommitmentLog::commit_ready()` to
apply available checkpoints. The owner temporarily buffers only decisions it
has not yet applied, including reordered IDs or IDs awaiting causal delivery.
It removes those entries when their checkpoints commit. The retained journal
used for peer repair stays entirely inside the worker.

The applied cursor and candidate are published together in each snapshot so a
worker cannot start the next slot from a partially updated frontier. Since
`commit_ready()` follows publication, the next hook publishes its completed
application progress.

A paused replica owner can leave its worker voting and learning from data that
was already causally delivered and published in a snapshot. New payload
delivery, fresh oracle observations, applying commitments, and acknowledging
strong requests still require the owner. A leader waits for its previous checkpoint to be applied before proposing
the next one. Separate threads and sockets do not remove these protocol
dependencies or shared CPU/NIC limits.

`IsOmega` re-exports the core `IsOracle` trait. The oracle need not be `Send`,
including oracles containing `Rc` data. A custom oracle used with `Node` also
implements `DeepSizeOf`, as the built-in oracles already do. Custom consensus
transports implement `Transport + Send + 'static`; only that independent
transport moves to the worker.

## Parallel TCP port usage

`Creek::spawn_tcp(&config, WorkerConfig::default())` derives the consensus
network from the ordinary node configuration:

| Configured causal endpoint | Derived Creek endpoint |
| --- | --- |
| Local `listen_port: 19001` | Local listener on `19002` |
| Peer `replica-b.service:19003` | Peer `replica-b.service:19004` |
| Peer `[::1]:20000` | Peer `[::1]:20001` |

The hostnames, peer identities, membership, and causal configuration stay the
same. Both transports use the existing TCP
membership handshake and independent reconnect/bounded-send queues.

Base ports must be in `1..=65534`; zero, overflow, malformed peer ports, and a
local HTTP port equal to the derived Creek port fail before startup. On a shared
host/network namespace, reserve **non-overlapping port pairs**: for example
19001/19002 for A and 19003/19004 for B. The supplied `config_a.json` and
`config_b.json` follow that spacing. Reserve both ports from other applications.

In Kubernetes, allow both ports in network policies/firewalls. If peers use
Service hostnames, those Services must also expose and route the derived
`base + 1` port. HTTP probes and client endpoints keep their configured HTTP
port. This crate does not rewrite Kubernetes manifests or Services.

`WorkerConfig` restores the original runtime options: retry/decision
synchronization defaults to 200 ms and worker polling to 2 ms. Set
`retry_interval` and `poll_interval` when spawning the worker; these are Rust
runtime options, not a second configuration file. Both intervals must be
positive and worker polling must be at most 50 ms. Queue acceptance never
counts as a Paxos vote. Each replica periodically asks a rotating peer for
missing decisions, at most 32 IDs per reply. Reconnection retains promises, accepted values, and decisions.

Paxos packets contain only message tags, instance numbers, ballots, and vertex
IDs. Operations and vector clocks travel exclusively through the causal
channel. Each ID is an origin `u16` and a sequence `u64`, encoded as bincode
varints. The original protocol/version discriminator byte is retained.
For small integers, an `Accept` payload is **7 bytes**: 1 version, 1 variant,
1 slot, 2 ballot fields, and 2 vertex-ID fields. Add the TCP adapter's 4-byte
length prefix and normal TCP/IP overhead. An empty `Promise` takes 6 bytes;
`None` encodes only a presence tag, with no accepted-value placeholder. Control
frames are limited to 8 KiB.

## Integration

```rust,ignore
use moirai_network_node::external_commit::creek::{self, Creek, WorkerConfig};

let protocol = Creek::new(oracle, config.members(), own_id)?;
let worker = protocol.spawn_tcp(&config, WorkerConfig::default())?; // Starts the separate run loop.
let log = CommitmentLog::new(child, protocol);
let mut node = Node::new(config, log, application)?;
node.run(Some(&mut creek::drive))?;
drop(worker);
```

Keep the worker guard alive throughout the node's run. Dropping the guard stops
and joins the thread. Dropping Creek also requests worker shutdown. Worker
errors and panics propagate through `poll`/`drive` and stop the node loop. One
protocol instance can start only one worker; it cannot restart volatile Paxos
acceptors after shutdown. `Creek::spawn(transport, worker_config)` supports
another independent transport. `Creek::poll()` alone publishes metadata and prepares cuts; it does
not run Paxos or send messages.

This uses the existing `protocol_mut()` and `commit_ready()` core APIs without
new changes outside `moirai-network-node`. Normal `effect()` retains its existing
commitment path. The hook can drain a chosen checkpoint during quiescence,
without inserting another operation or no-op into the causal log.

### Example

From the repository root, in separate terminals:

```sh
cargo run --manifest-path moirai-network-node/Cargo.toml --example creek_set -- moirai-network-node/examples/config_a.json
cargo run --manifest-path moirai-network-node/Cargo.toml --example creek_set -- moirai-network-node/examples/config_b.json
```

A uses causal port 19001, Creek port 19002, and HTTP port 8081. B uses causal
port 19003, Creek port 19004, and HTTP port 8082. Each process takes just its
ordinary node config. With two members, both must remain available for a
majority; use three or more members to test commitment after one crash.
`wait_for_commit=true` waits on the Creek committed cut. Client acknowledgment
mode does not select which operations enter consensus.

## Guarantees and limits

This is a **trusted-peer, fixed-membership, crash-stop** implementation, matching
the paper's basic crash model. Promises and accepted values are retained in
the worker-owned Paxos instance but are **not persisted**. Do not reset the
protocol or restart a replica under the same membership ID while the cluster continues;
crash recovery would require durable Paxos state and application/state-transfer
support.

Safety does not depend on Omega agreeing during leader changes. Quorums are
strict majorities of distinct members; phase 1 adopts the highest accepted value,
and acceptors reject older ballots or two values in the same ballot. Progress
requires eventual agreement on a live leader, a communicating majority, and
eventual causal dissemination of proposed vertices. `EventOracle` additionally
requires sufficiently frequent event traffic; optional idle no-ops can supply it.

The ID-only decision journal is retained for lagging peers. It is not a durable
recovery log and is not garbage-collected in this implementation. Uncommitted
versions remain in the adapter until their cuts commit. Log-memory measurements
again include only the owner-side log and commitment metadata. As in the
original worker design, they exclude the worker's Paxos journal/round state,
the shared handoff, transport buffers, and OS socket memory. Sampling never
locks or traverses the worker journal. These measurements are not total process
memory and are not directly comparable to the intervening design that included
Paxos in log-memory measurements.

Tests cover quorum uniqueness, stale ballots, delayed causal availability,
highest-accepted recovery after leader loss, minority partitions, lost decision
repair, reordered decisions, seeded leadership changes and packet loss,
concurrent-cut ordering, causal-past stabilization, quiescent commitment,
parallel voting/learning while both owners are paused, independent TCP progress
without polling causal transports, derived ports, failure propagation, lifecycle
cleanup, TCP leader failover, and continued proposal processing/sends while the
handoff mutex is held by the owner. These tests are not a formal proof or a
crash-recovery implementation.
