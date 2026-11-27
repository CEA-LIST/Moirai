# Correctness notes

Assumptions are the paper's fixed membership, crash-only behavior, unique
per-origin sequences, causal delivery, immutable event metadata, and eventually
accurate/agreed Omega output. Membership is the same set everywhere. Each correct
replica keeps issuing vertices, and delivered events eventually reach all correct
replicas. The selected unstable backend preserves all appended operations until
explicit pruning. These conditions exclude dynamic membership, state transfer and
multiple independently delivered derived events with the same origin/sequence.

## The deliberate predecessor variant

Let H(w) = past(w), the full sender history immediately before issuing w. Define
P(v,w) to mean that v is a partial leader in H(w), using the paper's partial-leader
predicate. Define C-minus as the least set containing every actual leader and
closed under P. This changes Algorithm 1's recursive projection from graph(w) to
H(w); leader detection itself is unchanged.

The key proof ingredients still hold:

1. H(w) is a fixed, downward-closed graph, independent of the observing replica.
2. Partial leaders in a common H(w) are causally comparable (Lemma 4).
3. If v precedes w, then H(v) is contained in H(w). A partial leader remains
   partial in a smaller graph that contains it (Lemma 7).
4. An actual leader is partial in every graph containing it (Lemma 5).

The latest partial leader p in H(w) is enough to encode the closure: any earlier
partial leader u in H(w) lies in H(p), and remains partial there by Lemma 7.
Following p's immutable predecessor link recursively recovers every required
older checkpoint. It can also recover historical partial leaders no longer
partial in H(w), exactly as the recursive closure requires.

The proofs of causal comparability, nesting and prefix agreement for the
checkpoint sets use only these properties of the projections (the argument in
Lemmas 9-11 and Theorems 12-13). Replacing graph(w) with H(w) preserves those
inclusions and membership facts: an earlier checkpoint v belongs to H(w)
whenever v causally precedes w. Consequently the same inductive argument applies
to C-minus. This is a proof sketch of the variant, not a machine-checked proof.

The code's birth flag is a conservative eligibility test: enough immediate
predecessors vote for the issuer. The new vertex's own vote can then disqualify
its own trace. That cannot miss a future partial leader, since partial status
cannot appear later in a growing graph once lost. The receiver's actual support
state determines eligibility and completed support.

Applying each checkpoint's complete present set, minus already committed events,
with one deterministic causal sorting policy gives a prefix-compatible committed
sequence. Prefix + sorted remaining tail gives causal and strongly eventually
consistent optimistic output. The additional historical checkpoints can change
the chosen legal order relative to the exact paper algorithm; comparing only
checkpoint equality with the unmodified Algorithm 1 is therefore not the right
safety test for this deliberate variant.

## Incremental support invariant

For candidate v and replica r:

- No map entry means no predecessor for r exists, or its observed region already
  contains a disagreeing vote.
- `false` means the predecessor and every observed r-event since it vote for
  v.origin, but r has not yet issued an observed successor of v.
- `true` means the first successor has arrived and the entire inclusive trace
  agrees. This state never changes when later r-events arrive.

At first delivery of v there is no delivered successor of v. A range from each
positive predecessor through the latest delivered r-event therefore computes
exactly the observed trace. Subsequent deliveries either extend an open trace,
disqualify it, or close it. A closed trace is unaffected by subsequent votes.
This gives the paper's leader predicate when a strict majority is closed.

## Pruning invariant

Application commitment uses the full version of the checkpoint c. Metadata
pruning retains previous(c), c and all later observed votes, using componentwise
saturating subtraction. That retention vector is not used as an application cut.

Every newly admitted candidate v strictly after c has each existing predecessor
at or beyond c's corresponding component: for v's own origin, its new sequence
is strictly above c's component. Thus all votes needed to initialize v's state
survive this metadata pruning. Existing candidates already hold their trace
summaries and do not need to reread removed votes.

A late candidate concurrent with the already committed checkpoint cannot become
a checkpoint in any compatible extension, by causal comparability of C-minus.
Do not initialize it using potentially pruned predecessor ranges. Still record
its vote and update existing candidates, since future vertices can depend on it.
The positive-predecessor assertion catches violations of the retention/delivery
invariant rather than turning missing history into vacuous support.

The newest committed leader remains retained. It supplies a predecessor boundary
even after older records have been pruned. Traversal can stop when a predecessor
is inside the committed cut; a missing predecessor outside it is an invariant
failure and must not silently terminate traversal.

## Integration boundaries

Wire IDs are compact stable IDs (`u16` replica number and `u64` event sequence).
Local indices are resolved only after TCSB has
internalized the clock. Quorums use the fixed configured membership, never an
empty default or merely the set of replicas observed so far. TCSB stability does
not advance application commitment. Optimistic queries fold the same ordering
policy as commitment. Client response/waiting semantics and the implementation
of Omega remain responsibilities of their respective layers.
