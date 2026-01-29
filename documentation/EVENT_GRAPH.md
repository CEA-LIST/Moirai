# Event Graphs

We recall that our problem consists in integrating a new event into a **directed acyclic graph (DAG)** whose nodes represent events and whose edges represent **precedence (causal) relations** between events.

Each event is equipped with:

- a **version vector of size $N$**, corresponding to the $N$ processes (replicas) of the system, and
- the identifier of the process that authored the event.

The goal is to attach each new event to the existing DAG by adding a set of edges that correctly capture its causal predecessors.

Among all DAGs containing the same nodes and representing the same causal order, the most compact representation—i.e., the one with the **minimum number of edges preserving reachability by transitivity**—is called the **transitive reduction** of the graph.

Our objective is therefore twofold:

1. **Minimize the number of edges** added when integrating a new event (ideally maintaining the transitive reduction).
2. **Minimize the computational cost** of this integration.

Finally, we consider the case where the DAG **does not contain all events that occurred since the beginning of the execution**. This situation arises, for example, when the graph stores only the operations of a specific CRDT type, while the system executes a **nested CRDT** involving multiple operation types. In such cases, some events referenced in a version vector may not be present in the graph.

## Studied Approaches

We investigated the following methods.

### 1. Exact computation of immediate predecessors from the version vector

This approach consists in computing, from the version vector of the new event, the set of its **immediate predecessors in the graph**, i.e., the minimal set of nodes that preserves the same causal relations.

Applying this method to every event yields the **exact transitive reduction** of the graph. However, computing immediate predecessors requires a global analysis of the graph and costs approximately

$O(g^3)$

where $g$ is the number of nodes and edges in the graph.

**Results (4 replicas, 1,000 events):**

- Throughput: ~160 events/s
- Graph size: ~1,200 edges for 1,000 nodes

### 2. Closest-predecessor per version-vector entry

For each entry of the version vector, we identify the **closest event present in the graph** that precedes the new event. This is necessary because the event identifier referenced in the version vector may not exist in the graph (e.g., it belongs to another CRDT type in a nested setting).

Using an auxiliary data structure, we iterate over the $N$ entries of the version vector and, for each entry, locate the closest existing predecessor in

$O(\log N)$

time, yielding a total cost of $O(N \log N)$.

This method does **not** compute the transitive reduction and therefore introduces redundant edges.

**Results (4 replicas, 1,000 events):**

- Throughput: ~320 events/s
- Graph size: ~4,000 edges for 1,000 nodes

### 3. Periodic transitive reduction

This approach combines Method 2 with a **periodic transitive reduction** of the graph, executed either:

- every $X$ events, or
- when the node-to-edge ratio exceeds a predefined threshold.

Each reduction costs $O(g^3)$, but amortizing this cost over multiple insertions yields a reasonable trade-off.

**Results (4 replicas, 1,000 events, reduction every 500 events):**

- Throughput: ~250 events/s
- Graph size: ~1,200 edges for 1,000 nodes

### 4. Local filtering of redundant predecessors (1-hop pruning)

This method refines Method 2 without performing a full transitive reduction.

First, we compute the candidate predecessor set $A$ (of size at most $N$) as in Method 2. Then, we **remove from $A$** any node whose **immediate predecessor in the graph** also belongs to $A$. Intuitively, this ensures that no two nodes in $A$ are causally related.

Crucially, this pruning only requires **1-hop exploration** of the graph, avoiding global traversal.

**Results (4 replicas, 1,000 events):**

- Throughput: ~350 events/s
- Graph size: ~1,850 edges for 1,000 nodes

## Summary

These results illustrate the trade-off between:

- **graph compactness** (approximating the transitive reduction), and
- **insertion throughput**.

Method 4 achieves the best throughput while significantly reducing redundancy compared to the naive closest-predecessor approach, without incurring the high cost of full transitive reduction.
