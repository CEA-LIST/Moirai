# Notes

A collection of thoughts, ideas, and notes on the design and implementation of
the Moirai CRDT framework. Some of these notes may be outdated or no longer
relevant, but they are preserved for reference and historical context.

## Issues

- Cloning the `origin_id` is expensive.
- Computing the column-wise minimum of a matrix clock is expensive.
- Merging two vector clocks is expensive.
- Logging
- `is_enabled`?

## Batch Structure

Batch { id: <proc_id>, events: Vec<Event>, }

Event { ... resolver: Resolver (diff), }

Each process keep a translation of its indices to the one of the other (matrix).
Indices of the Vec = other process mapping
Content of the Vec = local process mapping

Delivery procedure:

- iter over event version vector:
  - for each replica_idx, replace it with the value at this index in the matrix

## The Event Graph

Currently, in the Rust framework, CRDT operations are stored in an event graph—a
causal DAG or PO-log—where operations are represented as nodes, and direct
causal predecessors are the edges of the graph. As a result, the event graph
corresponds to the transitive reduction of the causal order.

Most arbitration policies for non-commutative CRDTs are based on the idea that,
in the case of two conflicting operations, one takes precedence over the other.
In the "add-wins" policy, this means that a "remove" operation only affects its
causal predecessors, not its concurrent operations. Consequently, an "add"
operation is never impacted by a concurrent "remove".

The framework supports the composition and combination of CRDTs to create
complex data types. These nested data types naturally form a tree structure.
Thus, the framework can be viewed as a tree of event graphs, where operations at
upper nodes can affect lower nodes.

Consider an "Update-Wins Map" (UWMap) where adding a key-value pair takes
precedence over the concurrent removal of the key. For example, we might define
a `UWMap<String, ResettableCounter<i32>>`, which is a map of integer counters
keyed by strings. Each operation only references its direct causal predecessors,
so when a `remove(k)` operation is delivered, we cannot use its clock directly
to find the "cut" in the ResettableCounter event graph at key k that corresponds
to the causal predecessors of `remove(k)`. In such cases, we are forced to
reconstruct the vector clock of `remove(k)`, as it is the only universal
representation of causal order that applies across all nested event graph
children. This reconstruction incurs the cost of a depth-first search (DFS).
Moreover, identifying the causal predecessors of this vector clock in each child
event graph also requires a DFS per child. To favor genericity, the framework is
currently "dumb"—it only considers causal relationships between operations and
ignores semantic links. For example, in an add-wins set, it is inefficient to
examine all keys to determine the effect of a particular remove operation on the
state. Ideally, we should only consider operations that share the same key as
the remove. A potential avenue for future work is to find a way to describe
these semantic links and incorporate them into the event graph. This would allow
the operation ordering to be tailored to each specific data type, leading to
improved performance. More generally, specifying CRDTs and formally describing
their semantics is a long-standing research challenge. When we began working
with pure operation-based CRDTs, we were drawn to the clean and elegant
specifications they enabled. However, to achieve sublinear complexity, we now
need to enrich these specifications with additional information. The segregation
of related operations into their own event graphs, combined with the ability to
define hierarchical relationships, significantly improves performance. This
design confines the effect of an operation to its children (if any), reducing
unnecessary computation. Additionally, event graphs can be extended with
datatype-specific stable storage—a data structure tailored to efficiently store
causally stable operations for that datatype. For example, in the case of an
add-wins set (AWSet), this storage can simply be a sequential set, which is
highly efficient, as set operations typically run in O(1) time.

## The Matrix Version

A matrix clock is valid if it:

- is square;
- no clock $i$ has an entry $j$ greater than the entry $j$ of clock $j$;
- every entry $i$ of the origin clock is equal or greater to the entry $i$ of
  the clock $i$.

The row of the replica where the matrix clock is stored is equal to the
column-wise maximum of the matrix. the column-wise minimum of the matrix is the
vector clock of events that have been delivered to every replica in the system
The matrix is monotonically increasing, that is, each new event updates the
appropriate row in the matrix by merging the incoming vector clock with the one
stored in the matrix. As a result, the matrix can contain arbitrary high integer
values.

## The Membership system

- The membership system is based on a view abstraction. Multiple other
  components rely on a pointer to the current view to retrieve the membership
  information. It is clear that the current implementation will produce issues
  in the future as the serialization/deserialization will have to convert the
  index of the "local replica" of the sender's view to its index in the receiver
  view.

## Feature Comparison of CRDT Frameworks

| Feature                                       | **⭐ [Moirai](https://gitlab.deeplab.fr/leo.olivier/po-crdt)**                                                                    | **[yjs](https://github.com/yjs/yjs)/[yrs](https://docs.rs/yrs/latest/yrs/)** | **[Automerge](https://github.com/automerge)**    | **[Collabs](https://github.com/composablesys/collabs)**                                                   | **[Loro](https://github.com/loro-dev/loro)**                                                        | **[Flec](https://gitlab.soft.vub.ac.be/jimbauwens/flec/)**                      |
| --------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------- | :----------------------------------------------- | :-------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------ |
| **Language**                                  | Rust                                                                                                                              | TypeScript / Rust                                                            | TypeScript / Rust                                | TypeScript                                                                                                | Rust                                                                                                | TypeScript                                                                      |
| **Maturity**                                  | In development                                                                                                                    | Production-ready                                                             | Production-ready                                 | Academic                                                                                                  | Production-ready                                                                                    | Academic                                                                        |
| **CRDT approach**                             | Pure op-based<sup>1</sup>                                                                                                         | Delta state-based                                                            | Operation-based                                  | Mixed                                                                                                     | Operation-based                                                                                     | Pure op-based                                                                   |
| **Provided CRDT types**                       | - LWW-Register<br>- MV-Register<br>- AW-Map<br>- AW-Set<br>- RW-Set<br>- Counter<br>- ResettableCounter<br>- AW-Graph<br>- Object | - JSON<br>- Map<br>- List (YATA)<br>- Text<br>- XML                          | - JSON<br>- Map<br>- MV-Register<br>- List (RGA) | - Flag<br>- Counter<br>- Text & Rich Text (Peritext)<br>- AW-Set<br>- LWW-Map<br>- List (Fugue, Peritext) | - JSON<br>- LWW Map<br>- List (Peritext, Fugue, Eg-Walker)<br>- Movable List<br>- Tree<br>- Counter | - RW-Map<br>- UW-Map<br>- LWW-Register<br>- MV-Register<br>- AW-Set<br>- RW-Set |
| **History & time travel**                     | 〰️, Configurable depth, subject to GC policy                                                                                      | ❌                                                                           | ✅, Maintains the entire causal history          | ❌                                                                                                        | ✅, Maintains the entire causal history                                                             | ❌                                                                              |
| **Extensibility**<sup>2</sup>                 | ✅                                                                                                                                | ❌                                                                           | ❌                                               | ✅                                                                                                        | ❌                                                                                                  | ✅                                                                              |
| **Tombstones garbage collection**<sup>3</sup> | ✅, Eventual causal stability                                                                                                     | 〰️                                                                           | ❌                                               | 〰️, Unclear                                                                                               | ❌                                                                                                  | 〰️, Clasical causal stability                                                   |
| **CRDT nesting**<sup>4</sup>                  | ✅, Generic                                                                                                                       | 〰️, Composition of already defined CRDTs                                     | 〰️, Composition of already defined CRDTs         | 〰️, Static generation                                                                                     | 〰️, Composition of already existing CRDTs                                                           | ✅, Generic                                                                     |
| **Partial replication / queries**<sup>5</sup> | 〰️, Foundations ready (hierarchical segregation)                                                                                  | ❌                                                                           | ❌                                               | ❌                                                                                                        | ❌                                                                                                  | ❌                                                                              |
| **Considered environment**                    | Permissioned<sup>1</sup>                                                                                                          | Permissionless                                                               | Permissionless, claims BFT<sup>6</sup>           | Permissionless                                                                                            | Permissionless                                                                                      | Unclear                                                                         |
| **Deployment targets**                        | ✅, Browser (Wasm) + native                                                                                                       | ✅, Browser (TS) + native                                                    | ✅, Browser (TS, Wasm) + native                  | 〰️, Browser only                                                                                          | ✅, Browser (Wasm) + native                                                                         | 〰️, Browser only                                                                |

## Legend

- **✅** Feature is supported.
- **❌** Feature is not supported.
- **〰️** Feature is partially supported or supported with limitations.

## Notes

- **<sup>1</sup>** Moirai draws inspiration from the concept of pure
  operation-based CRDTs, which rely on a generic log of operations and use
  causal stability as a first-class mechanism for garbage collection. However,
  for practical and performance reasons, not all Moirai CRDT operations are
  "pure": the `prepare` phase may inspect the state to decide how to emit an
  operation, rather than always producing a predefined, context-independent
  operation. Moirai assumes a permissioned environment where group members are
  always known and authorized, even though the group itself may be dynamic.
- **<sup>2</sup>** Flec, Moirai, and Collabs support the definition of new CRDT
  types by implementing an interface.
- **<sup>3</sup>** Moirai relies on a central authority to ensure eventual
  causal stability, i.e., every operation eventually becomes stable. This is
  currently achieved by removing replicas from the group if they remain
  unreachable for a specified period. In contrast, Flec adopts the classical
  approach to eventual stability, which is not fault-tolerant: the crash of a
  single replica can indefinitely block garbage collection. Yjs appears to
  retain tombstones for deleted operations
  indefinitely<sup>[1](https://github.com/yjs/yjs/blob/main/INTERNALS.md#deletions),[2](https://discuss.yjs.dev/t/should-size-of-binary-ydoc-be-monotonically-increasing/2325/3),[3](https://discuss.yjs.dev/t/clear-document-history-and-reject-old-updates/945),[4](https://github.com/yjs/yjs?tab=readme-ov-file#yjs-crdt-algorithm),[5](https://blog.kevinjahns.de/are-crdts-suitable-for-shared-editing)</sup>,
  but their memory footprint is highly optimized, making the overhead negligible
  in practice. Collabs claims to use a similar approach to that of Yjs.
  Automerge<sup>[6](https://automerge.org/docs/cookbook/modeling-data/)</sup>
  and Loro maintain the entire causal history of operations.
- **<sup>4</sup>** Yjs, Loro, and Automerge support the composition of already
  defined CRDTs, such as Map and List. Collabs allows to statically generate a
  new CRDT type that is the composition of existing CRDTs thanks to semi-direct
  product of CRDTs<sup>[1](https://arxiv.org/pdf/2212.02618)</sup>.
  Flec<sup>[2](https://drops.dagstuhl.de/storage/00lipics/lipics-vol263-ecoop2023/LIPIcs.ECOOP.2023.2/LIPIcs.ECOOP.2023.2.pdf)</sup>
  and Moirai support generic CRDT containers that can hold any CRDT type.
- **<sup>5</sup>** Partial replication and querying refer to the capability to
  replicate or access only a subset of a CRDT’s
  state<sup>[1](https://arxiv.org/pdf/1806.10254),[2](https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7396168)</sup>.
  Moirai lays the groundwork for this by maintaining a separate log for each
  CRDT at every level of the hierarchy, enabling selective replication and
  querying within the CRDT tree.
- **<sup>6</sup>** Automerge claims to support
  BFT<sup>[1](https://liangrunda.com/posts/automerge-internal-2/)</sup>. A
  recent article<sup>[2](https://doi.org/10.1016/j.parco.2025.103136)</sup>,
  shows that using recursive hash histories is not sufficient to guarantee
  Byzantine Fault Tolerance (BFT).
