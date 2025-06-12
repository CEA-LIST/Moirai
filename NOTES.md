# Notes on CRDTs and Event Graphs

Currently, in the Rust library, CRDT operations are stored in an event graph—a
causal DAG or PO-log—where operations are represented as nodes, and direct
causal predecessors are the edges of the graph. As a result, the event graph
corresponds to the transitive reduction of the causal order.

Most arbitration policies for non-commutative CRDTs are based on the idea that,
in the case of two conflicting operations, one takes precedence over the other.
In the "add-wins" policy, this means that a "remove" operation only affects its
causal predecessors, not its concurrent operations. Consequently, an "add"
operation is never impacted by a concurrent "remove".

The library supports the composition and combination of CRDTs to create complex
data types. These nested data types naturally form a tree structure. Thus, the
library can be viewed as a tree of event graphs, where operations at upper nodes
can affect lower nodes.

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
event graph also requires a DFS per child. To favor genericity, the library is
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

## On the matrix clock

A matrix clock is valid if it:

is square no clock i has an entry j greater than the entry j of clock j every
entry i of the origin clock is equal or greater to the entry i of the clock i
The row of the replica where the matrix clock is stored is equal to the
column-wise maximum of the matrix. the column-wise minimum of the matrix is the
vector clock of events that have been delivered to every replica in the system
The matrix is ​​monotonically increasing, that is, each new event updates the
appropriate row in the matrix by merging the incoming vector clock with the one
stored in the matrix. As a result, the matrix can contain arbitrary high integer
values
