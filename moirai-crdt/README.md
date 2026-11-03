# Moirai CRDT

Collection of Pure CRDTs implemented on top of the Moirai Protocol.

Each CRDT family defines its operation payloads in `update.rs`. Leaf datatypes
implement `ReplicatedDataType` on an empty struct and select the shared payload
through `type Update`. For example, `AWSet<V>` and `RWSet<V>` both use
`set::update::SetUpdate<V>`; the four register variants use
`register::update::RegisterUpdate<V>`.

Composed objects implement `IsLog` directly and keep their state in the log.
Their payloads follow the same convention: `MapUpdate` is shared by `RWMapLog`
and `UWMapLog`, and `NestedListUpdate` lives beside `ListUpdate` in
`list/update.rs`.

`typed_graph!` generates an empty `MyGraph<P>` datatype and a `MyGraphUpdate`
alias for `graph::update::TypedGraphUpdate<Vertex, Arcs>`. The policy parameter
belongs to the datatype; updates depend only on the schema. The shared enum
lives in `moirai-macros/src/typed_graph/update.rs` and is re-exported by the
graph family to keep macros independent of `moirai-crdt`.
