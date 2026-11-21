# Moirai Macros

Some replicated data types are too abstract to be implemented directly in Rust, for example because they accept a variable number of type parameters. The `moirai-macros` crate provides a set of procedural macros that generate the boilerplate code for implementing these CRDTs.

## CRDT macros

- **Record**: Generates a record CRDT with a variable number of fields, each of which is a replicated data type. It is simply a replicated object.
- **Union**: Generates a union CRDT with a variable number of variants, each of which is a replicated data type. Generalizes the MV-Register CRDT to an arbitrary number of mutable replicated variants.
- **Typed Graph**: Generates a typed graph CRDT from a given graph schema. The schema is a set of node types and edge types.

## Explicit operation layouts

`record!` keeps both existing forms: `field: Log` produces a generic read value,
and `field: Log => Value` produces an explicit read value. A field can additionally
specify its operation payload as `field: Operation, Log => Value`, following the
operation/log/value order used by `union!`:

```rust,ignore
record!(Node {
    value: VecLog<Counter<i32>> => i32,
    children: NestedList<Box<Node>>, NestedListLog<NodeLog> => Vec<NodeValue>,
});
```

The `Children` operation contains `NestedList<Box<Node>>`, while the child log is
an ordinary `NestedListLog<NodeLog>` and the query returns `Vec<NodeValue>`.
`Boxer` converts the operation before forwarding it. Validation borrows identity
and plain unboxing conversions directly; structural conversions use a temporary.
Existing and explicit payload fields can be mixed, with or without a trailing comma.
No recursive macro parser or per-field policy type is required.

For inline recursive storage, use ordinary boxes:

```rust,ignore
record!(Scalar {
    value: VecLog<Counter<i32>> => i32,
    next: Optional<Box<Scalar>>, OptionLog<Box<ScalarLog>> => Option<Box<ScalarValue>>,
});
```

`Box<L>` implements `IsLog` with unchanged commands/operations, boxed rejections,
and `Read<Box<V>>` when `L` implements `Read<V>`. Operation boxing is selected
separately in the record declaration. Nested lists, maps and unions already
indirect their rejection types, including recursive maps. These error boxes are
allocated only when returning a rejection.

`union!` already accepts explicit operation payloads and uses `Boxer`; its syntax
is unchanged. `Boxer` supports identity, plain boxing/unboxing, and conversions
inside optional, nested-list and map operation variants.
