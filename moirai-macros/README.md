# Moirai Macros

Some replicated data types are too abstract to be implemented directly in Rust, for example because they accept a variable number of type parameters. The `moirai-macros` crate provides a set of procedural macros that generate the boilerplate code for implementing these CRDTs.

## CRDT macros

- **Record**: Generates a record CRDT with a variable number of fields, each of which is a replicated data type. It is simply a replicated object.
- **Union**: Generates a union CRDT with a variable number of variants, each of which is a replicated data type. Generalizes the MV-Register CRDT to an arbitrary number of mutable replicated variants.
- **Typed Graph**: Generates a typed graph CRDT from a given graph schema. The schema is a set of node types and edge types.
