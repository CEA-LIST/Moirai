# Moirai Protocol

This crate implements the replication protocol of the Pure Op CRDT framework, including the Tagged Causal Stable Broadcast (TCSB) and the associated data structures for representing events and states, and defining the semantics of Pure CRDTs.

## Crate organization

```sh
.
└── moirai-protocol/
    └── src/
        ├── broadcast/
        │   ├── batch.rs # Batch of messages
        │   ├── message.rs # Kind of messages the TCSB can send and receive
        │   ├── since.rs # Request for pulling messages
        │   └── tcsb.rs # Tagged Causal Stable Broadcast, implements a Reliable Causal Broadcast
        ├── clock/
        │   ├── matrix_clock.rs # Implementation of a matrix clock
        │   └── version_clock.rs # Implementation of a dotted version clock
        ├── crdt/
        │   ├── eval.rs # Pure CRDT evaluation semantics
        │   ├── policy.rs # Abstract total order policy
        │   ├── pure_crdt.rs # General trait for defining Pure CRDT semantics
        │   ├── query.rs # Set of common query operations
        │   └── redundancy.rs # Causal redundancy rules
        ├── event/
        │   ├── id.rs # Unique identifier for an event
        │   ├── lamport.rs # Lamport clock
        │   ├── tag.rs # Lamport + Event id
        │   └── tagged_op.rs # Operation + Tag
        ├── state/
        │   ├── event_graph.rs # Unstable log implementation in the form of a DAG
        │   ├── log.rs # Abstract operation storage
        │   ├── object_path.rs # Unique path identifying an object in a replicated hierarchy
        │   ├── po_log.rs # Log implementation in the form 
        │   ├── sink.rs # Lifecycle signal for creation/update/deletion of a replicated object
        │   ├── stable_state.rs # Compact state for stable operations
        │   └── unstable_state.rs # Partially-ordered state for unstable operations
        ├── replica.rs # Library main entry point. Represents a replica that can send/receive messages corresponding to operations to be applied to a replicated data type.
        └── utils/ # General utility modules (e.g., string internalizer)
```
