# Comparison with other CRDT libraries

*Last updated: September 2025*

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
