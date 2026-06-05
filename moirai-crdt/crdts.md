## List of implemented CRDTs


### **Counter**
- **Counter** (`Inc`/`Dec`): Commutative operations on numeric values.
- **Resettable Counter**: Same as above, plus a `Reset` that zeroes the value.

### **Flag**
- **DWFlag** (Disable-Wins): Concurrent `Disable` beats `Enable`.
- **EWFlag** (Enable-Wins): Concurrent `Enable` beats `Disable`.

### **Register**
- **MVRegister** (Multi-Value): Keeps all concurrent writes as a conflict set.
- **TORegister** (Total-Order): Keeps the highest value (via `Ord`).
- **PORegister** (Partial-Order): Keeps only maximal incomparable values.
- **LWW/Fair Register**: Uses Lamport timestamps or round-robin tie-breaking.

### **Set**
- **AWSet** (Add-Wins): Concurrent `Add` beats `Remove`.
- **RWSet** (Remove-Wins): Concurrent `Remove` beats `Add`.
- **EWFlagSet**: Each element backed by an EWFlag (nested state-based).

### **Map**
- **UWMap** (Update-Wins): Key-value map where values are nested CRDTs themselves; supports `Update(key, op)`, `Remove(key)`, `Clear`.

### **Bag**
- **AWBag** (Add-Wins Bag): Multiset tracking element counts, composed of `UWMap<V, Counter>`.

### **List**
- **EG-Walker List**: Uses an event-graph algorithm for ordered sequences; supports `Insert`, `Delete`, `DeleteRange`, `Update`.
- **Nested List**: List where each element is itself a CRDT (e.g., a list of JSON objects).

### **Graph**
- **AW Multidigraph** (Add-Wins): Directed multigraph; concurrent `AddVertex`/`AddArc` always succeed.
- **UW Multidigraph** (Update-Wins): Each vertex/edge contains a nested CRDT payload.
- **TypedGraph**: Macro-generated type-safe graph with typed nodes and edges.

### **JSON**
- Recursive structure composing `Counter` (numbers), `EWFlag` (booleans), `List<char>` (strings), `UWMap` (objects), and `NestedList` (arrays).

### **Model (Class Diagram)**
- Domain-specific CRDT for collaborative UML class diagram editing: classes with names, attributes, operations, relationships (extends, implements, composes, aggregates, associates), using registers, flags, and maps.

### **Option**
- Wraps any CRDT with optional semantics: `Set(op)` / `Unset`.

### **Policy**
- **LWW** (Last-Writer-Wins): Lamport clock + origin ID.
- **Fair**: Round-robin per replica index.

