# Class Diagram CRDT

A class diagram is a model that describes the structure of a system by showing the system's classes, their attributes, methods, and the relationships among objects.

Structuraly, a class diagram is a directed multigraph, where:

- Nodes are classes.
- Edges are relationships between classes.

Nodes contain:

- Name: the name of the class.
- Attributes: properties of the class.
- Methods: functions that can be performed by the class.

Edges contain:

- Type: the type of relationship (e.g., association, aggregation, composition, inheritance).
- Source: the class that contains the relationship.
- Target: the class that is related to the source class.
- Label: a description of the relationship.

## Implementation

We first define a non-nested multigraph CRDT that we later extend to a nested multigraph CRDT.

Because we need to distinguish between potentially multiple parallel edges, we use a triple to represent an edge, which contains the source, target, and id of the edge.
