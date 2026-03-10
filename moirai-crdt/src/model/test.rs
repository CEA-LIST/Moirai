#[cfg(feature = "fuzz")]
use moirai_fuzz::value_generator::{StringConfig, ValueGenerator};
use moirai_macros::{record, typed_graph};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::Read},
    event::Event,
    state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
};
use petgraph::graph::DiGraph;

use crate::{
    HashMap,
    list::eg_walker::List,
    map::uw_map::{UWMap, UWMapLog},
    policy::LwwPolicy,
    register::unique_register::Register,
};

record!(Test {
    name: EventGraph::<List<char>>,
    reference: VecLog::<Register<TestId, LwwPolicy>>,
});

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct TestId(String);

#[cfg(feature = "fuzz")]
impl ValueGenerator for TestId {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        TestId(<String as ValueGenerator>::generate(
            rng,
            &StringConfig::default(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelfRefEdge;

typed_graph! {
    graph: ReferenceManager,
    vertex: Instance,
    edge: Ref,
    arcs_type: Refs,
    vertices { TestId },
    connections {
        SelfRef: TestId -> TestId (SelfRefEdge) [0, 1],
    }
}

// -- Model: top-level operation enum ------------------------------------------

#[derive(Debug, Clone)]
enum Model {
    /// Operation on an instance stored in the UWMap
    Child(UWMap<TestId, Test>),
    /// Operation on the reference graph
    Reference(ReferenceManager<LwwPolicy>),
}

// -- ModelValue: evaluated state of the whole model ---------------------------

#[derive(Debug, Default)]
struct ModelValue {
    children: HashMap<TestId, TestValue>,
    references: DiGraph<Instance, Ref>,
}

// -- ModelLog: composite log --------------------------------------------------

#[derive(Debug, Clone, Default)]
struct ModelLog {
    child: UWMapLog<TestId, TestLog>,
    reference_manager: VecLog<ReferenceManager<LwwPolicy>>,
}

impl IsLog for ModelLog {
    type Value = ModelValue;
    type Op = Model;

    fn is_enabled(&self, op: &Self::Op) -> bool {
        match op {
            Model::Child(op) => self.child.is_enabled(op),
            Model::Reference(op) => self.reference_manager.is_enabled(op),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            Model::Child(op) => {
                self.child.effect(event.unfold(op));
            }
            Model::Reference(op) => {
                self.reference_manager.effect(event.unfold(op));
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        self.child.stabilize(version);
        self.reference_manager.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.child.redundant_by_parent(version, conservative);
        self.reference_manager
            .redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.child.is_default() && self.reference_manager.is_default()
    }
}

impl EvalNested<Read<ModelValue>> for ModelLog {
    fn execute_query(&self, _q: Read<ModelValue>) -> ModelValue {
        ModelValue {
            children: self.child.execute_query(Read::new()),
            references: self.reference_manager.execute_query(Read::new()),
        }
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use moirai_macros::typed_graph::Arc;
    use moirai_protocol::replica::IsReplica;

    use crate::utils::membership::twins_log;

    use super::*;

    fn id(s: &str) -> TestId {
        TestId(s.to_string())
    }

    #[test]
    fn test_self_referencing_model() {
        let (mut replica_a, mut replica_b) = twins_log::<ModelLog>();

        // 1. Replica A creates instance "a" (write to reference field so it persists in UWMap)
        let e1 = replica_a
            .send(Model::Child(UWMap::Update(
                id("a"),
                Test::Reference(Register::Write(id("b"))),
            )))
            .unwrap();
        replica_b.receive(e1);

        // Register vertex "a" in the ReferenceManager
        let e2 = replica_a
            .send(Model::Reference(ReferenceManager::AddVertex {
                id: Instance::TestId(id("a")),
            }))
            .unwrap();
        replica_b.receive(e2);

        // 2. Replica B creates instance "b"
        let e3 = replica_b
            .send(Model::Child(UWMap::Update(
                id("b"),
                Test::Reference(Register::Write(id("a"))),
            )))
            .unwrap();
        replica_a.receive(e3);

        // Register vertex "b" in the ReferenceManager
        let e4 = replica_b
            .send(Model::Reference(ReferenceManager::AddVertex {
                id: Instance::TestId(id("b")),
            }))
            .unwrap();
        replica_a.receive(e4);

        // 3. Replica A adds a reference arc from "a" to "b"
        let e5 = replica_a
            .send(Model::Reference(ReferenceManager::AddArc(Refs::SelfRef(
                Arc {
                    source: id("a"),
                    target: id("b"),
                    kind: SelfRefEdge,
                },
            ))))
            .unwrap();
        replica_b.receive(e5);

        // 4. Query both replicas and verify convergence
        let state_a = replica_a.query(Read::new());
        let state_b = replica_b.query(Read::new());

        println!("State A: {:#?}", state_a);

        // Both should have 2 instances
        assert_eq!(state_a.children.len(), 2);
        assert_eq!(state_b.children.len(), 2);

        // Reference graph: 2 vertices, 1 edge (a -> b)
        assert_eq!(state_a.references.node_count(), 2);
        assert_eq!(state_b.references.node_count(), 2);
        assert_eq!(state_a.references.edge_count(), 1);
        assert_eq!(state_b.references.edge_count(), 1);
    }
}
