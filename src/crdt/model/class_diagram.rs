// Class Diagram CRDT
// This module defines a CRDT for class diagrams, allowing for collaborative editing of class structures,
// relationships, and features in a distributed manner. It uses various CRDT types to ensure consistency
// and convergence across different instances of the class diagram.
// It does not support: interfaces, enums, generics, static members, packages

use std::cmp::Ordering;

use petgraph::{
    dot::{Config, Dot},
    graph::{DiGraph, NodeIndex},
};

use crate::{
    crdt::{
        flag::ew_flag::EWFlag,
        graph::uw_multidigraph::{Content, UWGraphLog},
        map::uw_map::UWMapLog,
        register::{mv_register::MVRegister, to_register::TORegister},
    },
    protocol::state::po_log::VecLog,
    record, HashMap,
};

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub enum RelationType {
    Extends,
    Implements,
    Composes,
    Aggregates,
    #[default]
    Associates,
}

impl RelationType {
    fn rank(&self) -> u8 {
        match self {
            RelationType::Associates => 0,
            RelationType::Aggregates => 1,
            RelationType::Composes => 2,
            RelationType::Implements => 3,
            RelationType::Extends => 4,
        }
    }
}

impl PartialOrd for RelationType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelationType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

#[derive(Debug, Clone, Eq, Default, PartialEq, Hash)]
pub enum PrimitiveType {
    String,
    Number,
    Boolean,
    #[default]
    Void,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum TypeRef {
    Primitive(PrimitiveType),
    Class(String),
}

impl Default for TypeRef {
    fn default() -> Self {
        TypeRef::Primitive(PrimitiveType::Void)
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Visibility {
    #[default]
    Public,
    Private,
    Protected,
    Package,
}

impl Visibility {
    fn rank(&self) -> u8 {
        match self {
            Visibility::Public => 0,
            Visibility::Protected => 1,
            Visibility::Package => 2,
            Visibility::Private => 3,
        }
    }
}

impl PartialOrd for Visibility {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Visibility {
    fn cmp(&self, other: &Self) -> Ordering {
        // Note: lower rank = more permissive
        self.rank().cmp(&other.rank())
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Multiplicity {
    #[default]
    Unspecified,
    One,
    ZeroOrOne,
    ZeroOrMany,
    OneOrMany,
    ManyToMany(u8, u8), // (min, max) with max >= min
    Exactly(u8),        // exactly N
    ZeroToMany(u8),     // zero to N
    OneToMany(u8),      // one to N
}

impl Multiplicity {
    /// Ranking for total order from most to least constrained
    fn rank(&self) -> u8 {
        match self {
            Multiplicity::Exactly(_) => 8,
            Multiplicity::One => 7,
            Multiplicity::ZeroOrOne => 6,
            Multiplicity::OneToMany(_) => 5,
            Multiplicity::ZeroToMany(_) => 4,
            Multiplicity::ManyToMany(_, _) => 3,
            Multiplicity::OneOrMany => 2,
            Multiplicity::ZeroOrMany => 1,
            Multiplicity::Unspecified => 0,
        }
    }
}

impl PartialOrd for Multiplicity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Multiplicity {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank_cmp = self.rank().cmp(&other.rank());

        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }

        match (self, other) {
            (Multiplicity::Exactly(a), Multiplicity::Exactly(b)) => a.cmp(b),
            (Multiplicity::OneToMany(a), Multiplicity::OneToMany(b)) => a.cmp(b),
            (Multiplicity::ZeroToMany(a), Multiplicity::ZeroToMany(b)) => a.cmp(b),
            (Multiplicity::ManyToMany(a_min, a_max), Multiplicity::ManyToMany(b_min, b_max)) => {
                match a_min.cmp(b_min) {
                    Ordering::Equal => a_max.cmp(b_max),
                    other => other,
                }
            }
            _ => Ordering::Equal,
        }
    }
}

record!(Feature {
    typ: VecLog::<MVRegister::<PrimitiveType>>,
    visibility: VecLog::<TORegister::<Visibility>>,
});

record!(Operation {
    is_abstract: VecLog::<EWFlag>,
    visibility: VecLog::<TORegister::<Visibility>>,
    parameters: UWMapLog::<String, VecLog::<MVRegister::<TypeRef>>>,
    return_type: VecLog::<MVRegister::<TypeRef>>,
});

record!(Class {
    is_abstract: VecLog::<EWFlag>,
    name: VecLog::<MVRegister::<String>>,
    features: UWMapLog::<String, FeatureLog>,
    operations: UWMapLog::<String, OperationLog>,
});

record!(Ends {
    source: VecLog::<TORegister::<Multiplicity>>,
    target: VecLog::<TORegister::<Multiplicity>>,
});

record!(Relation {
    ends: EndsLog,
    label: VecLog::<MVRegister::<String>>,
    typ: VecLog::<TORegister::<RelationType>>,
});

pub type ClassDiagramCrdt<'a> = UWGraphLog<&'a str, &'a str, ClassLog, RelationLog>;
pub type ClassDiagram<'a> =
    DiGraph<Content<&'a str, ClassValue>, Content<(&'a str, &'a str, &'a str), RelationValue>>;

pub fn export_fancy_class_diagram(graph: &ClassDiagram) -> String {
    let fancy_dot = Dot::with_attr_getters(
        graph,
        &[Config::EdgeNoLabel, Config::NodeNoLabel],
        &edge_attr,
        &node_attr,
    );
    let mut fancy_string = format!("{fancy_dot:?}");
    fancy_string = fancy_string.replace(
        "digraph {",
        "digraph {\n    rankdir=BT\n    node [shape=record, fontname=\"Helvetica\", fontsize=10]\n    edge [fontname=\"Helvetica\", fontsize=10]\n",
    );
    fancy_string
}

fn edge_attr(
    _g: &ClassDiagram,
    edge: petgraph::graph::EdgeReference<Content<(&str, &str, &str), RelationValue>>,
) -> String {
    let label = &edge
        .weight()
        .val
        .label
        .iter()
        .cloned()
        .collect::<Vec<String>>()
        .join("/");
    let rtype = &edge.weight().val.typ;

    let multiplicity_from = format_mult(&edge.weight().val.ends.source);
    let multiplicity_to = format_mult(&edge.weight().val.ends.target);

    let (head, style) = match rtype {
        RelationType::Extends => ("empty", "normal"),
        RelationType::Implements => ("empty", "dashed"),
        RelationType::Aggregates => ("odiamond", "normal"),
        RelationType::Composes => ("diamond", "normal"),
        RelationType::Associates => ("normal", "normal"),
    };
    format!(
        "label=\"{label}\", arrowhead=\"{head}\", style=\"{style}\", taillabel=\"{multiplicity_from}\", headlabel=\"{multiplicity_to}\", labeldistance=1.25, labelangle=45, fontcolor=brown"
    )
}

fn format_mult(m: &Multiplicity) -> String {
    match m {
        Multiplicity::Unspecified => "".to_string(),
        Multiplicity::One => "1".to_string(),
        Multiplicity::ZeroOrOne => "0..1".to_string(),
        Multiplicity::ZeroOrMany => "0..*".to_string(),
        Multiplicity::OneOrMany => "1..*".to_string(),
        Multiplicity::ManyToMany(min, max) => format!("{min}..{max}"),
        Multiplicity::Exactly(n) => format!("{n}"),
        Multiplicity::ZeroToMany(n) => format!("0..{n}"),
        Multiplicity::OneToMany(n) => format!("1..{n}"),
    }
}

fn node_attr(g: &ClassDiagram, (_, class): (NodeIndex, &Content<&str, ClassValue>)) -> String {
    let name_vec: Vec<String> = class.val.name.iter().cloned().collect();
    let name = format_node_name(&class.val, &name_vec);
    let features = format_features(&class.val.features);
    let operations = format_operations(g, &class.val.operations);
    let is_abstract = if class.val.is_abstract {
        "style=filled, fillcolor=\"#e5f2ff\""
    } else {
        "style=filled, fillcolor=\"#e5ffe5\""
    };
    format!("label=\"{{{name}|{features}\\l|{operations}\\l}}\",{is_abstract}")
}

fn format_node_name(class: &ClassValue, name_vec: &[String]) -> String {
    let prefix = if class.is_abstract { "Ⓐ " } else { "Ⓒ " };
    let name_str = if name_vec.is_empty() {
        "Unnamed".to_string()
    } else {
        name_vec.join("/")
    };
    format!("{prefix}{name_str}")
}

fn format_features(features: &HashMap<String, FeatureValue>) -> String {
    features
        .iter()
        .map(|(k, v)| {
            let feature_name = k.clone();
            let types: Vec<String> = v.typ.iter().map(|t| format!("{t:?}")).collect();
            let feature_type = if types.is_empty() {
                "Unknown".to_string()
            } else {
                types.join("/")
            };
            let feature_vis = match v.visibility {
                Visibility::Public => "+",
                Visibility::Private => "-",
                Visibility::Protected => "#",
                Visibility::Package => "~",
            };
            format!("{feature_vis}{feature_name}: {feature_type}")
        })
        .collect::<Vec<String>>()
        .join("\\l")
}

fn format_operations(g: &ClassDiagram, operations: &HashMap<String, OperationValue>) -> String {
    operations
        .iter()
        .map(|(k, v)| {
            let op_name = k.clone();
            let op_vis = match v.visibility {
                Visibility::Public => "+",
                Visibility::Private => "-",
                Visibility::Protected => "#",
                Visibility::Package => "~",
            };
            let params: Vec<String> = v
                .parameters
                .iter()
                .map(|(p, t)| {
                    let types: Vec<String> = t.iter().map(|ty| format!("{ty:?}")).collect();
                    format!("{}: {}", p, types.join("/"))
                })
                .collect();
            let return_types: Vec<String> = v
                .return_type
                .iter()
                .cloned()
                .map(|t| match t {
                    TypeRef::Primitive(pt) => format!("{pt:?}"),
                    TypeRef::Class(c) => g
                        .raw_nodes()
                        .iter()
                        .find_map(|n| {
                            if n.weight.id == c {
                                Some(
                                    n.weight
                                        .val
                                        .name
                                        .iter()
                                        .cloned()
                                        .collect::<Vec<String>>()
                                        .join("/"),
                                )
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| "Unknown".to_string()),
                })
                .collect();
            let return_type_str = if return_types.is_empty() {
                "".to_string()
            } else {
                format!(": {}", return_types.join("/"))
            };
            format!(
                "{}{}{}({}){}",
                op_vis,
                if v.is_abstract { "Ⓐ " } else { "" },
                op_name,
                params.join("/"),
                return_type_str
            )
        })
        .collect::<Vec<String>>()
        .join("\\l")
}

#[cfg(test)]
mod tests {

    // @startuml
    // abstract class EnergyGenerator {
    //     + {abstract} getEnergyOutput(): Number
    // }

    // class WindTurbine {
    //     + start(): void
    //     + shutdown(): void
    // }

    // class Rotor {
    //     + diameter: Number
    //     - maxRpm: Number
    // }

    // class Blade {

    // }

    // class Tower {
    // + heightM: Number
    // + material: String
    // }

    // class Nacelle {
    // + weightTons: Number
    // - internalTempC: Number
    // }

    // class EnergyGrid {
    // + gridName: String
    // + capacityMW: Number
    // }

    // class Manufacturer {
    // + name: String
    // }

    // EnergyGenerator <|-- WindTurbine

    // WindTurbine "1" o-- "1" Rotor        : hasRotor
    // Rotor       "1" *-- "3" Blade        : comprises
    // WindTurbine "1" o-- "1" Nacelle      : hasNacelle
    // WindTurbine "1" o-- "1" Tower        : mountedOn

    // EnergyGenerator "1..*" --> "1" EnergyGrid : feedsInto
    // EnergyGrid "0..*" -- "0..*" EnergyGrid : connectedTo

    // Manufacturer "1" --> "0..*" WindTurbine : owns
    // Manufacturer "1..*" --> "0..*" WindTurbine : repairs
    // @enduml

    use crate::{
        crdt::{
            flag::ew_flag::EWFlag,
            graph::uw_multidigraph::{UWGraph, UWGraphLog},
            map::uw_map::UWMap,
            model::class_diagram::{
                export_fancy_class_diagram, Class, ClassDiagramCrdt, ClassLog, Ends, Feature,
                Multiplicity, Operation, PrimitiveType, Relation, RelationLog, RelationType,
                TypeRef, Visibility,
            },
            register::{mv_register::MVRegister, to_register::TORegister},
            test_util::twins_log,
        },
        protocol::{
            broadcast::tcsb::Tcsb,
            crdt::query::Read,
            replica::{IsReplica, Replica},
        },
    };

    fn wind_turbine_diagram() -> (
        Replica<
            UWGraphLog<&'static str, &'static str, ClassLog, RelationLog>,
            Tcsb<UWGraph<&'static str, &'static str, Class, Relation>>,
        >,
        Replica<
            UWGraphLog<&'static str, &'static str, ClassLog, RelationLog>,
            Tcsb<UWGraph<&'static str, &'static str, Class, Relation>>,
        >,
    ) {
        let (mut replica_a, mut replica_b) = twins_log::<ClassDiagramCrdt>();

        // WindTurbine class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "wt",
            Class::Name(MVRegister::Write("WindTurbine".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "start".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "shutdown".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));
        // EnergyGenerator class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "eg",
            Class::Name(MVRegister::Write("EnergyGenerator".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "eg",
            Class::Operations(UWMap::Update(
                "getEnergyOutput".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Class("wt".to_string()))),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "eg",
            Class::Operations(UWMap::Update(
                "getEnergyOutput".to_string(),
                Operation::IsAbstract(EWFlag::Enable),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "eg",
            Class::IsAbstract(EWFlag::Enable),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "wt",
            "eg",
            "ext",
            Relation::Typ(TORegister::Write(RelationType::Extends)),
        ));

        // Rotor class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "rotor",
            Class::Name(MVRegister::Write("Rotor".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "diameter".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Private)),
            )),
        ));
        // Blade class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "blade",
            Class::Name(MVRegister::Write("Blade".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Typ(TORegister::Write(RelationType::Composes)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::Exactly(3)))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Label(MVRegister::Write("comprises".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "rotor",
            "wt",
            "hasRotor",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "rotor",
            "wt",
            "hasRotor",
            Relation::Label(MVRegister::Write("hasRotor".to_string())),
        ));
        // Tower class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "tower",
            Class::Name(MVRegister::Write("Tower".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "tower",
            Class::Features(UWMap::Update(
                "heightM".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "tower",
            Class::Features(UWMap::Update(
                "material".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "tower",
            "wt",
            "mountedOn",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "tower",
            "wt",
            "mountedOn",
            Relation::Label(MVRegister::Write("mountedOn".to_string())),
        ));
        // Nacelle class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "nacelle",
            Class::Name(MVRegister::Write("Nacelle".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "weightTons".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "internalTempC".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "internalTempC".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Private)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "nacelle",
            "wt",
            "hasNacelle",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "nacelle",
            "wt",
            "hasNacelle",
            Relation::Label(MVRegister::Write("hasNacelle".to_string())),
        ));
        // EnergyGrid class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Name(MVRegister::Write("EnergyGrid".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Features(UWMap::Update(
                "gridName".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Features(UWMap::Update(
                "capacityMW".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Label(MVRegister::Write("feedsInto".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "energy_grid",
            "energy_grid",
            "connectedTo",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "energy_grid",
            "energy_grid",
            "connectedTo",
            Relation::Label(MVRegister::Write("connectedTo".to_string())),
        ));
        // Manufacturer class
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "manufacturer",
            Class::Name(MVRegister::Write("Manufacturer".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateVertex(
            "manufacturer",
            Class::Features(UWMap::Update(
                "name".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Label(MVRegister::Write("owns".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Label(MVRegister::Write("repairs".to_string())),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
        ));
        let _ = replica_a.send(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));

        let batch = replica_a.pull(replica_b.since());
        replica_b.receive_batch(batch);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );

        (replica_a, replica_b)
    }

    // Conflict resolution tests

    // Alice and Bob both concurrently edit the WindTurbine class diagram name
    #[test]
    fn concurrent_class_name() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        let event_a = replica_a
            .send(UWGraph::UpdateVertex(
                "wt",
                Class::Name(MVRegister::Write("WindGenerator".to_string())),
            ))
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateVertex(
                "wt",
                Class::Name(MVRegister::Write("WindTurbineGenerator".to_string())),
            ))
            .unwrap();
        // Deliver events
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());

        println!("Class Diagram A: {}", export_fancy_class_diagram(&eval_a));
        println!("Class Diagram B: {}", export_fancy_class_diagram(&eval_b));
    }

    /// Alice believes that the WindTurbine class should be removed, while Bob believes it should be renamed to WindGenerator.
    #[test]
    fn concurrent_remove_update_class() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = replica_a.send(UWGraph::RemoveVertex("wt")).unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        // B updates the class name
        let event_b = replica_b
            .send(UWGraph::UpdateVertex(
                "wt",
                Class::Name(MVRegister::Write("WindGenerator".to_string())),
            ))
            .unwrap();

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&replica_b.query(Read::new()))
        );

        // Deliver events
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the relation from EnergyGenerator to EnergyGrid should be removed,
    /// while Bob believes it should be updated to have a different multiplicity.
    #[test]
    fn concurrent_remove_update_arc() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = replica_a
            .send(UWGraph::RemoveArc("eg", "energy_grid", "feedsInto"))
            .unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        // B updates the class name
        let event_b = replica_b
            .send(UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::OneToMany(2)))),
            ))
            .unwrap();

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&replica_b.query(Read::new()))
        );

        // Deliver events
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the EnergyGrid class should be removed,
    /// while Bob believes there should be an association from Manufacturer to EnergyGrid
    /// to reprensent that the manufacturer operates the energy grid, with a multiplicity of 0..* from Manufacturer to EnergyGrid and
    /// 1 from EnergyGrid to Manufacturer.
    #[test]
    fn concurrent_remove_vertex_update_arc() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = replica_a
            .send(UWGraph::RemoveVertex("energy_grid"))
            .unwrap();

        let event_b_1 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "operates",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ))
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "operates",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::ZeroOrMany))),
            ))
            .unwrap();
        let event_b_3 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "operates",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
            ))
            .unwrap();
        let event_b_4 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "operates",
                Relation::Label(MVRegister::Write("operates".to_string())),
            ))
            .unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&replica_b.query(Read::new()))
        );

        // Deliver events
        replica_a.receive(event_b_1);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);
        replica_a.receive(event_b_4);
        replica_b.receive(event_a);

        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
    }

    /// Alice believes that the `maxRpm` feature of the Rotor class should be public,
    /// and have its unit type rad/s directly in the field as a string,
    /// while Bob believes it should be protected and remain a Number.
    /// In addition, Alice wants to remove the diameter feature,
    /// while Bob wants to keep it be private.
    /// They both update the class name: Alice to "RotorUnit" and Bob to "RotorSystem".
    #[test]
    fn concurrent_update_feature_visibility_class_name() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A updates the feature visibility and type
        let event_a = replica_a
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Public)),
                )),
            ))
            .unwrap();
        let event_a_2 = replica_a
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ))
            .unwrap();
        let event_a_3 = replica_a
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ))
            .unwrap();
        let event_a_4 = replica_a
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Remove("diameter".to_string())),
            ))
            .unwrap();

        let event_a_5 = replica_a
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Name(MVRegister::Write("RotorUnit".to_string())),
            ))
            .unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        // B updates the feature visibility and type
        let event_b = replica_b
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Protected)),
                )),
            ))
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ))
            .unwrap();
        let event_b_3 = replica_b
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Name(MVRegister::Write("RotorSystem".to_string())),
            ))
            .unwrap();
        let event_b_4 = replica_b
            .send(UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "diameter".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Private)),
                )),
            ))
            .unwrap();
        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&replica_b.query(Read::new()))
        );

        // Deliver events
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);
        replica_a.receive(event_b_4);
        replica_b.receive(event_a);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);
        replica_b.receive(event_a_4);
        replica_b.receive(event_a_5);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the `start()` operation of the WindTurbine class should return a Boolean,
    /// while Bob believes it should return a Number.
    #[test]
    fn concurrent_update_operation_return_type() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A updates the return type to Boolean
        let event_a = replica_a
            .send(UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ))
            .unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        // B updates the return type to Number
        let event_b = replica_b
            .send(UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ))
            .unwrap();

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&replica_b.query(Read::new()))
        );

        // Deliver events
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
    }

    /// Alice and Bob believes there should be a relation from Manufacturer to EnergyGrid.
    /// But they concurrently update the relation.
    /// Alice: label="employ", type=Aggregates, multiplicity=0..* from Manufacturer to EnergyGrid and 1..* from EnergyGrid to Manufacturer.
    /// Bob: label="operates" type=Associates, multiplicity=1..2 from Manufacturer to EnergyGrid and 1 from EnergyGrid to Manufacturer.
    #[test]
    fn concurrent_update_relation() {
        let (mut replica_a, mut replica_b) = wind_turbine_diagram();

        // A updates the relation
        let event_a = replica_a
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ))
            .unwrap();
        let event_a_2 = replica_a
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::ZeroOrMany))),
            ))
            .unwrap();
        let event_a_3 = replica_a
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::OneOrMany))),
            ))
            .unwrap();
        let event_a_4 = replica_a
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Label(MVRegister::Write("employs".to_string())),
            ))
            .unwrap();

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&replica_a.query(Read::new()))
        );

        // B updates the relation
        let event_b = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ))
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneToMany(2)))),
            ))
            .unwrap();
        let event_b_3 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
            ))
            .unwrap();
        let event_b_4 = replica_b
            .send(UWGraph::UpdateArc(
                "manufacturer",
                "energy_grid",
                "rel",
                Relation::Label(MVRegister::Write("operates".to_string())),
            ))
            .unwrap();

        // Deliver events
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);
        replica_a.receive(event_b_4);
        replica_b.receive(event_a);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);
        replica_b.receive(event_a_4);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
    }

    // #[cfg(feature = "op_weaver")]
    // #[test]
    // fn op_weaver_class_diagram() {
    //     use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

    //     let ops = vec![
    //         UWGraph::UpdateVertex(
    //             "wt",
    //             Class::Name(MVRegister::Write("WindTurbine".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "wt",
    //             Class::Operations(UWMap::Update(
    //                 "start".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "wt",
    //             Class::Operations(UWMap::Update(
    //                 "shutdown".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         // EnergyGenerator class
    //         UWGraph::UpdateVertex(
    //             "eg",
    //             Class::Name(MVRegister::Write("EnergyGenerator".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "eg",
    //             Class::Operations(UWMap::Update(
    //                 "getEnergyOutput".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Class("wt".to_string()))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "eg",
    //             Class::Operations(UWMap::Update(
    //                 "getEnergyOutput".to_string(),
    //                 Operation::IsAbstract(EWFlag::Enable),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex("eg", Class::IsAbstract(EWFlag::Enable)),
    //         UWGraph::UpdateArc(
    //             "wt",
    //             "eg",
    //             "ext",
    //             Relation::Typ(TORegister::Write(RelationType::Extends)),
    //         ),
    //         // Rotor class
    //         UWGraph::UpdateVertex("rotor", Class::Name(MVRegister::Write("Rotor".to_string()))),
    //         UWGraph::UpdateVertex(
    //             "rotor",
    //             Class::Features(UWMap::Update(
    //                 "diameter".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "rotor",
    //             Class::Features(UWMap::Update(
    //                 "maxRpm".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "rotor",
    //             Class::Features(UWMap::Update(
    //                 "maxRpm".to_string(),
    //                 Feature::Visibility(TORegister::Write(Visibility::Private)),
    //             )),
    //         ),
    //         // Blade class
    //         UWGraph::UpdateVertex("blade", Class::Name(MVRegister::Write("Blade".to_string()))),
    //         UWGraph::UpdateArc(
    //             "blade",
    //             "rotor",
    //             "comprises",
    //             Relation::Typ(TORegister::Write(RelationType::Composes)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "blade",
    //             "rotor",
    //             "comprises",
    //             Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "blade",
    //             "rotor",
    //             "comprises",
    //             Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::Exactly(3)))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "blade",
    //             "rotor",
    //             "comprises",
    //             Relation::Label(MVRegister::Write("comprises".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "rotor",
    //             "wt",
    //             "hasRotor",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "rotor",
    //             "wt",
    //             "hasRotor",
    //             Relation::Label(MVRegister::Write("hasRotor".to_string())),
    //         ),
    //         // Tower class
    //         UWGraph::UpdateVertex("tower", Class::Name(MVRegister::Write("Tower".to_string()))),
    //         UWGraph::UpdateVertex(
    //             "tower",
    //             Class::Features(UWMap::Update(
    //                 "heightM".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "tower",
    //             Class::Features(UWMap::Update(
    //                 "material".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateArc(
    //             "tower",
    //             "wt",
    //             "mountedOn",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "tower",
    //             "wt",
    //             "mountedOn",
    //             Relation::Label(MVRegister::Write("mountedOn".to_string())),
    //         ),
    //         // Nacelle class
    //         UWGraph::UpdateVertex(
    //             "nacelle",
    //             Class::Name(MVRegister::Write("Nacelle".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "nacelle",
    //             Class::Features(UWMap::Update(
    //                 "weightTons".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "nacelle",
    //             Class::Features(UWMap::Update(
    //                 "internalTempC".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "nacelle",
    //             Class::Features(UWMap::Update(
    //                 "internalTempC".to_string(),
    //                 Feature::Visibility(TORegister::Write(Visibility::Private)),
    //             )),
    //         ),
    //         UWGraph::UpdateArc(
    //             "nacelle",
    //             "wt",
    //             "hasNacelle",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "nacelle",
    //             "wt",
    //             "hasNacelle",
    //             Relation::Label(MVRegister::Write("hasNacelle".to_string())),
    //         ),
    //         // EnergyGrid class
    //         UWGraph::UpdateVertex(
    //             "energy_grid",
    //             Class::Name(MVRegister::Write("EnergyGrid".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "substation",
    //             Class::Name(MVRegister::Write("Substation".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "substation",
    //             Class::Features(UWMap::Update(
    //                 "location".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "substation",
    //             Class::Operations(UWMap::Update(
    //                 "monitor".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "transformer",
    //             Class::Name(MVRegister::Write("Transformer".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "transformer",
    //             Class::Features(UWMap::Update(
    //                 "capacity".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "transformer",
    //             Class::Operations(UWMap::Update(
    //                 "stepUp".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "control_unit",
    //             Class::Name(MVRegister::Write("ControlUnit".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "control_unit",
    //             Class::Features(UWMap::Update(
    //                 "firmwareVersion".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "control_unit",
    //             Class::Operations(UWMap::Update(
    //                 "reset".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "sensor",
    //             Class::Name(MVRegister::Write("Sensor".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "sensor",
    //             Class::Features(UWMap::Update(
    //                 "type".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "sensor",
    //             Class::Operations(UWMap::Update(
    //                 "readValue".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Number,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "maintenance_team",
    //             Class::Name(MVRegister::Write("MaintenanceTeam".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "maintenance_team",
    //             Class::Features(UWMap::Update(
    //                 "teamSize".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "maintenance_team",
    //             Class::Operations(UWMap::Update(
    //                 "scheduleMaintenance".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "operator",
    //             Class::Name(MVRegister::Write("Operator".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "operator",
    //             Class::Features(UWMap::Update(
    //                 "certification".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "operator",
    //             Class::Operations(UWMap::Update(
    //                 "operate".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "customer",
    //             Class::Name(MVRegister::Write("Customer".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "customer",
    //             Class::Features(UWMap::Update(
    //                 "accountNumber".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "customer",
    //             Class::Operations(UWMap::Update(
    //                 "requestService".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "power_plant",
    //             Class::Name(MVRegister::Write("PowerPlant".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "power_plant",
    //             Class::Features(UWMap::Update(
    //                 "outputMW".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "power_plant",
    //             Class::Operations(UWMap::Update(
    //                 "generatePower".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Number,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "solar_panel",
    //             Class::Name(MVRegister::Write("SolarPanel".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "solar_panel",
    //             Class::Features(UWMap::Update(
    //                 "panelType".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "solar_panel",
    //             Class::Operations(UWMap::Update(
    //                 "produceEnergy".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Number,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "battery_storage",
    //             Class::Name(MVRegister::Write("BatteryStorage".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "battery_storage",
    //             Class::Features(UWMap::Update(
    //                 "capacityKWh".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "battery_storage",
    //             Class::Operations(UWMap::Update(
    //                 "storeEnergy".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "distribution_line",
    //             Class::Name(MVRegister::Write("DistributionLine".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "distribution_line",
    //             Class::Features(UWMap::Update(
    //                 "lengthKm".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "distribution_line",
    //             Class::Operations(UWMap::Update(
    //                 "inspect".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "switchgear",
    //             Class::Name(MVRegister::Write("Switchgear".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "switchgear",
    //             Class::Features(UWMap::Update(
    //                 "voltageRating".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "switchgear",
    //             Class::Operations(UWMap::Update(
    //                 "switchOn".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex("relay", Class::Name(MVRegister::Write("Relay".to_string()))),
    //         UWGraph::UpdateVertex(
    //             "relay",
    //             Class::Features(UWMap::Update(
    //                 "relayType".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "relay",
    //             Class::Operations(UWMap::Update(
    //                 "trigger".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex("meter", Class::Name(MVRegister::Write("Meter".to_string()))),
    //         UWGraph::UpdateVertex(
    //             "meter",
    //             Class::Features(UWMap::Update(
    //                 "reading".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "meter",
    //             Class::Operations(UWMap::Update(
    //                 "readMeter".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Number,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "grid_controller",
    //             Class::Name(MVRegister::Write("GridController".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "grid_controller",
    //             Class::Features(UWMap::Update(
    //                 "controlMode".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "grid_controller",
    //             Class::Operations(UWMap::Update(
    //                 "balanceLoad".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "weather_station",
    //             Class::Name(MVRegister::Write("WeatherStation".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "weather_station",
    //             Class::Features(UWMap::Update(
    //                 "temperature".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "weather_station",
    //             Class::Operations(UWMap::Update(
    //                 "reportWeather".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "data_logger",
    //             Class::Name(MVRegister::Write("DataLogger".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "data_logger",
    //             Class::Features(UWMap::Update(
    //                 "storageMB".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "data_logger",
    //             Class::Operations(UWMap::Update(
    //                 "logData".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "alarm_system",
    //             Class::Name(MVRegister::Write("AlarmSystem".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "alarm_system",
    //             Class::Features(UWMap::Update(
    //                 "alarmType".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "alarm_system",
    //             Class::Operations(UWMap::Update(
    //                 "activateAlarm".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "remote_terminal",
    //             Class::Name(MVRegister::Write("RemoteTerminal".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "remote_terminal",
    //             Class::Features(UWMap::Update(
    //                 "ipAddress".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "remote_terminal",
    //             Class::Operations(UWMap::Update(
    //                 "connect".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Boolean,
    //                 ))),
    //             )),
    //         ),
    //         // 50 relations between these classes
    //         UWGraph::UpdateArc(
    //             "substation",
    //             "transformer",
    //             "contains",
    //             Relation::Typ(TORegister::Write(RelationType::Composes)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "transformer",
    //             "distribution_line",
    //             "connectedTo",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "distribution_line",
    //             "meter",
    //             "feeds",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "meter",
    //             "customer",
    //             "measures",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "power_plant",
    //             "substation",
    //             "supplies",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "solar_panel",
    //             "battery_storage",
    //             "charges",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "battery_storage",
    //             "distribution_line",
    //             "discharges",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "switchgear",
    //             "relay",
    //             "controls",
    //             Relation::Typ(TORegister::Write(RelationType::Composes)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "relay",
    //             "alarm_system",
    //             "triggers",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "control_unit",
    //             "switchgear",
    //             "manages",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "grid_controller",
    //             "control_unit",
    //             "controls",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "weather_station",
    //             "grid_controller",
    //             "reportsTo",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "data_logger",
    //             "weather_station",
    //             "logs",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "remote_terminal",
    //             "grid_controller",
    //             "connectsTo",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "maintenance_team",
    //             "substation",
    //             "maintains",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "operator",
    //             "control_unit",
    //             "operates",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "customer",
    //             "meter",
    //             "owns",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "alarm_system",
    //             "remote_terminal",
    //             "alerts",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "data_logger",
    //             "meter",
    //             "records",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "maintenance_team",
    //             "distribution_line",
    //             "inspects",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "operator",
    //             "switchgear",
    //             "switches",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "relay",
    //             "control_unit",
    //             "communicatesWith",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "battery_storage",
    //             "meter",
    //             "monitors",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "solar_panel",
    //             "weather_station",
    //             "senses",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "power_plant",
    //             "grid_controller",
    //             "managedBy",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "substation",
    //             "alarm_system",
    //             "protectedBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "transformer",
    //             "sensor",
    //             "monitoredBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "sensor",
    //             "data_logger",
    //             "logsTo",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "remote_terminal",
    //             "maintenance_team",
    //             "notifies",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "customer",
    //             "operator",
    //             "contacts",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "meter",
    //             "data_logger",
    //             "sendsData",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "distribution_line",
    //             "relay",
    //             "protectedBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "switchgear",
    //             "alarm_system",
    //             "alertsOnFault",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "grid_controller",
    //             "battery_storage",
    //             "controlsStorage",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "weather_station",
    //             "solar_panel",
    //             "providesData",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "data_logger",
    //             "remote_terminal",
    //             "transmits",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "alarm_system",
    //             "maintenance_team",
    //             "alertsTeam",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "remote_terminal",
    //             "operator",
    //             "communicatesWith",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "substation",
    //             "meter",
    //             "measuredBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "transformer",
    //             "battery_storage",
    //             "chargesFrom",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "control_unit",
    //             "sensor",
    //             "receivesData",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "sensor",
    //             "alarm_system",
    //             "triggersAlarm",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "maintenance_team",
    //             "relay",
    //             "services",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "operator",
    //             "data_logger",
    //             "reviewsLogs",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "customer",
    //             "remote_terminal",
    //             "accesses",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "power_plant",
    //             "weather_station",
    //             "monitorsWeather",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "solar_panel",
    //             "meter",
    //             "measuredBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "battery_storage",
    //             "alarm_system",
    //             "alertsOnLow",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "distribution_line",
    //             "data_logger",
    //             "logsStatus",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "switchgear",
    //             "remote_terminal",
    //             "remotelyControlledBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "relay",
    //             "meter",
    //             "monitorsCurrent",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "energy_grid",
    //             Class::Features(UWMap::Update(
    //                 "gridName".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "energy_grid",
    //             Class::Features(UWMap::Update(
    //                 "capacityMW".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateArc(
    //             "eg",
    //             "energy_grid",
    //             "feedsInto",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "eg",
    //             "energy_grid",
    //             "feedsInto",
    //             Relation::Label(MVRegister::Write("feedsInto".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "eg",
    //             "energy_grid",
    //             "feedsInto",
    //             Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "eg",
    //             "energy_grid",
    //             "feedsInto",
    //             Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "energy_grid",
    //             "energy_grid",
    //             "connectedTo",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "energy_grid",
    //             "energy_grid",
    //             "connectedTo",
    //             Relation::Label(MVRegister::Write("connectedTo".to_string())),
    //         ),
    //         // Manufacturer class
    //         UWGraph::UpdateVertex(
    //             "manufacturer",
    //             Class::Name(MVRegister::Write("Manufacturer".to_string())),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "manufacturer",
    //             Class::Features(UWMap::Update(
    //                 "name".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "owns",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "owns",
    //             Relation::Label(MVRegister::Write("owns".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "owns",
    //             Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "owns",
    //             Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "repairs",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "repairs",
    //             Relation::Label(MVRegister::Write("repairs".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "repairs",
    //             Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
    //         ),
    //         UWGraph::UpdateArc(
    //             "manufacturer",
    //             "wt",
    //             "repairs",
    //             Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "wt",
    //             Class::Operations(UWMap::Update(
    //                 "restart".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "wt",
    //             Class::Features(UWMap::Update(
    //                 "serialNumber".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "eg",
    //             Class::Operations(UWMap::Update(
    //                 "reset".to_string(),
    //                 Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
    //                     PrimitiveType::Void,
    //                 ))),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "rotor",
    //             Class::Features(UWMap::Update(
    //                 "material".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "blade",
    //             Class::Features(UWMap::Update(
    //                 "length".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "tower",
    //             Class::Features(UWMap::Update(
    //                 "foundationDepth".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "nacelle",
    //             Class::Features(UWMap::Update(
    //                 "manufacturer".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "energy_grid",
    //             Class::Features(UWMap::Update(
    //                 "region".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateVertex(
    //             "manufacturer",
    //             Class::Features(UWMap::Update(
    //                 "country".to_string(),
    //                 Feature::Typ(MVRegister::Write(PrimitiveType::String)),
    //             )),
    //         ),
    //         UWGraph::UpdateArc(
    //             "wt",
    //             "energy_grid",
    //             "supplies",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "wt",
    //             "energy_grid",
    //             "supplies",
    //             Relation::Label(MVRegister::Write("supplies".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "rotor",
    //             "blade",
    //             "contains",
    //             Relation::Typ(TORegister::Write(RelationType::Aggregates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "rotor",
    //             "blade",
    //             "contains",
    //             Relation::Label(MVRegister::Write("contains".to_string())),
    //         ),
    //         UWGraph::UpdateArc(
    //             "tower",
    //             "manufacturer",
    //             "builtBy",
    //             Relation::Typ(TORegister::Write(RelationType::Associates)),
    //         ),
    //         UWGraph::UpdateArc(
    //             "tower",
    //             "manufacturer",
    //             "builtBy",
    //             Relation::Label(MVRegister::Write("builtBy".to_string())),
    //         ),
    //         UWGraph::RemoveVertex("blade"),
    //         UWGraph::RemoveVertex("eg"),
    //         UWGraph::RemoveVertex("energy_grid"),
    //         UWGraph::RemoveVertex("manufacturer"),
    //         UWGraph::RemoveVertex("nacelle"),
    //         UWGraph::RemoveVertex("rotor"),
    //         UWGraph::RemoveVertex("tower"),
    //         UWGraph::RemoveArc("blade", "rotor", "comprises"),
    //         UWGraph::RemoveArc("eg", "energy_grid", "feedsInto"),
    //         UWGraph::RemoveArc("energy_grid", "energy_grid", "connectedTo"),
    //         UWGraph::RemoveArc("manufacturer", "wt", "owns"),
    //         UWGraph::RemoveArc("manufacturer", "wt", "repairs"),
    //         UWGraph::RemoveArc("nacelle", "wt", "hasNacelle"),
    //         UWGraph::RemoveArc("rotor", "blade", "contains"),
    //         UWGraph::RemoveArc("rotor", "wt", "hasRotor"),
    //         UWGraph::RemoveArc("tower", "manufacturer", "builtBy"),
    //         UWGraph::RemoveArc("tower", "wt", "mountedOn"),
    //         UWGraph::RemoveArc("wt", "eg", "ext"),
    //         UWGraph::RemoveVertex("wt"),
    //         UWGraph::RemoveArc("wt", "energy_grid", "supplies"),
    //     ];

    //     let config = EventGraphConfig {
    //         name: "wind_turbine_class_diagram",
    //         num_replicas: 16,
    //         num_operations: 100_000,
    //         operations: &ops,
    //         final_sync: true,
    //         churn_rate: 0.4,
    //         reachability: None,
    //         compare: |a: &ClassDiagram, b: &ClassDiagram| petgraph::algo::is_isomorphic(a, b),
    //         record_results: true,
    //         seed: None,
    //         witness_graph: false,
    //         concurrency_score: false,
    //     };

    //     op_weaver::<ClassDiagramCrdt>(config);
    // }

    // #[cfg(feature = "op_weaver")]
    // #[test]
    // fn class_diagram_lot_of_iterations() {
    //     for i in 0..1_000 {
    //         use log::info;

    //         info!("Completed: {:.2}%", (i as f64 / 1_000.0) * 100.0);
    //         op_weaver_class_diagram();
    //     }
    // }
}
