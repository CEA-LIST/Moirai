// Class Diagram CRDT
// This module defines a CRDT for class diagrams, allowing for collaborative editing of class structures,
// relationships, and features in a distributed manner. It uses various CRDT types to ensure consistency
// and convergence across different instances of the class diagram.
// It does not support: interfaces, enums, generics, static members, packages

use std::{cmp::Ordering, collections::HashMap};

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
    protocol::event_graph::EventGraph,
    record,
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
    typ: EventGraph::<MVRegister::<PrimitiveType>>,
    visibility: EventGraph::<TORegister::<Visibility>>,
});

record!(Operation {
    is_abstract: EventGraph::<EWFlag>,
    visibility: EventGraph::<TORegister::<Visibility>>,
    parameters: UWMapLog::<String, EventGraph::<MVRegister::<TypeRef>>>,
    return_type: EventGraph::<MVRegister::<TypeRef>>,
});

record!(Class {
    is_abstract: EventGraph::<EWFlag>,
    name: EventGraph::<MVRegister::<String>>,
    features: UWMapLog::<String, FeatureLog>,
    operations: UWMapLog::<String, OperationLog>,
});

record!(Ends {
    source: EventGraph::<TORegister::<Multiplicity>>,
    target: EventGraph::<TORegister::<Multiplicity>>,
});

record!(Relation {
    ends: EndsLog,
    label: EventGraph::<MVRegister::<String>>,
    typ: EventGraph::<TORegister::<RelationType>>,
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
    let mut fancy_string = format!("{:?}", fancy_dot);
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
        "label=\"{}\", arrowhead=\"{}\", style=\"{}\", taillabel=\"{}\", headlabel=\"{}\", labeldistance=1.25, labelangle=45, fontcolor=brown",
        label, head, style, multiplicity_from, multiplicity_to
    )
}

fn format_mult(m: &Multiplicity) -> String {
    match m {
        Multiplicity::Unspecified => "".to_string(),
        Multiplicity::One => "1".to_string(),
        Multiplicity::ZeroOrOne => "0..1".to_string(),
        Multiplicity::ZeroOrMany => "0..*".to_string(),
        Multiplicity::OneOrMany => "1..*".to_string(),
        Multiplicity::ManyToMany(min, max) => format!("{}..{}", min, max),
        Multiplicity::Exactly(n) => format!("{}", n),
        Multiplicity::ZeroToMany(n) => format!("0..{}", n),
        Multiplicity::OneToMany(n) => format!("1..{}", n),
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
    format!(
        "label=\"{{{}|{}\\l|{}\\l}}\",{}",
        name, features, operations, is_abstract
    )
}

fn format_node_name(class: &ClassValue, name_vec: &[String]) -> String {
    let prefix = if class.is_abstract { "Ⓐ " } else { "Ⓒ " };
    let name_str = if name_vec.is_empty() {
        "Unnamed".to_string()
    } else {
        name_vec.join("/")
    };
    format!("{}{}", prefix, name_str)
}

fn format_features(features: &HashMap<String, FeatureValue>) -> String {
    features
        .iter()
        .map(|(k, v)| {
            let feature_name = k.clone();
            let types: Vec<String> = v.typ.iter().cloned().map(|t| format!("{:?}", t)).collect();
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
            format!("{}{}: {}", feature_vis, feature_name, feature_type)
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
                    let types: Vec<String> =
                        t.iter().cloned().map(|ty| format!("{:?}", ty)).collect();
                    format!("{}: {}", p, types.join("/"))
                })
                .collect();
            let return_types: Vec<String> = v
                .return_type
                .iter()
                .cloned()
                .map(|t| match t {
                    TypeRef::Primitive(pt) => format!("{:?}", pt),
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
    use super::*;
    use crate::{
        crdt::{graph::uw_multidigraph::UWGraph, map::uw_map::UWMap, test_util::twins},
        protocol::{pulling::Since, tcsb::Tcsb},
    };

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

    fn wind_turbine_diagram() -> (
        Tcsb<ClassDiagramCrdt<'static>>,
        Tcsb<ClassDiagramCrdt<'static>>,
    ) {
        let (mut tcsb_a, mut tcsb_b) = twins::<ClassDiagramCrdt>();

        // WindTurbine class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Name(MVRegister::Write("WindTurbine".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "start".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "shutdown".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));
        // EnergyGenerator class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "eg",
            Class::Name(MVRegister::Write("EnergyGenerator".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "eg",
            Class::Operations(UWMap::Update(
                "getEnergyOutput".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Class("wt".to_string()))),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "eg",
            Class::Operations(UWMap::Update(
                "getEnergyOutput".to_string(),
                Operation::IsAbstract(EWFlag::Enable),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "eg",
            Class::IsAbstract(EWFlag::Enable),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "wt",
            "eg",
            "ext",
            Relation::Typ(TORegister::Write(RelationType::Extends)),
        ));

        // Rotor class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Name(MVRegister::Write("Rotor".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "diameter".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Private)),
            )),
        ));
        // Blade class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "blade",
            Class::Name(MVRegister::Write("Blade".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Typ(TORegister::Write(RelationType::Composes)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::Exactly(3)))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "blade",
            "rotor",
            "comprises",
            Relation::Label(MVRegister::Write("comprises".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "rotor",
            "wt",
            "hasRotor",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "rotor",
            "wt",
            "hasRotor",
            Relation::Label(MVRegister::Write("hasRotor".to_string())),
        ));
        // Tower class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "tower",
            Class::Name(MVRegister::Write("Tower".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "tower",
            Class::Features(UWMap::Update(
                "heightM".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "tower",
            Class::Features(UWMap::Update(
                "material".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "tower",
            "wt",
            "mountedOn",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "tower",
            "wt",
            "mountedOn",
            Relation::Label(MVRegister::Write("mountedOn".to_string())),
        ));
        // Nacelle class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "nacelle",
            Class::Name(MVRegister::Write("Nacelle".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "weightTons".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "internalTempC".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "nacelle",
            Class::Features(UWMap::Update(
                "internalTempC".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Private)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "nacelle",
            "wt",
            "hasNacelle",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "nacelle",
            "wt",
            "hasNacelle",
            Relation::Label(MVRegister::Write("hasNacelle".to_string())),
        ));
        // EnergyGrid class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Name(MVRegister::Write("EnergyGrid".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Features(UWMap::Update(
                "gridName".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "energy_grid",
            Class::Features(UWMap::Update(
                "capacityMW".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Label(MVRegister::Write("feedsInto".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "energy_grid",
            "energy_grid",
            "connectedTo",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "energy_grid",
            "energy_grid",
            "connectedTo",
            Relation::Label(MVRegister::Write("connectedTo".to_string())),
        ));
        // Manufacturer class
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "manufacturer",
            Class::Name(MVRegister::Write("Manufacturer".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "manufacturer",
            Class::Features(UWMap::Update(
                "name".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Label(MVRegister::Write("owns".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "owns",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Label(MVRegister::Write("repairs".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));

        let batch = tcsb_a.events_since(&Since::new_from(&tcsb_b));
        tcsb_b.deliver_batch(batch);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval())
            .first()
            .is_some());

        (tcsb_a, tcsb_b)
    }

    // Conflict resolution tests

    // Alice and Bob both concurrently edit the WindTurbine class diagram name
    #[test_log::test]
    fn concurrent_class_name() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Name(MVRegister::Write("WindGenerator".to_string())),
        ));
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Name(MVRegister::Write("WindTurbineGenerator".to_string())),
        ));
        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();

        println!("Class Diagram A: {}", export_fancy_class_diagram(&eval_a));
        println!("Class Diagram B: {}", export_fancy_class_diagram(&eval_b));
    }

    /// Alice believe that the WindTurbine class should be removed, while Bob believes it should be renamed to WindGenerator.
    #[test_log::test]
    fn concurrent_remove_update_class() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = tcsb_a.tc_bcast(UWGraph::RemoveVertex("wt"));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        // B updates the class name
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Name(MVRegister::Write("WindGenerator".to_string())),
        ));

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the relation from EnergyGenerator to EnergyGrid should be removed,
    /// while Bob believes it should be updated to have a different multiplicity.
    #[test_log::test]
    fn concurrent_remove_update_arc() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = tcsb_a.tc_bcast(UWGraph::RemoveArc("eg", "energy_grid", "feedsInto"));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        // B updates the class name
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::OneToMany(2)))),
        ));

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the EnergyGrid class should be removed,
    /// while Bob believes there should be an association from Manufacturer to EnergyGrid
    /// to reprensent that the manufacturer operates the energy grid, with a multiplicity of 0..* from Manufacturer to EnergyGrid and
    /// 1 from EnergyGrid to Manufacturer.
    #[test_log::test]
    fn concurrent_remove_vertex_update_arc() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A removes the class
        let event_a = tcsb_a.tc_bcast(UWGraph::RemoveVertex("energy_grid"));

        let event_b_1 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "operates",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let event_b_2 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "operates",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));
        let event_b_3 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "operates",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
        ));
        let event_b_4 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "operates",
            Relation::Label(MVRegister::Write("operates".to_string())),
        ));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b_1);
        tcsb_a.try_deliver(event_b_2);
        tcsb_a.try_deliver(event_b_3);
        tcsb_a.try_deliver(event_b_4);
        tcsb_b.try_deliver(event_a);

        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the `maxRpm` feature of the Rotor class should be public,
    /// and have its unit type rad/s directly in the field as a string,
    /// while Bob believes it should be protected and remain a Number.
    /// In addition, Alice wants to remove the diameter feature,
    /// while Bob wants to keep it be private.
    /// They both update the class name: Alice to "RotorUnit" and Bob to "RotorSystem".
    #[test_log::test]
    fn concurrent_update_feature_visibility_class_name() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A updates the feature visibility and type
        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Public)),
            )),
        ));
        let event_a_2 = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let event_a_3 = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::String)),
            )),
        ));
        let event_a_4 = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Remove("diameter".to_string())),
        ));

        let event_a_5 = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Name(MVRegister::Write("RotorUnit".to_string())),
        ));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        // B updates the feature visibility and type
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Protected)),
            )),
        ));
        let event_b_2 = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "maxRpm".to_string(),
                Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let event_b_3 = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Name(MVRegister::Write("RotorSystem".to_string())),
        ));
        let event_b_4 = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "rotor",
            Class::Features(UWMap::Update(
                "diameter".to_string(),
                Feature::Visibility(TORegister::Write(Visibility::Private)),
            )),
        ));
        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_a.try_deliver(event_b_2);
        tcsb_a.try_deliver(event_b_3);
        tcsb_a.try_deliver(event_b_4);
        tcsb_b.try_deliver(event_a);
        tcsb_b.try_deliver(event_a_2);
        tcsb_b.try_deliver(event_a_3);
        tcsb_b.try_deliver(event_a_4);
        tcsb_b.try_deliver(event_a_5);
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice believes that the `start()` operation of the WindTurbine class should return a Boolean,
    /// while Bob believes it should return a Number.
    #[test_log::test]
    fn concurrent_update_operation_return_type() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A updates the return type to Boolean
        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "start".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        // B updates the return type to Number
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "start".to_string(),
                Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(PrimitiveType::Void))),
            )),
        ));

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());

        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    /// Alice and Bob believes there should be a relation from Manufacturer to EnergyGrid.
    /// But they concurrently update the relation.
    /// Alice: label="employ", type=Aggregates, multiplicity=0..* from Manufacturer to EnergyGrid and 1..* from EnergyGrid to Manufacturer.
    /// Bob: label="operates" type=Associates, multiplicity=1..2 from Manufacturer to EnergyGrid and 1 from EnergyGrid to Manufacturer.
    #[test_log::test]
    fn concurrent_update_relation() {
        let (mut tcsb_a, mut tcsb_b) = wind_turbine_diagram();

        // A updates the relation
        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Typ(TORegister::Write(RelationType::Aggregates)),
        ));
        let event_a_2 = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::ZeroOrMany))),
        ));
        let event_a_3 = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::OneOrMany))),
        ));
        let event_a_4 = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Label(MVRegister::Write("employs".to_string())),
        ));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );

        // B updates the relation
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Typ(TORegister::Write(RelationType::Associates)),
        ));
        let event_b_2 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneToMany(2)))),
        ));
        let event_b_3 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
        ));
        let event_b_4 = tcsb_b.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "energy_grid",
            "rel",
            Relation::Label(MVRegister::Write("operates".to_string())),
        ));

        println!(
            "Class Diagram B: {}",
            export_fancy_class_diagram(&tcsb_b.eval())
        );

        // Deliver events
        tcsb_a.try_deliver(event_b);
        tcsb_a.try_deliver(event_b_2);
        tcsb_a.try_deliver(event_b_3);
        tcsb_a.try_deliver(event_b_4);
        tcsb_b.try_deliver(event_a);
        tcsb_b.try_deliver(event_a_2);
        tcsb_b.try_deliver(event_a_3);
        tcsb_b.try_deliver(event_a_4);
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert!(vf2::isomorphisms(&eval_a, &eval_b).first().is_some());
        println!("Merge result: {}", export_fancy_class_diagram(&eval_a));
    }

    #[cfg(feature = "op_weaver")]
    #[test_log::test]
    fn op_weaver_class_diagram() {
        use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

        let ops = vec![
            UWGraph::UpdateVertex(
                "wt",
                Class::Name(MVRegister::Write("WindTurbine".to_string())),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "shutdown".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            // EnergyGenerator class
            UWGraph::UpdateVertex(
                "eg",
                Class::Name(MVRegister::Write("EnergyGenerator".to_string())),
            ),
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "getEnergyOutput".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Class("wt".to_string()))),
                )),
            ),
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "getEnergyOutput".to_string(),
                    Operation::IsAbstract(EWFlag::Enable),
                )),
            ),
            UWGraph::UpdateVertex("eg", Class::IsAbstract(EWFlag::Enable)),
            UWGraph::UpdateArc(
                "wt",
                "eg",
                "ext",
                Relation::Typ(TORegister::Write(RelationType::Extends)),
            ),
            // Rotor class
            UWGraph::UpdateVertex("rotor", Class::Name(MVRegister::Write("Rotor".to_string()))),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "diameter".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "maxRpm".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Private)),
                )),
            ),
            // Blade class
            UWGraph::UpdateVertex("blade", Class::Name(MVRegister::Write("Blade".to_string()))),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Typ(TORegister::Write(RelationType::Composes)),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::Exactly(3)))),
            ),
            UWGraph::UpdateArc(
                "blade",
                "rotor",
                "comprises",
                Relation::Label(MVRegister::Write("comprises".to_string())),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "wt",
                "hasRotor",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "wt",
                "hasRotor",
                Relation::Label(MVRegister::Write("hasRotor".to_string())),
            ),
            // Tower class
            UWGraph::UpdateVertex("tower", Class::Name(MVRegister::Write("Tower".to_string()))),
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "heightM".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "material".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateArc(
                "tower",
                "wt",
                "mountedOn",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "tower",
                "wt",
                "mountedOn",
                Relation::Label(MVRegister::Write("mountedOn".to_string())),
            ),
            // Nacelle class
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Name(MVRegister::Write("Nacelle".to_string())),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "weightTons".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "internalTempC".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "internalTempC".to_string(),
                    Feature::Visibility(TORegister::Write(Visibility::Private)),
                )),
            ),
            UWGraph::UpdateArc(
                "nacelle",
                "wt",
                "hasNacelle",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "nacelle",
                "wt",
                "hasNacelle",
                Relation::Label(MVRegister::Write("hasNacelle".to_string())),
            ),
            // EnergyGrid class
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Name(MVRegister::Write("EnergyGrid".to_string())),
            ),
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "gridName".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "capacityMW".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Label(MVRegister::Write("feedsInto".to_string())),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
            ),
            UWGraph::UpdateArc(
                "eg",
                "energy_grid",
                "feedsInto",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "energy_grid",
                "energy_grid",
                "connectedTo",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "energy_grid",
                "energy_grid",
                "connectedTo",
                Relation::Label(MVRegister::Write("connectedTo".to_string())),
            ),
            // Manufacturer class
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Name(MVRegister::Write("Manufacturer".to_string())),
            ),
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Features(UWMap::Update(
                    "name".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Label(MVRegister::Write("owns".to_string())),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::One))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "owns",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Label(MVRegister::Write("repairs".to_string())),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Ends(Ends::Source(TORegister::Write(Multiplicity::OneOrMany))),
            ),
            UWGraph::UpdateArc(
                "manufacturer",
                "wt",
                "repairs",
                Relation::Ends(Ends::Target(TORegister::Write(Multiplicity::ZeroOrMany))),
            ),
            // Remove ops
            UWGraph::RemoveVertex("wt"),
            UWGraph::RemoveVertex("eg"),
            UWGraph::RemoveVertex("rotor"),
            UWGraph::RemoveVertex("blade"),
            UWGraph::RemoveVertex("tower"),
            UWGraph::RemoveVertex("nacelle"),
            UWGraph::RemoveVertex("energy_grid"),
            UWGraph::RemoveVertex("manufacturer"),
            UWGraph::RemoveArc("wt", "eg", "ext"),
            UWGraph::RemoveArc("eg", "energy_grid", "feedsInto"),
            UWGraph::RemoveArc("energy_grid", "energy_grid", "connectedTo"),
            UWGraph::RemoveArc("rotor", "wt", "hasRotor"),
            UWGraph::RemoveArc("nacelle", "wt", "hasNacelle"),
            UWGraph::RemoveArc("blade", "rotor", "comprises"),
            UWGraph::RemoveArc("tower", "wt", "mountedOn"),
            UWGraph::RemoveArc("manufacturer", "wt", "owns"),
            UWGraph::RemoveArc("manufacturer", "wt", "repairs"),
            // --- DOUBLED OPERATIONS BELOW ---
            // Add more operations, same as above, but with slight variations for diversity

            // WindTurbine - add new operation and feature
            UWGraph::UpdateVertex(
                "wt",
                Class::Operations(UWMap::Update(
                    "restart".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            UWGraph::UpdateVertex(
                "wt",
                Class::Features(UWMap::Update(
                    "serialNumber".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // EnergyGenerator - add new operation
            UWGraph::UpdateVertex(
                "eg",
                Class::Operations(UWMap::Update(
                    "reset".to_string(),
                    Operation::ReturnType(MVRegister::Write(TypeRef::Primitive(
                        PrimitiveType::Void,
                    ))),
                )),
            ),
            // Rotor - add new feature
            UWGraph::UpdateVertex(
                "rotor",
                Class::Features(UWMap::Update(
                    "material".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Blade - add new feature
            UWGraph::UpdateVertex(
                "blade",
                Class::Features(UWMap::Update(
                    "length".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            // Tower - add new feature
            UWGraph::UpdateVertex(
                "tower",
                Class::Features(UWMap::Update(
                    "foundationDepth".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            // Nacelle - add new feature
            UWGraph::UpdateVertex(
                "nacelle",
                Class::Features(UWMap::Update(
                    "manufacturer".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // EnergyGrid - add new feature
            UWGraph::UpdateVertex(
                "energy_grid",
                Class::Features(UWMap::Update(
                    "region".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Manufacturer - add new feature
            UWGraph::UpdateVertex(
                "manufacturer",
                Class::Features(UWMap::Update(
                    "country".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            // Add new arcs for more relations
            UWGraph::UpdateArc(
                "wt",
                "energy_grid",
                "supplies",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "wt",
                "energy_grid",
                "supplies",
                Relation::Label(MVRegister::Write("supplies".to_string())),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "blade",
                "contains",
                Relation::Typ(TORegister::Write(RelationType::Aggregates)),
            ),
            UWGraph::UpdateArc(
                "rotor",
                "blade",
                "contains",
                Relation::Label(MVRegister::Write("contains".to_string())),
            ),
            UWGraph::UpdateArc(
                "tower",
                "manufacturer",
                "builtBy",
                Relation::Typ(TORegister::Write(RelationType::Associates)),
            ),
            UWGraph::UpdateArc(
                "tower",
                "manufacturer",
                "builtBy",
                Relation::Label(MVRegister::Write("builtBy".to_string())),
            ),
            // Remove the new arcs
            UWGraph::RemoveArc("wt", "energy_grid", "supplies"),
            UWGraph::RemoveArc("rotor", "blade", "contains"),
            UWGraph::RemoveArc("tower", "manufacturer", "builtBy"),
            // Remove the new features/vertices (simulate deletions)
            UWGraph::RemoveVertex("energy_grid"),
            UWGraph::RemoveVertex("manufacturer"),
            UWGraph::RemoveVertex("nacelle"),
            UWGraph::RemoveVertex("tower"),
            UWGraph::RemoveVertex("blade"),
            UWGraph::RemoveVertex("rotor"),
            UWGraph::RemoveVertex("eg"),
            UWGraph::RemoveVertex("wt"),
        ];

        let config = EventGraphConfig {
            name: "wind_turbine_class_diagram",
            num_replicas: 8,
            num_operations: 10_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.4,
            reachability: None,
            compare: |a: &ClassDiagram, b: &ClassDiagram| petgraph::algo::is_isomorphic(a, b),
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<ClassDiagramCrdt>(config);
    }

    #[cfg(feature = "op_weaver")]
    #[test_log::test]
    fn class_diagram_lot_of_iterations() {
        for i in 0..1_000 {
            use log::info;

            info!("Completed: {:.2}%", (i as f64 / 1_000.0) * 100.0);
            op_weaver_class_diagram();
        }
    }
}
