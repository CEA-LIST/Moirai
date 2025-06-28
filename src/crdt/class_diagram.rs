use std::ops::{Add, AddAssign, SubAssign};

use petgraph::{
    dot::{Config, Dot},
    graph::DiGraph,
};

use crate::{
    crdt::{
        lww_register::LWWRegister, mv_register::MVRegister, uw_map::UWMapLog,
        uw_multigraph::UWGraphLog,
    },
    object,
    protocol::event_graph::EventGraph,
};

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub enum RelationType {
    Extends,    // 1
    Implements, // 4
    Aggregates, // 3
    Composes,   // 2
    #[default]
    Associates, // 0
}

// Multiplicity : from - to
// Extends: 1 - 1
// Implements: 1 - 1
// Composes: 1 - *
// Aggregates: 1 - *
// Associates: * - *

#[derive(Debug, Clone, Eq, Default, PartialEq, Hash)]
pub enum PrimitiveType {
    String,
    Number,
    Bool,
    #[default]
    Void,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Visibility {
    #[default]
    Public,
    Private,
    Protected,
    Package,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Multiplicity<V: Add + AddAssign + SubAssign + Default + Copy> {
    #[default]
    Unspecified,
    One,
    ZeroOrOne,
    ZeroOrMany,
    OneOrMany,
    ManyToMany(V, V), // (min, max) with max >= min
}

object!(Feature {
    // replace typ with enum Multiplicity { Collection, Scalar }
    typ: EventGraph::<MVRegister::<PrimitiveType>>,
    visibility: EventGraph::<LWWRegister::<Visibility>>,
});

object!(Operation {
    is_abstract: EventGraph::<LWWRegister::<bool>>,
    visibility: EventGraph::<LWWRegister::<Visibility>>,
    parameters: UWMapLog::<String, EventGraph::<MVRegister::<PrimitiveType>>>,
    return_type: EventGraph::<MVRegister::<PrimitiveType>>,
});

object!(Class {
    is_abstract: EventGraph::<LWWRegister::<bool>>,
    name: EventGraph::<MVRegister::<String>>,
    features: UWMapLog::<String, FeatureLog>,
    operations: UWMapLog::<String, OperationLog>,
});

object!(Relation {
    label: EventGraph::<MVRegister::<String>>,
    relation_type: EventGraph::<LWWRegister::<RelationType>>,
});

pub type ClassDiagramCrdt<'a> = UWGraphLog<&'a str, &'a str, ClassLog, RelationLog>;
pub type ClassDiagram = DiGraph<ClassValue, RelationValue>;

pub fn export_fancy_class_diagram(graph: &ClassDiagram) -> String {
    let fancy_dot = Dot::with_attr_getters(
        graph,
        &[Config::EdgeNoLabel, Config::NodeNoLabel],
        &|_g, edge| {
            let label = &edge
                .weight()
                .label
                .iter()
                .cloned()
                .collect::<Vec<String>>()
                .join(", ");
            let rtype = &edge.weight().relation_type;
            let (head, style) = match rtype {
                RelationType::Extends => ("empty", "normal"),
                RelationType::Implements => ("empty", "dashed"),
                RelationType::Aggregates => ("odiamond", "normal"),
                RelationType::Composes => ("diamond", "normal"),
                RelationType::Associates => ("normal", "normal"),
            };
            format!(
                "label=\"{}\", arrowhead=\"{}\", style=\"{}\"",
                label, head, style
            )
        },
        &|_g, (_, class)| {
            let name_vec: Vec<String> = class.name.iter().cloned().collect();
            let name = if name_vec.is_empty() {
                "Unnamed".to_string()
            } else {
                name_vec.join(", ")
            };
            let features = class
                .features
                .iter()
                .map(|(k, v)| {
                    let feature_name = k.clone();
                    let types: Vec<String> =
                        v.typ.iter().cloned().map(|t| format!("{:?}", t)).collect();
                    let feature_type = types.join(",");
                    let feature_vis = match v.visibility {
                        Visibility::Public => "+",
                        Visibility::Private => "-",
                        Visibility::Protected => "#",
                        Visibility::Package => "~",
                    };
                    format!("{}{}: {}", feature_vis, feature_name, feature_type)
                })
                .collect::<Vec<String>>()
                .join("\\l");
            let operations = class
                .operations
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
                            format!("{}: {}", p, types.join("|"))
                        })
                        .collect();
                    let return_types: Vec<String> = v
                        .return_type
                        .iter()
                        .cloned()
                        .map(|t| format!("{:?}", t))
                        .collect();
                    let return_type_str = if return_types.is_empty() {
                        "".to_string()
                    } else {
                        format!(": {}", return_types.join("|"))
                    };
                    format!(
                        "{}{}({}){}",
                        op_vis,
                        op_name,
                        params.join(", "),
                        return_type_str
                    )
                })
                .collect::<Vec<String>>()
                .join("\\l");
            let is_abstract = class.is_abstract;
            if is_abstract {
                format!(
                    "label=\"{{{}|{}\\l|{}\\l}}\", fontname=\"Helvetica-Oblique\"",
                    name, features, operations
                )
            } else {
                format!("label=\"{{{}|{}\\l|{}\\l}}\"", name, features, operations)
            }
        },
    );
    let mut fancy_string = format!("{:?}", fancy_dot);
    fancy_string = fancy_string.replace(
        "digraph {",
        "digraph {\n    rankdir=TB\n    node [shape=record, fontname=\"Helvetica\", fontsize=10]\n    edge [fontname=\"Helvetica\", fontsize=10]\n",
    );
    fancy_string
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{test_util::twins, uw_map::UWMap, uw_multigraph::UWGraph};

    #[test_log::test]
    fn wind_turbine_diagram() {
        // @startuml
        // abstract class EnergyGenerator {
        // + getEnergyOutput(): Number
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

        // '--------------------------
        // ' Inheritance (extension)
        // '--------------------------
        // EnergyGenerator <|-- WindTurbine

        // '--------------------------
        // ' Composition relationships
        // '--------------------------
        // WindTurbine "1" o-- "1" Rotor        : hasRotor
        // Rotor       "1" o-- "3" Blade        : comprises
        // WindTurbine "1" o-- "1" Nacelle      : hasNacelle
        // WindTurbine "1" o-- "1" Tower        : mountedOn

        // '--------------------------
        // ' Associations
        // '--------------------------
        // EnergyGenerator "1" --> "1..*" EnergyGrid : feedsInto
        // EnergyGrid "0..*" -- "0..*" EnergyGrid : connectedTo

        // Manufacturer "1" --> "0..*" WindTurbine : owns
        // Manufacturer "1..*" --> "0..*" WindTurbine : repairs
        // @enduml

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
                Operation::ReturnType(MVRegister::Write(PrimitiveType::Void)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "wt",
            Class::Operations(UWMap::Update(
                "shutdown".to_string(),
                Operation::ReturnType(MVRegister::Write(PrimitiveType::Void)),
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
                Operation::ReturnType(MVRegister::Write(PrimitiveType::Number)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateVertex(
            "eg",
            Class::IsAbstract(LWWRegister::Write(true)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "wt",
            "eg",
            "ext",
            Relation::RelationType(LWWRegister::Write(RelationType::Extends)),
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
                Feature::Visibility(LWWRegister::Write(Visibility::Private)),
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
            Relation::RelationType(LWWRegister::Write(RelationType::Composes)),
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
            Relation::RelationType(LWWRegister::Write(RelationType::Aggregates)),
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
            Relation::RelationType(LWWRegister::Write(RelationType::Aggregates)),
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
                Feature::Visibility(LWWRegister::Write(Visibility::Private)),
            )),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "nacelle",
            "wt",
            "hasNacelle",
            Relation::RelationType(LWWRegister::Write(RelationType::Aggregates)),
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
            Relation::RelationType(LWWRegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "eg",
            "energy_grid",
            "feedsInto",
            Relation::Label(MVRegister::Write("feedsInto".to_string())),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "energy_grid",
            "energy_grid",
            "connectedTo",
            Relation::RelationType(LWWRegister::Write(RelationType::Associates)),
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
            Relation::RelationType(LWWRegister::Write(RelationType::Associates)),
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
            "repairs",
            Relation::RelationType(LWWRegister::Write(RelationType::Associates)),
        ));
        let _ = tcsb_a.tc_bcast(UWGraph::UpdateArc(
            "manufacturer",
            "wt",
            "repairs",
            Relation::Label(MVRegister::Write("repairs".to_string())),
        ));

        println!(
            "Class Diagram A: {}",
            export_fancy_class_diagram(&tcsb_a.eval())
        );
    }
}
