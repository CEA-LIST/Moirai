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
    // is_abstract: EventGraph::<Flag>,
    visibility: EventGraph::<LWWRegister::<Visibility>>,
    parameters: UWMapLog::<String, EventGraph::<MVRegister::<PrimitiveType>>>,
    return_type: EventGraph::<MVRegister::<PrimitiveType>>,
});

object!(Class {
    // is_abstract: EventGraph::<Flag>,
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
                    format!("{}({}){}", op_name, params.join(", "), return_type_str)
                })
                .collect::<Vec<String>>()
                .join("\\l");
            format!("label=\"{{{}|{}\\l|{}\\l}}\"", name, features, operations)
        },
    );
    let mut fancy_string = format!("{:?}", fancy_dot);
    fancy_string = fancy_string.replace(
        "digraph {",
        "digraph {\n    rankdir=TB\n    node [shape=record, fontname=\"Helvetica\", fontsize=10]\n",
    );
    fancy_string
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{test_util::twins, uw_multigraph::UWGraph};

    #[test_log::test]
    fn car_class_diagram() {}
}
