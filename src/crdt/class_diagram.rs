use petgraph::{
    dot::{Config, Dot},
    graph::DiGraph,
};

use crate::{
    crdt::{
        aw_map::AWMapLog, lww_register::LWWRegister, mv_register::MVRegister,
        uw_multigraph::UWGraphLog,
    },
    object,
    protocol::event_graph::EventGraph,
};

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub enum RelationType {
    Extends,
    Implements,
    Aggregates,
    Composes,
    #[default]
    Associates,
}

#[derive(Debug, Clone, Eq, Default, PartialEq, Hash)]
pub enum PrimitiveType {
    String,
    Number,
    Bool,
    #[default]
    Null,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Visibility {
    #[default]
    Public,
    Private,
    Protected,
    Package,
}

object!(Feature {
    typ: EventGraph::<MVRegister::<PrimitiveType>>,
    visibility: EventGraph::<LWWRegister::<Visibility>>,
    // is_ordered: EventGraph::<Flag>,
    // is_unique: EventGraph::<Flag>,
});

object!(Class {
    // is_abstract: EventGraph::<Flag>,
    name: EventGraph::<MVRegister::<String>>,
    features: AWMapLog::<String, FeatureLog>,
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
            format!("label=\"{{{}|{}\\l}}\"", name, features)
        },
    );
    let mut fancy_string = format!("{:?}", fancy_dot);
    fancy_string = fancy_string.replace(
        "digraph {",
        "digraph {\n    rankdir=TB\n    node [shape=record, fontname=\"Helvetica\", fontsize=10]\n",
    );
    fancy_string
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::crdt::test_util::twins;

//     #[test_log::test]
//     fn car_class_diagram() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<ClassDiagramCrdt>();
//     }
// }
