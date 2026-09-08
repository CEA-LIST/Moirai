//! The read-out: the canonical form of `02 Validation Plan` §2, built
//! directly rather than projected onto afterwards.
//!
//! # Why this shape
//!
//! The generated path's `Read` value is a nested struct: an inherited feature
//! under a `<super>_super` field, a family as a `union!` wrapper of `Unset`,
//! `Value` or `Conflict`, an optional as an `Option`, a text attribute as a
//! `Vec<char>`. The equivalence oracle of step 5 has to compare it with this
//! one, and a comparison is only fair if both sides are normalised to a form
//! neither of them is native in. Section 2 of the validation plan is that
//! form, so this module produces it and the oracle projects the generated
//! side onto it:
//!
//! - an object is a JSON object whose keys are the features visible on its
//!   class, own and inherited alike, with `eClass` naming the class;
//! - a family wrapper is the contained object directly, `Unset` an absent
//!   key, a conflict set `{"__conflict": [...]}` ordered by class name and
//!   then by canonical bytes;
//! - a text attribute is a string, characters joined;
//! - an unset optional is an absent key;
//! - a sequence is an array in read order;
//! - a keyed collection is a JSON object, its keys the map's own keys in key
//!   order, and an entry whose value reads as its default is absent, which is
//!   `UWMapLog::execute_query`'s rule (`uw_map.rs:199-210`) and therefore how
//!   both paths spell an entry that has been removed;
//! - an instance of a **transparent** class is the value of the one feature
//!   the class is represented by, with no `eClass` key and no object around
//!   it: `json.ecore`'s `Array` is the array itself and its `Object` is the
//!   JSON object itself, which is what makes a model under `json.ecore` read
//!   out as a JSON document rather than as a description of one;
//! - a counter is a number, a flag a boolean, a register its value or a
//!   conflict object;
//! - keys are sorted, which `serde_json::Map` does on its own because it is a
//!   `BTreeMap` unless someone turns `preserve_order` on. A test below fails
//!   if anyone does.
//!
//! # Names here, slots on the delivery path
//!
//! This module walks names, and that is fine: it runs when a reader asks and
//! never when an operation arrives. The rule the implementation plan states
//! is the other half of the same sentence — a read-out that walks names mixed
//! into an `effect` that walks names is what would make the delivery path pay
//! for the read-out's convenience — and [`crate::node`] is where that is kept
//! to slots.
//!
//! # A feature nobody has written
//!
//! reads exactly as one written and then emptied: an unwritten text is `""`,
//! an unwritten counter `0`, an unwritten sequence `[]`. The node was never
//! minted, so the value comes from minting one and reading it, which is one
//! allocation on a path that runs per read rather than per operation, and
//! which cannot drift from what a minted-and-emptied feature reads.

use moirai_protocol::crdt::{eval::EvalNested, query::Read};
use moirai_semantics::MetamodelSemantics;
use serde_json::{Map, Value};

use crate::log::ModelLog;
use crate::node::{Node, ObjectNode, Shaped, Site, Target, shaped, visible};

/// The key a conflict set is carried under.
pub const CONFLICT: &str = "__conflict";

/// The key a class name is carried under.
pub const ECLASS: &str = "eClass";

/// The whole model, in canonical form.
///
/// `null` for a model whose root has not been written yet, which is the one
/// state a JSON document cannot otherwise spell.
pub fn read(sem: &MetamodelSemantics, root: &Node) -> Value {
    read_node(sem, root, Shaped::Bare(Site::Object(Target::Roots))).unwrap_or(Value::Null)
}

/// What a feature nobody has written reads as: whatever one minted from the
/// same rule and left alone reads as.
fn read_absent(sem: &MetamodelSemantics, shape: Shaped) -> Option<Value> {
    read_node(sem, &Node::for_shaped(shape), shape)
}

/// One node, or `None` where the canonical form has no key at all.
pub(crate) fn read_node(sem: &MetamodelSemantics, node: &Node, shape: Shaped) -> Option<Value> {
    match (shape, node) {
        (_, Node::Unbound) => None,
        (Shaped::Sequence(site), Node::Seq(seq)) => {
            let mut items = Vec::new();
            for id in seq.order() {
                // Only what is there: an id in the ordering with no child
                // behind it is a hole, and a hole is nothing, not a default.
                if let Some(child) = seq.children().get(&id)
                    && let Some(value) = read_node(sem, child, Shaped::Bare(site))
                {
                    items.push(value);
                }
            }
            Some(Value::Array(items))
        }
        (Shaped::Keyed(site), Node::Map(map)) => {
            // `uw_map.rs:199-210`: a child is rendered only when its value
            // differs from the default of its own log, and it must be, because
            // `UWMap::Remove` is not a tombstone — it leaves the child in the
            // map and resets it, so reading as the default *is* how a keyed
            // collection spells removed. The comparison is against what a
            // freshly minted child at this site reads, computed once.
            //
            // It is deliberately not `Node::is_default`. A `String` at a key,
            // removed, reads `""` on both paths: `redundant_by_parent` on an
            // `EventGraph` leaves the reset behind, so neither log is at its
            // default and neither read-out drops the key. A `Number` at a key,
            // removed, reads `0` on both paths for the same reason the
            // generated union does — `JsonKindValue::Value(Number(0))` is not
            // `JsonKindValue::Unset`. Only a leaf-valued map, whose child log
            // really does empty, loses the key.
            let default = read_absent(sem, Shaped::Bare(site));
            let mut entries = Map::new();
            for (key, child) in map.children() {
                let value = read_node(sem, child, Shaped::Bare(site));
                if value == default {
                    continue;
                }
                if let Some(value) = value {
                    entries.insert(key.to_key(Some(sem)), value);
                }
            }
            Some(Value::Object(entries))
        }
        (Shaped::Optional(site), Node::Opt(opt)) => opt
            .child()
            .and_then(|child| read_node(sem, child, Shaped::Bare(site))),
        (Shaped::Bare(Site::Leaf(_)), Node::Leaf(leaf)) => Some(leaf.read_json(Some(sem))),
        (Shaped::Bare(Site::Object(_)), Node::Slot(slot)) => {
            let objects: Vec<&ObjectNode> = slot.objects();
            match objects.len() {
                0 => None,
                // `union.rs`'s `Value` branch reads whatever is there,
                // default or not; only its `Conflicts` branch drops the
                // empty ones. Copied, so a model with one empty object in a
                // slot reads the same on both paths.
                1 => read_object(sem, objects[0]),
                _ => {
                    let mut values: Vec<(String, Vec<u8>, Value)> = objects
                        .iter()
                        .filter(|object| !object_is_empty(object))
                        .filter_map(|object| {
                            let value = read_object(sem, object)?;
                            let name = sem
                                .classes
                                .get(object.class().index())
                                .map_or_else(String::new, |class| class.name.to_string());
                            let bytes = serde_json::to_vec(&value).unwrap_or_default();
                            Some((name, bytes, value))
                        })
                        .collect();
                    match values.len() {
                        0 => None,
                        1 => Some(values.pop().expect("just counted").2),
                        _ => {
                            // Ordered by class name and then by canonical
                            // bytes, so two replicas holding the same
                            // conflict print it identically.
                            values.sort_by(|left, right| {
                                left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
                            });
                            let mut object = Map::new();
                            object.insert(
                                CONFLICT.to_string(),
                                Value::Array(
                                    values.into_iter().map(|(_, _, value)| value).collect(),
                                ),
                            );
                            Some(Value::Object(object))
                        }
                    }
                }
            }
        }
        // A node of another shape than its rule says is a state no operation
        // can build; reading it as absent is the answer that cannot panic.
        _ => None,
    }
}

/// Whether an object holds nothing at all.
fn object_is_empty(object: &ObjectNode) -> bool {
    object.fields().values().all(Node::is_default)
}

/// One object: its class, then every feature its class can see.
///
/// `None` only for a **transparent** class whose one feature reads as no key
/// at all: an instance of such a class *is* that feature's value, so a
/// feature with nothing to show leaves nothing to show. Every other class
/// carries at least its `eClass`.
fn read_object(sem: &MetamodelSemantics, object: &ObjectNode) -> Option<Value> {
    let mut out = Map::new();
    let Some(class) = sem.classes.get(object.class().index()) else {
        return Some(Value::Object(out));
    };

    // A transparent class has no record of its own on the generated path:
    // `classifier/mod.rs:497-517` puts the field's own construction straight
    // into the union variant, so `JsonKind::Array` carries a
    // `NestedList<Box<JsonKind>>` and there is no `Array` object anywhere for
    // an `eClass` to name. The read-out says the same thing.
    if let Some(slot) = class.transparent {
        let (name, _, _) = class.visible.get(slot.index())?;
        let _ = name;
        let (_, rule) = visible(sem, object.class(), slot)?;
        let shape = shaped(rule).ok()?;
        return match object.fields().get(&slot) {
            Some(node) => read_node(sem, node, shape),
            None => read_absent(sem, shape),
        };
    }

    out.insert(ECLASS.to_string(), Value::String(class.name.to_string()));

    for (slot, (name, _owner, _declared)) in class.visible.iter().enumerate() {
        let slot = moirai_semantics::FeatureSlot(slot as u16);
        let Some((_, rule)) = visible(sem, object.class(), slot) else {
            continue;
        };
        // A feature the interpreted path does not merge carries no value and
        // says so by not being there.
        let Ok(shape) = shaped(rule) else {
            continue;
        };
        let value = match object.fields().get(&slot) {
            Some(node) => read_node(sem, node, shape),
            None => read_absent(sem, shape),
        };
        if let Some(value) = value {
            out.insert(name.to_string(), value);
        }
    }
    Some(Value::Object(out))
}

impl EvalNested<Read<Value>> for ModelLog {
    fn execute_query(&self, _q: Read<Value>) -> Value {
        match self.semantics() {
            Some(sem) => read(sem, self.root()),
            None => Value::Null,
        }
    }
}

#[cfg(feature = "test_utils")]
impl EvalNested<Read<Value>> for crate::testing::Harness {
    fn execute_query(&self, _q: Read<Value>) -> Value {
        read_node(&self.sem, &self.root, self.site()).unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    //! The canonical form, read out of models the tests above built, plus the
    //! two properties the equivalence oracle of step 5 will rest on: keys are
    //! sorted, and a conflict is ordered by class name and then by bytes.

    use moirai_protocol::{crdt::query::Read, replica::IsReplica};
    use serde_json::{Value, json};

    use crate::leaf::{LeafOp, Scalar};
    use crate::log::ModelLog;
    use crate::op::{InstanceOp, ModelOp};
    use crate::testing::{
        BT_DESCRIPTOR, JSON_DESCRIPTOR, bt, class_slot, feature_slot, json, mini, mini_descriptor,
        opened, twins,
    };

    fn bt_descriptor() -> Value {
        serde_json::from_str(BT_DESCRIPTOR).expect("the fixture is JSON")
    }

    fn read_of(log: &ModelLog) -> Value {
        moirai_protocol::state::log::IsLog::eval(log, Read::<Value>::new())
    }

    #[test]
    fn a_model_with_no_root_reads_as_null() {
        let (a, _b) = opened("m1", &mini_descriptor());
        assert_eq!(read_of(a.state()), Value::Null);
    }

    #[test]
    fn an_object_reads_as_its_class_and_every_feature_its_class_can_see() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let (mut a, _b) = opened("m1", &mini_descriptor());

        let title = InstanceOp::variant(
            root,
            InstanceOp::field(
                feature_slot(&sem, root, "title"),
                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'T' }),
            ),
        );
        let _ = a.send(ModelOp::Instance(title)).unwrap();

        assert_eq!(
            read_of(a.state()),
            json!({
                "eClass": "Root",
                "children": [],
                "title": "T"
            }),
            "an unset optional and an unset containment are absent keys; an \
             unwritten sequence is an empty array"
        );
    }

    #[test]
    fn a_sequence_reads_in_order_and_a_nested_object_carries_its_own_class() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let (mut a, _b) = opened("m1", &mini_descriptor());

        let add = |pos: usize, class: &str, ch: char| {
            ModelOp::Instance(InstanceOp::variant(
                root,
                InstanceOp::field(
                    feature_slot(&sem, root, "children"),
                    InstanceOp::insert(
                        pos,
                        InstanceOp::variant(
                            class_slot(&sem, class),
                            InstanceOp::field(
                                feature_slot(&sem, class_slot(&sem, class), "ID"),
                                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch }),
                            ),
                        ),
                    ),
                ),
            ))
        };
        let _ = a.send(add(0, "Sequence", 'a')).unwrap();
        let _ = a.send(add(1, "Fallback", 'b')).unwrap();

        assert_eq!(
            read_of(a.state())["children"],
            json!([
                {"eClass": "Sequence", "ID": "a", "children": []},
                {"eClass": "Fallback", "ID": "b"}
            ])
        );
    }

    #[test]
    fn a_conflict_reads_as_one_object_ordered_by_class_name() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let (mut a, mut b) = opened("m1", &mini_descriptor());

        let put = |class: &str, ch: char| {
            ModelOp::Instance(InstanceOp::variant(
                root,
                InstanceOp::field(
                    feature_slot(&sem, root, "main"),
                    InstanceOp::variant(
                        class_slot(&sem, class),
                        InstanceOp::field(
                            feature_slot(&sem, class_slot(&sem, class), "ID"),
                            InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch }),
                        ),
                    ),
                ),
            ))
        };

        let from_a = a.send(put("Sequence", 's')).unwrap();
        let from_b = b.send(put("Fallback", 'f')).unwrap();
        a.receive(from_b);
        b.receive(from_a);

        let expected = json!({
            "__conflict": [
                {"eClass": "Fallback", "ID": "f"},
                {"eClass": "Sequence", "ID": "s", "children": []}
            ]
        });
        assert_eq!(read_of(a.state())["main"], expected, "on a");
        assert_eq!(read_of(b.state())["main"], expected, "on b");
        assert_eq!(read_of(a.state()), read_of(b.state()));
    }

    #[test]
    fn an_optional_appears_only_once_it_is_set_and_empties_when_it_is_unset() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let (mut a, _b) = opened("m1", &mini_descriptor());

        let tag = |inner: InstanceOp| {
            ModelOp::Instance(InstanceOp::variant(
                root,
                InstanceOp::field(feature_slot(&sem, root, "tag"), inner),
            ))
        };

        let _ = a
            .send(tag(InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar {
                pos: 0,
                ch: 'x',
            }))))
            .unwrap();
        assert_eq!(read_of(a.state())["tag"], json!("x"));

        // Unsetting a *text* optional leaves the key there and empty, on
        // this path and on the generated one alike: `option/mod.rs:118-122`
        // drops the child only when it reads as default afterwards, and an
        // `EventGraph` that has been reset is not default — it holds the
        // reset. Section 2's "an unset optional is an absent key" is the
        // never-written case and the case where the child does empty, and
        // this is neither. Both paths agree, which is what I-A1 asks of it.
        let _ = a.send(tag(InstanceOp::unset())).unwrap();
        assert_eq!(read_of(a.state())["tag"], json!(""));

        // An optional nobody ever set is absent, which is the case section 2
        // names.
        assert_eq!(read_of(a.state()).get("main"), None);
    }

    #[test]
    fn an_optional_whose_child_empties_goes_away_entirely() {
        use crate::testing::{bench, twins};
        let sem = bench();
        let bench_class = class_slot(&sem, "Bench");
        let (mut a, _b) = twins(&sem, "Bench");

        let opt = |inner: InstanceOp| {
            InstanceOp::variant(
                bench_class,
                InstanceOp::field(feature_slot(&sem, bench_class, "optCounter"), inner),
            )
        };
        let _ = a
            .send(opt(InstanceOp::set(InstanceOp::Leaf(LeafOp::Inc(
                Scalar::Int(3),
            )))))
            .unwrap();
        assert_eq!(a.query::<Read<Value>>(Read::new())["optCounter"], json!(3));

        let _ = a.send(opt(InstanceOp::unset())).unwrap();
        assert_eq!(
            a.query::<Read<Value>>(Read::new()).get("optCounter"),
            None,
            "a counter that has been reset reads as default, so the optional              drops it and the key goes with it"
        );
    }

    #[test]
    fn keys_are_sorted_because_the_map_behind_them_is_ordered() {
        // A `serde_json::Map` is a `BTreeMap` unless someone turns
        // `preserve_order` on somewhere in the tree, and the equivalence
        // oracle compares canonical bytes. This is where that assumption
        // fails loudly rather than at a diff nobody can read.
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let (mut a, _b) = opened("m1", &mini_descriptor());
        let _ = a
            .send(ModelOp::Instance(InstanceOp::variant(
                root,
                InstanceOp::field(
                    feature_slot(&sem, root, "title"),
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'T' }),
                ),
            )))
            .unwrap();
        let bytes = serde_json::to_string(&read_of(a.state())).unwrap();
        assert_eq!(bytes, r#"{"children":[],"eClass":"Root","title":"T"}"#);
    }

    #[test]
    fn every_leaf_family_reads_as_the_json_the_projection_names() {
        use crate::testing::bench;
        let sem = bench();
        let bench_class = class_slot(&sem, "Bench");
        let (mut a, _b) = twins(&sem, "Bench");

        let write = |feature: &str, op: LeafOp| {
            InstanceOp::variant(
                bench_class,
                InstanceOp::field(
                    feature_slot(&sem, bench_class, feature),
                    InstanceOp::Leaf(op),
                ),
            )
        };

        for op in [
            write("counter", LeafOp::Inc(Scalar::Int(7))),
            write("flagEw", LeafOp::Enable),
            write("regLww", LeafOp::Write(Scalar::text("v"))),
            write("enumReg", LeafOp::Write(Scalar::Enum(0, 1))),
            write("setAw", LeafOp::Add(Scalar::text("b"))),
            write("setAw", LeafOp::Add(Scalar::text("a"))),
            write("bag", LeafOp::Add(Scalar::text("a"))),
            write("bag", LeafOp::Add(Scalar::text("a"))),
            write("text", LeafOp::InsertChar { pos: 0, ch: 'h' }),
        ] {
            let _ = a.send(op).unwrap();
        }

        let read: Value = a.query(Read::<Value>::new());
        assert_eq!(read["counter"], json!(7));
        assert_eq!(read["flagEw"], json!(true));
        assert_eq!(read["regLww"], json!("v"));
        assert_eq!(
            read["enumReg"],
            json!("SUCCESS"),
            "an enum literal reads by name, which is what the table is for"
        );
        assert_eq!(read["setAw"], json!(["a", "b"]));
        assert_eq!(read["bag"], json!(["a", "a"]));
        assert_eq!(read["text"], json!("h"));
        assert_eq!(read["regMv"], json!(null), "an unwritten register is null");
        assert_eq!(read["simple"], json!(0), "an unwritten counter is zero");
        assert_eq!(read["many"], json!([]), "an unwritten sequence is empty");
        assert_eq!(read.get("maybe"), None, "an unwritten optional is absent");
        assert_eq!(read.get("one"), None, "an unset containment is absent");
    }

    #[test]
    fn a_multi_value_register_holding_two_values_reads_as_a_conflict() {
        use crate::testing::bench;
        let sem = bench();
        let bench_class = class_slot(&sem, "Bench");
        let (mut a, mut b) = twins(&sem, "Bench");

        let write = |value: &str| {
            InstanceOp::variant(
                bench_class,
                InstanceOp::field(
                    feature_slot(&sem, bench_class, "regMv"),
                    InstanceOp::Leaf(LeafOp::Write(Scalar::text(value))),
                ),
            )
        };
        let from_a = a.send(write("left")).unwrap();
        let from_b = b.send(write("right")).unwrap();
        a.receive(from_b);
        b.receive(from_a);

        let read: Value = a.query(Read::<Value>::new());
        assert_eq!(read["regMv"], json!({"__conflict": ["left", "right"]}));
        assert_eq!(read, b.query(Read::<Value>::new()));
    }

    #[test]
    fn a_behaviour_tree_reads_out_of_the_descriptor_arachne_emits() {
        let sem = bt();
        let root = class_slot(&sem, "Root");
        let (mut a, mut b) = opened("m1", &bt_descriptor());

        let tree = class_slot(&sem, "BehaviorTree");
        let seq = class_slot(&sem, "Sequence");

        let add_tree = ModelOp::Instance(InstanceOp::variant(
            root,
            InstanceOp::field(
                feature_slot(&sem, root, "behaviortrees"),
                InstanceOp::insert(
                    0,
                    InstanceOp::variant(
                        tree,
                        InstanceOp::field(
                            feature_slot(&sem, tree, "ID"),
                            InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 't' }),
                        ),
                    ),
                ),
            ),
        ));
        let event = a.send(add_tree).unwrap();
        b.receive(event);

        let name_child = ModelOp::Instance(InstanceOp::variant(
            root,
            InstanceOp::field(
                feature_slot(&sem, root, "behaviortrees"),
                InstanceOp::at(
                    0,
                    InstanceOp::variant(
                        tree,
                        InstanceOp::field(
                            feature_slot(&sem, tree, "child"),
                            InstanceOp::variant(
                                seq,
                                InstanceOp::field(
                                    feature_slot(&sem, seq, "ID"),
                                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 's' }),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ));
        let event = a.send(name_child).unwrap();
        b.receive(event);

        let read: Value = a.query(Read::<Value>::new());
        assert_eq!(read["eClass"], json!("Root"));
        let tree_value = &read["behaviortrees"][0];
        assert_eq!(tree_value["eClass"], json!("BehaviorTree"));
        assert_eq!(tree_value["ID"], json!("t"));
        assert_eq!(
            tree_value["child"],
            json!({
                "eClass": "Sequence",
                "ID": "s",
                "children": []
            }),
            "`ID` is `TreeNode`'s and `children` is `ControlNode`'s, and both              sit flat on the instance with no `_super` hop between them"
        );
        assert_eq!(read, b.query(Read::<Value>::new()));
    }

    // ------------------------------------------------------------- json.ecore

    fn json_descriptor() -> Value {
        serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
    }

    /// One entry of a JSON object: `Object.entry` at `key`, holding an
    /// instance of `class` with `inner` written into the field that class is
    /// represented by.
    fn put(sem: &moirai_semantics::MetamodelSemantics, key: &str, class: &str, inner: InstanceOp) -> ModelOp {
        let object = class_slot(sem, "Object");
        let made = class_slot(sem, class);
        ModelOp::Instance(InstanceOp::variant(
            object,
            InstanceOp::field(
                feature_slot(sem, object, "entry"),
                InstanceOp::entry(
                    Scalar::text(key),
                    InstanceOp::variant(
                        made,
                        InstanceOp::field(feature_slot(sem, made, transparent_field(class)), inner),
                    ),
                ),
            ),
        ))
    }

    /// The feature each of `json.ecore`'s transparent classes is represented
    /// by, spelled out so a test reads like the metamodel.
    fn transparent_field(class: &str) -> &'static str {
        match class {
            "Array" => "items",
            "Object" => "entry",
            _ => "value",
        }
    }

    /// **The whole point of the metamodel** — a model under `json.ecore`
    /// reads out as a JSON document and not as a description of one. There is
    /// no `eClass` anywhere, no `items` key around the array and no `value`
    /// key around the string: every concrete class is transparent, so each
    /// instance *is* the value of its one feature, and `Object.entry` is a
    /// keyed collection whose keys are the document's own keys.
    #[test]
    fn a_json_document_reads_out_as_a_json_document() {
        let sem = json();
        let (mut a, mut b) = opened("m1", &json_descriptor());

        let array = class_slot(&sem, "Array");
        let string = class_slot(&sem, "String");
        let items = feature_slot(&sem, array, "items");
        let value = feature_slot(&sem, string, "value");

        for op in [
            put(&sem, "name", "String", InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'a' })),
            put(&sem, "name", "String", InstanceOp::Leaf(LeafOp::InsertChar { pos: 1, ch: 'b' })),
            put(&sem, "ok", "Boolean", InstanceOp::Leaf(LeafOp::Enable)),
            put(&sem, "n", "Number", InstanceOp::Leaf(LeafOp::Inc(Scalar::float(3.5)))),
            put(
                &sem,
                "list",
                "Array",
                InstanceOp::insert(
                    0,
                    InstanceOp::variant(
                        string,
                        InstanceOp::field(
                            value,
                            InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'x' }),
                        ),
                    ),
                ),
            ),
        ] {
            let event = a.send(op).unwrap();
            b.receive(event);
        }
        let _ = items;

        assert_eq!(
            read_of(a.state()),
            json!({"list": ["x"], "n": 3.5, "name": "ab", "ok": true}),
            "a transparent class is its one field, and a keyed containment is \
             a JSON object"
        );
        assert_eq!(read_of(a.state()), read_of(b.state()));
    }

    /// A removal is update-wins, exactly as `uw_map.rs:150-152` makes it: the
    /// entry is not dropped, its subtree is reset against the removal's own
    /// version, so what is causally below the removal goes and what is
    /// concurrent with it stays.
    ///
    /// This is `moirai-crdt`'s own `concurrent_uw_map` on the interpreted
    /// path, with a counter under a key instead of a counter under a key.
    #[test]
    fn a_removed_key_keeps_a_concurrent_write_and_loses_a_causally_prior_one() {
        let sem = json();
        let (mut a, mut b) = opened("m1", &json_descriptor());

        let bump = |by: f64| put(&sem, "k", "Number", InstanceOp::Leaf(LeafOp::Inc(Scalar::float(by))));
        let object = class_slot(&sem, "Object");
        let remove = ModelOp::Instance(InstanceOp::variant(
            object,
            InstanceOp::field(
                feature_slot(&sem, object, "entry"),
                InstanceOp::remove(Scalar::text("k")),
            ),
        ));

        // Causally below the removal.
        let event = a.send(bump(1.0)).unwrap();
        b.receive(event);
        assert_eq!(read_of(a.state()), json!({"k": 1.0}));

        // Concurrent with it.
        let from_a = a.send(remove).unwrap();
        let from_b = b.send(bump(10.0)).unwrap();
        a.receive(from_b);
        b.receive(from_a);

        assert_eq!(
            read_of(a.state()),
            json!({"k": 10.0}),
            "the concurrent increment survives its own removal; the earlier one does not"
        );
        assert_eq!(read_of(a.state()), read_of(b.state()));
    }

    /// A key removed with nothing concurrent stays, holding the empty string,
    /// which is what the generated path does too and is therefore the answer
    /// I-A1 asks for rather than the one that reads nicer.
    ///
    /// `UWMap::Remove` resets the child instead of dropping it
    /// (`uw_map.rs:150-152`), and `redundant_by_parent` on an `EventGraph`
    /// leaves the reset in the log, so the child is not at its default on
    /// either path: `UWMapLog::execute_query` keeps it because
    /// `JsonKindValue::Value(String([]))` is not `JsonKindValue::Unset`, and
    /// this read-out keeps it because `Some("")` is not the `None` an unwritten
    /// entry reads. The same sentence is already written about an unset
    /// optional text, six tests up.
    #[test]
    fn a_key_removed_with_nothing_concurrent_keeps_the_emptied_entry() {
        let sem = json();
        let (mut a, mut b) = opened("m1", &json_descriptor());
        let object = class_slot(&sem, "Object");

        let event = a
            .send(put(
                &sem,
                "gone",
                "String",
                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'z' }),
            ))
            .unwrap();
        b.receive(event);

        let event = a
            .send(ModelOp::Instance(InstanceOp::variant(
                object,
                InstanceOp::field(
                    feature_slot(&sem, object, "entry"),
                    InstanceOp::remove(Scalar::text("gone")),
                ),
            )))
            .unwrap();
        b.receive(event);

        assert_eq!(read_of(a.state()), json!({"gone": ""}));
        assert_eq!(read_of(b.state()), read_of(a.state()));
    }

    /// Two writers who put different JSON kinds at one key open a conflict
    /// there and both survive it, which is `union.rs:180-200`'s retention
    /// reached through a map rather than through a record field.
    #[test]
    fn two_kinds_written_at_one_key_are_both_kept() {
        let sem = json();
        let (mut a, mut b) = opened("m1", &json_descriptor());

        let from_a = a
            .send(put(
                &sem,
                "x",
                "String",
                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 's' }),
            ))
            .unwrap();
        let from_b = b
            .send(put(&sem, "x", "Number", InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(7)))))
            .unwrap();
        a.receive(from_b);
        b.receive(from_a);

        assert_eq!(
            read_of(a.state()),
            json!({"x": {"__conflict": [7.0, "s"]}}),
            "ordered by class name, `Number` before `String`"
        );
        assert_eq!(read_of(a.state()), read_of(b.state()));
    }
}
