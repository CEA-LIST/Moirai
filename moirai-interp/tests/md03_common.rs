//! Shared MD.03 oracle data. No production implementation lives here.
#![allow(dead_code)]
#[path = "si05/mod.rs"]
pub mod si05;
use moirai_interp::testing::install;
use moirai_protocol::{log_id::LogId, replica::IsReplica};
use serde_json::{Value, json};
pub use si05::*;

pub const MODEL: &str = "03030303030303030303030303030303";
pub const FOREIGN_MODEL: &str = "cccccccccccccccccccccccccccccccc_components";
pub const LIBRARY: &str = "sysml:DataType kermllibrary:///00000000-0000-4000-8000-000000000001#00000000-0000-4000-8000-000000000002";
pub const SYSML_LIBRARY: &str = "sysml:PartDefinition sysmllibrary:///00000000-0000-4000-8000-000000000003#00000000-0000-4000-8000-000000000004";
pub const NAMES: [&str; 3] = ["alice", "bob", "carol"];

pub fn foreign(identity: &str) -> String {
    format!("{FOREIGN_MODEL}#{identity}")
}
pub fn values(items: &[&str]) -> std::collections::BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}
pub fn replicas() -> [Rep; 3] {
    let mut reps =
        NAMES.map(|id| Rep::bootstrap_with_log_id(id.into(), &NAMES, LogId::parse(MODEL).unwrap()));
    broadcast(
        &mut reps,
        0,
        vec![install(MODEL, &board_descriptor())],
        "Install",
    );
    reps
}
pub fn setup() -> (Board, [Rep; 3]) {
    let b = Board::new();
    let mut r = replicas();
    broadcast(&mut r, 0, structure(&b), "source and target");
    (b, r)
}
pub fn structure(b: &Board) -> Vec<moirai_interp::ModelOp> {
    let mut ops = vec![b.root(), b.new_card(0), b.new_card(1)];
    ops.extend(b.title_ops(At::Board(0), "x"));
    ops.extend(b.title_ops(At::Board(1), "y"));
    ops
}
pub fn finish(r: &mut [Rep; 3], streams: &[(usize, Vec<Msg>)], order: [usize; 2]) {
    for index in order {
        let (from, stream) = &streams[index];
        for m in stream {
            deliver_all(r, *from, m);
        }
    }
    assert_eq!(identified(&r[0]), identified(&r[1]), "replicas diverged");
    assert_eq!(identified(&r[0]), identified(&r[2]), "replicas diverged");
}
pub fn revised() -> Value {
    let mut d = board_descriptor();
    let attrs = d["classes"]["Card"]["attributes"].as_array_mut().unwrap();
    let note = attrs.iter_mut().find(|a| a["name"] == "note").unwrap();
    note["merge"]["leaf"] = json!({"kind":"register", "tie":"fair"});
    d
}
pub fn assert_foreign(doc: &Value, wanted: &[&str], gate: &str) {
    assert_eq!(
        refs(doc, At::Board(0), "related"),
        values(wanted),
        "{gate}: foreign bytes missing or rewritten: {doc}"
    );
}
