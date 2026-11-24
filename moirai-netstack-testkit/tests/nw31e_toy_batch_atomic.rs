//! NW.31e oracle: the toy's apply_remote is all or nothing on an array (vault
//! spec/items/NW.31e.md). Node batches go through it, so a batch whose second
//! event is refused must leave the replica as it was.

use moirai_netstack::{ReplicaFactory, ReplicaPort};
use moirai_netstack_testkit::ToyFactory;
use serde_json::json;

#[test]
fn toy_apply_remote_on_an_array_keeps_nothing_when_one_event_is_refused() {
    let mut factory = ToyFactory;
    let members = ["r".to_string(), "w".to_string()];
    let mut writer = factory.open("bt", "w", &members);
    let mut stranger = factory.open("uml", "w", &members);
    let valid = writer.submit(json!("red")).expect("a string element");
    let foreign = stranger.submit(json!("blue")).expect("a string element");

    for (case, input) in [
        ("valid then foreign", json!([valid.clone(), foreign])),
        ("valid then malformed", json!([valid.clone(), 42])),
    ] {
        let mut reader = factory.open("bt", "r", &members);
        let before = (reader.read_state(), reader.progress(), reader.frontier());
        assert!(
            reader.apply_remote("w", input).is_err(),
            "{case}: the refused event was accepted"
        );
        assert_eq!(
            (reader.read_state(), reader.progress(), reader.frontier()),
            before,
            "{case}: the event before the refused one was kept"
        );
        assert!(
            !reader.has_history_from("w"),
            "{case}: the writer left history"
        );
    }

    // All valid, the array still applies whole.
    let second = writer.submit(json!("green")).expect("a string element");
    let mut reader = factory.open("bt", "r", &members);
    reader
        .apply_remote("w", json!([valid, second]))
        .expect("two valid events");
    assert_eq!(reader.read_state(), json!(["green", "red"]));
    assert_eq!(reader.progress().delivered, 2);
}
