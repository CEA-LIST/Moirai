//! NW.31j (vault spec/items/NW.31j.md): a node of the crate arachne-cli 3c8c2bf
//! generates from upstream Arachne's examples/json.ecore, serde on. Everything
//! network is moirai-netstack-start's and moirai-netstack-crdt's; this file is
//! the whole application. tests/nw31j/gate.sh builds it in a consuming
//! workspace in scratch.

use nw31j_json::package::{JsonLog, JsonValue};

fn main() {
    let factory = moirai_netstack_crdt::factory::<JsonLog, JsonValue>();
    if moirai_netstack_start::run(factory, ()).is_err() {
        std::process::exit(1);
    }
}
