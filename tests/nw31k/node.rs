//! NW.31k round 2 (vault spec/items/NW.31k.md): a node of moirai-netstack-crdt's
//! add-wins set of strings. Everything network is moirai-netstack-start's and
//! moirai-netstack-crdt's; this file is the whole application.
//! tests/nw31k/gate.sh builds it in a consuming workspace in scratch.

fn main() {
    if moirai_netstack_start::run(moirai_netstack_crdt::awset::factory(), ()).is_err() {
        std::process::exit(1);
    }
}
