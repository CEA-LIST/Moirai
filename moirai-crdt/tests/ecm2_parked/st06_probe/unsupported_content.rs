//! ST.06, C3, a guard (`cea-cdrt-knowledge/spec/items/ST.06.md`): an
//! `OptionLog` composes only over a log that implements `OptionalContent`,
//! so over a keyed map or a list it does not compile. Cam's decision
//! (2026-09-30) documents this as unsupported and does not fix it; this
//! binary must keep failing to build, and only on those two unmet bounds.

use moirai_crdt::counter::resettable_counter::Counter;
use moirai_crdt::list::nested_list::NestedListLog;
use moirai_crdt::map::uw_map::UWMapLog;
use moirai_crdt::option::OptionLog;
use moirai_crdt::utils::membership::twins_log;
use moirai_protocol::state::po_log::VecLog;

type Counted = VecLog<Counter<i32>>;

fn main() {
    let _keyed = twins_log::<OptionLog<UWMapLog<String, Counted>>>();
    let _list = twins_log::<OptionLog<NestedListLog<Counted>>>();
}
