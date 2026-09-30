//! ST.06, C2 (`cea-cdrt-knowledge/spec/items/ST.06.md`): the same binary as
//! `empty_impl.rs`, with `Relay`'s `OptionalContent` impl stating its body:
//! it hands the removal to what it wraps, as `Box` and `BoxedLog` do. It must
//! build, before and after the change, and every replica must serve the
//! rule's document through it; this is what tells `empty_impl.rs`'s refusal
//! apart from a probe that does not build for any other reason. Exits 0 when
//! every replica serves the rule's document on every schedule, 1 otherwise.

#[macro_use]
#[path = "../st02_held/composition.rs"]
mod composition;
#[path = "relay.rs"]
mod relay;

use composition::*;
use moirai_crdt::option::OptionalContent;
use moirai_protocol::clock::version_vector::Version;
pub use relay::Relay;

impl<L: OptionalContent> OptionalContent for Relay<L> {
    fn redundant_by_optional(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_optional(version, conservative);
    }
}

composition!(relayed, Relay, same, Id, id_ref);

fn main() {
    let mut failures = Vec::new();
    for threads in [false, true] {
        let (found, runs) = relayed::run(threads);
        println!(
            "ST06-C2 explicit body, threads {threads}: runs {runs}, replicas serving another \
             value {} (ST.01 Flat {}, ST.01 Outer {}, claim A {})",
            found.len(),
            by_place(&found, "St01Flat"),
            by_place(&found, "St01Outer"),
            by_place(&found, "ClaimA")
        );
        failures.extend(found);
    }
    for line in failures.iter().take(2) {
        println!("ST06-C2 {line}");
    }
    std::process::exit(if failures.is_empty() { 0 } else { 1 });
}
