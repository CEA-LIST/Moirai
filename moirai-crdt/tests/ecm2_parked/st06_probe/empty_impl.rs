//! ST.06, C1 (`cea-cdrt-knowledge/spec/items/ST.06.md`): the compositions of
//! ST.02's `composition.rs` with `Relay<L>` between each `OptionLog` and its
//! content, and `Relay`'s `OptionalContent` impl written with no body. The
//! claim is that this binary does not build: `redundant_by_optional` has no
//! default, so an impl that does not say what it does with a removal an
//! optional hands down is refused (E0046, naming that method alone). Where it
//! builds, `../st06_optional_content_required.rs` runs it and reports whether
//! the wrapper lost the rule, before failing on the build itself. Exits 0
//! when every replica serves the rule's document on every schedule, 1
//! otherwise.

#[macro_use]
#[path = "../st02_held/composition.rs"]
mod composition;
#[path = "relay.rs"]
mod relay;

use composition::*;
use moirai_crdt::option::OptionalContent;
pub use relay::Relay;

impl<L: OptionalContent> OptionalContent for Relay<L> {}

composition!(relayed, Relay, same, Id, id_ref);

fn main() {
    let mut failures = Vec::new();
    for threads in [false, true] {
        let (found, runs) = relayed::run(threads);
        println!(
            "ST06-C1 empty impl, threads {threads}: runs {runs}, replicas serving another value {} \
             (ST.01 Flat {}, ST.01 Outer {}, claim A {})",
            found.len(),
            by_place(&found, "St01Flat"),
            by_place(&found, "St01Outer"),
            by_place(&found, "ClaimA")
        );
        failures.extend(found);
    }
    for line in failures.iter().take(2) {
        println!("ST06-C1 {line}");
    }
    std::process::exit(if failures.is_empty() { 0 } else { 1 });
}
