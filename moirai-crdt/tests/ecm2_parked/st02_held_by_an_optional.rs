//! ST.02, claim B (`cea-cdrt-knowledge/spec/items/ST.02.md`): how the
//! generated runtime knows that a union or a record is held by an optional.
//!
//! - B0 (L0): the three library files the item may change keep no state
//!   outside the logs through which the calls of one reset could tell a
//!   union or a record who holds it: nothing thread-local, nothing
//!   process-global, and no runtime type identity.
//! - B1 (L2): ST.01's V1 and claim A's rule hold for the compositions
//!   Arachne writes (no wrapper, and `Box` and `BoxedLog`, which it writes
//!   for recursive metamodels), with every send, delivery and read on the
//!   test's thread and, again, each on a thread of its own.
//! - B3 (L0): `OptionLog`'s impls carry no `'static` bound, and neither do
//!   the other two files' items; the spec does not justify one.
//!
//! B2, a user wrapper, is `st02_wrapper_probe.rs`.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-crdt --test st02_held_by_an_optional -- --nocapture
//! ```

#[macro_use]
#[path = "st02_held/composition.rs"]
mod composition;

use composition::*;
use moirai_protocol::state::log::BoxedLog;

composition!(bare, Bare, same, Id, id_ref);
composition!(boxed, Box, Box::new, Id, id_ref);
composition!(boxed_log, BoxedLog, Box::new, Box, box_ref);

/// The three files, read from the checkout this test is built from.
fn library_files() -> Vec<(&'static str, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    [
        "src/option/mod.rs",
        "../moirai-macros/src/union.rs",
        "../moirai-macros/src/record.rs",
    ]
    .into_iter()
    .map(|path| {
        let text = std::fs::read_to_string(root.join(path))
            .unwrap_or_else(|error| panic!("set-up: reading {path}: {error}"));
        (path, text)
    })
    .collect()
}

/// Each line of code, with what follows `//` on it dropped.
fn code_lines(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.lines().enumerate().map(|(index, line)| {
        let code = match line.find("//") {
            Some(at) => &line[..at],
            None => line,
        };
        (index + 1, code)
    })
}

/// The words that name state outside the logs, or a runtime type identity.
const SIDE_CHANNELS: [&str; 13] = [
    "thread_local!",
    "LocalKey",
    "ThreadId",
    "thread::current",
    "static mut",
    "Mutex",
    "RwLock",
    "Atomic",
    "OnceLock",
    "LazyLock",
    "TypeId",
    "type_id",
    "downcast",
];

/// B0: none of [`SIDE_CHANNELS`] in the code of the three files.
#[test]
fn st02_b0_no_thread_local_state_or_type_id_decides_what_an_optional_holds() {
    let mut found = Vec::new();
    for (path, text) in library_files() {
        for (line, code) in code_lines(&text) {
            for word in SIDE_CHANNELS {
                if code.contains(word) {
                    found.push(format!("{path}:{line}: {}", code.trim()));
                }
            }
        }
    }
    println!(
        "ST02-B0 lines naming state outside the logs or a runtime type identity: {}",
        found.len()
    );
    for line in &found {
        println!("ST02-B0 {line}");
    }
    assert!(
        found.is_empty(),
        "ST.02 B0: {} lines of the library's optional, union and record name state outside \
         the logs or a runtime type identity:\n  {}",
        found.len(),
        found.join("\n  ")
    );
}

/// B3: no `'static` bound in the three files (`L: 'static`, `+ 'static`).
#[test]
fn st02_b3_option_log_asks_no_static_bound_of_its_content() {
    let mut found = Vec::new();
    for (path, text) in library_files() {
        for (line, code) in code_lines(&text) {
            let squeezed: String = code.split_whitespace().collect::<Vec<_>>().join(" ");
            if squeezed.contains(": 'static")
                || squeezed.contains("+ 'static")
                || squeezed.contains("'static +")
            {
                found.push(format!("{path}:{line}: {}", code.trim()));
            }
        }
    }
    println!("ST02-B3 lines with a 'static bound: {}", found.len());
    for line in &found {
        println!("ST02-B3 {line}");
    }
    assert!(
        found.is_empty(),
        "ST.02 B3: {} lines bound a type by 'static:\n  {}",
        found.len(),
        found.join("\n  ")
    );
}

/// B1: every schedule of the composition, for each wrapper Arachne writes,
/// on one thread and with every call on a thread of its own.
#[test]
fn st02_b1_v1_and_claim_a_hold_for_every_generated_wrapper_on_any_thread() {
    let mut failures = Vec::new();
    let mut lines = Vec::new();
    for (name, run) in [
        ("no wrapper", bare::run as fn(bool) -> (Vec<String>, usize)),
        ("Box", boxed::run),
        ("BoxedLog", boxed_log::run),
    ] {
        for threads in [false, true] {
            let (found, runs) = run(threads);
            let count = |place: &str| by_place(&found, place);
            lines.push(format!(
                "{name}, threads {threads}: runs {runs}, replicas serving another value {} \
                 (ST.01 Flat {}, ST.01 Outer {}, claim A {})",
                found.len(),
                count("St01Flat"),
                count("St01Outer"),
                count("ClaimA")
            ));
            failures.extend(found);
        }
    }
    for line in &lines {
        println!("ST02-B1 {line}");
    }
    for line in failures.iter().take(4) {
        println!("ST02-B1 {line}");
    }
    assert!(
        failures.is_empty(),
        "ST.02 B1: {} replica reads other than the rule's; first:\n  {}",
        failures.len(),
        failures
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}
