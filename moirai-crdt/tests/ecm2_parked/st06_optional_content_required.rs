//! ST.06 (`cea-cdrt-knowledge/spec/items/ST.06.md`): what the content of an
//! optional does with a removal the optional hands down to it is stated by
//! every implementation of `OptionalContent`, never inherited by default.
//!
//! - C0 (set-up): the harness reads a real E0046 refusal of the same shape
//!   (a trait of the probe whose method has no default, and an empty impl)
//!   as the refusal C1 looks for, on every run.
//! - C1 (L0, compile-fail): a wrapper log between an `OptionLog` and its
//!   content whose impl is `impl<L: OptionalContent> OptionalContent for
//!   Relay<L> {}` does not build, and the only error is E0046 naming
//!   `redundant_by_optional` at that impl. Where it builds, the binary is run
//!   and whether the wrapper lost the rule is reported, then C1 fails.
//! - C2 (L0 and L1): the same wrapper with a body that forwards the removal
//!   to what it wraps builds, and every replica serves the rule's document on
//!   ST.02's 72 schedules, on one thread and on many. This tells C1's refusal
//!   apart from a probe that does not build for another reason, and pins the
//!   method's signature.
//! - C3 (L0, a guard, green before and after): an `OptionLog` over a keyed
//!   map or a list, neither of which implements `OptionalContent`, does not
//!   build, and only on those unmet bounds (E0277): unsupported, documented
//!   and not fixed, by Cam's decision of 2026-09-30.
//! - G1 to G4 (L1, guards, green before and after): the four implementations
//!   that took the default until this item (`POLog`, `GraphLog`, the log
//!   `record!` generates, `OptionLog`) behave as the default made them
//!   behave. Two replicas each, a removal alone and a removal raced by a
//!   write, each replica delivering the two in its own order; every expected
//!   read is written by hand from the rule (spec, section 3).
//!
//! The probe of C0 to C3 is a crate of its own, written under Cargo's
//! temporary directory for this crate's integration tests from
//! `st06_probe/` and ST.02's `st02_held/composition.rs`, depending on this
//! checkout's `moirai-crdt`, `moirai-protocol` and `moirai-macros` with their
//! default features, built with the workspace's lock file.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-crdt --test st06_optional_content_required -- --nocapture
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use moirai_crdt::counter::resettable_counter::Counter;
use moirai_crdt::list::eg_walker::List;
use moirai_crdt::option::{OptionLog, Optional};
use moirai_crdt::utils::membership::twins_log;
use moirai_macros::record;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::IsReplica;
use moirai_protocol::state::graph_log::GraphLog;
use moirai_protocol::state::po_log::VecLog;

/// The probe's sources, relative to this crate's `tests/`.
const PROBE_FILES: [&str; 6] = [
    "st02_held/composition.rs",
    "st06_probe/calibration.rs",
    "st06_probe/relay.rs",
    "st06_probe/empty_impl.rs",
    "st06_probe/explicit_body.rs",
    "st06_probe/unsupported_content.rs",
];

/// The impl C1 says does not compile, as `st06_probe/empty_impl.rs` spells it.
const EMPTY_IMPL: &str = "impl<L: OptionalContent> OptionalContent for Relay<L> {}";

/// The empty impl of C0's calibration, as `st06_probe/calibration.rs` spells
/// it.
const CALIBRATION_IMPL: &str = "impl Stated for Probe {}";

/// The one error C1 accepts.
const E0046: &str = "error[E0046]: not all trait items implemented, missing: \
                     `redundant_by_optional`";

/// Write the probe crate; answers its manifest.
fn write_probe() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = fs::canonicalize(manifest_dir.join(".."))
        .expect("set-up: the workspace sits above moirai-crdt");
    let probe = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("st06-optional-content-probe");
    if probe.exists() {
        fs::remove_dir_all(&probe).expect("set-up: removing the previous probe");
    }
    for file in PROBE_FILES {
        let to = probe.join("probe").join(file);
        fs::create_dir_all(to.parent().expect("set-up: a probe file has a directory"))
            .expect("set-up: creating the probe");
        fs::copy(manifest_dir.join("tests").join(file), &to)
            .unwrap_or_else(|error| panic!("set-up: copying {file}: {error}"));
    }
    let root = workspace.display();
    let manifest = format!(
        "[package]\n\
         name = \"st06-optional-content-probe\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         autobins = false\n\
         \n\
         [[bin]]\nname = \"calibration\"\npath = \"probe/st06_probe/calibration.rs\"\n\
         \n\
         [[bin]]\nname = \"empty_impl\"\npath = \"probe/st06_probe/empty_impl.rs\"\n\
         \n\
         [[bin]]\nname = \"explicit_body\"\npath = \"probe/st06_probe/explicit_body.rs\"\n\
         \n\
         [[bin]]\nname = \"unsupported_content\"\n\
         path = \"probe/st06_probe/unsupported_content.rs\"\n\
         \n\
         [dependencies]\n\
         moirai-crdt = {{ path = \"{root}/moirai-crdt\" }}\n\
         moirai-protocol = {{ path = \"{root}/moirai-protocol\" }}\n\
         moirai-macros = {{ path = \"{root}/moirai-macros\" }}\n\
         serde_json = \"1.0\"\n\
         \n\
         [workspace]\n"
    );
    fs::write(probe.join("Cargo.toml"), manifest).expect("set-up: writing the probe manifest");
    // The workspace's lock file, so that the probe builds against the
    // versions the workspace builds against.
    fs::copy(workspace.join("Cargo.lock"), probe.join("Cargo.lock"))
        .expect("set-up: copying the workspace's lock file");
    probe.join("Cargo.toml")
}

/// One build of one probe binary: whether it built, and what cargo printed.
struct Build {
    built: bool,
    stderr: String,
}

fn build(manifest: &Path, bin: &str) -> Build {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let target = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("st06-optional-content-target");
    let output = Command::new(&cargo)
        .args(["build", "-j", "2", "--color", "never", "--bin", bin])
        .arg("--manifest-path")
        .arg(manifest)
        .env("CARGO_TARGET_DIR", &target)
        .output()
        .expect("set-up: running cargo on the probe");
    Build {
        built: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

/// Run a probe binary that built; whether it exited 0, and the lines it
/// printed.
fn run(bin: &str) -> (bool, Vec<String>) {
    let binary = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("st06-optional-content-target")
        .join("debug")
        .join(bin);
    let output = Command::new(&binary)
        .output()
        .unwrap_or_else(|error| panic!("set-up: running {}: {error}", binary.display()));
    let printed = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    (output.status.success(), printed)
}

/// The line of `st06_probe/<file>` that holds `code`, which it holds once.
fn line_of(file: &str, code: &str) -> usize {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/st06_probe")
        .join(file);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("set-up: reading st06_probe/{file}: {error}"));
    let lines: Vec<usize> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| line.trim() == code)
        .map(|(index, _)| index + 1)
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "set-up: st06_probe/{file} holds `{code}` once"
    );
    lines[0]
}

/// Why a build is not the refusal C1 names, or `None` when it is: exactly
/// one coded error, [`E0046`], its span at line `at_line` of
/// `st06_probe/<file>`.
fn not_the_refusal(stderr: &str, file: &str, at_line: usize) -> Option<String> {
    let lines: Vec<&str> = stderr.lines().collect();
    let coded: Vec<(usize, &str)> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with("error["))
        .map(|(index, line)| (index, *line))
        .collect();
    if coded.len() != 1 || coded[0].1 != E0046 {
        return Some(format!(
            "{} coded errors, not the one E0046 naming redundant_by_optional alone: {:?}",
            coded.len(),
            coded.iter().map(|(_, line)| *line).collect::<Vec<_>>()
        ));
    }
    let span = lines[coded[0].0 + 1..]
        .iter()
        .find(|line| line.trim_start().starts_with("-->"))
        .copied()
        .unwrap_or("");
    let expected = format!("st06_probe/{file}:{at_line}:");
    if !span.contains(&expected) {
        return Some(format!(
            "the E0046 points at `{}`, not at the empty impl ({expected})",
            span.trim()
        ));
    }
    None
}

/// Why C3's build is not the refusal the guard expects, or `None` when it
/// is: every coded error an unmet bound (E0277), and both the keyed map and
/// the list named as not meeting `OptionalContent`.
fn not_unsupported(stderr: &str) -> Option<String> {
    let coded: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with("error["))
        .collect();
    if coded.is_empty() || coded.iter().any(|line| !line.starts_with("error[E0277]")) {
        return Some(format!(
            "its coded errors are not all unmet bounds (E0277): {coded:?}"
        ));
    }
    let unmet = |log: &str| {
        coded
            .iter()
            .any(|line| line.contains(log) && line.contains("OptionalContent"))
    };
    let missing: Vec<&str> = ["UWMapLog<", "NestedListLog<"]
        .into_iter()
        .filter(|log| !unmet(log))
        .collect();
    if !missing.is_empty() {
        return Some(format!(
            "no error says that {missing:?} does not implement OptionalContent: {coded:?}"
        ));
    }
    None
}

/// C0 to C3 in one test, one build after the other, since they share the
/// probe's directory and its target. Every outcome is printed before any is
/// judged.
#[test]
fn st06_c1_an_empty_optional_content_impl_for_a_wrapper_does_not_compile() {
    let manifest = write_probe();
    let at_line = line_of("empty_impl.rs", EMPTY_IMPL);
    let mut failures: Vec<String> = Vec::new();

    // C0: the harness reads a refusal like C1's as one.
    let calibration = build(&manifest, "calibration");
    assert!(
        !calibration.built,
        "set-up: the calibration binary, an empty impl of a trait whose method has no \
         default, built"
    );
    let calibration_line = line_of("calibration.rs", CALIBRATION_IMPL);
    if let Some(why) = not_the_refusal(&calibration.stderr, "calibration.rs", calibration_line) {
        panic!(
            "set-up: the harness does not read the calibration's refusal as C1's: \
             {why}:\n{}",
            calibration.stderr
        );
    }
    println!(
        "ST06-C0 calibration: refused, read as C1's refusal ({E0046}, at \
         st06_probe/calibration.rs:{calibration_line})"
    );

    // C2: the wrapper that states its body builds and serves the rule.
    let explicit = build(&manifest, "explicit_body");
    if explicit.built {
        let (served, printed) = run("explicit_body");
        for line in printed.iter().filter(|line| line.starts_with("ST06-C2")) {
            println!("{line}");
        }
        println!(
            "ST06-C2 explicit body: built; {}",
            if served {
                "every replica serves the rule's document"
            } else {
                "a replica serves another value"
            }
        );
        if !served {
            failures.push(
                "ST.06 C2: a wrapper that states its redundant_by_optional, forwarding it, \
                 builds, and a replica serves another value through it"
                    .to_string(),
            );
        }
    } else {
        println!("ST06-C2 explicit body: does not build");
        failures.push(format!(
            "ST.06 C2: a wrapper that states its redundant_by_optional, forwarding it, does \
             not build:\n{}",
            explicit.stderr
        ));
    }

    // C3, a guard: an optional over a keyed map or a list stays refused.
    let unsupported = build(&manifest, "unsupported_content");
    if unsupported.built {
        println!("ST06-C3 keyed map and list under an optional: built");
        failures.push(
            "ST.06 C3: OptionLog over a keyed map and a list builds; the decision was to \
             document it as unsupported, not to fix it"
                .to_string(),
        );
    } else {
        match not_unsupported(&unsupported.stderr) {
            None => println!(
                "ST06-C3 keyed map and list under an optional: refused at compile time, every \
                 coded error E0277, both named as not implementing OptionalContent"
            ),
            Some(why) => {
                println!("ST06-C3 keyed map and list under an optional: refused otherwise");
                failures.push(format!(
                    "ST.06 C3: the unsupported compositions do not build, but {why}:\n{}",
                    unsupported.stderr
                ));
            }
        }
    }

    // C1: the wrapper whose impl says nothing.
    let empty = build(&manifest, "empty_impl");
    if empty.built {
        let (served, printed) = run("empty_impl");
        for line in printed.iter().filter(|line| line.starts_with("ST06-C1")) {
            println!("{line}");
        }
        let run_says = if served {
            "every replica serves the rule's document"
        } else {
            "a replica serves another value (the wrapper silently lost the rule)"
        };
        println!(
            "ST06-C1 empty impl: built (redundant_by_optional still has a default body); \
             run: {run_says}"
        );
        failures.push(format!(
            "ST.06 C1: `{EMPTY_IMPL}` compiles, so a wrapper takes a default for what it does \
             with a removal an optional hands down; its run: {run_says}"
        ));
    } else {
        match not_the_refusal(&empty.stderr, "empty_impl.rs", at_line) {
            None => println!(
                "ST06-C1 empty impl: refused at compile time, one error: {E0046}, at \
                 st06_probe/empty_impl.rs:{at_line}"
            ),
            Some(why) => {
                println!("ST06-C1 empty impl: does not build, but not as the claim says");
                failures.push(format!(
                    "ST.06 C1: an empty OptionalContent impl for a wrapper does not build, but \
                     {why}:\n{}",
                    empty.stderr
                ));
            }
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// G1, `POLog`: an optional counter. Unset alone, it is absent; unset while
/// a concurrent write increments it, it holds that write alone.
#[test]
fn st06_g1_po_log_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<VecLog<Counter<i32>>>;
    let read = Read::<Option<i32>>::new();

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(Counter::Inc(3))).unwrap();
    b.receive(x);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(Counter::Inc(10))).unwrap();
    b.receive(x);
    let w = a.send(Optional::Set(Counter::Inc(2))).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G1 POLog: unset alone {alone:?}, unset raced by +2 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G1: POLog, an unset alone");
    assert_eq!(raced, [Some(2), Some(2)], "ST.06 G1: POLog, an unset raced");
}

/// G2, `GraphLog`: an optional text. A text never empties, since its reset
/// records the deletion: unset alone, it stays and reads ""; unset while a
/// concurrent write inserts, it reads that insert alone.
#[test]
fn st06_g2_graph_log_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<GraphLog<List<char>>>;
    let read = Read::<Option<String>>::new();

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(List::insert('x', 0))).unwrap();
    b.receive(x);
    let y = a.send(Optional::Set(List::insert('y', 1))).unwrap();
    b.receive(y);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(List::insert('x', 0))).unwrap();
    b.receive(x);
    let w = a.send(Optional::Set(List::insert('y', 1))).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G2 GraphLog: unset alone {alone:?}, unset raced by an insert {raced:?}");
    let empty = Some(String::new());
    assert_eq!(
        alone,
        [empty.clone(), empty],
        "ST.06 G2: GraphLog, an unset alone"
    );
    let y = Some("y".to_string());
    assert_eq!(raced, [y.clone(), y], "ST.06 G2: GraphLog, an unset raced");
}

record!(G3Box {
    k: VecLog<Counter<i32>> => i32,
    note: OptionLog<VecLog<Counter<i32>>> => Option<i32>,
});
record!(G3Holder {
    tick: VecLog<Counter<i32>> => i32,
    inner: OptionLog<G3BoxLog> => Option<G3BoxValue>,
});

/// G3, the log `record!` generates: a `G3Box` in an optional. Unset alone,
/// it is absent. Unset while a concurrent write increments its `k`, it holds
/// `k` 10 alone, and its optional `note`, which the unset emptied, is absent.
#[test]
fn st06_g3_a_record_under_an_optional_hands_its_unset_to_every_field() {
    let read = Read::<G3HolderValue>::new();
    let into = |op: G3Box| G3Holder::Inner(Optional::Set(op));

    let (mut a, mut b) = twins_log::<G3HolderLog>();
    let x = a.send(into(G3Box::K(Counter::Inc(4)))).unwrap();
    b.receive(x);
    let u = a.send(G3Holder::Inner(Optional::Unset)).unwrap();
    b.receive(u);
    let alone = [a.query(&read).inner, b.query(&read).inner];

    let (mut a, mut b) = twins_log::<G3HolderLog>();
    let x = a
        .send(into(G3Box::Note(Optional::Set(Counter::Inc(1)))))
        .unwrap();
    b.receive(x);
    let u = a.send(G3Holder::Inner(Optional::Unset)).unwrap();
    let w = b.send(into(G3Box::K(Counter::Inc(10)))).unwrap();
    a.receive(w);
    b.receive(u);
    let raced: Vec<Option<(i32, Option<i32>)>> = [a.query(&read), b.query(&read)]
        .into_iter()
        .map(|value| value.inner.map(|inner| (inner.k, inner.note)))
        .collect();

    println!("ST06-G3 record!: unset alone {alone:?}, unset raced by k +10 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G3: record!, an unset alone");
    assert_eq!(
        raced,
        vec![Some((10, None)), Some((10, None))],
        "ST.06 G3: record!, an unset raced by a write to k"
    );
}

/// G4, `OptionLog`: an optional counter held by an optional. The outer
/// unset alone leaves both absent; raced by a concurrent write through both,
/// it leaves that write alone.
#[test]
fn st06_g4_an_optional_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<OptionLog<VecLog<Counter<i32>>>>;
    let read = Read::<Option<Option<i32>>>::new();
    let set = |by: i32| Optional::Set(Optional::Set(Counter::Inc(by)));

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(set(2)).unwrap();
    b.receive(x);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(set(2)).unwrap();
    b.receive(x);
    let w = a.send(set(3)).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G4 OptionLog: outer unset alone {alone:?}, raced by +3 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G4: OptionLog, an unset alone");
    assert_eq!(
        raced,
        [Some(Some(3)), Some(Some(3))],
        "ST.06 G4: OptionLog, an unset raced"
    );
}
