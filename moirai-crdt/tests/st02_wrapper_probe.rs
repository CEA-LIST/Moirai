//! ST.02, B2 (`cea-cdrt-knowledge/spec/items/ST.02.md`): a wrapper a user
//! writes between an `OptionLog` and its content never silently disables
//! the rule that an optional drops what a removal emptied.
//!
//! The probe is a crate of its own, written under Cargo's temporary
//! directory for this crate's integration tests from the files of
//! `st02_held/`, depending on this checkout's `moirai-crdt`,
//! `moirai-protocol` and `moirai-macros` with their default features. It has
//! one binary per wrapper: `wrapped` (`Wrapped<L>`, which implements `IsLog`
//! and `EvalNested` by handing every call to `L`) and `threaded`
//! (`Threaded<L>`, the same, with `L`'s `effect`, `stabilize` and
//! `redundant_by_parent` run on a thread of its own). Each binary plays
//! `st02_held/composition.rs`'s 72 schedules (ST.01's Flat and Outer, claim
//! A's Flat) with its wrapper around every `OptionLog`'s content and exits 0
//! when every replica serves the rule's document.
//!
//! For each binary one of two outcomes passes:
//!
//! - it builds, runs, and exits 0 (the wrapper is accepted and the rule
//!   holds through it); or
//! - it does not build, and every error is a trait bound the wrapper does
//!   not meet (`E0277`, or `E0599` for a method whose bounds it does not
//!   meet), naming the wrapper, and none is about sending or sharing across
//!   threads (the wrapper is refused, loudly, before anything runs).
//!
//! It fails when the binary builds and a replica serves another value (the
//! wrapper silently disabled the rule), and it is a harness failure when the
//! binary does not build for any other reason.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-crdt --test st02_wrapper_probe -- --nocapture
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const FILES: [&str; 3] = ["composition.rs", "wrapped.rs", "threaded.rs"];

/// Write the probe crate; answers its manifest.
fn write_probe() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = fs::canonicalize(manifest_dir.join(".."))
        .expect("set-up: the workspace sits above moirai-crdt");
    let probe = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("st02-wrapper-probe");
    if probe.exists() {
        fs::remove_dir_all(&probe).expect("set-up: removing the previous probe");
    }
    fs::create_dir_all(probe.join("probe")).expect("set-up: creating the probe");
    for file in FILES {
        fs::copy(
            manifest_dir.join("tests/st02_held").join(file),
            probe.join("probe").join(file),
        )
        .unwrap_or_else(|error| panic!("set-up: copying {file}: {error}"));
    }
    let root = workspace.display();
    let manifest = format!(
        "[package]\n\
         name = \"st02-wrapper-probe\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         autobins = false\n\
         \n\
         [[bin]]\nname = \"wrapped\"\npath = \"probe/wrapped.rs\"\n\
         \n\
         [[bin]]\nname = \"threaded\"\npath = \"probe/threaded.rs\"\n\
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
    let _ = fs::copy(workspace.join("Cargo.lock"), probe.join("Cargo.lock"));
    probe.join("Cargo.toml")
}

/// What became of one wrapper.
enum Outcome {
    /// Built and ran; the exit status and what it printed.
    Ran(bool, String),
    /// Did not build: every error a bound the wrapper does not meet.
    Refused(Vec<String>),
    /// Did not build for another reason.
    Broken(String),
}

fn try_wrapper(manifest: &Path, bin: &str, wrapper: &str) -> Outcome {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let target = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("st02-wrapper-probe-target");
    let build = Command::new(&cargo)
        .args(["build", "-j", "2", "--bin", bin, "--manifest-path"])
        .arg(manifest)
        .env("CARGO_TARGET_DIR", &target)
        .output()
        .expect("set-up: running cargo on the probe");
    let stderr = String::from_utf8_lossy(&build.stderr).to_string();
    if !build.status.success() {
        let errors: Vec<String> = stderr
            .lines()
            .filter(|line| line.starts_with("error"))
            .map(str::to_string)
            .collect();
        let coded: Vec<&String> = errors
            .iter()
            .filter(|line| line.starts_with("error["))
            .collect();
        let bounds_only = !coded.is_empty()
            && coded
                .iter()
                .all(|line| line.starts_with("error[E0277]") || line.starts_with("error[E0599]"));
        let threads = stderr.contains("cannot be sent between threads")
            || stderr.contains("cannot be shared between threads");
        return if bounds_only && !threads && stderr.contains(&format!("{wrapper}<")) {
            Outcome::Refused(coded.into_iter().cloned().collect())
        } else {
            Outcome::Broken(stderr)
        };
    }
    let binary = target.join("debug").join(bin);
    let run = Command::new(&binary)
        .output()
        .unwrap_or_else(|error| panic!("set-up: running {}: {error}", binary.display()));
    Outcome::Ran(
        run.status.success(),
        String::from_utf8_lossy(&run.stdout).to_string(),
    )
}

#[test]
fn st02_b2_a_user_wrapper_is_refused_at_compile_time_or_serves_the_rule() {
    let manifest = write_probe();
    let mut silent = Vec::new();
    let mut broken = Vec::new();
    for (bin, wrapper) in [("wrapped", "Wrapped"), ("threaded", "Threaded")] {
        match try_wrapper(&manifest, bin, wrapper) {
            Outcome::Ran(ok, printed) => {
                for line in printed.lines().filter(|line| line.starts_with("ST02-B2")) {
                    println!("{line}");
                }
                println!(
                    "ST02-B2 {wrapper}: built and ran; {}",
                    if ok {
                        "every replica serves the rule's document"
                    } else {
                        "a replica serves another value"
                    }
                );
                if !ok {
                    silent.push(wrapper);
                }
            }
            Outcome::Refused(errors) => {
                println!(
                    "ST02-B2 {wrapper}: refused at compile time, {} errors, all unmet bounds: {}",
                    errors.len(),
                    errors.first().cloned().unwrap_or_default()
                );
            }
            Outcome::Broken(stderr) => {
                println!("ST02-B2 {wrapper}: does not build for another reason");
                broken.push(format!("{wrapper}:\n{stderr}"));
            }
        }
    }
    assert!(
        broken.is_empty(),
        "ST.02 B2, harness: the probe does not build for a reason other than refusing a \
         wrapper:\n{}",
        broken.join("\n")
    );
    assert!(
        silent.is_empty(),
        "ST.02 B2: {} of 2 wrappers build and silently disable the rule: {silent:?}",
        silent.len()
    );
}
