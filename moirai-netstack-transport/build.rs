//! Record which source tree the compiler takes `zenoh` from, so that
//! [`zenoh_patch::check`] can refuse a build that linked stock zenoh instead of
//! the vendored, patched copy (NW.31b, spec section 4).
//!
//! The answer is the build's own resolution: cargo writes the invoking
//! workspace's `Cargo.lock` before it compiles anything, and a `zenoh` package
//! table without a `source` is the path-patched vendored copy while one with a
//! registry source is stock. The workspace is the one cargo was started in
//! (`PWD`), found by walking up to its `Cargo.lock`; with no `PWD` or no lock
//! above it the answer is "not vendored", so the check fails loudly rather than
//! read another workspace's lock. The target directory cannot be used for this:
//! two workspaces that share one leave their dep-info side by side.
//!
//! [`zenoh_patch::check`]: ../src/zenoh_patch.rs

use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_FEATURE_ZENOH").is_none() {
        return;
    }
    println!(
        "cargo:rustc-env=NW31B_ZENOH_VENDORED={}",
        if vendored() { 1 } else { 0 }
    );
}

/// Whether the invoking workspace's lock resolves `zenoh` to a path package.
fn vendored() -> bool {
    std::env::var_os("PWD")
        .map(PathBuf::from)
        .and_then(|pwd| lock_in(&pwd))
        .and_then(|lock| std::fs::read_to_string(lock).ok())
        .is_some_and(|text| zenoh_is_path(&text))
}

/// The nearest `Cargo.lock` at or above `dir`.
fn lock_in(dir: &Path) -> Option<PathBuf> {
    dir.ancestors()
        .map(|dir| dir.join("Cargo.lock"))
        .find(|lock| lock.is_file())
}

/// Whether the `zenoh` package table in a lock file has no `source` line.
fn zenoh_is_path(lock: &str) -> bool {
    let mut in_zenoh = false;
    for line in lock.lines() {
        if line == "[[package]]" {
            if in_zenoh {
                return true;
            }
        } else if line == "name = \"zenoh\"" {
            in_zenoh = true;
        } else if in_zenoh && line.starts_with("source = ") {
            return false;
        }
    }
    in_zenoh
}
