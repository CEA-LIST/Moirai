//! NW.19 round 3 (vault spec/items/NW.19.md, "Round 3, written before any
//! rerun"): the port allocator of `e2e_convergence.rs` never hands out one
//! port twice in a test process, and a port it hands out can be bound.
//!
//! In round 2, `ProcessBackend::reserve` called the old `free_port` (bind
//! `127.0.0.1:0`, read the port, drop the listener) twice and got 41063 both
//! times; J4's first replica took the port for its replication listener and
//! its HTTP server failed with `AddrInUse`. The allocator is private to the
//! `e2e_convergence` test binary, and a test added there would move the
//! counts P3 and P4 register (43 passed, 5 ignored), so this file holds a
//! copy of it between the two marker lines. It checks first that the copy is,
//! line for line, the block between the same markers in `e2e_convergence.rs`,
//! then what the copy does.
//!
//! Run it with one test thread, `nice -n 10 cargo test -j 2 -p moirai-network
//! --test nw19_e2e_free_ports -- --test-threads=1`: a call of the allocator
//! keeps the listeners it skips bound until it returns, so a call made by
//! another test at the same moment could hold a port the first test is about
//! to bind.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Mutex, PoisonError};
use std::thread;

use anyhow::{bail, Context, Result};

/// The two marker lines around the allocator, here and in
/// `e2e_convergence.rs`.
const BEGIN: &str = "// BEGIN NW.19 round 3: the port allocator";
const END: &str = "// END NW.19 round 3: the port allocator";

// BEGIN NW.19 round 3: the port allocator
/// Every port [`free_ports`] has handed out in this test process. No port is
/// handed out twice, so no two endpoints of one process share a port: not a
/// replica's replication and HTTP ports, not two replicas, not a replica and
/// a bootnode, not two scenarios, one after another or at once.
static PORTS_HANDED_OUT: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());

/// How many binds one call of [`free_ports`] may make before it gives up.
const FREE_PORT_BINDS: usize = 1024;

/// Asks the OS for `N` unused TCP ports and hands them back, pairwise
/// distinct and distinct from every port handed out before in this process.
///
/// Two binds of `127.0.0.1:0`, each listener released at once, can return
/// the same port: in NW.19's round 2, `reserve` gave J4's first replica 41063
/// for both its replication and its HTTP port, the replication listener took
/// it and the HTTP server failed with `AddrInUse`. So every listener stays
/// bound until all `N` ports are chosen, and a port this process handed out
/// before is skipped. What remains is the window between the release here
/// and the child's own bind, in which another process may take the port. It
/// is still far better than hardcoding 8081/9001, which makes repeated runs
/// collide.
fn free_ports<const N: usize>() -> Result<[u16; N]> {
    let mut handed_out = PORTS_HANDED_OUT
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let mut ports = [0; N];
    let mut chosen = 0;
    // Every listener of this call, the skipped ones too, stays bound until
    // the call returns, so the kernel cannot return one port twice in it.
    let mut held = Vec::new();
    while chosen < N {
        if held.len() == FREE_PORT_BINDS {
            bail!("{FREE_PORT_BINDS} binds of 127.0.0.1:0 returned only ports handed out before");
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).context("bind ephemeral port")?;
        let port = listener.local_addr()?.port();
        if handed_out.insert(port) {
            ports[chosen] = port;
            chosen += 1;
        }
        held.push(listener);
    }
    Ok(ports)
}

/// One port from [`free_ports`]: never one this process handed out before.
fn free_port() -> Result<u16> {
    let [port] = free_ports::<1>()?;
    Ok(port)
}
// END NW.19 round 3: the port allocator

/// The lines strictly between the two marker lines of `source`.
fn allocator_block(source: &str) -> Result<Vec<&str>> {
    let mut lines = source.lines();
    if !lines.by_ref().any(|line| line == BEGIN) {
        bail!("no line {BEGIN:?}");
    }
    let mut block = Vec::new();
    for line in lines {
        if line == END {
            return Ok(block);
        }
        block.push(line);
    }
    bail!("no line {END:?} after {BEGIN:?}")
}

#[test]
fn the_allocator_here_is_e2e_convergences_line_for_line() {
    let theirs = allocator_block(include_str!("e2e_convergence.rs")).expect("e2e_convergence.rs");
    let ours = allocator_block(include_str!("nw19_e2e_free_ports.rs")).expect("this file");
    assert!(
        ours.iter().any(|line| line.starts_with("fn free_ports<")),
        "the block between the markers holds no free_ports"
    );
    assert_eq!(
        ours, theirs,
        "the allocator copied here is not e2e_convergence.rs's"
    );
}

/// Calls one after another, shaped as the suite makes them: a pair per
/// replica (`ProcessBackend::reserve`), one port per bootnode.
const PAIRS: usize = 1000;
const SINGLES: usize = 500;

#[test]
fn ports_handed_out_one_call_after_another_are_pairwise_distinct_and_free() {
    let mut seen = BTreeSet::new();
    let mut twice = Vec::new();
    let mut not_free = Vec::new();
    let mut take = |port: u16| {
        if !seen.insert(port) {
            twice.push(port);
        }
        if let Err(e) = TcpListener::bind(("127.0.0.1", port)) {
            not_free.push(format!("{port}: {e}"));
        }
    };
    for _ in 0..PAIRS {
        let [listen, http] = free_ports::<2>().expect("two ports");
        take(listen);
        take(http);
    }
    for _ in 0..SINGLES {
        take(free_port().expect("one port"));
    }
    assert!(
        twice.is_empty() && not_free.is_empty(),
        "of {} ports handed out one call after another, {} were handed out twice {twice:?} \
         and {} could not be bound at once {not_free:?}",
        2 * PAIRS + SINGLES,
        twice.len(),
        not_free.len()
    );
}

/// Threads calling at once, as scenarios on several test threads would.
const THREADS: usize = 8;
const PAIRS_PER_THREAD: usize = 100;

#[test]
fn ports_handed_out_to_threads_at_once_are_pairwise_distinct() {
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            thread::spawn(|| {
                (0..PAIRS_PER_THREAD)
                    .flat_map(|_| free_ports::<2>().expect("two ports"))
                    .collect::<Vec<u16>>()
            })
        })
        .collect();
    let mut seen = BTreeSet::new();
    let mut twice = Vec::new();
    for handle in handles {
        for port in handle.join().expect("an allocating thread panicked") {
            if !seen.insert(port) {
                twice.push(port);
            }
        }
    }
    assert!(
        twice.is_empty(),
        "of {} ports handed out to {THREADS} threads at once, {} were handed out twice {twice:?}",
        2 * THREADS * PAIRS_PER_THREAD,
        twice.len()
    );
}
