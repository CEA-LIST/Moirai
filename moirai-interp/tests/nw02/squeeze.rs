//! NW.02's adversary: a neighbour that holds nearly every port the kernel
//! would hand out, and keeps taking and letting go of the few it left.
//!
//! On Linux, a bind to port 0 on a socket with `SO_REUSEADDR` (every
//! `std::net::TcpListener` and every Node.js server sets it) is given an odd
//! port of the local port range, from its lower half first, chosen at random;
//! an outgoing connection is given an even one. So a process that holds every
//! odd port of the range but `free` of them leaves any later bind to port 0
//! exactly those `free` ports to choose from, while it lasts: two harness
//! picks of one cluster then collide by the pigeonhole principle as soon as a
//! cluster needs more ports than `free`, and the churn thread takes any of
//! them that is released and left unbound for more than a few milliseconds.
//! A node that binds port 0 itself is not affected: when the `free` ports are
//! taken the kernel falls back to even ports and the bind still succeeds.
//!
//! The squeeze checks itself before it is used: a set-up that does not
//! concentrate the kernel's picks on the ports it left is an instrument that
//! could not fail anything, and it says so instead of running.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

/// How long the churn thread holds each port it takes, and the pause after.
pub const CHURN_HOLD: Duration = Duration::from_millis(2);
pub const CHURN_GAP: Duration = Duration::from_millis(1);

pub struct Squeeze {
    held: Vec<TcpListener>,
    /// The odd ports left free, in the lower half of the range.
    pub free: BTreeSet<u16>,
    /// Probe picks made after the squeeze and before the churn, and how many
    /// landed on a port left free.
    pub probes: usize,
    pub probes_on_free: usize,
    stop: Arc<AtomicBool>,
    /// Ports the churn thread took, and how many of them were left free.
    pub churn_binds: Arc<AtomicU64>,
    pub churn_on_free: Arc<AtomicU64>,
    churn: Option<JoinHandle<()>>,
}

/// The kernel's local port range, `/proc/sys/net/ipv4/ip_local_port_range`.
pub fn local_port_range() -> Result<(u16, u16), String> {
    let text = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .map_err(|err| format!("reading the local port range: {err}"))?;
    let mut parts = text.split_whitespace().map(str::parse::<u16>);
    match (parts.next(), parts.next()) {
        (Some(Ok(low)), Some(Ok(high))) if low < high => Ok((low, high)),
        _ => Err(format!("unreadable local port range `{}`", text.trim())),
    }
}

/// A port the kernel picks for a bind to port 0, released at once: the way
/// the harnesses chose ports before NW.02.
pub fn pick_and_release() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("an ephemeral port")
        .local_addr()
        .expect("a bound address")
        .port()
}

impl Squeeze {
    /// Hold every odd port of the local range on 127.0.0.1 except `free` of
    /// the lower half, chosen at random, then check that the kernel's picks
    /// land on those; with `churn`, start the thread that keeps taking them.
    ///
    /// At random and checked, because a port a process recently used still
    /// has TIME_WAIT sockets for a minute, and a bind to port 0 skips such a
    /// port (it would need `SO_REUSEADDR` to share it): leaving the lowest
    /// ports free would leave, run after run, the very ports the previous
    /// run's nodes held. A choice the probes do not confirm is taken back and
    /// another is tried, five times at most.
    pub fn new(free: usize, churn: bool) -> Result<Self, String> {
        let (low, high) = local_port_range()?;
        // Where the kernel looks first for a bind to port 0 with
        // `SO_REUSEADDR`: the lower half of the range, as it computes it.
        let half = u32::from(low) + (((u32::from(high) + 1 - u32::from(low)) >> 2) << 1);
        let mut held: Vec<TcpListener> = (low..=high)
            .filter(|port| port % 2 == 1)
            .filter_map(|port| TcpListener::bind(("127.0.0.1", port)).ok())
            .collect();
        if held.len() < free + 1000 {
            return Err(format!(
                "set-up: the squeeze could bind only {} odd ports of {low}..={high}",
                held.len()
            ));
        }
        let mut seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0x9e37_79b9_7f4a_7c15)
            | 1;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let probes = 40;
        let mut tried = Vec::new();
        for _ in 0..5 {
            let lower: Vec<usize> = (0..held.len())
                .filter(|&index| {
                    held[index]
                        .local_addr()
                        .is_ok_and(|addr| u32::from(addr.port()) < half)
                })
                .collect();
            if lower.len() < free {
                break;
            }
            let mut chosen: BTreeSet<usize> = BTreeSet::new();
            while chosen.len() < free {
                chosen.insert(lower[(next() % lower.len() as u64) as usize]);
            }
            // Release from the highest index down so the others keep theirs.
            let released: BTreeSet<u16> = chosen
                .iter()
                .rev()
                .map(|&index| {
                    held.swap_remove(index)
                        .local_addr()
                        .expect("a bound address")
                        .port()
                })
                .collect();
            let probes_on_free = (0..probes)
                .filter(|_| released.contains(&pick_and_release()))
                .count();
            if probes_on_free * 10 >= probes * 9 {
                return Ok(Self::running(held, released, probes, probes_on_free, churn));
            }
            tried.push(format!("{released:?} {probes_on_free}/{probes}"));
            held.extend(
                released
                    .iter()
                    .filter_map(|&port| TcpListener::bind(("127.0.0.1", port)).ok()),
            );
        }
        Err(format!(
            "set-up: the squeeze does not steer the kernel's picks onto the ports it leaves free \
             (ports left free, probe picks on them: {})",
            tried.join("; ")
        ))
    }

    fn running(
        held: Vec<TcpListener>,
        released: BTreeSet<u16>,
        probes: usize,
        probes_on_free: usize,
        churn: bool,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let churn_binds = Arc::new(AtomicU64::new(0));
        let churn_on_free = Arc::new(AtomicU64::new(0));
        let handle = churn.then(|| {
            let (stop, binds, on_free, free) = (
                Arc::clone(&stop),
                Arc::clone(&churn_binds),
                Arc::clone(&churn_on_free),
                released.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)) {
                        binds.fetch_add(1, Ordering::Relaxed);
                        if listener
                            .local_addr()
                            .is_ok_and(|addr| free.contains(&addr.port()))
                        {
                            on_free.fetch_add(1, Ordering::Relaxed);
                        }
                        std::thread::sleep(CHURN_HOLD);
                        drop(listener);
                    }
                    std::thread::sleep(CHURN_GAP);
                }
            })
        });

        Squeeze {
            held,
            free: released,
            probes,
            probes_on_free,
            stop,
            churn_binds,
            churn_on_free,
            churn: handle,
        }
    }

    /// One line for the log: what the squeeze held and what the churn took.
    pub fn describe(&self) -> String {
        format!(
            "held {} odd ports, left {} free {:?}, probes on free {}/{}, churn binds {} ({} on free)",
            self.held.len(),
            self.free.len(),
            self.free,
            self.probes_on_free,
            self.probes,
            self.churn_binds.load(Ordering::Relaxed),
            self.churn_on_free.load(Ordering::Relaxed)
        )
    }
}

impl Drop for Squeeze {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.churn.take() {
            let _ = handle.join();
        }
    }
}
