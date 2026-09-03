//! M-E2 of the model plane's validation plan: what a hosted log costs a node.
//!
//! Two readings, in process, at N hosted logs for each N given:
//!
//! - **dispatch**: the cost of finding the log a frame names in the hosted
//!   map, which is what `handle_transport_message` does first with every
//!   frame. Measured as one `hosted(&id)` lookup, timed one by one with
//!   `Instant` so p50 and p99 are real quantiles and not a mean, for an id
//!   the node hosts (the hit path) and for one it does not (the miss path,
//!   which a node hosting one model of N takes N-1 times in N, since one
//!   session carries every model's frames to every node).
//! - **memory**: the deep size of the hosted map's entries with the member
//!   table counted once, `m(N)`, beside the size of one entry on its own,
//!   `s` (so `m(1) = s`), and the number of distinct member tables the logs
//!   hold, which the design says is one.
//!
//! The node is the real `Node<L>` over the real transport bound to a free
//! port, with nothing connected: the lookup is the production map on the
//! production key type, and the transport is idle. The log is the set log
//! the crate's own tests host (`EWFlagSetLog<String>`); an empty log
//! allocates nothing, so the per-log figure is the causal bookkeeping every
//! log type pays alike.
//!
//! Thresholds, fixed in the plan before implementation: p50 under 1 µs on
//! both paths at the largest N; p50 at the largest N at most twice p50 at
//! N = 1; and m(N) ≤ m(1) + (N − 1) × 2 × s. A crossed threshold is printed
//! and turns the exit status non-zero after the CSV is written, so a run is
//! never lost to its own verdict.
//!
//! ```text
//! cargo run --release --features test_utils --example model_plane_cost -- \
//!     --out results.csv --points 1,4,16,64 --lookups 100000
//! ```

use std::collections::BTreeSet;
use std::hint::black_box;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use moirai_crdt::set::ewflag_set::EWFlagSetLog;
use moirai_network::generic::Node;
use moirai_network::HashMap;
use moirai_protocol::log_id::LogId;

type Log = EWFlagSetLog<String>;

const DEFAULT_POINTS: [usize; 4] = [1, 4, 16, 64];
const DEFAULT_LOOKUPS: usize = 100_000;
const P50_CEILING: Duration = Duration::from_micros(1);
const GROWTH_CEILING: f64 = 2.0;

struct Args {
    out: Option<String>,
    points: Vec<usize>,
    lookups: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        out: None,
        points: DEFAULT_POINTS.to_vec(),
        lookups: DEFAULT_LOOKUPS,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next();
        match (flag.as_str(), value) {
            ("--out", Some(path)) => args.out = Some(path),
            ("--points", Some(list)) => {
                args.points = list
                    .split(',')
                    .map(|n| n.trim().parse().expect("--points takes integers"))
                    .collect();
            }
            ("--lookups", Some(n)) => args.lookups = n.parse().expect("--lookups takes an integer"),
            (flag, _) => {
                eprintln!(
                    "usage: model_plane_cost [--out FILE] [--points 1,4,16,64] [--lookups N]"
                );
                panic!("unknown or incomplete argument `{flag}`");
            }
        }
    }
    args
}

/// A port nothing else is listening on right now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind a loopback port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// The `i`th hosted log's id: deterministic, so a run is reproducible and a
/// miss id is one no run ever hosts.
fn hosted_id(i: usize) -> LogId {
    let mut bytes = [0u8; 16];
    bytes[0] = (i >> 8) as u8;
    bytes[1] = i as u8;
    LogId::from_bytes(bytes)
}

fn miss_id() -> LogId {
    LogId::from_bytes([0xff; 16])
}

/// A node hosting `n` logs, the first of them its default log, over the real
/// transport on a free port with no peers.
fn node_hosting(n: usize) -> Node<Log> {
    let mut node = Node::<Log>::new_with_log_id(
        "m".to_string(),
        &["m"],
        free_port(),
        HashMap::default(),
        hosted_id(0),
    );
    for i in 1..n {
        node.host_log(hosted_id(i)).expect("a fresh id");
    }
    assert_eq!(node.hosted_logs().count(), n);
    node
}

#[derive(Debug, Clone, Copy)]
struct Quantiles {
    p50: Duration,
    p99: Duration,
    mean: Duration,
    /// The mean of one lookup inside a loop timed as a whole, so a reader
    /// can see the cost with the clock read taken out of every sample.
    batch: Duration,
}

/// Time `lookups` calls of `lookup`, one by one.
fn timed<F: FnMut(usize) -> bool>(lookups: usize, mut lookup: F) -> Quantiles {
    let mut samples: Vec<Duration> = Vec::with_capacity(lookups);
    for i in 0..lookups {
        let started = Instant::now();
        let found = lookup(i);
        let elapsed = started.elapsed();
        black_box(found);
        samples.push(elapsed);
    }
    samples.sort_unstable();
    let total: Duration = samples.iter().sum();
    let started = Instant::now();
    for i in 0..lookups {
        black_box(lookup(i));
    }
    let batch = started.elapsed() / lookups as u32;
    Quantiles {
        p50: samples[lookups / 2],
        p99: samples[(lookups * 99) / 100],
        mean: total / lookups as u32,
        batch,
    }
}

/// What `Instant::now()` around nothing costs on this machine, so a reader
/// can see how much of a sub-100 ns sample is the clock.
fn timer_overhead(lookups: usize) -> Quantiles {
    timed(lookups, |_| true)
}

struct Point {
    n: usize,
    hit: Quantiles,
    miss: Quantiles,
    deep_size: usize,
    per_log: usize,
    member_tables: usize,
    self_rss: Option<usize>,
}

fn self_rss_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn measure(n: usize, lookups: usize) -> Point {
    let node = node_hosting(n);
    let ids: Vec<LogId> = (0..n).map(hosted_id).collect();
    let hosted: BTreeSet<LogId> = node.hosted_logs().cloned().collect();
    assert_eq!(hosted, ids.iter().cloned().collect::<BTreeSet<_>>());
    let miss = miss_id();
    assert!(!node.hosts(&miss));

    // Warm the map once so the first sample is not a page fault.
    for id in &ids {
        black_box(node.hosted(id).is_some());
    }
    let hit = timed(lookups, |i| node.hosted(&ids[i % n]).is_some());
    let miss = timed(lookups, |_| node.hosted(&miss).is_some());

    Point {
        n,
        hit,
        miss,
        deep_size: node.hosted_logs_deep_size(),
        per_log: node
            .hosted_log_deep_size(&ids[0])
            .expect("the default log is hosted"),
        member_tables: node.member_tables(),
        self_rss: self_rss_bytes(),
    }
}

fn main() {
    let args = parse_args();
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let timer = timer_overhead(args.lookups);
    eprintln!(
        "timer overhead ({profile}): p50 {:?}, p99 {:?}, mean {:?}; every sample below includes one such read",
        timer.p50, timer.p99, timer.mean
    );

    let points: Vec<Point> = args
        .points
        .iter()
        .map(|&n| {
            let point = measure(n, args.lookups);
            eprintln!(
                "N={:>3}: hit p50 {:?} p99 {:?} batch {:?} | miss p50 {:?} p99 {:?} batch {:?} | \
                 m(N) {} B, s {} B, member tables {}",
                point.n,
                point.hit.p50,
                point.hit.p99,
                point.hit.batch,
                point.miss.p50,
                point.miss.p99,
                point.miss.batch,
                point.deep_size,
                point.per_log,
                point.member_tables
            );
            point
        })
        .collect();

    let mut csv = String::from(
        "run,n_logs,hit_p50_ns,hit_p99_ns,miss_p50_ns,miss_p99_ns,deep_size_bytes,rss_bytes,\
         profile,per_log_bytes,member_tables,hit_mean_ns,miss_mean_ns,hit_batch_ns,miss_batch_ns,\
         timer_p50_ns,self_rss_bytes\n",
    );
    for point in &points {
        csv.push_str(&format!(
            "1,{},{},{},{},{},{},,{profile},{},{},{},{},{},{},{},{}\n",
            point.n,
            point.hit.p50.as_nanos(),
            point.hit.p99.as_nanos(),
            point.miss.p50.as_nanos(),
            point.miss.p99.as_nanos(),
            point.deep_size,
            point.per_log,
            point.member_tables,
            point.hit.mean.as_nanos(),
            point.miss.mean.as_nanos(),
            point.hit.batch.as_nanos(),
            point.miss.batch.as_nanos(),
            timer.p50.as_nanos(),
            point.self_rss.map(|b| b.to_string()).unwrap_or_default(),
        ));
    }
    match &args.out {
        Some(path) => std::fs::write(path, &csv).expect("write the CSV"),
        None => print!("{csv}"),
    }

    // The verdicts, against the plan's fixed thresholds.
    let first = points.first().expect("at least one point");
    let last = points.last().expect("at least one point");
    let s = first.per_log;
    let m1 = first.deep_size;
    let mut crossed = false;
    let mut verdict = |name: &str, met: bool, detail: String| {
        eprintln!("{}: {name}: {detail}", if met { "met" } else { "CROSSED" });
        crossed |= !met;
    };
    verdict(
        &format!("dispatch p50 < 1 us at N={} (hit)", last.n),
        last.hit.p50 < P50_CEILING,
        format!("{:?}", last.hit.p50),
    );
    verdict(
        &format!("dispatch p50 < 1 us at N={} (miss)", last.n),
        last.miss.p50 < P50_CEILING,
        format!("{:?}", last.miss.p50),
    );
    let ratio = |a: Duration, b: Duration| a.as_nanos() as f64 / b.as_nanos().max(1) as f64;
    verdict(
        &format!(
            "hit p50 at N={} <= {GROWTH_CEILING} x p50 at N={}",
            last.n, first.n
        ),
        ratio(last.hit.p50, first.hit.p50) <= GROWTH_CEILING,
        format!("{:.2}x", ratio(last.hit.p50, first.hit.p50)),
    );
    verdict(
        &format!(
            "miss p50 at N={} <= {GROWTH_CEILING} x p50 at N={}",
            last.n, first.n
        ),
        ratio(last.miss.p50, first.miss.p50) <= GROWTH_CEILING,
        format!("{:.2}x", ratio(last.miss.p50, first.miss.p50)),
    );
    for point in &points {
        let ceiling = m1 + (point.n - 1) * 2 * s;
        verdict(
            &format!("m({}) <= m(1) + (N-1) x 2 x s", point.n),
            point.deep_size <= ceiling,
            format!(
                "m={} B, ceiling={ceiling} B (m(1)={m1} B, s={s} B)",
                point.deep_size
            ),
        );
        verdict(
            &format!("one member table at N={}", point.n),
            point.member_tables == 1,
            format!("{} table(s)", point.member_tables),
        );
    }
    if crossed {
        eprintln!("a threshold was crossed; the CSV above stands as measured");
        std::process::exit(1);
    }
}
