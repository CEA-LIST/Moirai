//! NW.16d, the part its tests share (vault `spec/items/NW.16d.md`): the
//! meter that reads what the router and the nodes receive, and the Zenoh
//! configurations the transport-level tests open.
//!
//! Ground truth never comes from the code under test. Bytes are the
//! kernel's TCP counters for each ESTABLISHED socket (`ss -Htine`, the same
//! source as NW.16b's O2 counters, `nw16b::tcp_bytes`), attributed to a
//! process by the socket's inode in `/proc/<pid>/fd` rather than by port, so
//! a link the router dials counts as well as one it accepts. A window reads
//! every socket a party holds at its start and at its end; a socket that
//! closes inside the window takes its counters with it, so such a window is
//! reported void (`NW16D METER WITNESS`), never read as a small number.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::{Duration, Instant};

use zenoh::Config;

use crate::nw16a::socket_inodes;

/// One ESTABLISHED TCP socket: local and remote ports, bytes the other end
/// acknowledged (sent), bytes received.
#[derive(Clone, Debug)]
pub struct Sock {
    pub inode: u64,
    pub local: u16,
    pub remote: u16,
    pub sent: u64,
    pub received: u64,
}

fn port_of(endpoint: &str) -> u16 {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        .unwrap_or(0)
}

/// Every ESTABLISHED TCP socket of this network namespace, by inode, from
/// `ss -Htine state established`: a header line (ports, `ino:<n>`) and an
/// indented line of counters, where a counter still at 0 may be absent.
pub fn sockets() -> Result<BTreeMap<u64, Sock>, String> {
    let output = Command::new("ss")
        .args(["-Htine", "state", "established"])
        .output()
        .map_err(|e| format!("set-up: running ss: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "set-up: ss failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut found = BTreeMap::new();
    let mut current: Option<Sock> = None;
    let counter = |line: &str, key: &str| {
        line.split_whitespace()
            .find_map(|f| f.strip_prefix(key))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    for line in text.lines() {
        if !line.starts_with(char::is_whitespace) {
            if let Some(sock) = current.take() {
                found.insert(sock.inode, sock);
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            let inode = counter(line, "ino:");
            current = (fields.len() >= 4 && inode != 0).then(|| Sock {
                inode,
                local: port_of(fields[2]),
                remote: port_of(fields[3]),
                sent: 0,
                received: 0,
            });
        } else if let Some(sock) = current.as_mut() {
            sock.sent = counter(line, "bytes_acked:");
            sock.received = counter(line, "bytes_received:");
        }
    }
    if let Some(sock) = current.take() {
        found.insert(sock.inode, sock);
    }
    Ok(found)
}

/// One process the meter reads: its sockets by inode, less those on
/// `http` (a node's HTTP port, which the test's own requests reach).
#[derive(Clone, Debug)]
pub struct Party {
    pub name: String,
    pub pid: u32,
    pub http: Option<u16>,
}

impl Party {
    pub fn new(name: &str, pid: u32, http: Option<u16>) -> Party {
        Party {
            name: name.to_string(),
            pid,
            http,
        }
    }
}

fn held(all: &BTreeMap<u64, Sock>, party: &Party) -> BTreeMap<u64, Sock> {
    let inodes = socket_inodes(party.pid);
    all.iter()
        .filter(|(inode, sock)| inodes.contains(inode) && Some(sock.local) != party.http)
        .map(|(inode, sock)| (*inode, sock.clone()))
        .collect()
}

/// What one party received and sent in a window.
#[derive(Clone, Debug)]
pub struct Share {
    pub name: String,
    pub received: u64,
    pub sent: u64,
    pub sockets_before: usize,
    pub sockets_after: usize,
    /// Sockets held at the start and gone at the end: their bytes in the
    /// window are unknown.
    pub closed: Vec<u64>,
}

/// A window of the meter: every party's sockets at its start.
pub struct Window {
    label: String,
    parties: Vec<Party>,
    before: Vec<BTreeMap<u64, Sock>>,
    at: Instant,
}

/// What a window read.
#[derive(Clone, Debug)]
pub struct Reading {
    pub label: String,
    pub took: Duration,
    pub shares: Vec<Share>,
}

impl Window {
    pub fn open(label: &str, parties: &[Party]) -> Result<Window, String> {
        let all = sockets()?;
        let before = parties.iter().map(|p| held(&all, p)).collect();
        Ok(Window {
            label: label.to_string(),
            parties: parties.to_vec(),
            before,
            at: Instant::now(),
        })
    }

    pub fn close(self) -> Result<Reading, String> {
        let all = sockets()?;
        let took = self.at.elapsed();
        let mut shares = Vec::new();
        for (party, before) in self.parties.iter().zip(&self.before) {
            let after = held(&all, party);
            let mut share = Share {
                name: party.name.clone(),
                received: 0,
                sent: 0,
                sockets_before: before.len(),
                sockets_after: after.len(),
                closed: before
                    .keys()
                    .filter(|inode| !after.contains_key(inode))
                    .copied()
                    .collect(),
            };
            for (inode, sock) in &after {
                let (sent0, received0) = before
                    .get(inode)
                    .map_or((0, 0), |old| (old.sent, old.received));
                share.received += sock.received.saturating_sub(received0);
                share.sent += sock.sent.saturating_sub(sent0);
            }
            shares.push(share);
        }
        Ok(Reading {
            label: self.label,
            took,
            shares,
        })
    }
}

impl Reading {
    pub fn of(&self, name: &str) -> &Share {
        self.shares
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("set-up: no party {name} in window {}", self.label))
    }

    /// One line for the record: every party's bytes in the window.
    pub fn line(&self) -> String {
        let parts: Vec<String> = self
            .shares
            .iter()
            .map(|s| {
                format!(
                    "{} received {} B sent {} B ({} sockets, {} after, closed {:?})",
                    s.name, s.received, s.sent, s.sockets_before, s.sockets_after, s.closed
                )
            })
            .collect();
        format!("{} in {:?}: {}", self.label, self.took, parts.join("; "))
    }

    /// The window is void if a socket of `name` closed inside it.
    pub fn witness(&self, name: &str) -> Result<(), String> {
        let share = self.of(name);
        if share.closed.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "NW16D METER WITNESS: in window {}, {} closed the sockets {:?}, whose bytes in the \
                 window are unknown, so this window measures nothing: {}",
                self.label,
                name,
                share.closed,
                self.line()
            ))
        }
    }
}

/// Class names that sort apart and stay four characters long, so each is
/// four operations (`si01::Uml::named_class` writes one per character).
pub fn names(tag: char, count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{tag}{i:03}")).collect()
}

pub fn name_set(names: &[String]) -> BTreeSet<String> {
    names.iter().cloned().collect()
}

// ---------------------------------------------------------------------------
// Zenoh configurations of the transport-level tests, written here by hand
// (as NW.15's `zenoh_rig` writes its own), not taken from the code under
// test: what `transport_config.rs` builds for `ZENOH_ROUTER` in peer mode
// and in client mode, on loopback.
// ---------------------------------------------------------------------------

fn set(config: &mut Config, key: &str, value: &str) -> Result<(), String> {
    config
        .insert_json5(key, value)
        .map_err(|e| format!("set-up: Zenoh configuration {key} = {value}: {e}"))
}

/// A peer that listens on 127.0.0.1 at a port the kernel chooses and dials
/// `connect`; gossip on, one hop, when `gossip`; multicast scouting off.
pub fn peer_config(connect: &[String], gossip: bool) -> Result<Config, String> {
    let mut config = Config::default();
    set(&mut config, "mode", "\"peer\"")?;
    set(&mut config, "listen/endpoints", "[\"tcp/127.0.0.1:0\"]")?;
    set(
        &mut config,
        "connect/endpoints",
        &serde_json::to_string(connect).map_err(|e| e.to_string())?,
    )?;
    set(&mut config, "scouting/multicast/enabled", "false")?;
    set(
        &mut config,
        "scouting/gossip/enabled",
        if gossip { "true" } else { "false" },
    )?;
    if gossip {
        set(&mut config, "scouting/gossip/multihop", "false")?;
    }
    Ok(config)
}

/// A client of the router at `router`: listens on nothing, retries the
/// router until it answers; multicast scouting off.
pub fn client_config(router: &str) -> Result<Config, String> {
    let mut config = Config::default();
    set(&mut config, "mode", "\"client\"")?;
    set(&mut config, "listen/endpoints", "[]")?;
    set(
        &mut config,
        "connect/endpoints",
        &serde_json::to_string(&[router]).map_err(|e| e.to_string())?,
    )?;
    set(&mut config, "connect/timeout_ms", "-1")?;
    set(&mut config, "connect/exit_on_failure", "false")?;
    set(&mut config, "scouting/multicast/enabled", "false")?;
    Ok(config)
}

/// The multicast setting of a configuration, read back: the tests open no
/// session unless it reads `false`.
pub fn multicast_off(config: &Config) -> Result<(), String> {
    match config.get_json("scouting/multicast/enabled").as_deref() {
        Ok("false") => Ok(()),
        other => Err(format!(
            "set-up: refusing to open a session whose multicast scouting reads {other:?}"
        )),
    }
}
