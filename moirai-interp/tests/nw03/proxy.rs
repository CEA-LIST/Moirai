//! NW.03's instrument: a slow link between the joiner and one writer.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.03.md`. The joiner is given
//! the address of one of these instead of each writer's, so every frame it
//! sends a writer passes through here, as one newline-delimited JSON line (the
//! transport's `write_frame`). A link is a delay and nothing else: frames are
//! never dropped, never changed and never reordered. What the harness chooses
//! is how long each of the joiner's frames takes to arrive, which an
//! asynchronous network may choose too.
//!
//! A link is in one of four states:
//!
//! - **closed**: nothing reaches the writer, and the proxy has not even dialled
//!   it, so for the writer the joiner does not exist yet; the joiner, which
//!   dialled the proxy, holds a link it believes is up;
//! - **up to a frame**: the proxy dials the writer and forwards the joiner's
//!   frames in order up to and including the first one of a given type for a
//!   given log, then holds every later one;
//! - **held**: dialled, nothing more from the joiner reaches the writer;
//! - **open**: everything held is flushed in order, and every later frame
//!   follows at once.
//!
//! Frames from the writer to the joiner always pass at once, once the writer
//! has been dialled. Every frame in either direction is recorded with its type,
//! its log and the member list it carries, so a report can say what each side
//! had been told.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use serde_json::Value;

/// One frame, as read off the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Arrival order across every link of one proxy, so "which link saw it
    /// first" has an answer.
    pub seq: u64,
    /// The frame's `type`.
    pub kind: String,
    /// The log the frame names, at the top level or inside the message it
    /// wraps.
    pub log_id: Option<String>,
    /// The member list the frame carries (the resolver of the message it
    /// wraps, or of a snapshot), empty when it carries none.
    pub members: Vec<String>,
    /// The line itself, without its newline.
    pub line: String,
}

impl Frame {
    fn parse(seq: u64, line: String) -> Frame {
        let value: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
        let kind = value["type"].as_str().unwrap_or("<unparsed>").to_string();
        let inner = ["event", "batch", "since", "snapshot"]
            .iter()
            .map(|field| &value[*field])
            .find(|inner| inner.is_object());
        let log_id = value["log_id"]
            .as_str()
            .or_else(|| inner.and_then(|inner| inner["log_id"].as_str()))
            .map(str::to_string);
        let members = inner
            .and_then(|inner| inner["resolver"].as_array())
            .map(|list| {
                list.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Frame {
            seq,
            kind,
            log_id,
            members,
            line,
        }
    }

    /// `true` for a frame of type `kind` naming the log `log_id`.
    pub fn is(&self, kind: &str, log_id: &str) -> bool {
        self.kind == kind && self.log_id.as_deref() == Some(log_id)
    }

    /// `type` and log, for reports.
    pub fn label(&self) -> String {
        match &self.log_id {
            Some(log) => format!("{}({})", self.kind, &log[..log.len().min(8)]),
            None => self.kind.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Gate {
    Closed,
    UpTo { kind: String, log_id: String },
    Held,
    Open,
}

struct State {
    gate: Gate,
    /// The joiner's frames not yet forwarded, oldest first.
    queue: VecDeque<Frame>,
    /// Every frame the joiner sent on this link, in order.
    seen: Vec<Frame>,
    /// Every frame the writer sent the joiner on this link, in order.
    back: Vec<Frame>,
    /// The joiner's end, once it has dialled.
    joiner: Option<TcpStream>,
    /// The writer's end, once the proxy has dialled it.
    writer: Option<TcpStream>,
    /// The first thing that went wrong on this link, if anything did.
    error: Option<String>,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    stop: AtomicBool,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One link: the joiner dials `port`, and the proxy dials `upstream` when told.
pub struct Link {
    pub writer: String,
    pub port: u16,
    upstream: u16,
    shared: Arc<Shared>,
}

/// The joiner's links to every writer, one [`Link`] each.
pub struct Proxy {
    pub links: Vec<Link>,
}

impl Proxy {
    /// A closed link to each `(writer id, writer's listen port)`, each on a
    /// port the kernel gave it.
    pub fn start(writers: &[(&str, u16)]) -> Result<Proxy, String> {
        let seq = Arc::new(AtomicU64::new(0));
        let mut links = Vec::new();
        for (writer, upstream) in writers {
            let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
            let port = listener.local_addr().map_err(|err| err.to_string())?.port();
            let shared = Arc::new(Shared {
                state: Mutex::new(State {
                    gate: Gate::Closed,
                    queue: VecDeque::new(),
                    seen: Vec::new(),
                    back: Vec::new(),
                    joiner: None,
                    writer: None,
                    error: None,
                }),
                wake: Condvar::new(),
                stop: AtomicBool::new(false),
            });
            spawn_acceptor(listener, Arc::clone(&shared), Arc::clone(&seq));
            spawn_pump(Arc::clone(&shared), *upstream, Arc::clone(&seq));
            links.push(Link {
                writer: writer.to_string(),
                port,
                upstream: *upstream,
                shared,
            });
        }
        Ok(Proxy { links })
    }

    fn link(&self, writer: &str) -> &Link {
        self.links
            .iter()
            .find(|link| link.writer == writer)
            .unwrap_or_else(|| panic!("no link to {writer}"))
    }

    /// The port the joiner must dial to reach `writer`.
    pub fn port(&self, writer: &str) -> u16 {
        self.link(writer).port
    }

    /// The link whose joiner-side frames first held one of type `kind` for
    /// `log_id`, and that frame's arrival order.
    pub fn first(&self, kind: &str, log_id: &str) -> Option<(String, u64)> {
        self.links
            .iter()
            .filter_map(|link| {
                link.shared
                    .lock()
                    .seen
                    .iter()
                    .find(|frame| frame.is(kind, log_id))
                    .map(|frame| (link.writer.clone(), frame.seq))
            })
            .min_by_key(|(_, seq)| *seq)
    }

    /// Dial `writer` and forward the joiner's frames up to and including the
    /// first of type `kind` for `log_id`; hold everything after it.
    pub fn open_up_to(&self, writer: &str, kind: &str, log_id: &str) {
        let link = self.link(writer);
        let mut state = link.shared.lock();
        state.gate = Gate::UpTo {
            kind: kind.to_string(),
            log_id: log_id.to_string(),
        };
        link.shared.wake.notify_all();
    }

    /// Dial `writer` if it has not been, and forward everything held and
    /// everything that follows.
    pub fn release(&self, writer: &str) {
        let link = self.link(writer);
        let mut state = link.shared.lock();
        state.gate = Gate::Open;
        link.shared.wake.notify_all();
    }

    /// `true` once the link has been opened and every frame it held has been
    /// written to the writer.
    pub fn drained(&self, writer: &str) -> bool {
        let state = self.link(writer).shared.lock();
        state.gate == Gate::Open && state.queue.is_empty() && state.writer.is_some()
    }

    /// Every frame the joiner sent `writer`, forwarded or not.
    pub fn seen(&self, writer: &str) -> Vec<Frame> {
        self.link(writer).shared.lock().seen.clone()
    }

    /// The joiner's frames to `writer` still held.
    pub fn held(&self, writer: &str) -> Vec<Frame> {
        self.link(writer)
            .shared
            .lock()
            .queue
            .iter()
            .cloned()
            .collect()
    }

    /// Every frame `writer` sent the joiner.
    pub fn back(&self, writer: &str) -> Vec<Frame> {
        self.link(writer).shared.lock().back.clone()
    }

    /// The first failure on any link, if any.
    pub fn error(&self) -> Option<String> {
        self.links.iter().find_map(|link| {
            link.shared
                .lock()
                .error
                .clone()
                .map(|why| format!("link to {}: {why}", link.writer))
        })
    }

    /// One line per link: its state, what it forwarded and what it holds.
    pub fn describe(&self) -> String {
        self.links
            .iter()
            .map(|link| {
                let state = link.shared.lock();
                let forwarded = state.seen.len() - state.queue.len();
                let labels = |frames: &mut dyn Iterator<Item = &Frame>| -> String {
                    frames.map(Frame::label).collect::<Vec<_>>().join(" ")
                };
                format!(
                    "link to {} ({:?}): joiner sent {} frames, forwarded {} [{}], held {} [{}]; \
                     writer sent {} [{}]{}",
                    link.writer,
                    state.gate,
                    state.seen.len(),
                    forwarded,
                    labels(&mut state.seen.iter().take(forwarded)),
                    state.queue.len(),
                    labels(&mut state.queue.iter()),
                    state.back.len(),
                    labels(&mut state.back.iter()),
                    state
                        .error
                        .as_ref()
                        .map(|why| format!("; error: {why}"))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        for link in &self.links {
            link.shared.stop.store(true, Ordering::SeqCst);
            let state = link.shared.lock();
            for stream in [&state.joiner, &state.writer].into_iter().flatten() {
                let _ = stream.shutdown(Shutdown::Both);
            }
            link.shared.wake.notify_all();
        }
    }
}

/// Accept the joiner (it dials once) and read its frames into the queue.
fn spawn_acceptor(listener: TcpListener, shared: Arc<Shared>, seq: Arc<AtomicU64>) {
    thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let reader = match stream.try_clone() {
            Ok(reader) => reader,
            Err(err) => {
                shared.lock().error = Some(format!("cloning the joiner's stream: {err}"));
                return;
            }
        };
        shared.lock().joiner = Some(stream);
        shared.wake.notify_all();
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else {
                break;
            };
            let frame = Frame::parse(seq.fetch_add(1, Ordering::SeqCst), line);
            let mut state = shared.lock();
            state.seen.push(frame.clone());
            state.queue.push_back(frame);
            shared.wake.notify_all();
        }
    });
}

/// Dial the writer when the gate first opens, then forward whatever the gate
/// lets through, in order.
fn spawn_pump(shared: Arc<Shared>, upstream: u16, seq: Arc<AtomicU64>) {
    thread::spawn(move || {
        loop {
            let mut state = shared.lock();
            while !shared.stop.load(Ordering::SeqCst)
                && (state.gate == Gate::Closed
                    || state.joiner.is_none()
                    || (state.writer.is_some() && !can_forward(&state)))
            {
                state = shared
                    .wake
                    .wait_timeout(state, Duration::from_millis(200))
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .0;
            }
            if shared.stop.load(Ordering::SeqCst) {
                return;
            }
            if state.writer.is_none() {
                drop(state);
                match TcpStream::connect(("127.0.0.1", upstream)) {
                    Ok(writer) => {
                        let _ = writer.set_nodelay(true);
                        let mut state = shared.lock();
                        let joiner = state.joiner.as_ref().and_then(|j| j.try_clone().ok());
                        let reader = writer.try_clone().ok();
                        state.writer = Some(writer);
                        match (joiner, reader) {
                            (Some(joiner), Some(reader)) => {
                                spawn_back(reader, joiner, Arc::clone(&shared), Arc::clone(&seq))
                            }
                            _ => state.error = Some("cloning a stream".to_string()),
                        }
                    }
                    Err(err) => {
                        shared.lock().error = Some(format!("dialling the writer: {err}"));
                        return;
                    }
                }
                continue;
            }
            // Forward what the gate allows, in order.
            let mut out = Vec::new();
            while can_forward(&state) {
                let frame = state.queue.pop_front().expect("can_forward saw a frame");
                if let Gate::UpTo { kind, log_id } = &state.gate
                    && frame.is(kind, log_id)
                {
                    state.gate = Gate::Held;
                }
                out.push(frame);
            }
            let mut writer = state
                .writer
                .as_ref()
                .and_then(|w| w.try_clone().ok())
                .expect("dialled above");
            drop(state);
            for frame in out {
                if let Err(err) = writer
                    .write_all(frame.line.as_bytes())
                    .and_then(|()| writer.write_all(b"\n"))
                    .and_then(|()| writer.flush())
                {
                    shared.lock().error = Some(format!("writing to the writer: {err}"));
                    return;
                }
            }
        }
    });
}

fn can_forward(state: &State) -> bool {
    !state.queue.is_empty() && matches!(state.gate, Gate::UpTo { .. } | Gate::Open)
}

/// Copy the writer's frames to the joiner, recording each.
fn spawn_back(writer: TcpStream, mut joiner: TcpStream, shared: Arc<Shared>, seq: Arc<AtomicU64>) {
    thread::spawn(move || {
        for line in BufReader::new(writer).lines() {
            let Ok(line) = line else {
                break;
            };
            let frame = Frame::parse(seq.fetch_add(1, Ordering::SeqCst), line);
            let written = joiner
                .write_all(frame.line.as_bytes())
                .and_then(|()| joiner.write_all(b"\n"))
                .and_then(|()| joiner.flush());
            shared.lock().back.push(frame);
            if let Err(err) = written {
                if !shared.stop.load(Ordering::SeqCst) {
                    shared.lock().error = Some(format!("writing to the joiner: {err}"));
                }
                break;
            }
        }
    });
}
