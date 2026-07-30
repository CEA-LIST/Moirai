//! The page, in one string.
//!
//! Fully self-contained on purpose: no CDN, no external font, no external
//! script. The rig it watches routinely runs on an isolated Docker network or a
//! laptop with no route to the internet, and a dashboard that needs a CDN to
//! render is a dashboard that is blank exactly when it is wanted.
//!
//! It is also the only renderer. The initial paint and every subsequent update
//! consume the same `snapshot` shape, so there is no second code path that can
//! drift from the streaming one.

pub const PAGE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Moirai — live session</title>
<style>
  :root {
    --bg:#0e1116; --panel:#161b22; --line:#262d36; --text:#c9d1d9; --dim:#7d8590;
    --ok:#3fb950; --warn:#d29922; --bad:#f85149; --accent:#58a6ff; --local:#bc8cff;
    /* Relayed edges. Distinct from every other line on the graph, and from
       --local, which is the ring an origination draws. */
    --relay:#39c5cf;
  }
  * { box-sizing:border-box }
  body {
    margin:0; background:var(--bg); color:var(--text);
    font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  }
  header { padding:10px 14px; border-bottom:1px solid var(--line); display:flex;
           align-items:baseline; gap:14px; flex-wrap:wrap }
  h1 { font-size:14px; margin:0; letter-spacing:.06em; text-transform:uppercase }
  h1 span { color:var(--dim); text-transform:none; letter-spacing:0 }
  .tiles { display:flex; gap:8px; flex-wrap:wrap; margin-left:auto }
  .tile { background:var(--panel); border:1px solid var(--line); border-radius:5px;
          padding:4px 10px; min-width:88px }
  .tile b { display:block; font-size:16px; font-weight:600 }
  .tile i { font-style:normal; color:var(--dim); font-size:10px;
            text-transform:uppercase; letter-spacing:.06em }
  main { display:grid; grid-template-columns:minmax(0,3fr) minmax(0,2fr); gap:12px; padding:12px }
  @media (max-width:1100px) { main { grid-template-columns:1fr } }
  section { background:var(--panel); border:1px solid var(--line); border-radius:6px;
            overflow:hidden; min-width:0 }
  section > h2 { margin:0; padding:7px 10px; font-size:11px; letter-spacing:.08em;
                 text-transform:uppercase; color:var(--dim);
                 border-bottom:1px solid var(--line) }
  .scroll { max-height:46vh; overflow:auto }
  table { width:100%; border-collapse:collapse }
  th { text-align:left; font-weight:500; color:var(--dim); font-size:10px;
       text-transform:uppercase; letter-spacing:.05em; padding:5px 8px;
       position:sticky; top:0; background:var(--panel); border-bottom:1px solid var(--line) }
  td { padding:5px 8px; border-bottom:1px solid #1d232b; white-space:nowrap }
  tr.stale td { color:#4a525c }
  tr.diverged { background:rgba(248,81,73,.10) }
  tr.diverged td.digest { color:var(--bad); font-weight:700 }
  .dot { display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:6px }
  .dot.up { background:var(--ok) } .dot.down { background:#4a525c }
  .num { text-align:right; font-variant-numeric:tabular-nums }
  .frozen { color:var(--warn) }
  #feed { list-style:none; margin:0; padding:0; max-height:46vh; overflow:auto }
  #feed li { padding:5px 10px; border-bottom:1px solid #1d232b; display:flex;
             gap:7px; align-items:baseline; flex-wrap:wrap }
  .who { color:var(--accent); font-weight:600 }
  .who.local { color:var(--local) }
  .chip { font-size:10px; padding:0 5px; border-radius:9px; border:1px solid var(--line);
          color:var(--dim) }
  .chip.applied { color:var(--ok); border-color:rgba(63,185,80,.4) }
  .chip.redundant { color:var(--bad); border-color:rgba(248,81,73,.4) }
  .chip.reset { color:var(--warn); border-color:rgba(210,153,34,.4) }
  .chip.won { color:var(--accent); border-color:rgba(88,166,255,.4) }
  .op { color:var(--text) }
  .kind { font-size:10px; font-weight:700; letter-spacing:.04em; text-transform:uppercase;
          padding:0 5px; border-radius:3px; border:1px solid var(--line); color:var(--dim) }
  .kind.k-insert { color:var(--ok); border-color:rgba(63,185,80,.4) }
  .kind.k-update { color:var(--accent); border-color:rgba(88,166,255,.4) }
  .kind.k-remove { color:var(--bad); border-color:rgba(248,81,73,.4) }
  .kind.k-clear { color:var(--warn); border-color:rgba(210,153,34,.4) }
  .target { color:var(--text); font-weight:600 }
  .ms { margin-left:auto; color:var(--dim); font-variant-numeric:tabular-nums }
  .states { display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr));
            gap:10px; padding:10px }
  .states article { border:1px solid var(--line); border-radius:5px; padding:8px;
                    min-width:0 }
  .states article.diverged { border-color:var(--bad) }
  .states h3 { margin:0 0 5px; font-size:12px; display:flex; gap:8px; align-items:baseline }
  .states pre { margin:0; white-space:pre-wrap; word-break:break-word;
                color:var(--dim); max-height:150px; overflow:auto; font-size:11px }
  .empty { padding:16px; color:var(--dim) }
  section > h2.bar { display:flex; align-items:center; gap:10px }
  .legend { display:flex; gap:11px; flex-wrap:wrap; margin-left:auto;
            text-transform:none; letter-spacing:0; font-size:10px }
  .legend span { display:inline-flex; align-items:center; gap:4px }
  .legend i { width:8px; height:8px; border-radius:50%; display:inline-block;
              font-style:normal }
  .legend i.applied { background:var(--ok) }
  .legend i.redundant { background:var(--bad) }
  .legend i.origin { background:var(--local) }
  .legend i.stale { background:#4a525c }
  .legend i.diverged { background:none; border:2px solid var(--bad) }
  /* A bar, not a dot: the thing being described is an edge. */
  .legend i.relayed { width:14px; height:0; border-radius:0;
                      border-top:2px dashed var(--relay) }
  #graphwrap { position:relative; height:min(48vh,400px); min-height:240px }
  #graph { display:block; width:100%; height:100% }
  #tip { position:absolute; pointer-events:none; z-index:2; display:none;
         background:#0b0f14; border:1px solid var(--line); border-radius:4px;
         padding:5px 7px; font-size:11px; line-height:1.4; white-space:pre;
         color:var(--text) }
  footer { padding:8px 14px; color:var(--dim); font-size:11px;
           border-top:1px solid var(--line) }
</style>
</head>
<body>
<header>
  <h1>Moirai <span>live session</span></h1>
  <div class="tiles">
    <div class="tile"><i>replicas</i><b id="t-total">0</b></div>
    <div class="tile"><i>agreeing</i><b id="t-agree">0 / 0</b></div>
    <div class="tile"><i>stale</i><b id="t-stale">0</b></div>
    <div class="tile"><i>prop p50</i><b id="t-p50">–</b></div>
    <div class="tile"><i>prop p95</i><b id="t-p95">–</b></div>
    <div class="tile"><i>reports</i><b id="t-reports">0</b></div>
  </div>
</header>

<section style="margin:12px 12px 0">
  <h2 class="bar">Network
    <span class="legend">
      <span><i class="origin"></i>originated here</span>
      <span><i class="applied"></i>delivered &amp; applied</span>
      <span><i class="redundant"></i>discarded as redundant</span>
      <span><i class="relayed"></i>relayed edge</span>
      <span><i class="diverged"></i>diverged</span>
      <span><i class="stale"></i>stale</span>
    </span>
  </h2>
  <div id="graphwrap"><canvas id="graph"></canvas><div id="tip"></div></div>
</section>

<main>
  <section>
    <h2>Replicas</h2>
    <div class="scroll">
      <table>
        <thead><tr>
          <th>replica</th><th>uptime</th><th>last seen</th>
          <th class="num">peers</th><th class="num">known</th>
          <th class="num">stable</th><th class="num">retained</th>
          <th class="num">delivered</th><th class="num">applied</th>
          <th>digest</th>
        </tr></thead>
        <tbody id="rows"><tr><td colspan="10" class="empty">waiting for a replica to report…</td></tr></tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>Operations &amp; resolution</h2>
    <ul id="feed"><li class="empty">nothing delivered yet</li></ul>
  </section>
</main>

<section style="margin:0 12px 12px">
  <h2>Model state per replica</h2>
  <div class="states" id="states"><div class="empty">waiting…</div></div>
</section>

<footer id="foot">
  Propagation is measured between each replica&rsquo;s own clock, so it carries
  their skew; the spread across replicas is the signal, not the absolute value.
  Stale replicas are greyed out and kept — a replica that stops reporting is the
  silent-member case, not an absence. On the graph, a pulse is one delivery
  running from the replica that originated the operation to the replica that
  just delivered it; a ring opening out of a replica is an operation it
  originated itself. Edges are what the replicas report as their peers, dashed
  when only one end still claims the link, and drawn in a long teal dash when
  the pair has no direct connection and is talking through a relay.
</footer>

<script>
const $ = (id) => document.getElementById(id);

function ms(v) {
  if (v === null || v === undefined) return "–";
  if (v < 1000) return v + "ms";
  if (v < 60000) return (v / 1000).toFixed(1) + "s";
  return Math.floor(v / 60000) + "m" + Math.floor((v % 60000) / 1000) + "s";
}

function describeInner(v) {
  if (v === null || v === undefined) return "?";
  if (typeof v === "string") return v;
  if (v.String && v.String.Insert)
    return "insert '" + v.String.Insert.content + "' @" + v.String.Insert.pos;
  if (v.Number && v.Number.Inc !== undefined) return "+" + v.Number.Inc;
  if (v.Boolean !== undefined) return String(v.Boolean).toLowerCase();
  if (v.Array) return "array " + JSON.stringify(v.Array);
  return JSON.stringify(v);
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, function (c) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
  });
}

// The operation *kind*, the key it targets, and the payload, kept apart so the
// feed can lead with what happened rather than with an opaque key.
//
// The kinds are the CRDT's own vocabulary, not a prettier invention. There is
// deliberately no "add": on an add-wins map a key comes into existence on its
// first `Update`, so add and update are the same operation and separating them
// in the UI would claim a distinction the protocol does not make. A sequence
// `Insert` is the one genuine addition, and it is labelled as such.
function describeOp(op) {
  if (!op) return { kind: "evicted", target: "", detail: "(payload evicted)" };
  const jk = op.JsonKind;
  if (!jk) return { kind: "?", target: "", detail: JSON.stringify(op) };
  const o = jk.Object;
  if (o) {
    if (o.Update) {
      const inner = o.Update[1];
      const insert = inner && inner.String && inner.String.Insert;
      return {
        kind: insert ? "insert" : "update",
        target: o.Update[0],
        // The badge already says INSERT, so do not repeat the verb here.
        detail: insert
          ? "'" + insert.content + "' @" + insert.pos
          : describeInner(inner),
      };
    }
    if (o.Remove !== undefined) return { kind: "remove", target: o.Remove, detail: "" };
    if (o.Clear !== undefined) return { kind: "clear", target: "", detail: "" };
  }
  return { kind: "?", target: "", detail: JSON.stringify(jk) };
}

function renderRows(snap) {
  const tbody = $("rows");
  if (!snap.nodes.length) {
    tbody.innerHTML = '<tr><td colspan="10" class="empty">waiting for a replica to report…</td></tr>';
    return;
  }
  tbody.innerHTML = snap.nodes.map((n) => {
    const m = n.metrics || {};
    const cls = [n.stale ? "stale" : "", n.diverged ? "diverged" : ""].join(" ").trim();
    // A frozen stable prefix beside a climbing retained count is the whole
    // research problem; colour it so it is visible without reading numbers.
    const frozen = !n.stale && (m.retained_ops || 0) > 20 ? " frozen" : "";
    return "<tr class='" + cls + "'>"
      + "<td><span class='dot " + (n.stale ? "down" : "up") + "'></span>" + n.id + "</td>"
      + "<td>" + ms(n.uptime_ms) + "</td>"
      + "<td>" + ms(n.last_seen_ago_ms) + " ago</td>"
      + "<td class='num'>" + (m.peer_count ?? "–") + "</td>"
      + "<td class='num'>" + (m.known_replicas ?? "–") + "</td>"
      + "<td class='num'>" + (m.stable_prefix ?? "–") + "</td>"
      + "<td class='num" + frozen + "'>" + (m.retained_ops ?? "–") + "</td>"
      + "<td class='num'>" + (m.delivered_ops ?? "–") + "</td>"
      + "<td class='num'>" + (m.ops_applied ?? "–") + "</td>"
      + "<td class='digest'>" + (n.state_digest || "–").slice(0, 8) + "</td>"
      + "</tr>";
  }).join("");
}

function renderStates(snap) {
  const box = $("states");
  if (!snap.nodes.length) { box.innerHTML = '<div class="empty">waiting…</div>'; return; }
  box.innerHTML = snap.nodes.map((n) =>
    "<article class='" + (n.diverged ? "diverged" : "") + "'>"
    + "<h3>" + n.id
    + "<span style='color:var(--dim)'>" + (n.state_digest || "").slice(0, 8) + "</span>"
    + (n.diverged ? "<span style='color:var(--bad)'>diverged</span>"
                  : (n.stale ? "<span style='color:var(--dim)'>stale</span>"
                             : "<span style='color:var(--ok)'>agrees</span>"))
    + "</h3><pre>" + JSON.stringify(n.state, null, 1) + "</pre></article>"
  ).join("");
}

function feedItem(f) {
  const e = f.event;
  const li = document.createElement("li");
  const chips = [];
  if (e.applied) chips.push("<span class='chip applied'>applied</span>");
  if (e.redundant_on_arrival) chips.push("<span class='chip redundant'>discarded as redundant</span>");
  if (e.reset) chips.push("<span class='chip reset'>reset children</span>");
  for (const s of (e.superseded || [])) {
    chips.push("<span class='chip won'>" + (s.concurrent ? "beat " : "superseded ") + s.id + "</span>");
  }
  const d = describeOp(e.op);
  li.innerHTML =
      "<span class='who" + (e.local ? " local" : "") + "'>" + esc(e.origin) + ":" + esc(e.seq) + "</span>"
    + "<span class='kind k-" + esc(d.kind) + "'>" + esc(d.kind) + "</span>"
    + (d.target ? "<span class='target'>" + esc(d.target) + "</span>" : "")
    + (d.detail ? "<span class='op'>" + esc(d.detail) + "</span>" : "")
    + chips.join(" ")
    + "<span class='ms'>" + (e.local ? "on " + f.observer
        : "→ " + f.observer + (f.propagation_ms === null ? "" : " " + f.propagation_ms + "ms"))
    + "</span>";
  return li;
}

function pushFeed(entries) {
  const feed = $("feed");
  const first = feed.firstElementChild;
  if (first && first.classList.contains("empty")) feed.innerHTML = "";
  for (const f of entries) feed.insertBefore(feedItem(f), feed.firstChild);
  while (feed.childElementCount > 300) feed.removeChild(feed.lastChild);
}

/* ---------------------------------------------------------------- graph --

   One vertex per replica, edges from what the replicas themselves report as
   their peers, and a pulse per delivery travelling from the replica that
   originated the operation to the one that just delivered it.

   The layout is a ring (an ellipse, to use a wide panel) rather than a force
   simulation. Ring positions are a pure function of the replica list, which
   the dashboard already hands over sorted, so a vertex never wanders between
   snapshots; there is no solver left running while the session is idle; and
   every vertex sits on the hull, so none can end up hidden behind a
   neighbour. That is what keeps it readable from two replicas to twenty,
   where a spring layout would either tangle or need to keep iterating.

   Everything is drawn on one canvas. A pulse is four numbers in an array and
   never a DOM node, so a burst of deliveries cannot leave anything behind,
   and the animation loop exists only while there is something to animate.

   Nothing here asks the replicas for anything new: origin, deliverer and
   outcome are all already in the feed the page was showing as text. */

const CV = $("graph");
const CTX = CV.getContext("2d");
const TIP = $("tip");

/* Read once from `:root`, so the graph cannot drift from the table's colours. */
const C = {};
{
  const cs = getComputedStyle(document.documentElement);
  for (const k of ["ok", "warn", "bad", "local", "relay", "dim", "text"]) {
    C[k] = cs.getPropertyValue("--" + k).trim();
  }
}
const GREY = "#4a525c"; /* the same grey `tr.stale td` uses */

/** Pulses alive at once. Past this, new ones are dropped rather than queued —
    the same choice the replica's bounded report channel makes, and for the
    same reason: a backlog of animations would show the past, not the run. */
const MAX_PULSES = 64;
const TRAVEL_MS = 700;
const FLASH_MS = 500;

let verts = [];
let vindex = new Map();
let edges = [];
const pulses = [];
let frame = 0;
let vw = 0, vh = 0;

/** Schedule one frame. `tick` re-arms itself only while pulses remain, so an
    idle session costs nothing and no timer is left running. */
function requestFrame() {
  if (!frame) frame = requestAnimationFrame(tick);
}

function tick(ts) {
  frame = 0;
  draw(ts);
  if (pulses.length) frame = requestAnimationFrame(tick);
}

function resizeGraph() {
  const box = CV.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  vw = Math.max(1, Math.round(box.width));
  vh = Math.max(1, Math.round(box.height));
  CV.width = Math.round(vw * dpr);
  CV.height = Math.round(vh * dpr);
  CTX.setTransform(dpr, 0, 0, dpr, 0, 0);
  place();
}

function place() {
  const n = verts.length;
  const cx = vw / 2, cy = vh / 2;
  // Enough room outside the ring for a vertex plus its label at top and
  // bottom, where the label runs away from the ring rather than beside it.
  const ry = Math.max(30, vh / 2 - 44);
  // Capped against the height rather than filling the width: on a wide panel
  // an uncapped ellipse flattens into a line and the edges stop being
  // distinguishable from each other.
  const rx = Math.max(40, Math.min(vw / 2 - 78, ry * 2.4));
  for (let i = 0; i < n; i++) {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / n;
    verts[i].a = a;
    verts[i].x = n === 1 ? cx : cx + rx * Math.cos(a);
    verts[i].y = n === 1 ? cy : cy + ry * Math.sin(a);
  }
}

/** Vertex radius, shrinking with the population so twenty still fit. */
function nodeRadius() {
  return Math.max(5, Math.min(13, 110 / Math.max(verts.length, 1)));
}

function buildGraph(snap) {
  verts = (snap.nodes || []).map((n) => ({
    id: n.id,
    stale: !!n.stale,
    diverged: !!n.diverged,
    digest: (n.state_digest || "").slice(0, 8),
    metrics: n.metrics || {},
    x: 0, y: 0, a: 0,
  }));
  vindex = new Map(verts.map((v, i) => [v.id, i]));

  // An edge exists because a replica said so. Both ends saying "Connected" is
  // a link; one end alone is a link one side has already given up on, which is
  // exactly what a severed replica looks like from the outside — so it is
  // drawn, dashed, rather than silently deleted.
  //
  // `relayed` if *either* end says so. Deliberately not "both": the route is a
  // local decision, so a pair can disagree — one side reaching the other
  // directly while the other cannot — and drawing that as direct would hide
  // the half that is paying for a relay.
  const seen = new Map();
  for (const n of (snap.nodes || [])) {
    const i = vindex.get(n.id);
    const routes = (n.metrics && n.metrics.routes) || {};
    for (const p of (n.peers || [])) {
      const j = vindex.get(p.id);
      if (i === undefined || j === undefined || i === j) continue;
      const key = i < j ? i + ":" + j : j + ":" + i;
      let e = seen.get(key);
      if (!e) {
        e = { a: Math.min(i, j), b: Math.max(i, j), up: 0, relayed: false };
        seen.set(key, e);
      }
      if (p.status === "Connected") e.up++;
      if (routes[p.id] === "relayed") e.relayed = true;
    }
  }
  edges = Array.from(seen.values());
  place();
}

/** Colour of a delivery: what the CRDT did with it, same words as the feed. */
function tone(e) {
  if (e.redundant_on_arrival) return C.bad;
  if (e.applied) return C.ok;
  return C.warn;
}

/** Turn feed entries into pulses. Called only for streamed deliveries — the
    feed carried by the first snapshot is history, and animating history would
    claim traffic that is not happening. */
function animateFeed(entries) {
  for (const f of entries) {
    if (pulses.length >= MAX_PULSES) break;
    const e = f && f.event;
    if (!e) continue;
    if (e.local) {
      if (vindex.has(e.origin)) {
        pulses.push({ at: e.origin, t0: 0, dur: FLASH_MS, tone: C.local });
      }
    } else if (vindex.has(e.origin) && vindex.has(f.observer)) {
      pulses.push({ from: e.origin, to: f.observer, t0: 0, dur: TRAVEL_MS, tone: tone(e) });
    }
  }
  requestFrame();
}

function drawEdges(dash) {
  // A full mesh of twenty is 190 chords. Fading them as the population grows
  // keeps the topology legible as texture while leaving the pulses — the thing
  // actually being watched — on top of it.
  CTX.globalAlpha = Math.max(0.4, Math.min(1, 9 / Math.max(verts.length, 1)));
  for (const e of edges) {
    const A = verts[e.a], B = verts[e.b];
    const faded = A.stale || B.stale;
    const both = e.up >= 2;
    if (e.relayed) {
      // A colour of its own as well as a dash, because "dashed" is already
      // taken: it means one end has given up on the link. A relayed edge is
      // working — it is just not direct — so the two must not look alike.
      CTX.setLineDash(dash.relayed);
      CTX.lineWidth = both ? 1.6 : 1.2;
      CTX.strokeStyle = faded ? "#1f4c50" : C.relay;
      CTX.globalAlpha *= faded ? 0.5 : 0.85;
    } else {
      CTX.setLineDash(both ? dash.solid : dash.broken);
      CTX.lineWidth = both ? 1.4 : 1;
      CTX.strokeStyle = both
        ? (faded ? "#232c36" : "#31404f")
        : (faded ? "#212831" : "#2d3947");
    }
    CTX.beginPath();
    CTX.moveTo(A.x, A.y);
    CTX.lineTo(B.x, B.y);
    CTX.stroke();
    if (e.relayed) {
      CTX.globalAlpha = Math.max(0.4, Math.min(1, 9 / Math.max(verts.length, 1)));
    }
  }
  CTX.globalAlpha = 1;
  CTX.setLineDash(dash.solid);
}

function drawPulses(ts, r) {
  for (let k = pulses.length - 1; k >= 0; k--) {
    const p = pulses[k];
    if (!p.t0) p.t0 = ts;
    const t = (ts - p.t0) / p.dur;
    const from = p.from === undefined ? vindex.get(p.at) : vindex.get(p.from);
    const to = p.to === undefined ? from : vindex.get(p.to);
    // A replica can vanish from the roster mid-flight; drop the pulse rather
    // than draw it against a stale position.
    if (t >= 1 || from === undefined || to === undefined) {
      pulses.splice(k, 1);
      continue;
    }
    const A = verts[from], B = verts[to];
    if (p.from === undefined) {
      // Origination: a ring opening out of the replica that made the operation.
      CTX.globalAlpha = 1 - t;
      CTX.lineWidth = 2;
      CTX.strokeStyle = p.tone;
      CTX.beginPath();
      CTX.arc(A.x, A.y, r + 3 + t * 15, 0, 6.2832);
      CTX.stroke();
      CTX.globalAlpha = 1;
      continue;
    }
    // Delivery: a head running origin -> deliverer, with a short trail so the
    // direction is readable at a glance.
    const fade = t < 0.8 ? 1 : (1 - t) / 0.2;
    const tail = Math.max(0, t - 0.14);
    const hx = A.x + (B.x - A.x) * t, hy = A.y + (B.y - A.y) * t;
    CTX.globalAlpha = 0.35 * fade;
    CTX.lineWidth = 2.5;
    CTX.strokeStyle = p.tone;
    CTX.beginPath();
    CTX.moveTo(A.x + (B.x - A.x) * tail, A.y + (B.y - A.y) * tail);
    CTX.lineTo(hx, hy);
    CTX.stroke();
    CTX.globalAlpha = fade;
    CTX.fillStyle = p.tone;
    CTX.beginPath();
    CTX.arc(hx, hy, 3.2, 0, 6.2832);
    CTX.fill();
    CTX.globalAlpha = 1;
  }
}

function drawVerts(r, dash) {
  CTX.font = "10px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace";
  for (const v of verts) {
    if (v.diverged) {
      CTX.globalAlpha = 0.3;
      CTX.lineWidth = 2;
      CTX.strokeStyle = C.bad;
      CTX.beginPath();
      CTX.arc(v.x, v.y, r + 5, 0, 6.2832);
      CTX.stroke();
      CTX.globalAlpha = 1;
    }
    CTX.beginPath();
    CTX.arc(v.x, v.y, r, 0, 6.2832);
    CTX.fillStyle = "#0e1116";
    CTX.fill();
    // Stale is drawn, never removed: a replica that stopped reporting is the
    // silent-member case, and taking it off the graph would hide the finding.
    CTX.setLineDash(v.stale ? dash.broken : dash.solid);
    CTX.lineWidth = v.diverged ? 2.6 : 1.6;
    CTX.strokeStyle = v.stale ? GREY : (v.diverged ? C.bad : C.ok);
    CTX.stroke();
    CTX.setLineDash(dash.solid);

    const lx = v.x + Math.cos(v.a) * (r + 9);
    const ly = v.y + Math.sin(v.a) * (r + 9);
    CTX.textAlign = Math.abs(Math.cos(v.a)) < 0.3 ? "center"
      : (Math.cos(v.a) > 0 ? "left" : "right");
    CTX.textBaseline = Math.sin(v.a) > 0.3 ? "top"
      : (Math.sin(v.a) < -0.3 ? "bottom" : "middle");
    CTX.fillStyle = v.stale ? GREY : (v.diverged ? C.bad : C.text);
    CTX.fillText(v.id.slice(0, 6), lx, ly);
  }
}

/** How this replica reaches each peer, one line each, relayed ones first —
    they are the interesting half and a full mesh would otherwise bury them. */
function routeLines(routes) {
  const entries = Object.entries(routes || {});
  if (!entries.length) return "";
  entries.sort((x, y) =>
    (x[1] === "relayed" ? 0 : 1) - (y[1] === "relayed" ? 0 : 1)
    || x[0].localeCompare(y[0]));
  return "\n" + entries.map(([peer, route]) => "  " + route + " " + peer).join("\n");
}

function draw(ts) {
  CTX.clearRect(0, 0, vw, vh);
  const dash = { solid: [], broken: [3, 4], relayed: [7, 5] };
  if (!verts.length) {
    CTX.font = "12px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace";
    CTX.fillStyle = C.dim;
    CTX.textAlign = "center";
    CTX.textBaseline = "middle";
    CTX.fillText("waiting for a replica to report…", vw / 2, vh / 2);
    return;
  }
  const r = nodeRadius();
  drawEdges(dash);
  drawPulses(ts, r);
  drawVerts(r, dash);
}

CV.addEventListener("mousemove", (ev) => {
  const box = CV.getBoundingClientRect();
  const mx = ev.clientX - box.left, my = ev.clientY - box.top;
  const reach = nodeRadius() + 7;
  const hit = verts.find((v) =>
    (v.x - mx) * (v.x - mx) + (v.y - my) * (v.y - my) <= reach * reach);
  if (!hit) { TIP.style.display = "none"; return; }
  const m = hit.metrics || {};
  TIP.textContent = hit.id + "\n"
    + (hit.stale ? "stale — not reporting"
                 : (hit.diverged ? "diverged" : "agrees")) + "\n"
    + "digest " + (hit.digest || "–") + "\n"
    + "peers " + (m.peer_count ?? "–") + "   stable " + (m.stable_prefix ?? "–") + "\n"
    + "retained " + (m.retained_ops ?? "–") + "   applied " + (m.ops_applied ?? "–")
    + routeLines(m.routes);
  TIP.style.display = "block";
  TIP.style.left = Math.min(mx + 12, Math.max(0, vw - 160)) + "px";
  TIP.style.top = Math.max(0, my - 10) + "px";
});
CV.addEventListener("mouseleave", () => { TIP.style.display = "none"; });

// A hidden tab does not run `requestAnimationFrame`, so pulses would pile up
// against the cap and then all expire at once on return. Drop them instead.
document.addEventListener("visibilitychange", () => {
  if (document.hidden) pulses.length = 0;
});

new ResizeObserver(() => { resizeGraph(); requestFrame(); }).observe($("graphwrap"));
resizeGraph();
requestFrame();

function renderSnapshot(snap) {
  $("t-total").textContent = snap.total;
  $("t-agree").textContent = snap.agreeing + " / " + snap.live;
  $("t-stale").textContent = snap.total - snap.live;
  const p = snap.propagation || {};
  $("t-p50").textContent = p.count ? p.p50 + "ms" : "–";
  $("t-p95").textContent = p.count ? p.p95 + "ms" : "–";
  $("t-reports").textContent = snap.reports;
  $("t-agree").style.color =
    snap.live > 0 && snap.agreeing < snap.live ? "var(--bad)" : "var(--ok)";
  renderRows(snap);
  renderStates(snap);
  buildGraph(snap);
  requestFrame();
}

fetch("/api/snapshot").then((r) => r.json()).then((snap) => {
  renderSnapshot(snap);
  pushFeed(snap.feed || []);
});

const es = new EventSource("/api/stream");
es.addEventListener("snapshot", (m) => renderSnapshot(JSON.parse(m.data)));
es.addEventListener("feed", (m) => {
  const entries = JSON.parse(m.data);
  pushFeed(entries);
  animateFeed(entries);
});
es.onerror = () => { $("foot").style.color = "var(--warn)"; };
es.onopen = () => { $("foot").style.color = ""; };
</script>
</body>
</html>
"##;
