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
  silent-member case, not an absence.
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

function describeOp(op) {
  if (!op) return "(payload evicted)";
  const jk = op.JsonKind;
  if (!jk) return JSON.stringify(op);
  const o = jk.Object;
  if (o) {
    if (o.Update) return o.Update[0] + " ← " + describeInner(o.Update[1]);
    if (o.Remove !== undefined) return "remove " + o.Remove;
    if (o.Clear !== undefined) return "clear";
  }
  return JSON.stringify(jk);
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
  li.innerHTML =
      "<span class='who" + (e.local ? " local" : "") + "'>" + e.origin + ":" + e.seq + "</span>"
    + "<span class='op'>" + describeOp(e.op) + "</span>"
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
}

fetch("/api/snapshot").then((r) => r.json()).then((snap) => {
  renderSnapshot(snap);
  pushFeed(snap.feed || []);
});

const es = new EventSource("/api/stream");
es.addEventListener("snapshot", (m) => renderSnapshot(JSON.parse(m.data)));
es.addEventListener("feed", (m) => pushFeed(JSON.parse(m.data)));
es.onerror = () => { $("foot").style.color = "var(--warn)"; };
es.onopen = () => { $("foot").style.color = ""; };
</script>
</body>
</html>
"##;
