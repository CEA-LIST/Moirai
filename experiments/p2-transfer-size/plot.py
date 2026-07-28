#!/usr/bin/env python3
"""Plot the phase-2 transfer-size figure from the CSV `run.sh` produces.

    python3 -m venv .venv && .venv/bin/pip install matplotlib
    .venv/bin/python plot.py

One panel, not two. The question design §5.1 asks is whether the transfer stays
bounded for a session-sized model, and the answer is a single line and its
slope; splitting it would suggest a relationship where there is only a
proportion. The stacked bands show *where* the bytes are, which is the part
that turns the number into a diagnosis.

Colours are Okabe-Ito, the same pair E4 uses, so the two figures read as one
family.
"""

import csv
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = pathlib.Path(__file__).parent

LOG = "#0072B2"       # Okabe-Ito blue
SNAPSHOT = "#D55E00"  # Okabe-Ito vermillion
RENDERED = "#6b6b66"
SURFACE = "#fcfcfb"
INK = "#1b1b1a"
MUTED = "#6b6b66"


def main():
    with open(HERE / "size.csv", newline="") as handle:
        rows = [{k: int(float(v)) for k, v in row.items()} for row in csv.DictReader(handle)]

    ops = [r["ops"] for r in rows]
    log = [r["log_bytes"] / 1024 for r in rows]
    snapshot = [r["snapshot_bytes"] / 1024 for r in rows]
    rendered = [r["rendered_state_bytes"] / 1024 for r in rows]

    fig, axis = plt.subplots(figsize=(8.5, 5.0), layout="constrained")
    fig.patch.set_facecolor(SURFACE)
    axis.set_facecolor(SURFACE)
    axis.grid(True, color="#e4e4e0", linewidth=0.8)
    axis.set_axisbelow(True)
    for side in ("top", "right"):
        axis.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        axis.spines[side].set_color("#d6d6d1")
    axis.tick_params(colors=MUTED, labelsize=9)

    axis.stackplot(
        ops,
        log,
        snapshot,
        colors=[LOG, SNAPSHOT],
        labels=["compacted log", "clock, members and the events above the frontier"],
        edgecolor=SURFACE,
        linewidth=0.6,
    )
    axis.plot(
        ops,
        rendered,
        color=RENDERED,
        linewidth=1.6,
        linestyle=(0, (4, 3)),
        label="the rendered state, for scale",
    )

    total = log[-1] + snapshot[-1]
    axis.annotate(
        f"{total:.0f} KiB",
        xy=(ops[-1], total),
        xytext=(6, 0),
        textcoords="offset points",
        color=INK,
        fontsize=9,
        va="center",
    )
    slope = (log[-1] + snapshot[-1] - log[0] - snapshot[0]) * 1024 / (ops[-1] - ops[0])
    axis.annotate(
        f"{slope:.0f} bytes per operation, and no plateau",
        xy=(ops[len(ops) // 2], (log + snapshot)[0]),
        xytext=(ops[1], total * 0.72),
        color=INK,
        fontsize=10,
    )

    axis.set_xlabel("operations delivered", color=INK, fontsize=10)
    axis.set_ylabel("state transfer (KiB on the wire)", color=INK, fontsize=10)
    axis.set_title(
        "A joiner's state transfer grows with the history, not with the model",
        color=INK,
        fontsize=12,
        loc="left",
        pad=12,
    )
    axis.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=INK)

    out = HERE / "p2-transfer-size.png"
    fig.savefig(out, dpi=160, facecolor=SURFACE)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
