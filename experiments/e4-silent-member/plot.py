#!/usr/bin/env python3
"""Plot the E4 figure from the two CSVs `run.sh` produces.

    python3 -m venv .venv && .venv/bin/pip install matplotlib
    .venv/bin/python plot.py

Deliberately two stacked panels rather than one chart with two y-axes. The
stable prefix runs to the hundreds while the retained buffer sits at four in
the healthy case; putting both on one pair of axes would make the relationship
between them a property of the scaling rather than of the data.

Only the *survivors* are plotted. The severed replica stops answering when it
is cut — the rig has a single network — and the claim under test is about the
replicas that are still running.
"""

import csv
import pathlib
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = pathlib.Path(__file__).parent

# Okabe-Ito blue and vermillion. Fixed order, one hue per condition, and
# checked rather than eyeballed: worst adjacent pair is ΔE 21.9 under protan
# simulation and 31.2 for normal vision, both well clear of the floor.
COLOURS = {"baseline": "#0072B2", "silent": "#D55E00"}
LABELS = {
    "baseline": "all replicas live",
    "silent": "one replica silent from t+60 s",
}
SURFACE = "#fcfcfb"
INK = "#1b1b1a"
MUTED = "#6b6b66"


def read(name):
    """Return {elapsed: [row, ...]} for replicas that answered."""
    by_time = {}
    with open(HERE / f"{name}.csv", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["reachable"] != "1" or not row["stable_prefix"]:
                continue
            by_time.setdefault(int(row["elapsed_s"]), []).append(row)
    return by_time


def series(by_time, field, reduce=statistics.median):
    times = sorted(by_time)
    return times, [reduce([int(r[field]) for r in by_time[t]]) for t in times]


def main():
    runs = {name: read(name) for name in COLOURS}

    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(8.5, 6.4), sharex=True, layout="constrained"
    )
    fig.patch.set_facecolor(SURFACE)

    for axis in (top, bottom):
        axis.set_facecolor(SURFACE)
        axis.grid(True, color="#e4e4e0", linewidth=0.8)
        axis.set_axisbelow(True)
        for side in ("top", "right"):
            axis.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            axis.spines[side].set_color("#d6d6d1")
        axis.tick_params(colors=MUTED, labelsize=9)

    for name, by_time in runs.items():
        times, stable = series(by_time, "stable_prefix")
        top.plot(times, stable, color=COLOURS[name], linewidth=2, label=LABELS[name])
        times, retained = series(by_time, "retained_ops")
        bottom.plot(times, retained, color=COLOURS[name], linewidth=2, label=LABELS[name])

    sever_at = 60
    for axis in (top, bottom):
        axis.axvline(sever_at, color=MUTED, linewidth=1, linestyle=(0, (4, 3)))
    top.annotate(
        "replica severed",
        xy=(sever_at, top.get_ylim()[1] * 0.94),
        xytext=(sever_at + 6, top.get_ylim()[1] * 0.94),
        color=MUTED,
        fontsize=9,
        va="top",
    )

    # One direct label per line at its right end, so identity does not depend on
    # matching a legend swatch to a colour.
    for axis, field in ((top, "stable_prefix"), (bottom, "retained_ops")):
        for name, by_time in runs.items():
            times, values = series(by_time, field)
            axis.annotate(
                f"{round(values[-1])}",
                xy=(times[-1], values[-1]),
                xytext=(6, 0),
                textcoords="offset points",
                color=INK,
                fontsize=9,
                va="center",
            )

    top.set_ylabel("stable prefix (operations)", color=INK, fontsize=10)
    bottom.set_ylabel("retained operations", color=INK, fontsize=10)
    bottom.set_xlabel("seconds since the roster was complete", color=INK, fontsize=10)

    top.set_title(
        "One silent member freezes causal stability, and the log grows without bound",
        color=INK,
        fontsize=12,
        loc="left",
        pad=12,
    )
    top.legend(
        frameon=False, loc="upper left", fontsize=9, labelcolor=INK, bbox_to_anchor=(0, 0.88)
    )

    fig.savefig(HERE / "e4.png", dpi=160, facecolor=SURFACE)
    print(f"wrote {HERE / 'e4.png'}")

    for name, by_time in runs.items():
        _, stable = series(by_time, "stable_prefix")
        _, retained = series(by_time, "retained_ops")
        _, delivered = series(by_time, "delivered_ops")
        print(
            f"{name:9s} final: stable_prefix={round(stable[-1])} "
            f"retained_ops={round(retained[-1])} delivered_ops={round(delivered[-1])}"
        )


if __name__ == "__main__":
    main()
