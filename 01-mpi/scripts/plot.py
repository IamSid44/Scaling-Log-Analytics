#!/usr/bin/env python3
"""Turn results/benchmark.csv into results/summary.md and the scaling plots.

Repetitions are collapsed with the median. Plots are skipped (with a message)
if matplotlib is missing, so this still runs on the cluster login node.

    python3 scripts/plot.py
"""

import csv
import os
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "results")
METRICS = ("t_solve", "t_comp", "t_comm", "t_read", "t_imbalance", "t_total")
STYLE = {"split": ("-", "o"), "scatter": ("--", "s")}
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#4a3aa7"]


def human(n):
    n = int(n)
    if n >= 10**6 and n % 10**6 == 0:
        return "{}M".format(n // 10**6)
    if n >= 1000 and n % 1000 == 0:
        return "{}K".format(n // 1000)
    return str(n)


def load():
    """-> {(n, io_mode, p): {metric: median}}, {n: bytes}, {p: nodes_used}"""
    path = os.path.join(RESULTS, "benchmark.csv")
    if not os.path.exists(path):
        sys.exit("error: no results/benchmark.csv -- run scripts/benchmark.sh first")

    runs, nbytes, hosts = {}, {}, {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                key = (int(row["n"]), row["io_mode"], int(row["p"]))
                vals = {m: float(row[m]) if row.get(m) else 0.0 for m in METRICS}
                nbytes[int(row["n"])] = int(row["bytes"])
                # How many distinct machines this P actually landed on, as
                # observed by the program itself. Below P it means ranks were
                # packed, and every table has to say so.
                if row.get("nodes"):
                    hosts[int(row["p"])] = int(row["nodes"])
            except (ValueError, KeyError):
                continue          # a failed run leaves blank fields; drop the row
            runs.setdefault(key, []).append(vals)
    if not runs:
        sys.exit("error: no usable rows in results/benchmark.csv")

    med = {k: {m: statistics.median(r[m] for r in v) for m in METRICS}
           for k, v in runs.items()}
    return med, nbytes, hosts


def summarise(med, nbytes):
    sizes = sorted({k[0] for k in med})
    modes = [m for m in ("split", "scatter") if any(k[1] == m for k in med)]
    procs = sorted({k[2] for k in med if k[1] != "sequential"})

    rows = {}
    for n in sizes:
        seq = med.get((n, "sequential", 1))
        t_seq = seq["t_solve"] if seq else None
        for mode in modes:
            base = med.get((n, mode, 1))
            t1 = base["t_solve"] if base else None
            for p in procs:
                cur = med.get((n, mode, p))
                if not cur:
                    continue
                t = cur["t_solve"]
                # S(P) is measured against the same program at P=1, so it
                # isolates parallel scaling. Speed-up against the sequential
                # program is reported alongside it: that one also exposes any
                # overhead the MPI version pays even at P=1.
                su = t1 / t if t1 and t > 0 else float("nan")
                mb = nbytes.get(n, 0) / 1048576.0
                rows[(n, mode, p)] = dict(
                    cur, n=n, io_mode=mode, p=p, speedup=su, efficiency=su / p,
                    speedup_vs_seq=t_seq / t if t_seq and t > 0 else float("nan"),
                    throughput=mb / t if t > 0 else float("nan"),
                    comm_pct=100.0 * cur["t_comm"] / t if t > 0 else float("nan"),
                    imbal_pct=100.0 * cur["t_imbalance"] / t if t > 0 else float("nan"))
    return sizes, modes, procs, rows


def packed_note(procs, hosts):
    """Which P values did not get one rank per node, in words."""
    packed = [p for p in procs if hosts.get(p, p) < p]
    if not packed:
        return []
    bits = ["P=%d on %d nodes (%d ranks/node)"
            % (p, hosts[p], p // hosts[p]) for p in packed]
    return ["> **Placement.** Every other row is one rank per node, so P is the "
            "number of machines. The exception is " + "; ".join(bits) + " -- the "
            "allocation had fewer machines than ranks, so those ranks were packed. "
            "That point is not comparable with the rest: co-resident ranks keep "
            "part of their collectives in shared memory, but they also share one "
            "machine's memory bandwidth and page cache. For a bandwidth-bound "
            "workload like this one the contention dominates, so the packed point "
            "can sit *below* the one-per-node trend rather than above it.", ""]


def write_summary(sizes, modes, procs, rows, med, hosts):
    out = ["# MPI benchmark results", "",
           "Median over the repetitions in `benchmark.csv`, timed with `MPI_Wtime` "
           "on the slowest rank.", "",
           "`t_solve` = read + parse + aggregate + reduce. `t_comm` is time inside "
           "MPI calls *after* a barrier has absorbed straggler wait; that wait is "
           "reported separately as load imbalance.", ""]
    out += packed_note(procs, hosts)
    out += ["| P | " + " | ".join(str(p) for p in procs) + " |",
            "|---|" + "|".join([":---:"] * len(procs)) + "|",
            "| nodes used | " + " | ".join(str(hosts.get(p, "?")) for p in procs) + " |",
            ""]

    def table(title, note, key, fmt, mode):
        body = []
        for n in sizes:
            cells = [fmt(rows[(n, mode, p)][key]) if (n, mode, p) in rows else "--"
                     for p in procs]
            if any(c != "--" for c in cells):
                body.append("| N = " + human(n) + " | " + " | ".join(cells) + " |")
        if not body:
            return []
        head = ["#### " + title, "", note, "",
                "| Input size | " + " | ".join("P=" + str(p) for p in procs) + " |",
                "|---|" + "|".join([":---:"] * len(procs)) + "|"]
        return head + body + [""]

    for mode in modes:
        out += ["## I/O mode: `" + mode + "`", ""]
        out += table("Execution time (s)", "Lower is better.",
                     "t_solve", lambda v: "%.3f" % v, mode)
        out += table("Speed-up  S(P) = T(1)/T(P)", "Baseline: this program at P=1.",
                     "speedup", lambda v: "%.2f" % v, mode)
        out += table("Speed-up vs. sequential", "Baseline: `loganalytics_seq`.",
                     "speedup_vs_seq", lambda v: "%.2f" % v, mode)
        out += table("Efficiency  E(P) = S(P)/P", "1.00 is perfect scaling.",
                     "efficiency", lambda v: "%.2f" % v, mode)
        out += table("Throughput (MB/s)", "Log bytes processed per second.",
                     "throughput", lambda v: "%.0f" % v, mode)
        out += table("Time in MPI calls (%)", "Actual data movement.",
                     "comm_pct", lambda v: "%.1f%%" % v, mode)
        out += table("Load imbalance (%)", "Straggler wait at the barrier.",
                     "imbal_pct", lambda v: "%.1f%%" % v, mode)

    if len(modes) == 2:
        out += ["## `split` vs. `scatter`", "",
                "| Input size | P | split (s) | scatter (s) | scatter/split |",
                "|---|:---:|:---:|:---:|:---:|"]
        for n in sizes:
            for p in procs:
                a, b = rows.get((n, "split", p)), rows.get((n, "scatter", p))
                if a and b:
                    out.append("| N = %s | %d | %.3f | %.3f | %.2fx |"
                               % (human(n), p, a["t_solve"], b["t_solve"],
                                  b["t_solve"] / a["t_solve"]))
        out.append("")

    out += ["## Sequential baseline", "", "| Input size | t_solve (s) |", "|---|:---:|"]
    for n in sizes:
        s = med.get((n, "sequential", 1))
        if s:
            out.append("| N = %s | %.3f |" % (human(n), s["t_solve"]))

    path = os.path.join(RESULTS, "summary.md")
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")
    print("wrote " + path)


def style(ax, xlabel, ylabel, title):
    ax.set_title(title, fontsize=12, pad=10, loc="left")
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(True, color="#e1e0d9", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def plots(sizes, modes, procs, rows, hosts):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib missing -- skipped the plots (summary.md was written)")
        return

    # A P whose ranks were packed onto fewer machines gets a starred tick, so
    # the plot never implies it was measured at the same placement.
    def tick(p):
        return "%d*" % p if hosts.get(p, p) < p else str(p)

    any_packed = any(hosts.get(p, p) < p for p in procs)
    footnote = "* ranks packed onto fewer nodes than P" if any_packed else None

    def line(key, ylabel, title, fname, ideal=None):
        fig, ax = plt.subplots(figsize=(7.6, 4.8), dpi=150)
        for i, n in enumerate(sizes):
            for mode in modes:
                pts = [(p, rows[(n, mode, p)][key]) for p in procs
                       if (n, mode, p) in rows]
                if not pts:
                    continue
                ls, mk = STYLE[mode]
                ax.plot([x for x, _ in pts], [y for _, y in pts],
                        color=COLORS[i % len(COLORS)], linewidth=2, linestyle=ls,
                        marker=mk, markersize=5.5,
                        label="N=%s (%s)" % (human(n), mode))
        if ideal:
            ax.plot(procs, [ideal(p) for p in procs], color="#898781",
                    linewidth=1.5, linestyle=":", label="ideal")
        ax.set_xticks(procs)
        ax.set_xticklabels([tick(p) for p in procs])
        style(ax, "MPI processes (P)", ylabel, title)
        ax.legend(frameon=False, fontsize=8, ncol=2)
        if footnote:
            ax.annotate(footnote, xy=(0.99, -0.16), xycoords="axes fraction",
                        ha="right", fontsize=8, color="#898781")
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS, fname))
        plt.close(fig)
        print("wrote " + os.path.join(RESULTS, fname))

    line("speedup", "S(P) = T(1)/T(P)",
         "Speed-up: split (solid) vs scatter (dashed)", "speedup.png", lambda p: p)
    line("efficiency", "E(P) = S(P)/P", "Efficiency", "efficiency.png", lambda p: 1.0)

    # Where the time goes, one panel per (size, mode): read / parse / imbalance / MPI.
    panels = [(n, m) for n in sizes for m in modes
              if any((n, m, p) in rows for p in procs)]
    if not panels:
        return
    ncols = min(2, len(panels))
    nrows = -(-len(panels) // ncols)
    fig, axes = plt.subplots(nrows, ncols, dpi=150, squeeze=False,
                             figsize=(7.6, 2.8 * nrows + 0.8))

    def read_t(r):
        return r["t_read"]

    def parse_t(r, mode):
        # split times its freads inside the parse region, so t_read is a subset
        # of t_comp there; scatter times rank 0's read on its own.
        return max(0.0, r["t_comp"] - r["t_read"]) if mode == "split" else r["t_comp"]

    for i, (n, mode) in enumerate(panels):
        ax = axes[i // ncols][i % ncols]
        layers = (("file read", COLORS[0], read_t),
                  ("parse+aggregate", COLORS[2], lambda r: parse_t(r, mode)),
                  ("imbalance", COLORS[3], lambda r: r["t_imbalance"]),
                  ("MPI", COLORS[1], lambda r: r["t_comm"]))
        bottom = [0.0] * len(procs)
        for label, color, get in layers:
            vals = [get(rows[(n, mode, p)]) if (n, mode, p) in rows else 0.0
                    for p in procs]
            ax.bar([tick(p) for p in procs], vals, bottom=bottom, color=color,
                   width=0.6, label=label)
            bottom = [a + b for a, b in zip(bottom, vals)]
        style(ax, "P", "seconds", "N=%s  [%s]" % (human(n), mode))
    for k in range(len(panels), nrows * ncols):
        axes[k // ncols][k % ncols].axis("off")

    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, fontsize=9, loc="upper center", ncol=4)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(os.path.join(RESULTS, "time_breakdown.png"))
    plt.close(fig)
    print("wrote " + os.path.join(RESULTS, "time_breakdown.png"))


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)
    med_, nbytes_, hosts_ = load()
    sizes_, modes_, procs_, rows_ = summarise(med_, nbytes_)
    write_summary(sizes_, modes_, procs_, rows_, med_, hosts_)
    plots(sizes_, modes_, procs_, rows_, hosts_)
