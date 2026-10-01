#!/usr/bin/env python3
"""Measured results -> results/summary.md + results/*.png (the MPI vs MapReduce study).

    python3 scripts/plot.py

Reads (all under results/):
    cluster/benchmark_srun.csv   MapReduce on the cluster, driven by Slurm (scripts/run_slurm.sh)
    cluster/mpi_rerun.csv        the 01-mpi MPI program, SAME job, nodes and files
    cluster/mr_bsrun_*.log     for the sequential program's peak memory
    mpi_benchmark_original.csv        the original 01-mpi MPI sweep (a control)
    laptop/benchmark.csv         real Hadoop 3.3.6 + YARN + Streaming, one machine (WSL)

Rows whose `correct` column is 0 are dropped, with a warning. The plots need matplotlib;
summary.md is written regardless.
"""

import csv
import glob
import os
import re
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")

BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET = (
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7")
_RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"]


def ramp(k):
    if k <= 1:
        return [_RAMP[-1]]
    return [_RAMP[round(i * (len(_RAMP) - 1) / (k - 1))] for i in range(k)]
INK, INK2, GRID, GRAY = "#0b0b0b", "#52514e", "#e1e0d9", "#898781"
MB = 1024 * 1024

# Bytes each non-root MPI rank contributes to the MPI program's reductions, computed
# from loganalytics_mpi.cpp: 12 scalars + 2 x 64 server arrays + 2 x 512
# endpoint arrays + one count per 60 s interval (1,440 in the generated day),
# all int64. Independent of N.
MPI_REDUCE_BYTES_PER_RANK = 8 * (12 + 2 * 64 + 2 * 512 + 1440)


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------

def rows(path, check_correct=True):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        out = list(csv.DictReader(f))
    if check_correct:
        bad = [r for r in out if r.get("correct") == "0"]
        if bad:
            print("WARNING: %s: discarded %d row(s) whose output did not match the "
                  "sequential reference" % (os.path.relpath(path, ROOT), len(bad)),
                  file=sys.stderr)
        out = [r for r in out if r.get("correct") != "0"]
    return out


def median_by(rs, key, col):
    groups = {}
    for r in rs:
        try:
            v = float(r[col])          # a missing column or empty cell is skipped
        except (KeyError, ValueError, TypeError):
            continue
        groups.setdefault(key(r), []).append(v)
    return {k: statistics.median(v) for k, v in groups.items()}


def human(n):
    return "%dM" % (n // 10 ** 6) if n % 10 ** 6 == 0 else str(n)


def fit_fixed_parallel(ps, ts):
    """Least squares t = a + b/P. a = the part that does not shrink with P."""
    xs = [1.0 / p for p in ps]
    mx, my = sum(xs) / len(xs), sum(ts) / len(ts)
    sxx = sum((x - mx) ** 2 for x in xs)
    b = sum((x - mx) * (t - my) for x, t in zip(xs, ts)) / sxx
    return my - b * mx, b


# ---------------------------------------------------------------------------
# the data
# ---------------------------------------------------------------------------

mr = rows(os.path.join(RES, "cluster", "benchmark_srun.csv"))
mpi = rows(os.path.join(RES, "cluster", "mpi_rerun.csv"))
orig = rows(os.path.join(RES, "mpi_benchmark_original.csv"), check_correct=False)
lap = rows(os.path.join(RES, "laptop", "benchmark.csv"))
if not mr or not mpi:
    sys.exit("error: results/cluster/benchmark_srun.csv and mpi_rerun.csv are required")

k_mr = lambda r: (int(r["n"]), int(r["maps_requested"]))
k_mpi = lambda r: (int(r["n"]), int(r["p"]))
MR = {c: median_by(mr, k_mr, c) for c in
      ("t_jobs", "t_map", "t_reduce", "t_s2", "t_stage", "t_total", "nodes",
       "peak_map_mem", "peak_red_mem", "shuffle_bytes", "shuffled", "map_in", "bytes")}
MPI = {c: median_by(mpi, k_mpi, c) for c in
       ("t_solve", "t_comm", "t_imbalance", "nodes", "rss_max_kb", "rss_sum_kb", "bytes",
        "t_wall")}
LAUNCH = rows(os.path.join(RES, "cluster", "launch_overhead.csv"), check_correct=False)
L_SRUN = median_by(LAUNCH, lambda r: int(r["p"]), "srun_s")
L_MPIRUN = median_by(LAUNCH, lambda r: int(r["p"]), "mpirun_s")
# The single-job ablation (slurm/ablation_onejob.slurm): two-job, one-job and MPI,
# interleaved per (N, P) in one allocation.
ab_two = rows(os.path.join(RES, "cluster", "ablation_twojob.csv"))
ab_one = rows(os.path.join(RES, "cluster", "ablation_onejob.csv"))
ab_mpi = rows(os.path.join(RES, "cluster", "ablation_mpi.csv"))
AB = {}
if ab_two and ab_one and ab_mpi:
    AB = {"two": {c: median_by(ab_two, k_mr, c) for c in ("t_jobs", "t_map", "t_reduce", "t_s2")},
          "one": {c: median_by(ab_one, k_mr, c) for c in ("t_jobs", "t_map", "t_reduce", "nodes")},
          "mpi": {c: median_by(ab_mpi, k_mpi, c) for c in ("t_solve", "t_wall")}}
AB_LAUNCH = rows(os.path.join(RES, "cluster", "ablation_launch.csv"), check_correct=False)
AB_SRUN = median_by(AB_LAUNCH, lambda r: int(r["p"]), "srun_s")
AB_MPIRUN = median_by(AB_LAUNCH, lambda r: int(r["p"]), "mpirun_s")
# An input size whose two-job medians differ from the main benchmark (same code,
# same data) by more than 20% at 3 or more P values was disturbed by something
# outside the experiment -- in the ablation job, N = 20M, where single MapReduce
# repetitions took up to 7x their neighbours. Its rows stay in the tables but
# are left out of the fits and the fixed-cost bars.
AB_DISTURBED = set()
if AB:
    for n in sorted({k[0] for k in AB["two"]["t_jobs"]}):
        off = [k for k in AB["two"]["t_jobs"] if k[0] == n and k in MR["t_jobs"]
               and abs(AB["two"]["t_jobs"][k] / MR["t_jobs"][k] - 1) > 0.20]
        if len(off) >= 3:
            AB_DISTURBED.add(n)
ORIG = median_by([r for r in orig if r.get("io_mode") == "split"], k_mpi, "t_solve")
SEQ = median_by(mr, lambda r: int(r["n"]), "t_seq")
LAP = {c: median_by(lap, k_mr, c) for c in
       ("t_stage", "t_s1", "t_s2", "t_jobs", "peak_map_mem", "peak_red_mem",
        "shuffle_bytes", "map_out")}
LAP_SEQ = median_by(lap, lambda r: int(r["n"]), "t_seq")

SIZES = sorted({k[0] for k in MR["t_jobs"]})
PS = sorted({k[1] for k in MR["t_jobs"]})
NNODES = max(int(v) for v in MR["nodes"].values())
PACKED = [p for p in PS if MR["nodes"].get((SIZES[-1], p), p) < p]
LAP_PS = sorted({k[1] for k in LAP["t_jobs"]})

seq_rss_kb = None
for log in sorted(glob.glob(os.path.join(RES, "cluster", "mr_bsrun_*.log"))):
    m = re.findall(r"sequential peak RSS = (\d+) KB", open(log).read())
    if m:
        seq_rss_kb = statistics.median(int(x) for x in m)


def tick(p):
    return "%d*" % p if p in PACKED else str(p)


# ---------------------------------------------------------------------------
# summary.md
# ---------------------------------------------------------------------------

out = []
w = out.append


def table(header, body):
    w("| " + " | ".join(header) + " |")
    w("|" + "|".join(["---"] + [":---:"] * (len(header) - 1)) + "|")
    for row in body:
        w("| " + " | ".join(row) + " |")
    w("")


def grid(title, note, cell):
    """One N x P table; cell(n, p) -> str or None."""
    w("#### " + title)
    w("")
    if note:
        w(note)
        w("")
    table(["Input"] + ["P=" + tick(p) for p in PS],
          [["N = " + human(n)] + [cell(n, p) or "--" for p in PS] for n in SIZES])


w("# MPI vs MapReduce: measured results")
w("")
w("Generated by `scripts/plot.py` from the CSVs in this folder. Medians of 3 repetitions "
  "(after one untimed warm-up per configuration). Every row used here produced output "
  "byte-identical to the sequential reference.")
w("")
w("## Where each number comes from")
w("")
table(["Label", "What ran", "Where", "File"], [
    ["**MPI**", "the 01-mpi `loganalytics_mpi --io split`, unchanged",
     "cluster, %d nodes, one rank per node" % NNODES, "`cluster/mpi_rerun.csv`"],
    ["**MapReduce**", "our `mr_map` / `mr_reduce` / `mr_report`, two MapReduce jobs, "
     "driven by Slurm (`scripts/run_slurm.sh`)",
     "cluster, **same job, nodes and data files as MPI**", "`cluster/benchmark_srun.csv`"],
    ["**Hadoop (laptop)**", "the same binaries under real Hadoop 3.3.6 + YARN + "
     "Streaming (`scripts/run_hadoop.sh`)", "one laptop (WSL, 16 threads, 7 GB), "
     "pseudo-distributed", "`laptop/benchmark.csv`"],
    ["MPI (original sweep)", "the original 01-mpi sweep", "the cluster, node[01-07], during the original MPI sweep",
     "`mpi_benchmark_original.csv`"],
])
w("The cluster's shared Hadoop has no DataNodes and no YARN ResourceManager, and Hadoop "
  "was not installed on the shared cluster. So the multi-node comparison is MPI vs "
  "**Slurm-driven MapReduce** (the same binaries, with `srun` standing in for YARN), and "
  "real Hadoop is measured on one machine. See the README.")
w("")
w("Datasets: `gen_logs.py -k 10 --servers 64 --endpoints 512 --seed 42`, identical bytes "
  "to the 01-mpi datasets (md5 checked on the cluster and the laptop).")
w("")
table(["N", "bytes", "MB"], [[human(n), "%d" % MR["bytes"][(n, 1)],
                               "%.0f" % (MR["bytes"][(n, 1)] / MB)] for n in SIZES])

# -- correctness and placement
w("## Correctness and placement")
w("")
w("- MapReduce rows: **%d**, MPI rows: **%d**, Hadoop (laptop) rows: **%d** -- all "
  "byte-identical to `loganalytics_seq`." % (len(mr), len(mpi), len(lap)))
w("- Correctness suites: 88/88 local, 88/88 Slurm-driven on 3 cluster nodes, "
  "44/44 on real Hadoop (raw job logs not included in this repository).")
w("")
table(["P"] + [tick(p) for p in PS],
      [["MapReduce: distinct nodes that ran a map task"] +
       ["%d" % MR["nodes"][(SIZES[-1], p)] for p in PS],
       ["MPI: distinct nodes that ran a rank"] +
       ["%d" % MPI["nodes"][(SIZES[-1], p)] for p in PS]])
if PACKED:
    w("\\* P = %s: more tasks than nodes, so 2 per node on %d nodes -- the same packing "
      "the MPI sweep used. Not comparable with the one-per-node points; starred on every "
      "plot." % (",".join(map(str, PACKED)), MR["nodes"][(SIZES[-1], PACKED[0])]))
    w("")

# -- time
w("## 1. Execution time")
w("")
grid("MPI `t_solve` (s)", "Read + parse + aggregate + reduce, slowest rank.",
     lambda n, p: "%.3f" % MPI["t_solve"][(n, p)] if (n, p) in MPI["t_solve"] else None)
grid("MapReduce `t_jobs` (s)", "Both jobs: map phase + reduce phase + report job. "
     "Excludes staging the input (see §5).",
     lambda n, p: "%.3f" % MR["t_jobs"][(n, p)])
grid("MapReduce / MPI", "How many times slower MapReduce is.",
     lambda n, p: "%.1fx" % (MR["t_jobs"][(n, p)] / MPI["t_solve"][(n, p)]))

# -- like for like: including process launch
if MPI["t_wall"]:
    grid("MPI wall time `t_wall` (s): the whole `mpirun`, launch included",
         "`t_solve` above starts after `MPI_Init`, so it leaves out starting the processes; "
         "MapReduce's `t_jobs` includes its three `srun` launches. `t_wall` times the whole "
         "`mpirun` from outside, so it is the like-for-like counterpart.",
         lambda n, p: "%.3f" % MPI["t_wall"][(n, p)] if (n, p) in MPI["t_wall"] else None)
    grid("MapReduce / MPI, both including launch (`t_jobs` / `t_wall`)", None,
         lambda n, p: ("%.1fx" % (MR["t_jobs"][(n, p)] / MPI["t_wall"][(n, p)]))
         if (n, p) in MPI["t_wall"] else None)

# -- speed-up and efficiency
w("## 2. Scaling with the number of processes")
w("")
for label, T in (("MPI", MPI["t_solve"]), ("MapReduce", MR["t_jobs"])):
    grid("%s speed-up  S(P) = T(1)/T(P)" % label, None,
         lambda n, p, T=T: "%.2f" % (T[(n, 1)] / T[(n, p)]))
    grid("%s efficiency  E(P) = S(P)/P" % label, None,
         lambda n, p, T=T: "%.2f" % (T[(n, 1)] / T[(n, p)] / p))

w("#### The part that does not shrink with P")
w("")
w("Least-squares fit of `T(P) = a + b/P` over the one-per-node points. `b` is work "
  "that divides across processes; `a` is work that does not -- start-up, the reduce and "
  "report phases, the shuffle. `a / T(1)` is the serial fraction in Amdahl's law, and "
  "`T(1)/a` the speed-up it caps us at.")
w("")
body = []
one_per_node = [p for p in PS if p not in PACKED]
for n in SIZES:
    for label, T in (("MPI", MPI["t_solve"]), ("MapReduce", MR["t_jobs"])):
        a, b = fit_fixed_parallel(one_per_node, [T[(n, p)] for p in one_per_node])
        body.append(["N = " + human(n), label, "%.3f" % a, "%.3f" % b,
                     "%.0f%%" % (100 * max(a, 0) / T[(n, 1)]),
                     ("%.1fx" % (T[(n, 1)] / a)) if a > 0 else "--"])
table(["Input", "System", "a: fixed (s)", "b: parallel (s)", "serial fraction",
       "max speed-up"], body)

# -- throughput and input size
w("## 3. Throughput and scaling with input size")
w("")
grid("MPI throughput (MB/s)", None,
     lambda n, p: "%.0f" % (MPI["bytes"][(n, p)] / MB / MPI["t_solve"][(n, p)]))
grid("MapReduce throughput (MB/s)", "Input bytes / `t_jobs`.",
     lambda n, p: "%.0f" % (MR["bytes"][(n, p)] / MB / MR["t_jobs"][(n, p)]))

# -- phases
w("## 4. Where MapReduce's time goes")
w("")
body = []
for n in (SIZES[0], SIZES[-1]):
    for p in PS:
        k = (n, p)
        body.append(["N = " + human(n), tick(p), "%.3f" % MR["t_map"][k],
                     "%.3f" % MR["t_reduce"][k], "%.3f" % MR["t_s2"][k],
                     "%.3f" % MR["t_jobs"][k], "%.3f" % MPI["t_solve"][k]])
table(["Input", "P", "map phase", "reduce phase", "report job", "total", "MPI"], body)
w("The map phase is the only part that shrinks with P. The reduce phase and the report "
  "job cost about the same at every N and P: each is an `srun` launch across nodes plus "
  "a few thousand lines through the shared file system. MPI's equivalent -- one "
  "`MPI_Reduce` of ~20 KB per rank -- takes microseconds.")
w("")

# -- launch overhead
if L_SRUN:
    w("## 4b. The cost of starting processes")
    w("")
    w("An EMPTY `srun` step (what each MapReduce phase pays to start its tasks) and an "
      "EMPTY `mpirun` (what MPI pays once), timed at each P in the same allocation. "
      "Median of 5.")
    w("")
    table(["P"] + [tick(p) for p in PS],
          [["empty `srun` step (s)"] + ["%.3f" % L_SRUN[p] if p in L_SRUN else "--" for p in PS],
           ["empty `mpirun` (s)"] + ["%.3f" % L_MPIRUN[p] if p in L_MPIRUN else "--" for p in PS],
           ["3 x `srun` (one MapReduce run)"] + ["%.3f" % (3 * L_SRUN[p]) if p in L_SRUN else "--" for p in PS]])
    w("A MapReduce run is three distributed steps (map, reduce, report), so it pays about "
      "three `srun` launches; compare with the fitted fixed cost `a` in section 2. MPI pays "
      "one launch, outside its `t_solve`.")
    w("")

# -- staging
w("## 5. Getting the data in: staging")
w("")
w("MapReduce must first place its input in the (simulated) distributed file system: "
  "strip the header and write P split files to `/home`. Staged once per (N, P) and "
  "reused by every repetition, like an HDFS input. MPI reads the original file in place "
  "and has no such step.")
w("")
table(["Input", "staging (s)", "MapReduce jobs, P=%d (s)" % NNODES,
       "MapReduce end to end (s)", "MPI, P=%d (s)" % NNODES, "Hadoop (laptop) staging (s)"],
      [["N = " + human(n), "%.1f" % MR["t_stage"][(n, NNODES)],
        "%.2f" % MR["t_jobs"][(n, NNODES)],
        "%.2f" % (MR["t_stage"][(n, NNODES)] + MR["t_jobs"][(n, NNODES)]),
        "%.3f" % MPI["t_solve"][(n, NNODES)],
        "%.1f" % LAP["t_stage"][(n, 1)] if (n, 1) in LAP["t_stage"] else "--"]
       for n in SIZES])

# -- memory
w("## 6. Memory")
w("")
w("Peak resident memory (RSS) of one process, measured with `getrusage` (MPI, "
  "MapReduce on the cluster) or YARN's container counters (Hadoop, whole container: JVM + our "
  "C++ child).")
w("")
n = SIZES[-1]
body = []
if seq_rss_kb:
    body.append(["Sequential C++ (`loganalytics_seq`)", "%.1f" % (seq_rss_kb / 1024), "--"])
body.append(["MPI rank (max over ranks)", "%.1f" % (max(MPI["rss_max_kb"][(n, p)]
             for p in one_per_node) / 1024),
             "%.0f MB for all %d ranks" % (MPI["rss_sum_kb"][(n, NNODES)] / 1024, NNODES)])
body.append(["MapReduce map task (`mr_map`)", "%.1f" % (max(MR["peak_map_mem"][(n, p)]
             for p in one_per_node) / MB), "per task"])
body.append(["MapReduce reduce task (`mr_reduce`)", "%.1f" % (max(MR["peak_red_mem"][(n, p)]
             for p in one_per_node) / MB), "per task"])
if LAP["peak_map_mem"]:
    body.append(["Hadoop map container (JVM + `mr_map`)",
                 "%.0f" % (max(LAP["peak_map_mem"].values()) / MB), "per task"])
    body.append(["Hadoop reduce container (JVM + `mr_reduce`)",
                 "%.0f" % (max(LAP["peak_red_mem"].values()) / MB), "per task"])
table(["Process (N = %s)" % human(n), "peak RSS (MB)", "note"], body)
w("Memory does not grow with N for any of them: every process holds an aggregate of "
  "size O(servers + endpoints + intervals), never the records. MPI's ~27 MB is mostly the "
  "MPI library itself. A real Hadoop map container is ~13x an MPI rank and ~30x our C++ "
  "mapper alone, because of the JVM that feeds it -- before counting the four daemons (NameNode, DataNode, "
  "ResourceManager, NodeManager) that must also be running.")
w("")

# -- data movement
w("## 7. Communication and data movement")
w("")
body = []
for p in PS:
    k = (SIZES[-1], p)
    body.append([tick(p), "%d" % MR["shuffled"][k], "%.0f" % (MR["shuffle_bytes"][k] / 1024),
                 "%.0f" % (MPI_REDUCE_BYTES_PER_RANK * (p - 1) / 1024)])
table(["P", "MapReduce: records shuffled", "MapReduce: KB shuffled",
       "MPI: KB reduced (analytical)"], body)
w("N = %s, %.0f MB of input. Both designs aggregate *before* communicating, so neither "
  "moves more than ~0.05%% of the input, and neither volume depends on N (the shuffle is "
  "~2,019 records per mapper at every size). MapReduce moves its intermediate data as text "
  "through the file system (map output written, then read by the reducers, then job 1's "
  "output read by job 2); MPI moves binary int64 arrays over the network in one "
  "collective. MPI figure: %d bytes per non-root rank, from the reductions in "
  "`loganalytics_mpi.cpp`." % (human(SIZES[-1]), MR["bytes"][(SIZES[-1], 1)] / MB,
                               MPI_REDUCE_BYTES_PER_RANK))
w("")
w("The bigger data movement in MapReduce is the **input**: staging rewrites the whole "
  "file (§5), and every map task then reads its split from `/home` over NFS. Real HDFS "
  "would make most map reads local (data locality); the Slurm simulation cannot.")
w("")

# -- real Hadoop
if LAP["t_jobs"]:
    w("## 8. Real Hadoop 3.3.6 (laptop, one machine)")
    w("")
    body = []
    for n in SIZES:
        for p in LAP_PS:
            k = (n, p)
            if k not in LAP["t_jobs"]:
                continue
            body.append(["N = " + human(n), str(p), "%.1f" % LAP["t_stage"][k],
                         "%.1f" % LAP["t_s1"][k], "%.1f" % LAP["t_s2"][k],
                         "%.1f" % LAP["t_jobs"][k], "%.2f" % LAP_SEQ[n]])
    table(["Input", "P (maps)", "HDFS staging", "job 1", "job 2", "both jobs",
           "sequential C++"], body)
    w("About 45 s whatever N or P. Scanning 20M records takes ~1.5 s; the rest is the "
      "framework: client start-up, uploading the job, starting an ApplicationMaster JVM, "
      "YARN heartbeats, localizing files and starting a JVM per task, committing output -- "
      "twice, once per job. Job 2 processes ~2,000 lines and still costs ~21 s, which is "
      "the argument for folding the ranking into a single job on real Hadoop. All tasks "
      "share one machine here, so this measures Hadoop's overhead, not its scaling.")
    w("")

# -- control
if ORIG:
    w("## 9. Control: today's MPI vs. the original sweep")
    w("")
    grid("MPI today / MPI in the original sweep", "1.00 = identical. Within ~5% at N >= 5M, so "
         "the cluster and data are unchanged and the MPI conclusions hold. At N = 1M "
         "the runs last only 20-90 ms and wobble by up to ~20% at high P.",
         lambda n, p: ("%.2f" % (MPI["t_solve"][(n, p)] / ORIG[(n, p)]))
         if (n, p) in ORIG else None)

# -- ablation: one job instead of two
AB_FIT = {}
if AB:
    A_SIZES = sorted({k[0] for k in AB["one"]["t_jobs"]})
    A_PS = sorted({k[1] for k in AB["one"]["t_jobs"]})
    A_ONE = [p for p in A_PS if p not in PACKED]
    SERIES = (("MapReduce, two jobs", AB["two"]["t_jobs"]),
              ("MapReduce, one job", AB["one"]["t_jobs"]),
              ("MPI incl. launch (t_wall)", AB["mpi"]["t_wall"]),
              ("MPI (t_solve)", AB["mpi"]["t_solve"]))

    def agrid(title, note, cell):
        w("#### " + title)
        w("")
        if note:
            w(note)
            w("")
        body = []
        for n in A_SIZES:
            cells = []
            for p in A_PS:
                try:
                    cells.append(cell(n, p))
                except KeyError:
                    cells.append("--")
            body.append(["N = " + human(n)] + cells)
        table(["Input"] + ["P=" + tick(p) for p in A_PS], body)

    w("## 10. Ablation: one MapReduce job instead of two")
    w("")
    w("`slurm/ablation_onejob.slurm`: for every (N, P), the two-job pipeline (R = 4, then a "
      "report job), the single-job pipeline (`ONE_JOB=1`: map, then ONE reducer running "
      "`mr_report`) and MPI, back to back in one allocation on the same files. Medians of 3.")
    w("")
    agrid("Two jobs `t_jobs` (s)", None, lambda n, p: "%.3f" % AB["two"]["t_jobs"][(n, p)])
    agrid("One job `t_jobs` (s)", None, lambda n, p: "%.3f" % AB["one"]["t_jobs"][(n, p)])
    agrid("Saved by dropping job 2: two jobs - one job (s)", None,
          lambda n, p: "%.3f" % (AB["two"]["t_jobs"][(n, p)] - AB["one"]["t_jobs"][(n, p)]))
    agrid("MPI `t_wall` (s), launch included", None,
          lambda n, p: "%.3f" % AB["mpi"]["t_wall"][(n, p)])
    agrid("Two jobs / MPI `t_wall`", None,
          lambda n, p: "%.2fx" % (AB["two"]["t_jobs"][(n, p)] / AB["mpi"]["t_wall"][(n, p)]))
    agrid("One job / MPI `t_wall`", None,
          lambda n, p: "%.2fx" % (AB["one"]["t_jobs"][(n, p)] / AB["mpi"]["t_wall"][(n, p)]))
    agrid("One job: map phase / reduce task (s)", None,
          lambda n, p: "%.3f / %.3f" % (AB["one"]["t_map"][(n, p)], AB["one"]["t_reduce"][(n, p)]))

    if AB_DISTURBED:
        w("**Disturbed:** N = %s. Its two-job medians differ from the main benchmark by more "
          "than 20%% at 3 or more P values, and single repetitions of both MapReduce variants "
          "took up to 7x their neighbours, so something outside the experiment (most likely "
          "shared file-system load) interfered. Shown above, left out of the fits below and "
          "of the figure's fixed-cost bars." % ", ".join(human(n) for n in sorted(AB_DISTURBED)))
        w("")
    w("#### Fixed and parallel parts, `T(P) = a + b/P` (one-per-node points)")
    w("")
    body = []
    for n in A_SIZES:
        if n in AB_DISTURBED:
            continue
        for label, T in SERIES:
            if not all((n, p) in T for p in A_ONE):
                continue
            a, b = fit_fixed_parallel(A_ONE, [T[(n, p)] for p in A_ONE])
            AB_FIT[(label, n)] = (a, b)
            body.append(["N = " + human(n), label, "%.3f" % a, "%.3f" % b])
    table(["Input", "System", "a: fixed (s)", "b: parallel (s)"], body)

    if AB_SRUN:
        table(["P"] + [tick(p) for p in A_PS],
              [["empty `srun` step (s)"] + ["%.3f" % AB_SRUN[p] if p in AB_SRUN else "--" for p in A_PS],
               ["empty `mpirun` (s)"] + ["%.3f" % AB_MPIRUN[p] if p in AB_MPIRUN else "--" for p in A_PS]])

    # The two-job rows of this job against the main benchmark: a
    # control that the two allocations behaved alike.
    both = [(n, p) for n in A_SIZES for p in A_PS
            if (n, p) in MR["t_jobs"] and (n, p) in AB["two"]["t_jobs"]]
    if both:
        clean = [k for k in both if k[0] not in AB_DISTURBED]
        rs = [AB["two"]["t_jobs"][k] / MR["t_jobs"][k] for k in clean]
        w("Control: two-job `t_jobs` here / in the main benchmark: median %.2f, range "
          "%.2f-%.2f over the %d configurations of the undisturbed sizes."
          % (statistics.median(rs), min(rs), max(rs), len(rs)))
        w("")

path = os.path.join(RES, "summary.md")
with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(out) + "\n")
print("wrote " + os.path.relpath(path, ROOT))


# ---------------------------------------------------------------------------
# plots
# ---------------------------------------------------------------------------

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("matplotlib missing -- skipped the plots (summary.md was written)")
    sys.exit(0)

plt.rcParams.update({
    "font.size": 9, "axes.edgecolor": GRAY, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK,
    "axes.titlesize": 10, "axes.titlelocation": "left", "legend.frameon": False,
    "figure.facecolor": "white", "savefig.facecolor": "white",
})


def style(ax, xlabel=None, ylabel=None, title=None):
    if title:
        ax.set_title(title, pad=8)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def p_axis(ax):
    ax.set_xticks(PS)
    ax.set_xticklabels([tick(p) for p in PS])


def save(fig, name, footnote=True):
    if footnote and PACKED:
        fig.text(0.99, 0.005, "* %d tasks on %d nodes (2 per node)"
                 % (PACKED[0], MR["nodes"][(SIZES[-1], PACKED[0])]),
                 ha="right", va="bottom", fontsize=7.5, color=GRAY)
    fig.savefig(os.path.join(RES, name), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("wrote results/" + name)


LINE = dict(linewidth=2, marker="o", markersize=5)

# 1. time vs P, one panel per N
ncols = 3 if len(SIZES) > 4 else 2
nrows = -(-len(SIZES) // ncols)
fig, axes = plt.subplots(nrows, ncols, figsize=(4.2 * ncols, 3.0 * nrows + 0.4), squeeze=False)
for ax in list(axes.flat)[len(SIZES):]:
    ax.axis("off")
for ax, n in zip(axes.flat, SIZES):
    ax.plot(PS, [MR["t_jobs"][(n, p)] for p in PS], color=ORANGE, label="MapReduce (t_jobs)", **LINE)
    ax.plot(PS, [MPI["t_solve"][(n, p)] for p in PS], color=BLUE, label="MPI (t_solve)", **LINE)
    if all((n, p) in MPI["t_wall"] for p in PS):
        ax.plot(PS, [MPI["t_wall"][(n, p)] for p in PS], color=BLUE, linestyle="--",
                linewidth=1.6, marker="s", markersize=4, alpha=0.75,
                label="MPI incl. launch (t_wall)")
    ax.set_ylim(bottom=0)
    p_axis(ax)
    style(ax, "processes P", "seconds", "N = %s (%.0f MB)" % (human(n), MR["bytes"][(n, 1)] / MB))
h, l = axes[0][0].get_legend_handles_labels()
fig.legend(h, l, loc="upper right", ncol=3, bbox_to_anchor=(0.99, 0.995))
fig.suptitle("Execution time: MapReduce (Slurm-driven) vs MPI, same cluster nodes and data",
             x=0.01, ha="left", fontsize=11, color=INK)
fig.tight_layout(rect=(0, 0.02, 1, 0.95))
save(fig, "time_vs_p.png")

# 2. speed-up: one panel per system, N as a one-hue ramp
fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.8), sharey=True)
for ax, (label, T) in zip(axes, (("MPI", MPI["t_solve"]), ("MapReduce", MR["t_jobs"]))):
    ax.plot(one_per_node, one_per_node, color=GRAY, linewidth=1.2, linestyle=":",
            label="ideal")
    for c, n in zip(ramp(len(SIZES)), SIZES):
        ax.plot(PS, [T[(n, 1)] / T[(n, p)] for p in PS], color=c,
                label="N = " + human(n), **LINE)
    p_axis(ax)
    style(ax, "processes P", "speed-up T(1)/T(P)" if label == "MPI" else None, label)
axes[0].legend(loc="upper left", fontsize=8)
fig.tight_layout(rect=(0, 0.03, 1, 1))
save(fig, "speedup.png")

# 3. MapReduce phases (stacked) with MPI for reference
fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.8))
phases = (("map phase", "t_map", AQUA), ("reduce phase", "t_reduce", YELLOW),
          ("report job", "t_s2", VIOLET))
for ax, n in zip(axes, (SIZES[0], SIZES[-1])):
    bottom = [0.0] * len(PS)
    for label, col, color in phases:
        vals = [MR[col][(n, p)] for p in PS]
        ax.bar(PS, vals, bottom=bottom, color=color, width=0.62, label=label,
               edgecolor="white", linewidth=1)
        bottom = [a + b for a, b in zip(bottom, vals)]
    ax.plot(PS, [MPI["t_solve"][(n, p)] for p in PS], color=BLUE, label="MPI (total)", **LINE)
    p_axis(ax)
    style(ax, "processes P", "seconds", "N = %s" % human(n))
axes[1].legend(loc="upper right", fontsize=8)
fig.suptitle("Where MapReduce's time goes: only the map phase shrinks with P",
             x=0.01, ha="left", fontsize=11, color=INK)
fig.tight_layout(rect=(0, 0.03, 1, 0.95))
save(fig, "mapreduce_phases.png")

# 4. end to end at 20M (the largest size the laptop also ran), log scale
n = 20000000 if 20000000 in SIZES else SIZES[-1]
bars = [("Sequential C++ (1 core)", SEQ[n], GRAY),
        ("MPI, P=%d" % NNODES, MPI["t_solve"][(n, NNODES)], BLUE),
        ("MapReduce (Slurm), P=%d: jobs" % NNODES, MR["t_jobs"][(n, NNODES)], ORANGE),
        ("MapReduce (Slurm), P=%d: + staging" % NNODES,
         MR["t_jobs"][(n, NNODES)] + MR["t_stage"][(n, NNODES)], ORANGE)]
if (n, max(LAP_PS or [1])) in LAP["t_jobs"]:
    pl = max(LAP_PS)
    bars += [("Real Hadoop (laptop), P=%d: jobs" % pl, LAP["t_jobs"][(n, pl)], AQUA),
             ("Real Hadoop (laptop), P=%d: + staging" % pl,
              LAP["t_jobs"][(n, pl)] + LAP["t_stage"][(n, pl)], AQUA)]
fig, ax = plt.subplots(figsize=(8.4, 3.4))
ys = list(range(len(bars)))[::-1]
ax.barh(ys, [b[1] for b in bars], color=[b[2] for b in bars], height=0.6)
for y, (label, v, _) in zip(ys, bars):
    ax.text(v * 1.08, y, "%.2f s" % v, va="center", fontsize=8.5, color=INK)
ax.set_yticks(ys)
ax.set_yticklabels([b[0] for b in bars])
ax.set_xscale("log")
ax.set_xlim(right=max(b[1] for b in bars) * 4)
style(ax, "seconds (log scale)", None,
      "End to end, N = %s (%.0f MB)" % (human(n), MR["bytes"][(n, 1)] / MB))
ax.grid(axis="y", visible=False)
fig.tight_layout()
save(fig, "end_to_end.png", footnote=False)

# 5. memory
mem = []
if seq_rss_kb:
    mem.append(("Sequential C++", seq_rss_kb / 1024, GRAY))
mem.append(("MPI rank", max(MPI["rss_max_kb"][(n, p)] for p in one_per_node) / 1024, BLUE))
mem.append(("MapReduce map task", max(MR["peak_map_mem"][(n, p)] for p in one_per_node) / MB, ORANGE))
mem.append(("MapReduce reduce task", max(MR["peak_red_mem"][(n, p)] for p in one_per_node) / MB, ORANGE))
# Real Hadoop's containers (laptop) are left out: that run is in the README's
# laptop section, and its 300+ MB bars would squash the cluster ones to slivers.
fig, ax = plt.subplots(figsize=(8.4, 2.6))
ys = list(range(len(mem)))[::-1]
ax.barh(ys, [m[1] for m in mem], color=[m[2] for m in mem], height=0.6)
for y, (label, v, _) in zip(ys, mem):
    ax.text(v + 0.4, y, "%.0f MB" % v if v >= 20 else "%.1f MB" % v, va="center",
            fontsize=8.5, color=INK)
ax.set_yticks(ys)
ax.set_yticklabels([m[0] for m in mem])
ax.set_xlim(right=max(m[1] for m in mem) * 1.18)
style(ax, "peak resident memory per process (MB)", None,
      "Memory per process, N = %s (flat in N for all of them)" % human(n))
ax.grid(axis="y", visible=False)
fig.tight_layout()
save(fig, "memory.png", footnote=False)

# 6. real Hadoop breakdown
if LAP["t_jobs"]:
    fig, ax = plt.subplots(figsize=(8.4, 3.6))
    # Phase colours match mapreduce_phases.png (aggregate = aqua, report = violet);
    # blue and orange stay reserved for MPI and MapReduce as systems.
    layers = (("HDFS staging", "t_stage", MAGENTA), ("job 1 (aggregate)", "t_s1", AQUA),
              ("job 2 (rank + report)", "t_s2", VIOLET))
    xs, labels = [], []
    x = 0
    for n in SIZES:
        for p in LAP_PS:
            if (n, p) in LAP["t_jobs"]:
                xs.append(x); labels.append("%s\nP=%d" % (human(n), p)); x += 1
        x += 0.6
    keys = [(n, p) for n in SIZES for p in LAP_PS if (n, p) in LAP["t_jobs"]]
    bottom = [0.0] * len(keys)
    for label, col, color in layers:
        vals = [LAP[col][k] for k in keys]
        ax.bar(xs, vals, bottom=bottom, color=color, width=0.8, label=label,
               edgecolor="white", linewidth=1)
        bottom = [a + b for a, b in zip(bottom, vals)]
    ax.plot(xs, [LAP_SEQ[k[0]] for k in keys], color=INK, linewidth=0, marker="_",
            markersize=14, markeredgewidth=2, label="sequential C++")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, fontsize=8)
    style(ax, None, "seconds", "Real Hadoop 3.3.6 on one machine: the two jobs take ~45 s whatever the input size")
    ax.legend(loc="upper left", ncol=4, fontsize=8, bbox_to_anchor=(0, -0.2))
    fig.tight_layout()
    save(fig, "hadoop_laptop.png", footnote=False)

# 7. launch overhead
if L_SRUN:
    ps = [p for p in PS if p in L_SRUN]
    fig, ax = plt.subplots(figsize=(6.4, 3.4))
    ax.plot(ps, [L_SRUN[p] for p in ps], color=ORANGE, label="empty srun step", **LINE)
    ax.plot(ps, [3 * L_SRUN[p] for p in ps], color=ORANGE, linestyle="--", linewidth=1.4,
            label="3 x srun (one MapReduce run)")
    ax.plot(ps, [L_MPIRUN[p] for p in ps], color=BLUE, label="empty mpirun", **LINE)
    ax.set_ylim(bottom=0)
    ax.set_xticks(ps)
    ax.set_xticklabels([tick(p) for p in ps])
    style(ax, "processes P", "seconds", "The cost of starting processes, before any work")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    save(fig, "launch_overhead.png")

# 8. ablation: one job instead of two
if AB:
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 3.8))
    lines = (("MapReduce, two jobs", AB["two"]["t_jobs"], ORANGE, "-", "o"),
             ("MapReduce, one job", AB["one"]["t_jobs"], VIOLET, "-", "s"),
             ("MPI incl. launch (t_wall)", AB["mpi"]["t_wall"], BLUE, "--", "s"),
             ("MPI (t_solve)", AB["mpi"]["t_solve"], BLUE, "-", "o"))
    for ax, n in zip(axes[:2], (A_SIZES[0], A_SIZES[-1])):
        for label, T, color, ls, mk in lines:
            ax.plot(A_PS, [T[(n, p)] for p in A_PS], color=color, linestyle=ls, marker=mk,
                    linewidth=2, markersize=5, label=label)
        ax.set_ylim(bottom=0)
        ax.set_xticks(A_PS)
        ax.set_xticklabels([tick(p) for p in A_PS])
        style(ax, "processes P", "seconds", "N = %s" % human(n))
    # Fixed cost a from T(P) = a + b/P, per N and system.
    ax = axes[2]
    bars = (("MapReduce, two jobs", ORANGE), ("MapReduce, one job", VIOLET),
            ("MPI incl. launch (t_wall)", BLUE))
    width = 0.26
    fit_sizes = [n for n in A_SIZES if n not in AB_DISTURBED]
    for i, (label, color) in enumerate(bars):
        xs = [j + (i - 1) * width for j in range(len(fit_sizes))]
        ax.bar(xs, [max(AB_FIT[(label, n)][0], 0) for n in fit_sizes], width=width - 0.03,
               color=color, label=label, edgecolor="white", linewidth=1)
    ax.set_xticks(range(len(fit_sizes)))
    ax.set_xticklabels([human(n) for n in fit_sizes])
    style(ax, "input size N" + (" (%s disturbed, omitted)" % ", ".join(human(n) for n in sorted(AB_DISTURBED))
                                if AB_DISTURBED else ""),
          "seconds", "Fixed cost a (does not shrink with P)")
    ax.grid(axis="x", visible=False)
    # One legend for all three panels: the bars reuse the lines' colours.
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper right", ncol=4, bbox_to_anchor=(0.99, 0.995), fontsize=8.5)
    fig.suptitle("Ablation: dropping MapReduce's second job, same allocation as MPI",
                 x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.03, 1, 0.9))
    save(fig, "ablation_onejob.png")
