#!/usr/bin/env bash
# Re-runs the 01-mpi MPI program on the same datasets and nodes as the MapReduce
# sweep, adding per-rank peak memory (maxrss.py) and the whole-mpirun wall time.
#
#     MPI_DIR=../01-mpi bash scripts/mpi_rerun.sh      (from slurm/benchmark_srun.slurm)
#
# MPI_DIR defaults to ../01-mpi relative to this repository layout.
#
# Writes results/mpi_rerun.csv. t_solve is the program's own timer (after MPI_Init);
# t_wall is the whole mpirun, launch included, the counterpart of MapReduce's t_jobs.
# Placement as in the 01-mpi benchmark: --map-by node, or ppr:2:node above PACK_ABOVE ranks.

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MPI_DIR="${MPI_DIR:-$ROOT/../01-mpi}"
MPI_DIR="$(cd "$MPI_DIR" 2>/dev/null && pwd)" || { echo "error: MPI_DIR not found (expected ../01-mpi)" >&2; exit 1; }
MPI="$MPI_DIR/bin/loganalytics_mpi"
SEQ="$ROOT/bin/loganalytics_seq"
# absolute path: the wrapper runs on other nodes
PY="${PYTHON:-$(command -v python3)}"

SIZES="${SIZES:-1000000 5000000 10000000 20000000}"
PROCS="${PROCS:-1 2 3 4 5 6 7 8}"
REPS="${REPS:-3}"
SEED="${SEED:-42}"
FLAGS="${MPI_FLAGS---bind-to none -x OMPI_MCA_coll_hcoll_enable=0}"
PACK_ABOVE="${PACK_ABOVE:-999}"

[ -x "$MPI" ] || make -C "$MPI_DIR" all || { echo "no MPI binary at $MPI"; exit 1; }
[ -x "$SEQ" ] || make -C "$ROOT" all || exit 1

DATA="$ROOT/data"; CSV="${OUT_CSV:-$ROOT/results/mpi_rerun.csv}"
mkdir -p "$DATA" "$ROOT/results"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

f() { awk -v k="$1" '$1 == k {print $2; exit}' "$WORK/err"; }
run_mpi_plain() {   # run_mpi_plain <P> <input> <output> -- no memory wrapper
    local map="--map-by node"
    [ "$1" -gt "$PACK_ABOVE" ] && map="--map-by ppr:2:node"
    mpirun -np "$1" $map $FLAGS "$MPI" "$2" "$3" --io split
}
run_mpi() {   # run_mpi <P> <input> <output>
    local map="--map-by node"
    [ "$1" -gt "$PACK_ABOVE" ] && map="--map-by ppr:2:node"
    mpirun -np "$1" $map $FLAGS "$PY" "$ROOT/scripts/maxrss.py" \
        "$MPI" "$2" "$3" --io split
}

echo "n,bytes,p,nodes,rep,t_solve,t_comp,t_comm,t_read,t_imbalance,t_total,rss_max_kb,rss_sum_kb,t_wall,correct" > "$CSV"
echo "MPI binary: $MPI"
echo "sizes: $SIZES | procs: $PROCS | reps: $REPS"
echo

for n in $SIZES; do
    input="$DATA/logs_${n}.txt"
    [ -s "$input" ] || "$PY" "$ROOT/scripts/gen_logs.py" -n "$n" -o "$input" -k 10 \
        --servers 64 --endpoints 512 --seed "$SEED" || exit 1
    bytes=$(wc -c < "$input" | tr -d ' ')
    "$SEQ" "$input" "$WORK/ref" >/dev/null 2>&1
    echo "N=$n  bytes=$bytes  md5=$(md5sum < "$input" | cut -c1-32)"

    for p in $PROCS; do
        run_mpi "$p" "$input" /dev/null >/dev/null 2>&1          # warm-up
        for rep in $(seq 1 "$REPS"); do
            if ! run_mpi "$p" "$input" "$WORK/out" >/dev/null 2>"$WORK/err"; then
                echo "  RUN FAILED: N=$n P=$p rep=$rep"; head -5 "$WORK/err"; continue
            fi
            # The same run again, unwrapped and timed from outside (t_wall).
            tw0=$(date +%s.%N)
            run_mpi_plain "$p" "$input" /dev/null >/dev/null 2>&1
            t_wall=$(echo "$tw0 $(date +%s.%N)" | awk '{printf "%.4f", $2 - $1}')
            correct=0
            diff -q "$WORK/out" "$WORK/ref" >/dev/null 2>&1 && correct=1
            rss=$(awk '$1 == "MAXRSS_KB" {s += $3; if ($3 > m) m = $3} END {print m + 0, s + 0}' "$WORK/err")
            echo "$n,$bytes,$p,$(f NODES),$rep,$(f TIME_SOLVE),$(f TIME_COMP),$(f TIME_COMM),$(f TIME_READ),$(f TIME_IMBALANCE),$(f TIME_TOTAL),${rss% *},${rss#* },$t_wall,$correct" >> "$CSV"
            [ "$rep" = 1 ] && printf '  P=%-2s on %s node(s): solve=%ss  peak RSS/rank=%s KB  correct=%s\n' \
                "$p" "$(f NODES)" "$(f TIME_SOLVE)" "${rss% *}" "$correct"
        done
    done
    # The sequential program's own footprint, for the P=1 baseline.
    "$PY" "$ROOT/scripts/maxrss.py" "$SEQ" "$input" /dev/null 2>"$WORK/err" >/dev/null
    echo "  sequential peak RSS = $(awk '$1 == "MAXRSS_KB" {print $3}' "$WORK/err") KB"
    echo
done
echo "Wrote $CSV"
