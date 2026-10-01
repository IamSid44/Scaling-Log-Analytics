#!/usr/bin/env bash
# Correctness suite. Three checks, in increasing order of strength:
#
#   1. hand-computed fixtures in tests/ reproduced exactly, by both programs
#   2. MPI == sequential on generated datasets (the real oracle)
#   3. MPI output byte-identical across every P and both I/O modes
#
#   bash scripts/verify.sh
#   PROCS="1 2 4" bash scripts/verify.sh
#
# Every check runs for P in $PROCS and both I/O modes. Scratch files go to a
# temporary directory that is deleted on exit. run_one below retries a launch
# failure a few times with backoff -- a safety net for a transient flake that
# costs nothing when nothing is wrong.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEQ="$ROOT/bin/loganalytics_seq"
MPI="$ROOT/bin/loganalytics_mpi"
PY="${PYTHON:-python3}"
PROCS="${PROCS:-1 2 3 4 5 6 7 8}"
MODES="${MODES:-split scatter}"

# mpirun, not srun: srun's PMI wiring is broken for this HPC-X build and
# MPI_Init aborts. --bind-to none: Open MPI otherwise sizes core binding off
# the node's full CPU count rather than the cgroup SLURM granted. hcoll off:
# InfiniBand warnings would pollute stderr, which is where the timings go.
#
# Deliberately NO --map-by here: the mapping is chosen per process count by
# map_for below, so the job script can say how to place ranks that no longer
# fit one-per-node.
FLAGS="${MPI_FLAGS---bind-to none -x OMPI_MCA_coll_hcoll_enable=0}"
MAP_DEFAULT="${MAP_DEFAULT---map-by node}"
MAP_PACK="${MAP_PACK-}"
MAP_PACK_ABOVE="${MAP_PACK_ABOVE-999}"

map_for() {   # map_for <P> -> the --map-by spec for this process count
    if [ -n "$MAP_PACK" ] && [ "$1" -gt "$MAP_PACK_ABOVE" ]; then
        echo "$MAP_PACK"
    else
        echo "$MAP_DEFAULT"
    fi
}
run_mpi() { local p="$1"; shift; mpirun -np "$p" $(map_for "$p") $FLAGS "$@"; }

{ [ -x "$SEQ" ] && [ -x "$MPI" ]; } || make -C "$ROOT" all || exit 1

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pass=0; fail=0
ok()  { echo "  PASS  $*"; pass=$((pass + 1)); }
bad() { echo "  FAIL  $*"; fail=$((fail + 1)); }
hr()  { echo "-------------------------------------------------------------"; }

# check <label> <reference> -- compares $WORK/o, which run_one just wrote.
check() {
    if diff -q "$2" "$WORK/o" >/dev/null 2>&1; then ok "$1"
    else bad "$1: output differs"; diff "$2" "$WORK/o" 2>/dev/null | head -10; fi
}

# run_one <label> <P> <args...> -- runs the MPI program into a FRESH $WORK/o,
# retrying with backoff on a launch failure. Retrying cannot paper over a
# wrong answer: every attempt runs the identical input through the identical
# binary, only over a launch that produced no output at all.
run_one() {
    local label="$1" p="$2"; shift 2
    local status attempt
    for attempt in 1 2 3; do
        rm -f "$WORK/o"
        run_mpi "$p" "$MPI" "$@" "$WORK/o" 2>"$WORK/err"
        status=$?
        [ "$status" -eq 0 ] && [ -s "$WORK/o" ] && return 0
        if [ "$attempt" -lt 3 ]; then
            echo "  ...   $label: attempt $attempt failed (exit $status), retrying"
            sleep "$attempt"
        fi
    done
    bad "$label: launch failed 3 times (exit $status) -- not a wrong answer"
    sed 's/^/        | /' "$WORK/err" | grep -v '^ *| *$' | head -4
    return 1
}

hr; echo "1. hand-computed fixtures"; hr
for exp in "$ROOT"/tests/*.expected; do
    name="$(basename "$exp" .expected)"
    in="$ROOT/tests/$name.txt"
    rm -f "$WORK/o"; "$SEQ" "$in" "$WORK/o" 2>/dev/null
    check "$name [sequential]" "$exp"
    for m in $MODES; do for p in $PROCS; do
        run_one "$name [$m] P=$p" "$p" "$in" --io "$m" && check "$name [$m] P=$p" "$exp"
    done; done
done

hr; echo "2. MPI vs. sequential on generated datasets"; hr
# Shapes chosen to stress different parts: skew in the top-K ranking, a flat
# distribution where the ordering is decided by the id tie-break, more servers
# than ranks and fewer, and a span far wider than one day.
gen() { "$PY" "$ROOT/scripts/gen_logs.py" -o "$WORK/$1.txt" "${@:2}" >/dev/null; }
gen small      -n 5000   -k 5  --servers 8   --endpoints 40   --seed 1
gen skewed     -n 50000  -k 10 --servers 200 --endpoints 2000 --alpha 1.6 --seed 2
gen flat       -n 50000  -k 10 --servers 16  --endpoints 32   --alpha 0.0 --seed 3
gen fewservers -n 20000  -k 20 --servers 3   --endpoints 5    --seed 4
gen longspan   -n 30000  -k 5  --servers 32  --endpoints 64   --span 2592000 --seed 5

for in in "$WORK"/*.txt; do
    name="$(basename "$in" .txt)"
    "$SEQ" "$in" "$WORK/$name.ref" 2>/dev/null || { bad "$name: sequential run failed"; continue; }
    for m in $MODES; do for p in $PROCS; do
        run_one "$name [$m] P=$p" "$p" "$in" --io "$m" && \
            check "$name [$m] P=$p" "$WORK/$name.ref"
    done; done
done

hr; echo "3. output byte-identical across P and I/O mode"; hr
# Guards the fixed-point accumulation: if this fails, a sum went back to
# floating point and the result now depends on how the input was split.
for name in skewed longspan; do
    same=1
    for m in $MODES; do for p in $PROCS; do
        if run_one "$name det [$m] P=$p" "$p" "$WORK/$name.txt" --io "$m"; then
            diff -q "$WORK/$name.ref" "$WORK/o" >/dev/null 2>&1 || \
                { same=0; bad "$name: P=$p [$m] differs from the sequential baseline"; }
        else
            same=0   # run_one already reported the launch failure
        fi
    done; done
    [ "$same" -eq 1 ] && ok "$name: identical for every P and I/O mode"
done

hr; echo "RESULT: $pass passed, $fail failed"; hr
[ "$fail" -eq 0 ]
