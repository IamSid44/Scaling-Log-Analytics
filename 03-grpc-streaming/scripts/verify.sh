#!/usr/bin/env bash
# Correctness suite: the streamed analytics must be byte-identical to the C++
# sequential reference for every worker count, distribution and batch size.
#
#     bash scripts/verify.sh                        (inside salloc, or: sbatch slurm/verify.slurm)
#     WORKERS="1 2 4" BATCHES="1 1000" WIRE=parsed bash scripts/verify.sh
#
#   1. the four hand-computed fixtures, at every W
#   2. generated datasets vs the sequential program, at every W and distribution
#   3. the same bytes for every W x distribution x batch size
#   4. queries during ingestion are served, totals never decrease, final answer exact

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/src"
PY="${PYTHON:-python3}"

WORKERS="${WORKERS:-1 2 3 4}"
DISTS="${DISTS:-roundrobin hash}"
BATCHES="${BATCHES:-1 100 5000}"
WIRE="${WIRE:-raw}"          # raw: workers parse (default) | parsed: producer parses
# ports 20000-31999: below the ephemeral range, and away from 50000-59999 where the
# other users' job-derived ports fall
BASE_PORT="${BASE_PORT:-$((20000 + ${SLURM_JOB_ID:-$$} % 12 * 1000))}"

ext=""
[ -x "$SRC/loganalytics_seq" ] || ext=".exe"
SEQ="$SRC/loganalytics_seq$ext"

pass=0; fail=0
ok()  { echo "  PASS  $*"; pass=$((pass + 1)); }
bad() { echo "  FAIL  $*"; fail=$((fail + 1)); }
hr()  { echo "-------------------------------------------------------------"; }
same() { diff -q --strip-trailing-cr "$1" "$2" >/dev/null 2>&1; }

WORK="$(mktemp -d)"
PIDS=""
cleanup() {
    # shellcheck disable=SC2086
    [ -n "$PIDS" ] && kill $PIDS 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$SEQ" ]; then
    echo "building the sequential reference oracle ..."
    ${CXX:-g++} -O3 -std=c++17 -D_FILE_OFFSET_BITS=64 \
        -o "$SEQ" "$SRC/loganalytics_seq.cpp" || {
        echo "cannot build the oracle -- check that g++ is available" >&2; exit 1; }
fi

if [ ! -f "$SRC/analytics_pb2.py" ]; then
    bash "$ROOT/scripts/build_proto.sh" || exit 1
fi

# ---------------------------------------------------------------------------
# start_cluster <W> <distribution> -> sets CO_ADDR; records pids in PIDS
# ---------------------------------------------------------------------------
PORT_CURSOR="$BASE_PORT"
CO_ADDR=""
CLUSTER_PIDS=""

# port_free <port>: nothing is listening there. gRPC binds with SO_REUSEPORT, so a
# port already in use would be shared silently instead of failing.
port_free() {
    "$PY" - "$1" <<'CHK' >/dev/null 2>&1
import socket, sys
s = socket.socket(); s.settimeout(0.5)
sys.exit(1 if s.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 0)
CHK
}

start_cluster() {
    local w="$1" dist="$2" i addrs="" port blk_ok
    CLUSTER_PIDS=""
    addrs=""

    # coordinator on PORT_CURSOR, workers on the next w ports; skip busy blocks
    local skipped
    for skipped in $(seq 1 50); do
        blk_ok=1
        for i in $(seq 0 "$w"); do
            port_free $((PORT_CURSOR + i)) || { blk_ok=0; break; }
        done
        [ "$blk_ok" -eq 1 ] && break
        echo "      (port $((PORT_CURSOR + i)) is already in use on $(hostname) -- skipping to the next block)" >&2
        PORT_CURSOR=$((PORT_CURSOR + w + 3))
    done

    for i in $(seq 0 $((w - 1))); do
        port=$((PORT_CURSOR + 1 + i))
        "$PY" "$SRC/worker.py" "0.0.0.0:$port" --worker-id "w$i" --quiet \
            > "$WORK/worker_$i.log" 2>&1 &
        CLUSTER_PIDS="$CLUSTER_PIDS $!"
        addrs="$addrs,localhost:$port"
    done
    addrs="${addrs#,}"

    CO_ADDR="localhost:$PORT_CURSOR"
    "$PY" "$SRC/coordinator.py" "0.0.0.0:$PORT_CURSOR" --workers "$addrs" \
        --distribution "$dist" --quiet > "$WORK/coordinator.log" 2>&1 &
    CLUSTER_PIDS="$CLUSTER_PIDS $!"
    PIDS="$PIDS $CLUSTER_PIDS"

    # gap, so the next cluster avoids ports in TIME_WAIT
    PORT_CURSOR=$((PORT_CURSOR + w + 3))

    local tries
    for tries in $(seq 1 60); do
        if "$PY" - "$CO_ADDR" <<'PROBE' >/dev/null 2>&1; then return 0; fi
import socket, sys
h, _, p = sys.argv[1].rpartition(":")
s = socket.socket(); s.settimeout(1.0); s.connect((h, int(p)))
PROBE
        sleep 0.5
    done
    echo "      coordinator log:" >&2
    sed 's/^/        | /' "$WORK/coordinator.log" | head -6 >&2
    return 1
}

stop_cluster() {
    # shellcheck disable=SC2086
    [ -n "$CLUSTER_PIDS" ] && kill $CLUSTER_PIDS 2>/dev/null
    wait 2>/dev/null
    CLUSTER_PIDS=""
}

# run_stream <dataset> <W> <dist> <batch> <out>
run_stream() {
    local ds="$1" w="$2" dist="$3" batch="$4" out="$5"
    rm -f "$out"
    if ! start_cluster "$w" "$dist"; then
        bad "W=$w dist=$dist: cluster did not start -- not a wrong answer"
        stop_cluster
        return 1
    fi
    if ! "$PY" "$SRC/producer.py" "$CO_ADDR" --input "$ds" --batch "$batch" --wire "$WIRE" \
            --report "$out" --progress 0 > "$WORK/prod.log" 2>&1; then
        bad "W=$w dist=$dist batch=$batch: producer failed -- not a wrong answer"
        sed 's/^/        | /' "$WORK/prod.log" | head -6
        stop_cluster
        return 1
    fi
    stop_cluster
    [ -s "$out" ] || { bad "W=$w dist=$dist batch=$batch: empty report"; return 1; }
    return 0
}

hr; echo "1. the hand-computed fixtures"; hr
for exp in "$ROOT"/tests/*.expected; do
    name="$(basename "$exp" .expected)"
    ds="$ROOT/tests/$name.txt"
    for w in $WORKERS; do
        if run_stream "$ds" "$w" roundrobin 2 "$WORK/o"; then
            same "$WORK/o" "$exp" && ok "$name [W=$w]" || {
                bad "$name [W=$w]"; diff --strip-trailing-cr "$exp" "$WORK/o" | head -8; }
        fi
    done
done

hr; echo "2. generated datasets vs. the sequential C++ oracle"; hr
gen() { "$PY" "$ROOT/scripts/gen_logs.py" -o "$WORK/$1.txt" "${@:2}" >/dev/null; }
# skewed ranking, flat (ties decided by id), and a 30-day span (sparse intervals)
gen small   -n 5000  -k 5  --servers 8   --endpoints 40   --seed 1
gen skewed  -n 40000 -k 10 --servers 200 --endpoints 2000 --alpha 1.6 --seed 2
gen flat    -n 40000 -k 10 --servers 16  --endpoints 32   --alpha 0.0 --seed 3
gen wide    -n 20000 -k 5  --servers 32  --endpoints 64   --span 2592000 --seed 4

for name in small skewed flat wide; do
    ds="$WORK/$name.txt"
    "$SEQ" "$ds" "$WORK/$name.ref" 2>/dev/null || {
        bad "$name: the oracle itself failed"; continue; }
    for w in $WORKERS; do
        for dist in $DISTS; do
            if run_stream "$ds" "$w" "$dist" 1000 "$WORK/o"; then
                same "$WORK/o" "$WORK/$name.ref" \
                    && ok "$name [W=$w $dist] == sequential C++" || {
                    bad "$name [W=$w $dist] differs from the oracle"
                    diff --strip-trailing-cr "$WORK/$name.ref" "$WORK/o" | head -8; }
            fi
        done
    done
done

hr; echo "3. invariance across worker count, distribution and batch size"; hr
# batch size 1 sends every record in its own message
for name in skewed flat; do
    all_same=1
    for w in $WORKERS; do
        for dist in $DISTS; do
            for b in $BATCHES; do
                if run_stream "$WORK/$name.txt" "$w" "$dist" "$b" "$WORK/o"; then
                    same "$WORK/o" "$WORK/$name.ref" || {
                        all_same=0
                        bad "$name: W=$w dist=$dist batch=$b differs from the oracle"; }
                else
                    all_same=0
                fi
            done
        done
    done
    [ "$all_same" -eq 1 ] && ok "$name: identical bytes for every W, distribution and batch size"
done

hr; echo "4. queries during ingestion"; hr
# One dashboard polls while a rate-limited producer streams: queries must be answered
# mid-stream, the running total must never decrease, and the final report must be exact.
ds="$WORK/skewed.txt"
if start_cluster 3 roundrobin; then
    "$PY" "$SRC/producer.py" "$CO_ADDR" --input "$ds" --batch 50 --rate 8000 --wire "$WIRE" \
        --report "$WORK/final.txt" --progress 0 > "$WORK/prod.log" 2>&1 &
    PROD=$!

    # no --follow: the dashboard exits by itself once ingestion is complete
    "$PY" "$SRC/dashboard.py" "$CO_ADDR" --interval 0.15 --samples 400 \
        --plain --latency-out "$WORK/lat.csv" > /dev/null 2>"$WORK/dash.log" &
    DASH=$!

    wait "$PROD" 2>/dev/null
    prod_rc=$?
    wait "$DASH" 2>/dev/null
    stop_cluster

    if [ "$prod_rc" -ne 0 ]; then
        bad "producer failed during the concurrent-query test"
        sed 's/^/        | /' "$WORK/prod.log" | head -6
    fi

    if [ -s "$WORK/lat.csv" ]; then
        # Columns: sample,latency_ms,total_requests,records_ingested,merge_ms,done
        nq=$(awk -F, 'NR>1' "$WORK/lat.csv" | wc -l | tr -d " ")
        mono=$(awk -F, 'NR>1 {if ($3 < prev) bad=1; prev=$3} END {print bad+0}' "$WORK/lat.csv")
        during=$(awk -F, 'NR>1 && $6 == 0' "$WORK/lat.csv" | wc -l | tr -d " ")
        p95=$(awk -F, 'NR>1 {print $2}' "$WORK/lat.csv" | sort -n \
              | awk '{a[NR]=$1} END {i=int(0.95*NR); if (i<1) i=1; printf "%.1f", a[i]}')

        [ "$during" -gt 5 ] \
            && ok "$during analytics queries served WHILE ingesting (p95 ${p95} ms, $nq total)" \
            || bad "only $during queries were answered before ingestion finished"

        [ "$mono" -eq 0 ] \
            && ok "running total never decreased across $nq queries" \
            || bad "running total went backwards -- records lost or double-counted"
    else
        bad "the dashboard wrote no samples"
        sed 's/^/        | /' "$WORK/dash.log" | head -6
    fi

    if [ -s "$WORK/final.txt" ]; then
        same "$WORK/final.txt" "$WORK/skewed.ref" \
            && ok "final report is exact despite concurrent querying" \
            || { bad "final report differs after concurrent querying"
                 diff --strip-trailing-cr "$WORK/skewed.ref" "$WORK/final.txt" | head -8; }
    else
        bad "no final report was written"
    fi
fi

hr; echo "RESULT: $pass passed, $fail failed"; hr
[ "$fail" -eq 0 ]
