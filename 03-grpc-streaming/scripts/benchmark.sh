#!/usr/bin/env bash
# Benchmark sweep: SIZES x WORKERS x DISTS x BATCH_SIZES x QUERIERS, REPS repetitions
# (repetitions outermost), every run diffed against the sequential program (`correct`).
#
#     sbatch slurm/benchmark.slurm                 (sets WORKER_NODES and LOCAL_NODE)
#     SIZES=1000000 WORKERS="1 2" REPS=1 bash scripts/benchmark.sh     (inside salloc)
#
# Appends one row per run to results/benchmark.csv. cpu_* are CPU seconds over the
# stream; divided by t_ingest they give the cores each component kept busy.
# Workers go one per node; past PACK_ABOVE they wrap, and `nodes` records how many
# distinct machines the workers actually used.

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/src"
PY="${PYTHON:-python3}"

SIZES="${SIZES:-1000000 5000000 10000000}"
WORKERS="${WORKERS:-1 2 3 4 5 6 8}"
DISTS="${DISTS:-roundrobin hash}"
BATCH_SIZES="${BATCH_SIZES:-100 1000 10000}"
QUERIERS="${QUERIERS:-0 1 4}"
RATE="${RATE:-0}"
WIRE="${WIRE:-raw}"                 # raw (workers parse) | parsed (producer parses)
EXP="${EXP:-sweep}"                 # experiment label, written on every row
CO_CPUS="${CO_CPUS:-3}"             # CPUs for the coordinator's srun step
WORKER_CPUS="${WORKER_CPUS:-2}"     # CPUs for each worker's srun step
REPS="${REPS:-3}"
SEED="${SEED:-42}"
BASE_PORT="${BASE_PORT:-51000}"

# Filled in by the SLURM job. Empty means "everything on this machine".
WORKER_NODES="${WORKER_NODES:-}"
LOCAL_NODE="${LOCAL_NODE:-}"
PACK_ABOVE="${PACK_ABOVE:-999}"

ext=""
[ -x "$SRC/loganalytics_seq" ] || ext=".exe"
SEQ="$SRC/loganalytics_seq$ext"

DATA="$ROOT/data"; RESULTS="$ROOT/results"; CSV="${CSV:-$RESULTS/benchmark.csv}"
mkdir -p "$DATA" "$RESULTS"
WORK="$(mktemp -d)"
PIDS=""
cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null; rm -rf "$WORK"; return 0; }
trap cleanup EXIT

[ -x "$SEQ" ] || ${CXX:-g++} -O3 -std=c++17 -D_FILE_OFFSET_BITS=64 \
    -o "$SEQ" "$SRC/loganalytics_seq.cpp" || { echo "cannot build the oracle"; exit 1; }
[ -f "$SRC/analytics_pb2.py" ] || bash "$ROOT/scripts/build_proto.sh" || exit 1

# Append, so several experiments (EXP=...) can share one CSV; header once.
[ -s "$CSV" ] || echo "exp,wire,n,bytes,workers,nodes,rep,distribution,batch,rate,queriers,records,batches,t_ingest,throughput_rps,t_merge,q_samples,q_p50_ms,q_p95_ms,q_max_ms,balance_pct,correct,cpu_producer,cpu_coord,cpu_worker_max,fold_worker_max" > "$CSV"

# launch <node> <command...>  -- on the cluster, pin to that machine.
launch() {
    local node="$1"; shift
    if [ -n "${SLURM_JOB_ID:-}" ] && [ -n "$node" ]; then
        # the cluster confines each step to its CPUs (ConstrainCores=yes)
        srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap \
             --cpus-per-task="${LAUNCH_CPUS:-1}" "$@"
    else
        "$@"
    fi
}

# spawn <node> <command...> &  -- as launch, for background processes; exec makes
# $! the server itself, so killing it frees the port
spawn() {
    local node="$1"; shift
    if [ -n "${SLURM_JOB_ID:-}" ] && [ -n "$node" ]; then
        exec srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap \
             --cpus-per-task="${LAUNCH_CPUS:-1}" "$@"
    else
        exec "$@"
    fi
}

PORT_CURSOR="$BASE_PORT"
CO_ADDR=""; CLUSTER_PIDS=""; WORKER_HOSTS=""

start_cluster() {   # start_cluster <W> <dist>
    local w="$1" dist="$2" i node addrs="" port
    CLUSTER_PIDS=""; addrs=""; WORKER_HOSTS=""

    # worker i -> WORKER_NODES[i mod len]; without WORKER_NODES, all local
    local nodes=() n_nodes=0
    if [ -n "$WORKER_NODES" ]; then
        # shellcheck disable=SC2206
        nodes=($WORKER_NODES); n_nodes=${#nodes[@]}
    fi

    for i in $(seq 0 $((w - 1))); do
        port=$((PORT_CURSOR + 1 + i))
        node=""
        if [ "$n_nodes" -gt 0 ]; then node="${nodes[$((i % n_nodes))]}"; fi
        LAUNCH_CPUS="$WORKER_CPUS" spawn "$node" "$PY" "$SRC/worker.py" "0.0.0.0:$port" \
               --worker-id "w$i" --quiet > "$WORK/w$i.log" 2>&1 &
        CLUSTER_PIDS="$CLUSTER_PIDS $!"
        if [ -n "$node" ]; then
            addrs="$addrs,$node:$port"
            WORKER_HOSTS="$WORKER_HOSTS $node"
        else
            addrs="$addrs,localhost:$port"
            WORKER_HOSTS="$WORKER_HOSTS localhost"
        fi
    done
    addrs="${addrs#,}"

    local co_host="localhost"
    [ -n "$LOCAL_NODE" ] && co_host="$LOCAL_NODE"
    CO_ADDR="$co_host:$PORT_CURSOR"

    LAUNCH_CPUS="$CO_CPUS" spawn "$LOCAL_NODE" "$PY" "$SRC/coordinator.py" "0.0.0.0:$PORT_CURSOR" \
           --workers "$addrs" --distribution "$dist" --quiet \
           > "$WORK/co.log" 2>&1 &
    CLUSTER_PIDS="$CLUSTER_PIDS $!"
    PIDS="$PIDS $CLUSTER_PIDS"
    PORT_CURSOR=$((PORT_CURSOR + w + 3))

    local tries
    for tries in $(seq 1 120); do
        if "$PY" - "$CO_ADDR" <<'PROBE' >/dev/null 2>&1; then return 0; fi
import socket, sys
h, _, p = sys.argv[1].rpartition(":")
s = socket.socket(); s.settimeout(1.0); s.connect((h, int(p)))
PROBE
        sleep 0.5
    done
    echo "  cluster failed to start (W=$w dist=$dist):"
    sed 's/^/    | /' "$WORK/co.log" | head -6
    return 1
}

stop_cluster() {
    [ -n "$CLUSTER_PIDS" ] && kill $CLUSTER_PIDS 2>/dev/null
    wait 2>/dev/null
    CLUSTER_PIDS=""
    return 0
}

missing=0

for rep in $(seq 1 "$REPS"); do
for n in $SIZES; do
    input="$DATA/logs_${n}.txt"
    if [ ! -s "$input" ]; then
        echo "Generating $input ..."
        "$PY" "$ROOT/scripts/gen_logs.py" -n "$n" -o "$input" -k 10 \
              --servers 64 --endpoints 512 --seed "$SEED" || exit 1
    fi
    bytes=$(wc -c < "$input" | tr -d ' ')
    cat "$input" > /dev/null        # warm the page cache: time the pipeline, not a cold NFS read

    # The reference answer for this dataset, computed once.
    ref="$WORK/ref_${n}.txt"
    [ -s "$ref" ] || "$SEQ" "$input" "$ref" 2>/dev/null

    for dist in $DISTS; do
      for batch in $BATCH_SIZES; do
        for q in $QUERIERS; do
          for w in $WORKERS; do

              if ! start_cluster "$w" "$dist"; then missing=1; continue; fi

              # concurrent query clients, each with its own latency CSV
              QPIDS=""
              for qi in $(seq 1 "$q"); do
                  spawn "$LOCAL_NODE" "$PY" "$SRC/dashboard.py" "$CO_ADDR" \
                         --interval 0.1 --samples 100000 --plain \
                         --latency-out "$WORK/lat_$qi.csv" \
                         > /dev/null 2>&1 &
                  QPIDS="$QPIDS $!"
              done

              if ! LAUNCH_CPUS=2 launch "$LOCAL_NODE" "$PY" "$SRC/producer.py" "$CO_ADDR" \
                      --input "$input" --batch "$batch" --rate "$RATE" --wire "$WIRE" \
                      --report "$WORK/out.txt" --progress 0 \
                      > "$WORK/prod.out" 2>"$WORK/prod.err"; then
                  echo "  RUN FAILED: n=$n W=$w $dist batch=$batch q=$q rep=$rep"
                  head -4 "$WORK/prod.err"
                  [ -n "$QPIDS" ] && kill $QPIDS 2>/dev/null
                  stop_cluster; missing=1; continue
              fi

              # Per-worker record counts, for the load-balance column.
              "$PY" "$SRC/dashboard.py" "$CO_ADDR" --once --plain \
                    > "$WORK/final_panel.txt" 2>/dev/null

              [ -n "$QPIDS" ] && wait $QPIDS 2>/dev/null
              stop_cluster

              # CPU seconds per component over the stream
              cpu_co=$(awk '$1 == "INGEST_CPU" {v=$2} END {print v}' "$WORK/co.log")
              cpu_wk=$(cat "$WORK"/w*.log 2>/dev/null | awk '$1 == "INGEST_CPU" && $2 > m {m=$2} END {print m}')
              fold_wk=$(cat "$WORK"/w*.log 2>/dev/null | awk '$1 == "INGEST_CPU" && $4 > m {m=$4} END {print m}')

              f() { awk -v k="$1" '$1 == k {print $2; exit}' "$WORK/prod.out"; }

              # correctness, on every run
              correct=0
              diff -q --strip-trailing-cr "$WORK/out.txt" "$ref" >/dev/null 2>&1 && correct=1
              [ "$correct" -eq 0 ] && echo "  WRONG ANSWER: n=$n W=$w $dist batch=$batch q=$q"

              # Query latency, pooled over every concurrent querier.
              qs=0; p50=""; p95=""; qmax=""
              if [ "$q" -gt 0 ] && ls "$WORK"/lat_*.csv >/dev/null 2>&1; then
                  cat "$WORK"/lat_*.csv | awk -F, 'NR>1 && $2 ~ /^[0-9.]+$/ {print $2}' \
                      | sort -n > "$WORK/all_lat"
                  qs=$(wc -l < "$WORK/all_lat" | tr -d ' ')
                  if [ "$qs" -gt 0 ]; then
                      p50=$(awk '{a[NR]=$1} END {i=int(0.50*NR); if(i<1)i=1; printf "%.4f", a[i]}' "$WORK/all_lat")
                      p95=$(awk '{a[NR]=$1} END {i=int(0.95*NR); if(i<1)i=1; printf "%.4f", a[i]}' "$WORK/all_lat")
                      qmax=$(tail -1 "$WORK/all_lat")
                  fi
              fi
              rm -f "$WORK"/lat_*.csv

              # busiest worker's share, % of an even split (100 = even)
              balance=$(awk '/^ +w[0-9]/ {gsub("%","",$(NF-1)); s[++n]=$(NF-1)+0}
                             END {if (n==0) {print ""; exit}
                                  m=0; for (i=1;i<=n;i++) if (s[i]>m) m=s[i];
                                  printf "%.1f", m * n}' "$WORK/final_panel.txt")

              nodes_used=$(echo "$WORKER_HOSTS" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')

              printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
                  "$EXP" "$WIRE" "$n" "$bytes" "$w" "$nodes_used" "$rep" "$dist" "$batch" "$RATE" "$q" \
                  "$(f RECORDS)" "$(f BATCHES)" "$(f TIME_INGEST)" \
                  "$(f THROUGHPUT_RPS)" "$(f TIME_MERGE)" \
                  "$qs" "$p50" "$p95" "$qmax" "$balance" "$correct" \
                  "$(f CPU_PRODUCER)" "$cpu_co" "$cpu_wk" "$fold_wk" >> "$CSV"

              t_in=$(f TIME_INGEST)
              busy() { awk -v c="$1" -v t="$t_in" 'BEGIN { if (c != "" && t > 0) printf "%.2f", c / t; else printf "-" }'; }
              printf '[%s/%s] rep %s N=%-9s W=%-2s %-11s batch=%-6s q=%s : %9s rec/s, correct=%s | cores busy: producer %s, coordinator %s, worker %s\n' \
                  "$EXP" "$WIRE" "$rep" "$n" "$w" "$dist" "$batch" "$q" "$(f THROUGHPUT_RPS)" "$correct" \
                  "$(busy "$(f CPU_PRODUCER)")" "$(busy "$cpu_co")" "$(busy "$cpu_wk")"
          done
        done
      done
    done
    echo
done
done

echo "Wrote $CSV"
exit $missing
