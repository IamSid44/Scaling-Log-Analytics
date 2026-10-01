#!/bin/bash
#SBATCH --job-name=stream-bench
#SBATCH --nodes=6-7
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=stream_bench_%j.log
#SBATCH --error=stream_bench_%j.err
#SBATCH --partition=debug

# Performance evaluation of the gRPC streaming analytics: nine experiments,
# each varying ONE thing, all written to results/benchmark.csv (column `exp`).
#
#     sbatch slurm/benchmark.slurm
#
# PLACEMENT: node[0] runs the coordinator, producer and query clients; each
# worker gets its own node (node[1..]). W is the variable under study, so it is
# the only thing that moves. With 7 nodes, W goes up to 6.
#
#   exp            varies                     fixed                               question
#   workers        W = 1..NN-1                raw, roundrobin, batch 1000, N=5M    does throughput scale with workers?
#   wire           W (parsed wire format)     as above                            where must parsing live for that?
#   distribution   W (hash)                   raw, batch 1000, N=5M                does record distribution matter?
#   batch          batch = 10..10000          W=4, raw, roundrobin, N=2M           streaming granularity vs throughput
#   queries        concurrent queriers 0..8   W=4, raw, roundrobin, N=5M           do queries interfere with ingestion?
#   latency        W, 1 querier, fixed rate   raw, 150k rec/s, N=2M                query latency vs number of workers
#   size           N = 1M, 5M, 10M            W=4, raw, roundrobin                 scaling with input size
#   workers_b10k   W = 1..NN-1                as `workers`, batch 10000            does the knee move with message size?
#   distribution_b10k  W (hash)               as `distribution`, batch 10000       hash vs round-robin without the stream cap
#
#     sbatch --export=ALL,EXPS="workers_b10k distribution_b10k" slurm/benchmark.slurm
#   runs only the named experiments and appends them to the existing CSV.
#
# Every row is diffed against the sequential C++ reference (`correct`).

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1
# on the cluster only the system /usr/bin/python3 can import grpc (see the README)
module purge 2>/dev/null
export PYTHON=/usr/bin/python3
mkdir -p .pybin && ln -sf /usr/bin/python3 .pybin/python3 && export PATH="$PWD/.pybin:$PATH"

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
NN=${#NODES[@]}
[ "$NN" -ge 5 ] || { echo "need at least 5 nodes (1 coordinator + 4 workers)"; exit 1; }
export LOCAL_NODE="${NODES[0]}"
export WORKER_NODES="${NODES[*]:1}"
WMAX=$((NN - 1))
W_ALL=$(seq -s' ' 1 "$WMAX")

echo "============================================================"
echo "job $SLURM_JOB_ID on $NN node(s): $SLURM_NODELIST"
echo "  coordinator / producer / queriers : $LOCAL_NODE"
echo "  worker nodes                      : $WORKER_NODES"
echo "  W sweep                           : $W_ALL (one worker per node)"
echo "============================================================"
echo "python: $(command -v python3)  $(python3 -V 2>&1)"
python3 -c 'import grpc, google.protobuf as p
from google.protobuf.internal import api_implementation as a
print("grpc", grpc.__version__, "| protobuf", p.__version__, "| protobuf backend:", a.Type())' \
    || { echo "ERROR: grpc not importable with /usr/bin/python3 -- stopping"; exit 1; }
# the protobuf backend ("python" or "cpp") matters for the results (see the README)
bash scripts/build_proto.sh || { echo "stubs unusable"; exit 1; }
g++ -O3 -std=c++17 -D_FILE_OFFSET_BITS=64 -o src/loganalytics_seq src/loganalytics_seq.cpp \
    || { echo "could not build the reference oracle"; exit 1; }

# Reuse 02-mapreduce's datasets (same generator, same seed, same bytes) where present.
mkdir -p data results
for n in 1000000 5000000 10000000; do
    src="../02-mapreduce/data/logs_$n.txt"
    [ -s "data/logs_$n.txt" ] || { [ -s "$src" ] && ln -sf "$(cd "$(dirname "$src")" && pwd)/logs_$n.txt" "data/logs_$n.txt"; }
done

echo; echo "=== node reachability ==="
for n in "${NODES[@]}"; do
    printf '  %-10s ' "$n"; srun --nodes=1 --ntasks=1 --nodelist="$n" --overlap hostname 2>&1 | head -1
done

export CSV="$PWD/results/benchmark.csv"
# EXPS="a b" runs only those experiments and APPENDS to the existing CSV;
# without it, everything runs into a fresh CSV (the old one is kept as .prev).
EXPS="${EXPS:-}"
[ -z "$EXPS" ] && [ -f "$CSV" ] && mv "$CSV" "$CSV.prev"

EXP_NO=0
exp() {   # exp <label> VAR=value ...   -- one experiment
    local label="$1"; shift
    EXP_NO=$((EXP_NO + 1))
    [ -z "$EXPS" ] || [[ " $EXPS " == *" $label "* ]] || return 0
    echo; echo "============================================================"
    echo "experiment: $label   ($*)"
    echo "============================================================"
    # its own port range per experiment
    env EXP="$label" BASE_PORT=$((40000 + EXP_NO * 2000 + (SLURM_JOB_ID % 50) * 20)) "$@" \
        bash scripts/benchmark.sh || echo "  (experiment $label reported failures)"
}

B="WIRE=raw DISTS=roundrobin BATCH_SIZES=1000 QUERIERS=0 RATE=0"
exp workers      $B SIZES=5000000 WORKERS="$W_ALL" REPS=3
# parsed runs at ~22k rec/s on the cluster, so 1M records (throughput does not depend on N)
exp wire         WIRE=parsed DISTS=roundrobin BATCH_SIZES=1000 QUERIERS=0 RATE=0 \
                    SIZES=1000000 WORKERS="$W_ALL" REPS=2
exp distribution WIRE=raw DISTS=hash BATCH_SIZES=1000 QUERIERS=0 RATE=0 \
                    SIZES=5000000 WORKERS="$W_ALL" REPS=3
exp batch        WIRE=raw DISTS=roundrobin BATCH_SIZES="10 100 1000 10000" QUERIERS=0 RATE=0 \
                    SIZES=2000000 WORKERS=4 REPS=3
exp queries      WIRE=raw DISTS=roundrobin BATCH_SIZES=1000 QUERIERS="0 1 4 8" RATE=0 \
                    SIZES=5000000 WORKERS=4 REPS=3
exp latency      WIRE=raw DISTS=roundrobin BATCH_SIZES=1000 QUERIERS=1 RATE=150000 \
                    SIZES=2000000 WORKERS="$W_ALL" REPS=2
exp size         $B SIZES="1000000 5000000 10000000" WORKERS=4 REPS=3
# the same W sweeps with 10,000-record messages, which lift the stream's ceiling (see the README)
exp workers_b10k      WIRE=raw DISTS=roundrobin BATCH_SIZES=10000 QUERIERS=0 RATE=0 \
                         SIZES=5000000 WORKERS="$W_ALL" REPS=3
exp distribution_b10k WIRE=raw DISTS=hash BATCH_SIZES=10000 QUERIERS=0 RATE=0 \
                         SIZES=5000000 WORKERS="$W_ALL" REPS=3

echo
CCOL=$(head -1 "$CSV" | tr ',' '\n' | grep -nx correct | cut -d: -f1)
echo "rows: $(($(wc -l < "$CSV") - 1)),  wrong answers: $(awk -F, -v c="$CCOL" 'NR>1 && $c!=1' "$CSV" | wc -l)"
# Plots and summary.md are produced on the laptop: python3 scripts/plot.py
ls -la results/
