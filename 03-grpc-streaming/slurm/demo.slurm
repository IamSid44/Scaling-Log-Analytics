#!/bin/bash
#SBATCH --job-name=stream-demo
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=3
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=00:40:00
#SBATCH --output=stream_demo_%j.log
#SBATCH --error=stream_demo_%j.err
#SBATCH --partition=debug

# Demonstration + correctness run for the gRPC streaming analytics.
#
#     sbatch slurm/demo.slurm
#
#     node[0]        coordinator, producer, dashboard
#     node[1..3]     one analytics worker each
#
# The dashboard queries while the stream runs; at the end the report is diffed
# against the sequential C++ reference.

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1

# on the cluster only the system /usr/bin/python3 can import grpc (see the README)
module purge 2>/dev/null
export PYTHON=/usr/bin/python3
mkdir -p .pybin && ln -sf /usr/bin/python3 .pybin/python3 && export PATH="$PWD/.pybin:$PATH"

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
NN=${#NODES[@]}
CO="${NODES[0]}"
W_NODES=("${NODES[@]:1}")
NW=${#W_NODES[@]}
[ "$NW" -gt 3 ] && NW=3
if [ "$NW" -lt 1 ]; then
    echo "need at least 2 nodes"; exit 1
fi

# Port derived from the job id, so two of these jobs cannot collide.
PORT=$(( 51000 + (SLURM_JOB_ID % 5000) ))
N="${N:-2000000}"
DATASET="data/logs_${N}.txt"

echo "============================================================"
echo "job $SLURM_JOB_ID | $NN node(s): $SLURM_NODELIST"
echo "  coordinator + producer + dashboard : $CO  (port $PORT)"
echo "  workers                            : ${W_NODES[*]:0:$NW}"
echo "============================================================"
echo

bash scripts/build_proto.sh || exit 1
g++ -O3 -std=c++17 -D_FILE_OFFSET_BITS=64 \
    -o src/loganalytics_seq src/loganalytics_seq.cpp || exit 1

mkdir -p data results/demo
[ -s "$DATASET" ] || python3 scripts/gen_logs.py -n "$N" -o "$DATASET" \
    -k 10 --servers 64 --endpoints 512 --seed 42

echo "computing the sequential reference answer ..."
./src/loganalytics_seq "$DATASET" results/demo/reference.txt 2>/dev/null
echo

PIDS=""
trap 'kill $PIDS 2>/dev/null' EXIT

ADDRS=""
for i in $(seq 0 $((NW - 1))); do
    node="${W_NODES[$i]}"
    p=$((PORT + 1 + i))
    srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap \
         python3 src/worker.py "0.0.0.0:$p" --worker-id "w$i" \
         > "results/demo/worker_$i.log" 2>&1 &
    PIDS="$PIDS $!"
    ADDRS="$ADDRS,$node:$p"
    echo "  worker w$i -> $node:$p"
done
ADDRS="${ADDRS#,}"

srun --nodes=1 --ntasks=1 --nodelist="$CO" --overlap \
     python3 src/coordinator.py "0.0.0.0:$PORT" --workers "$ADDRS" \
     --distribution roundrobin > "results/demo/coordinator.log" 2>&1 &
PIDS="$PIDS $!"

# Probe from the coordinator's own node. If this never succeeds the problem is
# the coordinator, not the network; the worker logs are printed either way.
echo "waiting for the coordinator to accept connections ..."
READY=0
for _ in $(seq 1 120); do
    if srun --nodes=1 --ntasks=1 --nodelist="$CO" --overlap python3 -c "
import socket
s = socket.socket(); s.settimeout(2); s.connect(('$CO', $PORT))
" >/dev/null 2>&1; then READY=1; echo "ready"; break; fi
    sleep 0.5
done
if [ "$READY" -ne 1 ]; then
    echo "ERROR: the coordinator never accepted a connection."
    echo "--- coordinator log ---"; sed 's/^/  | /' results/demo/coordinator.log | head -20
    for i in $(seq 0 $((NW - 1))); do
        echo "--- worker $i log ---"; sed 's/^/  | /' "results/demo/worker_$i.log" | head -10
    done
    exit 1
fi
echo

# the dashboard queries for the whole stream, one panel per second
srun --nodes=1 --ntasks=1 --nodelist="$CO" --overlap \
     python3 src/dashboard.py "$CO:$PORT" --interval 1.0 --plain --samples 400 \
     --latency-out results/demo/query_latency.csv \
     --report-out results/demo/dashboard_report.txt > results/demo/dashboard.txt 2>&1 &
DASH=$!
PIDS="$PIDS $DASH"

echo "============================================================"
echo "streaming $N records from $CO through $NW worker node(s)"
echo "============================================================"
srun --nodes=1 --ntasks=1 --nodelist="$CO" --overlap \
     python3 src/producer.py "$CO:$PORT" --input "$DATASET" --batch 1000 \
     --rate "${RATE:-100000}" \
     --report results/demo/producer_report.txt --progress 0
# --rate: a live-looking stream (~20 s for 2M records) so the dashboard shows
# the analytics converging; unthrottled, the pipeline finishes in ~2 s.
echo

wait "$DASH" 2>/dev/null

echo "============================================================"
echo "live dashboard -- final panels (queried DURING ingestion)"
echo "============================================================"
tail -60 results/demo/dashboard.txt
echo

echo "============================================================"
echo "CORRECTNESS: streamed result vs. the sequential C++ reference"
echo "============================================================"
if diff results/demo/reference.txt results/demo/producer_report.txt > results/demo/diff.txt 2>&1; then
    echo "PASS -- byte-identical, across $NW workers on $NW separate nodes."
    echo
    cat results/demo/producer_report.txt
    RC=0
else
    echo "FAIL -- the streamed answer differs from the sequential reference:"
    head -20 results/demo/diff.txt
    RC=1
fi

echo
echo "artefacts: results/demo/dashboard.txt, results/demo/query_latency.csv,"
echo "           results/demo/producer_report.txt, results/demo/worker_*.log"
exit $RC
