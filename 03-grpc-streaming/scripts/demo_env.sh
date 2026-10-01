#!/usr/bin/env bash
# Sets up the values for running the demo by hand (see the README, "Cluster demo").
#
#     salloc --nodes=4 --ntasks-per-node=1 --partition=debug --time=02:00:00
#     bash scripts/demo_env.sh          <- inside that salloc shell, once
#
# It works out the port and the four node names from the allocation and writes them
# to ~/.stream_demo_env, which every terminal then loads with
#
#     . ~/.stream_demo_env
#
# so the port and the node names are never typed by hand.

set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "Run this inside the salloc shell (SLURM_JOB_ID is not set here)." >&2
    exit 1
fi

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
if [ "${#NODES[@]}" -lt 4 ]; then
    echo "Need 4 nodes (1 coordinator + 3 workers); this allocation has ${#NODES[@]}." >&2
    exit 1
fi

# a different port for every allocation
PORT=$((50000 + SLURM_JOB_ID % 10000))
ENVF="$HOME/.stream_demo_env"

cat > "$ENVF" <<EOF
export PORT=$PORT
export N1=${NODES[0]} N2=${NODES[1]} N3=${NODES[2]} N4=${NODES[3]}
export PYTHON=/usr/bin/python3
cd "$ROOT"
EOF

cat <<EOF
job $SLURM_JOB_ID   port $PORT   (written to $ENVF)

  coordinator, dashboard, producer : ${NODES[0]}
  worker w0                        : ${NODES[1]}
  worker w1                        : ${NODES[2]}
  worker w2                        : ${NODES[3]}

In every other terminal, on the login node:   . ~/.stream_demo_env
EOF
