#!/bin/bash
#SBATCH --job-name=stream-verify
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=stream_verify_%j.log
#SBATCH --error=stream_verify_%j.err
#SBATCH --partition=debug

# Correctness suite on a compute node: the streamed analytics must be
# byte-identical to the sequential C++ reference for every worker count,
# distribution, batch size, and both wire formats; and queries during ingestion
# must be served, monotone, and leave the final answer exact.
#
#     sbatch slurm/verify.slurm        (the multi-node run is slurm/demo.slurm)

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1
# on the cluster only the system /usr/bin/python3 can import grpc (see the README)
module purge 2>/dev/null
export PYTHON=/usr/bin/python3
mkdir -p .pybin && ln -sf /usr/bin/python3 .pybin/python3 && export PATH="$PWD/.pybin:$PATH"

echo "job $SLURM_JOB_ID on $(hostname)"
python3 -c 'import grpc, google.protobuf as p; print("grpc", grpc.__version__, "| protobuf", p.__version__)' \
    || { echo "ERROR: grpc not importable"; exit 1; }
bash scripts/build_proto.sh || exit 1
g++ -O3 -std=c++17 -D_FILE_OFFSET_BITS=64 -o src/loganalytics_seq src/loganalytics_seq.cpp || exit 1

echo; echo "=== wire = raw (workers parse; the default) ==="
WIRE=raw bash scripts/verify.sh; R1=$?
echo; echo "=== wire = parsed (producer parses) ==="
WIRE=parsed WORKERS="1 3" BATCHES="1 1000" bash scripts/verify.sh; R2=$?
echo; echo "raw rc=$R1  parsed rc=$R2"
exit $(( R1 || R2 ))
