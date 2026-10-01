#!/bin/bash
#SBATCH --job-name=loga-verify
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=01:00:00
#SBATCH --output=loga_verify_%j.log
#SBATCH --error=loga_verify_%j.err
#SBATCH --partition=debug

# Correctness suite, every rank on ONE node.
#
#     sbatch slurm/verify.slurm
#
# Correctness does not depend on physical placement -- only speed does, which
# is what slurm/benchmark.slurm (run separately, across real distinct nodes)
# measures. Keeping every rank on one node here means rank-to-rank
# communication stays in shared memory: the full P = 1..8 sweep, both I/O
# modes, runs quickly and without depending on the cluster's inter-node
# network being available.

set -uo pipefail
module load hpcx-2.7.0/hpcx-ompi
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1

# Only one node is allocated, so ranks fill its slots; there is nothing to
# spread across and no packed/default distinction is needed.
export MPI_FLAGS="--bind-to none -x OMPI_MCA_coll_hcoll_enable=0"
export MAP_DEFAULT="--map-by slot"
export PROCS="1 2 3 4 5 6 7 8"

echo "job $SLURM_JOB_ID on $SLURM_NNODES node(s): $SLURM_NODELIST"
echo "tasks: $SLURM_NTASKS | P sweep: $PROCS"
echo

make clean >/dev/null 2>&1
make all || { echo "build failed"; exit 1; }
echo

bash scripts/verify.sh
status=$?
echo
[ $status -eq 0 ] && echo "VERIFICATION PASSED" || echo "VERIFICATION FAILED (see above)"
exit $status
