#!/bin/bash
#SBATCH --job-name=mr-vsrun
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=3
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=mr_vsrun_%j.log
#SBATCH --error=mr_vsrun_%j.err
#SBATCH --partition=debug

# Correctness of the Slurm-driven MapReduce (scripts/run_slurm.sh) on real nodes.
#
#     sbatch slurm/verify_srun.slurm
#
# Part 1 is the no-cluster suite (the logic). Part 2 runs every case again with
# map and reduce tasks spread over the 3 allocated nodes by srun, P up to 8
# (3 per node at most). Expect "RESULT: 88 passed, 0 failed" twice.

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1
module load python/3.12.5 2>/dev/null

echo "job $SLURM_JOB_ID on $SLURM_NNODES node(s): $SLURM_NODELIST"
make all || { echo "build failed"; exit 1; }

echo "=== Part 1: local harness (no cluster) ==="
MODE=local bash scripts/verify.sh
echo "local rc=$?"
echo
echo "=== Part 2: Slurm-driven MapReduce across $SLURM_NNODES nodes ==="
MODE=slurm bash scripts/verify.sh
RC=$?
echo "slurm rc=$RC"

echo
echo "=== one run in full, to show where every task ran ==="
bash scripts/run_slurm.sh tests/tiny.txt /tmp/$USER-tiny.out 5 3 2>&1 | head -30
diff tests/tiny.expected /tmp/$USER-tiny.out && echo "tiny: identical to the expected report"
rm -rf "dfs/$SLURM_JOB_ID" ".mrwork/$SLURM_JOB_ID" /tmp/$USER-tiny.out
exit $RC
