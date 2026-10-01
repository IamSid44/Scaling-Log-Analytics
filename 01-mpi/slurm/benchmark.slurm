#!/bin/bash
#SBATCH --job-name=loga-bench
#SBATCH --nodes=6-7
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=loga_bench_%j.log
#SBATCH --error=loga_bench_%j.err
#SBATCH --partition=debug

# Benchmark sweep -- maximum distribution: ONE RANK PER NODE.
#
#     sbatch slurm/benchmark.slurm
#
# `--nodes=6-7` asks for seven machines but accepts six, because one of the
# eight is normally drained and waiting for a seventh can queue indefinitely.
# The P sweep is built from $SLURM_NNODES, so the job uses whatever it got:
#
#     P = 1 .. NNODES    --map-by node        1 rank per node, never two
#     P = 8              --map-by ppr:2:node  2 ranks on each of 4 nodes
#
# P = 8 cannot be one-per-node here (that needs 8 free machines), so it is run
# as 4 nodes x 2 ranks -- the widest spread available for it -- and is flagged
# as such: every CSV row records how many distinct hosts it landed on, and
# plot.py footnotes any P that was not one-per-node. This keeps the P = 8
# column without quietly pretending it was measured under the same placement.
#
# `--ntasks-per-node=2` is what reserves the second slot per machine that the
# packed P = 8 case needs; at P <= NNODES the second slot simply goes unused.
#
# Overwrites results/benchmark.csv -- move the old one aside to keep both.

set -uo pipefail
module load hpcx-2.7.0/hpcx-ompi
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1

# The --map-by choice is NOT in MPI_FLAGS: benchmark.sh picks it per process
# count, so each row can report its placement honestly.
export MPI_FLAGS="--bind-to none -x OMPI_MCA_coll_hcoll_enable=0"
export MAP_DEFAULT="--map-by node"
export MAP_PACK="--map-by ppr:2:node"
export MAP_PACK_ABOVE="$SLURM_NNODES"

export SIZES="${SIZES:-1000000 5000000 10000000 20000000}"
export REPS="${REPS:-3}"

# P = 1..NNODES one-per-node, then P = 8 packed 2-per-node (skipped if the
# allocation already covers 8 one-per-node, which would make it a duplicate).
PLIST=$(seq -s' ' 1 "$SLURM_NNODES")
[ "$SLURM_NNODES" -lt 8 ] && PLIST="$PLIST 8"
export PROCS="$PLIST"

echo "job $SLURM_JOB_ID on $SLURM_NNODES node(s): $SLURM_NODELIST"
echo "tasks: $SLURM_NTASKS | P sweep: $PROCS"
echo

make all || { echo "build failed"; exit 1; }
echo

echo "=== rank placement (ranks per node, at each P) ==="
for p in $PROCS; do
    if [ "$p" -gt "$MAP_PACK_ABOVE" ]; then m="$MAP_PACK"; else m="$MAP_DEFAULT"; fi
    echo "P=$p  [$m]:"
    mpirun -np "$p" $m $MPI_FLAGS hostname | sort | uniq -c | sed 's/^/    /'
done
echo "================================================="
echo

bash scripts/benchmark.sh || { echo "benchmark failed"; exit 1; }
echo
# The cluster has no matplotlib; plot.py still writes summary.md.
python3 scripts/plot.py || echo "plot.py failed -- benchmark.csv is still valid"
echo
ls -la results/
