#!/bin/bash
#SBATCH --job-name=mr-bsrun
#SBATCH --nodes=6-7
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=mr_bsrun_%j.log
#SBATCH --error=mr_bsrun_%j.err
#SBATCH --partition=debug

# The MapReduce-vs-MPI measurement, all in ONE allocation (same nodes, same
# files).
#
#     sbatch slurm/benchmark_srun.slurm
#
# Same allocation shape as the 01-mpi benchmark (6-7 nodes, 2 slots per
# node). Four parts:
#
#   0. Launch overhead: an EMPTY srun step and an EMPTY mpirun at each P.
#      The fixed cost each framework pays just to start its processes.
#      -> results/cluster/launch_overhead.csv
#   1. MapReduce, driven by Slurm (scripts/run_slurm.sh):
#        N in {1M, 5M, 10M, 20M, 50M} x P = 1..NNODES (one map task per node)
#        + P = 8 (2 per node on 4 nodes, flagged) x 3 reps, R = 4 reducers
#      -> results/cluster/benchmark_srun.csv
#   2. The 01-mpi MPI program on the same files and P, with per-rank peak
#      memory and the full mpirun wall time -> results/cluster/mpi_rerun.csv
#   3. Dataset identity: md5 of every file.

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1
module load python/3.12.5 2>/dev/null
module load hpcx-2.7.0/hpcx-ompi

NN="$SLURM_NNODES"
PLIST=$(seq -s' ' 1 "$NN"); [ "$NN" -lt 8 ] && PLIST="$PLIST 8"
echo "job $SLURM_JOB_ID on $NN node(s): $SLURM_NODELIST"
echo "P sweep: $PLIST"
make all || { echo "build failed"; exit 1; }
mkdir -p results/joblogs results/cluster
export LOGDIR="$PWD/results/joblogs"
export SIZES="${SIZES:-1000000 5000000 10000000 20000000 50000000}"
export REPS="${REPS:-3}"

echo
echo "============================================================"
echo "Part 0: launch overhead (empty srun step / empty mpirun), 5 reps"
echo "============================================================"
LCSV=results/cluster/launch_overhead.csv
echo "p,rep,srun_s,mpirun_s" > "$LCSV"
now() { date +%s.%N; }
for p in $PLIST; do
    per=1; while [ $(( (p + per - 1) / per )) -gt "$NN" ]; do per=$((per + 1)); done
    map="--map-by node"; [ "$p" -gt "$NN" ] && map="--map-by ppr:2:node"
    for rep in 1 2 3 4 5; do
        t=$(now); srun --nodes=$(( (p + per - 1) / per )) --ntasks="$p" --ntasks-per-node="$per" true
        ts=$(echo "$t $(now)" | awk '{printf "%.4f", $2 - $1}')
        t=$(now); mpirun -np "$p" $map --bind-to none -x OMPI_MCA_coll_hcoll_enable=0 true
        tm=$(echo "$t $(now)" | awk '{printf "%.4f", $2 - $1}')
        echo "$p,$rep,$ts,$tm" >> "$LCSV"
    done
    awk -F, -v p="$p" '$1==p {s+=$3; m+=$4; n++} END {printf "  P=%-2s  empty srun %.3fs   empty mpirun %.3fs\n", p, s/n, m/n}' "$LCSV"
done

echo
echo "============================================================"
echo "Part 1: MapReduce (Slurm-driven), sizes x P"
echo "============================================================"
SIZES="$SIZES" MAPS="${MAPS:-$PLIST}" REDUCES="${REDUCES:-4}" REPS="$REPS" \
    RUNNER=scripts/run_slurm.sh bash scripts/benchmark.sh
echo "mapreduce sweep rc=$?"
mv results/benchmark.csv results/cluster/benchmark_srun.csv
rm -rf "dfs/$SLURM_JOB_ID" ".mrwork/$SLURM_JOB_ID"

echo
echo "============================================================"
echo "Part 2: MPI (01-mpi program) on the same nodes and files"
echo "============================================================"
export PACK_ABOVE="$NN" PROCS="$PLIST" OUT_CSV=results/cluster/mpi_rerun.csv
MPI_DIR="${MPI_DIR:-$PWD/../01-mpi}" bash scripts/mpi_rerun.sh
echo "mpi rerun rc=$?"

echo
echo "============================================================"
echo "Part 3: dataset identity"
echo "============================================================"
for n in $SIZES; do md5sum "data/logs_$n.txt"; done
echo "expected (laptop): 1M 0d91114e29e92bdbc58b1ffce2df9218  5M d48c801842cce98e73720a7db0e42cf6"
echo "                   10M 86d357d9157a4b15130597f066a2ff25 20M 146c2ce3d832b77fade676aa4994c263"

echo
# Plots and summary.md are made on the laptop: python3 scripts/plot.py
ls -la results/cluster/
