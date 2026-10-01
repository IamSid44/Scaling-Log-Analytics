#!/bin/bash
#SBATCH --job-name=mr-onejob
#SBATCH --nodes=6-7
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=mr_onejob_%j.log
#SBATCH --error=mr_onejob_%j.err
#SBATCH --partition=debug

# Ablation: how much of MapReduce's gap to MPI is the second job?
#
#     sbatch slurm/ablation_onejob.slurm
#
# The two-job pipeline (map -> R = 4 reducers -> one report task) against the
# single-job one (map -> ONE reducer that sums AND ranks: mr_report), which has
# the same shape as MPI's local aggregate + MPI_Reduce to a root that prints.
# Same allocation shape as slurm/benchmark_srun.slurm. Four parts:
#
#   0. Correctness of the single-job variant across the nodes (verify.sh).
#   1. Launch overhead: an empty srun step and an empty mpirun at each P.
#   2. The sweep, INTERLEAVED: for every (N, P) the two-job run, the one-job
#      run and MPI back to back, on the same staged splits, so the three are
#      compared minutes apart rather than across a whole sweep. The order of
#      the two MapReduce variants alternates with P, so neither always runs
#      first. Each configuration gets its own warm-up, then 3 repetitions.
#   3. Dataset identity: md5 of every file.
#
# Writes results/cluster/ablation_{twojob,onejob,mpi,launch}.csv.

set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}" || exit 1
module load python/3.12.5 2>/dev/null
module load hpcx-2.7.0/hpcx-ompi

NN="$SLURM_NNODES"
PLIST=$(seq -s' ' 1 "$NN"); [ "$NN" -lt 8 ] && PLIST="$PLIST 8"
SIZES="${SIZES:-1000000 5000000 10000000 20000000 50000000}"
export REPS="${REPS:-3}"
OUT=results/cluster
mkdir -p "$OUT"
echo "job $SLURM_JOB_ID on $NN node(s): $SLURM_NODELIST"
echo "sizes: $SIZES | P sweep: $PLIST | reps: $REPS"
make all || { echo "build failed"; exit 1; }

echo
echo "============================================================"
echo "Part 0: correctness of the single-job variant (Slurm-driven)"
echo "============================================================"
MODE=slurm ONE_JOB=1 REDUCES=1 bash scripts/verify.sh
echo "verify rc=$?"

echo
echo "============================================================"
echo "Part 1: launch overhead (empty srun step / empty mpirun), 5 reps"
echo "============================================================"
LCSV="$OUT/ablation_launch.csv"
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
echo "Part 2: two-job vs one-job MapReduce vs MPI, interleaved per (N, P)"
echo "============================================================"
TMPC="$(mktemp -d)"; trap 'rm -rf "$TMPC"' EXIT
append() {   # append <from.csv> <to.csv>: keep one header
    [ -s "$1" ] || return
    [ -s "$2" ] || head -1 "$1" > "$2"
    tail -n +2 "$1" >> "$2"
}
rm -f "$OUT"/ablation_twojob.csv "$OUT"/ablation_onejob.csv "$OUT"/ablation_mpi.csv
two()  { ONE_JOB=0 SIZES="$1" MAPS="$2" REDUCES=4 RUNNER=scripts/run_slurm.sh \
             OUT_CSV="$TMPC/t.csv" bash scripts/benchmark.sh
         append "$TMPC/t.csv" "$OUT/ablation_twojob.csv"; }
one()  { ONE_JOB=1 SIZES="$1" MAPS="$2" REDUCES=1 RUNNER=scripts/run_slurm.sh \
             OUT_CSV="$TMPC/o.csv" bash scripts/benchmark.sh
         append "$TMPC/o.csv" "$OUT/ablation_onejob.csv"; }
for n in $SIZES; do
    for p in $PLIST; do
        echo "---- N=$n P=$p ----"
        if [ $((p % 2)) -eq 1 ]; then two "$n" "$p"; one "$n" "$p"
        else                          one "$n" "$p"; two "$n" "$p"; fi
        SIZES="$n" PROCS="$p" PACK_ABOVE="$NN" OUT_CSV="$TMPC/m.csv" \
            MPI_DIR="${MPI_DIR:-$PWD/../01-mpi}" bash scripts/mpi_rerun.sh
        append "$TMPC/m.csv" "$OUT/ablation_mpi.csv"
    done
done
rm -rf "dfs/$SLURM_JOB_ID" ".mrwork/$SLURM_JOB_ID"

echo
echo "============================================================"
echo "Part 3: dataset identity"
echo "============================================================"
for n in $SIZES; do md5sum "data/logs_$n.txt"; done

echo
echo "rows: two-job $(($(wc -l < "$OUT/ablation_twojob.csv") - 1)), one-job $(($(wc -l < "$OUT/ablation_onejob.csv") - 1)), MPI $(($(wc -l < "$OUT/ablation_mpi.csv") - 1))"
echo "correct=0 rows: $(cat "$OUT"/ablation_twojob.csv "$OUT"/ablation_onejob.csv "$OUT"/ablation_mpi.csv | awk -F, '$NF == "0"' | wc -l)"
ls -la "$OUT"/ablation_*
