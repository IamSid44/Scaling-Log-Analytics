#!/usr/bin/env bash
# run_slurm.sh -- the two MapReduce jobs across the nodes of a Slurm allocation, with
# srun standing in for YARN and the shared /home for HDFS (the cluster has no working Hadoop).
#
#     bash scripts/run_slurm.sh <input.txt> <output.txt> [num_maps] [num_reduces]
#
#   stage    strip the header, split the input into P line-aligned files on /home
#   map      srun P tasks (mr_task.sh map): mr_map, partition by hash(key) mod R,
#            then sort | mr_reduce (the combiner) per partition
#   reduce   srun R tasks: sort -m partition r from every map task | mr_reduce
#   job 2    srun 1 task: sort -m all job-1 outputs | mr_report
#
# ONE_JOB=1: mr_report is job 1's only reducer and there is no job 2 (the ablation).
# Not simulated: data locality, task retries, speculative execution.
# Prints the same KEY value lines on stderr as run_hadoop.sh and run_local.sh.

set -uo pipefail
export LC_ALL=C
[ -n "${SLURM_JOB_ID:-}" ] || { echo "run_slurm.sh: run inside salloc or sbatch" >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
SRUN="${SRUN:-srun}"

INPUT="${1:-}"
OUTPUT="${2:-}"
NMAP="${3:-4}"
NRED="${4:-4}"
ONE_JOB="${ONE_JOB:-0}"
[ "$ONE_JOB" = "1" ] && NRED=1

[ -z "$INPUT" ] || [ -z "$OUTPUT" ] && {
    echo "usage: run_slurm.sh <input> <output> [num_maps] [num_reduces]" >&2; exit 2; }
[ -r "$INPUT" ] || { echo "run_slurm.sh: cannot read $INPUT" >&2; exit 2; }
for b in mr_map mr_reduce mr_report; do
    [ -x "$BIN/$b" ] || { echo "run_slurm.sh: $BIN/$b missing -- run make" >&2; exit 1; }
done

NNODES="${SLURM_JOB_NUM_NODES:-${SLURM_NNODES:-1}}"
K=$(awk 'NR==1 {print $2; exit}' "$INPUT"); [ -z "$K" ] && K=10

# One task per node while there are enough nodes, else the fewest per node that fit
# (P = 8 on 7 nodes runs 2 per node on 4 nodes, as in 01-mpi).
place() {   # place <ntasks> -> "--nodes=N --ntasks=T --ntasks-per-node=K"
    local t="$1" per=1
    while [ $(( (t + per - 1) / per )) -gt "$NNODES" ]; do per=$((per + 1)); done
    # uneven counts (5 on 3 nodes): deal round-robin (2,2,1), not (3,1,1)
    if [ $(( t % per )) -eq 0 ]; then
        echo "--nodes=$(( (t + per - 1) / per )) --ntasks=$t --ntasks-per-node=$per"
    else
        echo "--nodes=$(( (t + per - 1) / per )) --ntasks=$t --distribution=cyclic"
    fi
}
launch() {  # launch <ntasks> <mr_task.sh args...>
    local t="$1"; shift
    $SRUN $(place "$t") --kill-on-bad-exit=1 bash "$ROOT/scripts/mr_task.sh" "$@"
}

now() { date +%s.%N; }
el()  { echo "$1 $2" | awk '{printf "%.3f", $2 - $1}'; }

# ---- stage: the "hdfs dfs -put" step --------------------------------------------
# Splits are cached per (input, P) and per Slurm job, and reused by repeated runs.
JOBKEY="${SLURM_JOB_ID:-local}"
DFS_BASE="${DFS_BASE:-$ROOT/dfs/$JOBKEY}"
SPLITS="$DFS_BASE/$(basename "$INPUT" .txt)_p$NMAP"
T_START=$(now)
if [ ! -f "$SPLITS/.complete" ]; then
    # keep at most one cached split set
    rm -rf "$DFS_BASE"/*_p* 2>/dev/null
    mkdir -p "$SPLITS"
    T=$(now)
    tail -n +2 "$INPUT" > "$SPLITS/.data" &&
    split -n "l/$NMAP" -d -a 3 "$SPLITS/.data" "$SPLITS/split_" || {
        echo "run_slurm.sh: staging failed" >&2; exit 1; }
    rm -f "$SPLITS/.data"
    # make sure all N split files exist, even empty ones
    for i in $(seq 0 $((NMAP - 1))); do
        f="$SPLITS/split_$(printf '%03d' "$i")"; [ -e "$f" ] || : > "$f"
    done
    el "$T" "$(now)" > "$SPLITS/.t_stage"
    touch "$SPLITS/.complete"
fi
T_STAGE=$(cat "$SPLITS/.t_stage")

# ---- job working directory ("HDFS" output + shuffle area) -------------------
TAG="${TAG:-$(basename "$INPUT" .txt)_m${NMAP}_r${NRED}_$$}"
W="${WORK_BASE:-$ROOT/.mrwork/$JOBKEY}/$TAG"
rm -rf "$W"; mkdir -p "$W/shuffle" "$W/s1" "$W/s2" "$W/logs"
[ "${KEEP_WORK:-0}" = "1" ] || trap 'rm -rf "$W"' EXIT

# ---- JOB 1: map -> (partition, sort, combine) -> shuffle -> reduce ----------
T=$(now)
launch "$NMAP" map "$W" "$SPLITS" "$NRED" "$BIN" || {
    echo "run_slurm.sh: map phase failed" >&2; cat "$W"/logs/map_*.err >&2 2>/dev/null | tail -5; exit 1; }
T_MAP=$(el "$T" "$(now)")

if [ "$ONE_JOB" = "1" ]; then
    # ---- single job: mr_report is the only reducer; there is no job 2 -------
    T=$(now)
    launch 1 report "$W" "$BIN" "$K" shuffle || {
        echo "run_slurm.sh: reduce phase failed" >&2; exit 1; }
    T_RED=$(el "$T" "$(now)")
    T_S1=$(echo "$T_MAP $T_RED" | awk '{printf "%.3f", $1 + $2}')
    T_S2=0.000
else
T=$(now)
launch "$NRED" reduce "$W" "$NRED" "$BIN" || {
    echo "run_slurm.sh: reduce phase failed" >&2; exit 1; }
T_RED=$(el "$T" "$(now)")
T_S1=$(echo "$T_MAP $T_RED" | awk '{printf "%.3f", $1 + $2}')

# ---- JOB 2: one reducer ranks and prints the report -------------------------
T=$(now)
launch 1 report "$W" "$BIN" "$K" || { echo "run_slurm.sh: report phase failed" >&2; exit 1; }
T_S2=$(el "$T" "$(now)")
fi

# ---- fetch --------------------------------------------------------------------
T=$(now)
cp "$W/s2/part-00000" "$OUTPUT" || exit 1
T_FETCH=$(el "$T" "$(now)")
T_TOTAL=$(echo "$T_STAGE $(el "$T_START" "$(now)")" | awk '{printf "%.3f", $1 + $2}')

# ---- counters: what actually happened ----------------------------------------
sumctr() { cat "$@" 2>/dev/null | awk -F, -v k="$CTR" '$2 == k {s += $3} END {print s + 0}'; }
CTR=input_lines;       MAP_IN=$(sumctr "$W"/logs/map_*.err)
CTR=emitted_pairs;     MAP_OUT=$(sumctr "$W"/logs/map_*.err)
CTR=reduce_in_records; COMB_IN=$(sumctr "$W"/logs/combine_*.err)
CTR=reduce_out_groups; COMB_OUT=$(sumctr "$W"/logs/combine_*.err)
SHUFFLED=$(cat "$W"/shuffle/m*/part_* 2>/dev/null | wc -l | tr -d ' ')
SHUFFLE_BYTES=$(cat "$W"/shuffle/m*/part_* 2>/dev/null | wc -c | tr -d ' ')
maxkb() { cat "$@" 2>/dev/null | awk '$1 == "MAXRSS_KB" && $3 > m {m = $3} END {print m + 0}'; }
PEAK_MAP=$(( $(maxkb "$W"/logs/map_*.err) * 1024 ))
MAPS=$(ls "$W"/logs/map_*.host 2>/dev/null | wc -l | tr -d ' ')
# Distinct nodes that ran a map task, from the mappers' H: records.
if [ "$ONE_JOB" = "1" ]; then
    PEAK_RED=$(( $(maxkb "$W"/logs/report.err) * 1024 ))
    REDS=1
    NODES=$(cat "$W"/shuffle/m*/part_* | cut -f1 | grep '^H:' | sort -u | wc -l | tr -d ' ')
else
    PEAK_RED=$(( $(maxkb "$W"/logs/reduce_*.err) * 1024 ))
    REDS=$(ls "$W"/logs/reduce_*.host 2>/dev/null | wc -l | tr -d ' ')
    NODES=$(cat "$W"/s1/part-* | grep -c '^H:')
fi
BYTES=$(cat "$SPLITS"/split_* | wc -c | tr -d ' ')

{
    echo "run_slurm.sh: $NMAP map(s), $NRED reduce(s) on $NNODES node(s)"
    echo "run_slurm.sh: task placement (host, seconds):"
    for f in "$W"/logs/map_*.host;    do echo "    map    $(basename "$f" .host | cut -d_ -f2)  $(cat "$f")"; done
    for f in "$W"/logs/reduce_*.host; do [ -e "$f" ] || continue; echo "    reduce $(basename "$f" .host | cut -d_ -f2)  $(cat "$f")"; done
    echo "    report     $(cat "$W/logs/report.host")"
    echo "run_slurm.sh: phases  stage=${T_STAGE}s  map=${T_MAP}s  reduce=${T_RED}s  report=${T_S2}s"
} >&2
[ -n "${LOGDIR:-}" ] && cp -r "$W/logs" "$LOGDIR/slurm_$TAG" 2>/dev/null

cat >&2 <<EOF
MAPS_LAUNCHED $MAPS
REDUCES_LAUNCHED $REDS
NODES $NODES
MAP_INPUT_RECORDS $MAP_IN
MAP_OUTPUT_RECORDS $MAP_OUT
COMBINE_INPUT_RECORDS $COMB_IN
COMBINE_OUTPUT_RECORDS $COMB_OUT
SHUFFLED_RECORDS $SHUFFLED
CPU_MS 0
DATA_LOCAL_MAPS 0
MAP_OUT_BYTES $SHUFFLE_BYTES
SHUFFLE_BYTES $SHUFFLE_BYTES
HDFS_READ_BYTES $BYTES
FILE_WRITTEN_BYTES $SHUFFLE_BYTES
PEAK_MAP_MEM $PEAK_MAP
PEAK_RED_MEM $PEAK_RED
BYTES $BYTES
TIME_STAGE $T_STAGE
TIME_MAP $T_MAP
TIME_REDUCE $T_RED
TIME_S1 $T_S1
TIME_S2 $T_S2
TIME_FETCH $T_FETCH
TIME_JOBS $(echo "$T_S1 $T_S2" | awk '{printf "%.3f", $1 + $2}')
TIME_TOTAL $T_TOTAL
EOF
exit 0
