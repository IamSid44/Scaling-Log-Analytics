#!/usr/bin/env bash
# MapReduce benchmark sweep: SIZES x MAPS x REDUCES, REPS repetitions after one warm-up,
# every run diffed against the sequential program (column `correct`).
#
#     RUNNER=scripts/run_slurm.sh bash scripts/benchmark.sh    (from slurm/benchmark_srun.slurm)
#     bash scripts/benchmark.sh                                (real Hadoop, laptop; the default runner)
#
# Writes $OUT_CSV (default results/benchmark.csv). Times: t_stage = copying the input into
# the DFS (once per size), t_s1/t_s2 = the two jobs, t_jobs = t_s1 + t_s2, t_total = all.

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
PY="${PYTHON:-python3}"

SIZES="${SIZES:-1000000 5000000 10000000 20000000}"
MAPS="${MAPS:-1 2 3 4 5 6 7 8}"
REDUCES="${REDUCES:-4}"
REPS="${REPS:-3}"
SEED="${SEED:-42}"
RUNNER="${RUNNER:-$ROOT/scripts/run_hadoop.sh}"

ext=""
[ -x "$BIN/mr_map" ] || ext=".exe"
SEQ="$BIN/loganalytics_seq$ext"
[ -x "$SEQ" ] || make -C "$ROOT" all || exit 1

DATA="$ROOT/data"; RESULTS="$ROOT/results"; CSV="${OUT_CSV:-$RESULTS/benchmark.csv}"
mkdir -p "$DATA" "$RESULTS"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "n,bytes,maps_requested,maps_launched,reduces,nodes,rep,t_stage,t_s1,t_s2,t_fetch,t_jobs,t_total,map_in,map_out,combine_in,combine_out,shuffled,cpu_ms,data_local_maps,map_out_bytes,shuffle_bytes,hdfs_read_bytes,file_written_bytes,peak_map_mem,peak_red_mem,t_map,t_reduce,t_seq,correct" > "$CSV"

f() { awk -v k="$1" '$1 == k {print $2; exit}' "$WORK/err"; }

# Real Hadoop: each dataset is staged into HDFS once per size, not once per run.
# STAGE_REPLICATION copies it to every DataNode so map tasks can read locally.
USE_HDFS=0
case "$RUNNER" in *run_hadoop.sh) USE_HDFS=1 ;; esac
STAGE_REPLICATION="${STAGE_REPLICATION:-${HC_NUM_WORKERS:-1}}"
HDFS_BIN="${HADOOP_HOME:-}/bin/hdfs"
HDFS_BASE="${HDFS_BASE:-/user/$USER/loganalytics}"

stage_input() {   # stage_input <local file> <n>  -> sets HDFS_INPUT, STAGED_BYTES, t_stage
    tail -n +2 "$1" > "$WORK/data"
    STAGED_BYTES=$(wc -c < "$WORK/data" | tr -d ' ')
    HDFS_INPUT="$HDFS_BASE/staged/n$2"
    local t=$(date +%s.%N)
    "$HDFS_BIN" dfs -rm -r -f -skipTrash "$HDFS_INPUT" >/dev/null 2>&1
    "$HDFS_BIN" dfs -mkdir -p "$HDFS_INPUT" || return 1
    "$HDFS_BIN" dfs -D dfs.replication="$STAGE_REPLICATION" \
        -put -f "$WORK/data" "$HDFS_INPUT/data.txt" || return 1
    t_stage=$(echo "$t $(date +%s.%N)" | awk '{printf "%.3f", $2 - $1}')
    rm -f "$WORK/data"
    export HDFS_INPUT STAGED_BYTES
}

echo "sizes: $SIZES | maps: $MAPS | reduces: $REDUCES | reps: $REPS"
echo "runner: $RUNNER"
echo

missing=0

for n in $SIZES; do
    input="$DATA/logs_${n}.txt"
    if [ ! -s "$input" ]; then
        echo "Generating $input ..."
        "$PY" "$ROOT/scripts/gen_logs.py" -n "$n" -o "$input" -k 10 \
              --servers 64 --endpoints 512 --seed "$SEED" || exit 1
    fi
    bytes=$(wc -c < "$input" | tr -d ' ')

    # reference answer and single-core time
    ref="$WORK/ref_${n}.txt"
    "$SEQ" "$input" "$ref" >/dev/null 2>&1
    "$SEQ" "$input" /dev/null 2>"$WORK/err" >/dev/null
    t_seq=$(f TIME_SOLVE)
    printf 'N=%-9s (%s MB)  sequential C++: %ss\n' \
        "$n" "$((bytes / 1048576))" "$t_seq"

    t_stage=""
    if [ "$USE_HDFS" = "1" ]; then
        stage_input "$input" "$n" || { echo "ERROR: could not stage $input into HDFS"; exit 1; }
        echo "  staged into HDFS in ${t_stage}s (replication $STAGE_REPLICATION)"
    fi

    for r in $REDUCES; do
      for m in $MAPS; do

        # untimed warm-up
        bash "$RUNNER" "$input" "$WORK/warm.txt" "$m" "$r" >/dev/null 2>&1

        got=0
        for rep in $(seq 1 "$REPS"); do
            rm -f "$WORK/out.txt"
            if ! bash "$RUNNER" "$input" "$WORK/out.txt" "$m" "$r" \
                    >"$WORK/out" 2>"$WORK/err"; then
                echo "  RUN FAILED: N=$n maps=$m reduces=$r rep=$rep"
                grep -vE '^(MAPS|REDUCES|NODES|MAP_|COMBINE_|SHUFFLED|CPU|BYTES|TIME_)' \
                    "$WORK/err" | head -5
                continue
            fi

            correct=0
            diff -q --strip-trailing-cr "$WORK/out.txt" "$ref" >/dev/null 2>&1 && correct=1
            [ "$correct" -eq 0 ] && echo "  WRONG ANSWER: N=$n maps=$m reduces=$r"

            printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
                "$n" "$bytes" "$m" "$(f MAPS_LAUNCHED)" "$r" "$(f NODES)" "$rep" \
                "${t_stage:-$(f TIME_STAGE)}" "$(f TIME_S1)" "$(f TIME_S2)" "$(f TIME_FETCH)" \
                "$(f TIME_JOBS)" "$(f TIME_TOTAL)" \
                "$(f MAP_INPUT_RECORDS)" "$(f MAP_OUTPUT_RECORDS)" \
                "$(f COMBINE_INPUT_RECORDS)" "$(f COMBINE_OUTPUT_RECORDS)" \
                "$(f SHUFFLED_RECORDS)" "$(f CPU_MS)" \
                "$(f DATA_LOCAL_MAPS)" "$(f MAP_OUT_BYTES)" "$(f SHUFFLE_BYTES)" \
                "$(f HDFS_READ_BYTES)" "$(f FILE_WRITTEN_BYTES)" \
                "$(f PEAK_MAP_MEM)" "$(f PEAK_RED_MEM)" "$(f TIME_MAP)" "$(f TIME_REDUCE)" \
                "$t_seq" "$correct" >> "$CSV"
            got=$((got + 1))

            [ "$rep" = 1 ] && printf 'N=%-9s maps=%-2s(%s launched on %s node(s)) reduces=%-2s : jobs=%ss total=%ss shuffled=%s correct=%s\n' \
                "$n" "$m" "$(f MAPS_LAUNCHED)" "$(f NODES)" "$r" \
                "$(f TIME_JOBS)" "$(f TIME_TOTAL)" "$(f SHUFFLED_RECORDS)" "$correct"
        done
        [ "$got" -eq 0 ] && { echo "ERROR: no samples for N=$n maps=$m reduces=$r"; missing=1; }
      done
    done
    if [ "$USE_HDFS" = "1" ]; then
        "$HDFS_BIN" dfs -rm -r -f -skipTrash "$HDFS_INPUT" >/dev/null 2>&1
        unset HDFS_INPUT STAGED_BYTES
    fi
    echo
done

echo "Wrote $CSV"
exit $missing
