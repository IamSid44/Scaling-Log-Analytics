#!/usr/bin/env bash
# run_hadoop.sh -- the two jobs as Hadoop Streaming jobs on HDFS + YARN (laptop).
#
#     bash scripts/run_hadoop.sh <local-input> <local-output> [num_maps] [num_reduces]
#
# Needs HADOOP_HOME and a running HDFS + YARN (hadoop/hadoop-cluster.sh).
#   job 1:  -mapper mr_map  -combiner mr_reduce  -reducer mr_reduce   (R reducers)
#   job 2:  -mapper cat     -reducer mr_report                        (1 reducer)
#
# mapreduce.job.maps is only a hint, so the split size is set to ceil(bytes / P) to get
# exactly P map tasks. stream.reduce.output=keyonlytext stops Streaming appending a tab
# to every report line (an empty output separator does not work on 3.3.6).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"

INPUT="${1:-}"
OUTPUT="${2:-}"
NMAP="${3:-4}"
NRED="${4:-4}"

[ -z "$INPUT" ] || [ -z "$OUTPUT" ] && {
    echo "usage: run_hadoop.sh <input> <output> [num_maps] [num_reduces]" >&2
    exit 2; }
[ -r "$INPUT" ] || { echo "run_hadoop.sh: cannot read $INPUT" >&2; exit 2; }

: "${HADOOP_HOME:?run_hadoop.sh: HADOOP_HOME is not set -- module load hdfs/hdfs, or source hadoop/hadoop-cluster.sh}"
HADOOP="${HADOOP:-$HADOOP_HOME/bin/hadoop}"
HDFS="${HDFS:-$HADOOP_HOME/bin/hdfs}"

# The streaming jar moves between releases, so find it rather than hard-code it.
STREAMING_JAR="${STREAMING_JAR:-}"
if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)
fi
[ -n "$STREAMING_JAR" ] || {
    echo "run_hadoop.sh: no hadoop-streaming jar found under $HADOOP_HOME" >&2
    exit 1; }

for b in mr_map mr_reduce mr_report; do
    [ -x "$BIN/$b" ] || { echo "run_hadoop.sh: $BIN/$b missing -- run make" >&2; exit 1; }
done

K=$(awk 'NR==1 {print $2; exit}' "$INPUT")
[ -z "$K" ] && K=10

TAG="${TAG:-$(basename "$INPUT" .txt)_m${NMAP}_r${NRED}_$$}"
HDFS_BASE="${HDFS_BASE:-/user/$USER/loganalytics}"
HDFS_IN="$HDFS_BASE/in/$TAG"
HDFS_S1="$HDFS_BASE/s1/$TAG"
HDFS_S2="$HDFS_BASE/s2/$TAG"

# benchmark.sh stages each input once and passes HDFS_INPUT and STAGED_BYTES
PRESTAGED=0
[ -n "${HDFS_INPUT:-}" ] && { HDFS_IN="$HDFS_INPUT"; PRESTAGED=1; }

WORK="$(mktemp -d)"
cleanup() {
    rm -rf "$WORK"
    if [ "${KEEP_HDFS:-0}" != "1" ]; then
        "$HDFS" dfs -rm -r -f -skipTrash "$HDFS_S1" "$HDFS_S2" >/dev/null 2>&1
        [ "$PRESTAGED" = "1" ] || \
            "$HDFS" dfs -rm -r -f -skipTrash "$HDFS_IN" >/dev/null 2>&1
    fi
    return 0
}
trap cleanup EXIT

t0() { date +%s.%N; }
el() { echo "$1 $2" | awk '{printf "%.3f", $2 - $1}'; }

# ---- stage the input -------------------------------------------------------

T_START=$(t0)
T=$(t0)
"$HDFS" dfs -rm -r -f -skipTrash "$HDFS_S1" "$HDFS_S2" >/dev/null 2>&1
if [ "$PRESTAGED" = "1" ]; then
    BYTES="${STAGED_BYTES:?run_hadoop.sh: HDFS_INPUT needs STAGED_BYTES}"
else
    tail -n +2 "$INPUT" > "$WORK/data"
    BYTES=$(wc -c < "$WORK/data" | tr -d ' ')
    "$HDFS" dfs -mkdir -p "$HDFS_IN" >/dev/null 2>&1
    # STAGE_REPLICATION: see scripts/benchmark.sh. Default is the cluster's.
    "$HDFS" dfs ${STAGE_REPLICATION:+-D dfs.replication=$STAGE_REPLICATION} \
        -put -f "$WORK/data" "$HDFS_IN/data.txt" || {
        echo "run_hadoop.sh: could not stage input into HDFS" >&2; exit 1; }
fi
T_STAGE=$(el "$T" "$(t0)")

# exactly P splits: ceil(bytes / P)
SPLIT=$(( (BYTES + NMAP - 1) / NMAP ))
[ "$SPLIT" -lt 1 ] && SPLIT=1

COMMON=(
  -D "mapreduce.job.reduce.slowstart.completedmaps=1.0"
  -D "mapreduce.map.speculative=false"
  -D "mapreduce.reduce.speculative=false"
)

# TUNED=1: 100 ms client polls and AM heartbeats instead of the 1-5 s defaults
if [ "${TUNED:-0}" = "1" ]; then
    COMMON+=(-D "mapreduce.client.completion.pollinterval=100"
             -D "mapreduce.client.progressmonitor.pollinterval=100"
             -D "yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms=100")
fi
# UBER=1: run all tasks inside the ApplicationMaster's JVM (not distributed; experiment only)
if [ "${UBER:-0}" = "1" ]; then
    COMMON+=(-D "mapreduce.job.ubertask.enable=true"
             -D "mapreduce.job.ubertask.maxmaps=64"
             -D "mapreduce.job.ubertask.maxbytes=100000000000")
fi

# ---- ONE_JOB=1: mr_report as the only reducer, no job 2 (NODES not reported) ----
if [ "${ONE_JOB:-0}" = "1" ]; then
    echo "run_hadoop.sh: one job -- $NMAP map(s), 1 reduce, split=$SPLIT bytes"
    T=$(t0)
    "$HADOOP" jar "$STREAMING_JAR" \
        "${COMMON[@]}" \
        -D "mapreduce.job.name=mr-onejob-$TAG" \
        -D "mapreduce.input.fileinputformat.split.maxsize=$SPLIT" \
        -D "mapreduce.input.fileinputformat.split.minsize=$SPLIT" \
        -D "mapreduce.job.maps=$NMAP" \
        -D "mapreduce.job.reduces=1" \
        -D "stream.reduce.output=keyonlytext" \
        -files "$BIN/mr_map,$BIN/mr_reduce,$BIN/mr_report" \
        -cmdenv "LOG_K=$K" \
        -input "$HDFS_IN" -output "$HDFS_S2" \
        -mapper mr_map -combiner mr_reduce -reducer mr_report \
        > "$WORK/s1.log" 2>&1
    S1_RC=$?
    T_S1=$(el "$T" "$(t0)")
    T_S2=0.000
    if [ "$S1_RC" -ne 0 ]; then
        echo "run_hadoop.sh: job failed (exit $S1_RC)" >&2
        grep -Ei 'error|exception|failed' "$WORK/s1.log" | head -15 >&2
        exit 1
    fi
    : > "$WORK/s2.log"
fi

# ---- stage 1 ---------------------------------------------------------------

if [ "${ONE_JOB:-0}" != "1" ]; then
echo "run_hadoop.sh: stage 1 -- $NMAP map(s), $NRED reduce(s), split=$SPLIT bytes"
T=$(t0)
"$HADOOP" jar "$STREAMING_JAR" \
    "${COMMON[@]}" \
    -D "mapreduce.job.name=mr-stage1-$TAG" \
    -D "mapreduce.input.fileinputformat.split.maxsize=$SPLIT" \
    -D "mapreduce.input.fileinputformat.split.minsize=$SPLIT" \
    -D "mapreduce.job.maps=$NMAP" \
    -D "mapreduce.job.reduces=$NRED" \
    -files "$BIN/mr_map,$BIN/mr_reduce" \
    -input "$HDFS_IN" \
    -output "$HDFS_S1" \
    -mapper mr_map \
    -combiner mr_reduce \
    -reducer mr_reduce \
    > "$WORK/s1.log" 2>&1
S1_RC=$?
T_S1=$(el "$T" "$(t0)")
if [ "$S1_RC" -ne 0 ]; then
    echo "run_hadoop.sh: stage 1 failed (exit $S1_RC)" >&2
    grep -Ei 'error|exception|failed' "$WORK/s1.log" | head -15 >&2
    exit 1
fi

# ---- stage 2 ---------------------------------------------------------------

echo "run_hadoop.sh: stage 2 -- 1 reduce (global ranking)"
T=$(t0)
"$HADOOP" jar "$STREAMING_JAR" \
    "${COMMON[@]}" \
    -D "mapreduce.job.name=mr-stage2-$TAG" \
    -D "mapreduce.job.reduces=1" \
    -D "stream.reduce.output=keyonlytext" \
    -files "$BIN/mr_report" \
    -cmdenv "LOG_K=$K" \
    -input "$HDFS_S1" \
    -output "$HDFS_S2" \
    -mapper cat \
    -reducer mr_report \
    > "$WORK/s2.log" 2>&1
S2_RC=$?
T_S2=$(el "$T" "$(t0)")
if [ "$S2_RC" -ne 0 ]; then
    echo "run_hadoop.sh: stage 2 failed (exit $S2_RC)" >&2
    grep -Ei 'error|exception|failed' "$WORK/s2.log" | head -15 >&2
    exit 1
fi
fi   # end of the two-job pipeline

T=$(t0)
"$HDFS" dfs -cat "$HDFS_S2/part-*" > "$OUTPUT" 2>/dev/null
T_FETCH=$(el "$T" "$(t0)")
T_TOTAL=$(el "$T_START" "$(t0)")

# ---- counters, from the job log ---------------------------------------------

ctr() { grep -oE "$1=[0-9]+" "$2" | tail -1 | cut -d= -f2; }
MAPS=$(ctr 'Launched map tasks' "$WORK/s1.log")
REDS=$(ctr 'Launched reduce tasks' "$WORK/s1.log")
LOCAL_MAPS=$(ctr 'Data-local map tasks' "$WORK/s1.log")
SHUFFLED=$(ctr 'Reduce input records' "$WORK/s1.log")
MAP_OUT=$(ctr 'Map output records' "$WORK/s1.log")
MAP_IN=$(ctr 'Map input records' "$WORK/s1.log")
COMBINE_IN=$(ctr 'Combine input records' "$WORK/s1.log")
COMBINE_OUT=$(ctr 'Combine output records' "$WORK/s1.log")
CPU_MS=$(ctr 'CPU time spent \(ms\)' "$WORK/s1.log")
# YARN measures memory over the container's process tree (JVM + our C++ program)
MAP_OUT_BYTES=$(ctr 'Map output materialized bytes' "$WORK/s1.log")
SHUFFLE_BYTES=$(ctr 'Reduce shuffle bytes' "$WORK/s1.log")
HDFS_READ=$(ctr 'HDFS: Number of bytes read' "$WORK/s1.log")
FILE_WRITTEN=$(ctr 'FILE: Number of bytes written' "$WORK/s1.log")
PEAK_MAP_MEM=$(ctr 'Peak Map Physical memory \(bytes\)' "$WORK/s1.log")
PEAK_RED_MEM=$(ctr 'Peak Reduce Physical memory \(bytes\)' "$WORK/s1.log")

# nodes that ran map tasks: one H: key per distinct host in job 1's output
NODES=$("$HDFS" dfs -cat "$HDFS_S1/part-*" 2>/dev/null | grep -c '^H:')
[ -z "$NODES" ] && NODES=0

echo "run_hadoop.sh: map task placement (host, tasks, lines):" >&2
"$HDFS" dfs -cat "$HDFS_S1/part-*" 2>/dev/null | grep '^H:' | sed 's/^/    /' >&2

cp "$WORK/s1.log" "${LOGDIR:-$WORK}/stage1_$TAG.log" 2>/dev/null
cp "$WORK/s2.log" "${LOGDIR:-$WORK}/stage2_$TAG.log" 2>/dev/null

cat >&2 <<EOF
MAPS_LAUNCHED ${MAPS:-0}
REDUCES_LAUNCHED ${REDS:-0}
NODES ${NODES:-0}
MAP_INPUT_RECORDS ${MAP_IN:-0}
MAP_OUTPUT_RECORDS ${MAP_OUT:-0}
COMBINE_INPUT_RECORDS ${COMBINE_IN:-0}
COMBINE_OUTPUT_RECORDS ${COMBINE_OUT:-0}
SHUFFLED_RECORDS ${SHUFFLED:-0}
CPU_MS ${CPU_MS:-0}
DATA_LOCAL_MAPS ${LOCAL_MAPS:-0}
MAP_OUT_BYTES ${MAP_OUT_BYTES:-0}
SHUFFLE_BYTES ${SHUFFLE_BYTES:-0}
HDFS_READ_BYTES ${HDFS_READ:-0}
FILE_WRITTEN_BYTES ${FILE_WRITTEN:-0}
PEAK_MAP_MEM ${PEAK_MAP_MEM:-0}
PEAK_RED_MEM ${PEAK_RED_MEM:-0}
BYTES $BYTES
TIME_STAGE ${T_STAGE}
TIME_S1 ${T_S1}
TIME_S2 ${T_S2}
TIME_FETCH ${T_FETCH}
TIME_JOBS $(echo "$T_S1 $T_S2" | awk '{printf "%.3f", $1 + $2}')
TIME_TOTAL ${T_TOTAL}
EOF
exit 0
