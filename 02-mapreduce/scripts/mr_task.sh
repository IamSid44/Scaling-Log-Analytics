#!/usr/bin/env bash
# mr_task.sh -- one map, reduce or report task, launched by run_slurm.sh through srun.
# SLURM_PROCID is the task index. Each task logs its host, time and counters to <workdir>/logs.
#
#     mr_task.sh map    <workdir> <splitdir> <num_reduces> <bindir>
#     mr_task.sh reduce <workdir> <num_reduces> <bindir>
#     mr_task.sh report <workdir> <bindir> <K> [s1|shuffle]    (shuffle: ONE_JOB=1)
#
# LC_ALL=C so sort orders keys by bytes, as Hadoop's shuffle does.

set -uo pipefail
export LC_ALL=C

role="${1:?role}"
W="${2:?workdir}"
ID="${SLURM_PROCID:?mr_task.sh must be launched by srun}"
HOST="$(hostname -s 2>/dev/null || hostname)"
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PYTHON:-python3}"
now() { date +%s.%N; }

# node-local scratch for the map output before it is partitioned
TMP="${TMPDIR:-/tmp}/$USER-mrtask-${SLURM_JOB_ID:-0}-$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

case "$role" in

map)
    SPLITS="${3:?splitdir}"; R="${4:?num_reduces}"; BIN="${5:?bindir}"
    i=$(printf '%03d' "$ID")
    out="$W/shuffle/m$i"
    mkdir -p "$out"
    t0=$(now)

    # 1. map (maxrss.py records peak memory)
    "$PY" "$SCRIPTS/maxrss.py" "$BIN/mr_map" < "$SPLITS/split_$i" \
        > "$TMP/map.out" 2> "$W/logs/map_$i.err" || exit 1

    # 2. partition: hash(key) mod R
    awk -F'\t' -v R="$R" -v dir="$TMP" '
        BEGIN { for (c = 0; c < 256; c++) ord[sprintf("%c", c)] = c }
        { h = 0
          for (k = 1; k <= length($1); k++) h = (h * 31 + ord[substr($1, k, 1)]) % 2147483647
          print > (dir "/p_" (h % R)) }' "$TMP/map.out"

    # 3. sort | combiner per partition, published on /home for the reducers
    for r in $(seq 0 $((R - 1))); do
        if [ -s "$TMP/p_$r" ]; then
            sort "$TMP/p_$r" | "$BIN/mr_reduce" > "$out/part_$r.tmp" \
                2>> "$W/logs/combine_$i.err" || exit 1
        else
            : > "$out/part_$r.tmp"
        fi
        mv "$out/part_$r.tmp" "$out/part_$r"
    done
    echo "$HOST $(echo "$t0 $(now)" | awk '{printf "%.3f", $2 - $1}')" > "$W/logs/map_$i.host"
    ;;

reduce)
    R="${3:?num_reduces}"; BIN="${4:?bindir}"
    part=$(printf '%05d' "$ID")
    t0=$(now)
    # merge this reducer's partition from every map task (each is already sorted)
    files=( "$W"/shuffle/m*/part_"$ID" )
    if [ -e "${files[0]}" ]; then
        sort -m "${files[@]}" \
            | "$PY" "$SCRIPTS/maxrss.py" "$BIN/mr_reduce" \
            > "$W/s1/part-$part.tmp" 2> "$W/logs/reduce_$part.err" || exit 1
    else
        : > "$W/s1/part-$part.tmp"
    fi
    mv "$W/s1/part-$part.tmp" "$W/s1/part-$part"
    echo "$HOST $(echo "$t0 $(now)" | awk '{printf "%.3f", $2 - $1}')" > "$W/logs/reduce_$part.host"
    ;;

report)
    BIN="${3:?bindir}"; K="${4:?K}"; SRC="${5:-s1}"
    t0=$(now)
    # job 2 (its identity map is skipped): one reducer over every job-1 output
    if [ "$SRC" = "shuffle" ]; then files=( "$W"/shuffle/m*/part_0 )
    else                            files=( "$W"/s1/part-* ); fi
    sort -m "${files[@]}" \
        | LOG_K="$K" "$PY" "$SCRIPTS/maxrss.py" "$BIN/mr_report" \
        > "$W/s2/part-00000.tmp" 2> "$W/logs/report.err" || exit 1
    mv "$W/s2/part-00000.tmp" "$W/s2/part-00000"
    echo "$HOST $(echo "$t0 $(now)" | awk '{printf "%.3f", $2 - $1}')" > "$W/logs/report.host"
    ;;

*)
    echo "mr_task.sh: unknown role '$role'" >&2; exit 2 ;;
esac
