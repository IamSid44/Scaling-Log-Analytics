#!/usr/bin/env bash
# run_local.sh -- the two jobs as local pipes, no Hadoop and no Slurm:
#     split | mr_map | partition | sort | mr_reduce,   then   sort | mr_report
#
#     bash scripts/run_local.sh <input.txt> <output.txt> [num_mappers] [num_reducers]
#
# Same binaries and same partitioner as run_slurm.sh; used by verify.sh (MODE=local).

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"

INPUT="${1:-}"
OUTPUT="${2:-}"
NMAP="${3:-4}"
NRED="${4:-2}"

[ -z "$INPUT" ] || [ -z "$OUTPUT" ] && {
    echo "usage: run_local.sh <input.txt> <output.txt> [num_mappers] [num_reducers]" >&2
    exit 2; }
[ -r "$INPUT" ] || { echo "run_local.sh: cannot read $INPUT" >&2; exit 2; }

ext=""
[ -x "$BIN/mr_map" ] || ext=".exe"
for b in mr_map mr_reduce mr_report; do
    [ -x "$BIN/$b$ext" ] || { echo "run_local.sh: $BIN/$b$ext missing -- run make" >&2
                              exit 1; }
done

# K from the header, passed as LOG_K (Hadoop: -cmdenv LOG_K=...)
K=$(awk 'NR==1 {print $2; exit}' "$INPUT")
[ -z "$K" ] && K=10
export LOG_K="$K"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

now() { date +%s.%N; }
el()  { echo "$1 $2" | awk '{printf "%.3f", $2 - $1}'; }
T_START=$(now)

# strip the "N K S" header, as staging does
tail -n +2 "$INPUT" > "$WORK/data"

# ---- stage 1: map (in-mapper combining) -> partition -> sort -> reduce ------

split -n "l/$NMAP" -d -a 3 "$WORK/data" "$WORK/chunk_" 2>/dev/null || {
    echo "run_local.sh: split -n unavailable, using one mapper" >&2
    cp "$WORK/data" "$WORK/chunk_000"
    NMAP=1
}

T=$(now)
for c in "$WORK"/chunk_*; do
    "$BIN/mr_map$ext" < "$c" >> "$WORK/mapped" 2>>"$WORK/map.err"
done

# partitioner: hash(key) mod R, the same hash as mr_task.sh
awk -F'\t' -v R="$NRED" -v out="$WORK/part" '
    BEGIN { for (c = 0; c < 256; c++) ord[sprintf("%c", c)] = c }
    { h = 0
      for (i = 1; i <= length($1); i++) h = (h * 31 + ord[substr($1, i, 1)]) % 2147483647
      print > (out "_" (h % R)) }' "$WORK/mapped"

for r in $(seq 0 $((NRED - 1))); do
    [ -s "$WORK/part_$r" ] || continue
    sort "$WORK/part_$r" | "$BIN/mr_reduce$ext" >> "$WORK/stage1" 2>>"$WORK/red.err"
done
T_S1=$(el "$T" "$(now)")

# ---- stage 2: identity map, one reducer produces the report ----------------
T=$(now)
sort "$WORK/stage1" | "$BIN/mr_report$ext" > "$OUTPUT" 2>>"$WORK/rep.err"
T_S2=$(el "$T" "$(now)")
T_TOTAL=$(el "$T_START" "$(now)")

lines=$(wc -l < "$WORK/mapped" | tr -d ' ')
groups=$(wc -l < "$WORK/stage1" | tr -d ' ')
map_in=$(awk -F, '/input_lines/ {s += $NF} END {print s + 0}' "$WORK/map.err")
bytes=$(wc -c < "$WORK/data" | tr -d ' ')

echo "run_local.sh: $NMAP mapper(s), $NRED reducer(s)" >&2
echo "run_local.sh: shuffled $lines pair(s) -> $groups group(s) -> report" >&2

# same KEY value lines as the other drivers; Hadoop-only fields are 0
cat >&2 <<EOF
MAPS_LAUNCHED $NMAP
REDUCES_LAUNCHED $NRED
NODES 1
MAP_INPUT_RECORDS $map_in
MAP_OUTPUT_RECORDS $lines
COMBINE_INPUT_RECORDS 0
COMBINE_OUTPUT_RECORDS 0
SHUFFLED_RECORDS $lines
CPU_MS 0
BYTES $bytes
TIME_STAGE 0.000
TIME_S1 $T_S1
TIME_S2 $T_S2
TIME_FETCH 0.000
TIME_JOBS $(echo "$T_S1 $T_S2" | awk '{printf "%.3f", $1 + $2}')
TIME_TOTAL $T_TOTAL
EOF
exit 0
