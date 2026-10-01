#!/usr/bin/env bash
# Correctness suite: the MapReduce report must be byte-identical to the C++
# sequential reference's, for every mapper and reducer count.
#
#     bash scripts/verify.sh                  local pipes (a laptop, or inside salloc)
#     MODE=slurm  bash scripts/verify.sh      tasks through srun (sbatch slurm/verify_srun.slurm)
#     MODE=hadoop bash scripts/verify.sh      real Hadoop Streaming (laptop)
#
#   1. the hand-computed fixtures
#   2. generated datasets vs the sequential program
#   3. identical output for every mapper x reducer count
#   4. the map output does not grow with the input (in-mapper combining)

set -uo pipefail
if command -v srun > /dev/null 2>&1 && [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "error: not inside a Slurm allocation -- use salloc or sbatch" >&2; exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
PY="${PYTHON:-python3}"
MODE="${MODE:-local}"

MAPS="${MAPS:-1 2 3 5 8}"
REDUCES="${REDUCES:-1 2 4}"

ext=""
[ -x "$BIN/mr_map" ] || ext=".exe"
SEQ="$BIN/loganalytics_seq$ext"

{ [ -x "$SEQ" ] && [ -x "$BIN/mr_map$ext" ]; } || make -C "$ROOT" all || exit 1

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pass=0; fail=0
ok()  { echo "  PASS  $*"; pass=$((pass + 1)); }
bad() { echo "  FAIL  $*"; fail=$((fail + 1)); }
hr()  { echo "-------------------------------------------------------------"; }
same() { diff -q --strip-trailing-cr "$1" "$2" >/dev/null 2>&1; }

# run_job <input> <out> <maps> <reduces> -- local harness, or Hadoop if asked.
run_job() {
    local in="$1" out="$2" m="$3" r="$4"
    rm -f "$out"
    if [ "$MODE" = "hadoop" ]; then
        bash "$ROOT/scripts/run_hadoop.sh" "$in" "$out" "$m" "$r" \
            >"$WORK/j.out" 2>"$WORK/j.err" || {
            bad "hadoop job failed (maps=$m reduces=$r) -- not a wrong answer"
            sed 's/^/        | /' "$WORK/j.err" | head -6
            return 1; }
    elif [ "$MODE" = "slurm" ]; then
        bash "$ROOT/scripts/run_slurm.sh" "$in" "$out" "$m" "$r" \
            >"$WORK/j.out" 2>"$WORK/j.err" || {
            bad "slurm job failed (maps=$m reduces=$r) -- not a wrong answer"
            sed 's/^/        | /' "$WORK/j.err" | head -6
            return 1; }
    else
        bash "$ROOT/scripts/run_local.sh" "$in" "$out" "$m" "$r" \
            >"$WORK/j.out" 2>"$WORK/j.err" || {
            bad "local job failed (maps=$m reduces=$r)"
            sed 's/^/        | /' "$WORK/j.err" | head -6
            return 1; }
    fi
    [ -s "$out" ] || { bad "job produced no output (maps=$m reduces=$r)"; return 1; }
    return 0
}

echo "mode: $MODE | maps: $MAPS | reduces: $REDUCES"

hr; echo "1. hand-computed fixtures"; hr
for exp in "$ROOT"/tests/*.expected; do
    name="$(basename "$exp" .expected)"
    in="$ROOT/tests/$name.txt"

    rm -f "$WORK/o"; "$SEQ" "$in" "$WORK/o" 2>/dev/null
    same "$WORK/o" "$exp" && ok "$name [sequential oracle]" || {
        bad "$name [sequential oracle]"; diff --strip-trailing-cr "$exp" "$WORK/o" | head -6; }

    for m in $MAPS; do
        if run_job "$in" "$WORK/o" "$m" 1; then
            same "$WORK/o" "$exp" && ok "$name [maps=$m]" || {
                bad "$name [maps=$m]"; diff --strip-trailing-cr "$exp" "$WORK/o" | head -8; }
        fi
    done
done

hr; echo "2. generated datasets vs. the sequential oracle"; hr
gen() { "$PY" "$ROOT/scripts/gen_logs.py" -o "$WORK/$1.txt" "${@:2}" >/dev/null; }
gen small   -n 5000   -k 5  --servers 8   --endpoints 40   --seed 1
gen skewed  -n 100000 -k 10 --servers 200 --endpoints 2000 --alpha 1.6 --seed 2
gen flat    -n 100000 -k 10 --servers 16  --endpoints 32   --alpha 0.0 --seed 3
gen wide    -n 50000  -k 5  --servers 32  --endpoints 64   --span 2592000 --seed 4

for name in small skewed flat wide; do
    in="$WORK/$name.txt"
    "$SEQ" "$in" "$WORK/$name.ref" 2>/dev/null || { bad "$name: oracle failed"; continue; }
    for m in $MAPS; do
        for r in $REDUCES; do
            if run_job "$in" "$WORK/o" "$m" "$r"; then
                same "$WORK/o" "$WORK/$name.ref" \
                    && ok "$name [maps=$m reduces=$r] == sequential" || {
                    bad "$name [maps=$m reduces=$r] differs"
                    diff --strip-trailing-cr "$WORK/$name.ref" "$WORK/o" | head -8; }
            fi
        done
    done
done

hr; echo "3. invariance across mapper and reducer counts"; hr
for name in skewed flat; do
    allsame=1
    for m in $MAPS; do
        for r in $REDUCES; do
            if run_job "$WORK/$name.txt" "$WORK/o" "$m" "$r"; then
                same "$WORK/o" "$WORK/$name.ref" || {
                    allsame=0; bad "$name: maps=$m reduces=$r differs from the oracle"; }
            else
                allsame=0
            fi
        done
    done
    [ "$allsame" -eq 1 ] && ok "$name: identical bytes for every mapper/reducer count"
done

hr; echo "4. in-mapper combining really does bound the shuffle"; hr
# 10x the input must give far less than 10x the map output.
gen tenth   -n 20000  -k 10 --servers 64 --endpoints 512 --span 86400 --seed 9
gen whole   -n 200000 -k 10 --servers 64 --endpoints 512 --span 86400 --seed 9

pairs_for() {
    tail -n +2 "$1" | "$BIN/mr_map$ext" 2>"$WORK/c.err" >/dev/null
    awk -F, '/emitted_pairs/ {print $NF; exit}' "$WORK/c.err"
}
p_small=$(pairs_for "$WORK/tenth.txt")
p_big=$(pairs_for "$WORK/whole.txt")

if [ -n "$p_small" ] && [ -n "$p_big" ] && [ "$p_small" -gt 0 ]; then
    ratio=$(awk -v a="$p_small" -v b="$p_big" 'BEGIN {printf "%.2f", b / a}')
    verdict=$(awk -v r="$ratio" 'BEGIN {print (r < 3.0) ? "ok" : "no"}')
    if [ "$verdict" = "ok" ]; then
        ok "10x more input -> only ${ratio}x more shuffled pairs ($p_small -> $p_big)"
    else
        bad "10x more input -> ${ratio}x more pairs ($p_small -> $p_big): in-mapper combining is not working"
    fi

    if [ "$p_big" -lt 20000 ]; then
        ok "200,000 log lines shuffled as only $p_big key/value pairs"
    else
        bad "200,000 log lines produced $p_big pairs -- far too many"
    fi
else
    bad "could not read the emitted_pairs counter from mr_map"
fi

hr; echo "RESULT: $pass passed, $fail failed"; hr
[ "$fail" -eq 0 ]
