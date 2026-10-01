#!/usr/bin/env bash
# Benchmark sweep: dataset size x process count x I/O mode, repeated.
#
# Appends to results/benchmark.csv:
#   n,bytes,p,nodes,rep,io_mode,t_solve,t_comp,t_comm,t_read,t_imbalance,t_io,t_total
#
# PLACEMENT. Ranks go one-per-node (MAP_DEFAULT) up to MAP_PACK_ABOVE
# processes; past that the allocation has run out of machines, so the job
# script supplies MAP_PACK saying how to pack the extra ranks. The `nodes`
# column is what the program actually observed via MPI_Get_processor_name, so
# no table has to assume a placement -- a P=8 row packed 2-per-node onto 4
# machines is visibly different from a one-per-node row.
#
# t_solve is the speed-up metric: read + parse + aggregate + reduce. The I/O
# strategy is one of the things being compared, so excluding the read would
# hide the effect being measured.
#
#   bash scripts/benchmark.sh
#   SIZES="1000000" PROCS="1 2 4" REPS=1 bash scripts/benchmark.sh

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEQ="$ROOT/bin/loganalytics_seq"
MPI="$ROOT/bin/loganalytics_mpi"
PY="${PYTHON:-python3}"

SIZES="${SIZES:-1000000 5000000 10000000 20000000}"
PROCS="${PROCS:-1 2 3 4 5 6 7 8}"
MODES="${MODES:-split scatter}"
REPS="${REPS:-3}"
SEED="${SEED:-42}"

# See verify.sh for why mpirun and each of these flags, and why --map-by is
# deliberately not in here.
FLAGS="${MPI_FLAGS---bind-to none -x OMPI_MCA_coll_hcoll_enable=0}"
MAP_DEFAULT="${MAP_DEFAULT---map-by node}"
MAP_PACK="${MAP_PACK-}"
MAP_PACK_ABOVE="${MAP_PACK_ABOVE-999}"

map_for() {   # map_for <P> -> the --map-by spec for this process count
    if [ -n "$MAP_PACK" ] && [ "$1" -gt "$MAP_PACK_ABOVE" ]; then
        echo "$MAP_PACK"
    else
        echo "$MAP_DEFAULT"
    fi
}
run_mpi() { local p="$1"; shift; mpirun -np "$p" $(map_for "$p") $FLAGS "$@"; }

{ [ -x "$SEQ" ] && [ -x "$MPI" ]; } || make -C "$ROOT" all || exit 1

DATA="$ROOT/data"; RESULTS="$ROOT/results"; CSV="$RESULTS/benchmark.csv"
mkdir -p "$DATA" "$RESULTS"
ERR="$(mktemp)"; trap 'rm -f "$ERR"' EXIT

echo "n,bytes,p,nodes,rep,io_mode,t_solve,t_comp,t_comm,t_read,t_imbalance,t_io,t_total" > "$CSV"

# field <KEY> -- pulls one line out of the program's stderr report
field() { awk -v k="$1" '$1 == k { print $2; exit }' "$ERR"; }
record() {  # record <n> <bytes> <p> <rep> <io_mode>
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$1" "$2" "$3" \
        "$(field NODES)" "$4" "$5" \
        "$(field TIME_SOLVE)" "$(field TIME_COMP)" "$(field TIME_COMM)" \
        "$(field TIME_READ)"  "$(field TIME_IMBALANCE)" "$(field TIME_IO)" \
        "$(field TIME_TOTAL)" >> "$CSV"
}

echo "sizes: $SIZES | procs: $PROCS | modes: $MODES | reps: $REPS"
echo

for n in $SIZES; do
    input="$DATA/logs_${n}.txt"
    [ -s "$input" ] || { echo "Generating $input ..."
        "$PY" "$ROOT/scripts/gen_logs.py" -n "$n" -o "$input" -k 10 \
              --servers 64 --endpoints 512 --seed "$SEED"; }
    bytes=$(wc -c < "$input" | tr -d ' ')

    # One untimed warm-up per dataset, so the first timed repetition is not
    # paying for the cold first touch of the file on this node.
    "$SEQ" "$input" /dev/null >/dev/null 2>&1

    for rep in $(seq 1 "$REPS"); do
        "$SEQ" "$input" /dev/null 2>"$ERR" >/dev/null
        record "$n" "$bytes" 1 "$rep" sequential
        [ "$rep" = 1 ] && printf 'N=%-9s (%s MB)  sequential : %ss\n' \
            "$n" "$((bytes / 1048576))" "$(field TIME_SOLVE)"
    done

    for mode in $MODES; do for p in $PROCS; do
        # Warm-up at this placement too: the ranks that just joined have not
        # touched the file yet, and each node has its own page cache.
        run_mpi "$p" "$MPI" "$input" /dev/null --io "$mode" >/dev/null 2>&1
        for rep in $(seq 1 "$REPS"); do
            if ! run_mpi "$p" "$MPI" "$input" /dev/null --io "$mode" 2>"$ERR" >/dev/null; then
                echo "  RUN FAILED: N=$n $mode P=$p rep=$rep"; head -5 "$ERR"; continue
            fi
            record "$n" "$bytes" "$p" "$rep" "$mode"
            [ "$rep" = 1 ] && printf 'N=%-9s %-8s P=%-2s on %s node(s) : solve=%s read=%s comm=%s imbal=%s\n' \
                "$n" "$mode" "$p" "$(field NODES)" "$(field TIME_SOLVE)" \
                "$(field TIME_READ)" "$(field TIME_COMM)" "$(field TIME_IMBALANCE)"
        done
    done; done
    echo
done

# A half-empty CSV must be loud, not silent.
missing=0
for n in $SIZES; do for mode in $MODES; do for p in $PROCS; do
    c=$(awk -F, -v n="$n" -v p="$p" -v m="$mode" '$1==n && $3==p && $6==m' "$CSV" | wc -l)
    [ "$c" -eq 0 ] && { echo "ERROR: no samples for N=$n $mode P=$p"; missing=1; }
done; done; done

echo "Wrote $CSV"
exit $missing
