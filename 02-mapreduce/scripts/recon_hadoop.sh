#!/usr/bin/env bash
# recon_hadoop.sh -- what does the cluster actually provide for running Hadoop?
#
# Run on a COMPUTE node inside a 2-node allocation (the second node is only
# used for the ssh test):
#
#     salloc --nodes=2 --ntasks-per-node=1 --partition=debug --time=00:30:00
#     scontrol show hostnames $SLURM_JOB_NODELIST     # note both names
#     ssh <first-node>
#     cd <repo>/02-mapreduce
#     bash scripts/recon_hadoop.sh 2>&1 | tee recon_hadoop.txt
#
# Read-only apart from a scratch directory under /tmp that it deletes again.
# Nothing here starts a long-lived daemon.

set -u
sec() { printf '\n===== %s =====\n' "$1"; }
# run: in this shell (so `module` works). trun: in a subshell with a timeout,
# for anything that can hang (hdfs/yarn clients, ssh, network).
run()  { echo "\$ $*"; eval "$*" 2>&1 | head -"${H:-40}"; echo; }
trun() { echo "\$ $*"; timeout "${T:-60}" bash -c "$*" 2>&1 | head -"${H:-40}"; echo; }

sec "0. where am I"
echo "host=$(hostname)  user=$USER  job=${SLURM_JOB_ID:-NONE}  nodes=${SLURM_JOB_NODELIST:-NONE}"
if [ -z "${SLURM_JOB_ID:-}" ]; then
    echo "!! Not inside a SLURM allocation. Do not run this on the login node."
fi
case "$(hostname)" in *login*|*head*) echo "!! This looks like the login node. Stop and ssh to a compute node.";; esac

sec "1. which modules exist (hadoop / java / mpi / python)"
H=80 run "module avail 2>&1 | grep -iE 'hadoop|hdfs|yarn|java|jdk|spark|hpcx|ompi|mpi|python'"

sec "2. what hdfs/hdfs actually sets"
run "module show hdfs/hdfs"

module load java/11.0.13 2>/dev/null
module load hdfs/hdfs    2>/dev/null

sec "3. binaries and versions after 'module load java/11.0.13 hdfs/hdfs'"
run "echo HADOOP_HOME=\${HADOOP_HOME:-unset}; echo HADOOP_CONF_DIR=\${HADOOP_CONF_DIR:-unset}; echo JAVA_HOME=\${JAVA_HOME:-unset}"
run "which java hadoop hdfs yarn mapred"
run "java -version"
trun "hadoop version"
JAR=""
if [ -n "${HADOOP_HOME:-}" ]; then
    JAR=$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)
fi
echo "streaming jar: ${JAR:-NOT FOUND}"
[ -d "$HOME/hadoop-3.3.6" ] && echo "note: ~/hadoop-3.3.6 exists"

sec "4. the module's config -- does it point at a shared cluster?"
CONF="${HADOOP_CONF_DIR:-${HADOOP_HOME:-/nonexistent}/etc/hadoop}"
run "ls $CONF"
for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
    run "grep -A1 '<name>' $CONF/$f | grep -vE '^--' | paste - - | sed -E 's/<[^>]+>//g; s/[[:space:]]+/ /g'"
done

sec "5. is a shared HDFS / YARN running right now?"
T=25 trun "hdfs dfs -ls /"
T=25 trun "hdfs dfsadmin -report | head -20"
T=25 trun "yarn node -list -all"
trun "jps"

sec "6. local-mode Hadoop Streaming smoke test (no daemons needed)"
# This is the key test. It needs only Java + the Hadoop jars + the streaming
# jar. If it prints 'LOCAL STREAMING OK', Hadoop itself works on this node and
# anything that fails later is cluster/daemon configuration, not the install.
if [ -n "$JAR" ] && command -v hadoop >/dev/null; then
    W="/tmp/$USER-recon-$$"; rm -rf "$W"; mkdir -p "$W/in"
    printf 'b\na\nb\nc\nb\n' > "$W/in/x.txt"
    T=180 H=15 trun "hadoop jar '$JAR' -D mapreduce.framework.name=local -fs file:/// \
        -input '$W/in' -output '$W/out' -mapper cat -reducer 'uniq -c' 2>&1 | tail -15"
    echo "--- output ---"; cat "$W"/out/part-* 2>/dev/null
    if grep -q '3 b' "$W"/out/part-* 2>/dev/null; then
        echo "LOCAL STREAMING OK"
    else
        echo "LOCAL STREAMING FAILED"
    fi
    rm -rf "$W"
else
    echo "skipped: no hadoop command or streaming jar"
fi

sec "7. SLURM settings that decide how we can start Hadoop daemons"
run "scontrol --version"
run "scontrol show config | grep -iE 'ProctrackType|TaskPlugin|PrologFlags|JobAcctGather|SelectTypeParameters|DefMemPer|MaxMemPer'"
run "cat /etc/slurm/cgroup.conf 2>/dev/null || cat /etc/slurm-llnl/cgroup.conf 2>/dev/null"
run "srun --help 2>&1 | grep -E -- '--overlap|--exact'"

sec "8. can this node ssh to the other allocated node?"
if [ -n "${SLURM_JOB_NODELIST:-}" ]; then
    for n in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        [ "$n" = "$(hostname -s)" ] && continue
        T=20 trun "ssh -o BatchMode=yes -o StrictHostKeyChecking=no $n hostname"
    done
fi

sec "9. capacity: cores, memory, disk, quota, node states"
run "nproc; free -g"
run "df -h /tmp \$HOME"
run "quota -s 2>/dev/null || echo 'no quota command'"
H=20 run "sinfo -N -o '%N %t %c %m %P'"

sec "10. ports already in use on this node (would a Hadoop default port collide?)"
run "ss -ltn 2>/dev/null | awk 'NR>1{print \$4}' | grep -oE '[0-9]+\$' | sort -n | uniq | tr '\n' ' '"

sec "11. internet from a compute node (only matters if we must download Hadoop)"
T=15 trun "curl -sI https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz | head -1"

sec "12. for the MPI comparison: is the 01-mpi build here, and are the datasets identical?"
MPI_DIR="${MPI_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/../01-mpi}"
run "ls -d $MPI_DIR 2>/dev/null"
for d in "$MPI_DIR"; do
    [ -d "$d" ] || continue
    run "ls -la $d/bin $d/data 2>/dev/null"
    # Expected, regenerated independently: 39723712 bytes,
    # md5 0d91114e29e92bdbc58b1ffce2df9218
    [ -f "$d/data/logs_1000000.txt" ] && run "md5sum $d/data/logs_1000000.txt"
    break
done
run "ls -l /usr/bin/time && /usr/bin/time -v true 2>&1 | grep -i 'maximum resident'"
run "module load hpcx-2.7.0/hpcx-ompi 2>&1 && which mpirun"
run "which g++ make python3; g++ --version | head -1; python3 -V"

sec "done"
echo "Send back the whole of recon_hadoop.txt."
