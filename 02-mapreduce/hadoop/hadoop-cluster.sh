#!/usr/bin/env bash
# hadoop-cluster.sh -- start and stop a private HDFS + YARN, on this machine or across
# the nodes of a Slurm allocation. Used for the laptop runs (see the README); not
# run on the shared cluster, where Hadoop could not be installed.
#
#     source hadoop/hadoop-cluster.sh
#     hc_start [containers_per_node] [nodemanager_count] [container_mb]
#     ... run jobs ...
#     hc_stop
#
# Each NodeManager advertises containers_per_node * container_mb (+ room for the
# ApplicationMaster), which caps how many map containers YARN can place per node.
# Under Slurm each daemon runs in the foreground in its own `srun --overlap` step:
# `--daemon start` would fork and be killed when its step ends.
# NameNode + ResourceManager on the first node; DataNode + NodeManager on each worker.
# Ports and directories are derived from the job id so two clusters cannot collide.

# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------

hc_setup_env() {
    : "${HADOOP_HOME:?hadoop-cluster.sh: HADOOP_HOME is not set (module load hdfs/hdfs, or point it at an unpacked Hadoop 3.3.6)}"

    HC_JOB="${SLURM_JOB_ID:-local$$}"
    HC_ROOT="${HC_ROOT:-${SCRATCH:-$HOME}/hadoop-run-$HC_JOB}"
    HC_CONF="$HC_ROOT/conf"
    HC_LOGS="$HC_ROOT/logs"
    # HDFS blocks on node-local disk, one directory per DataNode
    HC_DATA="${HC_DATA:-/tmp/$USER-hadoop-$HC_JOB}"

    if [ -n "${SLURM_JOB_NODELIST:-}" ]; then
        HC_NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
    else
        HC_NODES=("$(hostname)")
    fi
    HC_MASTER="${HC_NODES[0]}"

    # containers inherit the NodeManager step's CPU set, so give it all the node's CPUs
    HC_CPUS="${HC_CPUS:-${SLURM_CPUS_ON_NODE:-1}}"

    local jid="${HC_JOB##*[!0-9]}"
    local base=$(( 20000 + (${jid:-0} % 2000) * 20 ))
    HC_NN_PORT=$base;             HC_NN_HTTP=$((base + 1))
    HC_DN_PORT=$((base + 2));     HC_DN_HTTP=$((base + 3));   HC_DN_IPC=$((base + 4))
    HC_RM_PORT=$((base + 5));     HC_RM_SCHED=$((base + 6));  HC_RM_TRACK=$((base + 7))
    HC_RM_HTTP=$((base + 8));     HC_RM_ADMIN=$((base + 9))
    HC_NM_PORT=$((base + 10));    HC_NM_LOC=$((base + 11));   HC_NM_HTTP=$((base + 12))
    HC_SHUFFLE=$((base + 13))

    export HADOOP_CONF_DIR="$HC_CONF"
    export HADOOP_LOG_DIR="$HC_LOGS"
    export HADOOP_PID_DIR="$HC_ROOT/pids"
    export YARN_CONF_DIR="$HC_CONF"

    if [ -z "${JAVA_HOME:-}" ]; then
        echo "hadoop-cluster.sh: JAVA_HOME is not set -- module load java/11.0.13" >&2
    fi
}

# hc_write_conf <container_mb> <containers_per_node>
hc_write_conf() {
    local cmb="$1" cpn="$2"
    # AM container, smaller than a map container so its headroom cannot hold a map
    local am_mb=$(( cmb / 2 ))
    [ "$am_mb" -lt 512 ] && am_mb=512
    local nm_mb=$(( cmb * cpn + am_mb ))

    mkdir -p "$HC_CONF" "$HC_LOGS" "$HC_ROOT/pids"

    # start from the distribution's conf (capacity-scheduler.xml, log4j.properties)
    cp "$HADOOP_HOME"/etc/hadoop/* "$HC_CONF"/ 2>/dev/null
    {
        echo
        echo "# --- hadoop-cluster.sh ---"
        [ -n "${JAVA_HOME:-}" ] && echo "export JAVA_HOME=$JAVA_HOME"
        echo "export HADOOP_LOG_DIR=$HC_LOGS"
        echo "export HADOOP_PID_DIR=$HC_ROOT/pids"
    } >> "$HC_CONF/hadoop-env.sh"

    cat > "$HC_CONF/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://$HC_MASTER:$HC_NN_PORT</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HC_DATA/tmp</value></property>
</configuration>
EOF

    cat > "$HC_CONF/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.namenode.name.dir</name><value>file://$HC_DATA/nn</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$HC_DATA/dn</value></property>
  <property><name>dfs.namenode.http-address</name><value>0.0.0.0:$HC_NN_HTTP</value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:$HC_DN_PORT</value></property>
  <property><name>dfs.datanode.http.address</name><value>0.0.0.0:$HC_DN_HTTP</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:$HC_DN_IPC</value></property>
  <!-- Default replication 1: this cluster lives for the length of one job.
       The benchmark input is staged with a higher per-file replication so
       every node has a local copy; see scripts/benchmark.sh. -->
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <!-- Small blocks so a modest input still splits across many nodes; the real
       split count is forced separately by split.maxsize. -->
  <property><name>dfs.blocksize</name><value>33554432</value></property>
</configuration>
EOF

    cat > "$HC_CONF/yarn-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>$HC_MASTER</value></property>
  <property><name>yarn.resourcemanager.address</name><value>$HC_MASTER:$HC_RM_PORT</value></property>
  <property><name>yarn.resourcemanager.scheduler.address</name><value>$HC_MASTER:$HC_RM_SCHED</value></property>
  <property><name>yarn.resourcemanager.resource-tracker.address</name><value>$HC_MASTER:$HC_RM_TRACK</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>$HC_MASTER:$HC_RM_HTTP</value></property>
  <property><name>yarn.resourcemanager.admin.address</name><value>$HC_MASTER:$HC_RM_ADMIN</value></property>
  <property><name>yarn.nodemanager.address</name><value>0.0.0.0:$HC_NM_PORT</value></property>
  <property><name>yarn.nodemanager.localizer.address</name><value>0.0.0.0:$HC_NM_LOC</value></property>
  <property><name>yarn.nodemanager.webapp.address</name><value>0.0.0.0:$HC_NM_HTTP</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <!-- How often each NodeManager reports to the RM (and so how soon a freed
       or requested container is noticed). Hadoop's default is 1000 ms;
       HC_NM_HEARTBEAT_MS lowers it for the tuned experiment. -->
  <property><name>yarn.resourcemanager.nodemanagers.heartbeat-interval-ms</name><value>${HC_NM_HEARTBEAT_MS:-1000}</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>$HC_DATA/nm-local</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>$HC_DATA/nm-log</value></property>

  <!-- THE PLACEMENT CONTROL. A NodeManager advertises
       containers_per_node x container_mb + am_mb, so no more than
       containers_per_node map containers can ever fit on one machine, and the
       ApplicationMaster still fits beside them. See the header. -->
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>$nm_mb</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>$nm_mb</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>$(( cpn + 1 ))</value></property>

  <!-- SLURM already limits what this job may use, and the NodeManager's own
       accounting double-counts the JVM's virtual address space, which kills
       perfectly healthy containers. -->
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
  <!-- A NodeManager whose disk is more than 90% full (the default) marks
       itself UNHEALTHY and is never given a container. /tmp on a shared
       compute node can easily be that full for reasons that are not ours. -->
  <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>99.5</value></property>
</configuration>
EOF

    cat > "$HC_CONF/mapred-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.shuffle.port</name><value>$HC_SHUFFLE</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>$cmb</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>$cmb</value></property>
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>$am_mb</value></property>
  <property><name>mapreduce.map.java.opts</name><value>-Xmx$(( cmb * 8 / 10 ))m</value></property>
  <property><name>mapreduce.reduce.java.opts</name><value>-Xmx$(( cmb * 8 / 10 ))m</value></property>
  <property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx$(( am_mb * 8 / 10 ))m</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
</configuration>
EOF

    echo "$HC_MASTER" > "$HC_CONF/masters"
    printf '%s\n' "${HC_WORKERS[@]}" > "$HC_CONF/workers"
}

# hc_on <node> <command...>   -- run a SHORT command on exactly that machine
hc_on() {
    local node="$1"; shift
    if [ -n "${SLURM_JOB_ID:-}" ]; then
        srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap "$@"
    else
        "$@"
    fi
}

# hc_daemon <node> <name> <command...>  -- start a FOREGROUND daemon on that
# machine and leave it running in the background (see the header for why).
hc_daemon() {
    local node="$1" name="$2"; shift 2
    local log="$HC_LOGS/$name-$node.log"
    if [ -n "${SLURM_JOB_ID:-}" ]; then
        srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap \
             --cpus-per-task="$HC_CPUS" "$@" > "$log" 2>&1 &
    else
        "$@" > "$log" 2>&1 &
    fi
    HC_PIDS+=("$!")
    HC_PID_NAMES+=("$name@$node")
}

# Fails, with the log of the first daemon that died, if any has exited.
hc_check_alive() {
    local i
    for i in "${!HC_PIDS[@]}"; do
        if ! kill -0 "${HC_PIDS[$i]}" 2>/dev/null; then
            local nm="${HC_PID_NAMES[$i]}"
            echo
            echo "hadoop-cluster: ${nm} exited during startup. Last lines of its log:"
            tail -25 "$HC_LOGS/${nm%@*}-${nm#*@}.log" 2>/dev/null | sed 's/^/    | /'
            return 1
        fi
    done
    return 0
}

# hc_start [containers_per_node] [nodemanager_count] [container_mb]
hc_start() {
    local cpn="${1:-1}" nm_count="${2:-0}" cmb="${3:-2048}"
    hc_setup_env

    local total=${#HC_NODES[@]}
    [ "$nm_count" -le 0 ] && nm_count="$total"
    [ "$nm_count" -gt "$total" ] && nm_count="$total"
    HC_WORKERS=("${HC_NODES[@]:0:$nm_count}")
    HC_NUM_WORKERS="$nm_count"
    export HC_NUM_WORKERS
    HC_PIDS=(); HC_PID_NAMES=(); HC_STOPPED=0

    echo "hadoop-cluster: master=$HC_MASTER  nodemanagers=${HC_WORKERS[*]}"
    echo "hadoop-cluster: $cpn container(s)/node x ${cmb}MB + AM headroom, ${HC_CPUS} CPU(s)/node  (HDFS :$HC_NN_PORT, RM :$HC_RM_PORT)"
    echo "hadoop-cluster: config $HC_CONF   logs $HC_LOGS"

    hc_write_conf "$cmb" "$cpn"

    # a stale DataNode directory has an old namespace ID and would not register
    for n in "${HC_NODES[@]}"; do
        hc_on "$n" rm -rf "$HC_DATA" >/dev/null 2>&1
        hc_on "$n" mkdir -p "$HC_DATA" >/dev/null 2>&1
    done

    hc_on "$HC_MASTER" "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive \
        > "$HC_LOGS/format.log" 2>&1 || {
        echo "hadoop-cluster: namenode format failed"; tail -20 "$HC_LOGS/format.log"; return 1; }

    hc_daemon "$HC_MASTER" namenode "$HADOOP_HOME/bin/hdfs" namenode
    for n in "${HC_WORKERS[@]}"; do
        hc_daemon "$n" datanode "$HADOOP_HOME/bin/hdfs" datanode
    done
    hc_daemon "$HC_MASTER" resourcemanager "$HADOOP_HOME/bin/yarn" resourcemanager
    for n in "${HC_WORKERS[@]}"; do
        hc_daemon "$n" nodemanager "$HADOOP_HOME/bin/yarn" nodemanager
    done

    # wait for the DataNodes to register before any job is submitted
    echo -n "hadoop-cluster: waiting for $nm_count datanode(s) "
    local live=0 i
    for i in $(seq 1 90); do
        hc_check_alive || return 1
        live=$("$HADOOP_HOME/bin/hdfs" dfsadmin -report -live 2>/dev/null \
               | grep -c '^Name:' )
        [ "${live:-0}" -ge "$nm_count" ] && break
        echo -n "."
        sleep 2
    done
    echo
    if [ "${live:-0}" -lt "$nm_count" ]; then
        echo "hadoop-cluster: only $live of $nm_count datanode(s) came up"
        "$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>&1 | head -20
        tail -15 "$HC_LOGS/namenode-$HC_MASTER.log" 2>/dev/null | sed 's/^/    | /'
        return 1
    fi
    timeout 120 "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait >/dev/null 2>&1

    echo -n "hadoop-cluster: waiting for $nm_count nodemanager(s) "
    local nms=0
    for i in $(seq 1 90); do
        hc_check_alive || return 1
        nms=$("$HADOOP_HOME/bin/yarn" node -list -states RUNNING 2>/dev/null \
              | grep -cE 'RUNNING')
        [ "${nms:-0}" -ge "$nm_count" ] && break
        echo -n "."
        sleep 2
    done
    echo
    if [ "${nms:-0}" -lt "$nm_count" ]; then
        echo "hadoop-cluster: only ${nms:-0} of $nm_count nodemanager(s) came up"
        "$HADOOP_HOME/bin/yarn" node -list -all 2>&1 | head -20
        tail -15 "$HC_LOGS/resourcemanager-$HC_MASTER.log" 2>/dev/null | sed 's/^/    | /'
        return 1
    fi
    echo "hadoop-cluster: $live datanode(s), $nms nodemanager(s) up"
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER" >/dev/null 2>&1
    return 0
}

hc_stop() {
    [ -z "${HC_ROOT:-}" ] && return 0
    [ "${HC_STOPPED:-0}" = "1" ] && return 0
    echo "hadoop-cluster: stopping"
    # SIGTERM each daemon (srun forwards it); kill anything left after 30 s
    local p i
    for p in "${HC_PIDS[@]:-}"; do [ -n "$p" ] && kill -TERM "$p" 2>/dev/null; done
    for i in $(seq 1 30); do
        local alive=0
        for p in "${HC_PIDS[@]:-}"; do [ -n "$p" ] && kill -0 "$p" 2>/dev/null && alive=1; done
        [ "$alive" -eq 0 ] && break
        sleep 1
    done
    for p in "${HC_PIDS[@]:-}"; do [ -n "$p" ] && kill -KILL "$p" 2>/dev/null; done
    wait 2>/dev/null
    HC_PIDS=(); HC_PID_NAMES=(); HC_STOPPED=1
    for n in "${HC_NODES[@]:-}"; do
        [ -n "$n" ] || continue
        hc_on "$n" rm -rf "$HC_DATA" >/dev/null 2>&1
    done
    return 0
}

# print the cluster's own view of its layout
hc_report() {
    echo "--- hdfs ---"
    "$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null | head -12
    echo "--- yarn ---"
    "$HADOOP_HOME/bin/yarn" node -list -all 2>/dev/null | head -12
}
