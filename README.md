# Scaling Log Analytics - Batch, Stream & Service

One server-log analytics workload, built three ways - **batch MPI**, **MapReduce (Hadoop Streaming)** and
**real-time gRPC streaming** - plus a **gRPC food-ordering service** as the stateful-service counterpart.
All three analytics implementations produce reports that are byte-identical to a sequential C++ reference.

## Overview

The workload: read a stream of server access-log records and produce a fixed report. The input has a header
line `N K S` followed by `N` lines of

```
timestamp server_id endpoint_id user_id status_code response_time bytes_sent
```

The output has total / successful (`status < 400`) / failed request counts; average, min and max response time;
total bytes; 2xx-5xx counts; the busiest 60-second interval; and the top-`K` servers and endpoints (ties go to
the smallest id).

The project asks one question: how does the choice of distributed-systems model change the design, the
communication pattern and the performance of the *same* application?

| Part | Model | Implementation |
|---|---|---|
| [`01-mpi`](01-mpi) | batch, message passing | C++17 + MPI; every rank reads its own byte range; one reduction per aggregate |
| [`02-mapreduce`](02-mapreduce) | batch, dataflow | C++ Hadoop Streaming mapper / reducer / report reducer; run under Slurm/`srun` on a cluster and under real Hadoop 3.3.6 on a laptop |
| [`03-grpc-streaming`](03-grpc-streaming) | real-time stream | Python gRPC: producer -> coordinator -> workers, CLI dashboard, queries answered during ingestion |
| [`04-grpc-food-ordering`](04-grpc-food-ordering) | stateful service | Python gRPC food-ordering server with customer and restaurant clients and server-streaming order updates |

A [three-way comparison](results/comparison.md) at N = 10M with four processes closes the loop.

## Architecture

```
 BATCH - MPI (01)                MapReduce (02)                        STREAM - gRPC (03)

 log file                        log file                              producer.py (replays the log)
   |  split by byte range          |  split into P files                    |  stream RecordBatch
   v                               v                                        v
 rank 0 .. rank P-1              map task 0 .. P-1                      coordinator.py
 parse + aggregate               parse + aggregate (in-mapper             |  route (round-robin / hash)
   |                              combining) -> ~2,000 lines each         +-----+-----+
   |  MPI_Reduce (O(keys))         |  partition + sort + shuffle        v     v     v
   v                               v                                    worker worker worker
 rank 0                          job 1 reducers (R = 4)                 parse + fold into O(keys) state
 top-K once, print                 |                                      ^
   |                               v                                      |  GetPartial (on each query)
   v                             job 2: ONE reducer                  coordinator merges, ranks top-K once
 report                          top-K once, print                       ^
                                   |                                     |  GetAnalytics
                                   v                                dashboard.py (live CLI)
                                 report

                    all three  ==  byte-identical to the sequential C++ reference

 SERVICE - gRPC (04)
 customer.py (alice, bob) --PlaceOrder / Cancel / Subscribe--> server.py <--UpdateOrderStatus-- restaurant.py
                                                          (one lock, per-order queues)
                      <==== server-streaming OrderUpdate pushed on every status change ====
```

## Results at a glance

All numbers come from the part READMEs and their `results/` folders (`summary.md`, CSVs).

| Part | Result | Source |
|---|---|---|
| **MPI** | N = 20M: 1.692 s at P = 1 to 0.265 s at P = 7 = **6.39x** speed-up, efficiency **0.91**; peak throughput **2,862 MB/s**; time in MPI calls is **0.6%** of runtime at P = 7 | `01-mpi/results/summary.md` |
| MPI, data distribution | the naive `scatter` strategy is up to **24x slower** than per-rank `split` reads (24.12x at N = 20M, P = 7) | `01-mpi/results/summary.md` |
| **MapReduce** | up to **50M records / 1.9 GB**; parallel efficiency at P = 7 rises from **0.17** (1M) to **0.70** (50M); speed-up **4.92x** at 50M | `02-mapreduce/results/summary.md` |
| MapReduce, shuffle | in-mapper combining shrinks the shuffle to **~2,019 lines per mapper**, **0.04%** of the input at 20M | `02-mapreduce/results/summary.md`, `02-mapreduce/README.md` |
| MPI vs MapReduce | MPI is **1.8-20x** faster on its own timers (~1.7x with process launch counted on both sides) | `02-mapreduce/README.md` |
| Memory per task | Hadoop map container **342 MB** vs MPI rank **33 MB** vs C++ mapper **11 MB** | `02-mapreduce/results/summary.md` |
| Real Hadoop | **~45 s per run** on a laptop, dominated by framework overhead | `02-mapreduce/README.md` |
| **gRPC streaming** | **1.29M records/s** at W = 5 workers with 10,000 records per message (**5.00x** over one worker) | `03-grpc-streaming/results/summary.md` |
| gRPC, plateau | **~580k records/s** plateau at 1,000 records per message, explained by a fixed **~1.01 ms** per-message cost; the model predicts runs within **0.3%** | `03-grpc-streaming/results/summary.md` |
| gRPC, queries | query latency p50 grows **+17.1 ms per added worker**; **8 concurrent query clients cut ingestion by 74%** | `03-grpc-streaming/results/summary.md`, `03-grpc-streaming/README.md` |
| gRPC, routing | hash routing on Zipf-skewed data leaves the busiest worker at **180%** of its fair share (W = 5) | `03-grpc-streaming/results/summary.md` |
| gRPC, parsing | letting workers parse gives **27x** the throughput of parsing in the producer (576k vs 21k records/s at W = 5) | `03-grpc-streaming/results/summary.md` |
| **Food ordering** | **63 / 63** automated checks; **200** concurrent orders get **200** unique ids; **1 of 10** simultaneous accepts wins; streamed updates arrive **7-33 ms** after the server applies them | `04-grpc-food-ordering/README.md` |

## Design rules

The same three rules appear in every part, and they are why the three implementations agree to the byte.

1. **Integer-only aggregation.** Response times are accumulated as int64 microseconds, never floating point.
   Integer addition is associative, so the result is bit-exact at any number of ranks, mappers or workers, in any
   distribution order and batch size - and every test can demand byte equality with the sequential reference.
2. **Aggregate before communicating.** Each rank, map task or worker folds its records into an aggregate of size
   O(servers + endpoints + intervals), never O(N). What crosses the network is small and independent of the input
   size: a ~20 KB reduction per rank in MPI, ~2,000 lines per mapper in MapReduce, one partial state per query in
   gRPC.
3. **Global top-K once, after the merge.** A local top-K is not the global top-K, so workers ship per-key totals
   and the ranking and tie-breaking happen exactly once on the merged counts (rank 0 in MPI, a single reducer in
   MapReduce's second job, the coordinator in gRPC).

## Repo map

```
README.md                    this file
results/comparison.md        MPI vs MapReduce vs gRPC at N = 10M, 4 processes
01-mpi/                      C++17 + MPI log analytics     (src/, scripts/, slurm/, tests/, results/)
02-mapreduce/                Hadoop Streaming MapReduce    (src/, scripts/, slurm/, hadoop/, tests/, results/)
03-grpc-streaming/           real-time gRPC pipeline       (proto/, src/, scripts/, slurm/, tests/, results/)
04-grpc-food-ordering/       gRPC food-ordering service    (proto/, src/, scripts/, results/)
```

Each part has its own README with the design, run instructions and results. `tests/` in parts 01-03 holds the
same four hand-computed fixtures (`tiny`, `ties`, `single`, `edge_status`); `src/loganalytics_seq.cpp` is the
sequential reference every other implementation is diffed against.

## How to run

Prerequisites: a C++17 compiler and `make`; Python 3; for part 01 an MPI toolchain (Open MPI); for parts 03-04
`pip install -r requirements.txt` in the part folder (grpcio, grpcio-tools); matplotlib only to regenerate
figures. Generated datasets go to `data/` and binaries to `bin/` inside each part (both git-ignored).

**01 - MPI** (`cd 01-mpi`)

```bash
make                         # bin/loganalytics_seq, bin/loganalytics_mpi
make verify                  # correctness suite, no Slurm needed
mpirun -np 4 ./bin/loganalytics_mpi tests/tiny.txt out.txt --io split
sbatch slurm/benchmark.slurm # cluster benchmark sweep
```

**02 - MapReduce** (`cd 02-mapreduce`)

```bash
make && make verify                                   # 88-check suite using local pipes (no cluster, no Hadoop)
bash scripts/run_local.sh tests/tiny.txt out.txt 2 2  # <input> <output> <map tasks> <reducers>
sbatch slurm/benchmark_srun.slurm                     # Slurm/srun-driven MapReduce vs MPI (needs ../01-mpi built)
# real Hadoop 3.3.6 on a laptop: see the "Real Hadoop" section of 02-mapreduce/README.md
```

**03 - gRPC streaming** (`cd 03-grpc-streaming`)

```bash
pip install -r requirements.txt
bash scripts/verify.sh                                # correctness suite on local ports
# manual run: workers, coordinator, dashboard, producer - see the README
sbatch slurm/benchmark.slurm                          # full performance evaluation on a cluster
```

**04 - food ordering** (`cd 04-grpc-food-ordering`)

```bash
pip install -r requirements.txt
python3 src/server.py localhost:50051 &               # then customer.py / restaurant.py in other terminals
python3 scripts/verify.py localhost:50051             # 63 automated checks
```

**On a Slurm cluster.** The `slurm/*.slurm` scripts use `--partition=debug` and request 6-7 nodes; module names
(MPI, Python) and the partition are cluster specific, so adjust them and any `module load` lines for your site.
The MapReduce scripts locate the MPI build in `../01-mpi` (override with `MPI_DIR=/path/to/01-mpi`).

## Honest limitations

- **No fault tolerance.** None of the implementations retries, fails over or recovers: a failed MPI rank fails
  the job, the Slurm-driven MapReduce has no task retries, a dead gRPC worker or coordinator ends the run, and the
  food-ordering server keeps state only in memory.
- **Hadoop could not run on the cluster.** The shared cluster's Hadoop had no DataNodes and no YARN ResourceManager,
  so the cluster MapReduce runs use a Slurm/`srun` simulation of the same mapper and reducer binaries (with no
  JVMs, data locality or retries). Real Hadoop 3.3.6 was run only on a single laptop, where it measures framework
  start-up cost (~45 s per run), not scaling.
- **The gRPC coordinator serves one stream per start;** run again by restarting the processes.
- **The gRPC numbers are environment-specific.** Python workers, a pure-Python protobuf backend and a shared node
  mean absolute throughput and query cost differ from what a C++ or compiled-protobuf stack would give; the
  comparison with MPI and MapReduce mixes the model with the language (see `results/comparison.md`).
- **Small-timing noise.** Medians of 2-3 repetitions, no confidence intervals; runs at small N (tens of milliseconds)
  wobble by up to ~20%.

## Credits

Built with Mohd Ahmad.
