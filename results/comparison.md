# MPI vs MapReduce vs gRPC streaming on one workload

The same server-log analytics workload, built three ways, run on the same university Slurm cluster and
checked against the same sequential C++ reference. Every implementation produced a byte-identical report on
the same datasets.

**Setup.** N = 10M records (379 MB), four processes: MPI with 4 ranks (one per node), MapReduce with 4 map
tasks (4 reducers in job 1, one in job 2), and gRPC streaming with 4 workers on 4 nodes. MPI and MapReduce were
measured in the same Slurm job on the same nodes and files; the gRPC run is the input-size experiment at
W = 4 with the default settings (1,000 records per message, round-robin routing, no query load).

| | MPI ([01-mpi](../01-mpi)) | MapReduce ([02-mapreduce](../02-mapreduce)) | gRPC streaming ([03-grpc-streaming](../03-grpc-streaming)) |
|---|---|---|---|
| Model | batch: P ranks, one collective | batch: map, shuffle and reduce steps, passing files | a continuous stream through long-lived services |
| Language | C++ | C++ (Hadoop Streaming executables) | Python 3.6 |
| Where partial results live | each rank's memory, combined by `MPI_Reduce` | map-output files on the shared file system, combined by the reducers | each worker's memory, combined by the coordinator on every query |
| When the answer is available | at the end | at the end | at any moment during the stream; exact at the end |
| Wall time, N = 10M, 4 processes | 0.238 s (own timer); 0.473 s with process launch | 0.799 s (both jobs), plus staging the input | 14.59 s |
| Throughput (records per second) | 42M (21M with launch) | 12.5M | 0.69M |
| Result vs sequential reference | byte-identical | byte-identical | byte-identical |
| What limits it | reading and parsing (near-linear to P = 7) | fixed per-run cost at small N; reading input over NFS at large N | the single producer-to-coordinator stream (~1 ms per message); the workers once messages are large |

## Takeaways

- **MapReduce vs MPI is a like-for-like comparison.** Both run the same C++ parsing code on the same nodes, so
  the ~1.7x gap with launch counted on both sides (0.799 s vs 0.473 s here) is the model's cost: three
  distributed steps that each start processes and pass data through files, against one in-memory collective.
- **gRPC vs the other two is mostly the runtime.** MPI parses a record in about 85 ns (0.850 s for 10M on one
  rank); a Python worker takes about 4.1 us, roughly 50x longer. The cluster's protobuf was also the pure-Python
  build, which makes every query 11-16x more expensive. The model itself adds a single ingestion stream that caps
  throughput until messages carry 10,000 records.
- **MPI is the fastest and uses the least machinery,** but its communication code is written by hand and a
  failed rank fails the job.
- **MapReduce needs no communication code** and, on real Hadoop, would re-run failed tasks, but every job pays for
  starting processes and staging data; that pays off only for inputs large enough to hide it (its parallel
  efficiency rises from 0.17 at 1M to 0.70 at 50M).
- **gRPC is the only one of the three that answers questions while the data is still arriving:** the dashboard's
  totals rise monotonically during the stream and land exactly on the sequential answer at the end. The price is
  that every query competes with ingestion for the coordinator's CPU.
- **All three win the same way:** aggregate locally into O(keys) state, communicate a small fixed-size summary, and
  compute the global top-K once after the merge, using integer-only arithmetic so the result is bit-exact at any
  degree of parallelism.
