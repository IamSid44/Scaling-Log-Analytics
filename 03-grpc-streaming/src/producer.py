#!/usr/bin/env python3
"""Streaming client: replays a pre-generated dataset to the coordinator as one
client-streaming RPC, as if the records were arriving live.

    python3 producer.py node01:50051 --input data/logs_1000000.txt --batch 1000
    python3 producer.py node01:50051 --input data/logs.txt --rate 50000 --report final.txt

  --batch N         records per RecordBatch message (streaming granularity)
  --rate R          records per second; 0 = unthrottled
  --wire raw        send the text lines; the workers parse them (default)
  --wire parsed     parse here and send structured Records (the contrast discussed in the README)

The file is read line by line, so a large dataset costs one batch of memory.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc

import analytics_pb2 as pb
import analytics_pb2_grpc as pb_grpc
from aggregate import parse_line, render_report

CHANNEL_OPTS = [
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
]


def batches(path, batch_size, rate, limit, source, progress_every, malformed_box,
            wire="raw"):
    """Yield RecordBatch messages lazily; gRPC pulls from this generator, so
    backpressure from the coordinator slows the pull.
    """
    t0 = time.perf_counter()
    sent = 0
    batch_id = 0
    buf = []

    with open(path) as f:
        f.readline()                       # the "N K S" header, not a record
        if wire == "raw":
            for line in f:
                if limit and sent >= limit:
                    break
                if not line.strip():
                    continue
                buf.append(line)
                sent += 1
                if len(buf) >= batch_size:
                    yield pb.RecordBatch(lines="".join(buf).encode(), count=len(buf),
                                         batch_id=batch_id, source=source)
                    buf = []
                    batch_id += 1
                    if rate > 0:
                        behind = sent / float(rate) - (time.perf_counter() - t0)
                        if behind > 0:
                            time.sleep(behind)
            if buf:
                yield pb.RecordBatch(lines="".join(buf).encode(), count=len(buf),
                                     batch_id=batch_id, source=source)
            return

        for line in f:
            if limit and sent >= limit:
                break
            rec = parse_line(line)
            if rec is None:
                if line.strip():
                    malformed_box[0] += 1
                continue
            ts, sid, eid, uid, sc, rt, nb = rec
            buf.append(pb.Record(timestamp=ts, server_id=sid, endpoint_id=eid,
                                 user_id=uid, status_code=sc, rt_micros=rt,
                                 bytes_sent=nb))
            sent += 1

            if len(buf) >= batch_size:
                yield pb.RecordBatch(records=buf, batch_id=batch_id, source=source)
                buf = []
                batch_id += 1

                if rate > 0:
                    # pace against elapsed time, so the average rate stays exact
                    target = sent / float(rate)
                    behind = target - (time.perf_counter() - t0)
                    if behind > 0:
                        time.sleep(behind)

                if progress_every and batch_id % progress_every == 0:
                    el = time.perf_counter() - t0
                    sys.stderr.write("\r[producer] %d records, %.0f rec/s "
                                     % (sent, sent / max(el, 1e-9)))
                    sys.stderr.flush()

    if buf:
        yield pb.RecordBatch(records=buf, batch_id=batch_id, source=source)


def main():
    ap = argparse.ArgumentParser(description="Replay a log dataset as a gRPC stream")
    ap.add_argument("address", nargs="?", default="localhost:50051",
                    help="coordinator address (default localhost:50051)")
    ap.add_argument("--input", required=True, help="dataset produced by gen_logs.py")
    ap.add_argument("--batch", type=int, default=1000,
                    help="records per message (default 1000)")
    ap.add_argument("--rate", type=int, default=0,
                    help="records/second, 0 = unthrottled (default 0)")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after this many records, 0 = whole file")
    ap.add_argument("--source", default=None, help="producer name")
    ap.add_argument("--report", default=None,
                    help="write the final analytics report here when done")
    ap.add_argument("--top-k", type=int, default=0,
                    help="top-K for the final report, 0 = the dataset's own K")
    ap.add_argument("--wire", choices=("raw", "parsed"), default="raw",
                    help="raw: ship text lines, workers parse (default). "
                         "parsed: parse here and ship structured records")
    ap.add_argument("--progress", type=int, default=200,
                    help="print progress every N batches, 0 to disable")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        sys.exit("producer: no such dataset: %s" % args.input)
    if args.batch < 1:
        sys.exit("producer: --batch must be at least 1")

    # K comes from the dataset header, as in 01-mpi
    k = args.top_k
    if k <= 0:
        with open(args.input) as f:
            head = f.readline().split()
        k = int(head[1]) if len(head) >= 2 else 10

    source = args.source or ("producer-%d" % os.getpid())
    channel = grpc.insecure_channel(args.address, options=CHANNEL_OPTS)
    try:
        grpc.channel_ready_future(channel).result(timeout=60.0)
    except grpc.FutureTimeoutError:
        sys.exit("producer: no coordinator answering at %s" % args.address)
    stub = pb_grpc.CoordinatorServiceStub(channel)

    sys.stderr.write("[producer] streaming %s -> %s (batch=%d, rate=%s)\n"
                     % (args.input, args.address, args.batch,
                        args.rate if args.rate else "unthrottled"))
    sys.stderr.flush()

    malformed_box = [0]
    t0 = time.perf_counter()
    c0 = time.process_time()      # CPU of every thread in this process
    try:
        summary = stub.StreamRecords(
            batches(args.input, args.batch, args.rate, args.limit, source,
                    args.progress, malformed_box, args.wire))
    except grpc.RpcError as e:
        sys.exit("\nproducer: ingestion failed: %s: %s" % (e.code().name, e.details()))
    wall = time.perf_counter() - t0
    cpu = time.process_time() - c0

    if args.progress:
        sys.stderr.write("\r" + " " * 60 + "\r")

    # end-to-end throughput: the whole RPC, backpressure stalls included
    print("RECORDS %d" % summary.records_received)
    print("BATCHES %d" % summary.batches_received)
    print("MALFORMED %d" % malformed_box[0])
    print("TIME_INGEST %.6f" % wall)
    print("THROUGHPUT_RPS %.1f" % (summary.records_received / max(wall, 1e-9)))
    # CPU / TIME_INGEST = cores busy; ~1.0 means the producer was the bottleneck
    print("CPU_PRODUCER %.6f" % cpu)

    if args.report:
        # exact: StreamRecords returned only after every worker acknowledged
        snap = stub.GetAnalytics(pb.AnalyticsRequest(top_k=k))
        with open(args.report, "w", newline="\n") as f:
            f.write(render_report(snap, k))
        print("REPORT %s" % args.report)
        print("WORKERS %d" % snap.worker_count)
        print("TIME_MERGE %.6f" % snap.merge_seconds)

    channel.close()


if __name__ == "__main__":
    main()
