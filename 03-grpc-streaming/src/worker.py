#!/usr/bin/env python3
"""Analytics worker: folds the batches it receives into a partial aggregate
and returns a snapshot of it on GetPartial.

    python3 worker.py 0.0.0.0:50100 --worker-id w0

One lock guards the aggregate. It is held for one whole batch while folding
and for the whole snapshot copy, so a query sees a whole number of batches.
"""

import argparse
import os
import sys
import threading
import time
from concurrent import futures

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc

import analytics_pb2 as pb
import analytics_pb2_grpc as pb_grpc
from aggregate import Aggregate, fold_lines


class WorkerServicer(pb_grpc.WorkerServiceServicer):

    def __init__(self, worker_id, quiet=False):
        self.worker_id = worker_id
        self._lock = threading.Lock()
        self._agg = Aggregate()
        self._records = 0
        self._batches = 0
        self._quiet = quiet

    def _log(self, fmt, *args):
        if not self._quiet:
            sys.stderr.write("[worker %s] %s\n" % (self.worker_id, fmt % args))
            sys.stderr.flush()

    def ProcessBatch(self, request_iterator, context):
        """Client-streaming: one open stream from the coordinator for the whole run."""
        n_rec = n_batch = 0
        c0 = time.process_time()      # CPU of every thread in this process
        w0 = time.perf_counter()
        for batch in request_iterator:
            t0 = time.perf_counter()
            with self._lock:
                if batch.count:
                    # raw wire format: parse here, in the scaled component
                    got = fold_lines(self._agg, batch.lines.decode())
                else:
                    add = self._agg.add_record
                    for r in batch.records:
                        add(r)
                    got = len(batch.records)
                self._agg.batches += 1
                self._agg.busy_seconds += time.perf_counter() - t0
                self._records += got
                self._batches += 1
            n_rec += got
            n_batch += 1

        self._log("stream closed: %d record(s) in %d batch(es)", n_rec, n_batch)
        # read by benchmark.sh: CPU, wall time, and time spent folding
        sys.stderr.write("INGEST_CPU %.6f %.6f %.6f\n"
                         % (time.process_time() - c0, time.perf_counter() - w0,
                            self._agg.busy_seconds))
        sys.stderr.flush()
        return pb.WorkerAck(worker_id=self.worker_id, records=n_rec, batches=n_batch)

    def GetPartial(self, request, context):
        """Copy the aggregate into a PartialState, under the lock."""
        with self._lock:
            a = self._agg
            msg = pb.PartialState(
                worker_id=self.worker_id,
                total_requests=a.total,
                successful_requests=a.successful,
                failed_requests=a.failed,
                rt_sum=a.rt_sum,
                rt_min=a.rt_min if a.rt_min is not None else 0,
                rt_max=a.rt_max if a.rt_max is not None else 0,
                total_bytes=a.bytes_total,
                status_2xx=a.status[0], status_3xx=a.status[1],
                status_4xx=a.status[2], status_5xx=a.status[3],
                batches_processed=a.batches,
                busy_seconds=a.busy_seconds)
            msg.servers.extend(
                pb.ServerStat(server_id=sid, count=c, rt_sum=s)
                for sid, (c, s) in a.servers.items())
            msg.endpoints.extend(
                pb.EndpointStat(endpoint_id=eid, count=c, bytes=b)
                for eid, (c, b) in a.endpoints.items())
            msg.intervals.extend(
                pb.IntervalStat(interval_id=iv, count=c)
                for iv, c in a.intervals.items())
        return msg

    def Reset(self, request, context):
        with self._lock:
            self._agg = Aggregate()
            self._records = 0
            self._batches = 0
        self._log("reset")
        return pb.ResetResponse(ok=True)


def main():
    ap = argparse.ArgumentParser(description="Streaming analytics worker")
    ap.add_argument("address", nargs="?", default="0.0.0.0:50100",
                    help="host:port to bind (default 0.0.0.0:50100)")
    ap.add_argument("--worker-id", default=None)
    ap.add_argument("--max-workers", type=int, default=8,
                    help="gRPC server thread pool size (default 8)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    host, _, port = args.address.rpartition(":")
    worker_id = args.worker_id or ("w%s" % port)

    servicer = WorkerServicer(worker_id, quiet=args.quiet)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=args.max_workers),
        options=[
            # 10,000-record batches exceed gRPC's 4 MB default
            ("grpc.max_receive_message_length", 256 * 1024 * 1024),
            ("grpc.max_send_message_length", 256 * 1024 * 1024),
        ])
    pb_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    bound = server.add_insecure_port("0.0.0.0:%d" % int(port))
    if bound == 0:
        sys.exit("worker: could not bind port %s (already in use?)" % port)

    server.start()
    sys.stderr.write("[worker %s] listening on 0.0.0.0:%s\n" % (worker_id, port))
    sys.stderr.flush()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1.0).wait()


if __name__ == "__main__":
    main()
